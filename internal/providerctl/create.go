package providerctl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Create implements managed.ExternalClient (HBEP-4, Two controllers, Create):
// in ssh dial-home mode it checks the dial-home key; it stages the agent's
// binary and the Allocation's files; it claims attempt n+1 by compare-and-swap
// (a lost claim submits nothing); it submits, held in hold mode; it records the
// outcome, and an accepted job's ID by compare-and-swap; and in hold mode it
// settles at once, so the hold lasts the time of one listing read. It returns
// an error for anything that kept it from submitting and for a Rejected
// outcome, so crossplane backs off; an Unknown outcome is no error, and the
// tag resolves it. So is a refusal for a full submit limit once it is
// recorded (rejected).
func (e *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalCreation{}, errNotAllocation
	}
	if e.refused != nil {
		return managed.ExternalCreation{}, e.refused
	}
	name := meta.GetExternalName(a)
	if tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID); name != tag {
		return managed.ExternalCreation{}, fmt.Errorf("refusing to submit: the external name is not this Allocation's job tag %s", tag)
	}
	rec, err := readRecord(a)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	now := e.now()
	switch s := rec.sub; {
	case rec.jobID != "":
		return managed.ExternalCreation{}, fmt.Errorf("job %s is already recorded for this Allocation; refusing to submit another", rec.jobID)
	case needsResolution(rec) && !(e.hold() && now.Sub(attemptTime(a, s)) > e.inflightWindow()):
		return managed.ExternalCreation{}, fmt.Errorf("submission attempt %d is unresolved; refusing to submit another", s.Attempt)
	case s != nil && s.State == v1alpha1.SubmissionRejected && now.Sub(attemptTime(a, s)) < e.s.o.RejectBackoff:
		return managed.ExternalCreation{}, fmt.Errorf("attempt %d was rejected; the next waits for backoff", s.Attempt)
	}
	req := provider.SubmitRequest{Tag: name, Hold: e.hold(), Account: e.v.Settings.Account, QOS: e.v.Settings.QOS, Params: *a.Spec.ForProvider.Batch}
	if err := slurm.CheckRequest(req, e.v.Profile, e.v.Settings); err != nil {
		return managed.ExternalCreation{}, e.refuse(ctx, a, rec, now, err)
	}
	if why := e.prerequisites(a); why != "" {
		return managed.ExternalCreation{}, errors.New(why)
	}
	paths, err := e.p.Paths(ctx)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	if e.s.o.Front.Mode == agentapi.DialHomeSSH {
		present, err := e.p.DialHomeKeysPresent(ctx)
		if err != nil {
			return managed.ExternalCreation{}, err
		}
		if err := e.s.writeDialHomeKeyMissing(ctx, !present); err != nil {
			e.s.o.Log.Info("cannot write DialHomeKeyMissing; the status loop writes it at its next pass", "error", err)
		}
		if !present {
			return managed.ExternalCreation{}, fmt.Errorf("%s: the dial-home key files are missing in ~/%s on the platform; nothing is submitted", v1alpha1.ConditionDialHomeKeyMissing, agentapi.DialHomeDir)
		}
	}
	files, err := e.files(a, name, paths)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	for _, f := range files {
		if err := e.p.EnsureFile(ctx, f); err != nil {
			return managed.ExternalCreation{}, fmt.Errorf("stage %s: %w", filepath.Base(f.Path), err)
		}
	}

	n := int32(1)
	if rec.sub != nil {
		n = rec.sub.Attempt + 1
	}
	claim := v1alpha1.SubmissionRecord{Attempt: n, Token: newToken(), State: v1alpha1.SubmissionInFlight, Writer: e.s.o.Identity, Time: mt(now)}
	if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(claim)}); err != nil {
		return managed.ExternalCreation{}, fmt.Errorf("could not claim submission attempt %d, nothing submitted: %w", n, err)
	}
	e.p.SetBusy(name, true)
	id, outcome, serr := e.p.Submit(ctx, req)
	// The outcome is written with a fresh context: the reconcile's may have
	// run out with sbatch, and the record must not stay InFlight.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	switch outcome {
	case provider.Accepted:
		e.s.limitEvents.Delete(a.UID) // a full slot, if there was one, has cleared
		return managed.ExternalCreation{}, e.accepted(wctx, a, claim, id)
	case provider.Rejected:
		return managed.ExternalCreation{}, e.rejected(wctx, a, claim, serr)
	default:
		u := claim
		u.State, u.Reason = v1alpha1.SubmissionUnknown, truncate(errText(serr))
		if _, err := writeOwn(wctx, e.s.kube, e.s.reader, a, claim.Token, func(record) map[string]string {
			return map[string]string{v1alpha1.AnnotationSubmission: recordValue(u)}
		}); err != nil {
			e.s.o.Log.Info("cannot record the unknown outcome; the claim stays InFlight", "attempt", n, "error", err)
		}
		e.event(a, event.Warning("SubmissionUnknown", fmt.Errorf("attempt %d: %s; the tag will resolve it", n, u.Reason)))
		return managed.ExternalCreation{}, nil
	}
}

// rejected records a Rejected outcome as the attempt's state, and tells the
// caller what to return. A refusal for a full submit limit (a
// *provider.PolicyRefusal that is a SubmitLimit) is written with its Code in
// the same record as the state, so nothing ever sees the state without it; it
// waits for the backoff and fails nothing (HBEP-4, QOS limits). Once the
// record is written it returns no error, because a full slot can last hours
// and an error would flip the Allocation's Synced condition between
// ReconcileError and ReconcileSuccess at every backoff (`@hpc-burst-science-agent`,
// 1 Nov 2026, on the refusal contract). If the record cannot be written the
// error is returned, as for any Rejected outcome. Any other refusal keeps
// returning an error, so crossplane backs off.
func (e *external) rejected(ctx context.Context, a *v1alpha1.Allocation, claim v1alpha1.SubmissionRecord, serr error) error {
	r := claim
	r.State, r.Time, r.Reason = v1alpha1.SubmissionRejected, mt(e.now()), truncate(errText(serr))
	var pr *provider.PolicyRefusal
	limit := errors.As(serr, &pr) && pr.SubmitLimit()
	if limit {
		r.Code = v1alpha1.ReasonSubmitLimitReached
	}
	_, werr := writeOwn(ctx, e.s.kube, e.s.reader, a, claim.Token, func(record) map[string]string {
		return map[string]string{v1alpha1.AnnotationSubmission: recordValue(r)}
	})
	if werr != nil {
		e.s.o.Log.Info("cannot record the rejection", "attempt", claim.Attempt, "error", werr)
	}
	if !limit {
		e.s.limitEvents.Delete(a.UID) // the condition, if there was one, has ended
		e.event(a, event.Warning("SubmissionRejected", fmt.Errorf("attempt %d: %s", claim.Attempt, r.Reason)))
		return fmt.Errorf("attempt %d rejected: %s", claim.Attempt, r.Reason)
	}
	// The event keeps today's reason, SubmissionRejected: SubmitLimitReached is
	// a condition reason only, so HBEP-8's event reasons are unchanged (HBEP-7,
	// Events and conditions; `@hpc-burst-science-agent`, 1 Nov 2026). A full slot
	// can last hours, so the event is a repeating one: emitted when the refusal
	// starts, then at most once an hour (limitEventDue). The note says it is a
	// full submit limit.
	if e.s.limitEventDue(a.UID, e.now()) {
		e.event(a, event.Warning("SubmissionRejected", fmt.Errorf("attempt %d: submit limit reached: %s", claim.Attempt, r.Reason)))
	}
	if werr != nil {
		return fmt.Errorf("attempt %d rejected: %s", claim.Attempt, r.Reason)
	}
	return nil
}

// refuse records a request that HBEP-4 refuses before any call (Submission:
// memory 0, no GPU on a GPU partition, a time limit above maxTime, and the
// rest) as attempt n+1 Rejected, with an API write only: no file is staged, no
// command runs, and the next attempt waits for the backoff, as for any
// Rejected attempt (review of PR 76: Opus N1). The write is a
// compare-and-swap: a conflict means another reconciler moved the record, and
// the next reconcile decides again.
func (e *external) refuse(ctx context.Context, a *v1alpha1.Allocation, rec record, now time.Time, cause error) error {
	n := int32(1)
	if rec.sub != nil {
		n = rec.sub.Attempt + 1
	}
	r := v1alpha1.SubmissionRecord{Attempt: n, Token: newToken(), State: v1alpha1.SubmissionRejected, Writer: e.s.o.Identity,
		Time: mt(now), Reason: truncate(cause.Error())}
	if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(r)}); err != nil {
		return fmt.Errorf("attempt %d is refused before any call, but the refusal was not recorded: %w", n, err)
	}
	e.event(a, event.Warning("SubmissionRejected", fmt.Errorf("attempt %d: %s", n, r.Reason)))
	return fmt.Errorf("attempt %d rejected: %s", n, r.Reason)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// accepted records the job this Create submitted, unless another job was
// recorded first (a concurrent or older attempt), and then settles in hold
// mode. An extra job is cancelled while held, or, in fallback mode, reported.
func (e *external) accepted(ctx context.Context, a *v1alpha1.Allocation, claim v1alpha1.SubmissionRecord, id string) error {
	var recorded string
	wrote, err := writeOwnOrRecorded(ctx, e, a, claim, id, &recorded)
	if err != nil {
		return fmt.Errorf("job %s was submitted but not recorded; a later observe adopts it by tag: %w", id, err)
	}
	if recorded != id {
		if e.hold() {
			if err := e.cancel(ctx, []string{id}); err != nil {
				e.s.o.Log.Info("cannot cancel the extra held job", "job", id, "error", err)
			}
			e.event(a, event.Normal("HeldDuplicateCancelled", fmt.Sprintf("job %s was cancelled while held: job %s was recorded first", id, recorded)))
		} else {
			e.event(a, event.Warning("DuplicateJob", fmt.Errorf("job %s carries the tag but job %s was recorded first", id, recorded)))
		}
		return nil
	}
	if wrote {
		e.event(a, event.Normal("JobSubmitted", fmt.Sprintf("job %s submitted by attempt %d", id, claim.Attempt)))
	}
	if !e.hold() || !wrote || meta.WasDeleted(a) {
		return nil
	}
	rec, err := readRecord(a)
	if err != nil {
		return err
	}
	job, found, err := e.p.Observe(ctx, id)
	if err != nil || !found {
		return nil // the next observe settles
	}
	// A renamed job is left held: the next observe refuses and cancels it.
	// settle lifts the job it is given even when the tag's listing does not
	// show it, so the name is checked here.
	if job.Name != meta.GetExternalName(a) {
		return nil
	}
	if _, err := e.settle(ctx, a, &rec, meta.GetExternalName(a), job); err != nil {
		e.s.o.Log.Info("cannot settle after the submission; the next observe will", "job", id, "error", err)
	}
	return nil
}

// writeOwnOrRecorded records id as the job, and the attempt Done, unless a job
// is already recorded: then it only resolves its own attempt, and recorded is
// that other job. Conflicts re-read from the API server. Ported from spike
// S4's establishOwn.
func writeOwnOrRecorded(ctx context.Context, e *external, a *v1alpha1.Allocation, claim v1alpha1.SubmissionRecord, id string, recorded *string) (bool, error) {
	for i := 0; i < 5; i++ {
		rec, err := readRecord(a)
		if err != nil {
			return false, err
		}
		*recorded = rec.jobID
		set := map[string]string{}
		if rec.jobID == "" {
			*recorded = id
			set[v1alpha1.AnnotationJobID] = id
		}
		if rec.sub != nil && rec.sub.Token == claim.Token && rec.sub.State != v1alpha1.SubmissionDone {
			d := claim
			d.State, d.Time = v1alpha1.SubmissionDone, mt(e.now())
			if *recorded != id {
				d.Reason = fmt.Sprintf("job %s is an extra: job %s was recorded first", id, *recorded)
			}
			set[v1alpha1.AnnotationSubmission] = recordValue(d)
		}
		if len(set) == 0 {
			return false, nil
		}
		err = write(ctx, e.s.kube, a, set)
		if err == nil {
			_, wrote := set[v1alpha1.AnnotationJobID]
			return wrote, nil
		}
		if !kerrors.IsConflict(err) {
			return false, err
		}
		if err := refresh(ctx, e.s.reader, a); err != nil {
			return false, err
		}
	}
	return false, errors.New("still conflicting after 5 re-reads")
}

// prerequisites returns why the Allocation cannot be submitted yet, from the
// control-plane host alone (no platform call), or "" when it can: a front must
// be configured (HBEP-11) with a frpc digest for the partition's CPU
// architecture (agent.json names it; a configuration check, so it comes before
// any remote call or status write), the Allocation's credential must exist
// (docs/development.md: until it does, the provider waits; it neither submits
// nor marks the attempt Rejected), and an agent build with the Allocation's
// digest must exist for the partition's CPU architecture.
func (e *external) prerequisites(a *v1alpha1.Allocation) string {
	if e.s.o.Front == nil {
		return "no front is configured, so no job is submitted (HBEP-11)"
	}
	if _, err := e.frpcDigest(); err != nil {
		return err.Error()
	}
	if _, err := e.credential(a); err != nil {
		return "waiting for the Allocation's credential: " + err.Error()
	}
	if _, err := e.s.builds.find(e.s.o.AgentBuildsDir, e.v.Profile.Partition.CPUArch, a.Spec.ForProvider.Batch.AgentDigest); err != nil {
		return err.Error()
	}
	return ""
}

// credential reads the Allocation's credential, which the lifecycle controller
// writes once the Allocation has its UID. It is read with agentapi.ReadKeyFile,
// the reader the credential's consumers use (docs/development.md, Key files),
// so a file the agent API would refuse, a symlink among them, is never staged
// (review of PR 76: Astra B9, Opus N13). ReadKeyFile also takes modes 400 and
// 700; HBEP-4 says mode 600, so that is checked first. The errors reach the
// Allocation's status and events, so they name no path.
func (e *external) credential(a *v1alpha1.Allocation) ([]byte, error) {
	if e.s.o.CredentialsDir == "" {
		return nil, errors.New("no credentials directory is configured")
	}
	path := agentapi.AllocationCredentialPath(e.s.o.CredentialsDir, string(a.UID))
	fi, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, errors.New("the file does not exist yet")
	case err != nil:
		return nil, errors.New("the file cannot be read")
	case fi.Mode().Type() != 0 || fi.Mode().Perm() != 0o600:
		return nil, errors.New("the file must be a regular file of this user, mode 600, not a link")
	}
	key, err := agentapi.ReadKeyFile(path)
	if err != nil {
		return nil, errors.New(strings.ReplaceAll(err.Error(), path, "the credential file"))
	}
	return []byte(key), nil
}

// files are the three files staged before a claim (HBEP-4, Submission): the
// agent's binary, the Allocation's credential and agent.json.
func (e *external) files(a *v1alpha1.Allocation, tag string, paths slurm.Paths) ([]provider.File, error) {
	digest := a.Spec.ForProvider.Batch.AgentDigest
	bin, err := e.s.builds.read(e.s.o.AgentBuildsDir, e.v.Profile.Partition.CPUArch, digest)
	if err != nil {
		return nil, err
	}
	cred, err := e.credential(a)
	if err != nil {
		return nil, err
	}
	f := e.s.o.Front
	frpc, err := e.frpcDigest()
	if err != nil {
		return nil, err
	}
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: tag, Mode: f.Mode, Endpoint: f.Endpoint,
		CertSPKISHA256: f.CertPin, FrpcDigest: frpc, StagingDir: paths.Staging, DialHomeDir: paths.DialHomeDir(),
	}
	if f.Mode == agentapi.DialHomeSSH {
		cfg.SSH = &agentapi.SSHForward{User: f.SSHUser, AgentPort: f.AgentPort, DialHomePort: f.DialHomePort}
	}
	js, err := cfg.Encode()
	if err != nil {
		return nil, fmt.Errorf("agent.json: %w", err)
	}
	return []provider.File{
		file(agentapi.StagedAgentPath(digest), 0o700, bin),
		file(agentapi.StagedCredentialPath(tag), 0o600, cred),
		file(agentapi.StagedConfigPath(tag), 0o600, js),
	}, nil
}

// frpcDigest is the frpc digest that agent.json names: the Front settings' one
// for the partition's CPU architecture (HBEP-4, Submission: Files first). It
// reads configuration only. prerequisites calls it, so a missing digest stops a
// Create before any call to the platform.
func (e *external) frpcDigest() (string, error) {
	arch := e.v.Profile.Partition.CPUArch
	d, ok := e.s.o.Front.FrpcDigests[arch]
	if !ok {
		return "", fmt.Errorf("no frpc digest is configured for cpuArch %s", arch)
	}
	return d, nil
}

func file(path string, mode os.FileMode, content []byte) provider.File {
	sum := sha256.Sum256(content)
	return provider.File{Path: path, SHA256: hex.EncodeToString(sum[:]), Mode: mode, Content: content}
}

// buildCache finds an agent build by digest without hashing it on every
// reconcile: a file's digest is kept while its size and modification time
// stay the same.
type buildCache struct {
	mu   sync.Mutex
	seen map[string]buildKey
}

type buildKey struct {
	size   int64
	mtime  time.Time
	digest string
}

// maxAgentBuild bounds an agent build read into memory.
const maxAgentBuild = 256 << 20

func (c *buildCache) path(dir, arch string) (string, error) {
	if dir == "" {
		return "", errors.New("no agent-builds directory is configured")
	}
	if arch == "" || strings.ContainsAny(arch, "/\\") {
		return "", errors.New("the partition names no usable cpuArch")
	}
	return filepath.Join(dir, agentapi.AgentBuildName(arch)), nil
}

// find checks that the build for arch has the digest.
func (c *buildCache) find(dir, arch, digest string) (string, error) {
	p, err := c.path(dir, arch)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return "", fmt.Errorf("no agent build for cpuArch %s", arch)
	}
	c.mu.Lock()
	k, ok := c.seen[p]
	c.mu.Unlock()
	if !ok || k.size != fi.Size() || !k.mtime.Equal(fi.ModTime()) {
		b, err := c.read(dir, arch, "")
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(b)
		k = buildKey{size: fi.Size(), mtime: fi.ModTime(), digest: hex.EncodeToString(sum[:])}
		c.mu.Lock()
		if c.seen == nil {
			c.seen = map[string]buildKey{}
		}
		c.seen[p] = k
		c.mu.Unlock()
	}
	if k.digest != digest {
		return "", fmt.Errorf("the agent build for cpuArch %s has digest %s, not the Allocation's agentDigest %s", arch, k.digest, digest)
	}
	return p, nil
}

// read returns the build for arch; with a digest, only when its content has
// that digest.
func (c *buildCache) read(dir, arch, digest string) ([]byte, error) {
	p, err := c.path(dir, arch)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("no agent build for cpuArch %s", arch)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxAgentBuild+1))
	if err != nil {
		return nil, fmt.Errorf("read the agent build for cpuArch %s: %w", arch, err)
	}
	if len(b) > maxAgentBuild {
		return nil, fmt.Errorf("the agent build for cpuArch %s is larger than %d bytes", arch, maxAgentBuild)
	}
	if digest != "" {
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != digest {
			return nil, fmt.Errorf("the agent build for cpuArch %s does not have the Allocation's agentDigest", arch)
		}
	}
	return b, nil
}

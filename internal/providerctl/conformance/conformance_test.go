// Package conformance runs the Slurm provider against a real platform (HBEP-4,
// Graduation criteria, alpha): one Allocation submitted held by the
// reconciler, its hold lifted once, read back, observed Running with its hard
// end in UTC, cancelled by deleting it and found ended in accounting after it
// left the queue listing; and one job whose record was lost, found by tag,
// adopted, lifted once and observed through to its end. Every reply is parsed
// with the profile's data_parser version, so a run on Helios (v0.0.42) and one
// on Athena (v0.0.40) parse real replies of both.
//
// The platform under test is a shipped PlatformProfile, profiles/helios.yaml or
// profiles/athena.yaml, read whole (profile_test.go); only its stagingDir is
// replaced by a fresh subdirectory of it. Nothing about a platform is in this
// file.
//
// It runs only with HPCBURST_CONFORMANCE=1 and the settings below, all from
// the environment. It skips, loudly, otherwise. Job IDs, the account, host
// names and paths go only to the private output directory, never to the test
// log.
//
//	HPCBURST_CONF_PROFILE      the shipped profile: helios or athena
//	HPCBURST_CONF_PARTITION    the partition under test; the profile must list it
//	HPCBURST_CONF_CREDENTIALS  the mode-600 ssh credentials file (slurm.SSHConfig form)
//	HPCBURST_CONF_ACCOUNT      the account; never logged
//	HPCBURST_CONF_GPUS         GPUs per job (0 for a CPU partition)
//	HPCBURST_CONF_CPUS         cpusPerTask
//	HPCBURST_CONF_MEMORY       memory, a Kubernetes quantity
//	HPCBURST_CONF_STAGING      the run's stagingDir, a directory below the profile's
//	                           (for example $SCRATCH/<own dir>), not the root
//	HPCBURST_CONF_HOST_LOCK    the host's lock file: one ssh call at a time
//	HPCBURST_CONF_CONTROL_DIR  a short private directory for the ssh master
//	HPCBURST_CONF_OUT          a private directory for the run's records
//
// HPCBURST_CONF_GPU_MODEL, _CPU_ARCH and _DATA_PARSER are not read any more:
// the profile holds them, and a run that sets one stops.
package conformance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func load(t *testing.T) settings {
	t.Helper()
	if os.Getenv("HPCBURST_CONFORMANCE") != "1" {
		t.Skip("set HPCBURST_CONFORMANCE=1 and the HPCBURST_CONF_* settings to run against a real platform")
	}
	s, err := readSettings(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("profile %s, partition %s, data_parser %s", s.profile.Name, s.partition, s.profile.Spec.JSONDataParser)
	return s
}

// private records go to the output directory, mode 600, never to the log.
type private struct {
	t   *testing.T
	dir string
	f   *os.File
}

func newPrivate(t *testing.T, dir string) *private {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "run-"+time.Now().UTC().Format("20060102T150405Z")+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return &private{t: t, dir: dir, f: f}
}

func (p *private) log(format string, a ...any) {
	fmt.Fprintf(p.f, time.Now().UTC().Format(time.RFC3339)+" "+format+"\n", a...)
}

func (p *private) save(name string, b []byte) {
	_ = os.WriteFile(filepath.Join(p.dir, name), b, 0o600)
}

// capture is a Runner that records every reply, for the private record of
// the real JSON, and the duration of each sbatch.
type capture struct {
	slurm.Runner
	p       *private
	sbatchD []time.Duration
	n       int
	// submitted gets every job ID an sbatch printed, so cleanup can cancel
	// each job this run submitted, recorded or not.
	submitted func(id string)
}

func (c *capture) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	start := time.Now()
	res, err := c.Runner.Run(ctx, cmd)
	d := time.Since(start)
	word := strings.SplitN(cmd.Line, " ", 2)[0]
	if word == "sbatch" {
		c.sbatchD = append(c.sbatchD, d)
		if id := strings.TrimSpace(strings.SplitN(string(res.Stdout), ";", 2)[0]); err == nil && res.ExitCode == 0 && id != "" {
			c.submitted(id)
		}
	}
	c.n++
	c.p.log("call %d %s exit %d err %v %s", c.n, word, res.ExitCode, err, d)
	if word == "squeue" || word == "sacct" {
		c.p.save(fmt.Sprintf("reply-%03d-%s.json", c.n, word), res.Stdout)
	}
	return res, err
}

func (c *capture) Connect(ctx context.Context) error {
	if k, ok := c.Runner.(slurm.Connector); ok {
		return k.Connect(ctx)
	}
	return nil
}

// world is one run: the controller over a fake API client, and the real
// platform over ssh.
type world struct {
	t       *testing.T
	s       settings
	p       *private
	c       client.Client
	ctl     *providerctl.Controller
	batch   *slurm.Batch
	ssh     *sshrun.SSH
	cap     *capture
	jobIDs  map[string]bool
	digest  string
	credDir string
}

const testNS = "conformance"

func newWorld(t *testing.T) *world {
	s := load(t)
	w := &world{t: t, s: s, p: newPrivate(t, s.out), jobIDs: map[string]bool{}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// The profile is the shipped file as read; the Provider names it and allows
	// the one partition under test.
	pp := s.profile
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "conformance"}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: pp.Name}, Account: s.account, AllowedPartitions: []string{s.partition},
		CredentialsRef: &v1alpha1.CredentialsReference{File: s.creds},
	}}
	w.c = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(pp, prov).
		WithStatusSubresource(&v1alpha1.Allocation{}, &v1alpha1.Provider{}).Build()

	// The agent stand-in, run by the bootstrap after its digest check, with the
	// Allocation's directory as its argument: the first job outlives its 5 min
	// limit, so it is still running when its Allocation is deleted; the
	// second sleeps 90 s and ends by itself.
	agent := []byte("#!/bin/sh\ncase \"$1\" in *held-submit*) sleep 600 ;; *) sleep 90 ;; esac\nexit 0\n")
	sum := sha256.Sum256(agent)
	w.digest = hex.EncodeToString(sum[:])
	builds := t.TempDir()
	if err := os.WriteFile(filepath.Join(builds, agentapi.AgentBuildName(s.part.CPUArch)), agent, 0o700); err != nil {
		t.Fatal(err)
	}
	w.credDir = t.TempDir()

	ctl, err := providerctl.New(w.c, w.c, scheme, providerctl.Options{
		Provider: prov.Name,
		Profiles: &slurm.ProfileReader{Reader: w.c},
		NewPlatform: func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
			t, err := sshrun.New(sshrun.Options{Name: v.Settings.Provider, Config: v.SSH, ControlDir: s.control, HostLock: s.lock, MaxConcurrent: 1})
			if err != nil {
				return nil, nil, err
			}
			w.ssh = t
			w.cap = &capture{Runner: t, p: w.p, submitted: func(id string) { w.jobIDs[id] = true }}
			w.batch = slurm.NewBatch(w.cap, v.Profile, v.Settings, slurm.Options{PollInterval: 10 * time.Second, BusyInterval: 3 * time.Second})
			return w.batch, t, nil
		},
		CredentialsDir: w.credDir, AgentBuildsDir: builds,
		Front: &providerctl.FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "127.0.0.1:29261", CertPin: strings.Repeat("0", 64),
			FrpcDigests: map[string]string{s.part.CPUArch: strings.Repeat("d", 64)}},
		PollInterval: 10 * time.Second, BusyInterval: 3 * time.Second, CreationGracePeriod: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	w.ctl = ctl
	t.Cleanup(w.cleanup)
	return w
}

// cleanup cancels every job ID this run saw that is still live, and closes the
// ssh master. It cancels only this run's own job IDs.
func (w *world) cleanup() {
	if w.batch != nil {
		var ids []string
		for id := range w.jobIDs {
			if j, found, err := w.batch.Observe(context.Background(), id); err != nil || (found && j.EndTime == nil) {
				ids = append(ids, id)
			}
		}
		for _, r := range w.batch.Cancel(context.Background(), ids) {
			w.p.log("cleanup cancel %s: %s %v", r.JobID, r.Outcome, r.Err)
		}
	}
	if w.ssh != nil {
		w.t.Logf("ssh masters started in this run: %d", w.ssh.Starts())
		_ = w.ssh.Close()
	}
	if w.cap != nil {
		w.t.Logf("sbatch latencies over ssh: %v", w.cap.sbatchD)
	}
	w.ctl.Close()
}

func (w *world) newAlloc(name string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID(fmt.Sprintf("conf-%d-%s", time.Now().UnixNano(), name)),
			CreationTimestamp: metav1.NewTime(time.Now())},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "conformance"},
			ManagementPolicies:      v1alpha1.DefaultManagementPolicies(),
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: w.s.partition, TimeLimit: "5m", GPUs: w.s.gpus, CPUsPerTask: w.s.cpus, Memory: w.s.memory, Nodes: 1, AgentDigest: w.digest,
			}},
		},
	}
	if err := w.c.Create(context.Background(), a); err != nil {
		w.t.Fatal(err)
	}
	p := agentapi.AllocationCredentialPath(w.credDir, string(a.UID))
	_ = os.MkdirAll(filepath.Dir(p), 0o700)
	cred := make([]byte, 32)
	if _, err := rand.Read(cred); err != nil {
		w.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(hex.EncodeToString(cred)), 0o600); err != nil {
		w.t.Fatal(err)
	}
	return w.get(name)
}

func (w *world) get(name string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{}
	if err := w.c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: name}, a); err != nil {
		return nil
	}
	return a
}

func (w *world) reconcile(name string) {
	_, err := w.ctl.Reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}})
	if err != nil {
		w.p.log("reconcile %s: %v", name, err)
	}
	if a := w.get(name); a != nil {
		if id := a.Annotations[v1alpha1.AnnotationJobID]; id != "" {
			w.jobIDs[id] = true
		}
		w.p.log("%s phase %s reason %q job %s", name, a.Status.AtProvider.Phase, a.Status.AtProvider.Reason, a.Annotations[v1alpha1.AnnotationJobID])
	}
}

// until reconciles every 3 s until cond holds.
func (w *world) until(name, what string, timeout time.Duration, cond func(*v1alpha1.Allocation) bool) *v1alpha1.Allocation {
	w.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		w.reconcile(name)
		a := w.get(name)
		if a != nil && cond(a) {
			return a
		}
		if time.Now().After(deadline) {
			ph := ""
			if a != nil {
				ph = string(a.Status.AtProvider.Phase)
			}
			w.t.Fatalf("timed out after %s waiting until %s (phase %s)", timeout, what, ph)
		}
		time.Sleep(3 * time.Second)
	}
}

func (w *world) callWords() []string {
	b, _ := os.ReadFile(w.p.f.Name())
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 4 && f[1] == "call" {
			out = append(out, f[3])
		}
	}
	return out
}

func count(words []string, w string) int {
	n := 0
	for _, x := range words {
		if x == w {
			n++
		}
	}
	return n
}

// TestConformance is the run on one platform.
func TestConformance(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	start := time.Now()

	// 1. The reconciler submits held, lifts once, reads back and observes
	// Running. The Allocation's name has 45 characters, so its tag has the
	// full 56.
	name := "conformance-held-submit-and-cancel-on-delete"
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	if len(tag) != v1alpha1.JobTagLength {
		t.Fatalf("the tag has %d characters, want %d", len(tag), v1alpha1.JobTagLength)
	}
	a = w.until(name, "the job runs", 30*time.Minute, func(a *v1alpha1.Allocation) bool {
		return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning
	})
	words := w.callWords()
	if n := count(words, "sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1", n)
	}
	if n := count(words, "scontrol"); n != 1 {
		t.Errorf("%d scontrol release calls, want 1", n)
	}
	if a.Annotations[v1alpha1.AnnotationHoldLifted] != a.Annotations[v1alpha1.AnnotationJobID] {
		t.Error("the hold-lifted marker does not name the job")
	}
	// A read-back that matches sets no condition (api/v1alpha1,
	// ReasonReadBackMismatch); True would name the differing fields.
	if c := a.GetCondition(v1alpha1.ConditionSubmittedAsOther); c.Status == "True" {
		t.Errorf("read-back: SubmittedAsOther is True (%s): %s", c.Reason, c.Message)
	}
	o := a.Status.AtProvider
	if o.StartTime == nil || o.HardEnd == nil || o.HardEnd.Sub(o.StartTime.Time) != 5*time.Minute {
		t.Errorf("start %v, hard end %v: want a hard end 5 min after the start", o.StartTime, o.HardEnd)
	}
	// The hard end is the reply's epoch value as a UTC instant; the status
	// keeps the same instant (metav1.Time reads back in the local zone, so the
	// instants are compared, not the zones).
	if j, found, err := w.batch.Observe(ctx, a.Annotations[v1alpha1.AnnotationJobID]); err != nil || !found || j.HardEnd == nil {
		t.Errorf("observe the running job: found %v, err %v", found, err)
	} else {
		if j.HardEnd.Location() != time.UTC || j.StartTime.Location() != time.UTC {
			t.Error("the parsed times are not in UTC")
		}
		if o.HardEnd == nil || !o.HardEnd.Time.Equal(*j.HardEnd) {
			t.Errorf("the status's hard end %v is not the parsed %v", o.HardEnd, j.HardEnd)
		}
	}
	if o.SubmitTime == nil || o.SubmitTime.Before(&metav1.Time{Time: start.Add(-time.Minute)}) || o.SubmitTime.After(time.Now().Add(time.Minute)) {
		t.Errorf("submit time %v is not within this run (clock skew or a wrong zone)", o.SubmitTime)
	}
	job1 := a.Annotations[v1alpha1.AnnotationJobID]
	// The queue wait is in the Provider's status once, with the job's own
	// submit and start times, and the Allocation says it is recorded.
	var prov v1alpha1.Provider
	if err := w.c.Get(ctx, client.ObjectKey{Name: "conformance"}, &prov); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, qw := range prov.Status.QueueWaits {
		if qw.JobID == job1 {
			found++
			if o.SubmitTime == nil || o.StartTime == nil || !qw.SubmitTime.Equal(o.SubmitTime) || !qw.StartTime.Equal(o.StartTime) {
				t.Errorf("the queue wait's times are not the job's")
			}
		}
	}
	if found != 1 || !o.QueueWaitRecorded {
		t.Errorf("the job's queue wait is recorded %d times (queueWaitRecorded %v), want once", found, o.QueueWaitRecorded)
	}

	// 2. Cancel-all on delete, while the job runs: deleting the first
	// Allocation cancels its job; the finalizer goes once nothing with the tag
	// is live.
	if err := w.c.Delete(ctx, w.get(name)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for w.get(name) != nil && time.Now().Before(deadline) {
		w.reconcile(name)
		time.Sleep(3 * time.Second)
	}
	if w.get(name) != nil {
		t.Fatal("the deleted Allocation still holds its finalizer")
	}
	if n := count(w.callWords(), "scancel"); n < 1 {
		t.Error("no scancel call")
	}
	// Then the job reads Ended, CANCELLED, from accounting after it left the
	// queue.
	if j := w.endedInAccounting(job1, 3*time.Minute); len(j.State) == 0 || j.State[0] != "CANCELLED" {
		t.Errorf("the deleted Allocation's job ended as %v, want CANCELLED", j.State)
	}

	// 3. A job whose record was lost: submitted held by the plugin, never
	// recorded, then found by tag, adopted and lifted once.
	lostName := "conformance-lost-record-found-by-tag-observe-end"
	b := w.newAlloc(lostName)
	lostTag := v1alpha1.JobTag(b.Namespace, b.Name, b.UID)
	// The Allocation's directory must exist for the job's output files; the
	// agent binary is already staged under the same digest by step 1.
	cred, err := os.ReadFile(agentapi.AllocationCredentialPath(w.credDir, string(b.UID)))
	if err != nil {
		t.Fatal(err)
	}
	csum := sha256.Sum256(cred)
	if err := w.batch.EnsureFile(ctx, provider.File{Path: agentapi.StagedCredentialPath(lostTag), SHA256: hex.EncodeToString(csum[:]), Mode: 0o600, Content: cred}); err != nil {
		t.Fatal(err)
	}
	id2, outcome, err := w.batch.Submit(ctx, provider.SubmitRequest{Tag: lostTag, Hold: true, Account: w.s.account, Params: *b.Spec.ForProvider.Batch})
	if err != nil || outcome != provider.Accepted {
		t.Fatalf("direct held submit: %s %v", outcome, err)
	}
	w.jobIDs[id2] = true
	w.p.log("lost-record job %s submitted held", id2)
	b = w.until(lostName, "the lost-record job is adopted and runs", 30*time.Minute, func(a *v1alpha1.Allocation) bool {
		return a.Annotations[v1alpha1.AnnotationJobID] == id2 && a.Status.AtProvider.Phase == v1alpha1.AllocationRunning
	})
	words = w.callWords()
	if n := count(words, "sbatch"); n != 2 {
		t.Errorf("%d sbatch calls in total, want 2: the reconciler must not submit for an Allocation whose job exists", n)
	}
	if n := count(words, "scontrol"); n != 2 {
		t.Errorf("%d scontrol release calls in total, want 2", n)
	}

	// 4. The second job runs to its own end and is observed Ended.
	w.until(lostName, "the adopted job ends", 8*time.Minute, func(a *v1alpha1.Allocation) bool {
		return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded || a.Status.AtProvider.Phase == v1alpha1.AllocationFailed
	})
	w.endedInAccounting(id2, 3*time.Minute)

	if w.ssh.Starts() != 1 {
		t.Errorf("%d ssh masters were started; want 1 for the whole run", w.ssh.Starts())
	}
	summary, _ := json.Marshal(map[string]any{"sbatch": w.cap.sbatchD, "calls": w.cap.n, "masters": w.ssh.Starts()})
	w.p.save("summary.json", summary)
}

// endedInAccounting polls a job by ID until the per-ID queue read no longer
// lists it and accounting reports it ended.
func (w *world) endedInAccounting(id string, timeout time.Duration) provider.Job {
	w.t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		_, listed, qerr := w.batch.QueueJob(ctx, id)
		j, found, err := w.batch.Observe(ctx, id)
		w.p.log("job %s listed %v (%v) found %v source %s phase %s end %v err %v", id, listed, qerr, found, j.Source, j.Phase, j.EndTime, err)
		if qerr == nil && !listed && err == nil && found && j.Source == provider.SourceAccounting && j.EndTime != nil {
			if j.EndTime.Location() != time.UTC {
				w.t.Error("the end time is not in UTC")
			}
			return j
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("a job of this run was not seen ended in accounting after leaving the queue within %s", timeout)
		}
		time.Sleep(5 * time.Second)
	}
}

// TestProbe is the read-only part: one master, the login-node paths, one
// queue listing parsed with the profile's data_parser, and the dial-home key
// check. It submits nothing. Run it first on a new platform.
func TestProbe(t *testing.T) {
	if os.Getenv("HPCBURST_CONFORMANCE_PROBE") != "1" {
		t.Skip("set HPCBURST_CONFORMANCE_PROBE=1 and the HPCBURST_CONF_* settings to probe a real platform")
	}
	t.Setenv("HPCBURST_CONFORMANCE", "1")
	w := newWorld(t)
	ctx := context.Background()
	if err := w.ctl.SyncStatus(ctx); err != nil {
		t.Fatal(err)
	}
	if w.batch == nil {
		t.Fatal("the platform was not made")
	}
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.p.log("paths %+v", paths)
	if err := w.batch.Poll(ctx); err != nil {
		t.Fatalf("queue listing: %v", err)
	}
	if _, err := w.batch.DialHomeKeysPresent(ctx); err != nil {
		t.Fatalf("dial-home key check: %v", err)
	}
	h := w.batch.Health()
	if h.LastListing.IsZero() || h.DialectMismatch != "" || h.PathUnresolved != nil {
		t.Errorf("health %+v", h)
	}
	if w.ssh.Starts() != 1 {
		t.Errorf("%d masters started, want 1", w.ssh.Starts())
	}
	var prov v1alpha1.Provider
	if err := w.c.Get(ctx, client.ObjectKey{Name: "conformance"}, &prov); err != nil {
		t.Fatal(err)
	}
	for _, c := range prov.Status.Conditions {
		t.Logf("Provider condition %s=%s (%s)", c.Type, c.Status, c.Reason)
	}
}

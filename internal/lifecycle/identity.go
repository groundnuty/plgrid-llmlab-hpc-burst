package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// CheckpointSource reads what a checkpoint gives the deployment ID (HBEP-3,
// Current IDs, the file list and rollout): its per-file list and, when the
// serving spec names no chat template, the checkpoint's template sources.
type CheckpointSource interface {
	// Files returns the per-file list at the full commit id.
	Files(ctx context.Context, cp v1alpha1.CheckpointSpec) ([]deploymentid.File, error)
	// TemplateSources returns the checkpoint's chat-template files (HBEP-3's
	// lookup chain), read at the full commit id and checked against files.
	TemplateSources(ctx context.Context, cp v1alpha1.CheckpointSpec, files []deploymentid.File) (deploymentid.TemplateSources, error)
}

// cachedSource caches a CheckpointSource's successful answers by repository and
// revision: a revision is a full commit id, so its files never change, and
// each Files call downloads every non-LFS file. A failure is kept too, and the
// inner source is not asked again before it may be, decided by
// hubfiles.Retryable alone (HBEP-3 as revised after the #74 review, 31 Oct
// 2026):
//   - a transient failure (a transport error, a 429 or a 5xx) backs off from
//     30 s, doubling to 10 min, never sooner than the Hub's Retry-After;
//   - a permanent one (a 401, 403 or 404, a blob or size mismatch, a refused
//     listing, a cross-host Link, too many pages, a file too large, a bad
//     token file) is not tried again until the spec names another checkpoint
//     or token file (another key), the token file's content changes, or the
//     periodic resync, permanentRecheck, comes.
type cachedSource struct {
	inner CheckpointSource
	clock Clock
	mu    sync.Mutex
	files map[string][]deploymentid.File
	tmpl  map[string]deploymentid.TemplateSources
	fails map[string]*sourceFailure
}

// sourceFailure is a kept failure of one checkpoint read.
type sourceFailure struct {
	err      error
	attempts int
	// permanent failures wait for another token; retryAt is unused for them.
	permanent bool
	token     string
	retryAt   time.Time
}

const (
	sourceBackoffMin = 30 * time.Second
	sourceBackoffMax = 10 * time.Minute
	// permanentRecheck is the periodic resync of a permanent failure:
	// controller-runtime's default SyncPeriod, the cadence at which a
	// controller re-reads everything anyway.
	permanentRecheck = 10 * time.Hour
)

// NewCachedSource wraps a CheckpointSource with a cache by (repository,
// revision) and the failure policy above, timed by clock.
func NewCachedSource(inner CheckpointSource, clock Clock) CheckpointSource {
	if clock == nil {
		clock = SystemClock{}
	}
	return &cachedSource{inner: inner, clock: clock, files: map[string][]deploymentid.File{},
		tmpl: map[string]deploymentid.TemplateSources{}, fails: map[string]*sourceFailure{}}
}

func cacheKey(cp v1alpha1.CheckpointSpec) string { return cp.Repository + "@" + cp.Revision }

// failKey keys a failure by the checkpoint and the token file it was read
// with, so a spec that names another token file reads again.
func failKey(kind string, cp v1alpha1.CheckpointSpec) string {
	return kind + "/" + cacheKey(cp) + "#" + tokenFileOf(cp)
}

// tokenFingerprint identifies the token file's content, so a permanent failure
// is read again once the operator replaces the token. The content is hashed in
// memory only, never kept or logged; a file that cannot be read has its own
// fingerprint.
func tokenFingerprint(cp v1alpha1.CheckpointSpec) string {
	path := tokenFileOf(cp)
	if path == "" {
		return "none"
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "unreadable"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// backoffError is a kept failure with the time it is tried again.
type backoffError struct {
	err     error
	retryAt time.Time
}

func (e *backoffError) Error() string { return e.err.Error() }
func (e *backoffError) Unwrap() error { return e.err }

// retryAtOf returns when a failed checkpoint read is tried again, if err says.
func retryAtOf(err error) (time.Time, bool) {
	var b *backoffError
	if errors.As(err, &b) {
		return b.retryAt, true
	}
	return time.Time{}, false
}

// kept returns the kept failure of key while it stands: until its retry time,
// and a permanent one also only while the token's fingerprint is the same.
func (c *cachedSource) kept(key, token string) error {
	f, ok := c.fails[key]
	switch {
	case !ok, !c.clock.Now().Before(f.retryAt):
		return nil
	case f.permanent && f.token != token:
		return nil
	}
	return &backoffError{err: f.err, retryAt: f.retryAt}
}

// fail keeps a failure of key and returns it with its retry time.
func (c *cachedSource) fail(key, token string, err error) error {
	f, ok := c.fails[key]
	if !ok {
		f = &sourceFailure{}
		c.fails[key] = f
	}
	f.err, f.token = err, token
	f.attempts++
	if !retryableHub(err) {
		f.permanent, f.retryAt = true, c.clock.Now().Add(permanentRecheck)
		return &backoffError{err: err, retryAt: f.retryAt}
	}
	f.permanent = false
	delay := sourceBackoffMin << min(f.attempts-1, 10)
	if delay > sourceBackoffMax {
		delay = sourceBackoffMax
	}
	var he *hubfiles.HTTPError
	if errors.As(err, &he) && he.RetryAfter > delay {
		delay = he.RetryAfter
	}
	f.retryAt = c.clock.Now().Add(delay)
	return &backoffError{err: err, retryAt: f.retryAt}
}

func (c *cachedSource) Files(ctx context.Context, cp v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	key, fkey, token := cacheKey(cp), failKey("files", cp), tokenFingerprint(cp)
	c.mu.Lock()
	f, ok := c.files[key]
	kept := c.kept(fkey, token)
	c.mu.Unlock()
	if ok {
		return append([]deploymentid.File(nil), f...), nil
	}
	if kept != nil {
		return nil, kept
	}
	f, err := c.inner.Files(ctx, cp)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		return nil, c.fail(fkey, token, err)
	}
	delete(c.fails, fkey)
	c.files[key] = append([]deploymentid.File(nil), f...)
	return f, nil
}

func (c *cachedSource) TemplateSources(ctx context.Context, cp v1alpha1.CheckpointSpec, files []deploymentid.File) (deploymentid.TemplateSources, error) {
	key, fkey, token := cacheKey(cp), failKey("tmpl", cp), tokenFingerprint(cp)
	c.mu.Lock()
	t, ok := c.tmpl[key]
	kept := c.kept(fkey, token)
	c.mu.Unlock()
	if ok {
		return t, nil
	}
	if kept != nil {
		return deploymentid.TemplateSources{}, kept
	}
	t, err := c.inner.TemplateSources(ctx, cp, files)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		return deploymentid.TemplateSources{}, c.fail(fkey, token, err)
	}
	delete(c.fails, fkey)
	c.tmpl[key] = t
	return t, nil
}

// forgetFailures drops the kept failures of a checkpoint, so the next read asks
// the inner source: the spec changed (HBEP-3).
func (c *cachedSource) forgetFailures(cp v1alpha1.CheckpointSpec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.fails, failKey("files", cp))
	delete(c.fails, failKey("tmpl", cp))
}

// hubCause names the cause of a checkpoint read failure for the condition: the
// Hub's status (401, 403, 404, 5xx...) when it answered, a blob id mismatch or
// an invalid listing, else the error.
func hubCause(err error) string {
	var he *hubfiles.HTTPError
	switch {
	case errors.As(err, &he):
		return fmt.Sprintf("Hub %d: %v", he.Status, err)
	case errors.Is(err, hubfiles.ErrBlobMismatch):
		return "blob id mismatch: " + err.Error()
	case errors.Is(err, hubfiles.ErrInvalidListing):
		return "invalid listing: " + err.Error()
	}
	return err.Error()
}

// identityData is an identity record's data: the canonical v1 object and its
// full hash (HBEP-11, Ownership, records and deletion).
type identityData struct {
	Scheme    string          `json:"scheme"`
	SHA256    string          `json:"sha256"`
	Canonical json.RawMessage `json:"canonical"`
}

// canonicalFiles is the part of the canonical object that verification reads.
type canonicalFiles struct {
	Files []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

// classID is a class's current ID, or why it has none.
type classID struct {
	class  v1alpha1.AcceleratorClass
	id     v1alpha1.FullDeploymentID
	record string
	// problem, when set, says why the class has no current ID: no Allocation is
	// created for it (HBEP-7, Reconcile loop, step 2).
	problem string
	// collision and collisionRV name the identity record whose display form
	// the ID collides with, and its resourceVersion.
	collision, collisionRV string
	// retryAt, when set, is when the checkpoint read is tried again.
	retryAt time.Time
	// transient marks a failure worth a retry with backoff (hubfiles.Retryable):
	// it sets no reason, only a later pass (HBEP-3).
	transient bool
	// blocked, when set, says why provisioning in the class is stopped: a
	// verification of its current ID failed on a settings contradiction or a
	// shape that does not fit (HBEP-7, Instance start).
	blocked string
}

// templateSHA resolves the chat template that scheme v1 hashes (HBEP-3).
func (r *Reconciler) templateSHA(ctx context.Context, s *v1alpha1.ServingSpec, files []deploymentid.File) (string, error) {
	src := deploymentid.TemplateSources{Spec: s.ChatTemplate}
	if s.ChatTemplate == nil {
		ts, err := r.Checkpoints.TemplateSources(ctx, s.Checkpoint, files)
		if err != nil {
			return "", err
		}
		src = ts
		src.Spec = nil
	}
	text, err := deploymentid.ResolveChatTemplate(src)
	if err != nil {
		return "", err
	}
	return deploymentid.ChatTemplateSHA256(text)
}

// currentIDs computes each class's current ID from the serving spec and creates
// its identity record if absent (HBEP-3, Current IDs; HBEP-11). A record whose
// stored hash differs from the ID is a display-form collision: the class gets
// no ID, and an event fires.
//
// A collision is not evaluated again while the spec's generation and the
// colliding record's resourceVersion stay the same (HBEP-3: a collision is
// re-evaluated only when the spec or the identity records change; a token
// change or a resync never re-raises it). A permanent Hub failure is read again
// when the spec's generation changes, besides the token change and the resync
// the CheckpointSource applies. An API error is returned, never shown as
// IdentityUnresolved.
func (r *Reconciler) currentIDs(ctx context.Context, md *v1alpha1.ModelDeployment, cls []v1alpha1.AcceleratorClass) ([]classID, error) {
	if memo, ok := r.memory().collision(md.UID, md.Generation); ok {
		var rec appsv1.ControllerRevision
		err := r.Reader.Get(ctx, client.ObjectKey{Namespace: md.Namespace, Name: memo.record}, &rec)
		switch {
		case err == nil && rec.ResourceVersion == memo.rv:
			return memo.result, nil
		case err != nil && !apierrors.IsNotFound(err):
			return nil, fmt.Errorf("read identity record %s: %w", memo.record, err)
		}
	}
	if r.memory().specChanged(md.UID, md.Generation) {
		if f, ok := r.Checkpoints.(interface {
			forgetFailures(v1alpha1.CheckpointSpec)
		}); ok {
			f.forgetFailures(md.Spec.Source.Served.Checkpoint)
		}
	}
	out, err := r.computeIDs(ctx, md, cls)
	if err != nil {
		return nil, err
	}
	r.memory().forgetCollision(md.UID)
	for _, c := range out {
		if c.collision != "" {
			r.memory().rememberCollision(md.UID, collisionMemo{gen: md.Generation, record: c.collision, rv: c.collisionRV, result: out})
		}
	}
	return out, nil
}

// computeIDs computes the classes' current IDs and their identity records.
func (r *Reconciler) computeIDs(ctx context.Context, md *v1alpha1.ModelDeployment, cls []v1alpha1.AcceleratorClass) ([]classID, error) {
	s := md.Spec.Source.Served
	out := make([]classID, 0, len(cls))
	fail := func(err error) ([]classID, error) {
		at, _ := retryAtOf(err)
		for _, c := range cls {
			out = append(out, classID{class: c, problem: "checkpoint " + s.Checkpoint.Repository + ": " + hubCause(err), retryAt: at, transient: retryableHub(err)})
		}
		return out, nil
	}
	files, err := r.Checkpoints.Files(ctx, s.Checkpoint)
	if err != nil {
		return fail(err)
	}
	tmpl, err := r.templateSHA(ctx, s, files)
	if err != nil {
		return fail(err)
	}
	spec := deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: s.Checkpoint.Repository, Revision: s.Checkpoint.Revision},
		Engine:             deploymentid.Engine{Name: s.Engine.Name, Version: s.Engine.Version, ImageDigest: s.Engine.ImageDigest},
		ChatTemplateSHA256: tmpl,
	}
	for _, f := range s.Flags {
		spec.Flags = append(spec.Flags, deploymentid.Flag{Name: f.Name, Value: f.Value})
	}
	for _, e := range s.Env {
		spec.Env = append(spec.Env, deploymentid.EnvVar{Name: e.Name, Value: e.Value})
	}
	for _, c := range cls {
		cid := classID{class: c}
		id, err := deploymentid.Compute(spec, deploymentid.Accelerator{GPUModel: c.GPUModel, CPUArch: c.CPUArch}, files)
		if err != nil {
			cid.problem = "deployment ID: " + err.Error()
			out = append(out, cid)
			continue
		}
		name, _, err := r.ensureIdentityRecord(ctx, md, id)
		var col *collisionError
		switch {
		case errors.As(err, &col):
			cid.problem, cid.collision, cid.collisionRV = err.Error(), col.record, col.rv
			out = append(out, cid)
			continue
		case err != nil:
			return nil, err
		}
		cid.id, cid.record = v1alpha1.FullDeploymentID(id.SHA256), name
		out = append(out, cid)
	}
	return out, nil
}

// collisionError is a display-form collision with an identity record, whose
// resourceVersion keys the re-evaluation.
type collisionError struct {
	record, rv, msg string
}

func (e *collisionError) Error() string { return e.msg }
func (e *collisionError) Unwrap() error { return errCollision }

// errCollision is a display-form collision of two deployment IDs (HBEP-3).
var errCollision = errors.New("display-form collision")

// ensureIdentityRecord creates the identity record of id if absent, and reads
// an existing one, never rewriting it.
func (r *Reconciler) ensureIdentityRecord(ctx context.Context, md *v1alpha1.ModelDeployment, id deploymentid.ID) (string, string, error) {
	data, err := json.Marshal(identityData{Scheme: deploymentid.Scheme, SHA256: id.SHA256, Canonical: json.RawMessage(id.Canonical)})
	if err != nil {
		return "", "", err
	}
	rec, err := v1alpha1.NewIdentityRecord(md.Namespace, md.Name, id.SHA256, data)
	if err != nil {
		return "", "", err
	}
	var got appsv1.ControllerRevision
	err = r.Reader.Get(ctx, client.ObjectKeyFromObject(rec), &got)
	switch {
	case apierrors.IsNotFound(err):
		if cerr := r.Client.Create(ctx, rec, client.FieldOwner(v1alpha1.FieldManagerLifecycle)); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			return "", "", fmt.Errorf("create identity record %s: %w", rec.Name, cerr)
		} else if cerr == nil {
			return rec.Name, rec.ResourceVersion, nil
		}
		if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(rec), &got); err != nil {
			return "", "", fmt.Errorf("read identity record %s: %w", rec.Name, err)
		}
	case err != nil:
		return "", "", fmt.Errorf("read identity record %s: %w", rec.Name, err)
	}
	var stored identityData
	if err := json.Unmarshal(got.Data.Raw, &stored); err != nil || stored.SHA256 != id.SHA256 {
		// A collision never loops: the event fires once per spec generation
		// and record version, and nothing is retried until one of them
		// changes (`@hpc-burst-science-agent`, 31 Oct 2026).
		if r.memory().once(fmt.Sprintf("collision/%s/%d/%s/%s", md.UID, md.Generation, rec.Name, got.ResourceVersion)) {
			r.Recorder.Event(md, nil, eventDegraded, v1alpha1.ReasonIdentityUnresolved,
				fmt.Sprintf("identity record %s holds another deployment ID than %s: a display-form collision; the ID is refused (HBEP-3)", rec.Name, id.SHA256))
		}
		return "", "", &collisionError{record: rec.Name, rv: got.ResourceVersion,
			msg: fmt.Sprintf("%v with record %s: it holds another deployment ID than %s", errCollision, rec.Name, id.SHA256)}
	}
	return rec.Name, got.ResourceVersion, nil
}

// recordFiles reads the per-file list from an identity record.
func recordFiles(rec *appsv1.ControllerRevision) ([]deploymentid.File, error) {
	var d identityData
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		return nil, fmt.Errorf("identity record %s: %w", rec.Name, err)
	}
	var cf canonicalFiles
	if err := json.Unmarshal(d.Canonical, &cf); err != nil {
		return nil, fmt.Errorf("identity record %s: canonical object: %w", rec.Name, err)
	}
	out := make([]deploymentid.File, len(cf.Files))
	for i, f := range cf.Files {
		out[i] = deploymentid.File{Path: f.Path, SHA256: f.SHA256}
	}
	return out, nil
}

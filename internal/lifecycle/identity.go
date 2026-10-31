package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// inner source is not asked again before its retry time: a transient failure
// (hubfiles.Retryable: a transport error, a 429 or a 5xx) backs off from 30 s,
// doubling to 10 min, and never sooner than the Hub's Retry-After; a permanent
// one (a 401, a 404) is asked again after 10 min, so a fixed token or a
// published revision is picked up without a restart.
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
	retryAt  time.Time
}

const (
	sourceBackoffMin = 30 * time.Second
	sourceBackoffMax = 10 * time.Minute
)

// NewCachedSource wraps a CheckpointSource with a cache by (repository,
// revision) and a backoff on failures, timed by clock.
func NewCachedSource(inner CheckpointSource, clock Clock) CheckpointSource {
	if clock == nil {
		clock = SystemClock{}
	}
	return &cachedSource{inner: inner, clock: clock, files: map[string][]deploymentid.File{},
		tmpl: map[string]deploymentid.TemplateSources{}, fails: map[string]*sourceFailure{}}
}

func cacheKey(cp v1alpha1.CheckpointSpec) string { return cp.Repository + "@" + cp.Revision }

// backoffError is a kept failure returned before its retry time.
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

// kept returns the kept failure of key while its retry time has not come.
func (c *cachedSource) kept(key string) error {
	f, ok := c.fails[key]
	if !ok || !c.clock.Now().Before(f.retryAt) {
		return nil
	}
	return &backoffError{err: f.err, retryAt: f.retryAt}
}

// fail keeps a failure of key and returns it with its retry time.
func (c *cachedSource) fail(key string, err error) error {
	f, ok := c.fails[key]
	if !ok {
		f = &sourceFailure{}
		c.fails[key] = f
	}
	f.err = err
	f.attempts++
	delay := sourceBackoffMax
	if retryableHub(err) {
		delay = sourceBackoffMin << min(f.attempts-1, 10)
		if delay > sourceBackoffMax {
			delay = sourceBackoffMax
		}
		var he *hubfiles.HTTPError
		if errors.As(err, &he) && he.RetryAfter > delay {
			delay = he.RetryAfter
		}
	}
	f.retryAt = c.clock.Now().Add(delay)
	return &backoffError{err: err, retryAt: f.retryAt}
}

func (c *cachedSource) Files(ctx context.Context, cp v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	key := cacheKey(cp)
	c.mu.Lock()
	f, ok := c.files[key]
	kept := c.kept("files/" + key)
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
		return nil, c.fail("files/"+key, err)
	}
	delete(c.fails, "files/"+key)
	c.files[key] = append([]deploymentid.File(nil), f...)
	return f, nil
}

func (c *cachedSource) TemplateSources(ctx context.Context, cp v1alpha1.CheckpointSpec, files []deploymentid.File) (deploymentid.TemplateSources, error) {
	key := cacheKey(cp)
	c.mu.Lock()
	t, ok := c.tmpl[key]
	kept := c.kept("tmpl/" + key)
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
		return deploymentid.TemplateSources{}, c.fail("tmpl/"+key, err)
	}
	delete(c.fails, "tmpl/"+key)
	c.tmpl[key] = t
	return t, nil
}

// hubCause names the cause of a checkpoint read failure for the condition: the
// Hub's status (401, 404, 5xx...) when it answered, else the error.
func hubCause(err error) string {
	var he *hubfiles.HTTPError
	if errors.As(err, &he) {
		return fmt.Sprintf("Hub %d: %v", he.Status, err)
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
	// retryAt, when set, is when the checkpoint read is tried again.
	retryAt time.Time
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
func (r *Reconciler) currentIDs(ctx context.Context, md *v1alpha1.ModelDeployment, cls []v1alpha1.AcceleratorClass) []classID {
	s := md.Spec.Source.Served
	out := make([]classID, 0, len(cls))
	fail := func(err error) []classID {
		at, _ := retryAtOf(err)
		for _, c := range cls {
			out = append(out, classID{class: c, problem: "checkpoint " + s.Checkpoint.Repository + ": " + hubCause(err), retryAt: at})
		}
		return out
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
		name, err := r.ensureIdentityRecord(ctx, md, id)
		if err != nil {
			cid.problem = err.Error()
			out = append(out, cid)
			continue
		}
		cid.id, cid.record = v1alpha1.FullDeploymentID(id.SHA256), name
		out = append(out, cid)
	}
	return out
}

// errCollision is a display-form collision of two deployment IDs (HBEP-3).
var errCollision = errors.New("display-form collision")

// ensureIdentityRecord creates the identity record of id if absent, and reads
// an existing one, never rewriting it.
func (r *Reconciler) ensureIdentityRecord(ctx context.Context, md *v1alpha1.ModelDeployment, id deploymentid.ID) (string, error) {
	data, err := json.Marshal(identityData{Scheme: deploymentid.Scheme, SHA256: id.SHA256, Canonical: json.RawMessage(id.Canonical)})
	if err != nil {
		return "", err
	}
	rec, err := v1alpha1.NewIdentityRecord(md.Namespace, md.Name, id.SHA256, data)
	if err != nil {
		return "", err
	}
	var got appsv1.ControllerRevision
	err = r.Reader.Get(ctx, client.ObjectKeyFromObject(rec), &got)
	switch {
	case apierrors.IsNotFound(err):
		if cerr := r.Client.Create(ctx, rec, client.FieldOwner(v1alpha1.FieldManagerLifecycle)); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			return "", fmt.Errorf("create identity record %s: %w", rec.Name, cerr)
		} else if cerr == nil {
			return rec.Name, nil
		}
		if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(rec), &got); err != nil {
			return "", fmt.Errorf("read identity record %s: %w", rec.Name, err)
		}
	case err != nil:
		return "", fmt.Errorf("read identity record %s: %w", rec.Name, err)
	}
	var stored identityData
	if err := json.Unmarshal(got.Data.Raw, &stored); err != nil || stored.SHA256 != id.SHA256 {
		// A collision never loops: the event fires once per spec generation
		// and record version, and nothing is retried until one of them
		// changes (HBEP owner, 31 Oct 2026).
		if r.memory().once(fmt.Sprintf("collision/%s/%d/%s/%s", md.UID, md.Generation, rec.Name, got.ResourceVersion)) {
			r.Recorder.Event(md, true, eventDegraded, v1alpha1.ReasonIdentityUnresolved,
				fmt.Sprintf("identity record %s holds another deployment ID than %s: a display-form collision; the ID is refused (HBEP-3)", rec.Name, id.SHA256))
		}
		return "", fmt.Errorf("%w with record %s: it holds another deployment ID than %s", errCollision, rec.Name, id.SHA256)
	}
	return rec.Name, nil
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

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
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

// ErrTemplateSourcesUnavailable is returned by a CheckpointSource that cannot
// read file contents. The controller then refuses a serving spec that names no
// chat template, rather than hash a template it has not read.
var ErrTemplateSourcesUnavailable = errors.New("the checkpoint's chat-template files cannot be read yet; give spec.source.served.chatTemplate")

// cachedSource caches a CheckpointSource's successful answers by repository and
// revision: a revision is a full commit id, so its files never change, and
// each Files call downloads every non-LFS file.
type cachedSource struct {
	inner CheckpointSource
	mu    sync.Mutex
	files map[string][]deploymentid.File
	tmpl  map[string]deploymentid.TemplateSources
}

// NewCachedSource wraps a CheckpointSource with a cache by (repository,
// revision). Errors are not cached.
func NewCachedSource(inner CheckpointSource) CheckpointSource {
	return &cachedSource{inner: inner, files: map[string][]deploymentid.File{}, tmpl: map[string]deploymentid.TemplateSources{}}
}

func cacheKey(cp v1alpha1.CheckpointSpec) string { return cp.Repository + "@" + cp.Revision }

func (c *cachedSource) Files(ctx context.Context, cp v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	c.mu.Lock()
	f, ok := c.files[cacheKey(cp)]
	c.mu.Unlock()
	if ok {
		return append([]deploymentid.File(nil), f...), nil
	}
	f, err := c.inner.Files(ctx, cp)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.files[cacheKey(cp)] = append([]deploymentid.File(nil), f...)
	c.mu.Unlock()
	return f, nil
}

func (c *cachedSource) TemplateSources(ctx context.Context, cp v1alpha1.CheckpointSpec, files []deploymentid.File) (deploymentid.TemplateSources, error) {
	c.mu.Lock()
	t, ok := c.tmpl[cacheKey(cp)]
	c.mu.Unlock()
	if ok {
		return t, nil
	}
	t, err := c.inner.TemplateSources(ctx, cp, files)
	if err != nil {
		return deploymentid.TemplateSources{}, err
	}
	c.mu.Lock()
	c.tmpl[cacheKey(cp)] = t
	c.mu.Unlock()
	return t, nil
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
	// retryable marks a problem worth an early retry (a Hub that is busy).
	retryable bool
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
		for _, c := range cls {
			out = append(out, classID{class: c, problem: "checkpoint " + s.Checkpoint.Repository + ": " + err.Error(), retryable: retryableHub(err)})
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
		r.Recorder.Event(md, true, eventDegraded, reasonIdentityUnresolved,
			fmt.Sprintf("identity record %s holds another deployment ID than %s: a display-form collision; the ID is refused (HBEP-3)", rec.Name, id.SHA256))
		return "", fmt.Errorf("%w: identity record %s holds another deployment ID than %s", errCollision, rec.Name, id.SHA256)
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

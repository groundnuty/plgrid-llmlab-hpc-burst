package providerctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The Allocation's record (HBEP-11, The Allocation's record; HBEP-7, open
// question 11): the submission attempt, the job ID and the hold-lifted marker
// are annotations, each written with a resourceVersion precondition. A
// conflict means re-read, never overwrite. Annotations rather than status,
// because they survive a restore without status (S4).

// record is the parsed record of one Allocation. readBack is the job ID named
// by v1alpha1.AnnotationReadBack (HBEP-4, The Slurm dialect, Read-back: "on the
// first observe after a job is established"). It keys the read-back on the
// read-back itself, not on the stored status's jobID, which a failed or empty
// first observe also writes (review of PR 76: Opus R1, Astra B6). A mismatch
// writes no marker: the condition SubmittedAsOther True stops later read-backs,
// and if its status write is lost, the next observe compares again. It is an
// annotation, like the rest of the record, so it survives a restore without
// status and an administrator's later change is never read back.
type record struct {
	sub      *v1alpha1.SubmissionRecord
	jobID    string
	lifted   string
	readBack string
}

func unresolved(r *v1alpha1.SubmissionRecord) bool {
	return r != nil && (r.State == v1alpha1.SubmissionInFlight || r.State == v1alpha1.SubmissionUnknown)
}

// readRecord parses the annotations. A malformed record is an error, never "no
// record": guessing could submit twice.
func readRecord(a *v1alpha1.Allocation) (record, error) {
	var r record
	an := a.GetAnnotations()
	if s := an[v1alpha1.AnnotationSubmission]; s != "" {
		var sub v1alpha1.SubmissionRecord
		if err := json.Unmarshal([]byte(s), &sub); err != nil {
			return r, fmt.Errorf("annotation %s is not a submission record: %w", v1alpha1.AnnotationSubmission, err)
		}
		if sub.Attempt < 1 {
			return r, fmt.Errorf("annotation %s names attempt %d; attempts count from 1", v1alpha1.AnnotationSubmission, sub.Attempt)
		}
		r.sub = &sub
	}
	r.jobID = an[v1alpha1.AnnotationJobID]
	r.lifted = an[v1alpha1.AnnotationHoldLifted]
	r.readBack = an[v1alpha1.AnnotationReadBack]
	if r.jobID != "" && !isJobID(r.jobID) {
		return r, fmt.Errorf("annotation %s is not a job ID", v1alpha1.AnnotationJobID)
	}
	return r, nil
}

func isJobID(s string) bool {
	if s == "" || len(s) > 20 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func recordValue(r v1alpha1.SubmissionRecord) string {
	b, err := json.Marshal(r)
	if err != nil {
		panic("submission record: " + err.Error()) // strings, numbers and a time cannot fail
	}
	return string(b)
}

func newToken() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// write sets annotations ("" deletes one) on a copy of a and updates it with
// a's resourceVersion as the precondition. On success the new metadata is
// copied into a; a's status in memory is kept, because the API answers with
// the stored status. A conflict is returned as it is.
func write(ctx context.Context, kube client.Client, a *v1alpha1.Allocation, set map[string]string) error {
	cp := a.DeepCopy()
	an := map[string]string{}
	for k, v := range cp.GetAnnotations() {
		an[k] = v
	}
	for k, v := range set {
		if v == "" {
			delete(an, k)
		} else {
			an[k] = v
		}
	}
	cp.SetAnnotations(an)
	if err := kube.Update(ctx, cp); err != nil {
		return err
	}
	cp.ObjectMeta.DeepCopyInto(&a.ObjectMeta)
	return nil
}

// refresh replaces a's metadata with the API server's, not the cache's.
func refresh(ctx context.Context, reader client.Reader, a *v1alpha1.Allocation) error {
	fresh := &v1alpha1.Allocation{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(a), fresh); err != nil {
		return err
	}
	fresh.ObjectMeta.DeepCopyInto(&a.ObjectMeta)
	return nil
}

// writeOwn writes set only while the current attempt is still the one with
// token. On a conflict it re-reads from the API server and tries again; an
// attempt that has been replaced is left alone.
func writeOwn(ctx context.Context, kube client.Client, reader client.Reader, a *v1alpha1.Allocation, token string, set func(record) map[string]string) (bool, error) {
	for i := 0; i < 5; i++ {
		rec, err := readRecord(a)
		if err != nil {
			return false, err
		}
		if rec.sub == nil || rec.sub.Token != token {
			return false, nil
		}
		s := set(rec)
		if len(s) == 0 {
			return false, nil
		}
		err = write(ctx, kube, a, s)
		if err == nil {
			return true, nil
		}
		if !kerrors.IsConflict(err) {
			return false, err
		}
		if err := refresh(ctx, reader, a); err != nil {
			return false, err
		}
	}
	return false, errors.New("submission record: still conflicting after 5 re-reads")
}

// criticalAnnotationUpdater replaces crossplane-runtime's. v2.4.3's
// RetryingCriticalAnnotationUpdater re-applies every annotation held in memory
// after a conflict, so a stale copy could roll the submission record back
// (S4). This one re-applies only crossplane's own keys, and re-reads from the
// API server, not the cache. Ported from spike S4 (internal/controller).
type criticalAnnotationUpdater struct {
	kube   client.Client
	reader client.Reader
}

var crossplaneKeys = []string{
	meta.AnnotationKeyExternalName, meta.AnnotationKeyExternalCreatePending,
	meta.AnnotationKeyExternalCreateSucceeded, meta.AnnotationKeyExternalCreateFailed,
}

var _ managed.CriticalAnnotationUpdater = criticalAnnotationUpdater{}

// UpdateCriticalAnnotations implements managed.CriticalAnnotationUpdater.
//
// The update answers with the stored object, status included, which would
// discard what this reconcile has put in the Allocation's status so far; the
// status is kept, so that the display a Create sets from its outcome (showRejected,
// showAttempt) reaches the status write that crossplane-runtime makes next,
// and a stale display, a full slot's reason after another refusal, does not.
func (u criticalAnnotationUpdater) UpdateCriticalAnnotations(ctx context.Context, o client.Object) error {
	var status *v1alpha1.AllocationStatus
	if a, ok := o.(*v1alpha1.Allocation); ok {
		status = a.Status.DeepCopy()
	}
	crit := map[string]string{}
	for _, k := range crossplaneKeys {
		if v, ok := o.GetAnnotations()[k]; ok {
			crit[k] = v
		}
	}
	err := retry.OnError(retry.DefaultRetry, func(err error) bool { return !errors.Is(err, context.Canceled) }, func() error {
		err := u.kube.Update(ctx, o)
		if kerrors.IsConflict(err) {
			if getErr := u.reader.Get(ctx, client.ObjectKeyFromObject(o), o); getErr != nil {
				return getErr
			}
			meta.AddAnnotations(o, crit)
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("cannot update critical annotations: %w", err)
	}
	if status != nil {
		o.(*v1alpha1.Allocation).Status = *status
	}
	return nil
}

// tagInitializer sets the external name to the Allocation's job tag before the
// first Observe (HBEP-3; HBEP-7, open question 11: "The tag is the
// Allocation's external name, set before any submission"). It replaces
// crossplane's NameAsExternalName and runs before the create-incomplete check,
// so the tag is persisted before any sbatch. Ported from spike S4.
type tagInitializer struct{ kube client.Client }

// Initialize implements managed.Initializer.
func (t tagInitializer) Initialize(ctx context.Context, mg resource.Managed) error {
	if meta.GetExternalName(mg) != "" {
		return nil
	}
	meta.SetExternalName(mg, v1alpha1.JobTag(mg.GetNamespace(), mg.GetName(), mg.GetUID()))
	return t.kube.Update(ctx, mg)
}

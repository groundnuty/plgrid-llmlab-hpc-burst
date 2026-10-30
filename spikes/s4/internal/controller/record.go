package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// The submission record lives in annotations, not status: annotations
// survive a backup and restore without status, which is crossplane's own
// reason for keeping its create annotations there. Every write is a
// compare-and-swap on the resourceVersion.
const (
	// AnnoSubmission holds the latest submission attempt as JSON (Attempt).
	AnnoSubmission = v1alpha1.Group + "/submission"
	// AnnoJobID is the established Slurm job ID: written once, never
	// overwritten or removed. Only this job is ever released.
	AnnoJobID = v1alpha1.Group + "/job-id"
	// AnnoReleased is the job ID after its one release (or once it was seen
	// released). A job is never released twice.
	AnnoReleased = v1alpha1.Group + "/released"
	// AnnoAbandon is the operator's decision on an unresolved attempt: set it
	// to the attempt number after checking the cluster, and the attempt is
	// abandoned, which allows a new one.
	AnnoAbandon = v1alpha1.Group + "/abandon-attempt"
)

// Attempt states.
const (
	AttemptInFlight  = "InFlight"  // claimed; sbatch may be running
	AttemptUnknown   = "Unknown"   // sbatch outcome ambiguous; resolve by tag lookup
	AttemptRejected  = "Rejected"  // definite rejection; nothing was created
	AttemptDone      = "Done"      // a job ID was recorded (established, or an extra)
	AttemptAbandoned = "Abandoned" // an operator gave up on an unresolved attempt
)

// Attempt is one submission attempt, identified by its number and a random
// token. Ordering never compares timestamps; At only measures age.
type Attempt struct {
	N      int       `json:"n"`
	Token  string    `json:"token"`
	State  string    `json:"state"`
	By     string    `json:"by"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason,omitempty"`
	JobID  int64     `json:"jobId,omitempty"`
}

func (t *Attempt) unresolved() bool {
	return t != nil && (t.State == AttemptInFlight || t.State == AttemptUnknown)
}

func (t Attempt) value() string {
	b, _ := json.Marshal(t)
	return string(b)
}

type record struct {
	attempt  *Attempt
	jobID    int64
	released int64
}

func (r record) nextN() int {
	if r.attempt == nil {
		return 1
	}
	return r.attempt.N + 1
}

// readRecord parses the submission annotations. A malformed value is an
// error, never "no record": guessing would risk a second submission.
func readRecord(o metav1.Object) (record, error) {
	var r record
	an := o.GetAnnotations()
	if s := an[AnnoSubmission]; s != "" {
		var t Attempt
		if err := json.Unmarshal([]byte(s), &t); err != nil {
			return r, fmt.Errorf("annotation %s is not a submission record: %w", AnnoSubmission, err)
		}
		r.attempt = &t
	}
	var err error
	if r.jobID, err = annoID(an, AnnoJobID); err != nil {
		return r, err
	}
	if r.released, err = annoID(an, AnnoReleased); err != nil {
		return r, err
	}
	return r, nil
}

func annoID(an map[string]string, k string) (int64, error) {
	s := an[k]
	if s == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("annotation %s=%q is not a job ID", k, s)
	}
	return id, nil
}

func newToken() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// write sets annotations ("" deletes) on a copy of a and updates it with a's
// resourceVersion as the precondition. On success the new metadata is copied
// into a; a's in-memory status is left alone, because the API returns the
// stored status. A conflict is returned as is.
func (e *external) write(ctx context.Context, a *v1alpha1.Allocation, set map[string]string) error {
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
	if err := e.kube.Update(ctx, cp); err != nil {
		return err
	}
	cp.ObjectMeta.DeepCopyInto(&a.ObjectMeta)
	return nil
}

// refresh replaces a's metadata with the API server's (not the cache's).
func (e *external) refresh(ctx context.Context, a *v1alpha1.Allocation) error {
	fresh := &v1alpha1.Allocation{}
	if err := e.reader.Get(ctx, client.ObjectKeyFromObject(a), fresh); err != nil {
		return err
	}
	fresh.ObjectMeta.DeepCopyInto(&a.ObjectMeta)
	return nil
}

// writeOwn writes set only while the current attempt is still ours (same
// token). On a conflict it re-reads from the API server and tries again; if
// the attempt has been superseded it leaves the record alone.
func (e *external) writeOwn(ctx context.Context, a *v1alpha1.Allocation, token string, set map[string]string) error {
	for i := 0; i < 5; i++ {
		rec, err := readRecord(a)
		if err != nil {
			return err
		}
		if rec.attempt == nil || rec.attempt.Token != token {
			return nil // superseded: never overwrite a newer attempt
		}
		err = e.write(ctx, a, set)
		if err == nil || !kerrors.IsConflict(err) {
			return err
		}
		if err := e.refresh(ctx, a); err != nil {
			return err
		}
	}
	return errors.New("submission record: still conflicting after 5 re-reads")
}

// CriticalAnnotationUpdater replaces crossplane-runtime's. v2.4.3's
// RetryingCriticalAnnotationUpdater re-applies every in-memory annotation
// after a conflict (api.go:283-301), so a stale copy could roll the
// submission record back. This one re-applies only crossplane's own keys,
// and re-reads from the API server rather than the cache.
type CriticalAnnotationUpdater struct {
	Kube   client.Client
	Reader client.Reader
}

var crossplaneKeys = []string{
	meta.AnnotationKeyExternalName, meta.AnnotationKeyExternalCreatePending,
	meta.AnnotationKeyExternalCreateSucceeded, meta.AnnotationKeyExternalCreateFailed,
}

// UpdateCriticalAnnotations implements managed.CriticalAnnotationUpdater.
func (u CriticalAnnotationUpdater) UpdateCriticalAnnotations(ctx context.Context, o client.Object) error {
	crit := map[string]string{}
	for _, k := range crossplaneKeys {
		if v, ok := o.GetAnnotations()[k]; ok {
			crit[k] = v
		}
	}
	err := retry.OnError(retry.DefaultRetry, func(err error) bool { return !errors.Is(err, context.Canceled) }, func() error {
		err := u.Kube.Update(ctx, o)
		if kerrors.IsConflict(err) {
			if getErr := u.Reader.Get(ctx, client.ObjectKeyFromObject(o), o); getErr != nil {
				return getErr
			}
			meta.AddAnnotations(o, crit)
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("cannot update critical annotations: %w", err)
	}
	return nil
}

var _ managed.CriticalAnnotationUpdater = CriticalAnnotationUpdater{}

// PollIntervalHook re-checks soon while a submission is unresolved or the
// Allocation is being deleted, and otherwise keeps the normal poll interval.
// The guard therefore waits on a RequeueAfter, not on a hot loop of errors.
func PollIntervalHook(o Options) managed.PollIntervalHook {
	return func(mg resource.Managed, poll time.Duration) time.Duration {
		rec, err := readRecord(mg)
		busy := err != nil || rec.attempt.unresolved() || meta.WasDeleted(mg)
		if busy && o.UnresolvedPoll > 0 && o.UnresolvedPoll < poll {
			return o.UnresolvedPoll
		}
		return poll
	}
}

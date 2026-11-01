package fake

import (
	"context"
	"errors"
	"fmt"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// syncDeleting cancels the Allocation's live jobs and, once none is live and no
// attempt is unresolved, removes its files and its finalizer. The recorded job
// is read by its ID as well as by tag, as the real provider does: a job the
// platform renamed is invisible to the tag lookup, and its cancel may have
// failed or not happened yet when the Allocation was deleted. A failed read is
// unknown, so the finalizer stays.
func (c *Controller) syncDeleting(ctx context.Context, p *pass, s *allocState) error {
	a := s.a
	rec, err := record(a)
	if err != nil {
		return err
	}
	s.rec = rec
	jobs, err := c.deletingJobs(ctx, p, s)
	if err != nil {
		return nil
	}
	noJobID := a.Annotations[v1alpha1.AnnotationJobID] == ""
	if unresolved(rec) && noJobID && len(jobs) > 0 {
		// The unknown attempt is resolved by the job it made; it is cancelled
		// below and its hold is never lifted.
		adopted := pickAdopted(jobs)
		setRecord(a, c.newRecord(rec.Attempt, v1alpha1.SubmissionDone, "", p.now))
		a.Annotations[v1alpha1.AnnotationJobID] = adopted.ID
		if err := update(ctx, c.Client, a); err != nil {
			return fmt.Errorf("adopt job %s: %w", adopted.ID, err)
		}
		s.rec, _ = record(a)
	}
	if ids := liveIDs(jobs); len(ids) > 0 {
		for _, r := range c.Batch.Cancel(ctx, ids) {
			if r.Outcome == provider.CancelError && !errors.Is(r.Err, provider.ErrNotFound) {
				return fmt.Errorf("cancel job %s: %w", r.JobID, r.Err)
			}
		}
		// Confirm the cancels before letting go of the Allocation.
		if jobs, err = c.deletingJobs(ctx, p, s); err != nil {
			return nil
		}
		if len(liveIDs(jobs)) > 0 {
			return nil
		}
	}
	// A completing job has an end time but is still live: its processes and
	// its epilog may run. The record, the files and the finalizer wait for the
	// flag to clear. It is not cancelled again; scancel would answer that it
	// is already completing (the real provider's observeDeleting does the
	// same).
	if err := c.mirrorDeletingCompleting(ctx, p, s, jobs); err != nil {
		return err
	}
	if completing(jobs) {
		return nil
	}
	if unresolved(s.rec) && a.Annotations[v1alpha1.AnnotationJobID] == "" && !c.attemptReleased(s, p.now) {
		return nil // an unresolved attempt keeps the finalizer
	}
	// The accounting record is written before the files and the finalizer go,
	// from the reads above; it may keep the finalizer for the final read.
	if wait, err := c.keepAccounting(ctx, p, s, jobs); err != nil {
		return err
	} else if wait {
		return nil
	}
	if err := c.Batch.RemoveAllocationFiles(ctx, s.tag); err != nil {
		return fmt.Errorf("remove files: %w", err)
	}
	controllerutil.RemoveFinalizer(a, v1alpha1.FinalizerCancelAll)
	if err := update(ctx, c.Client, a); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

// deletingJobs returns the jobs with the tag and, when it does not carry the
// tag, the recorded job read by its ID.
func (c *Controller) deletingJobs(ctx context.Context, p *pass, s *allocState) ([]provider.Job, error) {
	jobs, err := c.findByTag(ctx, p, s.tag, s.a.CreationTimestamp.Add(-listLookback))
	if err != nil {
		return nil, err
	}
	id := s.a.Annotations[v1alpha1.AnnotationJobID]
	if id == "" {
		return jobs, nil
	}
	for _, j := range jobs {
		if j.ID == id {
			return jobs, nil
		}
	}
	job, found, err := c.observeJob(ctx, p, id)
	if err != nil {
		return nil, err
	}
	if found {
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// liveIDs returns the IDs of the jobs that can still be cancelled: held,
// pending or running, that is with no end time. A job requeued under its ID is
// Failed by phase but still holds its allocation, so the phase is not the test.
func liveIDs(jobs []provider.Job) []string {
	var ids []string
	for _, j := range jobs {
		if j.EndTime == nil {
			ids = append(ids, j.ID)
		}
	}
	return ids
}

// completing reports whether any job carries the COMPLETING flag.
func completing(jobs []provider.Job) bool {
	for _, j := range jobs {
		if j.Completing {
			return true
		}
	}
	return false
}

// mirrorDeletingCompleting writes status.atProvider.jobCompleting and the
// JobCompleting condition from the deleting pass's reads, as the real provider
// does: a job the delete cancelled shows COMPLETING while its epilog runs, and a
// deletion that waits on it must say so (HBEP-4, Risks). The recorded job's flag
// is mirrored when a read lists it and cleared when neither read does; nothing
// is written when the stored status already says the same. It is, with the
// absentSince anchor, the only status a deleting pass writes.
func (c *Controller) mirrorDeletingCompleting(ctx context.Context, p *pass, s *allocState, jobs []provider.Job) error {
	id := s.a.Annotations[v1alpha1.AnnotationJobID]
	if id == "" {
		return nil
	}
	job, listed := jobByID(jobs, id)
	obs := s.prior.DeepCopy()
	var conds []xpv2.Condition
	switch cur := s.a.GetCondition(v1alpha1.ConditionJobCompleting); {
	case listed && job.Completing:
		obs.JobCompleting = true
		conds = append(conds, xpv2.Condition{Type: v1alpha1.ConditionJobCompleting, Status: corev1.ConditionTrue, Reason: v1alpha1.ReasonJobCompleting,
			LastTransitionTime: metav1.NewTime(p.now), Message: completingMessage(id, job)})
	case cur.Status == corev1.ConditionTrue:
		conds = append(conds, xpv2.Condition{Type: v1alpha1.ConditionJobCompleting, Status: corev1.ConditionFalse, Reason: v1alpha1.ReasonJobNotCompleting,
			LastTransitionTime: metav1.NewTime(p.now)})
	}
	storedConds := append([]xpv2.Condition(nil), s.a.Status.Conditions...)
	s.a.SetConditions(conds...)
	if obs.JobCompleting == s.prior.JobCompleting && apiequality.Semantic.DeepEqual(storedConds, s.a.Status.Conditions) {
		return nil
	}
	s.a.Status.AtProvider = *obs
	if err := updateStatus(ctx, c.Client, s.a); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

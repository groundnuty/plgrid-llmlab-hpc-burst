package fake

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// syncDeleting cancels the Allocation's live jobs and, once none is live and no
// attempt is unresolved, removes its files and its finalizer.
func (c *Controller) syncDeleting(ctx context.Context, p *pass, s *allocState) error {
	a := s.a
	since := a.CreationTimestamp.Add(-listLookback)
	rec, err := record(a)
	if err != nil {
		return err
	}
	s.rec = rec
	jobs, err := c.findByTag(ctx, p, s.tag, since)
	if err != nil {
		return nil
	}
	noJobID := a.Annotations[v1alpha1.AnnotationJobID] == ""
	if unresolved(rec) && noJobID && len(jobs) > 0 {
		// The unknown attempt is resolved by the job it made; it is cancelled
		// below and its hold is never lifted.
		setRecord(a, c.newRecord(rec.Attempt, v1alpha1.SubmissionDone, "", p.now))
		a.Annotations[v1alpha1.AnnotationJobID] = jobs[0].ID
		if err := update(ctx, c.Client, a); err != nil {
			return fmt.Errorf("adopt job %s: %w", jobs[0].ID, err)
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
		if jobs, err = c.findByTag(ctx, p, s.tag, since); err != nil {
			return nil
		}
		if len(liveIDs(jobs)) > 0 {
			return nil
		}
	}
	if unresolved(s.rec) && a.Annotations[v1alpha1.AnnotationJobID] == "" && !c.attemptReleased(s, p.now) {
		return nil // an unresolved attempt keeps the finalizer
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

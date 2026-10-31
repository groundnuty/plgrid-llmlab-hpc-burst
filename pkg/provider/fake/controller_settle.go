package fake

import (
	"context"
	"errors"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// settle is HBEP-7's Settle (open question 11), run in hold mode on every
// observe of a recorded job while the Allocation is not deleting: it cancels
// every held job with the tag except the recorded one, then lifts the recorded
// job's hold once. A failed read stops it, with nothing cancelled and nothing
// lifted, and counts as a failed read of the pass. A failed cancel or lift is
// returned.
func (c *Controller) settle(ctx context.Context, p *pass, s *allocState, id string) error {
	jobs, err := c.Batch.FindByTag(ctx, s.tag, s.a.CreationTimestamp.Add(-listLookback))
	if err != nil {
		p.readFailed = true
		return nil
	}
	var recorded *provider.Job
	var extras []string
	for i, j := range jobs {
		switch {
		case j.ID == id:
			recorded = &jobs[i]
		case j.Held():
			extras = append(extras, j.ID)
		}
	}
	if len(extras) > 0 {
		for _, r := range c.Batch.Cancel(ctx, extras) {
			if r.Outcome == provider.CancelError && !errors.Is(r.Err, provider.ErrNotFound) {
				return fmt.Errorf("cancel held duplicate %s: %w", r.JobID, r.Err)
			}
		}
	}
	if recorded == nil || !liftWanted(s.a, *recorded) {
		return nil
	}
	return c.liftHold(ctx, s.a, id)
}

// liftWanted reports whether the recorded job's hold is to be lifted: the
// hold-lifted annotation does not name the job, and the job is held by its
// user, has restart count 0 and has never started.
func liftWanted(a *v1alpha1.Allocation, job provider.Job) bool {
	return a.Annotations[v1alpha1.AnnotationHoldLifted] != job.ID &&
		job.Hold == provider.HoldUser && job.RestartCount == 0 && job.StartTime == nil
}

// liftHold lifts the hold and writes the hold-lifted annotation. A job that
// stopped being held by its user between the read and the lift, or that the
// scheduler no longer knows, is not an error here: the observation reports what
// it is, and no annotation is written.
func (c *Controller) liftHold(ctx context.Context, a *v1alpha1.Allocation, id string) error {
	err := c.Batch.LiftHold(ctx, id)
	switch {
	case errors.Is(err, provider.ErrNotHeld), errors.Is(err, provider.ErrAdminHold), errors.Is(err, provider.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("lift hold of job %s: %w", id, err)
	}
	a.Annotations[v1alpha1.AnnotationHoldLifted] = id
	if err := update(ctx, c.Client, a); err != nil {
		return fmt.Errorf("record hold lift of job %s: %w", id, err)
	}
	return nil
}

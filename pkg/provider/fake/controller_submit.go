package fake

import (
	"context"
	"errors"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// establish gets the Allocation a job ID: it adopts a job found by tag, waits
// out a backoff, or submits. It returns settled=true when the Allocation now has
// a job ID and its job can be observed; otherwise it has set obs to the phase
// that explains why not.
func (c *Controller) establish(ctx context.Context, p *pass, a *v1alpha1.Allocation, rec *v1alpha1.SubmissionRecord, tag string, obs *v1alpha1.AllocationObservation) (settled bool, err error) {
	since := a.CreationTimestamp.Add(-listLookback)
	if rec != nil && (rec.State == v1alpha1.SubmissionUnknown || rec.State == v1alpha1.SubmissionInFlight) {
		// The outcome is unknown: only finding the tag resolves it.
		jobs, err := c.Batch.FindByTag(ctx, tag, since)
		if err != nil {
			p.readFailed = true
			return false, nil
		}
		if len(jobs) == 0 {
			obs.Phase, obs.Submission = v1alpha1.AllocationSubmissionUnknown, rec
			return false, nil
		}
		return true, c.adopt(ctx, p, a, rec, jobs[0])
	}
	if rec != nil && rec.State == v1alpha1.SubmissionRejected && rec.Time != nil && p.now.Sub(rec.Time.Time) < c.rejectBackoff() {
		obs.Phase, obs.Submission = v1alpha1.AllocationSubmitting, rec
		return false, nil
	}

	bp := a.Spec.ForProvider.Batch
	if bp == nil {
		return false, errors.New("forProvider.batch is not set: the fake Controller serves batch Allocations only")
	}
	attempt := int32(1)
	if rec != nil {
		attempt = rec.Attempt + 1
	}
	// Claim the attempt before the call, as the real protocol does.
	setRecord(a, c.newRecord(attempt, v1alpha1.SubmissionInFlight, "", p.now))
	if err := c.Client.Update(ctx, a); err != nil {
		return false, fmt.Errorf("claim attempt %d: %w", attempt, err)
	}
	id, outcome, serr := c.Batch.Submit(ctx, provider.SubmitRequest{
		Tag:     a.Annotations[v1alpha1.AnnotationExternalName],
		Hold:    c.HoldRelease,
		Account: accountName,
		Params:  *bp,
	})
	switch outcome {
	case provider.Accepted:
		setRecord(a, c.newRecord(attempt, v1alpha1.SubmissionDone, "", p.now))
		a.Annotations[v1alpha1.AnnotationJobID] = id
		if err := c.Client.Update(ctx, a); err != nil {
			return false, fmt.Errorf("record job %s: %w", id, err)
		}
		if c.HoldRelease {
			if err := c.liftHold(ctx, a, id); err != nil {
				return true, err
			}
		}
		return true, nil
	case provider.Rejected:
		r := c.newRecord(attempt, v1alpha1.SubmissionRejected, errText(serr), p.now)
		setRecord(a, r)
		obs.Phase, obs.Submission = v1alpha1.AllocationSubmitting, &r
	default:
		r := c.newRecord(attempt, v1alpha1.SubmissionUnknown, errText(serr), p.now)
		setRecord(a, r)
		obs.Phase, obs.Submission = v1alpha1.AllocationSubmissionUnknown, &r
	}
	if err := c.Client.Update(ctx, a); err != nil {
		return false, fmt.Errorf("record attempt %d: %w", attempt, err)
	}
	return false, nil
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// adopt records the found job as the Allocation's: the job ID, and the attempt
// Done. A held job gets its hold lifted once in hold mode (HBEP-7, Settle).
func (c *Controller) adopt(ctx context.Context, p *pass, a *v1alpha1.Allocation, rec *v1alpha1.SubmissionRecord, job provider.Job) error {
	done := c.newRecord(rec.Attempt, v1alpha1.SubmissionDone, "", p.now)
	setRecord(a, done)
	a.Annotations[v1alpha1.AnnotationJobID] = job.ID
	if err := c.Client.Update(ctx, a); err != nil {
		return fmt.Errorf("adopt job %s: %w", job.ID, err)
	}
	if c.HoldRelease && job.Hold == provider.HoldUser && job.RestartCount == 0 && job.StartTime == nil {
		return c.liftHold(ctx, a, job.ID)
	}
	return nil
}

// liftHold lifts the hold and writes the hold-lifted annotation. A job that is
// not held, held by an administrator, or unknown is not an error here: the
// observation reports what it is.
func (c *Controller) liftHold(ctx context.Context, a *v1alpha1.Allocation, id string) error {
	err := c.Batch.LiftHold(ctx, id)
	switch {
	case errors.Is(err, provider.ErrNotHeld), errors.Is(err, provider.ErrAdminHold), errors.Is(err, provider.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("lift hold of job %s: %w", id, err)
	}
	a.Annotations[v1alpha1.AnnotationHoldLifted] = id
	if err := c.Client.Update(ctx, a); err != nil {
		return fmt.Errorf("record hold lift of job %s: %w", id, err)
	}
	return nil
}

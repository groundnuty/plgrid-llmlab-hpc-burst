package fake

import (
	"context"
	"errors"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// unresolved reports whether the record is an attempt whose outcome is not
// known: InFlight or Unknown. Only finding the tag resolves it, or the rules of
// stall.
func unresolved(rec *v1alpha1.SubmissionRecord) bool {
	return rec != nil && (rec.State == v1alpha1.SubmissionUnknown || rec.State == v1alpha1.SubmissionInFlight)
}

// establish gets the Allocation a job ID: it adopts a job found by tag, waits
// out a backoff or an unresolved attempt, or submits. It returns settled=true
// when the Allocation now has a job ID and its job can be observed; otherwise
// it has set obs to the phase that explains why not.
func (c *Controller) establish(ctx context.Context, p *pass, s *allocState) (settled bool, err error) {
	rec := s.rec
	if unresolved(rec) {
		jobs, err := c.findByTag(ctx, p, s.tag, s.a.CreationTimestamp.Add(-listLookback))
		if err != nil {
			return false, nil
		}
		if len(jobs) > 0 {
			return true, c.adopt(ctx, p, s, jobs[0])
		}
		resubmit, err := c.stall(ctx, p, s)
		if err != nil || !resubmit {
			return false, err
		}
	} else if rec != nil && rec.State == v1alpha1.SubmissionRejected && rec.Time != nil && p.now.Sub(rec.Time.Time) < c.rejectBackoff() {
		s.obs.Phase, s.obs.Submission = v1alpha1.AllocationSubmitting, rec
		return false, nil
	}
	return c.submit(ctx, p, s)
}

// submit claims the next attempt, runs the submission and records its outcome.
// The hold, in hold mode, is lifted by settle, in the observation that follows
// an accepted submission.
func (c *Controller) submit(ctx context.Context, p *pass, s *allocState) (bool, error) {
	a := s.a
	bp := a.Spec.ForProvider.Batch
	if bp == nil {
		return false, errors.New("forProvider.batch is not set: the fake Controller serves batch Allocations only")
	}
	attempt := int32(1)
	if s.rec != nil {
		attempt = s.rec.Attempt + 1
	}
	// Claim the attempt before the call, as the real protocol does. The claim
	// replaces the record of the attempt before it.
	if err := c.writeRecord(ctx, s, c.newRecord(attempt, v1alpha1.SubmissionInFlight, "", p.now)); err != nil {
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
		if err := update(ctx, c.Client, a); err != nil {
			return false, fmt.Errorf("record job %s: %w", id, err)
		}
		return true, nil
	case provider.Rejected:
		s.obs.Phase = v1alpha1.AllocationSubmitting
		return false, c.writeRecord(ctx, s, c.newRecord(attempt, v1alpha1.SubmissionRejected, errText(serr), p.now))
	default:
		s.obs.Phase = v1alpha1.AllocationSubmissionUnknown
		return false, c.writeRecord(ctx, s, c.newRecord(attempt, v1alpha1.SubmissionUnknown, errText(serr), p.now))
	}
}

// writeRecord stores r as the Allocation's submission record and mirrors it in
// the observation.
func (c *Controller) writeRecord(ctx context.Context, s *allocState, r v1alpha1.SubmissionRecord) error {
	setRecord(s.a, r)
	if err := update(ctx, c.Client, s.a); err != nil {
		return fmt.Errorf("record attempt %d as %s: %w", r.Attempt, r.State, err)
	}
	s.rec, s.obs.Submission = &r, &r
	return nil
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// adopt records the found job as the Allocation's: the job ID, and the attempt
// Done. In hold mode the observation that follows settles it (HBEP-7, Settle).
func (c *Controller) adopt(ctx context.Context, p *pass, s *allocState, job provider.Job) error {
	done := c.newRecord(s.rec.Attempt, v1alpha1.SubmissionDone, "", p.now)
	setRecord(s.a, done)
	s.a.Annotations[v1alpha1.AnnotationJobID] = job.ID
	if err := update(ctx, c.Client, s.a); err != nil {
		return fmt.Errorf("adopt job %s: %w", job.ID, err)
	}
	s.markResolved(p.now, fmt.Sprintf("attempt %d resolved by job %s", done.Attempt, job.ID))
	return nil
}

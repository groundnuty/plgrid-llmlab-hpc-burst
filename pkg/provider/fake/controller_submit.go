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
	limitReason(s.obs, rec)
	rejected := rec != nil && rec.State == v1alpha1.SubmissionRejected
	if unresolved(rec) || rejected {
		// The tag is looked up before anything else is done about the attempt,
		// as the real provider does (internal/providerctl's establish), after a
		// refusal too: a job whose reply was lost and that registered late
		// (Script.RegisteredLate) is the reason the next attempt was refused
		// for a full submit limit, and it is found here, adopted, and not
		// followed by an attempt n+2 (HBEP-4, Test plan, "A lost reply under a
		// one-job limit").
		jobs, err := c.findByTag(ctx, p, s.tag, s.a.CreationTimestamp.Add(-listLookback))
		if err != nil {
			return false, nil
		}
		if len(jobs) > 0 {
			return true, c.adopt(ctx, p, s, pickAdopted(jobs))
		}
		if rejected {
			if rec.Time != nil && p.now.Sub(rec.Time.Time) < c.rejectBackoff() {
				s.obs.Phase, s.obs.Submission = v1alpha1.AllocationSubmitting, rec
				return false, nil
			}
		} else {
			resubmit, err := c.stall(ctx, p, s)
			if err != nil || !resubmit {
				return false, err
			}
		}
	}
	if why := c.Prerequisites.missing(s.a); why != "" {
		s.obs.Phase, s.obs.Reason = v1alpha1.AllocationSubmitting, why
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
		r := c.newRecord(attempt, v1alpha1.SubmissionRejected, errText(serr), p.now)
		// A refusal for a full submit limit carries its code in the same
		// record (v1alpha1.SubmissionRecord.Code; HBEP-4, QOS limits).
		var pr *provider.PolicyRefusal
		if errors.As(serr, &pr) && pr.SubmitLimit() {
			r.Code = v1alpha1.ReasonSubmitLimitReached
		}
		return false, c.writeRecord(ctx, s, r)
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
	limitReason(s.obs, &r)
	return nil
}

// limitReason derives the observation's reason from the current submission
// record: SubmitLimitReached while the record carries that code, and never
// otherwise. Each pass starts from the stored status, so a reason set for an
// earlier limit refusal is cleared once the record moves on: to a refusal of
// another kind, which HBEP-4 says is never read as a full slot, to an Unknown
// outcome, a new claim or an established job. Any other reason is left as it
// is.
func limitReason(obs *v1alpha1.AllocationObservation, rec *v1alpha1.SubmissionRecord) {
	switch {
	case rec != nil && rec.State == v1alpha1.SubmissionRejected && rec.Code == v1alpha1.ReasonSubmitLimitReached:
		obs.Reason = v1alpha1.ReasonSubmitLimitReached
	case obs.Reason == v1alpha1.ReasonSubmitLimitReached:
		obs.Reason = ""
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// pickAdopted chooses the job to adopt among those carrying the tag, in
// HBEP-7's order (Adoption): a live job that is not held (no end time, no hold),
// then a job that ran (it has a start time), then any. Within each class it is
// the lowest job ID; jobs are sorted by ID, never taken in the order given. The
// caller passes at least one job.
func pickAdopted(jobs []provider.Job) provider.Job {
	sorted := append([]provider.Job(nil), jobs...)
	byID(sorted)
	for _, class := range []func(provider.Job) bool{
		func(j provider.Job) bool { return live(j) && !j.Held() },
		func(j provider.Job) bool { return j.StartTime != nil },
		func(provider.Job) bool { return true },
	} {
		for _, j := range sorted {
			if class(j) {
				return j
			}
		}
	}
	panic("fake: pickAdopted needs a job") // the last class takes any
}

// adopt records the found job as the Allocation's: the job ID, and an
// unresolved attempt Done. A Rejected attempt's record stays as it is, as in the
// real provider: the job is an earlier attempt's, and the refusal happened. In
// hold mode the observation that follows settles it (HBEP-7, Settle).
func (c *Controller) adopt(ctx context.Context, p *pass, s *allocState, job provider.Job) error {
	attempt := s.rec.Attempt
	if unresolved(s.rec) {
		setRecord(s.a, c.newRecord(attempt, v1alpha1.SubmissionDone, "", p.now))
	}
	s.a.Annotations[v1alpha1.AnnotationJobID] = job.ID
	if err := update(ctx, c.Client, s.a); err != nil {
		return fmt.Errorf("adopt job %s: %w", job.ID, err)
	}
	s.markResolved(p.now, fmt.Sprintf("attempt %d resolved by job %s", attempt, job.ID))
	return nil
}

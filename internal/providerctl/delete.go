package providerctl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// observeDeleting decides whether a deleting Allocation still holds anything:
// it exists while any job with the tag, or the recorded job, is live (held,
// pending, running or suspended: no end time), or an attempt is unresolved
// (HBEP-7, open question 11, Deletion). A submission that returns during
// deletion is recorded, never lifted, and cancelled. Once nothing is left, the
// accounting record is written (keepAccounting, which may wait for the final
// read of a job that started, at most lostGrace), the Allocation's files are
// removed and the reconciler may drop the finalizer.
//
// The recorded job is read by its ID as well as by tag: a job the platform
// stored under another name (refuseRenamed) is invisible to the tag lookup,
// and its cancel may have failed or not happened yet when the Allocation was
// deleted. A failed read of it is unknown, never absence, so the finalizer
// stays (review of PR 76: Astra B3, Opus R2).
//
// An unresolved attempt keeps the finalizer whether or not a job is recorded:
// an adopted job (an older attempt's late registration) says nothing about the
// claimant of a newer attempt still inside its window, whose job could
// register after the finalizer went (Astra B4). The recorded job resolves the
// attempt here by the same rule as observeRecorded, so a fallback-mode
// deletion needs no operator for an attempt the recorded job answers.
func (e *external) observeDeleting(ctx context.Context, a *v1alpha1.Allocation, rec record, name string, now time.Time) (managed.ExternalObservation, error) {
	e.p.SetBusy(name, true)
	jobs, err := e.p.FindByTag(ctx, name, since(a))
	if err != nil {
		return exists, e.readFailed(err)
	}
	if rec.jobID == "" && needsResolution(rec) && len(jobs) > 0 {
		if err := e.adopt(ctx, a, &rec, choose(jobs), now); err != nil {
			return exists, err
		}
	}
	if rec.jobID != "" && !hasJob(jobs, rec.jobID) {
		job, found, err := e.p.Observe(ctx, rec.jobID)
		if err != nil {
			return exists, e.readFailed(err)
		}
		if found {
			jobs = append(jobs, job)
		}
	}
	if rec.jobID != "" {
		if err := e.resolveByRecorded(ctx, a, &rec, now); err != nil {
			return exists, err
		}
	}
	e.toCancel = nil
	for _, j := range jobs {
		if j.EndTime == nil {
			e.toCancel = append(e.toCancel, j.ID)
		}
	}
	if len(e.toCancel) > 0 {
		return exists, nil
	}
	if needsResolution(rec) && !e.attemptReleased(a, rec.sub, now) {
		if !e.hold() && now.Sub(attemptTime(a, rec.sub)) > e.unknownBound() {
			e.needOperator(a, rec.sub)
		}
		return exists, nil
	}
	if kept, err := e.keepAccounting(ctx, a, rec, name, jobs, now); err != nil {
		return exists, err
	} else if !kept {
		return exists, nil
	}
	if err := e.p.RemoveAllocationFiles(ctx, name); err != nil {
		return exists, fmt.Errorf("remove the Allocation's files: %w", err)
	}
	e.p.SetBusy(name, false)
	e.s.finals.Delete(rec.jobID)
	e.s.limitEvents.Delete(a.UID)
	return missing, nil
}

func hasJob(jobs []provider.Job, id string) bool {
	for _, j := range jobs {
		if j.ID == id {
			return true
		}
	}
	return false
}

// attemptReleased reports whether a deleting Allocation may let go of its
// unresolved attempt, which has no job: in hold mode once the in-flight window
// has passed (a job that registers later is a held orphan, which never runs),
// in fallback mode when an operator abandoned the attempt.
func (e *external) attemptReleased(a *v1alpha1.Allocation, s *v1alpha1.SubmissionRecord, now time.Time) bool {
	if s == nil {
		return true
	}
	if e.hold() {
		return now.Sub(attemptTime(a, s)) > e.inflightWindow()
	}
	v, ok := a.Annotations[v1alpha1.AnnotationAbandonAttempt]
	return ok && v == strconv.Itoa(int(s.Attempt))
}

// Delete implements managed.ExternalClient: it cancels every live job with the
// tag that Observe found in this reconcile, each with its own result. JobCancelled
// is raised only for a job Cancel found live (Outcome Cancelled). A job that had
// already ended by then (AlreadyEnded), or that Slurm no longer knows, is a
// no-op: no event, no error, and deletion goes on (#199). Any other failure is
// returned and the finalizer stays.
func (e *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalDelete{}, errNotAllocation
	}
	if len(e.toCancel) == 0 {
		return managed.ExternalDelete{}, nil
	}
	var errs []error
	for _, r := range e.p.Cancel(ctx, e.toCancel) {
		switch {
		case r.Outcome == provider.Cancelled:
			e.event(a, event.Normal("JobCancelled", fmt.Sprintf("job %s with the tag was cancelled", r.JobID)))
		case r.Outcome == provider.CancelError && !errors.Is(r.Err, provider.ErrNotFound):
			errs = append(errs, fmt.Errorf("cancel job %s: %w", r.JobID, r.Err))
		}
	}
	return managed.ExternalDelete{}, errors.Join(errs...)
}

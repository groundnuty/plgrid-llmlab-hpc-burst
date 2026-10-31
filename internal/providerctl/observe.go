package providerctl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Reasons of the read-back's SubmittedAsOther condition and of the refusal of
// a rewritten name (HBEP-4, The Slurm dialect, Read-back).
const (
	reasonReadBackMatched xpv2.ConditionReason = "ReadBackMatched"
	reasonReadBackDiffers xpv2.ConditionReason = "ReadBackDiffers"
	reasonAdminHold       xpv2.ConditionReason = "JobHeldAdmin"
	reasonNoAdminHold     xpv2.ConditionReason = "NotHeldByAdmin"
	renamedPrefix                              = "the job's name came back as "
)

// observeRecorded observes the recorded job: it resolves the attempt the job
// answers, reads the job back once, settles in hold mode, mirrors the job into
// the status, and judges Lost from reads that succeeded.
func (e *external) observeRecorded(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string, now time.Time) (managed.ExternalObservation, error) {
	o := &a.Status.AtProvider
	prior := o.DeepCopy()
	if s := rec.sub; unresolved(s) && (s.State == v1alpha1.SubmissionUnknown || now.Sub(attemptTime(a, s)) > e.inflightWindow()) {
		done := *s
		done.State, done.Writer, done.Time = v1alpha1.SubmissionDone, e.s.o.Identity, mt(now)
		done.Reason = "resolved by the recorded job " + rec.jobID
		if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(done)}); err != nil {
			return exists, err
		}
		if err := e.reread(a, rec); err != nil {
			return exists, err
		}
	}
	e.markResolved(a, rec.sub, now)
	o.JobID, o.Submission = rec.jobID, rec.sub

	// A job refused for a rewritten name stays refused; it is only kept
	// cancelled.
	if o.Phase == v1alpha1.AllocationRefused && strings.HasPrefix(o.Reason, renamedPrefix) {
		return e.keepRefused(ctx, a, rec.jobID)
	}
	// An ended job's accounting record is final: no more reads.
	if (o.Phase == v1alpha1.AllocationEnded || o.Phase == v1alpha1.AllocationFailed) && o.EndTime != nil {
		e.p.SetBusy(name, false)
		return exists, nil
	}

	job, found, err := e.p.Observe(ctx, rec.jobID)
	if err != nil {
		*o = *prior
		o.JobID, o.Submission = rec.jobID, rec.sub
		return exists, e.readFailed(err)
	}
	if !found {
		e.absent(a, rec.jobID, now)
		return exists, nil
	}
	if refused, err := e.readBack(ctx, a, name, job); refused || err != nil {
		return exists, err
	}
	var settleErr error
	if e.hold() {
		lifted, err := e.settle(ctx, a, rec, name, job)
		if err != nil && !errors.Is(err, errLiftFailed) {
			*o = *prior
			o.JobID, o.Submission = rec.jobID, rec.sub
			return exists, err
		}
		settleErr = err
		if lifted {
			if j, ok, err := e.p.Observe(ctx, rec.jobID); err == nil && ok {
				job = j
			}
		}
	}
	if err := e.duplicates(ctx, a, name, rec.jobID); err != nil {
		*o = *prior
		o.JobID, o.Submission = rec.jobID, rec.sub
		return exists, err
	}
	e.mirror(a, prior, rec.jobID, job)
	e.p.SetBusy(name, o.Phase == v1alpha1.AllocationHeld)
	return exists, settleErr
}

// mirror writes what the read of the recorded job showed into the status.
func (e *external) mirror(a *v1alpha1.Allocation, prior *v1alpha1.AllocationObservation, id string, job provider.Job) {
	o := &a.Status.AtProvider
	if prior.Phase == v1alpha1.AllocationLost {
		e.event(a, event.Normal("JobReappeared", fmt.Sprintf("job %s shows again in the %s after being reported Lost", id, job.Source)))
	}
	o.AbsentSince = nil
	o.Phase = job.Phase
	o.Held = job.Held()
	o.Requeued = o.Requeued || job.Requeued // kept once seen
	o.Reason = job.Reason
	if o.Requeued {
		o.Phase, o.Reason = v1alpha1.AllocationFailed, v1alpha1.ReasonRequeued
	}
	o.RestartCount = job.RestartCount
	o.SubmitTime = mt(job.SubmitTime)
	o.StartEstimate, o.StartTime = mtp(job.StartEstimate), mtp(job.StartTime)
	o.HardEnd, o.EndTime = mtp(job.HardEnd), mtp(job.EndTime)
	if job.StartTime != nil && prior.StartTime == nil {
		e.s.waits.add(id)
	}
	switch {
	case job.Hold == provider.HoldAdmin:
		a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionAdminHold, Status: corev1.ConditionTrue, Reason: reasonAdminHold,
			Message: fmt.Sprintf("job %s is held by an administrator; the provider never lifts that hold", id), LastTransitionTime: *mt(e.now())})
	case a.GetCondition(v1alpha1.ConditionAdminHold).Status == corev1.ConditionTrue:
		a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionAdminHold, Status: corev1.ConditionFalse, Reason: reasonNoAdminHold,
			LastTransitionTime: *mt(e.now())})
	}
	setAvailability(a)
}

// setAvailability maps the phase onto crossplane's Ready condition.
func setAvailability(a *v1alpha1.Allocation) {
	o := &a.Status.AtProvider
	switch o.Phase {
	case v1alpha1.AllocationRunning:
		a.SetConditions(xpv2.Available())
	case v1alpha1.AllocationHeld, v1alpha1.AllocationPending, v1alpha1.AllocationSubmitting, v1alpha1.AllocationSubmissionUnknown:
		a.SetConditions(xpv2.Creating())
	default:
		a.SetConditions(xpv2.Unavailable().WithMessage(fmt.Sprintf("phase %s: %s", o.Phase, o.Reason)))
	}
}

// absent handles a recorded job that both reads, which succeeded, did not show:
// it keeps its phase until lostGrace has passed, then it is Lost, with a
// Warning event, and never resubmitted.
func (e *external) absent(a *v1alpha1.Allocation, id string, now time.Time) {
	o := &a.Status.AtProvider
	if o.AbsentSince == nil {
		o.AbsentSince = mt(now)
	}
	if now.Sub(o.AbsentSince.Time) <= e.lostGrace() {
		return
	}
	if o.Phase != v1alpha1.AllocationLost {
		e.event(a, event.Warning("JobLost", fmt.Errorf("job %s has been absent from the queue listing and accounting since %s", id, o.AbsentSince.UTC().Format(time.RFC3339))))
	}
	o.Phase = v1alpha1.AllocationLost
	o.Reason = "absent from the queue listing and accounting"
	setAvailability(a)
}

// readBack compares the job as established with the request, once, on the
// first observe after it is established (HBEP-4, Read-back). A name that
// differs refuses the Allocation and cancels the job; any other difference
// sets SubmittedAsOther. The condition, True or False, marks the read-back as
// done.
func (e *external) readBack(ctx context.Context, a *v1alpha1.Allocation, name string, job provider.Job) (bool, error) {
	if a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status != corev1.ConditionUnknown {
		return false, nil
	}
	o := &a.Status.AtProvider
	if job.Name != name {
		o.Phase = v1alpha1.AllocationRefused
		o.Reason = fmt.Sprintf("%s%q, not the tag; the site rewrote it, so the job could never be found by tag", renamedPrefix, job.Name)
		o.SubmitTime = mt(job.SubmitTime)
		a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
		e.event(a, event.Warning("JobNameRewritten", errors.New(o.Reason)))
		if job.EndTime == nil {
			return true, e.cancel(ctx, []string{job.ID})
		}
		return true, nil
	}
	p := a.Spec.ForProvider.Batch
	var diffs []string
	if job.Partition != "" && job.Partition != p.Partition {
		diffs = append(diffs, fmt.Sprintf("partition %s, requested %s", job.Partition, p.Partition))
	}
	if want, err := p.TimeLimit.AsDuration(); err == nil && job.TimeLimit > 0 && job.TimeLimit != want {
		diffs = append(diffs, fmt.Sprintf("time limit %s, requested %s", job.TimeLimit, want))
	}
	if job.Source == provider.SourceQueue && job.GPUs != p.GPUs {
		diffs = append(diffs, fmt.Sprintf("%d GPUs, requested %d", job.GPUs, p.GPUs))
	}
	c := xpv2.Condition{Type: v1alpha1.ConditionSubmittedAsOther, Status: corev1.ConditionFalse, Reason: reasonReadBackMatched,
		Message: "the job was established as requested", LastTransitionTime: *mt(e.now())}
	if len(diffs) > 0 {
		c.Status, c.Reason = corev1.ConditionTrue, reasonReadBackDiffers
		c.Message = "the job was established with " + strings.Join(diffs, "; ")
		e.event(a, event.Warning("SubmittedAsOther", errors.New(c.Message)))
	}
	a.SetConditions(c)
	return false, nil
}

// keepRefused keeps a refused job cancelled.
func (e *external) keepRefused(ctx context.Context, a *v1alpha1.Allocation, id string) (managed.ExternalObservation, error) {
	job, found, err := e.p.Observe(ctx, id)
	if err != nil {
		return exists, e.readFailed(err)
	}
	if found && job.EndTime == nil {
		return exists, e.cancel(ctx, []string{id})
	}
	return exists, nil
}

var errLiftFailed = errors.New("lifting the hold failed")

// settle is HBEP-7's Settle, in hold mode, on every observe of the recorded
// job while the Allocation is not deleting: it cancels every held job with the
// tag except the recorded one, then lifts the recorded job's hold once, only if
// the hold-lifted annotation does not name it, it is held by its user, has
// restart count 0, has never started and passed the read-back. The annotation
// is written after the lift. A failed read stops settle; a lift that fails
// with an unknown error wraps errLiftFailed and the next observe tries again.
func (e *external) settle(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string, recorded provider.Job) (bool, error) {
	listed, err := e.p.ListByTag(ctx, name)
	if err != nil {
		return false, e.readFailed(err)
	}
	var extras []string
	for _, j := range listed {
		if j.ID != rec.jobID && j.Held() && j.EndTime == nil {
			extras = append(extras, j.ID)
		}
	}
	if len(extras) > 0 {
		if err := e.cancel(ctx, extras); err != nil {
			return false, err
		}
		e.event(a, event.Normal("HeldDuplicateCancelled", fmt.Sprintf("held job(s) %s with the tag were cancelled; job %s is the Allocation's", strings.Join(extras, ", "), rec.jobID)))
	}
	if rec.lifted == recorded.ID || recorded.Hold != provider.HoldUser || recorded.RestartCount != 0 || recorded.StartTime != nil ||
		a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == corev1.ConditionTrue {
		return false, nil
	}
	err = e.p.LiftHold(ctx, recorded.ID)
	switch {
	case errors.Is(err, provider.ErrNotHeld), errors.Is(err, provider.ErrAdminHold), errors.Is(err, provider.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("%w for job %s: %v", errLiftFailed, recorded.ID, err)
	}
	if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationHoldLifted: recorded.ID}); err != nil {
		return true, fmt.Errorf("record the hold lift of job %s: %w", recorded.ID, err)
	}
	rec.lifted = recorded.ID
	e.event(a, event.Normal("HoldLifted", fmt.Sprintf("the hold of job %s was lifted", recorded.ID)))
	return true, nil
}

// duplicates records extra jobs with the tag that are not held, in the sticky
// duplicates list, with a Warning event when the list grows.
func (e *external) duplicates(ctx context.Context, a *v1alpha1.Allocation, name, recorded string) error {
	listed, err := e.p.ListByTag(ctx, name)
	if err != nil {
		return e.readFailed(err)
	}
	o := &a.Status.AtProvider
	have := map[string]bool{}
	for _, d := range o.Duplicates {
		have[d] = true
	}
	var added []string
	for _, j := range listed {
		if j.ID != recorded && !j.Held() && !have[j.ID] {
			have[j.ID] = true
			added = append(added, j.ID)
		}
	}
	if len(added) == 0 {
		return nil
	}
	o.Duplicates = append(o.Duplicates, added...)
	sort.Strings(o.Duplicates)
	e.event(a, event.Warning("DuplicateJob", fmt.Errorf("job(s) %s carry the tag and are not held; job %s is the Allocation's; they are cancelled on deletion", strings.Join(added, ", "), recorded)))
	return nil
}

// cancel cancels jobs and returns the failures; a job Slurm no longer knows is
// not a failure.
func (e *external) cancel(ctx context.Context, ids []string) error {
	var errs []error
	for _, r := range e.p.Cancel(ctx, ids) {
		if r.Outcome == provider.CancelError && !errors.Is(r.Err, provider.ErrNotFound) {
			errs = append(errs, fmt.Errorf("cancel job %s: %w", r.JobID, r.Err))
		}
	}
	return errors.Join(errs...)
}

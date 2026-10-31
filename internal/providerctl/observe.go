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
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Reasons of the AdminHold condition.
const (
	reasonAdminHold   xpv2.ConditionReason = "JobHeldAdmin"
	reasonNoAdminHold xpv2.ConditionReason = "NotHeldByAdmin"
)

// observeRecorded observes the recorded job: it resolves the attempt the job
// answers, checks the job's name and, on the first observe, its settings
// (the read-back), settles in hold mode, mirrors the job into the status,
// records its queue wait once, and judges Lost from reads that succeeded.
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
	refused := prior.Phase == v1alpha1.AllocationRefused

	// An ended job's accounting record is final: no more reads.
	if (o.Phase == v1alpha1.AllocationEnded || o.Phase == v1alpha1.AllocationFailed) && o.EndTime != nil && o.QueueWaitRecorded {
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
	var errs []error
	renamed := job.Name != name
	if renamed {
		if err := e.refuseRenamed(ctx, a, prior, job); err != nil {
			errs = append(errs, err)
		}
		refused = true
	} else if prior.JobID != job.ID {
		e.readBack(a, job)
	}
	if e.hold() && !refused {
		lifted, err := e.settle(ctx, a, rec, name, job)
		if err != nil && !errors.Is(err, errLiftFailed) {
			*o = *prior
			o.JobID, o.Submission = rec.jobID, rec.sub
			return exists, err
		}
		if err != nil {
			errs = append(errs, err)
		}
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
	if refused {
		// Refused, once set, is kept on every path; the rest of the
		// observation follows the job.
		o.Phase = v1alpha1.AllocationRefused
		if renamed && prior.Phase != v1alpha1.AllocationRefused {
			o.Reason = v1alpha1.ReasonJobRenamed
		} else {
			o.Reason = prior.Reason
		}
		setAvailability(a)
	}
	if job.StartTime != nil && !prior.QueueWaitRecorded {
		if err := e.recordQueueWait(ctx, rec.jobID, job); err != nil {
			errs = append(errs, err)
		} else {
			o.QueueWaitRecorded = true
		}
	}
	e.p.SetBusy(name, o.Phase == v1alpha1.AllocationHeld)
	return exists, errors.Join(errs...)
}

// recordQueueWait adds the job's queue wait, its own submit and start times,
// to the Provider's status (docs/development.md, Queue waits), at once, so the
// caller sets queueWaitRecorded only after the write succeeded. A wait already
// there (the Provider's write succeeded and the Allocation's did not) costs no
// write. The list is kept oldest start first. Conflicts re-read; the status
// loop's writes and this one never overwrite each other.
func (e *external) recordQueueWait(ctx context.Context, id string, job provider.Job) error {
	w := v1alpha1.QueueWait{JobID: id, SubmitTime: *mt(job.SubmitTime), StartTime: *mt(*job.StartTime)}
	var err error
	for i := 0; i < 3; i++ {
		var prov v1alpha1.Provider
		if err = e.s.reader.Get(ctx, client.ObjectKey{Name: e.s.o.Provider}, &prov); err != nil {
			return fmt.Errorf("get Provider %s to record the queue wait of job %s: %w", e.s.o.Provider, id, err)
		}
		for _, old := range prov.Status.QueueWaits {
			if old.JobID == id {
				return nil
			}
		}
		prov.Status.QueueWaits = addQueueWait(prov.Status.QueueWaits, w)
		if err = e.s.kube.Status().Update(ctx, &prov); err == nil {
			return nil
		}
		if !kerrors.IsConflict(err) {
			break
		}
	}
	return fmt.Errorf("record the queue wait of job %s: %w", id, err)
}

// addQueueWait is v1alpha1.AddQueueWait with the list kept oldest start
// first, whatever order the jobs were observed in.
func addQueueWait(waits []v1alpha1.QueueWait, w v1alpha1.QueueWait) []v1alpha1.QueueWait {
	out := v1alpha1.AddQueueWait(waits, w)
	sort.SliceStable(out, func(i, k int) bool { return out[i].StartTime.Before(&out[k].StartTime) })
	return out
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

// readBack compares the job's partition, time limit and GPU count with the
// request, on the first observe after it is established only (HBEP-4, The
// Slurm dialect, Read-back): a later change, such as an administrator's
// extension of the time limit, is a later hard end, never a mismatch. A
// difference sets SubmittedAsOther True, reason ReasonReadBackMismatch, with
// each differing field and both values; the condition is never cleared, and
// the lifecycle controller replaces the Allocation. The GPU count is compared
// only from the queue listing, whose tres_req_str always carries it.
func (e *external) readBack(a *v1alpha1.Allocation, job provider.Job) {
	if a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == corev1.ConditionTrue {
		return
	}
	diffs := readBackDiffs(a.Spec.ForProvider.Batch, job)
	if len(diffs) == 0 {
		return
	}
	msg := "job " + job.ID + " was established with other settings than requested: " + strings.Join(diffs, "; ")
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionSubmittedAsOther, Status: corev1.ConditionTrue,
		Reason: v1alpha1.ReasonReadBackMismatch, Message: msg, LastTransitionTime: *mt(e.now())})
	e.event(a, event.Warning("SubmittedAsOther", errors.New(msg)))
}

// readBackDiffs lists the fields of job that differ from the request, each
// with both values, in the order partition, time limit, GPUs.
func readBackDiffs(p *v1alpha1.BatchParameters, job provider.Job) []string {
	var diffs []string
	if job.Partition != "" && job.Partition != p.Partition {
		diffs = append(diffs, fmt.Sprintf("partition: requested %q, got %q", p.Partition, job.Partition))
	}
	if want, err := p.TimeLimit.AsDuration(); err == nil && job.TimeLimit > 0 && job.TimeLimit != want {
		diffs = append(diffs, fmt.Sprintf("timeLimit: requested %s, got %s", want, job.TimeLimit))
	}
	if job.Source == provider.SourceQueue && job.GPUs != p.GPUs {
		diffs = append(diffs, fmt.Sprintf("gpus: requested %d, got %d", p.GPUs, job.GPUs))
	}
	return diffs
}

// refuseRenamed handles a recorded job stored under a name other than the
// tag: it can never be found by tag again, so deletion's cancel by tag would
// miss it. The phase becomes Refused, reason ReasonJobRenamed, and the job is
// cancelled while it is live. The name is checked on every observe, so a
// failed cancel is retried; a cancelled job has an end time, so it is not
// cancelled twice.
func (e *external) refuseRenamed(ctx context.Context, a *v1alpha1.Allocation, prior *v1alpha1.AllocationObservation, job provider.Job) error {
	if prior.Phase != v1alpha1.AllocationRefused {
		e.event(a, event.Warning("JobNameRewritten", fmt.Errorf("job %s came back under the name %q, not the tag; it is cancelled", job.ID, job.Name)))
	}
	if job.EndTime != nil {
		return nil
	}
	return e.cancel(ctx, []string{job.ID})
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
	// A job established as something else (SubmittedAsOther) is not lifted:
	// the lifecycle controller replaces the Allocation, and the job never runs.
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

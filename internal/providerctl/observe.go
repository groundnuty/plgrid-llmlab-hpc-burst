package providerctl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	if err := e.resolveByRecorded(ctx, a, rec, now); err != nil {
		return exists, err
	}
	o.JobID, o.Submission = rec.jobID, rec.sub
	refused := prior.Phase == v1alpha1.AllocationRefused

	if e.final(rec.jobID, o) {
		return e.observeFinal(ctx, a, rec, name)
	}
	job, found, err := e.p.Observe(ctx, rec.jobID)
	if err != nil {
		*o = *prior
		o.JobID, o.Submission = rec.jobID, rec.sub
		return exists, e.readFailed(err)
	}
	if job.EndTime != nil && job.Source == provider.SourceAccounting {
		e.s.finals.Store(rec.jobID, true)
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
	} else if rec.readBack != job.ID && e.readBack(a, job) {
		if err := e.markReadBack(ctx, a, rec, job.ID); err != nil {
			errs = append(errs, err)
		}
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
	job, read, err := e.allocatedRead(ctx, job, prior, now)
	if err != nil {
		errs = append(errs, err)
	}
	e.mirrorAccounting(a, job, prior, read)
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

// resolveByRecorded resolves an attempt that the recorded job answers: an
// Unknown attempt, or an InFlight one past the in-flight window, becomes Done.
// An InFlight attempt inside the window stays: its claimant resolves it when
// its sbatch returns, and a job adopted meanwhile (an older attempt's late
// registration) says nothing about it.
func (e *external) resolveByRecorded(ctx context.Context, a *v1alpha1.Allocation, rec *record, now time.Time) error {
	if s := rec.sub; unresolved(s) && (s.State == v1alpha1.SubmissionUnknown || now.Sub(attemptTime(a, s)) > e.inflightWindow()) {
		done := *s
		done.State, done.Writer, done.Time = v1alpha1.SubmissionDone, e.s.o.Identity, mt(now)
		done.Reason = "resolved by the recorded job " + rec.jobID
		if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(done)}); err != nil {
			return err
		}
		if err := e.reread(a, rec); err != nil {
			return err
		}
	}
	e.markResolved(a, rec.sub, now)
	return nil
}

// final reports whether the recorded job's end is final, so it is not read
// again (review of PR 76: Opus N10, Astra B5). That needs an accounting record
// with an end time, read by this controller (finals): a queue listing that
// shows an ended job, as Athena's does until MinJobAge, is not final. The
// stored status must show the end and, for a job that started, a recorded
// queue wait; a job that ended without starting has none to record. After a
// restart, finals is empty, so each ended Allocation costs one more accounting
// read.
func (e *external) final(id string, o *v1alpha1.AllocationObservation) bool {
	if _, ok := e.s.finals.Load(id); !ok {
		return false
	}
	return o.EndTime != nil && (o.StartTime == nil || o.QueueWaitRecorded)
}

// observeFinal observes an Allocation whose recorded job has ended for good:
// the job is not read again, but the tag still is, from the cached queue
// listing, so a late registration of an older attempt is still cancelled while
// held (hold mode) and still reported when not held (Astra B5).
func (e *external) observeFinal(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string) (managed.ExternalObservation, error) {
	e.p.SetBusy(name, false)
	if e.hold() {
		if err := e.cancelHeldExtras(ctx, a, rec, name); err != nil {
			return exists, err
		}
	}
	return exists, e.duplicates(ctx, a, name, rec.jobID)
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
// request, on the first successful observe of the recorded job only (HBEP-4,
// The Slurm dialect, Read-back): a later change, such as an administrator's
// extension of the time limit, is a later hard end, never a mismatch. The
// caller runs it until the read-back marker names the job (v1alpha1.AnnotationReadBack).
// A difference sets SubmittedAsOther True, reason ReasonReadBackMismatch, with
// each differing field and both values; the condition is never cleared, and
// the lifecycle controller replaces the Allocation. It reports whether the job
// matched, so the caller writes the marker; a raised condition is not a match
// and is not compared again.
func (e *external) readBack(a *v1alpha1.Allocation, job provider.Job) bool {
	if a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == corev1.ConditionTrue {
		return false
	}
	diffs := readBackDiffs(a.Spec.ForProvider.Batch, job)
	if len(diffs) == 0 {
		return true
	}
	msg := "job " + job.ID + " was established with other settings than requested: " + strings.Join(diffs, "; ")
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionSubmittedAsOther, Status: corev1.ConditionTrue,
		Reason: v1alpha1.ReasonReadBackMismatch, Message: msg, LastTransitionTime: *mt(e.now())})
	e.event(a, event.Warning("SubmittedAsOther", errors.New(msg)))
	return false
}

// markReadBack writes the read-back marker for job id. A conflict re-reads the
// Allocation from the API server and writes again, unless the marker is
// already there.
func (e *external) markReadBack(ctx context.Context, a *v1alpha1.Allocation, rec *record, id string) error {
	var err error
	for i := 0; i < 3; i++ {
		if a.Annotations[v1alpha1.AnnotationReadBack] == id {
			rec.readBack = id
			return nil
		}
		if err = write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationReadBack: id}); err == nil {
			rec.readBack = id
			return nil
		}
		if !kerrors.IsConflict(err) {
			break
		}
		if err = refresh(ctx, e.s.reader, a); err != nil {
			break
		}
	}
	return fmt.Errorf("record the read-back of job %s: %w", id, err)
}

// readBackDiffs lists the fields of job that differ from the request, each
// with both values, in the order partition, time limit, GPUs. The queue
// listing's tres_req_str always carries the GPU count, so a queue read is
// always compared. Accounting carries it in its tres lists, and a record with
// no gres entry reads as 0 GPUs (the synthetic v0.0.42 fixture has none), so
// an accounting count is compared only when it is reported, that is non-zero.
func readBackDiffs(p *v1alpha1.BatchParameters, job provider.Job) []string {
	var diffs []string
	if job.Partition != "" && job.Partition != p.Partition {
		diffs = append(diffs, fmt.Sprintf("partition: requested %q, got %q", p.Partition, job.Partition))
	}
	if want, err := p.TimeLimit.AsDuration(); err == nil && job.TimeLimit > 0 && job.TimeLimit != want {
		diffs = append(diffs, fmt.Sprintf("timeLimit: requested %s, got %s", want, job.TimeLimit))
	}
	if (job.Source == provider.SourceQueue || job.GPUs != 0) && job.GPUs != p.GPUs {
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
// tag except the recorded one (cancelHeldExtras), then lifts the recorded
// job's hold once (liftRecorded). A failed read stops settle; a lift that fails
// with an unknown error wraps errLiftFailed and the next observe tries again.
func (e *external) settle(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string, recorded provider.Job) (bool, error) {
	if err := e.cancelHeldExtras(ctx, a, rec, name); err != nil {
		return false, err
	}
	return e.liftRecorded(ctx, a, rec, recorded)
}

// cancelHeldExtras cancels every held, live job with the tag except the
// recorded one, from the queue listing (a held job is always listed).
func (e *external) cancelHeldExtras(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string) error {
	listed, err := e.p.ListByTag(ctx, name)
	if err != nil {
		return e.readFailed(err)
	}
	var extras []string
	for _, j := range listed {
		if j.ID != rec.jobID && j.Held() && j.EndTime == nil {
			extras = append(extras, j.ID)
		}
	}
	if len(extras) == 0 {
		return nil
	}
	if err := e.cancel(ctx, extras); err != nil {
		return err
	}
	e.event(a, event.Normal("HeldDuplicateCancelled", fmt.Sprintf("held job(s) %s with the tag were cancelled; job %s is the Allocation's", strings.Join(extras, ", "), rec.jobID)))
	return nil
}

// liftRecorded lifts the recorded job's hold once (HBEP-7, Settle), only if the
// hold-lifted annotation does not name it, it is held by its user (reason
// JobHeldUser: HBEP-7's closed list), has restart count 0 and has never
// started. A job established as something else (SubmittedAsOther) is lifted
// like any other, as pkg/provider/fake.Controller does, and the lifecycle
// controller replaces the Allocation. The annotation is written after the
// lift, with a conflict retry (Opus N8).
//
// What can overlap (Astra B8). Within one manager, controller-runtime's
// workqueue never runs two reconciles of one Allocation at once, and leader
// election runs one manager per Provider at a time. Two lifters can still meet
// across a handover: in one process, when the supervisor restarts a Provider's
// manager while a reconcile of the old one is still inside its release; across
// processes, when a leader stalls past its Lease and acts on after another has
// taken over. In one process, liftLocks serialises the lift of a job, and the
// lifter re-reads the marker from the API server and the job before it
// releases, so a job gets one release. Across processes that re-read narrows
// the window to one release still in flight on the platform; a second release
// then meets the same first hold, already released, and the guards keep any
// later hold from being lifted. Claiming the marker before the release would
// fence that case too, but a crash between the claim and the release would
// leave the job held for good, so HBEP-7's order stands: release, then marker.
func (e *external) liftRecorded(ctx context.Context, a *v1alpha1.Allocation, rec *record, recorded provider.Job) (bool, error) {
	if !liftable(rec, recorded) {
		return false, nil
	}
	unlock := liftLocks.lock(e.s.o.Provider + "/" + recorded.ID)
	defer unlock()
	if err := refresh(ctx, e.s.reader, a); err != nil {
		return false, fmt.Errorf("re-read the Allocation before lifting job %s: %w", recorded.ID, err)
	}
	if a.Annotations[v1alpha1.AnnotationHoldLifted] == recorded.ID {
		rec.lifted = recorded.ID
		return false, nil
	}
	job, found, err := e.p.Observe(ctx, recorded.ID)
	if err != nil {
		return false, e.readFailed(err)
	}
	if !found || !liftable(rec, job) {
		return false, nil
	}
	err = e.p.LiftHold(ctx, recorded.ID)
	switch {
	case errors.Is(err, provider.ErrNotHeld), errors.Is(err, provider.ErrAdminHold), errors.Is(err, provider.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("%w for job %s: %v", errLiftFailed, recorded.ID, err)
	}
	if err := e.markLifted(ctx, a, rec, recorded.ID); err != nil {
		return true, err
	}
	e.event(a, event.Normal("HoldLifted", fmt.Sprintf("the hold of job %s was lifted", recorded.ID)))
	return true, nil
}

// liftable is HBEP-7's closed list for the lift: no hold-lifted marker for the
// job, a user hold with reason JobHeldUser (HoldOf also calls a priority-zero
// hold with another JobHeld reason a user hold), restart count 0, never
// started.
func liftable(rec *record, j provider.Job) bool {
	return rec.lifted != j.ID && j.Hold == provider.HoldUser && j.Reason == provider.ReasonHeldUser &&
		j.RestartCount == 0 && j.StartTime == nil
}

// markLifted writes the hold-lifted marker after a lift. A conflict re-reads
// the Allocation from the API server and writes again: until the marker is
// stored, only the job's state (released, so no longer held) guards against a
// second lift.
func (e *external) markLifted(ctx context.Context, a *v1alpha1.Allocation, rec *record, id string) error {
	var err error
	for i := 0; i < 5; i++ {
		if a.Annotations[v1alpha1.AnnotationHoldLifted] == id {
			rec.lifted = id
			return nil
		}
		if err = write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationHoldLifted: id}); err == nil {
			rec.lifted = id
			return nil
		}
		if !kerrors.IsConflict(err) {
			break
		}
		if err = refresh(ctx, e.s.reader, a); err != nil {
			break
		}
	}
	return fmt.Errorf("record the hold lift of job %s: %w", id, err)
}

// liftLocks serialises the lift of one job within this process, across the
// Provider's managers (liftRecorded).
var liftLocks = &keyedMutex{m: map[string]*keyedEntry{}}

type keyedMutex struct {
	mu sync.Mutex
	m  map[string]*keyedEntry
}

type keyedEntry struct {
	mu    sync.Mutex
	users int
}

// lock locks key and returns its unlock; an entry lives while it is used.
func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	en := k.m[key]
	if en == nil {
		en = &keyedEntry{}
		k.m[key] = en
	}
	en.users++
	k.mu.Unlock()
	en.mu.Lock()
	return func() {
		en.mu.Unlock()
		k.mu.Lock()
		if en.users--; en.users == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
}

// duplicates records extra jobs with the tag that are not held, in the sticky
// duplicates list, with a Warning event when the list grows. A job that ended
// without starting is no duplicate: it never ran, and Athena's listing shows a
// held extra that settle cancelled in that state, not held (Opus N9).
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
		if j.ID != recorded && !j.Held() && !have[j.ID] && (j.EndTime == nil || j.StartTime != nil) {
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

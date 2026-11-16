package providerctl

// What a pass changed, as typed Allocation events (beta; HBEP-8, Allocation
// events (beta); #625). Each is computed against the stored status (prior) in
// the pass that first sees the change, and raised before the status write that
// records it, as HBEP-7 orders a restart's event before its generation write. A
// failed write leaves the stored status as it was, so the next pass computes the
// change again and raises the event again under the same id, which is the
// marking fact in the id (typed).
//
// After a restart the first pass compares the job with the stored status, so a
// change made while the controller was down is announced late, and the steps in
// between are not replayed (HBEP-8, After a restart).

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// hardEndWarning is how long before a running job's hard end HardEndNear is
// raised.
const hardEndWarning = time.Hour

// completingEventAfter is how long a job carries COMPLETING before JobCompleting
// is raised: ten minutes are far past a normal cleanup (HBEP-8, Allocation
// events, Repeating conditions; S7 saw the flag for one 2 s poll).
const completingEventAfter = 10 * time.Minute

// endedAtHardEnd reports whether a job ended at or after its hard end, as the
// stored status shows the times (to the second). A job with no hard end or no
// end time did not.
func endedAtHardEnd(end, hard *time.Time) bool {
	return end != nil && hard != nil && !end.UTC().Truncate(time.Second).Before(hard.UTC().Truncate(time.Second))
}

// drainedAtEnd reports whether an Instance of the Allocation is Draining, for a
// job that this pass sees end before its hard end for the first time: only then
// does the answer decide between JobEnded and AllocationEndedEarly. A read that
// fails is an error, so the pass keeps the stored status and the next one
// decides again, never calling a planned end a failure.
func (e *external) drainedAtEnd(ctx context.Context, a *v1alpha1.Allocation, prior *v1alpha1.AllocationObservation, job provider.Job) (bool, error) {
	if job.Phase != v1alpha1.AllocationEnded || prior.Phase == v1alpha1.AllocationEnded || prior.Requeued || job.Requeued {
		return false, nil
	}
	if endedAtHardEnd(job.EndTime, job.HardEnd) {
		return false, nil
	}
	drained, err := e.s.draining(ctx, a)
	if err != nil {
		return false, fmt.Errorf("read the Instance of the Allocation, to tell a planned end from an early one: %w", err)
	}
	return drained, nil
}

// transitions raises the typed events of the change the pass made to a's
// status, against prior, the stored status, after the job was mirrored into it.
func (e *external) transitions(a *v1alpha1.Allocation, prior *v1alpha1.AllocationObservation, rec record, job provider.Job, drained bool) {
	o := &a.Status.AtProvider
	if o.Phase == v1alpha1.AllocationRefused {
		// Refused is kept once set, and the rest of what the job did is not
		// announced for an Allocation that is refused.
		if prior.Phase != v1alpha1.AllocationRefused {
			e.typed(a, v1alpha1.ReasonAllocationRefused, attemptFact(rec), "the Allocation is refused"+reasonSuffix(o.Reason), nil)
		}
		return
	}
	if prior.Phase == v1alpha1.AllocationLost {
		e.typed(a, evJobReappeared, factTime(timeOf(prior.AbsentSince)),
			fmt.Sprintf("the job shows again in the %s as %s, after being reported Lost; it was absent since %s", job.Source, o.Phase, factTime(timeOf(prior.AbsentSince))), nil)
	}
	if prior.Phase != v1alpha1.AllocationRunning && o.Phase == v1alpha1.AllocationRunning {
		e.typed(a, evJobStarted, strconv.Itoa(int(o.RestartCount)), e.startedNote(a, job), nil)
	}
	switch {
	case !prior.Requeued && o.Requeued:
		e.typed(a, v1alpha1.ReasonRequeued, strconv.Itoa(int(o.RestartCount)),
			fmt.Sprintf("the job was requeued; its restart count is %d and its base state %s", o.RestartCount, baseState(job)), nil)
	case o.Phase == v1alpha1.AllocationFailed && prior.Phase != v1alpha1.AllocationFailed && !o.Requeued:
		e.typed(a, v1alpha1.ReasonAllocationFailed, factTime(job.EndTime),
			"the job failed in state "+baseState(job)+reasonSuffix(job.Reason), nil)
	}
	if o.Phase == v1alpha1.AllocationEnded && prior.Phase != v1alpha1.AllocationEnded {
		e.endedLive(a, job, drained)
	}
	if o.Phase == v1alpha1.AllocationRunning && o.HardEnd != nil && !e.now().Before(o.HardEnd.Add(-hardEndWarning)) &&
		e.s.gates.hardEndDue(a.UID, o.HardEnd.Time) {
		e.typed(a, evHardEndNear, factTime(&o.HardEnd.Time),
			fmt.Sprintf("the job's hard end is %s, within an hour", factTime(&o.HardEnd.Time)), nil)
	}
	if job.Hold == provider.HoldAdmin {
		if e.s.gates.hourlyDue(gateAdminHold, a.UID, e.now()) {
			e.typedUnmarked(a, evAdminHold, "the job is held by an administrator"+reasonSuffix(job.Reason)+"; the provider never lifts that hold", nil)
		}
	} else {
		e.s.gates.clearHourly(gateAdminHold, a.UID)
	}
}

// endedLive raises the event of a job that this pass sees end: JobEnded when it
// ended at or after its hard end or while its Instance drains, a planned end
// whatever Slurm's state (TIMEOUT, COMPLETED or CANCELLED), and
// AllocationEndedEarly when it ended before its hard end with no Instance of it
// draining: ended from outside the framework, or its agent exited early (HBEP-8,
// Allocation events, Planned or not). No deletion was asked for, or this would
// be a deleting pass (endedWhileDeleting).
func (e *external) endedLive(a *v1alpha1.Allocation, job provider.Job, drained bool) {
	fact := factTime(job.EndTime)
	switch {
	case endedAtHardEnd(job.EndTime, job.HardEnd):
		e.typed(a, evJobEnded, fact, fmt.Sprintf("the job ended in state %s at %s, at or after its hard end", baseState(job), fact), nil)
	case drained:
		e.typed(a, evJobEnded, fact, fmt.Sprintf("the job ended in state %s at %s, while its Instance drains", baseState(job), fact), nil)
	default:
		e.typed(a, v1alpha1.ReasonAllocationEndedEarly, fact,
			fmt.Sprintf("the job ended in state %s at %s, before its hard end, with no Instance of it draining", baseState(job), fact), nil)
	}
}

// endedWhileDeleting raises JobEnded for an Allocation that is being deleted, in
// the deleting pass that first sees the recorded job's end time. A deleting
// pass writes no phase (observeDeleting), so the phase never turns Ended while
// the Allocation is deleted: the end time is the trigger (HBEP-8, Allocation
// events, the erratum for #625; R3).
//
// It raises nothing when the stored phase already announced an end or a
// failure (Ended, Failed, Lost or Refused), or the Allocation carries
// SubmittedAsOther or RelayEndedForGood, the failures the lifecycle controller
// releases for (HBEP-7, Reconcile loop, step 5): a deletion that follows a
// failure raises no end event, since its failing event has fired. And once per
// process: the gate is keyed by the Allocation's UID and dropped when its
// finalizer goes, so a restart in the middle of a deletion raises it once more
// under the same id.
func (e *external) endedWhileDeleting(a *v1alpha1.Allocation, rec record, jobs []provider.Job) {
	if rec.jobID == "" {
		return
	}
	var job provider.Job
	found := false
	for _, j := range jobs {
		if j.ID == rec.jobID {
			job, found = j, true
		}
	}
	if !found || job.EndTime == nil || endAnnounced(a) {
		return
	}
	if !e.s.gates.endedOnce(a.UID) {
		return
	}
	end := factTime(job.EndTime)
	note := fmt.Sprintf("the job ended in state %s at %s while the Allocation was being deleted", baseState(job), end)
	var related runtime.Object
	if cause := a.Annotations[v1alpha1.AnnotationReleaseCause]; cause != "" {
		note += "; release cause: " + plainCause(cause, rec.jobID)
		related = operationOf(cause)
	}
	e.typed(a, evJobEnded, end, note, related)
}

// endAnnounced reports whether the Allocation's stored status says an end or a
// failure was already announced.
func endAnnounced(a *v1alpha1.Allocation) bool {
	switch a.Status.AtProvider.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true
	}
	return a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == "True" ||
		a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == "True"
}

// startedNote is JobStarted's note: the start time, the hard end, the queue wait
// and the allocated GPUs and CPUs, without the job ID. A count the scheduler's
// accounting has not given is unknown, never 0.
func (e *external) startedNote(a *v1alpha1.Allocation, job provider.Job) string {
	gpus, cpus := "unknown", "unknown"
	if acc := a.Status.AtProvider.Accounting; acc != nil {
		if acc.AllocatedGPUs != nil {
			gpus = strconv.Itoa(int(*acc.AllocatedGPUs))
		}
		if acc.AllocatedCPUs != nil {
			cpus = strconv.Itoa(int(*acc.AllocatedCPUs))
		}
	}
	wait := "not a scheduler wait"
	if a.Annotations[v1alpha1.AnnotationQueuedSuccessor] == "" && job.StartTime != nil && !job.SubmitTime.IsZero() {
		wait = job.StartTime.Sub(job.SubmitTime).Truncate(time.Second).String()
	}
	return fmt.Sprintf("the job started at %s; its hard end is %s; queue wait %s; %s GPUs and %s CPUs allocated",
		factTime(job.StartTime), factTime(job.HardEnd), wait, gpus, cpus)
}

// timeOf is the time of a status time, nil for none.
func timeOf(m *metav1.Time) *time.Time {
	if m == nil {
		return nil
	}
	t := m.Time
	return &t
}

// reasonSuffix is ": <reason>" for a scheduler's state reason that is one bare
// word (NonZeroExitCode, JobHeldAdmin), and "" for anything else: a state reason
// can carry a node list or free text, and a note names no text that comes from
// outside the controller (HBEP-8, Allocation events, No job ID, path or name in a
// note), so it stays in the Allocation's status.
func reasonSuffix(reason string) string {
	if reasonCode.MatchString(reason) {
		return ": " + reason
	}
	return ""
}

// reasonCode matches one bare word.
var reasonCode = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// plainCause is the release cause as the note carries it. An Operation's
// cause, in the form the executor writes, stays whole. Any other cause stays
// when it is plain words, with the job ID taken out as a token, and a cause that
// holds a path separator is not copied: a person's text can name a path, and a
// note names none (HBEP-8, Allocation events, No job ID, path or name in a note).
func plainCause(cause, jobID string) string {
	if operationOf(cause) != nil {
		return cause
	}
	if strings.ContainsAny(cause, `/\`) {
		return "(not plain text; the Allocation's release-cause annotation has it)"
	}
	return redactToken(cause, jobID, "the job")
}

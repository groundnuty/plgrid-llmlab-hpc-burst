package lifecycle

// The report of an executing Operation (HBEP-10, The report): one outcome per committed target, and when the Operation is
// Done. Split from opexec.go, which holds the stale check, the commit and the acts.

import (
	"fmt"
	"strings"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// noActDone is the ONE place that says when a committed target the controller does not act on by itself is Done
// (the science agent's ruling on #589, 16 Nov 2026, reading HBEP-10, The report):
//   - Keep is done at the commit: there is no act, and its later release by retention carries retention's own cause;
//   - SetEndTime is done once the spec write has landed (specWritten), or the deployment is gone;
//   - DeleteDeployment is done once the ModelDeployment is gone.
//
// Every other effect has an act, and is done by what the act leaves, never by this.
func noActDone(effect v1alpha1.PlanEffect, specWritten, deploymentGone bool) bool {
	switch effect {
	case v1alpha1.PlanEffectKeep:
		return true
	case v1alpha1.PlanEffectSetEndTime:
		return specWritten || deploymentGone
	case v1alpha1.PlanEffectDeleteDeployment:
		return deploymentGone
	}
	return false
}

// reasonJobStartedSinceCommit is the reason of a committed CancelPending Allocation target whose job started between the
// commit and the act (HBEP-10, The report, as amended by #665 and #666: the science agent's rulings on #664, 16 and 17 Nov 2026).
const reasonJobStartedSinceCommit = "JobStartedSinceCommit"

// reasonSpecChangedSinceCommit is the reason of a target the Operation did not act on because the deployment's spec changed
// after the commit so that the effect it was approved for no longer holds (specEffectHolds).
const reasonSpecChangedSinceCommit = "SpecChangedSinceCommit"

// reasonLastReadySinceCommit is the reason of a committed CancelPending or EndStarting Instance of a Scale that is Ready now
// and would be the last ready one: draining it is a DrainLastReady that nobody approved.
const reasonLastReadySinceCommit = "LastReadySinceCommit"

// failureIsFinal is the ONE function that says which failures of a target are final (HBEP-10, The report, as amended by
// #665 and #666): a failure that no retry can fix, because it is the approved effect that cannot apply any more. A refused cancel and
// the controller's own write's error are retried with backoff; a job that started after it was approved for cancelling
// in the queue is never released under that approval, an Instance that has become the last ready one is never drained under
// an approval that did not cover it, and nothing is acted on once the spec effect no longer holds. phaseOf says what a final
// failure does to the Operation.
func failureIsFinal(reason string) bool {
	switch reason {
	case reasonJobStartedSinceCommit, reasonSpecChangedSinceCommit, reasonLastReadySinceCommit:
		return true
	}
	return false
}

// phaseOf is the ONE function that says what the outcomes of the committed targets make of the Operation, and whether it is
// finished (HBEP-10, The report, as amended by #665 and #666): Done when every target is done; PartlyFailed and finished (finishedAt
// is set, so the deployment is free for the next Operation) when every target is
// done or has failed for good; PartlyFailed and not finished while a failure that a retry can fix remains, which is
// retried; and Executing while none has failed.
func phaseOf(outcomes []v1alpha1.TargetOutcome) (phase v1alpha1.OperationPhase, finished bool) {
	done, final, failed := 0, 0, 0
	for _, o := range outcomes {
		switch o.State {
		case v1alpha1.TargetStateDone:
			done++
		case v1alpha1.TargetStateFailed:
			failed++
			if failureIsFinal(o.Reason) {
				final++
			}
		}
	}
	switch {
	case done == len(outcomes):
		return v1alpha1.OperationPhaseDone, true
	case failed == 0:
		return v1alpha1.OperationPhaseExecuting, false
	case done+final == len(outcomes):
		return v1alpha1.OperationPhasePartlyFailed, true
	}
	return v1alpha1.OperationPhasePartlyFailed, false
}

// targetDone reports whether a committed target is done, as the report says it: an Instance that has ended or is gone, an
// Allocation that is gone (its finalizer cancels every tagged job first, so a delete that is held is not done), and a
// target with no act by noActDone. A committed Allocation the controller released by its own rule meanwhile is gone, so it
// is done too: a failed Instance's, released by tombstone() at once, which the plan listed conservatively as Keep.
func (p *pass) targetDone(t v1alpha1.CommittedTarget, specWritten bool) (bool, error) {
	switch t.Kind {
	case v1alpha1.TargetKindInstance:
		in := p.instanceByUID(t.UID)
		return in == nil || ended(in), nil
	case v1alpha1.TargetKindAllocation:
		if !releasingEffect(t.Effect) {
			return noActDone(t.Effect, specWritten, false), nil
		}
		a, err := p.committedAllocation(t)
		return a == nil, err
	case v1alpha1.TargetKindModelDeployment:
		// Gone, or being deleted under the kept-warm finalizer: the delete was made and waits for the warm Allocations, which a
		// new Operation's Release may then free, so the End must not hold the deployment's one-at-a-time slot meanwhile.
		gone := p.md == nil || p.md.UID != t.UID || (!p.md.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(p.md, v1alpha1.FinalizerKeptWarm))
		return noActDone(t.Effect, specWritten, gone), nil
	}
	return false, nil
}

// reasonWriteFailed is the reason of a target whose act failed on the controller's own write.
const reasonWriteFailed = "WriteFailed"

// reasonReleaseFailed is the reason of a released Allocation that the Provider's controller could not finish deleting,
// when its Synced condition names none.
const reasonReleaseFailed = "ReleaseFailed"

// targetFailure says why a committed target is Failed, and ok is false when it is not (HBEP-10, The report). A target
// fails by an error of the controller's own write, or, for a released Allocation, by being present with its deletion
// asked for before this pass and the Provider's controller reporting Synced=False: it cannot finish the delete (the
// finalizer cancels every job with the tag first, and a cancel that fails keeps the Allocation, which crossplane retries
// with backoff). The message is the condition's, cut to the events' note bound.
func (p *pass) targetFailure(t v1alpha1.CommittedTarget, own error, deleting map[types.UID]bool) (reason, message string, ok bool, err error) {
	if own != nil {
		return reasonWriteFailed, eventNote(own.Error()), true, nil
	}
	if t.Kind != v1alpha1.TargetKindAllocation || !releasingEffect(t.Effect) || !deleting[t.UID] {
		return "", "", false, nil
	}
	a, err := p.committedAllocation(t)
	if err != nil || a == nil {
		return "", "", false, err
	}
	c := a.GetCondition(xpv2.TypeSynced)
	if c.Status != corev1.ConditionFalse {
		return "", "", false, nil
	}
	reason = string(c.Reason)
	if reason == "" {
		reason = reasonReleaseFailed
	}
	return reason, eventNote(c.Message), true, nil
}

// deletingBeforeThePass returns the committed Allocations that were being deleted when this pass began: the pass's own
// release of an Allocation sets its deletion time on the object it holds, and must not read as one the Provider has
// failed to finish.
func (p *pass) deletingBeforeThePass(op *v1alpha1.Operation) map[types.UID]bool {
	out := map[types.UID]bool{}
	for _, t := range op.Status.Committed {
		if t.Kind != v1alpha1.TargetKindAllocation || !releasingEffect(t.Effect) {
			continue
		}
		if a, err := p.committedAllocation(t); err == nil && a != nil && !a.DeletionTimestamp.IsZero() {
			out[t.UID] = true
		}
	}
	return out
}

// finalFailure reports whether a target has failed for good, with its reason and message: a committed CancelPending
// Allocation whose job started since the commit (it is never released under that approval, which was to cancel a queued job
// and not a running one, and is left to retention); a committed CancelPending or EndStarting Instance of a Scale that is
// Ready now and would be the last ready one (not drained, since nobody approved that); and a releasing Allocation target that
// such an Instance keeps from ever being empty. Once it is recorded in status it stays: the Allocation's later release by
// retention is not the approved act. A last-ready failure is recorded ONLY by a pass that acts (acted): on a pass whose spec
// write failed or did not land, nothing was or could be drained, and the Instance that is the last ready one now may not be
// the last one by the time the pass does act; recording it there would freeze a passing state as final (HBEP-10, The report,
// as amended by #665, #666 and #668). What is recorded stays, and nothing acts on it again (recordedFinal).
func (p *pass) finalFailure(op *v1alpha1.Operation, t v1alpha1.CommittedTarget, prior *v1alpha1.TargetOutcome, acted bool) (reason, message string, ok bool, err error) {
	if prior != nil && prior.State == v1alpha1.TargetStateFailed && failureIsFinal(prior.Reason) {
		return prior.Reason, prior.Message, true, nil
	}
	switch t.Kind {
	case v1alpha1.TargetKindInstance:
		if acted && p.lastReadyScaleTarget(op, t, p.instanceByUID(t.UID)) {
			return reasonLastReadySinceCommit, lastReadyMessage, true, nil
		}
		if in := p.instanceByUID(t.UID); acted && in != nil && !ended(in) && scaleAboveZero(op) && startedSinceCommit(t, p.allocs[in.Spec.AllocationRef.Name]) {
			return reasonJobStartedSinceCommit, "its job started after the Operation was committed, and the approval covered cancelling a queued one: under a Scale above zero the Instance is not taken, ending it would be a release nobody approved, and it keeps starting and serving; its count is given back", true, nil
		}
	case v1alpha1.TargetKindAllocation:
		a, err := p.committedAllocation(t)
		if err != nil || a == nil {
			return "", "", false, err
		}
		if startedSinceCommit(t, a) {
			// The Operation never releases a job it sees started: the approval was to cancel a queued one. It carried the mark since
			// before the first act (markBeforeActing), so the outcome records what the Operation did, and a later clearing of the mark
			// does not change it (HBEP-10, The report, Kept warm).
			return reasonJobStartedSinceCommit, "its job started after the Operation was committed, and the approval covered cancelling a queued job, which is priced differently from releasing a running one: the Operation did not release it, and it was kept warm, marked before the Operation acted. Only the profile's maxIdle, its hard end, a failure's release, a person's delete while no finalizer holds, or a new approved Release releases a marked, started Allocation, until an Instance is placed on it or, under a Scale above zero, its Instance serves on once the Operation has finished, and the mark is cleared. A maxIdle the profile sets still applies, unpriced", true, nil
		}
		if acted && releasingEffect(t.Effect) {
			// An Instance that is a committed target and has failed for good stays live, so this Allocation is never empty.
			for _, c := range op.Status.Committed {
				if c.Kind == v1alpha1.TargetKindInstance && p.instanceByUID(c.UID) != nil && p.instanceByUID(c.UID).Spec.AllocationRef.Name == a.Name &&
					p.lastReadyScaleTarget(op, c, p.instanceByUID(c.UID)) {
					return reasonLastReadySinceCommit, "the Instance on it, a target of this Operation, became the last ready one and was not drained, so the Allocation never empties: " + lastReadyMessage, true, nil
				}
			}
		}
	}
	return "", "", false, nil
}

// lastReadyMessage says why a committed Instance was not drained.
const lastReadyMessage = "it became ready after the Operation was committed and is now the last ready Instance, so draining it would be a drain of the last ready Instance, which the approval did not cover: the Operation did not drain it"

// outcomes is the report: one entry per committed target, Done, Failed or NotStarted, with the time its state last changed
// kept while it does not. A target fails by a final failure (finalFailure) or by one that is retried (targetFailure).
// specWritten is whether the pass applied the spec and so acted: only then can a last-ready failure be recorded.
func (p *pass) outcomes(op *v1alpha1.Operation, failed map[types.UID]error, deleting map[types.UID]bool, specWritten bool) (out []v1alpha1.TargetOutcome, err error) {
	prior := map[types.UID]v1alpha1.TargetOutcome{}
	for _, o := range op.Status.Targets {
		prior[o.UID] = o
	}
	for _, t := range op.Status.Committed {
		o := v1alpha1.TargetOutcome{Kind: t.Kind, Name: t.Name, UID: t.UID, State: v1alpha1.TargetStateNotStarted, LastTransitionTime: metav1.NewTime(p.now)}
		var was *v1alpha1.TargetOutcome
		if w, ok := prior[t.UID]; ok {
			was = &w
		}
		// A final failure first: a target is never reported done when its approved act did not happen, so an Allocation that
		// started and that retention then released is still the failure it was.
		reason, msg, bad, err := p.finalFailure(op, t, was, specWritten)
		if err != nil {
			return nil, err
		}
		if !bad {
			done, err := p.targetDone(t, specWritten)
			if err != nil {
				return nil, err
			}
			if done {
				o.State = v1alpha1.TargetStateDone
				if t.Kind == v1alpha1.TargetKindAllocation && releasingEffect(t.Effect) {
					o.Message = p.releasedByMessage(t, was)
				}
			} else if reason, msg, bad, err = p.targetFailure(t, failed[t.UID], deleting); err != nil {
				return nil, err
			} else if !bad && t.Kind == v1alpha1.TargetKindAllocation && releasingEffect(t.Effect) {
				o.Message = p.failedAllocationMessage(t)
			}
		}
		if bad {
			o.State, o.Reason, o.Message = v1alpha1.TargetStateFailed, reason, msg
		}
		if was != nil && was.State == o.State {
			o.LastTransitionTime = was.LastTransitionTime
		}
		out = append(out, o)
	}
	return out, nil
}

// releasedByMessage is the message of a releasing Allocation target that is gone: when a failure released it first (an
// Instance on it ended with a failure reason: a gate miss, RestartLoop, an Allocation failed or lost), the report names
// that failure, and never says the Operation released it (HBEP-10, The report, as amended by the science agent's ruling on
// #664, 17 Nov 2026). Once said, it is kept while the Instance's tombstone goes. It is empty when the Operation, or the
// controller's own scale-down or retention, released it.
func (p *pass) releasedByMessage(t v1alpha1.CommittedTarget, was *v1alpha1.TargetOutcome) string {
	if reason, instance, ok := p.failureOn(t.Name); ok {
		return eventNote(fmt.Sprintf("released by %s: Instance %s failed, and its failure released the Allocation before the Operation did, so the release cause is the failure's", reason, instance))
	}
	if was != nil && strings.HasPrefix(was.Message, "released by ") {
		return was.Message // said while it was going (failedAllocationMessage), kept once it is gone
	}
	return ""
}

// failedAllocationMessage is the message of a releasing Allocation target whose job failed or was lost, and that is not gone
// yet: retention releases it by its own reason, which the report names, and the Operation does not (releasePending).
func (p *pass) failedAllocationMessage(t v1alpha1.CommittedTarget) string {
	if a, err := p.committedAllocation(t); err == nil && a != nil {
		if reason, ok := failureOfAllocation(a); ok {
			return eventNote(fmt.Sprintf("released by %s: its job failed or was lost, so retention releases the Allocation by that reason, and the release cause is retention's, not the Operation's", reason))
		}
	}
	return ""
}

// failureLines says each failed target of the report in a line: its kind, name, reason and message.
func failureLines(out []v1alpha1.TargetOutcome) []string {
	var lines []string
	for _, o := range out {
		if o.State == v1alpha1.TargetStateFailed {
			lines = append(lines, fmt.Sprintf("%s %s (%s): %s", o.Kind, o.Name, o.Reason, o.Message))
		}
	}
	return lines
}

// partlyFailedText is what the OperationPartlyFailed event says: each failed target's line, or, for an Operation with none (a Scale
// of no target that a person's edit stopped), the reason itself.
func partlyFailedText(out []v1alpha1.TargetOutcome) string {
	if lines := failureLines(out); len(lines) > 0 {
		return strings.Join(lines, "; ")
	}
	return reasonSpecChangedSinceCommit + ": the deployment's spec changed after the Operation was committed, so that the effect it was approved for no longer holds"
}

// raisePartlyFailed raises the OperationPartlyFailed event of an Operation that is PartlyFailed: when it starts, then at
// most once an hour while it lasts, whichever way it came to be (a retried failure, a final one, or a person's edit that
// stopped it: failSpecChanged). The next phase that is not PartlyFailed clears it.
func (p *pass) raisePartlyFailed(op *v1alpha1.Operation, phase v1alpha1.OperationPhase, out []v1alpha1.TargetOutcome) {
	key := "partly-failed/" + string(op.UID)
	if phase != v1alpha1.OperationPhasePartlyFailed {
		p.mem.clear(key)
		return
	}
	if p.mem.repeat(key, p.now) {
		p.r.Recorder.Event(op, nil, eventFailing, v1alpha1.ReasonOperationPartlyFailed,
			fmt.Sprintf("Operation %s/%s is partly failed: %s", op.Namespace, op.Name, partlyFailedText(out)))
	}
}

// report writes status.targets and the phase phaseOf gives: PartlyFailed while any target is Failed, finished (finishedAt set)
// when the rest are done and the failures are final, with OperationPartlyFailed raised when it starts and then at most once
// an hour while it lasts; Executing again once none is Failed; and Done, with its finish time, once every target is done.
// specWritten is whether the spec write has landed: an Operation is not Done before it has.
func (p *pass) report(op *v1alpha1.Operation, failed map[types.UID]error, deleting map[types.UID]bool, specWritten bool) error {
	targets, err := p.outcomes(op, failed, deleting, specWritten)
	if err != nil {
		return err
	}
	failures := failureLines(targets)
	phase, finished := phaseOf(targets)
	givingBack := finished && op.Status.CountGivenBack == nil && p.untakenUnits(op, targets) > 0
	if givingBack {
		finished = false // the Operation finishes only after the record of its give-back lands (giveBack)
	}
	if phase == v1alpha1.OperationPhaseDone && !specWritten {
		phase, finished = v1alpha1.OperationPhaseExecuting, false // a Scale whose targets are all done is not Done while its spec write has not landed
	}
	p.raisePartlyFailed(op, phase, targets)
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Targets = targets
		s.Phase = phase
		switch {
		case phase == v1alpha1.OperationPhaseDone:
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone, "every committed target is done")
		case givingBack:
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionTrue, v1alpha1.ReasonExecuting, "every committed target is done or has failed for good, and the count the untaken units hold is given back before the Operation finishes")
		case finished:
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone, "every committed target is done or has failed for good: the Operation is finished")
		case phase == v1alpha1.OperationPhasePartlyFailed:
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionTrue, v1alpha1.ReasonExecuting,
				fmt.Sprintf("%d of %d committed targets failed; those a retry can fix are retried with backoff", len(failures), len(targets)))
		default:
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionTrue, v1alpha1.ReasonExecuting,
				fmt.Sprintf("committed to %d targets: the controller acts on this list and on nothing else", len(targets)))
		}
		if finished && s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

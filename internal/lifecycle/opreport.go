package lifecycle

// The report of an executing Operation (HBEP-10, The report): one outcome per committed target, and when the Operation is
// Done. Split from opexec.go, which holds the stale check, the commit and the acts.

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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
// commit and the act (HBEP-10, The report, as amended by #665: the science agent's ruling on #664, 16 Nov 2026).
const reasonJobStartedSinceCommit = "JobStartedSinceCommit"

// failureIsFinal is the ONE function that says which failures of a target are final (HBEP-10, The report, as amended by
// #665): a failure that no retry can fix, because it is the approved effect that cannot apply any more. A refused cancel and
// the controller's own write's error are retried with backoff; a job that started after it was approved for cancelling
// in the queue is never released under that approval. phaseOf says what a final failure does to the Operation.
func failureIsFinal(reason string) bool { return reason == reasonJobStartedSinceCommit }

// phaseOf is the ONE function that says what the outcomes of the committed targets make of the Operation, and whether it is
// finished (HBEP-10, The report, as amended by #665): Done when every target is done; PartlyFailed and finished (finishedAt
// is set, so the deployment is free for the next Operation and the Operation is removed 7 days later) when every target is
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

// targetDone reports whether a committed target is done, as the report says it. The spec write has landed by the
// time the report runs (executing reports only after applySpec).
func (p *pass) targetDone(t v1alpha1.CommittedTarget) (bool, error) {
	switch t.Kind {
	case v1alpha1.TargetKindInstance:
		in := p.instanceByUID(t.UID)
		return in == nil || ended(in), nil
	case v1alpha1.TargetKindAllocation:
		if !releasingEffect(t.Effect) {
			return noActDone(t.Effect, true, false), nil
		}
		a, err := p.committedAllocation(t)
		return a == nil, err
	case v1alpha1.TargetKindModelDeployment:
		return noActDone(t.Effect, true, p.md == nil || p.md.UID != t.UID), nil
	}
	return false, nil
}

// finalFailure reports whether a target has failed for good, with its reason and message: a committed CancelPending
// Allocation whose job started since the commit. It is never released under that approval, which was to cancel a queued job
// and not a running one, and is left to retention. Once it is recorded in status it stays: the Allocation's later release
// by retention is not the approved act.
func (p *pass) finalFailure(t v1alpha1.CommittedTarget, prior *v1alpha1.TargetOutcome) (reason, message string, ok bool, err error) {
	if prior != nil && prior.State == v1alpha1.TargetStateFailed && failureIsFinal(prior.Reason) {
		return prior.Reason, prior.Message, true, nil
	}
	if t.Kind != v1alpha1.TargetKindAllocation || t.Effect != v1alpha1.PlanEffectCancelPending {
		return "", "", false, nil
	}
	a, err := p.committedAllocation(t)
	if err != nil || !startedSinceCommit(t, a) {
		return "", "", false, err
	}
	return reasonJobStartedSinceCommit, "its job started after the Operation was committed, and the approval covered cancelling a queued job, which is priced differently from releasing a running one: the Operation did not release it, and its retention policy now keeps or releases it", true, nil
}

// outcomes is the report: one entry per committed target, Done, Failed for good or NotStarted, with the time its state last
// changed kept while it does not.
func (p *pass) outcomes(op *v1alpha1.Operation) ([]v1alpha1.TargetOutcome, error) {
	prior := map[types.UID]v1alpha1.TargetOutcome{}
	for _, o := range op.Status.Targets {
		prior[o.UID] = o
	}
	var out []v1alpha1.TargetOutcome
	for _, t := range op.Status.Committed {
		o := v1alpha1.TargetOutcome{Kind: t.Kind, Name: t.Name, UID: t.UID, State: v1alpha1.TargetStateNotStarted, LastTransitionTime: metav1.NewTime(p.now)}
		var was *v1alpha1.TargetOutcome
		if w, ok := prior[t.UID]; ok {
			was = &w
		}
		// A final failure first: a target is never reported done when its approved act did not happen, so an Allocation that
		// started and that retention then released is still the failure it was.
		reason, msg, failed, err := p.finalFailure(t, was)
		if err != nil {
			return nil, err
		}
		if failed {
			o.State, o.Reason, o.Message = v1alpha1.TargetStateFailed, reason, msg
		} else {
			done, err := p.targetDone(t)
			if err != nil {
				return nil, err
			}
			if done {
				o.State = v1alpha1.TargetStateDone
			}
		}
		if was != nil && was.State == o.State {
			o.LastTransitionTime = was.LastTransitionTime
		}
		out = append(out, o)
	}
	return out, nil
}

// report writes status.targets and the phase phaseOf gives: Done, with its finish time, when every target is done;
// PartlyFailed with its finish time when the rest are done and the failures are final.
func (p *pass) report(op *v1alpha1.Operation) error {
	targets, err := p.outcomes(op)
	if err != nil {
		return err
	}
	phase, finished := phaseOf(targets)
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Targets = targets
		switch phase {
		case v1alpha1.OperationPhaseDone:
			s.Phase = phase
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone, "every committed target is done")
		case v1alpha1.OperationPhasePartlyFailed:
			s.Phase = phase
			if finished {
				p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone, "every committed target is done or has failed for good: the Operation is finished")
			}
		}
		if finished && s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

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

// outcomes is the report: one entry per committed target, Done or NotStarted, with the time its state last
// changed kept while it does not. all is true when every target is done.
func (p *pass) outcomes(op *v1alpha1.Operation) (out []v1alpha1.TargetOutcome, all bool, err error) {
	prior := map[types.UID]v1alpha1.TargetOutcome{}
	for _, o := range op.Status.Targets {
		prior[o.UID] = o
	}
	all = true
	for _, t := range op.Status.Committed {
		done, err := p.targetDone(t)
		if err != nil {
			return nil, false, err
		}
		state := v1alpha1.TargetStateNotStarted
		if done {
			state = v1alpha1.TargetStateDone
		}
		all = all && done
		o := v1alpha1.TargetOutcome{Kind: t.Kind, Name: t.Name, UID: t.UID, State: state, LastTransitionTime: metav1.NewTime(p.now)}
		if was, ok := prior[t.UID]; ok && was.State == state {
			o.LastTransitionTime = was.LastTransitionTime
		}
		out = append(out, o)
	}
	return out, all, nil
}

// report writes status.targets, records that the spec write has landed, and finishes the Operation (Done, with its
// finish time) when every target is done.
func (p *pass) report(op *v1alpha1.Operation) error {
	targets, all, err := p.outcomes(op)
	if err != nil {
		return err
	}
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Targets = targets
		p.setCondition(s, op, v1alpha1.ConditionSpecWritten, metav1.ConditionTrue, "Written", "the spec change this Operation commits to has landed, or there was none to make")
		if !all {
			return
		}
		s.Phase = v1alpha1.OperationPhaseDone
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone, "every committed target is done")
		if s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

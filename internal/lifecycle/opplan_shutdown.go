package lifecycle

// The shutdown and managed-End half of the Operation planner (HBEP-10, Operations, The plan; #589 plan, 7.1):
// what run()'s shutdown branch takes (shutdown), the End of a managed-source deployment that holds nothing
// (managedEnd), and the Allocation fates they add (addShutdownFate). Split from opplan.go with no change of
// behaviour. Like the rest of the planner it is pure: it writes nothing, asks for no pass and reads no clock.

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// shutdownPlan says how the shutdown case differs: End sets the end time to now (endsNow) or deletes the
// deployment (deleted), and lists the deployment itself as a target (deployment); a Scale to zero does
// neither.
type shutdownPlan struct {
	endsNow, deleted, deployment bool
}

// shutdown plans what run()'s shutdown branch takes (HBEP-7, Release): every live Instance, every
// Allocation, and, for an End, the deployment itself. Every ready Instance drains, so none stays ready
// and each is the last. An Allocation follows retention at the plan's time: with a delete, a started one is
// released at once ("its deployment was deleted"); with the end time set to now, the keep date becomes
// now unless the policy's until is later (keepUntilOf); a Scale to zero keeps the deployment's own end
// time. A kept one on which the plan takes no Instance needs approval when its retention then releases it
// before its hard end (shutdownKeepNeeds); one on which it takes an Instance is asked through that
// Instance's target, by the same test.
func (b *planBuilder) shutdown(s shutdownPlan) {
	p, md, at := b.p, b.p.md, b.req.at
	base := fateView{deleted: s.deleted, endTime: endTimeOf(md)}
	if s.endsNow {
		base.endTime = &at
	}
	instanceTaken := map[string]bool{} // by Allocation: the plan takes an Instance on it
	for _, in := range p.insts {
		// An Instance that has ended, is draining already or is being deleted is on its way out, and a
		// stale one belongs to an earlier deployment of the name: the shutdown takes nothing from them.
		if ended(in) || in.Status.Phase == v1alpha1.InstanceDraining || !in.DeletionTimestamp.IsZero() || p.stale[in.UID] {
			continue
		}
		a := p.allocOf(in)
		effect := v1alpha1.PlanEffectCancelPending
		switch {
		case in.Status.Phase == v1alpha1.InstanceReady:
			effect = v1alpha1.PlanEffectDrainLastReady
		case jobStarted(a):
			effect = v1alpha1.PlanEffectEndStarting
		}
		needs, rule := instanceNeeds(effect, a, b.view(a, base), s.endsNow)
		b.addInstance(in, a, effect, needs, rule)
		instanceTaken[in.Spec.AllocationRef.Name] = true
	}
	for _, name := range orderedKeys(p.allocs) {
		a := p.allocs[name]
		if !a.DeletionTimestamp.IsZero() || allocationDead(a) {
			continue // the controller releases it itself, Operation or none
		}
		b.addShutdownFate(a, base, s.endsNow, instanceTaken[name])
	}
	if !s.deployment {
		return
	}
	effect, rule := v1alpha1.PlanEffectSetEndTime, ruleSetEndTime
	if s.deleted {
		effect, rule = v1alpha1.PlanEffectDeleteDeployment, ruleDeleteMD
	}
	b.targets = append(b.targets, v1alpha1.PlanTarget{
		Kind: v1alpha1.TargetKindModelDeployment, Name: md.Name, UID: md.UID, Generation: md.Generation,
		Effect: effect, ApprovalRule: rule,
	})
}

// holdsNothing reports whether the pass holds no Allocation and no live Instance of the deployment. A person
// can switch a serving deployment's source to managed (the CRD has no transition rule), and what it held
// stays: its Instances drain, and a started Allocation under keep is kept (#653's review, finding 1). Such a
// deployment's End is planned as the shutdown, so a delete that would release a started Allocation at once
// needs approval as on a served source: the science agent's ruling on #653, 16 Nov 2026, checks the premise
// "a managed-source deployment holds no Allocation" rather than assume it. An Allocation being deleted counts
// as held: the shutdown skips it.
func (b *planBuilder) holdsNothing() bool {
	if len(b.p.allocs) > 0 {
		return false
	}
	for _, in := range b.p.insts {
		if !ended(in) {
			return false
		}
	}
	return true
}

// managedEnd plans the End of a managed-source ModelDeployment that holds nothing (holdsNothing): ONE target,
// the deployment itself, since the lifecycle controller provisions nothing for it. HBEP-10 lets an agent end
// any deployment through an Operation, and the capacity guard leaves it no other way. Whether it needs
// approval is managedEndNeedsApproval's, the one place that says; its cost is the note noCapacityHeld, so the
// actor sees what ending it does.
func (b *planBuilder) managedEnd(del bool) {
	md := b.p.md
	effect := v1alpha1.PlanEffectSetEndTime
	if del {
		effect = v1alpha1.PlanEffectDeleteDeployment
	}
	t := b.target(v1alpha1.TargetKindModelDeployment, md.Name, md.UID, md.Generation, nil, effect, managedEndNeedsApproval, ruleManagedEnd)
	t.Cost = &v1alpha1.ReleaseCost{Note: noCapacityHeld}
	b.targets = append(b.targets, t)
}

// allocationDead reports whether the Allocation's job is over, or nothing can reach it: the controller
// releases it by itself (retentionFate), so no Operation's plan lists it.
func allocationDead(a *v1alpha1.Allocation) bool {
	switch a.Status.AtProvider.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true
	}
	return a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == "True"
}

// allocationFailed reports whether the Allocation's job failed or was lost, or nothing can reach it (a relay ended for
// good): the failures that release it, as a failed Instance's does (releasePending).
func allocationFailed(a *v1alpha1.Allocation) bool {
	switch a.Status.AtProvider.Phase {
	case v1alpha1.AllocationFailed, v1alpha1.AllocationLost:
		return true
	}
	return a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == "True"
}

// failureOfAllocation is the reason of the failure that released the Allocation, when allocationFailed says one did.
func failureOfAllocation(a *v1alpha1.Allocation) (reason string, ok bool) {
	switch {
	case a.Status.AtProvider.Phase == v1alpha1.AllocationFailed:
		return v1alpha1.ReasonAllocationFailed, true
	case a.Status.AtProvider.Phase == v1alpha1.AllocationLost:
		return v1alpha1.ReasonAllocationLost, true
	case a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == "True":
		return string(v1alpha1.ConditionRelayEndedForGood), true
	}
	return "", false
}

// addShutdownFate adds the Allocation target for what retention does with an Allocation the shutdown leaves:
// released at once, by allocationNeeds' rule; or kept. A kept one on which the plan takes no Instance
// (instanceTaken false) needs approval when its retention then releases it before its hard end
// (shutdownKeepNeeds); one on which it takes an Instance is asked through that Instance's target.
func (b *planBuilder) addShutdownFate(a *v1alpha1.Allocation, base fateView, endsNow, instanceTaken bool) {
	v := b.view(a, base)
	effect, until := allocationFate(a, v, b.req.at)
	needs, rule := allocationNeeds(effect)
	if effect == v1alpha1.PlanEffectKeep {
		rule = ruleKeep + ", until " + keepDate(until)
		if !instanceTaken {
			needs, rule = shutdownKeepNeeds(a, v, endsNow, until)
		}
	}
	b.addAllocation(a, effect, needs, rule)
}

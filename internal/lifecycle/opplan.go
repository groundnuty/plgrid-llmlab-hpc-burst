package lifecycle

// The Operation planner (HBEP-10, Operations, The plan; #589 plan, 7.1). It is PURE: from what the pass
// read and from the plan's observation time it returns a plan or a refusal. It writes nothing, asks for no
// pass (p.at) and reads no clock. It calls the selections the controller itself acts by (scaleDownTargets,
// retentionFate), so a plan and the controller never choose apart.

import (
	"fmt"
	"slices"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The causes of a scale-down target: what made the controller's own selection take it (scaleCause).
const (
	causeLowered        = string(scaleLowered)
	causeOverAllocation = string(scaleOverAllocationTrim)
)

// planCause is the cause of a scale target, read through this one function: the typed cause #635 (#616, PR 3)
// gives scaleTarget, where an absent cause reads as lowered. It is read on the Scale(N) path only, from the
// planner's own call with a surplus (scale), never from step 7's call.
func planCause(t scaleTarget) string {
	if t.cause == "" {
		return causeLowered
	}
	return string(t.cause)
}

// maxPlanTargets is the most targets an Operation's status holds (the CRD's maxItems of status.plan.targets): a
// larger plan is refused, since writing it would fail on every pass.
const maxPlanTargets = 256

// planRequest is what one planning reads besides the pass.
type planRequest struct {
	op *v1alpha1.Operation
	// at is the plan's observation time.
	at time.Time
	// policies are the ApprovalPolicies, as listed.
	policies []v1alpha1.ApprovalPolicy
	// allocs are the namespace's Allocations, a Release's candidates: filtered by
	// spec.modelDeploymentRef.name, never by the model-deployment label, which a long name shortens.
	allocs []*v1alpha1.Allocation
}

// planBuilder collects the targets of one plan.
type planBuilder struct {
	p       *pass
	req     planRequest
	targets []v1alpha1.PlanTarget
}

// planOperation plans an Operation on the state the pass read. A refusal is the reason the Operation
// cannot be planned.
func (p *pass) planOperation(req planRequest) (*v1alpha1.OperationPlan, string) {
	md := p.md
	switch {
	case md == nil:
		return nil, fmt.Sprintf("ModelDeployment %s does not exist", p.key)
	case !md.DeletionTimestamp.IsZero():
		return nil, fmt.Sprintf("ModelDeployment %s is being deleted", p.key)
	case md.Spec.Source.Managed != nil && req.op.Spec.Action != v1alpha1.OperationActionEnd:
		return nil, fmt.Sprintf("ModelDeployment %s has a managed source: the lifecycle controller provisions nothing for it, so there is nothing to scale or release; only an End is planned", p.key)
	}
	b := &planBuilder{p: p, req: req}
	var refusal string
	switch spec := req.op.Spec; spec.Action {
	case v1alpha1.OperationActionScale:
		if spec.Scale == nil {
			return nil, "a Scale names its count in spec.scale"
		}
		refusal = b.scale(spec.Scale.Instances)
	case v1alpha1.OperationActionEnd:
		del := spec.End != nil && spec.End.Delete
		if md.Spec.Source.Managed != nil && b.holdsNothing() {
			b.managedEnd(del)
			break
		}
		b.shutdown(shutdownPlan{endsNow: !del, deleted: del, deployment: true})
	case v1alpha1.OperationActionRelease:
		if spec.Release == nil {
			return nil, "a Release names its Allocations in spec.release"
		}
		refusal = b.release(*spec.Release)
	default:
		return nil, fmt.Sprintf("the action %q is not planned", spec.Action)
	}
	if refusal != "" {
		return nil, refusal
	}
	if len(b.targets) > maxPlanTargets {
		return nil, fmt.Sprintf("the plan has %d targets, more than the %d an Operation's status holds: scale in steps, or release in groups", len(b.targets), maxPlanTargets)
	}
	sort.SliceStable(b.targets, func(i, j int) bool { return targetBefore(b.targets[i], b.targets[j]) })
	plan := &v1alpha1.OperationPlan{
		ObservedAt:      metav1.NewTime(req.at),
		ModelDeployment: v1alpha1.PlannedDeployment{Name: md.Name, UID: md.UID, Generation: md.Generation},
		Targets:         b.targets,
	}
	plan.ID = planID(req.op.Spec.Action, plan.ModelDeployment, plan.Targets)
	return plan, ""
}

// scale plans a Scale to n Instances: the controller's own scale-down selection, with each victim's effect
// and the retention fate of every Allocation it empties. Zero is the shutdown case.
func (b *planBuilder) scale(n int32) string {
	md := b.p.md
	if n >= md.Spec.Instances {
		return fmt.Sprintf("a Scale lowers the count: %d is not below spec.instances %d; raise it directly (HBEP-10, Operations)", n, md.Spec.Instances)
	}
	if n == 0 {
		b.shutdown(shutdownPlan{})
		return ""
	}
	p := b.p
	prim := p.primaries()
	// The cause is read on this path only, from this call: step 7's own call can label a target
	// overAllocationTrim after a person lowered the count directly. The selection asks for a later pass
	// (p.at) while a marked peer waits for the front's probe; a plan asks for none, so that wake-up is
	// dropped here (step 7's own call makes it).
	next := p.next
	victims := p.scaleDownTargets(prim, max(len(prim)-int(n), 0))
	p.next = next
	taken := map[types.UID]bool{}
	for _, t := range victims {
		taken[t.instance.UID] = true
	}
	staysReady := false
	for _, in := range p.insts {
		if in.Status.Phase == v1alpha1.InstanceReady && in.DeletionTimestamp.IsZero() && !p.stale[in.UID] && !taken[in.UID] {
			staysReady = true
		}
	}
	trimmed := map[string]bool{} // by Allocation: every Instance that empties it is a trim
	emptied := map[string]bool{}
	for _, t := range victims {
		effect := scaleEffectOf(t, staysReady)
		trim := planCause(t) == causeOverAllocation
		v := b.view(t.allocation, fateView{endTime: endTimeOf(md)})
		needs, rule := instanceNeeds(effect, t.allocation, v, false)
		if trim {
			needs, rule = false, ruleOverAllocation
		}
		b.addInstance(t.instance, t.allocation, effect, needs, rule)
		if t.allocation != nil {
			name := t.allocation.Name
			if prev, seen := trimmed[name]; seen {
				trimmed[name] = prev && trim
			} else {
				trimmed[name] = trim
			}
			emptied[name] = true
		}
	}
	for _, name := range orderedKeys(emptied) {
		a := p.allocs[name]
		if !b.emptiedBy(a, taken) {
			continue
		}
		b.addFate(a, fateView{endTime: endTimeOf(md)}, trimmed[name])
	}
	return ""
}

func endTimeOf(md *v1alpha1.ModelDeployment) *time.Time {
	if md.Spec.EndTime == nil {
		return nil
	}
	t := md.Spec.EndTime.Time
	return &t
}

// scaleEffectOf maps the controller's own effect to the plan's: a pending Allocation is cancelled; an
// Instance still starting on a started Allocation ends; a ready one drains, and it is the last ready one
// when no Instance of the deployment would stay ready.
func scaleEffectOf(t scaleTarget, staysReady bool) v1alpha1.PlanEffect {
	switch t.effect {
	case effectCancelPending:
		return v1alpha1.PlanEffectCancelPending
	case effectEndKeepAllocation:
		return v1alpha1.PlanEffectEndStarting
	}
	if staysReady {
		return v1alpha1.PlanEffectDrainInstance
	}
	return v1alpha1.PlanEffectDrainLastReady
}

// emptiedBy reports whether the plan leaves the Allocation with no live Instance: every Instance that is
// live on it is one the plan takes.
func (b *planBuilder) emptiedBy(a *v1alpha1.Allocation, taken map[types.UID]bool) bool {
	if a == nil {
		return false
	}
	for _, in := range b.p.liveOn(a.Name) {
		if !taken[in.UID] {
			return false
		}
	}
	return true
}

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

// release plans a Release: the named Allocations, or those a selector matches, each one of this
// deployment's by spec.modelDeploymentRef.name and hosting no live Instance. Each is released.
func (b *planBuilder) release(r v1alpha1.ReleaseParameters) string {
	p, md := b.p, b.p.md
	byName := map[string]*v1alpha1.Allocation{}
	for _, a := range b.req.allocs {
		byName[a.Name] = a
	}
	var chosen []*v1alpha1.Allocation
	switch {
	case len(r.Allocations) > 0:
		for _, name := range r.Allocations {
			a := byName[name]
			switch {
			case a == nil:
				return fmt.Sprintf("Allocation %s does not exist in namespace %s", name, md.Namespace)
			case a.Spec.ModelDeploymentRef == nil || a.Spec.ModelDeploymentRef.Name != md.Name:
				return fmt.Sprintf("Allocation %s is not an Allocation of ModelDeployment %s: a Release covers one deployment's Allocations, found by spec.modelDeploymentRef.name", name, md.Name)
			case !a.DeletionTimestamp.IsZero():
				return fmt.Sprintf("Allocation %s is being released already", name)
			}
			chosen = append(chosen, a)
		}
	case r.Selector != nil:
		sel, err := metav1.LabelSelectorAsSelector(r.Selector)
		if err != nil {
			return fmt.Sprintf("the release selector is not valid: %v", err)
		}
		if sel.Empty() {
			return "an empty release selector would match every Allocation: name the Allocations, or give a selector with labels"
		}
		for _, name := range orderedKeys(byName) {
			a := byName[name]
			if a.Spec.ModelDeploymentRef != nil && a.Spec.ModelDeploymentRef.Name == md.Name && a.DeletionTimestamp.IsZero() && sel.Matches(labels.Set(a.Labels)) {
				chosen = append(chosen, a)
			}
		}
		if len(chosen) == 0 {
			return fmt.Sprintf("the release selector matches no Allocation of ModelDeployment %s", md.Name)
		}
	default:
		return "a Release names its Allocations or a selector"
	}
	for _, a := range chosen {
		if live := p.liveOn(a.Name); len(live) > 0 {
			return fmt.Sprintf("Allocation %s hosts a live Instance (%s): a Release covers Allocations that host none; scale or end the deployment instead", a.Name, live[0].Name)
		}
	}
	for _, a := range chosen {
		needs, rule := allocationNeeds(v1alpha1.PlanEffectReleaseKept)
		b.addAllocation(a, v1alpha1.PlanEffectReleaseKept, needs, rule)
	}
	return ""
}

// view is the retention view of an Allocation for the plan: base, with the profile's maxIdle of the
// Allocation's Provider.
func (b *planBuilder) view(a *v1alpha1.Allocation, base fateView) fateView {
	if a != nil {
		if pv := b.p.pvOf(a.Labels); pv != nil && pv.profile != nil {
			base.maxIdle = pv.settings.maxIdle
		}
	}
	return base
}

// addFate adds the Allocation target for what retention does with an Allocation the plan empties: kept, or
// released at once. A trim, over-allocation's own, needs no approval.
func (b *planBuilder) addFate(a *v1alpha1.Allocation, base fateView, trim bool) {
	effect, until := allocationFate(a, b.view(a, base), b.req.at)
	needs, rule := allocationNeeds(effect)
	if effect == v1alpha1.PlanEffectKeep {
		rule = ruleKeep + ", until " + keepDate(until)
	}
	if trim {
		needs, rule = false, ruleOverAllocation
	}
	b.addAllocation(a, effect, needs, rule)
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

// keepDate writes the date a kept Allocation is next looked at, or says it is kept until its hard end.
func keepDate(t time.Time) string {
	if t.IsZero() {
		return "its hard end"
	}
	return t.UTC().Format(time.RFC3339)
}

// addInstance adds an Instance target, with its Allocation's cost.
func (b *planBuilder) addInstance(in *v1alpha1.Instance, a *v1alpha1.Allocation, effect v1alpha1.PlanEffect, needs bool, rule string) {
	t := b.target(v1alpha1.TargetKindInstance, in.Name, in.UID, in.Generation, in.Labels, effect, needs, rule)
	t.Cost = b.cost(a, in.Labels, effect == v1alpha1.PlanEffectDrainLastReady)
	b.targets = append(b.targets, t)
}

// addAllocation adds an Allocation target; only a target that lowers capacity carries a cost: every effect
// but Keep, and a Keep that needs approval, since its retention then releases it before its hard end.
func (b *planBuilder) addAllocation(a *v1alpha1.Allocation, effect v1alpha1.PlanEffect, needs bool, rule string) {
	t := b.target(v1alpha1.TargetKindAllocation, a.Name, a.UID, a.Generation, a.Labels, effect, needs, rule)
	if effect != v1alpha1.PlanEffectKeep || needs {
		t.Cost = b.cost(a, a.Labels, false)
	}
	b.targets = append(b.targets, t)
}

// target builds the common part: identity, Provider, effect, approval rule and the policies that apply.
func (b *planBuilder) target(kind v1alpha1.TargetKind, name string, uid types.UID, gen int64, lbl map[string]string, effect v1alpha1.PlanEffect, needs bool, rule string) v1alpha1.PlanTarget {
	t := v1alpha1.PlanTarget{
		Kind: kind, Name: name, UID: uid, Generation: gen, Provider: lbl[v1alpha1.LabelProvider],
		Effect: effect, NeedsApproval: needs, ApprovalRule: rule,
	}
	if needs {
		md := b.p.md
		t.Policies = appliedPolicies(resolvePolicies(md.Namespace, md.Name, t.Provider, b.req.policies))
	}
	return t
}

// cost is the release cost of a target on the Allocation a, whose labels name the Provider.
func (b *planBuilder) cost(a *v1alpha1.Allocation, lbl map[string]string, lastReady bool) *v1alpha1.ReleaseCost {
	in := costInput{at: b.req.at, alloc: a, startToReady: b.p.md.Status.StartToReady, lastReady: lastReady}
	if pv := b.p.pvOf(lbl); pv != nil {
		in.prov = pv.prov
		if pv.hasClass {
			c := pv.class
			in.class = &c
		}
	}
	return releaseCost(in)
}

// orderedKeys returns a map's keys in order, so a plan never depends on a map's.
func orderedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

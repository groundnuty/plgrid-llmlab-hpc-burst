package lifecycle

import (
	"context"
	"fmt"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The extra (beta; HBEP-7, Over-allocation and qualification (beta), k, and when the extra is submitted; Its
// checks; In host-dialed mode; #616 PR 4).
//
// In the pass that makes a new Allocation for a missing primary on a Provider, one more Allocation with its
// Instance is made there, the extra, when k is 1 and the Provider passes three checks: the bound, a slot beyond the
// deployment's own handovers, and the per-user caps. The extra carries no mark: the selection (select.go) decides
// which of the round's two Instances is the primary, by which starts and passes first. A refused extra is silent,
// since it is k = 0 on that Provider in effect and no degradation (placeOpts.extra).

// createExtras gives each Provider in placed one extra, in order. It is called with the Providers on which the
// missing-primary loop of the pass made a NEW Allocation: a missing primary placed on a warm Allocation made none,
// so it brings no extra, and replacements and hedge members never pass through capacity()'s loop. With k = 0
// (spec.overAllocation) no extra is ever made. A pass cut short before this leaves its round without one, since the
// next pass has no missing primary.
func (p *pass) createExtras(placed []string) error {
	if p.md == nil || p.md.Spec.OverAllocationCount() < 1 {
		return nil
	}
	for _, name := range placed {
		pv := p.pvByName[name]
		if pv == nil {
			continue
		}
		x, ok := p.extraChecks(pv, nil, p.plan.gpus*p.plan.workers)
		if !ok {
			continue
		}
		if _, err := p.provisionWith(nil, placeOpts{only: pv.name, extra: true, caps: true, alsoLeave: x.alsoLeave, headroom: x.headroom,
			qosAlsoLeave: x.qosAlsoLeave, qosHeadroom: x.qosHeadroom}); err != nil {
			return err
		}
	}
	return nil
}

// extraBound is the first check, and it is made first: the deployment holds no surplus Instance on the Provider in
// the selection recomputed after this pass's creations (selectPrimaries over p.insts, which createInstance
// appends to), and no warm Allocation there (warmOf). So k bounds the deployment's surplus Allocations per Provider
// at any time, whatever their phase. A surplus hedge counts on every Provider one of its live members is on.
func (p *pass) extraBound(pv *providerView) bool { return p.extraBoundWithout(pv, nil) }

// extraBoundWithout is extraBound with one Instance left out of the selection: the cut-short extra whose retry asks
// (extraChecks), which would otherwise be the surplus Instance that refuses it, or, when it wins the selection's UID
// tie against its primary, make that primary the surplus.
func (p *pass) extraBoundWithout(pv *providerView, exclude *v1alpha1.Instance) bool {
	if len(p.warmOf(pv)) > 0 {
		return false
	}
	_, surplus := selectPrimaries(p.selectionWithout(exclude), p.desiredCount())
	for _, rep := range surplus {
		for _, m := range p.membersOf(rep) {
			if m.Labels[v1alpha1.LabelProvider] == pv.name {
				return false
			}
		}
	}
	return true
}

// selectionWithout is the pass's selection without one Instance (nil: none left out).
func (p *pass) selectionWithout(exclude *v1alpha1.Instance) selection {
	sel := p.selection(nil)
	if exclude == nil {
		return sel
	}
	insts := make([]*v1alpha1.Instance, 0, len(p.insts))
	for _, in := range p.insts {
		if in != exclude {
			insts = append(insts, in)
		}
	}
	sel.insts = insts
	return sel
}

// extraChecks are the extra's checks on a Provider, in ONE place for createExtras, which makes the extra, and
// ensureAllocation, which retries one whose Allocation's create was cut short, so the two never drift (#708, finding 4):
// the bound, the slot beyond its own handovers (alsoLeave) and the admission headroom (headroom, read under the Provider's
// lock by the reservation). exclude is the Instance that retries (nil for a new extra). The per-user caps and the silence
// are the reservation's own, asked with params and quiet. ok is false when the bound refuses. gpus is the extra's GPU
// count: where the Provider's partition sets qosByGpus it says which QOS the extra runs under, and the same slots and
// headroom are then asked a second time over the Allocations under that QOS alone (HBEP-7, Per-user limits).
func (p *pass) extraChecks(pv *providerView, exclude *v1alpha1.Instance, gpus int32) (x extraAsk, ok bool) {
	if !p.extraBoundWithout(pv, exclude) {
		return extraAsk{}, false
	}
	x = extraAsk{alsoLeave: p.extraAlsoLeaveWithout(pv, exclude), headroom: p.headroomOf(pv)}
	if rv, err := pv.forGPUs(gpus); err == nil && rv.scope != nil {
		x.qosAlsoLeave = p.extraAlsoLeaveWhere(pv, exclude, rv.scope)
		x.qosHeadroom = p.headroomWhere(pv, rv.scope)
	}
	return x, true
}

// headroomOf is the admission headroom H of an extra on the Provider (HBEP-7, Over-allocation and qualification (beta), Its
// checks, admission headroom; the science agent's ruling on #708's delta reviews, amendment #711): the number of missing
// PRIMARIES, counted as primaries, of the OTHER deployments that the Provider allows, at the time of the check, and 0 when
// there are none, so an extra never takes a slot a primary needs at the time, and nothing is cancelled to make room. It is the callback a reservation calls under the Provider's lock
// (slotAsk.headroom), reading through the uncached Reader: one List of the Providers and one of the ModelDeployments of
// every namespace, and, for each deployment that allows the Provider, its Instances and Allocations by its model-deployment
// label. A deployment with a missing primary may have NO Instance on this Provider, so no view of the Provider's own
// Instances (deploymentViews) can tell. A deployment counts when it is not this one, not being deleted, serves, wants
// Instances, has not passed its end time (by the clock read under the lock), allows the Provider (selectedProviderNames, as its own pass reads it) and its
// shortage by Allocation presence (shortageOfDeployment) is above zero.
func (p *pass) headroomOf(pv *providerView) func(context.Context) (int, error) {
	return p.headroomWhere(pv, nil)
}

// headroomWhere is headroomOf counting, with a scope, only the other deployments whose missing primary would run
// under the scope's QOS: the extra's admission headroom over the Allocations under its own QOS alone (HBEP-7,
// Per-user limits). A deployment whose size cannot be read counts, so the headroom never falls short.
func (p *pass) headroomWhere(pv *providerView, sc *qosScope) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		var mds v1alpha1.ModelDeploymentList
		if err := p.r.Reader.List(ctx, &mds); err != nil {
			return 0, fmt.Errorf("list ModelDeployments for the extra's headroom: %w", err)
		}
		var provs v1alpha1.ProviderList
		if err := p.r.Reader.List(ctx, &provs); err != nil {
			return 0, fmt.Errorf("list Providers for the extra's headroom: %w", err)
		}
		// The clock is read here, with the lock held and every read back, never the pass's p.now from before it: a pass that
		// waited for the lock must not count a deployment whose end time passed meanwhile.
		now := p.reserveTime()
		h := 0
		for i := range mds.Items {
			md := &mds.Items[i]
			if md.Namespace == p.key.Namespace && md.Name == p.key.Name {
				continue
			}
			if !md.DeletionTimestamp.IsZero() || md.Spec.Source.Served == nil || md.Spec.Instances == 0 ||
				(md.Spec.EndTime != nil && !now.Before(md.Spec.EndTime.Time)) {
				continue
			}
			allowed, err := selectedProviderNames(md.Spec.Providers, provs.Items)
			if err != nil || !slices.Contains(allowed, pv.name) {
				continue
			}
			if sc != nil {
				// Each missing primary is charged to the QOS it will run under: an existing pending unit at its OWN
				// launch, a unit not made yet at the current spec (shortageUnder).
				n, err := shortageUnder(ctx, p.r.Reader, md, sc)
				if err != nil {
					return 0, err
				}
				h += n
				continue
			}
			short, err := shortageOfDeployment(ctx, p.r.Reader, md)
			if err != nil {
				return 0, err
			}
			h += short
		}
		return h, nil
	}
}

// shortageOf is how many primaries a deployment lacks, counted by ALLOCATION PRESENCE and never by the selection's
// choice: its desired count less the units, one per hedge, that countable Instances make and that HAVE an Allocation. A
// cut-short Instance (made, its Allocation's create failed, retried in ensureAllocation) adds nothing, so it counts as
// missing; and the selection's tie between same-second pending Instances (select.go: the UID) can never turn a missing
// primary into "chosen". The pass, the extra's classification in ensureAllocation and the headroom's reading of the other
// deployments all use this one function.
func shortageOf(sel selection, desired int) int {
	has := 0
	for _, u := range sel.units() {
		for _, m := range u.members {
			if sel.allocOf(m) != nil {
				has++
				break
			}
		}
	}
	return max(desired-has, 0)
}

// shortage is shortageOf this pass's Instances and Allocations.
func (p *pass) shortage() int { return shortageOf(p.selection(nil), p.desiredCount()) }

// shortageOfDeployment is shortageOf the deployment's own Instances and Allocations, read through the Reader by its
// model-deployment label in its namespace, as its own pass reads them (selectionOfView).
func shortageOfDeployment(ctx context.Context, r client.Reader, md *v1alpha1.ModelDeployment) (int, error) {
	v, err := deploymentViewOf(ctx, r, md)
	if err != nil {
		return 0, err
	}
	return shortageOf(selectionOfView(md, v), int(md.Spec.Instances)), nil
}

// deploymentViewOf reads the deployment's own Instances and Allocations, by its model-deployment label in its namespace.
func deploymentViewOf(ctx context.Context, r client.Reader, md *v1alpha1.ModelDeployment) (deploymentView, error) {
	v := deploymentView{key: md.Namespace + "/" + md.Name, allocs: map[string]*v1alpha1.Allocation{}}
	var il v1alpha1.InstanceList
	if err := r.List(ctx, &il, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return v, fmt.Errorf("list Instances of %s for the extra's headroom: %w", v.key, err)
	}
	for i := range il.Items {
		v.insts = append(v.insts, &il.Items[i])
	}
	var al v1alpha1.AllocationList
	if err := r.List(ctx, &al, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return v, fmt.Errorf("list Allocations of %s for the extra's headroom: %w", v.key, err)
	}
	for i := range al.Items {
		v.allocs[al.Items[i].Name] = &al.Items[i]
	}
	return v, nil
}

// shortageUnder is shortageOfDeployment counting only the missing primaries that will run under the scope's QOS: a
// missing primary that already has an Instance (made, its Allocation not yet) will be sized at that Instance's own launch,
// so it is charged at its own GPU count; one not made yet is sized at the deployment's current spec (HBEP-7, Per-user
// limits). A spec that gives no plan counts its unmade primaries, so the count never falls short.
func shortageUnder(ctx context.Context, r client.Reader, md *v1alpha1.ModelDeployment, sc *qosScope) (int, error) {
	v, err := deploymentViewOf(ctx, r, md)
	if err != nil {
		return 0, err
	}
	sel := selectionOfView(md, v)
	has := 0
	var pending []int32 // the GPU count of each existing unit that has no Allocation yet
	for _, u := range sel.units() {
		made := false
		for _, m := range u.members {
			if sel.allocOf(m) != nil {
				made = true
				break
			}
		}
		if made {
			has++
		} else {
			pending = append(pending, launchGPUs(u.first))
		}
	}
	short := max(int(md.Spec.Instances)-has, 0)
	current := int32(0) // 0: not known, counted under every QOS
	if plan, err := checkServed(md); err == nil {
		current = plan.gpus * plan.workers
	}
	n := 0
	for i := range short {
		gpus := current
		if i < len(pending) {
			gpus = pending[i]
		}
		if sc.under(gpus) {
			n++
		}
	}
	return n, nil
}

// extraAlsoLeave is how many slots the extra leaves free beside itself on the Provider: one for each of the
// deployment's chosen primaries there that holds no reserved slot yet but will once Running (HBEP-7, Its checks, a
// slot beyond its own handovers). A primary will hold one when the controller would create its replacement
// (reservedOn): it has none yet, and the deployment has no end time or one after its expected drain start, its
// creation plus its Allocation's time limit minus the drain window. Without it an extra made beside a pending
// primary takes the slot that primary reserves once it runs, and its handover finds none (Per-user limits, Why).
//
// It reads the pass's own objects. The slots reserved by Running primaries, and by other deployments', are read
// under the Provider's lock as the count always did (reservedOn), so none is counted twice here: a primary whose
// Allocation is Running holds its slot there, and is no part of alsoLeave.
func (p *pass) extraAlsoLeave(pv *providerView) int { return p.extraAlsoLeaveWithout(pv, nil) }

// extraAlsoLeaveWithout is extraAlsoLeave over the chosen primaries of the selection without one Instance (extraChecks).
func (p *pass) extraAlsoLeaveWithout(pv *providerView, exclude *v1alpha1.Instance) int {
	return p.extraAlsoLeaveWhere(pv, exclude, nil)
}

// extraAlsoLeaveWhere is extraAlsoLeaveWithout counting, with a scope, only the chosen primaries whose Allocation
// runs under the scope's QOS (HBEP-7, Per-user limits).
func (p *pass) extraAlsoLeaveWhere(pv *providerView, exclude *v1alpha1.Instance, sc *qosScope) int {
	n := 0
	chosen, _ := selectPrimaries(p.selectionWithout(exclude), p.desiredCount())
	for _, c := range chosen {
		if c.Labels[v1alpha1.LabelProvider] != pv.name || p.replaced(c) {
			continue
		}
		if sc != nil && !sc.under(launchGPUs(c)) {
			continue
		}
		a := p.allocOf(c)
		if a != nil && !notYetRunning(a) {
			continue // Running: reservedOn counts its slot; ended, failed, lost or refused: it will hold none
		}
		if p.holdsHandoverSlotLater(c, a, pv) {
			n++
		}
	}
	return n
}

// notYetRunning reports whether the Allocation has not started and still may: its phase is none, Submitting,
// SubmissionUnknown, Held or Pending.
func notYetRunning(a *v1alpha1.Allocation) bool {
	switch a.Status.AtProvider.Phase {
	case "", v1alpha1.AllocationSubmitting, v1alpha1.AllocationSubmissionUnknown, v1alpha1.AllocationHeld, v1alpha1.AllocationPending:
		return true
	}
	return false
}

// holdsHandoverSlotLater reports whether the primary c, whose Allocation a has not started, will hold a reserved
// slot once it runs: the deployment serves past its expected drain start (servesPast, the test replacements() and
// reservedOn share). An Allocation not made yet, or without a readable time limit, is counted: a slot left over
// is a refused extra, and a slot taken is a refused handover.
func (p *pass) holdsHandoverSlotLater(c *v1alpha1.Instance, a *v1alpha1.Allocation, pv *providerView) bool {
	if a == nil || a.Spec.ForProvider.Batch == nil {
		return true
	}
	limit, err := a.Spec.ForProvider.Batch.TimeLimit.AsDuration()
	if err != nil {
		return true
	}
	created := c.CreationTimestamp.Time
	if created.IsZero() {
		created = p.now // an Instance this pass made, before the API server stamped it
	}
	drainStart := created.Add(limit).Add(-drainWindowOf(pv, p.md))
	return servesPast(p.md, drainStart)
}

// appendOnce appends s to the list unless it is there.
func appendOnce(list []string, s string) []string {
	if slices.Contains(list, s) {
		return list
	}
	return append(list, s)
}

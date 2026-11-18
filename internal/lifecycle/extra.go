package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"time"

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
		ask, ok := p.extraChecks(pv, nil)
		if !ok {
			continue
		}
		if _, err := p.provisionWith(nil, placeOpts{only: pv.name, extra: true, caps: true, ask: ask}); err != nil {
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

// extraAsk is what an extra's checks add to its reservation, in ONE value for createExtras, which makes the extra, and
// ensureAllocation, which retries one whose Allocation's create was cut short (extraChecks).
type extraAsk struct {
	// deployment is the asking deployment (namespace/name). Non-empty marks an extra's reservation: it then also counts,
	// under the Provider's lock, the primaries of the OTHER deployments that will hand over (slotAsk.deployment).
	deployment string
	// alsoLeave is how many slots the extra leaves free beside itself for the deployment's own chosen primaries that are
	// pending and will hold a handover slot once Running (extraAlsoLeave).
	alsoLeave int
	// replacements are those primaries' sizes: their future replacements need that much of the per-user caps (capsFit).
	replacements []v1alpha1.BatchParameters
	// headroom is the admission headroom (headroomOf), read under the Provider's lock by the reservation.
	headroom func(context.Context) (int, error)
}

// extraChecks are the extra's checks on a Provider, in ONE place for createExtras, which makes the extra, and
// ensureAllocation, which retries one whose Allocation's create was cut short, so the two never drift (#708, finding 4):
// the bound, the slot beyond its own handovers (alsoLeave), the room its own pending primaries' replacements will need
// under the per-user caps (replacements) and the admission headroom (headroom, read under the Provider's lock by the
// reservation). exclude is the Instance that retries (nil for a new extra). The other deployments' primaries, a
// pending one's slot and every handing-over primary's caps room (futureReplacements), and the per-user caps and the
// silence are the reservation's own, asked with params and quiet. ok is false when the bound refuses.
func (p *pass) extraChecks(pv *providerView, exclude *v1alpha1.Instance) (ask extraAsk, ok bool) {
	if !p.extraBoundWithout(pv, exclude) {
		return extraAsk{}, false
	}
	n, sizes := p.extraAlsoLeaveWithout(pv, exclude)
	return extraAsk{deployment: p.key.Namespace + "/" + p.key.Name, alsoLeave: n, replacements: sizes, headroom: p.headroomOf(pv)}, true
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
//
// Each other deployment is read ONCE, under the Provider's lock, from the API. A spec edit that lands between that read and
// the deployment's own pass (its instances raised, its providers or end time changed) is not seen by this count: the next
// pass reads it afresh, which bounds the error to one pass, and nothing is cancelled for it (#725, from #708's review).
func (p *pass) headroomOf(pv *providerView) func(context.Context) (int, error) {
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
	v := deploymentView{key: md.Namespace + "/" + md.Name, allocs: map[string]*v1alpha1.Allocation{}}
	var il v1alpha1.InstanceList
	if err := r.List(ctx, &il, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return 0, fmt.Errorf("list Instances of %s for the extra's headroom: %w", v.key, err)
	}
	for i := range il.Items {
		v.insts = append(v.insts, &il.Items[i])
	}
	var al v1alpha1.AllocationList
	if err := r.List(ctx, &al, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return 0, fmt.Errorf("list Allocations of %s for the extra's headroom: %w", v.key, err)
	}
	for i := range al.Items {
		v.allocs[al.Items[i].Name] = &al.Items[i]
	}
	return shortageOf(selectionOfView(md, v), int(md.Spec.Instances)), nil
}

// extraAlsoLeave is how many slots the extra leaves free beside itself on the Provider: one for each of the
// deployment's chosen primaries there that holds no reserved slot yet but will once Running (HBEP-7, Its checks, a
// slot beyond its own handovers). A primary will hold one when the controller would create its replacement
// (reservedOn): it has none yet, and the deployment has no end time or one after its expected drain start, its
// creation plus its Allocation's time limit minus the drain window. Without it an extra made beside a pending
// primary takes the slot that primary reserves once it runs, and its handover finds none (Per-user limits, Why).
//
// It reads the pass's own objects, and gives with the count the size of each such primary, which its future
// replacement needs of the per-user caps (the caps' own, #725). The slots reserved by Running primaries, and by other
// deployments', and the other deployments' pending primaries that will hold one, are read under the Provider's lock
// (reservedAndLaterOn, slotAsk.deployment), so none is counted twice here: a primary whose Allocation is Running holds
// its slot there, and is no part of alsoLeave.
func (p *pass) extraAlsoLeaveWithout(pv *providerView, exclude *v1alpha1.Instance) (n int, sizes []v1alpha1.BatchParameters) {
	chosen, _ := selectPrimaries(p.selectionWithout(exclude), p.desiredCount())
	var fallback *v1alpha1.BatchParameters // the size a primary with no Allocation yet will have: the extra's own, as provisionWith sizes it
	for _, c := range chosen {
		if c.Labels[v1alpha1.LabelProvider] != pv.name || p.replaced(c) {
			continue
		}
		a := p.allocOf(c)
		if a != nil && !notYetRunning(a) {
			continue // Running: reservedOn counts its slot; ended, failed, lost or refused: it will hold none
		}
		if !p.holdsHandoverSlotLater(c, a, pv) {
			continue
		}
		n++
		switch {
		case a != nil && a.Spec.ForProvider.Batch != nil:
			sizes = append(sizes, *a.Spec.ForProvider.Batch)
		default:
			if fallback == nil {
				if params, err := partitionSizingFor(pv, p.plan.gpus, p.plan.workers, p.md.Spec.LeaseLimit); err == nil {
					fallback = &params
				} else {
					fallback = &v1alpha1.BatchParameters{}
				}
			}
			sizes = append(sizes, *fallback)
		}
	}
	return n, sizes
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
// slot once it runs: the deployment serves past its expected drain start (holdsSlotOnceRunning, which reservedOn reads
// for the other deployments' primaries by the same test). An Allocation not made yet, or without a readable time
// limit, is counted: a slot left over is a refused extra, and a slot taken is a refused handover.
func (p *pass) holdsHandoverSlotLater(c *v1alpha1.Instance, a *v1alpha1.Allocation, pv *providerView) bool {
	return holdsSlotOnceRunning(p.md, c, a, pv.profile.Spec, p.now)
}

// holdsSlotOnceRunning is that test, over what a pass and reservedOn both hold: the deployment md, the primary in and
// its not yet started Allocation a (nil: not made yet), the Provider's profile, and now, the time of an Instance this
// pass made before the API server stamped its creation (the zero time for a reader of stored Instances, which are
// all stamped).
func holdsSlotOnceRunning(md *v1alpha1.ModelDeployment, in *v1alpha1.Instance, a *v1alpha1.Allocation, profile v1alpha1.PlatformProfileSpec, now time.Time) bool {
	if a == nil || a.Spec.ForProvider.Batch == nil {
		return true
	}
	limit, err := a.Spec.ForProvider.Batch.TimeLimit.AsDuration()
	if err != nil {
		return true
	}
	created := in.CreationTimestamp.Time
	if created.IsZero() {
		created = now
	}
	window, err := v1alpha1.InstanceDrainWindow(profile, md.Spec.Routing)
	if err != nil {
		window = v1alpha1.DefaultDrainWindow
	}
	return servesPast(md, created.Add(limit).Add(-window))
}

// appendOnce appends s to the list unless it is there.
func appendOnce(list []string, s string) []string {
	if slices.Contains(list, s) {
		return list
	}
	return append(list, s)
}

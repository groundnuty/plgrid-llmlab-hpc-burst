package lifecycle

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/types"

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
	// deployment is the asking deployment (namespace/name). Non-empty marks an extra's reservation: it then reads, under the
	// Provider's lock, the unit set of the handovers it must leave room for (extraUnits, slotAsk.deployment).
	deployment string
	// exclude is the asking Instance, the cut-short extra that retries (zero for a new one), left out of the unit set.
	exclude types.UID
	// sizeOf is the size of a replacement that does not exist yet on the Provider, from the deployment's current spec, and
	// sizeRecorded the size of an existing Instance's Allocation, from its recorded launch (extraUnits).
	sizeOf       func(*v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error)
	sizeRecorded func(*v1alpha1.ModelDeployment, *v1alpha1.Instance) (v1alpha1.BatchParameters, error)
	// headroom is the admission headroom (headroomOf), read under the Provider's lock by the reservation.
	headroom func(context.Context, slotSnapshot) (int, error)
}

// extraChecks are the extra's checks on a Provider, in ONE place for createExtras, which makes the extra, and
// ensureAllocation, which retries one whose Allocation's create was cut short, so the two never drift (#708, finding 4):
// the bound, the admission headroom (headroom) and what the unit set of the handovers needs of the reservation: the
// asking deployment, the Instance to leave out, and how to size a deployment's replacement and an Instance's Allocation here. The slot beyond the
// handovers and the room the per-user caps keep for their replacements are read by the reservation itself, from the unit set
// (extraUnits), and the caps and the silence are its own, asked with params and quiet. exclude is the Instance that retries
// (nil for a new extra). ok is false when the bound refuses.
func (p *pass) extraChecks(pv *providerView, exclude *v1alpha1.Instance) (ask extraAsk, ok bool) {
	if !p.extraBoundWithout(pv, exclude) {
		return extraAsk{}, false
	}
	ask = extraAsk{deployment: p.key.Namespace + "/" + p.key.Name, headroom: p.headroomOf(pv),
		sizeOf: func(md *v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error) {
			plan, err := checkServed(md)
			if err != nil {
				return v1alpha1.BatchParameters{}, err
			}
			return partitionSizingFor(pv, plan.gpus, plan.workers, md.Spec.LeaseLimit)
		},
		sizeRecorded: func(md *v1alpha1.ModelDeployment, in *v1alpha1.Instance) (v1alpha1.BatchParameters, error) {
			return partitionSizingFor(pv, in.Spec.Launch.GPUs, v1alpha1.LaunchWorkers(in.Spec.Launch), md.Spec.LeaseLimit)
		}}
	if exclude != nil {
		ask.exclude = exclude.UID
	}
	return ask, true
}

// headroomOf is the admission headroom H of an extra on the Provider (HBEP-7, Over-allocation and qualification (beta), Its
// checks, admission headroom; the science agent's ruling on #708's delta reviews, amendment #711): the number of missing
// PRIMARIES, counted as primaries, of the OTHER deployments that the Provider allows, at the time of the check, and 0 when
// there are none, so an extra never takes a slot a primary needs at the time, and nothing is cancelled to make room. It is the callback a reservation calls under the Provider's lock
// (slotAsk.headroom), reading through the uncached Reader one List of the Providers and one of the ModelDeployments of
// every namespace, and, for each deployment that allows the Provider, its Instances and Allocations by its model-deployment
// label FROM THE RESERVATION'S SNAPSHOT (slotSnapshot), the one the count's reads and the extra's unit set are derived from,
// so a primary seen there has its Allocation counted as the count counted it. A deployment with a missing primary may have
// NO Instance on this Provider, so no view of the Provider's own Instances can tell. A deployment counts when it is not this one, not being deleted, serves, wants
// Instances, has not passed its end time (by the clock read under the lock), allows the Provider (selectedProviderNames, as its own pass reads it) and its
// shortage by Allocation presence (shortageOfDeployment) is above zero.
//
// Each other deployment is read ONCE, under the Provider's lock. A spec edit that lands between that read and
// the deployment's own pass (its instances raised, its providers or end time changed) is not seen by this count: the next
// pass reads it afresh, which bounds the error to one pass, and nothing is cancelled for it (#725, from #708's review).
func (p *pass) headroomOf(pv *providerView) func(context.Context, slotSnapshot) (int, error) {
	return func(ctx context.Context, snap slotSnapshot) (int, error) {
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
			h += shortageOfDeployment(snap, md)
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

// shortageOfDeployment is shortageOf the deployment's own Instances and Allocations, from the reservation's snapshot by its
// model-deployment label in its namespace (slotSnapshot.deploymentView), as its own pass reads them (selectionOfView).
func shortageOfDeployment(snap slotSnapshot, md *v1alpha1.ModelDeployment) int {
	v := snap.deploymentView(md.Namespace, v1alpha1.LabelValue(md.Name))
	v.key = md.Namespace + "/" + md.Name
	return shortageOf(selectionOfView(md, v), int(md.Spec.Instances))
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

// appendOnce appends s to the list unless it is there.
func appendOnce(list []string, s string) []string {
	if slices.Contains(list, s) {
		return list
	}
	return append(list, s)
}

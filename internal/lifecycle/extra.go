package lifecycle

import (
	"slices"

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
		alsoLeave, ok := p.extraChecks(pv, nil)
		if !ok {
			continue
		}
		if _, err := p.provisionWith(nil, placeOpts{only: pv.name, extra: true, caps: true, alsoLeave: alsoLeave}); err != nil {
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

// extraChecks are the extra's first two checks on a Provider, the bound and the slot beyond its own handovers
// (alsoLeave), in ONE place for createExtras, which makes the extra, and ensureAllocation, which retries one whose
// Allocation's create was cut short, so the two never drift (#708, finding 4). exclude is the Instance that retries
// (nil for a new extra). The third check, the per-user caps, and the silence are the reservation's own, asked with
// params and quiet. ok is false when the bound refuses.
func (p *pass) extraChecks(pv *providerView, exclude *v1alpha1.Instance) (alsoLeave int, ok bool) {
	if !p.extraBoundWithout(pv, exclude) {
		return 0, false
	}
	return p.extraAlsoLeaveWithout(pv, exclude), true
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
	n := 0
	chosen, _ := selectPrimaries(p.selectionWithout(exclude), p.desiredCount())
	for _, c := range chosen {
		if c.Labels[v1alpha1.LabelProvider] != pv.name || p.replaced(c) {
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

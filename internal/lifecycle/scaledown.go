package lifecycle

import (
	"fmt"
	"slices"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// What step 7 takes when the deployment has surplus or its count is lowered (beta; HBEP-7, Reconcile loop, step 7;
// Over-allocation and qualification (beta), How it counts, What step 7 takes; #616, PR 3). One selection decides it
// (selectPrimaries); this file reads what the selection leaves over.

// scaleCause says why a scale-down takes a target (HBEP-10, The plan). overAllocationTrim is over-allocation's own
// bound at work: the target is non-ready surplus beyond k, or a ranked-out peer, which the selection takes at the
// current count with no edit to the spec. lowered is everything else, a ready surplus Instance that is not a
// ranked-out peer included: a person lowered spec.instances or overAllocation. An absent cause reads as lowered.
type scaleCause string

const (
	scaleLowered            scaleCause = "lowered"
	scaleOverAllocationTrim scaleCause = "overAllocationTrim"
)

// frontProbeInterval is the front's probe interval, which a ranked-out peer's drain waits for after its successor is
// ready (HBEP-7, How it counts, What step 7 takes, 1; the science agent's ruling on #616's plan, Q5). It equals
// internal/front's DefaultProbeInterval, which a test pins. A front probeInterval set by hand is not seen: the
// launcher never sets one.
const frontProbeInterval = 10 * time.Second

// selection is the pass's own Instances and Allocations, read by the selection.
func (p *pass) selection(passed *v1alpha1.Instance) selection {
	return selection{insts: p.insts, stale: p.stale, allocOf: p.allocOf, passed: passed}
}

// desiredCount is spec.instances, the number of primaries the selection chooses; all of them for a pass with no
// ModelDeployment.
func (p *pass) desiredCount() int {
	if p.md == nil {
		return len(p.insts)
	}
	return int(p.md.Spec.Instances)
}

// keepRoom returns how many non-ready surplus Instances may be left alone on a Provider: k, the deployment's
// overAllocation. The deployment's warm Allocations there (Running, hosting no live Instance) are not subtracted: a
// warm Allocation counts toward k only in the extra's creation bound (PR 4), never in the trim of surplus that
// already exists. A drained Instance's Allocation turns warm, and counting it here would have a later pass cancel a
// pending surplus Instance that a lowering left alone, outside any plan's approval and at the cost of its queue age
// (the science agent's ruling on #635, option B; HBEP-7 amendment #636).
func (p *pass) keepRoom() int {
	if p.md == nil {
		return 0
	}
	return int(p.md.Spec.OverAllocationCount())
}

// frontAdmitsAt is when the front's probe has had its interval to admit an Instance that is ready: its Healthy
// condition's last transition to True, which becomeReady writes and a restart resets (climb sets Healthy False), plus
// frontProbeInterval. ok is false while Healthy is not True. It is measured from the transition and not from
// status.readyTime, which becomeReady writes once and no restart resets: an Instance that was ready before, restarted
// and re-ranked, would otherwise have its peer drain in the pass it became ready again, before the front routes to it
// (HBEP-7, How it counts, What step 7 takes, 1).
func frontAdmitsAt(in *v1alpha1.Instance) (time.Time, bool) {
	c := apimeta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionHealthy)
	if c == nil || c.Status != metav1.ConditionTrue {
		return time.Time{}, false
	}
	return c.LastTransitionTime.Add(frontProbeInterval), true
}

// scaleUnit is what a scale-down takes of one hedge: its representative and, when it takes less than the whole hedge,
// the members it takes (only). A surplus hedge goes member by member where its Providers' room under k decides it
// (trimSurplusMembers): a member beyond k is a unit of its own, so the effect cancels that member and no other. trim
// is whether it is taken as over-allocation's own trim (non-ready surplus beyond k, a marked peer, or an extra that
// yielded its submit slot), yield names the deployment an extra yielded to (annotationYieldTo), and, for a marked
// peer, notTo is when its drain is due: not before notTo, and never while the Instance it made way for is not
// healthy (waiting).
type scaleUnit struct {
	rep     *v1alpha1.Instance
	only    []*v1alpha1.Instance
	trim    bool
	marked  bool
	waiting bool
	notTo   time.Time
	yield   string
}

// members are the Instances the unit takes: the members it names, or the whole hedge.
func (p *pass) unitMembers(u scaleUnit) []*v1alpha1.Instance {
	if len(u.only) > 0 {
		return u.only
	}
	return p.membersOf(u.rep)
}

// scaleUnits is what step 7 takes when the deployment keeps desired primaries, the least advanced first.
//
// The surplus is every Instance the selection leaves out, the dropped primaries of a lowered count among them. A
// ready one is taken, which drains it, the nearest hard end first. Of the others, up to k per Provider are
// left alone, taken in one fixed order that does not depend on desired (the selection's: most advanced, then the
// earlier job start, then the older, then the UID), and the rest are taken: pending ones first, newest first, then
// started ones. A surplus Instance with a live replacement is never left alone: it goes with its replacement, which
// is never promoted (the science agent's ruling on #616's plan, Q4). A surplus hedge with several live members is
// charged to each member's OWN Provider: a member is kept where its Provider has room under k, taking one unit of
// it there, and only the members whose Provider has none are taken, each alone, as over-allocation's own trim; the
// whole hedge goes only when no member's Provider has room (the science agent's ruling on #616, D9, 17 Nov 2026:
// "Queue age is capacity in waiting"; trimSurplusMembers). A marked peer is taken once the Instance it made way for
// has been healthy for the front's probe interval (frontAdmitsAt), and not before. A pending surplus Instance that
// yields its submit slot (p.yielding, settleYields) is taken alone and takes no room.
func (p *pass) scaleUnits(desired int) []scaleUnit {
	_, surplus := selectPrimaries(p.selection(nil), desired)
	k := p.keepRoom()
	byName := map[string]*v1alpha1.Instance{}
	for _, in := range p.insts {
		byName[in.Name] = in
	}
	room := map[string]int{} // per Provider, how many more non-ready surplus Instances may be left alone
	left := map[*v1alpha1.Instance]bool{}
	parts := map[*v1alpha1.Instance][]scaleUnit{}
	for _, rep := range surplus {
		if rep.Status.Phase == v1alpha1.InstanceReady || p.replaced(rep) {
			continue
		}
		var yields, rest []*v1alpha1.Instance
		for _, m := range p.membersOf(rep) {
			if _, ok := p.yielding[m.UID]; ok {
				yields = append(yields, m)
			} else {
				rest = append(rest, m)
			}
		}
		for _, m := range rest {
			if _, ok := room[m.Labels[v1alpha1.LabelProvider]]; !ok {
				room[m.Labels[v1alpha1.LabelProvider]] = k
			}
		}
		kept, trimmed := trimSurplusMembers(rest, room)
		switch {
		case len(yields) == 0 && len(trimmed) == 0:
			left[rep] = true
		case len(yields) == 0 && len(kept) == 0:
			// no member's Provider has room: the whole hedge goes, as one unit
		default:
			for _, m := range yields {
				parts[rep] = append(parts[rep], scaleUnit{rep: rep, only: []*v1alpha1.Instance{m}, trim: true, yield: p.yielding[m.UID]})
			}
			for _, m := range trimmed {
				parts[rep] = append(parts[rep], scaleUnit{rep: rep, only: []*v1alpha1.Instance{m}, trim: true})
			}
		}
	}
	var out []scaleUnit
	for _, rep := range slices.Backward(surplus) {
		if left[rep] {
			continue
		}
		if us, ok := parts[rep]; ok {
			out = append(out, us...)
			continue
		}
		u := scaleUnit{rep: rep, trim: rep.Status.Phase != v1alpha1.InstanceReady}
		if markedOut(rep, byName) {
			u.trim, u.marked = true, true
			if at, ok := frontAdmitsAt(byName[rep.Annotations[annotationRankedOut]]); ok {
				u.notTo = at
			} else {
				u.waiting = true
			}
		}
		out = append(out, u)
	}
	return out
}

// trimSurplusMembers splits a surplus hedge's live members by their Providers' room under k: a member is kept when
// its own Provider still has room, which it then charges one unit of, and trimmed when it has none (D9; the science
// agent's ruling on #616, 17 Nov 2026: "the trim is per Provider, not all or nothing"). room holds, for every
// Provider a member is on, how many more non-ready surplus Instances may be left alone there; the caller fills it
// with k for a Provider it has not charged yet, and it is charged here, so the surplus units that follow in the
// selection's fixed order see what this hedge took. Members come in the order given. This is the one place the
// per-Provider rule lives.
func trimSurplusMembers(members []*v1alpha1.Instance, room map[string]int) (kept, trimmed []*v1alpha1.Instance) {
	for _, m := range members {
		prov := m.Labels[v1alpha1.LabelProvider]
		if room[prov] > 0 {
			room[prov]--
			kept = append(kept, m)
			continue
		}
		trimmed = append(trimmed, m)
	}
	return kept, trimmed
}

// membersOf is the members of the hedge the Instance represents, or the Instance alone.
func (p *pass) membersOf(rep *v1alpha1.Instance) []*v1alpha1.Instance {
	for _, g := range p.primaryHedges() {
		if g.key == hedgeKey(rep) && len(g.members) > 0 {
			return g.members
		}
	}
	return []*v1alpha1.Instance{rep}
}

// replaced reports whether the Instance, or any member of its hedge, has a live replacement.
func (p *pass) replaced(rep *v1alpha1.Instance) bool {
	for _, m := range p.membersOf(rep) {
		if len(p.replacementMembers(m)) > 0 {
			return true
		}
	}
	return false
}

// rankedOutPeer raises the RankedOut event of a peer whose drain starts: replaced, regarding the peer, with the
// candidate that outranked it in related. Once per peer.
func (p *pass) rankedOutPeer(peer *v1alpha1.Instance) {
	if !p.mem.once("ranked-out/" + string(peer.UID)) {
		return
	}
	var related client.Object
	msg := fmt.Sprintf("Instance %s was outranked on Provider %s and drains: its successor is faster by more than 10%% in time per output token", peer.Name, peer.Labels[v1alpha1.LabelProvider])
	if cand := p.instanceNamed(peer.Annotations[annotationRankedOut]); cand != nil {
		related = cand
		msg = fmt.Sprintf("Instance %s was outranked on Provider %s by Instance %s and drains: its successor is faster by more than 10%% in time per output token", peer.Name, peer.Labels[v1alpha1.LabelProvider], cand.Name)
	}
	p.r.Recorder.Event(peer, related, eventReplaced, v1alpha1.ReasonRankedOut, msg)
}

// scaleDownTargets selects what a scale-down takes, from the chosen primaries prim (as primaries returns them, one
// per hedge) and the number surplus of them to drop on top of what the selection already leaves over: the deployment
// keeps len(prim) - surplus primaries (HBEP-7, Reconcile loop, step 7; Hedging (beta), Interactions, Operations;
// Over-allocation and qualification (beta), How it counts). Step 7 calls it with surplus 0, every pass; a Scale
// Operation's plan calls it with the count it would lower to.
//
// A hedge that a lowered count drops goes whole: each member is a target with its own effect, and its replacement
// hedge goes with it, never promoted, its members first. A surplus hedge left over by the selection goes member by
// member where k allows (scaleUnits): a trimmed member is a target of its own, and its hedge-mates are left alone.
// The targets come least advanced first. It is monotone: a lower count takes every
// target a higher one takes, and more, since the surplus only grows and the order that decides what is left alone
// does not depend on the count.
//
// Each target carries its cause (scaleCause): overAllocationTrim only when the selection at the current count,
// len(prim), takes it too and it is non-ready surplus beyond k, a marked peer or an extra that yielded its submit
// slot; lowered for the rest.
func (p *pass) scaleDownTargets(prim []*v1alpha1.Instance, surplus int) []scaleTarget {
	now := len(prim)
	trim := map[*v1alpha1.Instance]bool{}
	for _, u := range p.scaleUnits(now) {
		for _, m := range p.unitMembers(u) {
			trim[m] = u.trim
		}
	}
	var out []scaleTarget
	for _, u := range p.scaleUnits(max(now-surplus, 0)) {
		if u.waiting {
			continue // the Instance it made way for is not healthy: the drain waits for its next transition
		}
		if p.now.Before(u.notTo) {
			p.at(u.notTo)
			continue
		}
		// The words are the release cause of a cancelled Allocation too (endCause): "scaled down" for every target but a
		// marked peer, whose own are the ranking's, and an extra that yielded its slot.
		why := "scaled down"
		if u.marked {
			why = "ranked out: its successor serves"
		}
		if u.yield != "" {
			why = "yielded its submit slot to " + u.yield
		}
		members := p.unitMembers(u)
		for _, m := range members {
			for _, r := range p.replacementMembers(m) {
				if countable(r) {
					out = append(out, scaleTarget{instance: r, allocation: p.allocOf(r), effect: effectOf(r, p.allocOf(r)), why: "its predecessor was scaled down", cause: scaleCauseOf(trim[m])})
				}
			}
		}
		for _, m := range members {
			out = append(out, scaleTarget{instance: m, allocation: p.allocOf(m), effect: effectOf(m, p.allocOf(m)), why: why, cause: scaleCauseOf(trim[m])})
		}
	}
	return out
}

// scaleCauseOf is the cause of a target: overAllocationTrim when the selection at the current count takes it as
// over-allocation's own trim, else lowered.
func scaleCauseOf(trimmed bool) scaleCause {
	if trimmed {
		return scaleOverAllocationTrim
	}
	return scaleLowered
}

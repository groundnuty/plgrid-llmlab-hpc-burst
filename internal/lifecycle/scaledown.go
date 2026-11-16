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

// scaleUnit is one hedge that a scale-down takes: its representative, whether it is taken as over-allocation's own
// trim (non-ready surplus beyond k, or a marked peer), and, for a marked peer, when its drain is due: not before
// notTo, and never while the Instance it made way for is not healthy (waiting).
type scaleUnit struct {
	rep     *v1alpha1.Instance
	trim    bool
	marked  bool
	waiting bool
	notTo   time.Time
}

// scaleUnits is what step 7 takes when the deployment keeps desired primaries, the least advanced first.
//
// The surplus is every Instance the selection leaves out, the dropped primaries of a lowered count among them. A
// ready one is taken, which drains it, the nearest hard end first. Of the others, up to k per Provider are
// left alone, taken in one fixed order that does not depend on desired (the selection's: most advanced, then the
// earlier job start, then the older, then the UID), and the rest are taken: pending ones first, newest first, then
// started ones. A surplus Instance with a live replacement is never left alone: it goes with its replacement, which
// is never promoted (the science agent's ruling on #616's plan, Q4). A hedge with several live members is never left
// alone either: it spans Providers, and k is counted per Provider. A marked peer is taken once the Instance it
// made way for has been healthy for the front's probe interval (frontAdmitsAt), and not before.
func (p *pass) scaleUnits(desired int) []scaleUnit {
	_, surplus := selectPrimaries(p.selection(nil), desired)
	k := p.keepRoom()
	byName := map[string]*v1alpha1.Instance{}
	for _, in := range p.insts {
		byName[in.Name] = in
	}
	left, used := map[*v1alpha1.Instance]bool{}, map[string]int{}
	for _, rep := range surplus {
		if rep.Status.Phase == v1alpha1.InstanceReady || len(p.membersOf(rep)) > 1 || p.replaced(rep) {
			continue
		}
		if prov := rep.Labels[v1alpha1.LabelProvider]; used[prov] < k {
			left[rep] = true
			used[prov]++
		}
	}
	var out []scaleUnit
	for _, rep := range slices.Backward(surplus) {
		if left[rep] {
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
// A hedge goes whole: each member is a target with its own effect, and its replacement hedge goes with it, never
// promoted, its members first. The targets come least advanced first. It is monotone: a lower count takes every
// target a higher one takes, and more, since the surplus only grows and the order that decides what is left alone
// does not depend on the count.
//
// Each target carries its cause (scaleCause): overAllocationTrim only when the selection at the current count,
// len(prim), takes it too and it is non-ready surplus beyond k or a marked peer; lowered for the rest.
func (p *pass) scaleDownTargets(prim []*v1alpha1.Instance, surplus int) []scaleTarget {
	now := len(prim)
	trim := map[*v1alpha1.Instance]bool{}
	for _, u := range p.scaleUnits(now) {
		trim[u.rep] = u.trim
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
		// marked peer, whose own are the ranking's.
		cause, why := scaleLowered, "scaled down"
		if trim[u.rep] {
			cause = scaleOverAllocationTrim
		}
		if u.marked {
			why = "ranked out: its successor serves"
		}
		for _, m := range p.membersOf(u.rep) {
			for _, r := range p.replacementMembers(m) {
				if countable(r) {
					out = append(out, scaleTarget{instance: r, allocation: p.allocOf(r), effect: effectOf(r, p.allocOf(r)), why: "its predecessor was scaled down", cause: cause})
				}
			}
		}
		for _, m := range p.membersOf(u.rep) {
			out = append(out, scaleTarget{instance: m, allocation: p.allocOf(m), effect: effectOf(m, p.allocOf(m)), why: why, cause: cause})
		}
	}
	return out
}

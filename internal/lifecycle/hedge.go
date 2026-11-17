package lifecycle

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A hedge as a unit (beta; HBEP-7, Hedging (beta); #604, PR 1).
//
// A hedge is the Allocations submitted for one need: one missing primary or one primary's replacement. Its first
// member is created as any Instance is; every later member carries AnnotationHedgeGroup, the first member's name,
// written in its create request (placeOpts.hedgeGroup). The hedge is the first member and the live Instances that
// name it, found among the Instances the pass lists by owner, never by a scan of names. An Instance that names no
// hedge and that no member names is a hedge of one, as in alpha.
//
// A hedge counts once: as one primary in step 7 (primaries) and as its predecessor's one live replacement in step 8
// (replacementOf). The member whose Instance becomes ready first wins (hedgeWinner), and the pass that sees it
// resolves the hedge (resolveHedges): a pending loser is cancelled, a started one ends with HedgeLost and keeps its
// Allocation as warm capacity, and a ready one drains first. HedgeLost is a clean end, like Drained (cleanEnd).
//
// The grouping and the winner rule are pure functions over a list of Instances, so the reserved count (PR 2), which
// reads another deployment's Instances under a Provider's lock, reads the hedge exactly as the pass does.

// annotationHedgeLost is on a hedge loser that was ready when the hedge was resolved, while it drains: the
// winner's name. Its drain then ends with HedgeLost rather than Drained (drainEndReason), across a restart.
const annotationHedgeLost = v1alpha1.Group + "/hedge-lost"

// annotationCandidateOf is on a hedge's started loser that was kept as an over-allocation candidate at the win
// (HBEP-7, Over-allocation and qualification (beta), Hedging's started losers; #616, PR 5): the winner's name. It is
// written in the pass that resolves the hedge, before the loser's checks end, so a restarted controller reads it
// from the object. It stays on the Instance for good, which is how its end is read: a marked Instance ends with
// HedgeLost only at its ranking point, after it passed its checks (startOutcomeOf). It is a lifecycle-owned
// annotation, proposed for api/v1alpha1/constants.go beside annotationRankedOut.
const annotationCandidateOf = v1alpha1.Group + "/candidate-of"

// candidateOf is the winner a marked Instance is the candidate of, or "".
func candidateOf(in *v1alpha1.Instance) string { return in.Annotations[annotationCandidateOf] }

// candidateWinnerOf is the winner a kept started loser still waits on: the Instance its mark names, when the Instance
// has never been ready and that winner is countable (not ended, draining or deleting) and not stale. Such a
// candidate is the surplus of its Provider whatever its rank, and is ranked against that winner alone
// (HBEP-7, Hedging's started losers, How it counts). One whose winner has gone is chosen like any
// Instance: the round's primary failed, so its extra is chosen in its place and no Allocation is made (How it counts).
// named finds an Instance by name among the deployment's. The pass and the selection both read it through here, so
// they never disagree.
func candidateWinnerOf(in *v1alpha1.Instance, named func(string) *v1alpha1.Instance, stale map[types.UID]bool) *v1alpha1.Instance {
	name := candidateOf(in)
	if name == "" || in.Status.ReadyTime != nil || ended(in) {
		return nil
	}
	w := named(name)
	if w == nil || !countable(w) || stale[w.UID] {
		return nil
	}
	return w
}

// hedgeKey is the hedge an Instance belongs to: its AnnotationHedgeGroup, or its own name for a first member or a
// hedge of one. A kept started loser, which carries annotationCandidateOf, is a hedge of one of its own from the
// win on, under a key no member's can be: it is ranked against its hedge's winner and is no part of the hedge's
// count, so the winner is never read as a member beside it (hedgeWinner would keep choosing the earlier ready
// member, and resolveHedges would end the candidate once it served).
func hedgeKey(in *v1alpha1.Instance) string {
	if candidateOf(in) != "" {
		return "candidate/" + in.Name
	}
	if k := in.Annotations[v1alpha1.AnnotationHedgeGroup]; k != "" {
		return k
	}
	return in.Name
}

// hedgeGroup is one hedge's members, in the order the Instances were given.
type hedgeGroup struct {
	key     string
	members []*v1alpha1.Instance
}

// groupHedges groups the Instances that keep keeps by hedgeKey. The groups come in the order of each one's first
// member in insts, never a map's order, so a hedge of one keeps its place.
func groupHedges(insts []*v1alpha1.Instance, keep func(*v1alpha1.Instance) bool) []hedgeGroup {
	at := map[string]int{}
	var out []hedgeGroup
	for _, in := range insts {
		if !keep(in) {
			continue
		}
		k := hedgeKey(in)
		i, ok := at[k]
		if !ok {
			i = len(out)
			at[k] = i
			out = append(out, hedgeGroup{key: k})
		}
		out[i].members = append(out[i].members, in)
	}
	return out
}

// olderThan orders Instances by creation time, then by name.
func olderThan(a, b *v1alpha1.Instance) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// hedgeWinner returns the member whose Instance became ready first, or nil when none is ready: the earliest
// status.readyTime, then the older member by creation time, then by name (HBEP-7, Hedging (beta), The winner).
func hedgeWinner(members []*v1alpha1.Instance) *v1alpha1.Instance {
	var w *v1alpha1.Instance
	for _, in := range members {
		if in.Status.Phase != v1alpha1.InstanceReady || in.Status.ReadyTime == nil {
			continue
		}
		switch {
		case w == nil:
			w = in
		case in.Status.ReadyTime.Before(w.Status.ReadyTime):
			w = in
		case in.Status.ReadyTime.Equal(w.Status.ReadyTime) && olderThan(in, w):
			w = in
		}
	}
	return w
}

// memberStage is how far a member has come: 2 ready, 1 its job started, 0 pending (its job has not started, or it
// has no Allocation yet).
func memberStage(in *v1alpha1.Instance, a *v1alpha1.Allocation) int {
	switch {
	case in.Status.Phase == v1alpha1.InstanceReady:
		return 2
	case jobStarted(a):
		return 1
	}
	return 0
}

// jobStarted reports whether the Allocation's job has started, as the Provider last read it.
func jobStarted(a *v1alpha1.Allocation) bool {
	return a != nil && a.Status.AtProvider.StartTime != nil
}

// hedgeRepresentative is the member that stands for the hedge where one Instance is read: the most advanced
// (ready, then started, then pending), the winner's rule among ready ones, else the older. So a hedge reads as
// started when any member's job has started, which ReplacementLate's check reads.
func hedgeRepresentative(members []*v1alpha1.Instance, allocOf func(*v1alpha1.Instance) *v1alpha1.Allocation) *v1alpha1.Instance {
	if w := hedgeWinner(members); w != nil {
		return w
	}
	var r *v1alpha1.Instance
	for _, in := range members {
		if r == nil {
			r = in
			continue
		}
		si, sr := memberStage(in, allocOf(in)), memberStage(r, allocOf(r))
		if si > sr || (si == sr && olderThan(in, r)) {
			r = in
		}
	}
	return r
}

// cleanEnd reports whether an end reason is no failure: a drain, a hedge loss, or a ranking (beta; HBEP-7,
// Over-allocation and qualification (beta), RankedOut is no failure). None releases the Allocation by itself
// (releaseRequired), counts towards the class's failure streak (failedEnd) or marks the Instance unhealthy (end).
func cleanEnd(reason string) bool {
	return reason == v1alpha1.ReasonDrained || reason == v1alpha1.ReasonHedgeLost || reason == v1alpha1.ReasonRankedOut
}

// drainEndReason is the reason a drained Instance ends with: HedgeLost for a hedge loser that drained, RankedOut
// for a peer a candidate outranked (the ranked-out mark, rank.go), else Drained.
func drainEndReason(in *v1alpha1.Instance) string {
	switch {
	case in.Annotations[annotationHedgeLost] != "":
		return v1alpha1.ReasonHedgeLost
	case in.Annotations[annotationRankedOut] != "":
		return v1alpha1.ReasonRankedOut
	}
	return v1alpha1.ReasonDrained
}

// allocOf is the pass's Allocation of an Instance, or nil.
func (p *pass) allocOf(in *v1alpha1.Instance) *v1alpha1.Allocation {
	return p.allocs[in.Spec.AllocationRef.Name]
}

// primaryCandidates are the Instances that count towards capacity, each member of a hedge apart (primaryCandidatesOf).
func (p *pass) primaryCandidates() []*v1alpha1.Instance {
	return primaryCandidatesOf(p.insts, p.stale)
}

// primaryHedges are the primaries' hedges: the candidates grouped by hedge.
func (p *pass) primaryHedges() []hedgeGroup {
	return groupHedges(p.primaryCandidates(), func(*v1alpha1.Instance) bool { return true })
}

// replacementMembers returns every live member of the primary's replacement hedge: the Instances that have not
// ended and name it as their predecessor.
func (p *pass) replacementMembers(in *v1alpha1.Instance) []*v1alpha1.Instance {
	var out []*v1alpha1.Instance
	for _, o := range p.insts {
		if !ended(o) && o.Annotations[v1alpha1.AnnotationPredecessor] == in.Name {
			out = append(out, o)
		}
	}
	return out
}

// resolveHedges resolves every hedge whose member is ready: that member wins, and each other live member loses by
// its own state (HBEP-7, Hedging (beta), The winner and the losers; step 6). A started loser may instead be kept as
// an over-allocation candidate (keepsStartedLoser), which takes it out of the hedge. The winner is read as this pass left
// it, after its phase was advanced, and a winner whose Allocation is being deleted resolves nothing, so no loser
// ends beside a winner that no longer serves. Losers already draining or ended are skipped, so a pass cut short in
// the middle is finished by the next one.
func (p *pass) resolveHedges() error {
	if p.md == nil || p.shutdown {
		return nil
	}
	var errs []error
	for _, g := range groupHedges(p.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !p.stale[in.UID] }) {
		if len(g.members) < 2 {
			continue
		}
		w := hedgeWinner(g.members)
		if w == nil {
			continue
		}
		if a := p.allocOf(w); a == nil || !a.DeletionTimestamp.IsZero() {
			continue
		}
		// The winner's Allocation is read again before any loser ends: one deleted since the pass's own read (by an
		// operator, or its Provider's controller) resolves nothing, and the next pass sees the winner end and the
		// hedge go on (HBEP-7, Hedging (beta); #604 plan §10, path 2).
		if serving, err := p.allocationStillServes(w); err != nil || !serving {
			errs = append(errs, err)
			continue
		}
		for _, l := range g.members {
			if l != w {
				errs = append(errs, p.loseHedge(g.key, l, w))
			}
		}
	}
	return errors.Join(errs...)
}

// allocationStillServes re-reads the Instance's Allocation: false when it is gone, being deleted, or no longer
// Running.
func (p *pass) allocationStillServes(in *v1alpha1.Instance) (bool, error) {
	var a v1alpha1.Allocation
	err := p.r.Reader.Get(p.ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Spec.AllocationRef.Name}, &a)
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read the Allocation of hedge winner %s: %w", in.Name, err)
	}
	return a.DeletionTimestamp.IsZero() && a.Status.AtProvider.Phase == v1alpha1.AllocationRunning, nil
}

// loseHedge takes one loser out of its resolved hedge (HBEP-7, Hedging (beta)): a pending one is cancelled, its
// Allocation deleted; a started one ends with HedgeLost and keeps its Allocation, whose retention policy then
// applies (anchorIfLast writes idle-since), or, with over-allocation on and no surplus or warm Allocation of the
// deployment on its Provider, is kept as a candidate (keepsStartedLoser); a ready one drains first, marked so its
// drain ends with HedgeLost. Each that ends raises replaced with reason HedgeLost, regarding the loser, with the
// winner related. No approval is asked: cancelling a hedge loser needs none (HBEP-10, What needs approval).
func (p *pass) loseHedge(key string, l, w *v1alpha1.Instance) error {
	a := p.allocOf(l)
	if l.Status.Phase != v1alpha1.InstanceReady && jobStarted(a) && p.keepsStartedLoser(l) {
		return p.markCandidate(l, w)
	}
	// The words are the controller's own, so they are the release cause of a pending loser's Allocation too
	// (endCause): "Instance <loser> ended: hedge lost to <winner>, ...".
	msg := fmt.Sprintf("hedge lost to %s, on Provider %s (hedge %s)", w.Name, w.Labels[v1alpha1.LabelProvider], key)
	if p.mem.once("hedge-lost/" + string(l.UID)) {
		p.r.Recorder.Event(l, w, eventReplaced, v1alpha1.ReasonHedgeLost, msg)
	}
	switch {
	case l.Status.Phase == v1alpha1.InstanceReady:
		if err := p.setMeta(l, func(n *v1alpha1.Instance) {
			if n.Annotations == nil {
				n.Annotations = map[string]string{}
			}
			n.Annotations[annotationHedgeLost] = w.Name
		}); err != nil {
			return err
		}
		return p.startDrain(l)
	case jobStarted(a):
		return p.end(l, a, v1alpha1.ReasonHedgeLost, msg, false)
	default:
		return p.end(l, a, v1alpha1.ReasonHedgeLost, msg, true)
	}
}

// keepsStartedLoser reports whether a hedge's started loser, whose job has started and which is not ready, is kept
// as an over-allocation candidate at the win, not ended (HBEP-7, Over-allocation and qualification (beta), Hedging's
// started losers; D1): k is at least 1, it has never been ready, the deployment has no surplus Instance on its
// Provider in the selection but it, and no warm Allocation there (warmOf, the extra's bound). The loser is then that
// Provider's one surplus, and k's bound holds. Otherwise it ends at the win, as Hedging says.
func (p *pass) keepsStartedLoser(l *v1alpha1.Instance) bool {
	if p.md == nil || p.md.Spec.OverAllocationCount() < 1 || l.Status.ReadyTime != nil {
		return false
	}
	pv := p.pvOf(l.Labels)
	if pv == nil || len(p.warmOf(pv)) > 0 {
		return false
	}
	_, surplus := selectPrimaries(p.selection(nil), p.desiredCount())
	for _, rep := range surplus {
		for _, m := range p.membersOf(rep) {
			if m != l && m.Labels[v1alpha1.LabelProvider] == pv.name {
				return false
			}
		}
	}
	return true
}

// markCandidate keeps a started loser as a candidate: its mark names the winner. The write is the whole act; its
// Instance runs its checks as any does, and the ranking (rankCandidate) decides its end. Once per loser.
func (p *pass) markCandidate(l, w *v1alpha1.Instance) error {
	return p.setMeta(l, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		n.Annotations[annotationCandidateOf] = w.Name
	})
}

// hedgeProviders describes, for Progressing's message, each hedge that has more than one live member and no
// winner yet: its Providers in the deployment's allowed order, so the message does not change with the order the
// Instances are listed in (HBEP-7, Hedging (beta), Events and conditions).
func (p *pass) hedgeProviders() []string {
	rank := map[string]int{}
	for i, pv := range p.providers {
		rank[pv.name] = i
	}
	at := func(name string) int {
		if i, ok := rank[name]; ok {
			return i
		}
		return len(rank) // not allowed any more: after the allowed ones
	}
	var out []string
	for _, g := range groupHedges(p.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !p.stale[in.UID] }) {
		if len(g.members) < 2 || hedgeWinner(g.members) != nil {
			continue
		}
		var provs []string
		for _, m := range g.members {
			provs = append(provs, m.Labels[v1alpha1.LabelProvider])
		}
		sort.SliceStable(provs, func(i, j int) bool {
			if at(provs[i]) != at(provs[j]) {
				return at(provs[i]) < at(provs[j])
			}
			return provs[i] < provs[j]
		})
		out = append(out, fmt.Sprintf("hedge %s spans Providers %s", g.key, strings.Join(provs, ", ")))
	}
	return out
}

// scaleEffect is what a scale-down does to one Instance and its Allocation.
type scaleEffect string

const (
	// effectCancelPending ends the Instance and deletes its Allocation, whose job has not started.
	effectCancelPending scaleEffect = "cancelPending"
	// effectEndKeepAllocation ends the Instance, not yet ready, and leaves its started Allocation to its retention
	// policy.
	effectEndKeepAllocation scaleEffect = "endKeepAllocation"
	// effectDrain drains the ready Instance; it ends once drained, and its Allocation follows its policy.
	effectDrain scaleEffect = "drain"
)

// scaleTarget is one Instance a scale-down takes, with its Allocation (nil when there is none) and its effect. A
// Scale Operation's plan lists the same targets (HBEP-10; #589 PR 4), so a plan and step 7 never choose apart.
type scaleTarget struct {
	instance   *v1alpha1.Instance
	allocation *v1alpha1.Allocation
	effect     scaleEffect
	why        string
	// cause is why step 7 takes it (scaleCause); absent reads as lowered.
	cause scaleCause
}

// effectOf is a scale-down's effect on one Instance, by its own state.
func effectOf(in *v1alpha1.Instance, a *v1alpha1.Allocation) scaleEffect {
	switch {
	case in.Status.Phase == v1alpha1.InstanceReady:
		return effectDrain
	case jobStarted(a):
		return effectEndKeepAllocation
	}
	return effectCancelPending
}

// applyScaleTarget carries out one scale-down target (HBEP-7, Release: scale-down).
func (p *pass) applyScaleTarget(t scaleTarget) error {
	switch t.effect {
	case effectDrain:
		if t.instance.Annotations[annotationRankedOut] != "" {
			p.rankedOutPeer(t.instance)
		}
		return p.startDrain(t.instance)
	case effectEndKeepAllocation:
		return p.end(t.instance, t.allocation, v1alpha1.ReasonDrained, t.why, false)
	default:
		return p.end(t.instance, t.allocation, v1alpha1.ReasonDrained, t.why, true)
	}
}

package lifecycle

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

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

// hedgeKey is the hedge an Instance belongs to: its AnnotationHedgeGroup, or its own name for a first member or a
// hedge of one.
func hedgeKey(in *v1alpha1.Instance) string {
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

// cleanEnd reports whether an end reason is no failure: a drain, or a hedge loss. Neither releases the Allocation
// by itself (releaseRequired), counts towards the class's failure streak (failedEnd) or marks the Instance
// unhealthy (end).
func cleanEnd(reason string) bool {
	return reason == v1alpha1.ReasonDrained || reason == v1alpha1.ReasonHedgeLost
}

// drainEndReason is the reason a drained Instance ends with: HedgeLost for a hedge loser that drained, else
// Drained.
func drainEndReason(in *v1alpha1.Instance) string {
	if in.Annotations[annotationHedgeLost] != "" {
		return v1alpha1.ReasonHedgeLost
	}
	return v1alpha1.ReasonDrained
}

// allocOf is the pass's Allocation of an Instance, or nil.
func (p *pass) allocOf(in *v1alpha1.Instance) *v1alpha1.Allocation {
	return p.allocs[in.Spec.AllocationRef.Name]
}

// primaryCandidates are the Instances that count towards capacity, each member of a hedge apart: not draining,
// ended or deleting, not stale, and not the replacement of a predecessor that still counts.
func (p *pass) primaryCandidates() []*v1alpha1.Instance {
	counted := map[string]bool{}
	for _, in := range p.insts {
		if countable(in) && !p.stale[in.UID] {
			counted[in.Name] = true
		}
	}
	var out []*v1alpha1.Instance
	for _, in := range p.insts {
		if !countable(in) || p.stale[in.UID] {
			continue
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && counted[pred] {
			continue
		}
		out = append(out, in)
	}
	return out
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
// its own state (HBEP-7, Hedging (beta), The winner and the losers; step 6). The winner is read as this pass left
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
		for _, l := range g.members {
			if l != w {
				errs = append(errs, p.loseHedge(g.key, l, w))
			}
		}
	}
	return errors.Join(errs...)
}

// loseHedge takes one loser out of its resolved hedge (HBEP-7, Hedging (beta)): a pending one is cancelled, its
// Allocation deleted; a started one ends with HedgeLost and keeps its Allocation, whose retention policy then
// applies (anchorIfLast writes idle-since); a ready one drains first, marked so its drain ends with HedgeLost.
// Each raises replaced with reason HedgeLost, regarding the loser, with the winner related. No approval is asked:
// cancelling a hedge loser needs none (HBEP-10, What needs approval).
func (p *pass) loseHedge(key string, l, w *v1alpha1.Instance) error {
	msg := fmt.Sprintf("hedge %s: Instance %s on Provider %s served first", key, w.Name, w.Labels[v1alpha1.LabelProvider])
	if p.mem.once("hedge-lost/" + string(l.UID)) {
		p.r.Recorder.Event(l, w, eventReplaced, v1alpha1.ReasonHedgeLost, msg)
	}
	a := p.allocOf(l)
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

// hedgeProviders describes, for Progressing's message, each hedge that has more than one live member and no
// winner yet: its Providers in the members' order (HBEP-7, Hedging (beta), Events and conditions).
func (p *pass) hedgeProviders() []string {
	var out []string
	for _, g := range groupHedges(p.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !p.stale[in.UID] }) {
		if len(g.members) < 2 || hedgeWinner(g.members) != nil {
			continue
		}
		var provs []string
		for _, m := range g.members {
			provs = append(provs, m.Labels[v1alpha1.LabelProvider])
		}
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

// scaleDownTargets selects what a scale-down by surplus primaries takes, from the primaries prim (one per hedge,
// as primaries returns them) (HBEP-7, Reconcile loop, step 7; Hedging (beta), Interactions, Operations). A hedge is
// one primary, ranked by its most advanced member: pending hedges first, newest first by their first member's
// creation; then starting ones, newest first; then ready ones with the nearest hard end. It goes whole: each member
// is a target with its own effect, and its replacement hedge goes with it, never promoted, its members first.
func (p *pass) scaleDownTargets(prim []*v1alpha1.Instance, surplus int) []scaleTarget {
	if surplus <= 0 {
		return nil
	}
	groups := map[string][]*v1alpha1.Instance{}
	for _, g := range p.primaryHedges() {
		groups[g.key] = g.members
	}
	membersOf := func(rep *v1alpha1.Instance) []*v1alpha1.Instance {
		if m := groups[hedgeKey(rep)]; len(m) > 0 {
			return m
		}
		return []*v1alpha1.Instance{rep}
	}
	rank := func(rep *v1alpha1.Instance) int {
		best := 0
		for _, m := range membersOf(rep) {
			best = max(best, memberStage(m, p.allocOf(m)))
		}
		return best
	}
	first := func(rep *v1alpha1.Instance) *v1alpha1.Instance {
		ms := slices.Clone(membersOf(rep))
		sort.SliceStable(ms, func(i, j int) bool { return olderThan(ms[i], ms[j]) })
		return ms[0]
	}
	victims := slices.Clone(prim)
	sort.SliceStable(victims, func(i, j int) bool {
		a, b := victims[i], victims[j]
		if ra, rb := rank(a), rank(b); ra != rb {
			return ra < rb
		}
		if rank(a) == 2 {
			return leaseBefore(a, b)
		}
		fa, fb := first(a), first(b)
		return fb.CreationTimestamp.Before(&fa.CreationTimestamp) // newest first
	})
	var out []scaleTarget
	for _, rep := range victims[:min(surplus, len(victims))] {
		for _, m := range membersOf(rep) {
			for _, r := range p.replacementMembers(m) {
				if countable(r) {
					out = append(out, scaleTarget{instance: r, allocation: p.allocOf(r), effect: effectOf(r, p.allocOf(r)), why: "its predecessor was scaled down"})
				}
			}
		}
		for _, m := range membersOf(rep) {
			out = append(out, scaleTarget{instance: m, allocation: p.allocOf(m), effect: effectOf(m, p.allocOf(m)), why: "scaled down"})
		}
	}
	return out
}

// applyScaleTarget carries out one scale-down target (HBEP-7, Release: scale-down).
func (p *pass) applyScaleTarget(t scaleTarget) error {
	switch t.effect {
	case effectDrain:
		return p.startDrain(t.instance)
	case effectEndKeepAllocation:
		return p.end(t.instance, t.allocation, v1alpha1.ReasonDrained, t.why, false)
	default:
		return p.end(t.instance, t.allocation, v1alpha1.ReasonDrained, t.why, true)
	}
}

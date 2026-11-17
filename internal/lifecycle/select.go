package lifecycle

import (
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The selection of a deployment's primaries (beta; HBEP-7, Over-allocation and qualification (beta), How it
// counts; #616, PR 3).
//
// With over-allocation on, the primaries are the spec.instances most advanced of the Instances that count, and the
// rest are the deployment's surplus. Step 6 (an Instance that passed its checks becomes ready only if the selection
// chooses it), step 7 (the count, and what a scale-down takes), step 8 (only a chosen primary gets a replacement)
// and the reserved slot (slots_reserved.go) all read the one selection below, so none of them disagrees with
// another about which Instance is a primary.
//
// selectPrimaries is a pure function of the Instances it is given and their Allocations: it writes nothing, reads
// no clock and never a map's order, so a pass and a reader that holds another deployment's objects under a
// Provider's lock (reservedOn) read it the same.

// annotationRankedOut is on a peer a candidate outranked, from the moment the outranking is decided until the peer
// ends: the name of the candidate. It is written before the candidate becomes ready, so the intent is durable before
// either Instance changes: the selection reads it (markedOut), and a pass cut short between the candidate's becoming
// ready and the peer's drain never drains the candidate in place of the peer. The peer's drain then ends with
// RankedOut (drainEndReason). It is a lifecycle-owned annotation, proposed for api/v1alpha1/constants.go.
const annotationRankedOut = v1alpha1.Group + "/ranked-out"

// selection is selectPrimaries's input.
type selection struct {
	// insts are the deployment's Instances, ended ones and a hedge's members included: the live replacements and
	// the Instance a ranked-out mark names are read from this list.
	insts []*v1alpha1.Instance
	// stale marks Instances an earlier ModelDeployment of the same name owned: they never count (nil: none).
	stale map[types.UID]bool
	// allocOf is an Instance's Allocation, or nil.
	allocOf func(*v1alpha1.Instance) *v1alpha1.Allocation
	// passed is an Instance to read as having passed its checks, whatever its status says: the one a pass is about
	// to rank, in the pass that sees it pass (nil: none).
	passed *v1alpha1.Instance
}

// selUnit is one hedge as the selection sees it: the Instance that stands for it, its members and its oldest
// member.
type selUnit struct {
	rep     *v1alpha1.Instance
	first   *v1alpha1.Instance
	members []*v1alpha1.Instance
	ready   bool
	repl    bool
	passed  bool
	started bool
	marked  bool
}

// primaryCandidatesOf are the Instances that count towards capacity, each member of a hedge apart: not draining,
// ended or deleting, not stale, and not the replacement of a predecessor that still counts.
func primaryCandidatesOf(insts []*v1alpha1.Instance, stale map[types.UID]bool) []*v1alpha1.Instance {
	counted := map[string]bool{}
	for _, in := range insts {
		if countable(in) && !stale[in.UID] {
			counted[in.Name] = true
		}
	}
	var out []*v1alpha1.Instance
	for _, in := range insts {
		if !countable(in) || stale[in.UID] {
			continue
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && counted[pred] {
			continue
		}
		out = append(out, in)
	}
	return out
}

// passedChecks reports whether the Instance has passed verification, qualification and warm-up, as its status
// says: the three conditions True and, with several workers, every worker verified and warmed up at its current
// generation. A restart of either rung resets them (climb), so a restarted Instance is no longer passed.
//
// The agent's own report that the warm-up is done for the current verification generation is not read here: the
// hook that lets an Instance become ready sits after that gate in starting, and a reader under a Provider's lock
// (reservedOn) must take no lock of the agent's backend (slots.go, the lock order). The selection and the reserved
// slot therefore read the same status alone.
func passedChecks(in *v1alpha1.Instance) bool {
	c := in.Status.Conditions
	if !isTrue(c, v1alpha1.ConditionVerified) || !isTrue(c, v1alpha1.ConditionQualified) || !isTrue(c, v1alpha1.ConditionWarmedUp) {
		return false
	}
	if several(in) {
		return everyWorker(workerNames(in), func(w string) bool { return workerWarmed(in, w) })
	}
	return true
}

// units groups the candidates by hedge and reads each hedge's rank inputs.
func (s selection) units() []selUnit {
	byName := map[string]*v1alpha1.Instance{}
	replaced := map[string]bool{}
	for _, in := range s.insts {
		byName[in.Name] = in
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && !ended(in) {
			replaced[pred] = true
		}
	}
	var out []selUnit
	for _, g := range groupHedges(primaryCandidatesOf(s.insts, s.stale), func(*v1alpha1.Instance) bool { return true }) {
		rep := hedgeRepresentative(g.members, s.allocOf)
		u := selUnit{rep: rep, first: g.members[0], members: g.members, ready: rep.Status.Phase == v1alpha1.InstanceReady}
		for _, m := range g.members {
			if olderThan(m, u.first) {
				u.first = m
			}
			u.repl = u.repl || replaced[m.Name]
			u.passed = u.passed || m == s.passed || passedChecks(m)
		}
		u.started = jobStarted(s.allocOf(rep))
		u.marked = markedOut(rep, byName)
		out = append(out, u)
	}
	return out
}

// markedOut reports whether the Instance is a marked peer: it carries the ranked-out annotation (rank.go), and the
// Instance the mark names is ready. Such a peer is surplus whatever its rank, so a pass cut short between the
// candidate's becoming ready and the peer's drain never drains the candidate (HBEP-7, How it counts, What step 7
// takes). Before the named Instance is ready the peer stays chosen.
func markedOut(in *v1alpha1.Instance, byName map[string]*v1alpha1.Instance) bool {
	name := in.Annotations[annotationRankedOut]
	if name == "" {
		return false
	}
	named := byName[name]
	return named != nil && named.DeletionTimestamp.IsZero() && named.Status.Phase == v1alpha1.InstanceReady
}

// tier is a unit's place in HBEP-7's order, lower first: ready with a live replacement, ready, not ready with a live
// replacement, passed, job started, pending. A ready Instance ranks above one that is not whatever its replacement
// (the science agent's ruling on #616's plan, Q3), so a scale-down never drains the only ready Instance in favour of
// one that is still verifying.
func (u selUnit) tier() int {
	switch {
	case u.ready && u.repl:
		return 0
	case u.ready:
		return 1
	case u.repl:
		return 2
	case u.passed:
		return 3
	case u.started:
		return 4
	}
	return 5
}

// before is the total order of the units: by tier; among ready ones the later hard end first, so that a scale-down
// drains the nearest hard end; among the others, and then among equals, the earlier job start, then the older
// creation (a hedge by its first member, as scaleDownTargets always did), then the UID, then the name.
func (s selection) before(a, b selUnit) bool {
	if ta, tb := a.tier(), b.tier(); ta != tb {
		return ta < tb
	}
	if a.ready {
		switch {
		case leaseBefore(b.rep, a.rep):
			return true
		case leaseBefore(a.rep, b.rep):
			return false
		}
	}
	sa, sb := jobStart(s.allocOf(a.rep)), jobStart(s.allocOf(b.rep))
	switch {
	case sa != nil && sb == nil:
		return true
	case sa == nil && sb != nil:
		return false
	case sa != nil && !sa.Equal(*sb):
		return sa.Before(*sb)
	}
	if !a.first.CreationTimestamp.Equal(&b.first.CreationTimestamp) {
		return a.first.CreationTimestamp.Before(&b.first.CreationTimestamp)
	}
	if a.first.UID != b.first.UID {
		return a.first.UID < b.first.UID
	}
	return a.first.Name < b.first.Name
}

// jobStart is the start time of the Allocation's job as the Provider last read it, or nil.
func jobStart(a *v1alpha1.Allocation) *time.Time {
	if a == nil || a.Status.AtProvider.StartTime == nil {
		return nil
	}
	t := a.Status.AtProvider.StartTime.Time
	return &t
}

// ordered returns the units in the selection's order.
func (s selection) ordered() []selUnit {
	us := s.units()
	sort.SliceStable(us, func(i, j int) bool { return s.before(us[i], us[j]) })
	return us
}

// selectUnits is the selection by hedge: the chosen units, the first desired in HBEP-7's order (a marked peer
// excepted: it is surplus), and the surplus units, both in that order. Instances that do not count, and a
// replacement whose predecessor still counts, are in neither: a replacement is never ranked. selectPrimaries
// reads the representatives of these units; reservedOn reads their members (a surplus hedge's every member
// holds no reserved slot).
func selectUnits(s selection, desired int) (chosen, surplus []selUnit) {
	for _, u := range s.ordered() {
		if !u.marked && len(chosen) < desired {
			chosen = append(chosen, u)
			continue
		}
		surplus = append(surplus, u)
	}
	return chosen, surplus
}

// selectPrimaries returns the deployment's chosen primaries, the first desired of its Instances in HBEP-7's order
// (a marked peer excepted: it is surplus), and its surplus, both in that order, one Instance per hedge: the hedge's
// representative (hedgeRepresentative). Instances that do not count, and a replacement whose predecessor still
// counts, are in neither: a replacement is never ranked.
func selectPrimaries(s selection, desired int) (chosen, surplus []*v1alpha1.Instance) {
	cu, su := selectUnits(s, desired)
	for _, u := range cu {
		chosen = append(chosen, u.rep)
	}
	for _, u := range su {
		surplus = append(surplus, u.rep)
	}
	return chosen, surplus
}

// selectionOfView is the selection a deployment's own pass makes, from its Instances and Allocations read by label
// (reservedOn's views, the yield's re-read of a deployment): its Instances in the pass's order, the ones an earlier
// ModelDeployment of the same name owned marked stale, and its Allocations by name.
func selectionOfView(md *v1alpha1.ModelDeployment, v deploymentView) selection {
	var insts []*v1alpha1.Instance
	stale := map[types.UID]bool{}
	for _, in := range v.insts {
		owned, old := ownedBy(in, md.Name, md)
		if owned || old {
			insts = append(insts, in)
		}
		if old {
			stale[in.UID] = true
		}
	}
	sort.Slice(insts, func(i, j int) bool {
		a, b := insts[i], insts[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Name < b.Name
	})
	return selection{insts: insts, stale: stale, allocOf: func(in *v1alpha1.Instance) *v1alpha1.Allocation { return v.allocs[in.Spec.AllocationRef.Name] }}
}

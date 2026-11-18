package lifecycle

import (
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// countEachMember is the one rule for a hedge in the extra's unit set (extraUnits; the science agent's ruling on #751,
// 18 Nov 2026): true, each live member of an unresolved hedge counts on its own Provider, as a primary there that will
// hand over if it wins, since any member may win and then holds the handover slot there; false, the hedge counts once, at
// its representative's Provider. A hedge with a winner counts its winner alone either way. It is a variable so that a row
// can read the rule both ways; nothing else sets it.
var countEachMember = true

// unitRole is what a unit is for.
type unitRole int

const (
	// roleHandover is a primary's future replacement: a Running chosen primary's or a hedge winner's, whose slot the reserved
	// slots already hold, or a chosen primary's that is pending or in flight, whose slot nothing holds yet.
	roleHandover unitRole = iota
	// roleOwn is a replacement's own Allocation, not made yet (an in-flight replacement): its slot is already one of the
	// reserved slots, and its size is in no count until the Allocation is listed.
	roleOwn
)

// extraUnit is one handover an extra must leave room for.
type extraUnit struct {
	// uid is the Instance the unit is of; with role it is the unit's identity, so none counts twice.
	uid  types.UID
	role unitRole
	// deployment is the Instance's ModelDeployment (namespace/name), and key the Allocation (namespace/name) a roleOwn unit
	// waits for.
	deployment, key string
	// reserved is whether the unit's slot is already one of the reserved slots (a Running primary's, a winner's, an in-flight
	// replacement's own), so the extra's slot check leaves none more for it; it still counts for the caps.
	reserved bool
	// size is what the unit's Allocation requests, for the per-user caps: its own recorded launch's, for an Instance that
	// exists (roleOwn), or the current spec's, for a replacement that does not exist yet (roleHandover); zero when it cannot
	// be sized. gpus is its GPU count, beside it, from the same source even when the size is zero: a unit carries its GPU
	// count, so the sums can be taken Provider-wide or per QOS (unitTerms is the one place the units are summed).
	size v1alpha1.BatchParameters
	gpus int32
}

// unitSizing is how the unit set sizes an Allocation on the Provider, read under the Provider's lock (extraAsk.sizing).
// sizeOf is the size of a replacement that does not exist yet on this Provider, a handing-over primary's next one: the
// deployment's CURRENT served plan and lease limit through the Provider's partition (partitionSizingFor). sizeRecorded is
// the size of the Allocation of an Instance that exists, from the Instance's RECORDED launch (its GPUs a worker and its
// workers), as ensureAllocation will request it. Both read nothing but their arguments and the Provider's view.
type unitSizing struct {
	sizeOf       func(*v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error)
	sizeRecorded func(*v1alpha1.ModelDeployment, *v1alpha1.Instance) (v1alpha1.BatchParameters, error)
}

// unitAsk is what the unit set needs to know of the extra that asks.
type unitAsk struct {
	// deployment is the asking deployment (namespace/name): it counts like any other.
	deployment string
	// exclude is the asking Instance, the cut-short extra that retries (zero for a new one): it is left out of its
	// deployment's selection, so a retry never counts itself.
	exclude types.UID
	unitSizing
}

// future is the size of a handing-over primary's next replacement, which does not exist yet: the CURRENT spec's, and its
// GPU count (zero when the spec cannot be sized).
func (a unitAsk) future(md *v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, int32) {
	if md == nil || a.sizeOf == nil {
		return v1alpha1.BatchParameters{}, 0
	}
	b, err := a.sizeOf(md)
	if err != nil {
		return v1alpha1.BatchParameters{}, 0
	}
	return b, b.GPUs
}

// recorded is the size of the Allocation of an Instance that exists, from its RECORDED launch, never the spec's now, and
// its GPU count, which the launch gives even when the size cannot be computed.
func (a unitAsk) recorded(md *v1alpha1.ModelDeployment, in *v1alpha1.Instance) (v1alpha1.BatchParameters, int32) {
	gpus := in.Spec.Launch.GPUs * v1alpha1.LaunchWorkers(in.Spec.Launch)
	if md == nil || a.sizeRecorded == nil {
		return v1alpha1.BatchParameters{}, gpus
	}
	b, err := a.sizeRecorded(md, in)
	if err != nil {
		return v1alpha1.BatchParameters{}, gpus
	}
	return b, gpus
}

// handoverReads are the reads an extra's unit set is made from, all made under the Provider's lock by the reserved count
// (reservedAndReadsOn): the deployments' Instances and Allocations by label on every Provider (deploymentViews), their
// ModelDeployments, and the reserved slots.
type handoverReads struct {
	provider string
	profile  *v1alpha1.PlatformProfileSpec
	views    []deploymentView
	mds      map[string]*v1alpha1.ModelDeployment
	reserved []reservedSlot
}

// extraUnits returns the handovers an extra must leave room for on the Provider, from the reads the Provider's count has
// made under its lock, and no others (HBEP-7, Over-allocation and qualification (beta), Its checks; #725, #751's round).
// It is the ONE reader of the extra's two terms: how many slots it leaves beside itself (the units whose slot is not
// reserved yet) and which sizes its per-user caps check adds (every unit's).
//
// 1. A unit is a chosen, countable Instance of any deployment on the Provider, the asking deployment's included, in
// whichever state its Allocation is: Running, pending, or none yet (in flight, or cut short), since a deployment counts
// its chosen units whichever Instance a same-second UID tie picks. Each is counted once, by its Instance's UID and role.
// The asking Instance is left out of its deployment's selection by its UID. A chosen primary with a live replacement is
// no unit; its in-flight replacement is one, of role roleOwn, for the size of the Allocation it will make, counted once
// and gone when that Allocation is listed.
//
// 2. A hedge counts by countEachMember: each live member on this Provider of an unresolved hedge, else only the hedge's
// winner when it has one. A primary's REPLACEMENT hedge counts the same way while the primary serves: each live member on
// this Provider of an unresolved replacement hedge of two or more members counts, whatever its Allocation's phase, since it
// holds no reserved slot until it wins; a hedge with a winner counts none here (the winner's slot is a reserved one).
//
// 3. A unit's slot is judged as the controller judges it for the replacement (servesPast): the deployment has no end
// time, or one after the drain start of the Instance, its creation plus its Allocation's time limit (an in-flight
// Instance's, the time limit its recorded launch is sized for) minus the drain window. A Running chosen primary and a hedge
// winner are the reserved slots of kind slotForReplacement, read as they are; a pending or in-flight one holds no slot yet,
// so it is counted beside them.
//
// 4. A unit's size depends on whether its Instance exists. An Instance that exists, an in-flight replacement (roleOwn,
// until its Allocation is listed), is charged at its OWN recorded launch (Launch.GPUs and workers, immutable), the size
// ensureAllocation will request; once its Allocation is listed it is no unit, the listed Allocation counts. ONLY a
// replacement that does not exist yet, a handing-over primary's next one (roleHandover), is charged at the CURRENT spec's
// size on THIS Provider (checkServed's plan through the Provider's partition), never the replaced Allocation's, so a spec
// edited since (cpusPerGpu, workers, leaseLimit) is read as the replacement's launch will read it. Each unit carries the
// GPU count of the same source.
//
// 5. It reads, under the lock, the deployments' Instances and Allocations by label and their ModelDeployments (the views
// of reservedOn, which also feed its selection) and the reserved slots; it lists and gets nothing of its own.
//
// 6. Its callers are the extra's reservation (slotAsk.deployment set, reserveAsk): the slots its alsoLeave adds and the
// sizes its caps check adds, summed by unitTerms, in this file, and nowhere else. A unit carries its GPU count beside its
// size and unitTerms takes a filter over the units, so the same units are counted Provider-wide (no filter) or per QOS
// (a filter by the unit's GPUs). The reserved slots every other ask counts, and the selection, are unchanged.
func extraUnits(r handoverReads, a unitAsk) []extraUnit {
	var out []extraUnit
	seen := map[struct {
		uid  types.UID
		role unitRole
	}]bool{}
	// add counts a unit once by its Instance's UID and role. Both loops below read ONE snapshot (slot_snapshot.go), so a
	// Running Allocation is never a reserved unit in one loop and absent from the other because the world moved between two
	// lists; a duplicate remains possible when an Allocation names the Provider but not its deployment's label: the Provider's
	// own list shows it Running (a reserved unit) while the deployment's view, by label, shows its Instance with no Allocation
	// (a unit in flight). The row pins that case.
	add := func(u extraUnit) {
		k := struct {
			uid  types.UID
			role unitRole
		}{u.uid, u.role}
		if u.uid != "" && seen[k] {
			return
		}
		seen[k] = true
		out = append(out, u)
	}
	// The reserved slots that are units already: a Running chosen primary's or a winner's replacement, and an in-flight
	// replacement's own Allocation.
	for _, s := range r.reserved {
		if (s.kind != slotForReplacement && s.kind != slotForOwnAllocation) || (a.exclude != "" && types.UID(s.uid) == a.exclude) {
			continue
		}
		md := r.mds[s.deployment]
		if s.kind == slotForOwnAllocation {
			// An in-flight replacement exists: its own recorded launch sizes it.
			var sz v1alpha1.BatchParameters
			var gpus int32
			if in := r.instanceOf(types.UID(s.uid)); in != nil {
				sz, gpus = a.recorded(md, in)
			}
			add(extraUnit{uid: types.UID(s.uid), role: roleOwn, deployment: s.deployment, key: s.key, reserved: true, size: sz, gpus: gpus})
			continue
		}
		// A Running primary's or a winner's replacement does not exist yet: the current spec sizes it.
		sz, gpus := a.future(md)
		add(extraUnit{uid: types.UID(s.uid), role: roleHandover, deployment: s.deployment, key: s.key, reserved: true, size: sz, gpus: gpus})
	}
	// addMember counts the member m of deployment key's view v, on this Provider, as a primary that will hand over: a unit
	// when its deployment serves past m's drain start, judged by its Allocation's time limit, or, in flight, by its recorded
	// launch's. A chosen primary skips a Running or ended Allocation (the reserved slots above hold a Running one's); a
	// member of an unresolved replacement hedge counts whatever its Allocation's phase, since it holds no reserved slot
	// until it wins.
	addMember := func(m *v1alpha1.Instance, v deploymentView, md *v1alpha1.ModelDeployment, key string, primary bool) {
		if m.Labels[v1alpha1.LabelProvider] != r.provider || (a.exclude != "" && m.UID == a.exclude) {
			return
		}
		al := v.allocs[m.Spec.AllocationRef.Name]
		if primary && al != nil && !notYetRunning(al) {
			return // Running: a reserved slot above; ended: it holds none
		}
		judged := al
		if judged == nil {
			// In flight: its own Allocation is not made yet, and its drain start is read from the time limit its
			// recorded launch is sized for.
			own, _ := a.recorded(md, m)
			judged = &v1alpha1.Allocation{}
			judged.Spec.ForProvider.Batch = &own
		}
		if !holdsSlotOnceRunning(md, m, judged, *r.profile, time.Time{}) {
			return
		}
		// Its replacement does not exist yet: the current spec sizes it.
		sz, gpus := a.future(md)
		add(extraUnit{uid: m.UID, role: roleHandover, deployment: key, size: sz, gpus: gpus})
	}
	// The chosen primaries that have not started, or have no Allocation yet, and the members of an unresolved replacement
	// hedge.
	for _, v := range r.views {
		if len(v.insts) == 0 {
			continue
		}
		key := deploymentKey(v.insts[0])
		md := r.mds[key]
		if md == nil || !md.DeletionTimestamp.IsZero() || md.Spec.Source.Served == nil || r.profile == nil {
			continue
		}
		sel := selectionOfView(md, v)
		if a.exclude != "" {
			kept := sel.insts[:0:0]
			for _, in := range sel.insts {
				if in.UID != a.exclude {
					kept = append(kept, in)
				}
			}
			sel.insts = kept
		}
		replaced := map[string]bool{}
		for _, in := range v.insts {
			if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && !ended(in) {
				replaced[pred] = true
			}
		}
		chosen, surplus := selectUnits(sel, int(md.Spec.Instances))
		if key != a.deployment {
			chosen = preferAllocated(sel, chosen, surplus)
		}
		for _, u := range chosen {
			held := false
			for _, m := range u.members {
				held = held || replaced[m.Name]
			}
			if held {
				// A live replacement carries the handover, but an UNRESOLVED replacement hedge's members each count.
				for _, m := range replacementHedgeMembers(v, u, countEachMember) {
					addMember(m, v, md, key, false)
				}
				continue
			}
			members := u.members
			if w := hedgeWinner(members); w != nil {
				members = []*v1alpha1.Instance{w}
			} else if !countEachMember {
				members = []*v1alpha1.Instance{u.rep}
			}
			for _, m := range members {
				addMember(m, v, md, key, true)
			}
		}
	}
	return out
}

// replacementHedgeMembers returns the members, by Instance, of the unresolved replacement hedges of the held unit u: those of
// its live replacements that make a hedge of two or more live members with no winner yet (a hedge of one is a plain
// replacement, whose own slot the reserved slots hold; a winner's slot is a reserved slot too). With each false only the
// hedge's representative counts, as for a primary's hedge (countEachMember).
func replacementHedgeMembers(v deploymentView, u selUnit, each bool) []*v1alpha1.Instance {
	names := map[string]bool{}
	for _, m := range u.members {
		names[m.Name] = true
	}
	var repls []*v1alpha1.Instance
	for _, in := range v.insts {
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && names[pred] && countable(in) {
			repls = append(repls, in)
		}
	}
	var out []*v1alpha1.Instance
	for _, g := range groupHedges(repls, func(*v1alpha1.Instance) bool { return true }) {
		if len(g.members) < 2 || hedgeWinner(g.members) != nil {
			continue
		}
		if !each {
			out = append(out, hedgeRepresentative(g.members, func(in *v1alpha1.Instance) *v1alpha1.Allocation { return v.allocs[in.Spec.AllocationRef.Name] }))
			continue
		}
		out = append(out, g.members...)
	}
	return out
}

// preferAllocated is chosen with each unit that has no Allocation swapped for a surplus unit that has one and is its equal by
// the selection's order before the UID, one of the same tier made at the same time with no job started: the selection's UID
// tie would let a lower-UID Instance in flight displace its listed twin, whose Allocation is the one whose time limit, and so
// drain start, is real (the in-flight one is sized at the lease limit now, which an increase has moved). It is applied to the
// OTHER deployments' selections; the asking deployment's own retry is left out of its selection by its UID instead.
func preferAllocated(sel selection, chosen, surplus []selUnit) []selUnit {
	has := func(u selUnit) bool { return sel.allocOf(u.rep) != nil }
	out := append([]selUnit(nil), chosen...)
	used := map[int]bool{}
	for i, c := range out {
		if has(c) {
			continue
		}
		for j, s := range surplus {
			if used[j] || s.marked || s.candidate || !has(s) || s.tier() != c.tier() || !s.first.CreationTimestamp.Equal(&c.first.CreationTimestamp) {
				continue
			}
			out[i], used[j] = s, true
			break
		}
	}
	return out
}

// instanceOf is the Instance of the reads that has the UID, nil when none: the reserved slots name Instances by UID.
func (r handoverReads) instanceOf(uid types.UID) *v1alpha1.Instance {
	for _, v := range r.views {
		for _, in := range v.insts {
			if in.UID == uid {
				return in
			}
		}
	}
	return nil
}

// unitTerms sums an extra's units into its two terms: how many slots it leaves free beside itself (the units whose slot is
// not reserved yet) and the sizes its caps check adds as if already expected (every unit's, but an in-flight replacement's
// own Allocation that an expectation already sizes: sized says so for its key). It is the one place the units are summed,
// and counts only the units that in keeps, so the same set is summed Provider-wide (in nil: every unit) or per QOS (in by
// the unit's GPUs, which each unit carries).
func unitTerms(units []extraUnit, sized func(key string) bool, in func(extraUnit) bool) (alsoLeave int, sizes []v1alpha1.BatchParameters) {
	for _, u := range units {
		if in != nil && !in(u) {
			continue
		}
		if !u.reserved {
			alsoLeave++
		}
		if u.role == roleOwn && sized(u.key) {
			continue
		}
		sizes = append(sizes, u.size)
	}
	return alsoLeave, sizes
}

// holdsSlotOnceRunning reports whether the primary in, whose Allocation a has not started, will hold a reserved slot once
// it runs: the deployment md serves past its expected drain start, in's creation plus its Allocation's time limit minus the
// drain window (servesPast; the test replacements() and reservedOn share). An Allocation without a readable time limit is
// counted: a slot left over is a refused extra, and a slot taken is a refused handover. now stands for an Instance's creation
// not stamped yet (the zero time for stored Instances, which all are).
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

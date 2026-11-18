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
	// size is what a new Allocation of that deployment on this Provider requests now, for the per-user caps; zero when the
	// spec cannot be sized.
	size v1alpha1.BatchParameters
}

// unitAsk is what the unit set needs to know of the extra that asks.
type unitAsk struct {
	// deployment is the asking deployment (namespace/name): it counts like any other.
	deployment string
	// exclude is the asking Instance, the cut-short extra that retries (zero for a new one): it is left out of its
	// deployment's selection, so a retry never counts itself.
	exclude types.UID
	// sizeOf is the size a new Allocation of a deployment requests on this Provider now: its served plan and lease limit
	// through the Provider's partition (partitionSizingFor). It reads nothing but its argument and the Provider's view.
	sizeOf func(*v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error)
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
// winner when it has one.
//
// 3. A unit's slot is judged as the controller judges it for the replacement (servesPast): the deployment has no end
// time, or one after the drain start of the Instance, its creation plus its Allocation's time limit (an in-flight
// Instance's, the time limit of the size a new Allocation requests now) minus the drain window. A Running chosen primary
// and a hedge winner are the reserved slots of kind slotForReplacement, read as they are; a pending or in-flight one holds
// no slot yet, so it is counted beside them.
//
// 4. A unit's size is what a NEW Allocation of ITS deployment on THIS Provider requests now (sizeOf), never the size of the
// Allocation it replaces, so a spec edited since (cpusPerGpu, workers, leaseLimit) is read as the replacement's launch
// will read it.
//
// 5. It reads, under the lock, the deployments' Instances and Allocations by label and their ModelDeployments (the views
// of reservedOn, which also feed its selection) and the reserved slots; it lists and gets nothing of its own.
//
// 6. Its callers are the extra's reservation (slotAsk.deployment set, reserveAsk): the slots its alsoLeave adds and the
// sizes its caps check adds. The reserved slots every other ask counts, and the selection, are unchanged.
func extraUnits(r handoverReads, a unitAsk) []extraUnit {
	var out []extraUnit
	seen := map[struct {
		uid  types.UID
		role unitRole
	}]bool{}
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
	size := func(md *v1alpha1.ModelDeployment) v1alpha1.BatchParameters {
		if md == nil || a.sizeOf == nil {
			return v1alpha1.BatchParameters{}
		}
		b, err := a.sizeOf(md)
		if err != nil {
			return v1alpha1.BatchParameters{}
		}
		return b
	}
	// The reserved slots that are units already: a Running chosen primary's or a winner's replacement, and an in-flight
	// replacement's own Allocation.
	for _, s := range r.reserved {
		if (s.kind != slotForReplacement && s.kind != slotForOwnAllocation) || (a.exclude != "" && types.UID(s.uid) == a.exclude) {
			continue
		}
		role := roleHandover
		if s.kind == slotForOwnAllocation {
			role = roleOwn
		}
		add(extraUnit{uid: types.UID(s.uid), role: role, deployment: s.deployment, key: s.key, reserved: true, size: size(r.mds[s.deployment])})
	}
	// The chosen primaries that have not started, or have no Allocation yet.
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
		chosen, _ := selectUnits(sel, int(md.Spec.Instances))
		for _, u := range chosen {
			held := false
			for _, m := range u.members {
				held = held || replaced[m.Name]
			}
			if held {
				continue // a live replacement carries the handover
			}
			members := u.members
			if w := hedgeWinner(members); w != nil {
				members = []*v1alpha1.Instance{w}
			} else if !countEachMember {
				members = []*v1alpha1.Instance{u.rep}
			}
			for _, m := range members {
				if m.Labels[v1alpha1.LabelProvider] != r.provider {
					continue
				}
				al := v.allocs[m.Spec.AllocationRef.Name]
				if al != nil && !notYetRunning(al) {
					continue // Running: a reserved slot above; ended: it holds none
				}
				sz := size(md)
				judged := al
				if judged == nil {
					judged = &v1alpha1.Allocation{}
					judged.Spec.ForProvider.Batch = &sz
				}
				if !holdsSlotOnceRunning(md, m, judged, *r.profile, time.Time{}) {
					continue
				}
				add(extraUnit{uid: m.UID, role: roleHandover, deployment: key, size: sz})
			}
		}
	}
	return out
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

package lifecycle

// The placement order (HBEP-7, Provisioning, Where; #604 PR 3, the plan's sections 1.2 and 1.3): a missing primary goes
// to a warm Allocation of its deployment, of any class, on the allowed Providers in order, then to a new submission; a
// replacement goes to its predecessor's Provider first, then to a warm Allocation of the predecessor's class, then of
// another class, then to a new submission. Fixtures are placement_order_rig_test.go's. Every row names the Provider and
// the Allocation it expects, and a row with a Provider list lists C before B, so a single sweep in the allowed order
// is not the answer it expects.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// allocsOn returns the names of the deployment's Allocations on the Provider that are not being deleted, so a new
// submission is a name that was not there before.
func (o *ordRig) allocsOn(md, prov string) []string {
	o.t.Helper()
	var out []string
	for _, a := range o.allocations(md) {
		if a.Labels[v1alpha1.LabelProvider] == prov && a.DeletionTimestamp.IsZero() {
			out = append(out, a.Name)
		}
	}
	return out
}

// hostsLive reports whether a live Instance of the deployment runs in the Allocation.
func (o *ordRig) hostsLive(md, alloc string) bool {
	for _, in := range o.liveInstances(md) {
		if in.Spec.AllocationRef.Name == alloc {
			return true
		}
	}
	return false
}

func sameAllocs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAMissingPrimaryGoesToAWarmAllocationBeforeANewSubmission: B holds the deployment's warm Allocation, kept after an
// earlier run, and A has room. The deployment now wants one Instance and allows A, then B. Its missing primary is
// placed on B's warm Allocation, and A gets no Allocation. B's submit limit is 1 and its warm Allocation holds that one
// slot, so a placement that reserved a slot would be refused there: the warm placement takes none. The control has no
// warm Allocation, and the missing primary is submitted on A, the first allowed Provider with room.
func TestAMissingPrimaryGoesToAWarmAllocationBeforeANewSubmission(t *testing.T) {
	for _, warm := range []bool{true, false} {
		name := "a warm Allocation on B and room on A"
		if !warm {
			name = "no warm Allocation (the control)"
		}
		t.Run(name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{bLimit: 1})
			o.create(ordMD(hoMD))
			wb := ""
			if warm {
				wb = o.warmUp(hoMD, ordB)
			}
			hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
			o.allowOn(hoMD, 1, rigProvider, ordB)
			o.clock.Advance(10 * time.Second)
			o.Step()
			live := o.liveInstances(hoMD)
			if len(live) != 1 {
				t.Fatalf("%d live Instances after the pass, want the one missing primary", len(live))
			}
			prov, alloc := where(live[0])
			onA, onB := o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB)
			if warm {
				if prov != ordB || alloc != wb || len(onA) != 0 || !sameAllocs(onB, []string{wb}) {
					t.Fatalf("the missing primary is on %s, Allocation %s; A has %v, B has %v; want it on B's warm Allocation %s, with no Allocation on A", prov, alloc, onA, onB, wb)
				}
				return
			}
			if prov != rigProvider || len(onA) != 1 || onA[0] != alloc || len(onB) != 0 {
				t.Fatalf("the missing primary is on %s, Allocation %s; A has %v, B has %v; want a new Allocation on A and none on B", prov, alloc, onA, onB)
			}
		})
	}
}

// TestAReplacementGoesToItsOwnProviderBeforeWarmCapacityElsewhere: the primary P is on A, which has no submit limit, and
// B, of A's class, holds a warm Allocation. At P's replacement time, 10:19:30, the replacement is a new submission on A
// (HBEP-7: "goes first to its predecessor's Provider when it has a slot"), though B's warm Allocation is of A's class
// and would take it: A gets a second Allocation, B keeps its one, and no Instance runs in B's.
func TestAReplacementGoesToItsOwnProviderBeforeWarmCapacityElsewhere(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.create(ordMD(hoMD))
	wb := o.warmUp(hoMD, ordB)
	p := o.primaryOnA(hoMD, rigProvider, ordB)
	r := o.replacementAt(p)
	prov, alloc := where(r)
	onA, onB := o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB)
	_, pAlloc := where(*p)
	if prov != rigProvider || alloc == pAlloc || alloc == wb || len(onA) != 2 || !sameAllocs(onB, []string{wb}) || o.hostsLive(hoMD, wb) {
		t.Fatalf("the replacement is on %s, Allocation %s; A has %v, B has %v; want a new Allocation on A (beside P's %s) and B's warm %s untouched", prov, alloc, onA, onB, pAlloc, wb)
	}
}

// TestAReplacementWithoutASlotOnItsProviderTakesWarmCapacityByClass: P is on A, whose submit limit is 1 and which P's
// Allocation fills, so the replacement cannot be placed beside P on A. The deployment allows A, then C, then B: C of
// another class, B of A's class. With a warm Allocation on each, the replacement takes B's, of P's class, before C's,
// though C comes first in the list; with only C's, it takes C's before any new submission, and carries C's class's
// current ID, compared with a control deployment's Instance on C; with neither, it is submitted on C, the first allowed
// Provider with room after A, and not on B.
func TestAReplacementWithoutASlotOnItsProviderTakesWarmCapacityByClass(t *testing.T) {
	for _, c := range []struct {
		name         string
		warmB, warmC bool
		wantProv     string
		wantNew      bool // a new Allocation on wantProv
	}{
		{"warm on B and C: B's, the same class, first", true, true, ordB, false},
		{"warm on C only: C's, before any submission", false, true, ordC, false},
		{"no warm Allocation: a submission on C, the first with room", false, false, ordC, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{aLimit: 1, withC: true})
			o.create(ordMD(hoMD))
			var wb, wc string
			if c.warmB {
				wb = o.warmUp(hoMD, ordB)
			}
			if c.warmC {
				wc = o.warmUp(hoMD, ordC)
			}
			p := o.primaryOnA(hoMD, rigProvider, ordC, ordB)
			r := o.replacementAt(p)
			prov, alloc := where(r)
			if prov != c.wantProv {
				t.Fatalf("the replacement is on %s, Allocation %s; want a placement on %s", prov, alloc, c.wantProv)
			}
			onA, onB, onC := o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB), o.allocsOn(hoMD, ordC)
			if len(onA) != 1 {
				t.Fatalf("A has %v, want only P's Allocation: the replacement has no slot there", onA)
			}
			var wantB, wantC []string
			if c.warmB {
				wantB = []string{wb}
			}
			if c.warmC {
				wantC = []string{wc}
			}
			switch {
			case c.wantNew:
				wantC = append(wantC, alloc)
				if alloc == wb || alloc == wc {
					t.Fatalf("the replacement is on %s, an existing Allocation, want a new one", alloc)
				}
			case c.warmB:
				if alloc != wb {
					t.Fatalf("the replacement is on Allocation %s, want B's warm %s", alloc, wb)
				}
			default:
				if alloc != wc {
					t.Fatalf("the replacement is on Allocation %s, want C's warm %s", alloc, wc)
				}
			}
			if !sameAllocs(onB, wantB) || !sameAllocs(onC, wantC) {
				t.Fatalf("B has %v and C has %v, want %v and %v: no submission where a warm Allocation took it", onB, onC, wantB, wantC)
			}
			if !c.warmC || c.warmB {
				return
			}
			// Another class's ID: the replacement on C's warm Allocation is an Instance of C's class and carries its
			// current ID, the one a control deployment's Instance made on C carries; A's class has another.
			ctl := ordMD("ctl")
			ctl.Spec.Providers.Names = []string{ordC}
			ctl.Spec.Instances = 1
			o.create(ctl)
			o.until(10*time.Minute, 10*time.Second, "the control's Instance on C", func() bool { return len(o.instances("ctl")) == 1 })
			want := o.instances("ctl")[0].Spec.DeploymentID.Display()
			if got := r.Spec.DeploymentID.Display(); got != want {
				t.Fatalf("the replacement on C's warm Allocation carries ID %s, want C's class's current ID %s (the control's)", got, want)
			}
			if got := p.Spec.DeploymentID.Display(); got == want {
				t.Fatalf("P (class of A) carries the same ID %s as the control on C: the fixture's classes do not differ, so the row proves nothing", got)
			}
			if got := r.Labels[v1alpha1.LabelDeploymentID]; got != want {
				t.Fatalf("the replacement's ID label is %s, want %s", got, want)
			}
		})
	}
}

// TestALosersEndIsNoFailureAndItsAllocationTakesTheNextPrimary is the second half of HBEP-7's "A loser's end is no
// failure" (the first half is TestALosersEndIsNoFailure): three hedges, each with a member on A and one on B. A's jobs
// start at 08:00:20, 08:10:20 and 08:20:20 (queue waits of 0, 10 and 20 min) with engines that never get ready, and
// B's start at about 08:30 and win, so A's three started members end with HedgeLost and A's three Allocations are
// kept, with three different hard ends. One of B's winners then fails (its job is cancelled outside the controller),
// and the missing primary is placed on one of A's kept Allocations, the one with the latest hard end, before any new
// submission: no Allocation is made, and the Instance carries A's class's ID.
func TestALosersEndIsNoFailureAndItsAllocationTakesTheNextPrimary(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: 20 * time.Minute})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 30 * time.Minute}, pfake.Script{QueueWait: 30 * time.Minute}, pfake.Script{QueueWait: 30 * time.Minute})
	h.Step()
	md := mdSpec(hoMD)
	seededOnly(md)
	md.Spec.Instances = 3
	h.create(md)
	h.rig.engine = func(alloc string) afake.Engine {
		if a, ok := h.allocation(alloc); ok && a.Labels[v1alpha1.LabelProvider] == rigProvider {
			return afake.Engine{StartDelay: 6 * time.Hour}
		}
		return afake.Engine{}
	}
	h.Step()
	firsts := h.onProvider(hoMD, rigProvider)
	if len(firsts) != 3 {
		t.Fatalf("%d Instances on A, want the three missing primaries", len(firsts))
	}
	for _, f := range firsts {
		h.seedHedgeMember(hoMD, f.Name, "", hedgeB)
	}
	h.until(time.Hour, 10*time.Second, "three ready on B", func() bool { return h.readyCount(hoMD) == 3 })
	h.clock.Advance(10 * time.Second)
	h.Step()
	var kept []v1alpha1.Allocation
	for _, a := range h.allocations(hoMD) {
		if a.Labels[v1alpha1.LabelProvider] == rigProvider && a.DeletionTimestamp.IsZero() {
			kept = append(kept, a)
		}
	}
	if len(kept) != 3 {
		t.Fatalf("%d of A's Allocations are kept, want 3: the row proves nothing with fewer", len(kept))
	}
	hardEnds := map[time.Time]bool{}
	latest := kept[0]
	for _, a := range kept {
		he := a.Status.AtProvider.HardEnd
		if he == nil {
			t.Fatalf("A's Allocation %s has no hard end", a.Name)
		}
		hardEnds[he.Time] = true
		if he.After(latest.Status.AtProvider.HardEnd.Time) {
			latest = a
		}
	}
	if len(hardEnds) != 3 {
		t.Fatalf("A's three kept Allocations have %d different hard ends, want 3: the latest-hard-end pick shows nothing otherwise", len(hardEnds))
	}
	subsA, subsB := h.batch.Count("sbatch"), h.b.batch.Count("sbatch")
	var failed v1alpha1.Instance
	for _, in := range h.liveInstances(hoMD) {
		if in.Labels[v1alpha1.LabelProvider] == hedgeB {
			failed = in
			break
		}
	}
	bAlloc, _ := h.allocation(failed.Spec.AllocationRef.Name)
	h.b.batch.CancelExternally(bAlloc.Annotations[v1alpha1.AnnotationJobID])
	h.until(10*time.Minute, 10*time.Second, "the failed winner replaced", func() bool {
		live := h.liveInstances(hoMD)
		if len(live) != 3 {
			return false
		}
		for _, in := range live {
			if in.Name == failed.Name {
				return false
			}
		}
		return true
	})
	var placed *v1alpha1.Instance
	for _, in := range h.liveInstances(hoMD) {
		if in.Spec.AllocationRef.Name == latest.Name {
			placed = &in
		}
	}
	if placed == nil {
		t.Fatalf("no live Instance runs in A's kept Allocation %s, the one with the latest hard end %s; live: %v", latest.Name, latest.Status.AtProvider.HardEnd.Time.Format(time.TimeOnly), h.liveInstances(hoMD))
	}
	if a, b := h.batch.Count("sbatch"), h.b.batch.Count("sbatch"); a != subsA || b != subsB {
		t.Fatalf("A received %d submissions and B %d after the failure, want %d and %d: the missing primary takes a kept Allocation before any submission", a, b, subsA, subsB)
	}
}

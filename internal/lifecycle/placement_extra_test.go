package lifecycle

// The placement order beside #616 PR 4's extra and its admission headroom (HBEP-7, Over-allocation and qualification
// (beta), When the extra is submitted, Its checks; amendment #711): a missing primary placed on a warm Allocation is no
// new Allocation, so it brings no extra; a missing primary takes the slot headroom leaves it on the first Provider with
// room, and a missing primary refused on a full Provider goes on to the next, and in neither is anything cancelled. An
// extra never costs a primary its slot, and no pending extra yields: there is no yield (#708).

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestAMissingPrimaryOnAWarmAllocationGetsNoExtra: k = 1, B's warm Allocation, room on A and on B. The missing primary
// is placed on B's warm Allocation and makes no Allocation, so the deployment gets no extra (the extra goes only to a
// Provider where the pass made a new Allocation): one live Instance and one Allocation five minutes of passes later,
// and nothing on A. The control has no warm Allocation: the primary is submitted on A and its extra beside it in the
// same pass, two live Instances on two Allocations on A, so the row can see an extra.
func TestAMissingPrimaryOnAWarmAllocationGetsNoExtra(t *testing.T) {
	for _, warm := range []bool{true, false} {
		name := "a warm Allocation on B"
		if !warm {
			name = "no warm Allocation (the control)"
		}
		t.Run(name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{})
			o.create(ordMD(hoMD))
			wb := ""
			if warm {
				wb = o.warmUp(hoMD, ordB)
			}
			hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
			o.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
				m.Spec.Providers.Names = []string{rigProvider, ordB}
				m.Spec.Instances = 1
				m.Spec.OverAllocation = ptrTo(int32(1))
			})
			o.clock.Advance(10 * time.Second)
			o.Step()
			live := o.liveInstances(hoMD)
			if !warm {
				// At the pass that makes them: both are pending, so neither is ranked out yet.
				if onA := o.allocsOn(hoMD, rigProvider); len(live) != 2 || len(onA) != 2 || len(o.allocsOn(hoMD, ordB)) != 0 {
					t.Fatalf("%d live Instances, A has %v, B has %v; want the primary and its extra on two Allocations on A", len(live), onA, o.allocsOn(hoMD, ordB))
				}
				return
			}
			o.run(5*time.Minute, 10*time.Second)
			live = o.liveInstances(hoMD)
			prov, alloc := "", ""
			if len(live) == 1 {
				prov, alloc = where(live[0])
			}
			if len(live) != 1 || prov != ordB || alloc != wb || len(o.allocsOn(hoMD, rigProvider)) != 0 || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
				t.Fatalf("%d live Instances (on %s, Allocation %s); A has %v, B has %v; want one, on B's warm Allocation %s, and no extra", len(live), prov, alloc, o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB), wb)
			}
		})
	}
}

// limitOnA is the rig with A's submit limit set, the Provider B beside it with none, and a log of the Allocations the
// controller deletes (cancelsOf): nothing in these rows may delete one.
func limitOnA(t *testing.T, limit int32) (*rig, *causeLog) {
	t.Helper()
	log := newCauseLog()
	rg := newRig(t, append(limitOpts(limit), withInterceptors(log.funcs()))...)
	rg.addProvider(ordB, "gpu-b", 12*time.Hour, nil, nil)
	return rg, log
}

// md-c's deployment: one Instance, allowed on A and then B, one member per hedge (the rows are about placement).
func missingOnAThenB(name string) *v1alpha1.ModelDeployment {
	md := mdSpec(name)
	md.Spec.HedgeWidth = ptrTo(int32(1))
	md.Spec.Providers.Names = []string{rigProvider, ordB}
	return md
}

// TestAMissingPrimaryTakesTheSlotHeadroomLeavesItOnAAndNothingIsCancelled: A's submit limit is 4, B has none. md-a (k = 1,
// an end time before its drain start, so its handover reserves no slot) makes its primary and its extra at 08:00:10: 1 listed
// + 0 + 1 + H is 2 <= 4, H being 0 (#711's rule: the other deployments with a missing primary, with no floor; md-b and md-c
// arrive later). md-b (k = 1) and md-c arrive at 08:00:20, and md-b sorts before md-c, so at
// md-b's pass md-c is a missing primary that A allows (H = 1): md-b's primary makes 3 listed, and its extra, 3 + 0 + 1 + 1 =
// 5 > 4, is refused. md-c's primary then takes the fourth slot, on A, the first allowed Provider with room, and not on B: no
// pending extra is cancelled for it, and none could be, since headroom kept its slot (HBEP-7, Its checks). md-a has 2
// Allocations, md-b 1 and md-c 1, on A; B has none; no Allocation is deleted in ten minutes.
func TestAMissingPrimaryTakesTheSlotHeadroomLeavesItOnAAndNothingIsCancelled(t *testing.T) {
	rg, log := limitOnA(t, 4)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(extraMD("md-a"))
	tick10(rg) // 08:00:10: md-a's primary and its extra
	rg.create(extraMD("md-b"))
	rg.create(missingOnAThenB("md-c"))
	tick10(rg) // 08:00:20: md-b's primary, its extra refused, then md-c's primary
	for range 60 {
		tick10(rg) // to 08:10:20
	}
	if a, b, c := len(rg.allocations("md-a")), len(rg.allocations("md-b")), len(rg.allocations("md-c")); a != 2 || b != 1 || c != 1 {
		t.Fatalf("md-a has %d Allocations, md-b %d and md-c %d, want 2, 1 and 1", a, b, c)
	}
	live := rg.liveInstances("md-c")
	if len(live) != 1 || live[0].Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("md-c's live Instances are %v, want its primary on A: headroom left it the slot there", live)
	}
	for _, a := range rg.allAllocations() {
		if a.Labels[v1alpha1.LabelProvider] == ordB {
			t.Fatalf("Allocation %s was made on B: md-c's primary went past A's free slot", a.Name)
		}
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none", n)
	}
}

// TestAMissingPrimaryRefusedOnAForTheLimitGoesToBAndNothingIsCancelled: A's submit limit is 3, B has none. md-a (k = 1,
// an end time) makes its primary and its extra at 08:00:10 (1 + 0 + 1 + H = 2 <= 3, H = 0: no other deployment is missing a primary, #711's rule), and md-b (k = 0) its
// primary at 08:00:30: 3 listed, A is full. md-c, allowed on A and then B, arrives at 08:00:40 and is refused on A for the
// limit alone at 08:00:50, and its primary goes to B, the next allowed Provider, in the same pass. Nothing is cancelled for
// it: md-a keeps its primary and its extra, md-b its primary, in ten minutes, and md-c has its one Instance on B.
func TestAMissingPrimaryRefusedOnAForTheLimitGoesToBAndNothingIsCancelled(t *testing.T) {
	rg, log := limitOnA(t, 3)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(extraMD("md-a"))
	tick10(rg) // 08:00:10: md-a's primary and its extra
	tick10(rg) // 08:00:20: their jobs are submitted
	rg.create(mdSpec("md-b"))
	tick10(rg) // 08:00:30: md-b's primary: A has 3 listed
	tick10(rg) // 08:00:40
	rg.create(missingOnAThenB("md-c"))
	tick10(rg) // 08:00:50: md-c is refused on A, and placed on B
	if !rg.clock.Now().Equal(hoT(8, 0, 50)) {
		t.Fatalf("the rig is at %s, want 08:00:50", rg.clock.Now().Format(time.TimeOnly))
	}
	if a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b")); a != 2 || b != 1 {
		t.Fatalf("md-a has %d Allocations and md-b %d at 08:00:50, want 2 and 1: A's limit of 3 is full", a, b)
	}
	for range 60 {
		tick10(rg) // to 08:10:50
	}
	if a, b := len(rg.liveInstances("md-a")), len(rg.liveInstances("md-b")); a != 2 || b != 1 {
		t.Fatalf("md-a has %d live Instances and md-b %d, want 2 and 1: nothing is cancelled for md-c", a, b)
	}
	live := rg.liveInstances("md-c")
	if len(live) != 1 || live[0].Labels[v1alpha1.LabelProvider] != ordB {
		t.Fatalf("md-c's live Instances are %v, want its primary on B, refused on A for the limit", live)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none", n)
	}
}

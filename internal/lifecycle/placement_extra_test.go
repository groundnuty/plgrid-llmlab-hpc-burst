package lifecycle

// The placement order beside #616 PR 4's extra and yield (HBEP-7, Over-allocation and qualification (beta), When the
// extra is submitted, Its checks; amendment #674): a missing primary placed on a warm Allocation is no new Allocation,
// so it brings no extra; a missing primary refused on one Provider for the submit limit alone and placed on the next
// leaves no extra cancelled for it; the yield still fires when every allowed Provider refuses. The rows assert
// outcomes, never the yield's mechanism, which #708's fix round reworks.

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

// yieldOnTwo is yield_rig_test.go's fixture with Provider B beside A: A's submit limit is 4 and four of md-a's and
// md-b's Allocations (a primary and an extra each, k = 1, pending 10 h) fill it. md-c wants one Instance, hedgeWidth 1,
// and allows A, then B, which has no limit.
func yieldOnTwo(t *testing.T) (*rig, *rigExtra) {
	t.Helper()
	rg := yieldRig(t, newCauseLog())
	b := rg.addProvider(ordB, "gpu-b", 12*time.Hour, nil, nil)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10: md-a makes P1 and X1
	tick10(rg) // 08:00:20: their jobs are submitted
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30: md-b makes P2 and X2
	tick10(rg) // 08:00:40: their jobs are submitted
	c := mdSpec("md-c")
	c.Spec.HedgeWidth = ptrTo(int32(1))
	c.Spec.Providers.Names = []string{rigProvider, ordB}
	rg.create(c)
	if a, bb := len(rg.allocations("md-a")), len(rg.allocations("md-b")); a != 2 || bb != 2 {
		t.Fatalf("md-a has %d Allocations and md-b %d before md-c, want 2 each: A's limit of 4 is full", a, bb)
	}
	return rg, b
}

// TestAMissingPrimaryRefusedOnAForTheLimitIsSubmittedOnBAndNoExtraIsCancelled: md-c's primary is refused on A for the
// submit limit alone at 08:00:50 (4 listed Allocations at the limit of 4), so the yield marks md-b's newest pending
// extra, the Instance made at 08:00:30; and it is submitted on B, which has room, in the same pass. The mark is
// cleared by md-c's next pass, since no missing primary of md-c is refused on A any more, before md-b's pass cancels
// the extra: md-b's owner waits until md-c's status.lastReconcileTime is later than the mark's time, and md-c's pass at
// 08:01:00 comes after md-b's in the same step. Through 08:11:00 no extra is cancelled: md-a and md-b keep two live
// Instances and two Allocations each, no mark is left, and md-c has its one Instance, on B.
func TestAMissingPrimaryRefusedOnAForTheLimitIsSubmittedOnBAndNoExtraIsCancelled(t *testing.T) {
	rg, _ := yieldOnTwo(t)
	tick10(rg) // 08:00:50: md-c's primary is refused on A and placed on B
	if !rg.clock.Now().Equal(hoT(8, 0, 50)) {
		t.Fatalf("the rig is at %s, want 08:00:50", rg.clock.Now().Format(time.TimeOnly))
	}
	live := rg.liveInstances("md-c")
	if len(live) != 1 || live[0].Labels[v1alpha1.LabelProvider] != ordB {
		t.Fatalf("md-c has %d live Instances at 08:00:50 (%v), want its primary, on B", len(live), live)
	}
	if ms := marks(rg); len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, on md-b's newest pending extra: A refused md-c's primary for the limit, and the yield fires at that refusal", markNames(ms))
	}
	for range 66 { // to 08:11:50
		tick10(rg)
		for _, name := range []string{"md-a", "md-b"} {
			if n := len(rg.liveInstances(name)); n != 2 {
				t.Fatalf("at %s %s has %d live Instances, want its primary and extra: the extra was cancelled for a primary that was placed on B", rg.clock.Now().Format(time.TimeOnly), name, n)
			}
		}
	}
	if ms := marks(rg); len(ms) != 0 {
		t.Fatalf("marks %v are left at 08:11:50, want none: md-c's later passes clear it", markNames(ms))
	}
	for _, name := range []string{"md-a", "md-b"} {
		for _, a := range rg.allocations(name) {
			if !a.DeletionTimestamp.IsZero() {
				t.Fatalf("%s's Allocation %s is being deleted", name, a.Name)
			}
		}
	}
	if live := rg.liveInstances("md-c"); len(live) != 1 || live[0].Labels[v1alpha1.LabelProvider] != ordB {
		t.Fatalf("md-c has %d live Instances at 08:11:50 (%v), want its one, on B", len(live), live)
	}
}

// TestTheYieldFiresWhenEveryAllowedProviderRefusesForTheLimit: A and B each have a submit limit of 2, and each is filled
// by one deployment's primary and extra (md-a on A, md-d on B; k = 1, all four jobs pending 10 h). md-c allows A, then
// B, and wants one Instance: both refuse it for the limit alone at 08:00:50, so the yield fires and an extra is
// cancelled, and md-c's primary is placed, on a Provider it had been refused on, within 10 min. Nothing else could free a
// slot, since every job pends: a yield that fired only at a Provider's refusal that another Provider then answered, or
// not at all, leaves md-c without an Instance.
func TestTheYieldFiresWhenEveryAllowedProviderRefusesForTheLimit(t *testing.T) {
	o := newOrdRig(t, ordOpts{aLimit: 2, bLimit: 2}, withMaxTime(12*time.Hour))
	o.batch.Enqueue(pending10h(2)...)
	o.b.batch.Enqueue(pending10h(2)...)
	a := yieldMD("md-a")
	o.create(a)
	tick10(o.rig) // 08:00:10: md-a makes P1 and X1
	tick10(o.rig) // 08:00:20: their jobs are submitted
	d := yieldMD("md-d")
	d.Spec.Providers.Names = []string{ordB}
	o.create(d)
	tick10(o.rig) // 08:00:30: md-d makes P2 and X2
	tick10(o.rig) // 08:00:40: their jobs are submitted
	if x, y := len(o.allocations("md-a")), len(o.allocations("md-d")); x != 2 || y != 2 {
		t.Fatalf("md-a has %d Allocations and md-d %d, want 2 each: both limits of 2 are full", x, y)
	}
	c := mdSpec("md-c")
	c.Spec.HedgeWidth = ptrTo(int32(1))
	c.Spec.Providers.Names = []string{rigProvider, ordB}
	o.create(c)
	tick10(o.rig) // 08:00:50: md-c's primary is refused on A and on B
	if n := len(o.allocations("md-c")); n != 0 {
		t.Fatalf("md-c has %d Allocations at 08:00:50, want none: both Providers are at their limit", n)
	}
	if len(marks(o.rig)) == 0 {
		t.Fatal("no yield mark at 08:00:50: md-c's primary was refused on both Providers for the limit, and no extra yielded")
	}
	until := o.clock.Now().Add(10 * time.Minute)
	for len(o.liveInstances("md-c")) == 0 {
		if !o.clock.Now().Before(until) {
			t.Fatalf("md-c has no Instance at %s: the yield did not free a slot", o.clock.Now().Format(time.TimeOnly))
		}
		tick10(o.rig)
	}
	cancelled := 0
	for _, name := range []string{"md-a", "md-d"} {
		if len(o.liveInstances(name)) < 2 {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Fatal("md-c is placed but no extra was cancelled: it found its slot some other way")
	}
}

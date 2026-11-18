package lifecycle

// What becomes of a hedge member whose Allocation's create was cut short and whose retry's reservation is refused
// (ensureAllocation; endsOnRefusal): #756 ended it whatever refused it; HBEP-7 (Hedging, Slots reserved atomically,
// Interactions) says the newer of two racing members on one Provider waits for the slot. It ends only when the per-user CAPS
// refuse it, and waits when the SLOT LIMIT does.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAHedgeMembersRetryEndsWhenTheCapsRefuseItAndWaitsWhenTheSlotLimitDoes (rows i and ii of the science agent's ruling on #725,
// 18 Nov 2026): the deployment hedges onto B, whose QOS (alpha-shaped, one QOS) is set per row. The member's first creation passes
// (B is empty) and its Allocation's create is cut short; then another deployment's primary takes B. The member's retry asks again:
//   - B caps a user at 1 GPU and allows 4 submissions: the primary's 1 GPU + the member's 1 = 2 above 1, refused for the CAPS, so
//     the member ends: it holds no job and no queue age, so nothing is released (no Allocation of ours is cancelled), it is no
//     failed end and adds no start outcome;
//   - B allows 4 GPUs and 1 submission: the primary holds B's one slot, 1 + 0 >= 1, refused for the SLOT LIMIT, so the member
//     waits: it is kept, pending, with no Allocation, and the deployment shows SubmitLimitReached, retried on the next pass.
func TestAHedgeMembersRetryEndsWhenTheCapsRefuseItAndWaitsWhenTheSlotLimitDoes(t *testing.T) {
	for _, c := range []struct {
		name         string
		submit, gpus int32
		alive        bool
	}{{"the caps refuse it: it ends", 4, 1, false}, {"the slot limit refuses it: it waits", 1, 4, true}} {
		t.Run(c.name, func(t *testing.T) {
			cut := &allocCutter{}
			opts := []rigOption{withMaxTime(4 * time.Hour), withInterceptors(cut.funcs(interceptor.Funcs{})),
				withProfile(func(s *v1alpha1.PlatformProfileSpec) {
					s.QOS = append(s.QOS, v1alpha1.QOSProfile{Name: "example-b1", MaxSubmitPerUser: ptrTo(c.submit), MaxGPUsPerUser: ptrTo(c.gpus)})
				})}
			rg := newRig(t, opts...)
			b := rg.addProvider(hedgeB, hedgeBSlurm, 4*time.Hour, func(pp *v1alpha1.PartitionProfile) { pp.AcceleratorClass.GPUModel = hedgeBGPU; pp.QOS = "example-b1" },
				func(cfg *pfake.Config) { cfg.Partitions[0].GPUModel = hedgeBGPU })
			h := &hedgeRig{rig: rg, b: b, slow: map[string]bool{}}
			md := mdSpec(hoMD)
			seededOnly(md)
			h.create(md)
			h.clock.Advance(10 * time.Second)
			h.Step()
			primary := h.onProvider(hoMD, rigProvider)
			if len(primary) != 1 {
				t.Fatalf("%d Instances of %s on A, want the primary", len(primary), hoMD)
			}
			cut.arm(hoMD, 0)
			p := h.passOf(hoMD)
			if _, err := p.provisionWith(nil, placeOpts{only: hedgeB, hedgeGroup: primary[0].Name, caps: true, quiet: true}); err == nil {
				t.Fatal("the member's Allocation create was not cut short")
			}
			cut.disarm()
			members := h.onProvider(hoMD, hedgeB)
			if len(members) != 1 || len(h.allocationsOnB(hoMD)) != 0 {
				t.Fatalf("%d members on B and %d Allocations there, want one member and none", len(members), len(h.allocationsOnB(hoMD)))
			}
			other := mdSpec("md-x")
			other.Spec.Providers.Names = []string{hedgeB}
			h.create(other)
			h.clock.Advance(10 * time.Second)
			stepTolerantAll(h.rig, hoMD)
			if n := len(h.allocations("md-x")); n != 1 {
				t.Fatalf("md-x has %d Allocations, want its primary on B", n)
			}
			outcomes := startOutcomeCount(h, hoMD)
			if err := h.passOf(hoMD).ensureAllocation(&members[0]); err != nil {
				t.Fatal(err)
			}
			if got := h.allocationsOnB(hoMD); len(got) != 0 {
				t.Fatalf("the member's retry made %d Allocation(s) on B, want none", len(got))
			}
			m := instOrNil(h.rig, members[0].Name)
			alive := m != nil && !ended(m) && m.DeletionTimestamp.IsZero()
			if alive != c.alive {
				t.Fatalf("the member is %+v, live %v, want live %v", m, alive, c.alive)
			}
			if c.alive {
				if m.Status.Phase != v1alpha1.InstancePending {
					t.Fatalf("the waiting member is %s, want pending: it keeps waiting for its Allocation", m.Status.Phase)
				}
				return
			}
			assertMemberEndedForTheCaps(t, h, hoMD, m, outcomes)
		})
	}
}

// startOutcomeCount is the deployment's recorded start outcomes, over every class.
func startOutcomeCount(h *hedgeRig, md string) int {
	n := 0
	for _, c := range h.md(md).Status.StartOutcomes {
		n += len(c.Outcomes)
	}
	return n
}

// assertMemberEndedForTheCaps checks that the never-made member ended for the caps and released nothing: no failed end, no
// failure streak, no start outcome, no Allocation of the deployment on B and none of another deployment's cancelled.
func assertMemberEndedForTheCaps(t *testing.T, h *hedgeRig, md string, m *v1alpha1.Instance, outcomesBefore int) {
	t.Helper()
	if m != nil && !ended(m) {
		t.Fatalf("the member is %s, want ended: refused for the caps it holds no job and no queue age", m.Status.Phase)
	}
	if m != nil && failedEnd(m) {
		t.Fatal("the member's end counts as a failed end: a member dropped for the caps is no failure")
	}
	pass := h.passOf(md)
	if pv := pass.pvByName[hedgeB]; pv != nil {
		if streak := pass.classStreak(pv.class); len(streak) != 0 {
			t.Fatalf("the class's failure streak is %v, want none", streak)
		}
	}
	if n := startOutcomeCount(h, md); n != outcomesBefore {
		t.Fatalf("%d start outcomes, want the %d there were: the member never started, so its end adds none", n, outcomesBefore)
	}
	for _, a := range h.allAllocations() {
		if h.released(a.Name) {
			t.Fatalf("Allocation %s was released: ending a never-made member releases nothing", a.Name)
		}
	}
}

// TestTheNewerOfTwoRacingMembersRefusedForTheCapsEnds (row iii; the race case): two leaders (two slot tables, the second behind a
// cache that shows no Allocation made after its pass began) retry the two members X (older) and Y (newer) of one hedge on B, whose
// QOS caps a user at 1 GPU and has no submit limit. Both Instances exist and neither has its Allocation (the creates were cut
// short). In both orders of the leaders' passes:
//   - X, the older, takes B's GPU: 1 <= 1, its Allocation is made;
//   - Y, the newer, is refused for the CAPS (X's 1 GPU, recorded with its reservation where the list does not show it yet, + Y's
//     1 = 2 above 1), so Y ENDS: it holds no job and no queue age, nothing of the race is lost, no member pends behind a cap
//     of our own;
//   - B holds X's Allocation alone, nothing is released, and the deployment shows no SubmitLimitReached.
func TestTheNewerOfTwoRacingMembersRefusedForTheCapsEnds(t *testing.T) {
	for _, order := range []struct {
		name     string
		firstOne bool // the rig's own leader passes first
	}{
		{"the rig's leader passes first, the other leader second", true},
		{"the other leader passes first, the rig's leader second", false},
	} {
		t.Run(order.name, func(t *testing.T) {
			cut := &allocCutter{}
			h := newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b2", MaxGPUsPerUser: i32(1)}, withInterceptors(cut.funcs(interceptor.Funcs{})))
			h.batch.Enqueue(pfake.Script{})
			h.Step()
			d := mdSpec(hoMD)
			onBoth(d)
			h.create(d)
			for range 7 {
				tick10(h.rig) // to 08:01:10: P is ready on A
			}
			p := h.liveInstances(hoMD)[0].Name
			cut.arm(hoMD, 0)
			x := h.seedCutShortMember(hoMD, "", p, hedgeB) // 08:01:10
			h.clock.Advance(10 * time.Second)
			y := h.seedCutShortMember(hoMD, x, p, hedgeB) // 08:01:20
			cut.disarm()
			xi, yi := h.mustInstance(x), h.mustInstance(y)
			if !xi.CreationTimestamp.Time.Before(yi.CreationTimestamp.Time) {
				t.Fatalf("X created %v, Y %v, want X the older", xi.CreationTimestamp, yi.CreationTimestamp)
			}
			xAlloc, yAlloc := xi.Spec.AllocationRef.Name, yi.Spec.AllocationRef.Name
			if _, ok := h.allocation(xAlloc); ok {
				t.Fatal("X has its Allocation: the cut did not hold")
			}
			if _, ok := h.allocation(yAlloc); ok {
				t.Fatal("Y has its Allocation: the cut did not hold")
			}
			outcomes := startOutcomeCount(h, hoMD)
			other := h.newLeader()
			stale := newStaleReader(t, h.c) // shows no Allocation made from now on
			other.Reader = stale
			leaders := []*Reconciler{h.r, other}
			if !order.firstOne {
				leaders = []*Reconciler{other, h.r}
			}
			for _, r := range leaders {
				h.clock.Advance(10 * time.Second)
				if _, err := r.Reconcile(h.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}}); err != nil {
					t.Fatalf("reconcile: %v", err)
				}
			}
			if _, ok := h.allocation(xAlloc); !ok {
				t.Fatal("X, the older, has no Allocation: it takes B's one GPU")
			}
			if _, ok := h.allocation(yAlloc); ok {
				t.Fatal("Y, the newer, has its Allocation: B's one GPU is X's")
			}
			if n := holdersOn(h.rig, hedgeB); n != 1 {
				t.Fatalf("B holds %d Allocations, want X's alone: its cap is 1 GPU", n)
			}
			if in := instOrNil(h.rig, x); in == nil || ended(in) || !in.DeletionTimestamp.IsZero() {
				t.Fatalf("X is %+v after the race, want it kept", in)
			}
			assertMemberEndedForTheCaps(t, h, hoMD, instOrNil(h.rig, y), outcomes)
			if c := h.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
				t.Fatalf("D Degraded = %s %q, want no SubmitLimitReached: Y was refused for the caps", c.Reason, c.Message)
			}
		})
	}
}

package lifecycle

// What step 7 takes with a surplus (HBEP-7, How it counts, What step 7 takes; #616, PR 3): the ready surplus, the
// non-ready surplus beyond k per Provider less the deployment's warm Allocations, what is left alone, and each
// target's cause. Every expected target and cause is written out by hand in its row, never computed by
// scaleDownTargets, selectPrimaries or primaries.

import (
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// noOverAllocation pins overAllocation 0 on a fixture. The scale-down rows written before over-allocation lower
// instances while a primary that is not ready is still starting or pending, and expect it to be taken. With the
// default k = 1 step 7 leaves one such Instance per Provider alone, as the surplus k allows (HBEP-7, How it counts,
// Left alone), so these rows run at k = 0, where step 7 takes what it always took: the regression check of #616's PR
// 3. The rows of k = 1 are in rank_keep_test.go.
func noOverAllocation(md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	md.Spec.OverAllocation = ptrTo(int32(0))
	return md
}

// warmAlloc adds an Allocation that runs and hosts no Instance, on the Provider.
func (f *selFx) warmAlloc(name, provider string) {
	a := &v1alpha1.Allocation{}
	a.Name = name
	a.Labels = map[string]string{v1alpha1.LabelProvider: provider}
	a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	f.allocs[name] = a
}

// targetCauses reads the targets as "name:effect:cause", in order.
func targetCauses(ts []scaleTarget) []string {
	out := []string{}
	for _, t := range ts {
		out = append(out, t.instance.Name+":"+string(t.effect)+":"+string(t.cause))
	}
	return out
}

func wantTargets(t *testing.T, what string, got []scaleTarget, want ...string) {
	t.Helper()
	if g := targetCauses(got); !slices.Equal(g, want) {
		t.Errorf("%s: %v, want %v", what, g, want)
	}
}

// TestFrontProbeIntervalIsTheFrontsDefault: the ranked-out peer's drain waits one probe interval of the front. The
// constant here is a copy, since the lifecycle controller does not import the front; this pins it to the front's
// default. A front probeInterval set by hand is not seen: the launcher never sets one.
func TestFrontProbeIntervalIsTheFrontsDefault(t *testing.T) {
	if frontProbeInterval != front.DefaultProbeInterval {
		t.Fatalf("frontProbeInterval %s, the front's DefaultProbeInterval %s", frontProbeInterval, front.DefaultProbeInterval)
	}
	if frontProbeInterval != 10*time.Second {
		t.Fatalf("frontProbeInterval %s, want 10s", frontProbeInterval)
	}
}

// TestScaleDownLeavesUpToKAndNamesTheCause: instances 1, overAllocation 1, so one non-ready surplus Instance per
// Provider is left alone. Each row is a population and the targets it gives, by hand.
func TestScaleDownLeavesUpToKAndNamesTheCause(t *testing.T) {
	t.Run("one pending extra is left alone", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0))
	})
	t.Run("a second extra on the Provider is beyond k: the less advanced goes, pending, with the trim cause", func(t *testing.T) {
		// X started 10:05 is more advanced than Y, pending: X is left alone and Y, beyond k, is cancelled.
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 5, 0)))
		f.add("Y", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "Y:cancelPending:overAllocationTrim")
	})
	t.Run("started extras beyond k: their Instances end, the Allocation kept (endKeepAllocation)", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 5, 0)), passedChecksOpt())
		f.add("Y", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 6, 0)))
		p := f.pass(1, 1)
		// X passed, so it is more advanced than Y and is left alone.
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "Y:endKeepAllocation:overAllocationTrim")
	})
	t.Run("k is lowered to 0: the pending extra is cancelled and nothing drains", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(1, 0)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "X:cancelPending:overAllocationTrim")
	})
	t.Run("k is lowered to 0 with the extra started: its Instance ends, its Allocation is kept", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 5, 0)))
		p := f.pass(1, 0)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "X:endKeepAllocation:overAllocationTrim")
	})
	t.Run("a warm Allocation on the Provider uses the room", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		f.warmAlloc("warm-a", "prov-a")
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "X:cancelPending:overAllocationTrim")
		// A warm Allocation on another Provider uses none of it.
		g := newSelFx()
		g.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		g.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		g.warmAlloc("warm-b", "prov-b")
		q := g.pass(1, 1)
		wantTargets(t, "another Provider's warm Allocation", q.scaleDownTargets(q.primaries(), 0))
	})
	t.Run("k counts per Provider", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X-a", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		f.add("X-b", v1alpha1.InstancePending, createdAt(hoT(9, 6, 0)), onProv("prov-b"))
		p := f.pass(1, 1)
		wantTargets(t, "one extra on each Provider", p.scaleDownTargets(p.primaries(), 0))
	})
	t.Run("a surplus Instance with a live replacement is never left alone: taken with its replacement", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("X", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 5, 0)))
		f.add("X-repl", v1alpha1.InstancePending, replacementOf("X"))
		p := f.pass(1, 1)
		// X-repl is taken before X, pending, cancelled; X's Allocation is kept.
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "X-repl:cancelPending:overAllocationTrim", "X:endKeepAllocation:overAllocationTrim")
	})
	t.Run("a hedge of several members is never left alone: it goes whole", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("H", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		f.add("H-b", v1alpha1.InstanceStarting, createdAt(hoT(9, 6, 0)), inHedge("H"), jobStartedAt(hoT(10, 5, 0)), onProv("prov-b"))
		p := f.pass(1, 1)
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "H:cancelPending:overAllocationTrim", "H-b:endKeepAllocation:overAllocationTrim")
	})
}

// TestScaleDownTakesTheReadySurplusAndNamesItsCause: a ready surplus Instance always drains, the nearest hard end
// first, with the lowered cause, since only lowering spec.instances makes one; a ranked-out peer drains once its
// successor has been ready for the front's probe interval, with the trim cause.
func TestScaleDownTakesTheReadySurplusAndNamesItsCause(t *testing.T) {
	t.Run("instances lowered to 1: the nearest hard end drains, the extra is left", func(t *testing.T) {
		// P1 hard end 12:10 and P2 12:20 ready, X pending, k 1. Chosen at 1: P2 (the later hard end). P1 drains.
		f := newSelFx()
		f.add("P1", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("P2", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)))
		f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(1, 1)
		prim := p.primaries()
		sameNames(t, "primaries", prim, "P2")
		wantTargets(t, "targets", p.scaleDownTargets(prim, 0), "P1:drain:lowered")
	})
	t.Run("a Scale plan lowering from 2 to 1 lists P1 alone", func(t *testing.T) {
		f := newSelFx()
		f.add("P1", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("P2", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)))
		f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
		p := f.pass(2, 1)
		prim := p.primaries()
		sameNames(t, "primaries", prim, "P2", "P1")
		wantTargets(t, "no lowering", p.scaleDownTargets(prim, 0))
		wantTargets(t, "lowering by one", p.scaleDownTargets(prim, 1), "P1:drain:lowered")
	})
	t.Run("the excluded case: lowering keeps the only ready Instance and takes the verifying primary with its replacement", func(t *testing.T) {
		// P1 verifying with a live replacement R1, P2 ready. instances 2, lowered to 1: chosen P2, whatever HBEP-7's
		// earlier order said. P1 ends with its Allocation kept, R1 is cancelled.
		f := newSelFx()
		f.add("P1", v1alpha1.InstanceVerifying, jobStartedAt(hoT(9, 10, 0)))
		f.add("R1", v1alpha1.InstancePending, replacementOf("P1"))
		f.add("P2", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)), jobStartedAt(hoT(9, 0, 0)))
		p := f.pass(2, 0)
		prim := p.primaries()
		sameNames(t, "primaries", prim, "P2", "P1")
		wantTargets(t, "lowering by one", p.scaleDownTargets(prim, 1), "R1:cancelPending:lowered", "P1:endKeepAllocation:lowered")
	})
	t.Run("a marked peer waits for the probe interval, then drains with the trim cause", func(t *testing.T) {
		// P (ready, hard end 12:15) is marked for X, ready since 10:00:00. At 10:00:05 nothing is taken, and a
		// pass is asked for at 10:00:10; at 10:00:10 P drains. The selection at the current count already leaves P
		// over, so its cause is the trim.
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 15, 0)), rankedOutBy("X"))
		f.add("X", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)), readyAt(hoT(10, 0, 0)))
		p := f.pass(1, 1)
		p.now = hoT(10, 0, 5)
		prim := p.primaries()
		sameNames(t, "primaries", prim, "X")
		wantTargets(t, "at +5 s", p.scaleDownTargets(prim, 0))
		if !p.next.Equal(hoT(10, 0, 10)) {
			t.Errorf("next pass asked at %v, want 10:00:10", p.next)
		}
		p.now = hoT(10, 0, 10)
		wantTargets(t, "at +10 s", p.scaleDownTargets(prim, 0), "P:drain:overAllocationTrim")
	})
	t.Run("a ready surplus that is no marked peer is lowered, not a trim, whatever the count it is read at", func(t *testing.T) {
		f := newSelFx()
		f.add("P1", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.add("P2", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)))
		p := f.pass(1, 1)
		// instances is already 1 in the spec: P1 is surplus now, and still not a trim.
		wantTargets(t, "targets", p.scaleDownTargets(p.primaries(), 0), "P1:drain:lowered")
	})
}

// TestScaleDownCauseIsTheCurrentCountsMembership: instances 2 with P1 ready, P2 and the extra X pending, k 1. At the
// current count P2 is chosen, being older than X, and X is left alone, so nothing is taken. Lowering to 1 drops P2,
// which outranks X in the fixed order that decides what is left alone, so P2 is left alone as the extra and X is
// taken: only because of the lowering, so its cause is lowered. The count that takes it is not the current one.
func TestScaleDownCauseIsTheCurrentCountsMembership(t *testing.T) {
	f := newSelFx()
	f.add("P1", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
	f.add("P2", v1alpha1.InstancePending, createdAt(hoT(9, 1, 0)))
	f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 5, 0)))
	p := f.pass(2, 1)
	prim := p.primaries()
	sameNames(t, "primaries", prim, "P1", "P2")
	wantTargets(t, "current count", p.scaleDownTargets(prim, 0))
	wantTargets(t, "lowered to 1", p.scaleDownTargets(prim, 1), "X:cancelPending:lowered")
}

// TestScaleDownTargetsAreMonotoneWithASurplus: for a lower desired count, every target of a higher one is a target,
// by Instance and by Allocation, with k, two Providers and a warm Allocation in play. instances 3, k 1:
//
//	A: P1 ready 12:10, P2 ready 12:20, P3 started 10:00, X pending created 09:10, and the warm Allocation warm-a
//	B: Y passed, started 10:05, and Z pending created 09:20
//
// The order is P2, P1, Y, P3, X, Z. At 3 the chosen are P2, P1 and Y; A's room is k less the warm Allocation, 0,
// so P3 and X are taken; B's room is 1, which the fixed order gives to the more advanced Z. At 2 the chosen are P2
// and P1; Y joins Z in B's competition and, being more advanced, takes the room, so Z is taken too. At 1 P1 drains;
// at 0 P2 does too. The sets, by hand: {X, P3}; {Z, X, P3}; {Z, X, P3, P1}; {Z, X, P3, P1, P2}.
func TestScaleDownTargetsAreMonotoneWithASurplus(t *testing.T) {
	f := newSelFx()
	f.add("P1", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
	f.add("P2", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)))
	f.add("P3", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 0, 0)))
	f.add("X", v1alpha1.InstancePending, createdAt(hoT(9, 10, 0)))
	f.add("Y", v1alpha1.InstanceVerifying, passedChecksOpt(), jobStartedAt(hoT(10, 5, 0)), onProv("prov-b"))
	f.add("Z", v1alpha1.InstancePending, createdAt(hoT(9, 20, 0)), onProv("prov-b"))
	f.warmAlloc("warm-a", "prov-a")
	p := f.pass(3, 1)
	prim := p.primaries()
	sameNames(t, "primaries", prim, "P2", "P1", "Y")
	want := [][]string{
		{"P3", "X"},
		{"P3", "X", "Z"},
		{"P3", "X", "Z", "P1"},
		{"P3", "X", "Z", "P1", "P2"},
	}
	var prev []string
	for s := 0; s <= 3; s++ {
		var got, gotAllocs []string
		for _, tg := range p.scaleDownTargets(prim, s) {
			got = append(got, tg.instance.Name)
			gotAllocs = append(gotAllocs, tg.allocation.Name)
		}
		slices.Sort(got)
		slices.Sort(gotAllocs)
		w := slices.Clone(want[s])
		slices.Sort(w)
		var wAllocs []string
		for _, n := range w {
			wAllocs = append(wAllocs, "alloc-"+n)
		}
		if !slices.Equal(got, w) || !slices.Equal(gotAllocs, wAllocs) {
			t.Fatalf("lowered by %d: targets %v (Allocations %v), want %v (%v)", s, got, gotAllocs, w, wAllocs)
		}
		for _, n := range prev {
			if !slices.Contains(got, n) {
				t.Fatalf("lowered by %d drops %s, which lowering by %d takes: not monotone", s, n, s-1)
			}
		}
		prev = got
	}
}

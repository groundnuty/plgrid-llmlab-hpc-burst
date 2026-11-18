package inventory

// The token budget's inputs (HBEP-5, Workloads, The count in tokens): an Instance's B is its status.kvCacheTokens times the
// workers its cap counts, which are the healthy workers behind a router and else the one worker of an Instance without one
// (instanceCap's own count), and a Workload's charge c is its shape's typical context plus typical output. Unknown is nil, and
// the Instance then meets the count rule alone. Every expected number is worked by hand from the fixture's launch.maxNumSeqs of 48
// and the sentence above.

import (
	"math"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func i64(v int64) *int64 { return &v }

// budgetSnap is routedSnap's Instance with kvCacheTokens kv, n launch workers and healthyWorkers h, a router when h is set.
func budgetSnap(kv *int64, n int32, h *int32, instanceCap *int32) Snapshot {
	s := routedSnap(h)
	s.Instances[0].Spec.Launch.Workers = &n
	s.Instances[0].Status.KVCacheTokens = kv
	if instanceCap != nil {
		s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: instanceCap}
	}
	return s
}

func i32(v int32) *int32 { return &v }

func TestTheTokenBudgetIsKVCacheTokensTimesTheWorkersTheCapCounts(t *testing.T) {
	for _, c := range []struct {
		name      string
		kv        *int64
		launch    int32  // launch.workers
		healthy   *int32 // status.healthyWorkers, nil: no router
		instCap   *int32
		wantCap   int
		wantB     int64
		wantKnown bool
	}{
		// No router: the Instance has one worker, the cap is 48 x 1, and B is 1,000 x 1.
		{"no router", i64(1000), 1, nil, nil, 48, 1000, true},
		// Beside the cap's own rule: with no healthyWorkers the cap counts the one worker, whatever launch.workers says (48 x 1), so B
		// does too (1,000 x 1), never 1,000 x 2.
		{"no healthyWorkers yet, two launch workers", i64(1000), 2, nil, nil, 48, 1000, true},
		// A router with one healthy worker: 48 x 1 and 1,000 x 1.
		{"one healthy worker", i64(1000), 1, i32(1), nil, 48, 1000, true},
		// Three healthy of three: cap 48 x 3 = 144, B 1,000 x 3 = 3,000.
		{"three healthy workers", i64(1000), 3, i32(3), nil, 144, 3000, true},
		// Three healthy of four, instanceCap 10: cap 10 x 3 / 4 = 7 (rounded down); B 1,000 x 3 = 3,000, whatever the cap's rule.
		{"three healthy of four with an instanceCap", i64(1000), 4, i32(3), i32(10), 7, 3000, true},
		// No healthy worker: B unknown (the Instance is not eligible anyway), cap 0.
		{"no healthy worker", i64(1000), 3, i32(0), nil, 0, 0, false},
		// No kvCacheTokens: B unknown, whatever the workers are.
		{"no kvCacheTokens, a router", nil, 3, i32(3), nil, 144, 0, false},
		{"no kvCacheTokens, no router", nil, 1, nil, nil, 48, 0, false},
		// A size no engine reports: the product 2^62 x 2 = 2^63 would wrap, so B is unknown, never a wrapped number.
		{"a product that does not fit an int64", i64(1 << 62), 2, i32(2), nil, 96, 0, false},
		// One below that edge: (2^62 - 1) x 2 = 2^63 - 2 fits.
		{"the largest product that fits", i64(1<<62 - 1), 2, i32(2), nil, 96, 1<<63 - 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := member(t, budgetSnap(c.kv, c.launch, c.healthy, c.instCap))
			if m.Cap != c.wantCap {
				t.Errorf("cap %d, want %d", m.Cap, c.wantCap)
			}
			b, ok := m.TokenBudget()
			if ok != c.wantKnown || b != c.wantB {
				t.Errorf("B = %d (known %v), want %d (known %v)", b, ok, c.wantB, c.wantKnown)
			}
			if c.kv != nil && (m.KVCacheTokens == nil || *m.KVCacheTokens != *c.kv) {
				t.Errorf("the view's kvCacheTokens %v, want %d", m.KVCacheTokens, *c.kv)
			}
			if c.kv == nil && m.KVCacheTokens != nil {
				t.Errorf("the view's kvCacheTokens %d, want none", *m.KVCacheTokens)
			}
		})
	}
}

// The view holds its own copy of the size: a later change to the snapshot's object does not move a view already built.
func TestTheViewCopiesKVCacheTokens(t *testing.T) {
	s := budgetSnap(i64(1000), 1, nil, nil)
	m := member(t, s)
	*s.Instances[0].Status.KVCacheTokens = 5
	if b, ok := m.TokenBudget(); !ok || b != 1000 {
		t.Fatalf("B = %d (known %v) after the snapshot's value changed, want 1000", b, ok)
	}
}

// A size below 1 is no size (the field's minimum): the budget is unknown, never 0 (which would refuse every request).
func TestAKVCacheSizeBelowOneIsNoBudget(t *testing.T) {
	for _, v := range []int64{0, -5} {
		m := member(t, budgetSnap(i64(v), 1, i32(1), nil))
		if b, ok := m.TokenBudget(); ok {
			t.Errorf("kvCacheTokens %d gave B = %d, want unknown", v, b)
		}
	}
}

// The charge is the Workload's typical context plus typical output, in int64: two int32 maxima sum past an int32.
func TestAWorkloadsChargeIsItsTypicalContextPlusItsTypicalOutput(t *testing.T) {
	mk := func(name string, ctx, out int32) v1alpha1.Workload {
		w := workload("ns", name, "m")
		w.Spec.Shape.ContextTokens.Typical = ctx
		w.Spec.Shape.OutputTokens.Typical = out
		return w
	}
	s := batchSnap()
	s.Workloads = []v1alpha1.Workload{
		mk("long", 2000, 500),
		mk("short", 300, 40),
		mk("edge", math.MaxInt32, math.MaxInt32),
		mk("noshape", 0, 0), // a Workload built with no shape (the older tests' fixtures): charge 0
	}
	v := Build(s, Inputs{Now: now})
	for name, want := range map[string]int64{
		"long":    2500,       // 2,000 + 500
		"short":   340,        // 300 + 40
		"edge":    4294967294, // 2 x 2,147,483,647
		"noshape": 0,
	} {
		ws := v.Workloads["m"][name]
		if len(ws) != 1 || ws[0].Charge != want {
			t.Errorf("Workload %s: %+v, want one with charge %d", name, ws, want)
		}
	}
}

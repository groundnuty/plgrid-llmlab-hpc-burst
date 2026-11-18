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

func TestTheTokenBudgetIsKVCacheTokensAndTheWorkersTheCapCounts(t *testing.T) {
	for _, c := range []struct {
		name      string
		kv        *int64
		launch    int32  // launch.workers
		healthy   *int32 // status.healthyWorkers, nil: no router
		instCap   *int32
		wantCap   int
		wantPer   int64 // B's per-worker factor
		wantW     int   // B's workers factor
		wantKnown bool
	}{
		// No router: the Instance has one worker, the cap is 48 x 1, and B is 1,000 x 1.
		{"no router", i64(1000), 1, nil, nil, 48, 1000, 1, true},
		// Beside the cap's own rule: with no healthyWorkers the cap counts the one worker, whatever launch.workers says (48 x 1), so B
		// does too (1,000 x 1), never 1,000 x 2.
		{"no healthyWorkers yet, two launch workers", i64(1000), 2, nil, nil, 48, 1000, 1, true},
		// A router with one healthy worker: 48 x 1 and 1,000 x 1.
		{"one healthy worker", i64(1000), 1, i32(1), nil, 48, 1000, 1, true},
		// Three healthy of three: cap 48 x 3 = 144, B 1,000 x 3 = 3,000.
		{"three healthy workers", i64(1000), 3, i32(3), nil, 144, 1000, 3, true},
		// Three healthy of four, instanceCap 10: cap 10 x 3 / 4 = 7 (rounded down); B 1,000 x 3 = 3,000, whatever the cap's rule.
		{"three healthy of four with an instanceCap", i64(1000), 4, i32(3), i32(10), 7, 1000, 3, true},
		// No healthy worker: B unknown (the Instance is not eligible anyway), cap 0.
		{"no healthy worker", i64(1000), 3, i32(0), nil, 0, 0, 0, false},
		// No kvCacheTokens: B unknown, whatever the workers are.
		{"no kvCacheTokens, a router", nil, 3, i32(3), nil, 144, 0, 0, false},
		{"no kvCacheTokens, no router", nil, 1, nil, nil, 48, 0, 0, false},
		// A budget whose product 2^62 x 2 = 2^63 is above any int64 is STILL KNOWN: the Instance carries its two factors, never a
		// wrapped number and never "unknown" (the balancer compares them exactly).
		{"a product above an int64 stays known", i64(1 << 62), 2, i32(2), nil, 96, 1 << 62, 2, true},
		// The largest size any report can carry, with two workers.
		{"the largest size, two workers", i64(math.MaxInt64), 2, i32(2), nil, 96, math.MaxInt64, 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := member(t, budgetSnap(c.kv, c.launch, c.healthy, c.instCap))
			if m.Cap != c.wantCap {
				t.Errorf("cap %d, want %d", m.Cap, c.wantCap)
			}
			per, workers, ok := m.TokenBudget()
			if ok != c.wantKnown || per != c.wantPer || workers != c.wantW {
				t.Errorf("B = %d x %d (known %v), want %d x %d (known %v)", per, workers, ok, c.wantPer, c.wantW, c.wantKnown)
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
	if per, workers, ok := m.TokenBudget(); !ok || per != 1000 || workers != 1 {
		t.Fatalf("B = %d x %d (known %v) after the snapshot's value changed, want 1000 x 1", per, workers, ok)
	}
}

// A size below 1 is no size (the field's minimum): the budget is unknown, never 0 (which would refuse every request).
func TestAKVCacheSizeBelowOneIsNoBudget(t *testing.T) {
	for _, v := range []int64{0, -5} {
		m := member(t, budgetSnap(i64(v), 1, i32(1), nil))
		if per, workers, ok := m.TokenBudget(); ok {
			t.Errorf("kvCacheTokens %d gave B = %d x %d, want unknown", v, per, workers)
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

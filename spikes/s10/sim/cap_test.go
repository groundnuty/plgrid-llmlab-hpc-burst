package sim

// Spike S10b: the hard per-Instance cap with a front queue, and the adaptive
// bound's control step. Expected values are worked out by hand.

import (
	"math"
	"testing"
)

// capCluster has negligible prefill, so a request lasts out * 0.1 s, and an
// engine limit (64) far above anything these tests place.
func capCluster() Cluster {
	return Cluster{Instances: 2, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 1e9, TPOT: 0.1}
}

// six one-turn conversations sharing one system prompt (so one key and one
// home, H), arriving 1 ms apart, lasting 1, 2, 3, 4, 5 and 6 s.
func capConvs() []Conversation {
	var convs []Conversation
	for i := 0; i < 6; i++ {
		c := conv(i, float64(i)*0.001, 160, 16, 1, 10*(i+1), 0, 0)
		c.Sys = []Seg{{ID: SegID(300), Len: 160}}
		convs = append(convs, c)
	}
	return convs
}

// Two Instances, cap 2, c = 2.0, one shared key. By hand:
//
//	r1, r2 -> H (load 2, the cap).
//	r3: T = 2, bound 2*3/2 = 3, H at 2 passes, but is at the cap -> next on
//	    the ring, O (0): a spill. r4: the same, O at 1 -> O (2): a spill.
//	r5, r6: every Instance holds 2 -> they wait at the front.
//	r1 ends at 1.0 s: r5 (arrived 0.004) is placed on H, waiting 0.996 s.
//	r2 ends at 2.0 s: r6 (arrived 0.005) is placed on H, waiting 1.995 s.
//
// Without the cap, c = 2.0 keeps all six on H (loads 2..5 against bounds
// 3, 4, 5, 6), which is what makes the cap bind here.
func TestCapSpillsThenQueuesAtTheFront(t *testing.T) {
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 200, Cap: 2}
	m := runSim(t, capCluster(), pol, capConvs(), nil)
	if m.Spills != 2 {
		t.Errorf("spills %d, want 2", m.Spills)
	}
	if m.FrontHeld != 2 || len(m.FrontWaits) != 2 {
		t.Fatalf("held at the front %d, placed from it %d, want 2 and 2", m.FrontHeld, len(m.FrontWaits))
	}
	for i, want := range []float64{0.996, 1.995} {
		if got := m.FrontWaits[i]; math.Abs(got-want) > 0.002 {
			t.Errorf("wait %d is %.4f s, want %.3f", i, got, want)
		}
	}
	if m.PeakLoad != 2 {
		t.Errorf("peak in flight %d, want the cap 2", m.PeakLoad)
	}
	if len(m.Recs) != 6 {
		t.Errorf("%d requests completed, want 6", len(m.Recs))
	}
	// the control: no cap, all six pile onto H
	free := runSim(t, capCluster(), Policy{Kind: CHWBL, Key: KeySystem, Percent: 200}, capConvs(), nil)
	if free.PeakLoad != 6 || free.Spills != 0 || free.FrontHeld != 0 {
		t.Errorf("without the cap: peak %d (want 6), spills %d, held %d (want 0, 0)", free.PeakLoad, free.Spills, free.FrontHeld)
	}
}

// Latency counts the wait at the front: r6 arrives at 0.005, is placed at
// 2.0 and lasts 6 s, so it takes about 7.995 s.
func TestFrontWaitCountsInLatency(t *testing.T) {
	cl := capCluster()
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 200, Cap: 2}
	e := NewEngine(cl, pol, noCompaction(), capConvs(), nil)
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	if got := e.M.latencies[len(e.M.latencies)-1]; math.Abs(got-7.995) > 0.01 {
		t.Errorf("longest latency %.3f s, want 7.995 (1.995 s waiting plus 6 s of service)", got)
	}
}

// An adaptive bound: four Instances, desired 4, control step 1 s, ticks at
// 0.5, 1.5, 2.5, ... s. Instance 1 drains at 2.3 s, so the first tick that
// sees 3 of 4 ready is 2.5 s. The bound is 1.25 before it and 2.0 after,
// the switch is recorded once, and a run without the drain never switches.
func TestAdaptiveBoundSwitchesAtTheFirstTickAfterTheDrain(t *testing.T) {
	pol := Policy{Kind: CHWBL, Key: KeyOpening, Percent: 125, AdaptiveLoose: 200, ControlStep: 1, ReleaseOnDrain: true}
	m := runSim(t, slowCluster(4), pol, loadedConvs(64, false), []Event{{Time: 2.3, Kind: Drain, Instance: 1}})
	if len(m.Switches) != 1 {
		t.Fatalf("%d switches, want 1: %v", len(m.Switches), m.Switches)
	}
	sw := m.Switches[0]
	if sw.From != 125 || sw.To != 200 || math.Abs(sw.T-2.5) > 1e-9 {
		t.Errorf("switch %+v, want 125 -> 200 at 2.5 s", sw)
	}
	if got := m.PctSpan[125][1]; got >= 2.5 {
		t.Errorf("a placement at 1.25 at %.3f s, after the switch at 2.5 s", got)
	}
	if got := m.PctSpan[200][0]; got < 2.5 {
		t.Errorf("a placement at 2.0 at %.3f s, before the switch at 2.5 s", got)
	}
	if m.PctSpan[200][1] <= m.PctSpan[200][0] {
		t.Error("only one placement was made at 2.0; the run does not test the bound after the switch")
	}
	steady := runSim(t, slowCluster(4), pol, loadedConvs(64, false), nil)
	if len(steady.Switches) != 0 || len(steady.PctSpan) != 1 {
		t.Errorf("no drain: %d switches, bounds used %v; want none and only 1.25", len(steady.Switches), steady.PctSpan)
	}
	plain := runSim(t, slowCluster(4), Policy{Kind: CHWBL, Key: KeyOpening, Percent: 125}, loadedConvs(64, false), nil)
	if steady.HitTokens != plain.HitTokens || steady.Placements != plain.Placements {
		t.Errorf("adaptive without a drain differs from c = 1.25: hit tokens %d vs %d", steady.HitTokens, plain.HitTokens)
	}
}

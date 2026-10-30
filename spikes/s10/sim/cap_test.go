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

// Spike S10c, option 5: the cap works only while every desired Instance is
// ready. Three Instances, one shared key (home H), cap 2, tight bound 3.0 and
// loose bound 6.0, control step 1 s (ticks at 0.5, 1.5, 2.5, ... s), every
// request lasting 10 s. The Instance that is neither H nor H's spill target
// (call it X) drains at 1.2 s, so the first tick that sees 2 of 3 ready is
// 1.5 s.
//
//	0.000, 0.001: r1, r2 -> H (load 2, the cap).
//	0.002: r3: the bound passes H (2 <= 3.0*4/3), but H is at the cap -> spill
//	       to the next Instance on the ring, S.            Spills so far: 1.
//	1.5:   the tick sees 2 of 3 ready: the bound goes 3.0 -> 6.0 and the cap
//	       2 -> none, as one switch.
//	2.0, 2.001, 2.002: r4, r5, r6. The members are H (2) and S (1). With the
//	       cap off they all go to H: loads 3, 4, 5. Peak 5, no further spill.
//
// The control is the same policy with the cap kept after the drain (option
// 3's cap on option 4's bound): r4 spills to S (load 2), and r5 and r6 find H
// and S full and are held. The cap would have bound after 1.5 s: 1 spill and
// 2 held requests after it, 2 spills in all (r3 and r4). That is what makes
// "0 spills after the switch" mean something.
func TestCapFullOnlySwitchesOffWithTheBound(t *testing.T) {
	cl := Cluster{Instances: 3, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 1e9, TPOT: 0.1}
	mkConvs := func(n int) []Conversation {
		var convs []Conversation
		for i := 0; i < n; i++ {
			at := float64(i) * 0.001
			if i >= 3 {
				at = 2.0 + float64(i-3)*0.001
			}
			c := conv(i, at, 160, 16, 1, 100, 0, 0)
			c.Sys = []Seg{{ID: SegID(300), Len: 160}}
			convs = append(convs, c)
		}
		return convs
	}
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 300, AdaptiveLoose: 600, ControlStep: 1, Cap: 2, CapFullOnly: true, ReleaseOnDrain: true}

	// Find H and S, then the Instance X that neither of them is, from the
	// first three requests alone (no drain, so the cap decides everything).
	probe := runSim(t, cl, pol, mkConvs(3), nil)
	used := map[int]int{}
	for _, r := range probe.Recs {
		used[r.Inst]++
	}
	if len(used) != 2 || probe.Spills != 1 {
		t.Fatalf("probe: %d Instances used (%v), %d spills; want 2 Instances (H with two, S with one) and 1 spill", len(used), used, probe.Spills)
	}
	x := -1
	for i := 0; i < cl.Instances; i++ {
		if used[i] == 0 {
			x = i
		}
	}
	events := []Event{{Time: 1.2, Kind: Drain, Instance: x}}

	m := runSim(t, cl, pol, mkConvs(6), events)
	if len(m.Switches) != 1 {
		t.Fatalf("%d switches, want 1: %v", len(m.Switches), m.Switches)
	}
	sw := m.Switches[0]
	if math.Abs(sw.T-1.5) > 1e-9 || sw.From != 300 || sw.To != 600 || sw.CapFrom != 2 || sw.CapTo != 0 {
		t.Errorf("switch %+v, want 300 -> 600 and cap 2 -> none, both at 1.5 s", sw)
	}
	if m.Spills != 1 || m.SpillsBetween(0, 1.5) != 1 || m.SpillsBetween(1.5, 1e9) != 0 {
		t.Errorf("spills %d (%d before 1.5 s, %d after), want 1, 1 and 0", m.Spills, m.SpillsBetween(0, 1.5), m.SpillsBetween(1.5, 1e9))
	}
	if m.FrontHeld != 0 {
		t.Errorf("%d requests held at the front, want 0", m.FrontHeld)
	}
	if m.PeakLoad != 5 {
		t.Errorf("peak in flight %d, want 5 (H takes r4, r5 and r6 on top of its 2)", m.PeakLoad)
	}
	if len(m.Recs) != 6 {
		t.Errorf("%d requests completed, want 6", len(m.Recs))
	}

	ctl := pol
	ctl.CapFullOnly = false
	c := runSim(t, cl, ctl, mkConvs(6), events)
	if c.Spills != 2 || c.SpillsBetween(1.5, 1e9) != 1 || c.HeldBetween(1.5, 1e9) != 2 || c.PeakLoad != 2 {
		t.Errorf("control with the cap kept: %d spills (%d after 1.5 s), %d held after 1.5 s, peak %d; want 2, 1, 2 and 2",
			c.Spills, c.SpillsBetween(1.5, 1e9), c.HeldBetween(1.5, 1e9), c.PeakLoad)
	}
	if len(c.Switches) != 1 || c.Switches[0].CapFrom != 2 || c.Switches[0].CapTo != 2 {
		t.Errorf("control switches %v, want the bound to change once and the cap to stay 2", c.Switches)
	}
}

// A request held at the front when the cap goes off is placed at the switch,
// not at the next completion. Two Instances, desired 2, cap 1, one key. By
// hand: r1 -> H, r2 spills to S, r3 is held (all at the cap); the Instance H
// does not drain, S does at 0.2 s, and the 0.5 s tick turns the cap off, so r3
// is placed at 0.5 s: a wait of about 0.5 s, long before any completion at
// 10 s.
func TestCapFullOnlyReleasesHeldRequestsWhenTheCapGoesOff(t *testing.T) {
	cl := Cluster{Instances: 2, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 1e9, TPOT: 0.1}
	mk := func() []Conversation {
		var convs []Conversation
		for i := 0; i < 3; i++ {
			c := conv(i, float64(i)*0.001, 160, 16, 1, 100, 0, 0)
			c.Sys = []Seg{{ID: SegID(300), Len: 160}}
			convs = append(convs, c)
		}
		return convs
	}
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 300, AdaptiveLoose: 600, ControlStep: 1, Cap: 1, CapFullOnly: true, ReleaseOnDrain: true}
	probe := runSim(t, cl, pol, mk()[:2], nil)
	var s int
	for _, r := range probe.Recs {
		if r.Conv == 1 {
			s = r.Inst // r2, the spilled request, sits on S
		}
	}
	m := runSim(t, cl, pol, mk(), []Event{{Time: 0.2, Kind: Drain, Instance: s}})
	if m.FrontHeld != 1 || len(m.FrontWaits) != 1 {
		t.Fatalf("held %d, placed from the front %d, want 1 and 1", m.FrontHeld, len(m.FrontWaits))
	}
	if w := m.FrontWaits[0]; math.Abs(w-0.498) > 0.01 {
		t.Errorf("r3 waited %.3f s, want 0.498 (placed at the 0.5 s tick, not when a request ends at 10 s)", w)
	}
}

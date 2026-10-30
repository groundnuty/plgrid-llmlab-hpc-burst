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

// Spike S10c review: the cap comes back when a replacement joins. Option 5's
// state machine on a small scale (cap 2 in place of 48, tight bound 3.0 and
// loose bound 6.0 in place of 1.25 and 2.0, so that the cap can bind on a
// handful of requests), with a drain followed by the join of a spare
// Instance. Three desired Instances and one spare, control step 1 s (ticks at
// 0.5, 1.5, 2.5, ... s), one shared key whose home is H, every request
// lasting 30 s. X is an Instance that is not H and drains at 1.2 s; the
// spare joins at 3.2 s.
//
//	1.5:   the tick sees 2 of 3 ready: bound 3.0 -> 6.0, cap 2 -> none.
//	2.000, 2.001, 2.002, 2.003: r1..r4. With the cap off and bound 6.0 over
//	       two members, H passes every time: H holds 4, above the cap of 2.
//	3.2:   the spare joins (cold, 0 in flight). The bound and the cap stay as
//	       they are until the next tick; nothing is placed in between.
//	3.5:   the tick sees 3 of 3 ready: bound 6.0 -> 3.0, cap none -> 2.
//	4.000, 4.001, 4.002, 4.003: r5..r8. The bound is 3.0*(T+1)/3 = T+1 for
//	       T in flight, and H (4, so 5 with the request) passes it each
//	       time, so H is the choice, and H holds 4, at or above the cap: each
//	       spills to the next Instance on the ring that holds fewer than 2.
//	       r5, r6 go to the first Instance after H (call it N1: 1, then 2),
//	       r7 and r8 to the other (N2: 1, then 2). H is never given another
//	       request and nothing is cut.
//	4.004: r9: H holds 4, N1 and N2 hold 2: every Instance is at or above the
//	       cap, so r9 is held at the front.
//	32.000-32.003: r1, r2, r3, r4 end (each 30 s after its start). H falls to
//	       3, 2, then 1: r9 is retried each time, and is still held while H
//	       holds 2 or more. When r3 ends at 32.002 H holds 1, below the cap,
//	       and r9 is placed on H: a wait of 32.002 - 4.004 = 27.998 s, not
//	       the 0.5 s a held request waits when the cap goes off.
//
// The control is the same run without the join: the bound stays 6.0 and the
// cap stays off, so r5..r9 all go to H (peak 9, no spill, none held, one
// switch).
func TestCapFullOnlyComesBackWhenAReplacementJoins(t *testing.T) {
	cl := Cluster{Instances: 3, Spare: 1, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 1e9, TPOT: 0.1}
	arrivals := []float64{2.000, 2.001, 2.002, 2.003, 4.000, 4.001, 4.002, 4.003, 4.004}
	mkConvs := func(n int) []Conversation {
		var convs []Conversation
		for i := 0; i < n; i++ {
			c := conv(i, arrivals[i], 160, 16, 1, 300, 0, 0)
			c.Sys = []Seg{{ID: SegID(300), Len: 160}}
			convs = append(convs, c)
		}
		return convs
	}
	pol := Policy{Kind: CHWBL, Key: KeySystem, Percent: 300, AdaptiveLoose: 600, ControlStep: 1, Cap: 2, CapFullOnly: true, ReleaseOnDrain: true}

	// H from one request on its own; X is the first Instance that is not H.
	probe := runSim(t, cl, pol, mkConvs(1), nil)
	h := probe.Recs[0].Inst
	x := 0
	if h == 0 {
		x = 1
	}
	spare := cl.Instances // the spare's index
	drain, join := Event{Time: 1.2, Kind: Drain, Instance: x}, Event{Time: 3.2, Kind: Join, Instance: spare}

	m := runSim(t, cl, pol, mkConvs(9), []Event{drain, join})
	if len(m.Switches) != 2 {
		t.Fatalf("%d switches, want 2 (off at the drain, on after the join): %v", len(m.Switches), m.Switches)
	}
	if sw := m.Switches[0]; math.Abs(sw.T-1.5) > 1e-9 || sw.From != 300 || sw.To != 600 || sw.CapFrom != 2 || sw.CapTo != 0 {
		t.Errorf("first switch %+v, want 300 -> 600 and cap 2 -> none at 1.5 s", sw)
	}
	if sw := m.Switches[1]; math.Abs(sw.T-3.5) > 1e-9 || sw.From != 600 || sw.To != 300 || sw.CapFrom != 0 || sw.CapTo != 2 {
		t.Errorf("second switch %+v, want 600 -> 300 and cap none -> 2 at 3.5 s, the first tick after the join at 3.2 s", sw)
	}
	if got := m.PctSpan[600][1]; got >= 3.5 {
		t.Errorf("a placement at the loose bound at %.3f s, after the switch back at 3.5 s", got)
	}
	if got := m.PctSpan[300][1]; got < 4.0 {
		t.Errorf("the last placement at the tight bound is at %.3f s, want one at 4.0 s or later (r5 to r9)", got)
	}
	place := map[int]int{} // request -> Instance
	for _, r := range m.Recs {
		place[r.Conv] = r.Inst
	}
	if len(m.Recs) != 9 {
		t.Fatalf("%d requests completed, want 9: nothing may be cut or lost", len(m.Recs))
	}
	for i := 0; i < 4; i++ {
		if place[i] != h {
			t.Errorf("r%d on instance %d, want H (%d): the loose bound with no cap sends every request to H", i+1, place[i], h)
		}
	}
	loads := map[int]int{}
	for i := 4; i < 8; i++ {
		if place[i] == h {
			t.Errorf("r%d placed on H, which holds 4 against a cap of 2", i+1)
		}
		loads[place[i]]++
	}
	if len(loads) != 2 || loads[place[4]] != 2 || loads[place[6]] != 2 || place[4] == place[6] {
		t.Errorf("r5..r8 went to %v, want two Instances other than H with two requests each", loads)
	}
	if place[8] != h {
		t.Errorf("r9 on %d, want H (%d), the first Instance to fall below the cap", place[8], h)
	}
	if m.PeakLoad != 4 {
		t.Errorf("peak in flight %d, want 4: H holds 4 when the cap returns and takes no more until it is below 2", m.PeakLoad)
	}
	if m.Spills != 4 || m.SpillsBetween(0, 3.5) != 0 || m.SpillsBetween(3.5, 1e9) != 4 {
		t.Errorf("%d spills (%d before 3.5 s, %d from it), want 4, 0 and 4", m.Spills, m.SpillsBetween(0, 3.5), m.SpillsBetween(3.5, 1e9))
	}
	if m.FrontHeld != 1 || len(m.FrontWaits) != 1 {
		t.Fatalf("held %d, placed from the front %d, want 1 and 1 (r9)", m.FrontHeld, len(m.FrontWaits))
	}
	if w := m.FrontWaits[0]; math.Abs(w-27.998) > 0.01 {
		t.Errorf("r9 waited %.3f s, want 27.998 (until r3 ends and H holds 1)", w)
	}

	// The control: no replacement joins, so nothing brings the cap back.
	c := runSim(t, cl, pol, mkConvs(9), []Event{drain})
	if len(c.Switches) != 1 || c.Spills != 0 || c.FrontHeld != 0 || c.PeakLoad != 9 {
		t.Errorf("without the join: %d switches, %d spills, %d held, peak %d; want 1, 0, 0 and 9 (every request on H)", len(c.Switches), c.Spills, c.FrontHeld, c.PeakLoad)
	}
}

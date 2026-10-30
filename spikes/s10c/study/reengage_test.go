package study

// Option 5 after a drain followed by a replacement: the bound goes back to
// c = 1.25 and the cap back to 48 at the next control tick after the ready
// Instances equal the desired ones again, while the surviving Instances may
// hold more than 48. Option 5's own constants (125, 200 and 48, the 10 s
// control step), on a small hand-built load, not the sweep. The expected
// values are worked out below from the rule in HBEP-5 and the issue, not
// taken from the run. The same sequence with a cap of 2 and exact placements
// is in spikes/s10/sim (TestCapFullOnlyComesBackWhenAReplacementJoins).

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// A shared system prompt makes one key, whose home is H. Three desired
// Instances and one spare; every request lasts 300 s (3,000 tokens at 0.1 s),
// so none ends before the test's last arrival. The control ticks fall at 5,
// 15, 25, 35, 45, ... s.
//
//	12:        an Instance X that is not H drains.
//	15:        the tick sees 2 of 3 ready: bound 1.25 -> 2.0, cap 48 -> none.
//	20.000+:   60 requests. Two members, c = 2.0, no cap: the bound
//	           2.0*(T+1)/2 = T+1 lets H take every one, so H holds 60, above
//	           48, and the other member holds 0.
//	32:        the spare joins, cold. Nothing changes until the tick.
//	35:        the tick sees 3 of 3 ready: bound 2.0 -> 1.25, cap none -> 48.
//	40.000+:   100 requests. The bound is 1.25*(T+1)/3. With T = 60 it is
//	           25.4, far below H's 60, so each request goes to the first of
//	           the other two Instances on the ring that fits, and H gets
//	           none: H is above the bound, and, being at 60, above the cap
//	           as well. The other two fill up and the cap stops each at 48.
//	           They can take 96 between them, so at least 100 - 96 = 4
//	           requests are held at the front, and none goes to H while H
//	           holds 48 or more. H's requests end from 320 s on (20 s plus
//	           300 s), the others' from 340 s, so the held requests wait for
//	           H to fall below 48 (13 ends, about 320.012 s) and are then
//	           placed on it. Whether some requests also spill (a choice of an
//	           Instance already at 48 passes on to the other) depends on the
//	           ring and is not worked out by hand: the test asks only for
//	           more than none, which shows the cap fired.
func TestOption5ReturnsToTheTightBoundAndTheCapAfterAReplacement(t *testing.T) {
	cl := sim.Cluster{Instances: 3, Spare: 1, KVTokens: 10_000_000, BlockSize: 16, MaxRunning: 64, PrefillRate: 1e9, TPOT: 0.1}
	const early, late, tick1, tick2 = 60, 100, 15.0, 35.0
	var convs []sim.Conversation
	add := func(n int, from float64) {
		for i := 0; i < n; i++ {
			id := len(convs)
			convs = append(convs, sim.Conversation{
				ID: id, Arrival: from + float64(i)*0.001,
				Sys:       []sim.Seg{{ID: sim.SegID(300), Len: 160}},
				FirstUser: []sim.Seg{{ID: sim.SegID(101, uint64(id)), Len: 16}},
				Turns:     []sim.Turn{{Out: 3000}},
				Summaries: []sim.Seg{{ID: sim.SegID(103, uint64(id)), Len: 100}},
			})
		}
	}
	add(early, 20)
	add(late, 40)
	run := func(events []sim.Event) *sim.Metrics {
		t.Helper()
		e := sim.NewEngine(cl, Option5.Policy(sim.KeySystem), sim.Workload{CompactAt: 1 << 30}, convs, events)
		e.M.Trace = true
		if err := e.Run(); err != nil {
			t.Fatal(err)
		}
		return e.M
	}

	probe := sim.NewEngine(cl, Option5.Policy(sim.KeySystem), sim.Workload{CompactAt: 1 << 30}, convs[:1], nil)
	probe.M.Trace = true
	if err := probe.Run(); err != nil {
		t.Fatal(err)
	}
	h := probe.M.Recs[0].Inst
	x := 0
	if h == 0 {
		x = 1
	}
	drain, join := sim.Event{Time: 12, Kind: sim.Drain, Instance: x}, sim.Event{Time: 32, Kind: sim.Join, Instance: cl.Instances}

	m := run([]sim.Event{drain, join})
	if len(m.Switches) != 2 {
		t.Fatalf("%d switches, want 2: %v", len(m.Switches), m.Switches)
	}
	if sw := m.Switches[0]; sw.T != tick1 || sw.From != 125 || sw.To != 200 || sw.CapFrom != b.EngineLimit || sw.CapTo != 0 {
		t.Errorf("first switch %+v, want 125 -> 200 and cap %d -> none at the tick after the drain, %.0f s", sw, b.EngineLimit, tick1)
	}
	if sw := m.Switches[1]; sw.T != tick2 || sw.From != 200 || sw.To != 125 || sw.CapFrom != 0 || sw.CapTo != b.EngineLimit {
		t.Errorf("second switch %+v, want 200 -> 125 and cap none -> %d at the first tick after the join at 32 s, %.0f s", sw, b.EngineLimit, tick2)
	}
	if got := m.PctSpan[200][1]; got >= tick2 {
		t.Errorf("a placement at c = 2.0 at %.3f s, after the switch back at %.0f s", got, tick2)
	}
	if got := m.PctSpan[125][1]; got < 40 {
		t.Errorf("the last placement at c = 1.25 is at %.3f s, want one at 40 s or later", got)
	}

	// The surviving Instance was above the cap when it came back.
	if got := m.PerInst[h].PeakInFlight; got != early {
		t.Errorf("H peaked at %d in flight, want %d: the loose bound with no cap puts the first %d requests on it", got, early, early)
	}
	if early <= b.EngineLimit {
		t.Fatalf("the setup holds %d on H, not above %d", early, b.EngineLimit)
	}
	// And the cap holds from the switch back: no other Instance goes above
	// 48 (a request that would take it there is spilled or held), and H, at
	// 60, takes nothing until it is below 48. A placement on an Instance
	// holding the cap or more is also an error of the simulation
	// (Metrics.placement), so the run itself failing would show it.
	for i, st := range m.PerInst {
		if i != h && i != x && st.PeakInFlight > b.EngineLimit {
			t.Errorf("Instance %d peaked at %d in flight, above the cap %d", i, st.PeakInFlight, b.EngineLimit)
		}
	}
	// Of the requests that arrive after the switch back, H gets only those
	// that were held at the front, and each only once H has fallen below 48:
	// H's requests end from 320 s (20 s + 300 s), and the first ends at
	// 320.000, 13 ends bring H to 47. So every held request waits until
	// about 320 s, at least 270 s after its arrival at about 40.1 s.
	onH := 0
	for _, r := range m.Recs {
		if r.Dispatch >= tick2 && r.Inst == h {
			onH++
		}
	}
	if onH != m.FrontHeld || len(m.FrontWaits) != m.FrontHeld {
		t.Errorf("%d requests that arrived after the switch back were placed on H, %d were held and %d placed from the front: want the held ones and only those",
			onH, m.FrontHeld, len(m.FrontWaits))
	}
	for i, w := range m.FrontWaits {
		if w < 270 {
			t.Errorf("held request %d waited %.1f s, want at least 270: H must fall below %d before it takes another", i, w, b.EngineLimit)
		}
	}
	peaks := make([]int, len(m.PerInst))
	for i, st := range m.PerInst {
		peaks[i] = st.PeakInFlight
	}
	t.Logf("H is instance %d, X is %d, the spare is %d; peaks in flight per Instance %v; from the switch back %d spills and %d held",
		h, x, cl.Instances, peaks, m.SpillsBetween(tick2, 1e9), m.HeldBetween(tick2, 1e9))
	if m.SpillsBetween(0, tick2) != 0 || m.SpillsBetween(tick2, 1e9) == 0 {
		t.Errorf("%d spills before the switch back and %d from it, want 0 and more than 0: the cap must fire once H is chosen again at or above it",
			m.SpillsBetween(0, tick2), m.SpillsBetween(tick2, 1e9))
	}
	// 100 requests, 2 Instances free of the cap's limit of 48 each: at least 4 are held.
	if want := late - 2*b.EngineLimit; m.HeldBetween(tick2, 1e9) < want || m.HeldBetween(0, tick2) != 0 {
		t.Errorf("%d held before the switch back and %d from it, want 0 and at least %d (%d requests, room for 2 x %d)",
			m.HeldBetween(0, tick2), m.HeldBetween(tick2, 1e9), want, late, b.EngineLimit)
	}
	if len(m.Recs) != early+late {
		t.Errorf("%d requests completed, want %d: nothing is cut when the cap returns", len(m.Recs), early+late)
	}

	// The control: with no replacement the bound stays 2.0 and the cap off,
	// so the late requests also go to H (one switch, no spill, no hold).
	c := run([]sim.Event{drain})
	if len(c.Switches) != 1 || c.Spills != 0 || c.FrontHeld != 0 || c.PerInst[h].PeakInFlight != early+late {
		t.Errorf("without the join: %d switches, %d spills, %d held, H peak %d; want 1, 0, 0 and %d",
			len(c.Switches), c.Spills, c.FrontHeld, c.PerInst[h].PeakInFlight, early+late)
	}
}

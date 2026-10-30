package study

// The three non-vacuous checks, the byte-for-byte regression against S10b's
// committed rows, and the currency of the committed results. Expected values
// come from issue #45's follow-up, S10b's committed files (written before
// this study's changes to the simulator) and hand-worked unit tests, never
// from the code under test. The simulation tests run S10's full-size regimes
// and take about two minutes on two cores; -short skips them.

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

const (
	s10bResults = "../../s10b/results"
	ourResults  = "../results"
)

var (
	sweepOnce sync.Once
	sweepRes  []Result
	sweepErr  error
)

// sweep runs option 5 in every regime on seeds 1-3, once.
func sweep(t *testing.T) []Result {
	t.Helper()
	if testing.Short() {
		t.Skip("full-size simulation")
	}
	sweepOnce.Do(func() {
		sweepRes, sweepErr = Execute(Plan(b.Regimes, b.RequiredSeeds), 2)
	})
	if sweepErr != nil {
		t.Fatal(sweepErr)
	}
	return sweepRes
}

func resultOf(t *testing.T, rs []Result, regime string, seed uint64) Result {
	t.Helper()
	for _, r := range rs {
		if r.Cell.Regime == regime && r.Cell.Seed == seed {
			return r
		}
	}
	t.Fatalf("no result for %s seed %d", regime, seed)
	return Result{}
}

// s10bCell is S10b's committed row for an option under the issue's key.
func s10bCell(t *testing.T, cells []b.Cell, regime, option string, seed uint64) b.Cell {
	t.Helper()
	for _, c := range cells {
		if c.Regime == regime && c.Option == option && c.Seed == seed && (c.Key == string(Key) || c.Key == "-") {
			return c
		}
	}
	t.Fatalf("S10b committed no row for %s / %s seed %d", regime, option, seed)
	return b.Cell{}
}

func committed(t *testing.T) []b.Cell {
	t.Helper()
	cells, err := ReadCells(s10bResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	return cells
}

// Check 1 of the follow-up: "at full membership in the tight regime, option
// 5's cap binds: spills are above 0". THE CHECK FAILS AS WORDED. In the tight
// regime plain c = 1.25 never reaches the cap: S10b committed peaks of 43, 46
// and 46 in flight for it, against a cap of 48. A request is spilled only when
// its choice already holds 48, which needs a peak of 49, so option 5 makes
// the same choices as option 1 and has no spill on any seed. This test
// asserts what is true: zero spills, the same run as option 1 on every
// measure S10b committed, and the reason (option 1's peak is at most 48).
// Where the cap does fire is TestCapBindsWhereC125ReachesTheLimit.
func TestCapDoesNotBindInTheTightRegime(t *testing.T) {
	rs, cells := sweep(t), committed(t)
	for _, seed := range b.RequiredSeeds {
		x, o1 := resultOf(t, rs, b.Tight.Name, seed), s10bCell(t, cells, b.Tight.Name, "c=1.25", seed)
		t.Logf("seed %d: option 5 spilled %d requests (the follow-up wanted more than 0); plain c=1.25 peaks at %d in flight against a cap of %d",
			seed, x.Cell.Spills, o1.PeakRun, b.EngineLimit)
		if o1.PeakRun > b.EngineLimit {
			t.Errorf("seed %d: S10b's c=1.25 peaked at %d, above %d: the cap could bind and the premise of this test is gone", seed, o1.PeakRun, b.EngineLimit)
		}
		if x.Cell.Spills != 0 || x.Cell.FrontHeld != 0 {
			t.Errorf("seed %d: %d spills and %d held requests, want 0 and 0 (plain c=1.25 never reaches the cap)", seed, x.Cell.Spills, x.Cell.FrontHeld)
		}
		if x.Cell.Requests != o1.Requests || x.Cell.HitTokens != o1.HitTokens || x.Cell.DoneOutput != o1.DoneOutput || x.Cell.PeakRun != o1.PeakRun {
			t.Errorf("seed %d: option 5 differs from S10b's c=1.25 (requests %d/%d, hit tokens %d/%d)", seed, x.Cell.Requests, o1.Requests, x.Cell.HitTokens, o1.HitTokens)
		}
	}
}

// Where the cap never fires, option 5 is the run of the option it is built
// on: with no spill the choices are those of plain c = 1.25 until the switch,
// and after the switch it is c = 2.0 without a cap, which is option 4's. So
// the tight regime equals S10b's c = 1.25 and adaptive rows, and the drain
// regime equals S10b's adaptive row, on every column of cells.csv (the option
// name aside), on all three seeds. This is what lets the results say that the
// cap costs nothing there, and that after the drain option 5 is option 4.
func TestOption5EqualsTheOptionItIsBuiltOnWhereTheCapIsSilent(t *testing.T) {
	rs, cells := sweep(t), committed(t)
	line := func(c b.Cell) string { c.Option = "x"; return b.CellsCSV([]b.Cell{c}) }
	for _, seed := range b.RequiredSeeds {
		for _, tc := range []struct {
			regime string
			refs   []string
		}{{b.Tight.Name, []string{"c=1.25", "adaptive 1.25/2.0"}}, {b.Drain.Name, []string{"adaptive 1.25/2.0"}}} {
			x := resultOf(t, rs, tc.regime, seed)
			for _, ref := range tc.refs {
				if got, want := line(x.Cell), line(s10bCell(t, cells, tc.regime, ref, seed)); got != want {
					t.Errorf("%s seed %d: option 5 differs from S10b's %s:\n  option 5: %s\n  S10b:     %s", tc.regime, seed, ref, got, want)
				}
			}
		}
	}
}

// Where the cap does bind. A request spills when its choice holds 48 before
// it, which for c = 1.25 happens exactly when plain c = 1.25 would place a
// request as the 49th. So in every full-membership run, option 5 spills at
// least once if and only if S10b's committed c = 1.25 run peaked above 48.
// The oracle is S10b's peaks, not option 5's own output. The scarce and half
// prefill regimes each have seeds on both sides (49 or 50 against 48).
func TestCapBindsWhereC125ReachesTheLimit(t *testing.T) {
	rs, cells := sweep(t), committed(t)
	bound := 0
	for _, rg := range b.FullMembership {
		for _, seed := range b.RequiredSeeds {
			x, o1 := resultOf(t, rs, rg.Name, seed), s10bCell(t, cells, rg.Name, "c=1.25", seed)
			wantSpill := o1.PeakRun > b.EngineLimit
			if wantSpill {
				bound++
			}
			t.Logf("%s seed %d: c=1.25 peaks at %d; option 5 spills %d, peaks at %d, %.0f s above %d", rg.Name, seed, o1.PeakRun, x.Cell.Spills, x.Cell.PeakRun, x.Cell.AboveRunS, b.EngineLimit)
			if (x.Cell.Spills > 0) != wantSpill {
				t.Errorf("%s seed %d: %d spills, but plain c=1.25 peaked at %d (spills expected: %v)", rg.Name, seed, x.Cell.Spills, o1.PeakRun, wantSpill)
			}
			if x.Cell.PeakRun > b.EngineLimit || x.Cell.AboveRunS != 0 {
				t.Errorf("%s seed %d: peak %d and %.1f s above %d; option 5 must stay within the limit at full membership", rg.Name, seed, x.Cell.PeakRun, x.Cell.AboveRunS, b.EngineLimit)
			}
		}
	}
	if bound == 0 {
		t.Error("no full-membership run has plain c=1.25 above the limit, so this test checks nothing")
	}
}

// Check 2: after the drain option 5 switches to the uncapped c = 2.0, within
// one control step. The drain is at 3,000 s and the step is 10 s, so the
// switch must fall in (3000, 3010]. The bound goes 1.25 -> 2.0 and the cap
// 48 -> none as one switch; every placement before it used 1.25, every one
// after it used 2.0. It is off for real: after the drain the largest number
// in flight on an Instance is above 48, which the cap would have prevented.
// With no drain nothing switches. The control step is S10b's.
func TestSwitchesToUncappedC2AtTheDrain(t *testing.T) {
	rs := sweep(t)
	for _, seed := range b.RequiredSeeds {
		d := resultOf(t, rs, b.Drain.Name, seed)
		x, c := d.Extra, d.Cell
		t.Logf("seed %d: drain at %.0f s; switch at %.1f s, bound %d -> %d, cap %d -> %d; bounds used %s; peak in flight after the drain %d",
			seed, b.DrainT, x.SwitchT, x.BoundFrom, x.BoundTo, x.CapFrom, x.CapTo, c.BoundsUsed, c.PostPeak)
		if x.Switches != 1 {
			t.Fatalf("seed %d: %d switches, want exactly 1", seed, x.Switches)
		}
		if !(x.SwitchT > b.DrainT && x.SwitchT <= b.DrainT+sim.DefaultControlStep) {
			t.Errorf("seed %d: switched at %.1f s, want within one control step of the drain: (%.0f, %.0f]", seed, x.SwitchT, b.DrainT, b.DrainT+sim.DefaultControlStep)
		}
		if x.BoundFrom != 125 || x.BoundTo != 200 || x.CapFrom != b.EngineLimit || x.CapTo != 0 {
			t.Errorf("seed %d: switch is bound %d -> %d and cap %d -> %d, want 125 -> 200 and %d -> 0", seed, x.BoundFrom, x.BoundTo, x.CapFrom, x.CapTo, b.EngineLimit)
		}
		if c.BoundsUsed != "125;200" {
			t.Errorf("seed %d: bounds used %q, want 125 then 200", seed, c.BoundsUsed)
		}
		if last125, first200 := c.PctSpan[125][1], c.PctSpan[200][0]; last125 >= x.SwitchT || first200 < x.SwitchT {
			t.Errorf("seed %d: a placement at 1.25 at %.2f s or at 2.0 at %.2f s straddles the switch at %.2f s", seed, last125, first200, x.SwitchT)
		}
		if c.PostPeak <= b.EngineLimit {
			t.Errorf("seed %d: peak in flight after the drain is %d, not above %d: the cap is still on, or the load never needed it off", seed, c.PostPeak, b.EngineLimit)
		}
	}
	for _, rg := range b.FullMembership {
		for _, seed := range b.RequiredSeeds {
			if x := resultOf(t, rs, rg.Name, seed).Extra; x.Switches != 0 {
				t.Errorf("%s seed %d: %d switches with every Instance ready, want 0", rg.Name, seed, x.Switches)
			}
		}
	}
}

// Check 3: the cap is not active below full membership, so no spill is
// counted after the switch (and no request is held at the front). Zero would
// also be the answer if the workload never filled an Instance after the
// drain, or if the run had no spills at all (it has none before the switch
// either, because the cap is inert in the tight regime). The control is the
// same run with the cap kept: from the same switch time it must spill and
// hold requests, and stay within 48 in flight. S10b committed option 3's
// 11,041 held requests after the drain, so the load is there. The
// hand-worked version of this check is in spikes/s10/sim.
func TestNoSpillAfterTheSwitch(t *testing.T) {
	rs := sweep(t)
	for _, seed := range b.RequiredSeeds {
		x := resultOf(t, rs, b.Drain.Name, seed).Extra
		if x.SpillsAfter != 0 || x.HeldAfter != 0 {
			t.Errorf("seed %d: %d spills and %d held requests from the switch at %.1f s on, want 0 and 0", seed, x.SpillsAfter, x.HeldAfter, x.SwitchT)
		}
	}
	var ctlCells []b.Cell
	for _, seed := range b.RequiredSeeds {
		run := b.Run{Regime: b.Drain, Opt: ControlCapKept, Key: Key, Seed: seed}
		m, err := run.Metrics()
		if err != nil {
			t.Fatal(err)
		}
		cx, cc := NewExtra(m), b.NewCell(run, m)
		ctlCells = append(ctlCells, cc)
		sw := resultOf(t, rs, b.Drain.Name, seed).Extra.SwitchT
		t.Logf("control (cap kept), seed %d: switch at %.1f s; %d spills and %d held before it, %d spills and %d held from it; peak after the drain %d",
			seed, cx.SwitchT, cx.SpillsBefore, cx.HeldBefore, cx.SpillsAfter, cx.HeldAfter, cc.PostPeak)
		if cx.SwitchT != sw {
			t.Errorf("seed %d: the control switches at %.1f s, option 5 at %.1f s: not comparable", seed, cx.SwitchT, sw)
		}
		if cx.SpillsAfter == 0 || cx.HeldAfter == 0 {
			t.Errorf("seed %d: with the cap kept, %d spills and %d held requests after the switch: the workload does not reach the cap after the drain, so zero for option 5 would mean nothing", seed, cx.SpillsAfter, cx.HeldAfter)
		}
		if cc.PostPeak > b.EngineLimit {
			t.Errorf("seed %d: with the cap kept the peak after the drain is %d, above %d", seed, cc.PostPeak, b.EngineLimit)
		}
	}
	want, err := os.ReadFile(ourResults + "/control-cap-kept.csv")
	if err != nil {
		t.Fatal(err)
	}
	if got := b.CellsCSV(ctlCells); got != string(want) {
		t.Error("results/control-cap-kept.csv differs from a fresh run; rerun s10csim")
	}
}

// Options 1-4 must behave as they did when S10b committed its rows: option
// 5's code path (a router-held cap, a cap-aware control step, timestamps on
// spills) changes none of them. Options 3 and 4 are run again on seed 1 in
// all four regimes and each cell is compared byte for byte with the line in
// S10b's cells.csv. (Round robin, c = 1.25 and c = 2.0 are covered against
// S10's own rows by S10b's TestMatchesS10CommittedResults.)
func TestRerunOfOptions3And4IsByteIdenticalToS10b(t *testing.T) {
	if testing.Short() {
		t.Skip("full-size simulation")
	}
	text, err := os.ReadFile(s10bResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	var opts []b.Option
	for _, o := range b.Options {
		if o.ID == 3 || o.ID == 4 {
			opts = append(opts, o)
		}
	}
	rs, err := Execute(PlanOptions(opts, b.Regimes, 1), 2)
	if err != nil {
		t.Fatal(err)
	}
	n, problems := CompareWithS10b(Cells(rs), string(text))
	if n != 8 {
		t.Errorf("compared %d cells, want 8 (options 3 and 4 in four regimes)", n)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// The comparison can fail: a re-run cell that differs in one column, a cell
// S10b has no row for, and an added column are all reported.
func TestComparisonWithS10bCatchesDifferences(t *testing.T) {
	text, err := os.ReadFile(s10bResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	cells := committed(t)
	var o3 b.Cell
	for _, c := range cells {
		if c.Option == "c=2.0+cap48" && c.Regime == b.Drain.Name && c.Seed == 1 && c.Key == string(Key) {
			o3 = c
		}
	}
	if n, p := CompareWithS10b([]b.Cell{o3}, string(text)); n != 1 || len(p) != 0 {
		t.Fatalf("the committed row compared against itself: %d compared, problems %v", n, p)
	}
	o3.Spills++
	if n, p := CompareWithS10b([]b.Cell{o3}, string(text)); n != 1 || len(p) != 1 {
		t.Errorf("a changed spill count: %d compared, %d problems, want 1 and 1", n, len(p))
	}
	o3.Seed = 99
	if n, p := CompareWithS10b([]b.Cell{o3}, string(text)); n != 0 || len(p) != 1 || !strings.Contains(p[0], "no row") {
		t.Errorf("a seed S10b has no row for: %d compared, problems %v, want 0 and a missing-row report", n, p)
	}
	changed := strings.Replace(string(text), "regime,key,option,seed,", "regime,key,option,seed,extra,", 1)
	if _, p := CompareWithS10b(nil, changed); len(p) != 1 {
		t.Errorf("a changed header: problems %v, want 1", p)
	}
}

// The committed results are what the code produces: the file written by
// s10csim equals a fresh render of this sweep, so the README's numbers can
// be traced to the code in the same commit.
func TestCommittedResultsAreCurrent(t *testing.T) {
	rs := sweep(t)
	want, err := os.ReadFile(ourResults + "/cells.csv")
	if err != nil {
		t.Fatal(err)
	}
	if got := b.CellsCSV(Cells(rs)); got != string(want) {
		t.Error("results/cells.csv differs from a fresh run; rerun s10csim")
	}
	extra, err := os.ReadFile(ourResults + "/extras.csv")
	if err != nil {
		t.Fatal(err)
	}
	if got := ExtraCSV(rs); got != string(extra) {
		t.Error("results/extras.csv differs from a fresh run; rerun s10csim")
	}
}

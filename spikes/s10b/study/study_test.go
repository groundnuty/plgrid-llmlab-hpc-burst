package study

// The issue's two non-vacuous checks, the regression check against S10, and
// the pass rule. Expected values come from issue #45, S10's committed
// results and the rule's wording, never from this code's own output. The
// simulation tests run S10's full-size regimes (160 users, 1.5 M tokens per
// Instance), about two minutes on two cores; -short skips them.

import (
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

const key = sim.KeyHeaderOpening

var (
	seed1Once  sync.Once
	seed1Cells []Cell
	seed1Err   error
)

// seed1 runs every option in every regime once, on seed 1.
func seed1(t *testing.T) []Cell {
	t.Helper()
	if testing.Short() {
		t.Skip("full-size simulation")
	}
	seed1Once.Do(func() {
		seed1Cells, seed1Err = Execute(Plan(Regimes, []sim.KeyKind{key}, []uint64{1}), 2)
	})
	if seed1Err != nil {
		t.Fatal(seed1Err)
	}
	return seed1Cells
}

func cellOf(t *testing.T, cells []Cell, regime, option string) Cell {
	t.Helper()
	for _, c := range cells {
		if c.Regime == regime && c.Option == option {
			return c
		}
	}
	t.Fatalf("no cell for %s / %s", regime, option)
	return Cell{}
}

// Issue #45: "The tight regime at full membership must show the cap binding
// for option 3." The cap binds when a request's choice is full and it
// spills, so the spill count must be above 0. A cap that cannot bind would
// also show no spills, so the control is plain c = 2.0 on the same seed: it
// must exceed the engine limit, otherwise the workload never reaches the cap
// and the zero would be the trivial answer. S10 measured 70-73 in flight on
// plain c = 2.0 here. Seeds 1-3.
func TestCapBindsInTheTightRegime(t *testing.T) {
	cells := seed1(t)
	more, err := Execute(Plan([]Regime{Tight}, []sim.KeyKind{key}, []uint64{2, 3}), 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, seed := range []uint64{1, 2, 3} {
		var plain, capped Cell
		for _, c := range append(append([]Cell(nil), cells...), more...) {
			if c.Regime == Tight.Name && c.Seed == seed {
				switch c.Option {
				case "c=2.0":
					plain = c
				case "c=2.0+cap48":
					capped = c
				}
			}
		}
		t.Logf("seed %d: plain c=2.0 peaks at %d in flight (%.0f s above %d); with the cap: %d spills (%d in the window), peak %d, %.0f s above, %d held at the front",
			seed, plain.PeakRun, plain.AboveRunS, EngineLimit, capped.Spills, capped.SpillsWin, capped.PeakRun, capped.AboveRunS, capped.FrontHeld)
		if plain.PeakRun <= EngineLimit {
			t.Errorf("seed %d: plain c=2.0 peaks at %d, not above %d: the workload never reaches the cap, so a cap that never binds would pass", seed, plain.PeakRun, EngineLimit)
		}
		if capped.Spills == 0 || capped.SpillsWin == 0 {
			t.Errorf("seed %d: option 3 spilled %d requests (%d in the window); the cap did not bind", seed, capped.Spills, capped.SpillsWin)
		}
		if capped.PeakRun > EngineLimit || capped.AboveRunS != 0 {
			t.Errorf("seed %d: option 3 peaked at %d in flight and spent %.1f s above %d", seed, capped.PeakRun, capped.AboveRunS, EngineLimit)
		}
	}
}

// Issue #45: the adaptive option "must switch at the drain, and the switch
// time must be reported." The drain is at 3,000 s and the control step is
// 10 s, so the switch must fall in (3000, 3010]. The bound must be 1.25 for
// every placement before it and 2.0 for every placement after it; with no
// drain the bound must never change and the run must equal option 1's.
func TestAdaptiveSwitchesAtTheDrain(t *testing.T) {
	cells := seed1(t)
	d := cellOf(t, cells, Drain.Name, "adaptive 1.25/2.0")
	t.Logf("drain at %.0f s; the adaptive bound switched at %.1f s (control step %.0f s); bounds used %s", DrainT, d.SwitchT, sim.DefaultControlStep, d.BoundsUsed)
	if d.Switches != 1 {
		t.Fatalf("%d switches, want exactly 1", d.Switches)
	}
	if !(d.SwitchT > DrainT && d.SwitchT <= DrainT+sim.DefaultControlStep) {
		t.Errorf("switched at %.1f s, want within one control step of the drain: (%.0f, %.0f]", d.SwitchT, DrainT, DrainT+sim.DefaultControlStep)
	}
	if d.BoundsUsed != "125;200" {
		t.Errorf("bounds used %q, want 125 then 200", d.BoundsUsed)
	}
	if last125, first200 := d.PctSpan[125][1], d.PctSpan[200][0]; last125 >= d.SwitchT || first200 < d.SwitchT {
		t.Errorf("a placement at 1.25 at %.2f s or at 2.0 at %.2f s straddles the switch at %.2f s", last125, first200, d.SwitchT)
	}
	for _, rg := range FullMembership {
		a, o := cellOf(t, cells, rg.Name, "adaptive 1.25/2.0"), cellOf(t, cells, rg.Name, "c=1.25")
		if a.Switches != 0 || a.BoundsUsed != "125" {
			t.Errorf("%s: %d switches, bounds %q; with every Instance ready the bound must stay 1.25", rg.Name, a.Switches, a.BoundsUsed)
		}
		if a.Requests != o.Requests || a.HitTokens != o.HitTokens || a.DoneOutput != o.DoneOutput || a.PeakRun != o.PeakRun {
			t.Errorf("%s: adaptive with no switch differs from c=1.25 (requests %d/%d, hit tokens %d/%d)", rg.Name, a.Requests, o.Requests, a.HitTokens, o.HitTokens)
		}
	}
}

// The 300 s windows partition the measurement window, so their tokens must
// sum to each run's totals. The drain table's hit rates are read from these
// windows, so a mismatch would mean they measure something else than the
// run's hit rate.
func TestWindowSeriesSumsToTheRun(t *testing.T) {
	for _, c := range seed1(t) {
		var h, p int64
		for _, w := range c.Series {
			h += w.Hit
			p += w.Prompt
		}
		if len(c.Series) != 12 || h != c.HitTokens || p != c.PromptTokens {
			t.Errorf("%s / %s: %d windows summing to %d hit and %d prompt tokens, the run has %d and %d", c.Regime, c.Option, len(c.Series), h, p, c.HitTokens, c.PromptTokens)
		}
	}
}

// Options 1 and 2 and round robin use none of what S10b added, so their
// cells must equal the rows S10 committed before the additions.
func TestMatchesS10CommittedResults(t *testing.T) {
	cells := seed1(t)
	n, problems, err := CheckAgainstS10(cells, "../../s10/results")
	if err != nil {
		t.Fatal(err)
	}
	if want := len(Regimes) * 3; n != want {
		t.Errorf("compared %d cells, want %d (round robin, c=1.25, c=2.0 in 4 regimes)", n, want)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// The regression check can fail: a cell that differs from S10's row is
// reported on every column.
func TestRegressionCheckCatchesADifference(t *testing.T) {
	bad := Cell{Regime: "tight", Key: string(key), Option: "c=1.25", Seed: 1, Requests: 1, PromptTokens: 1, HitTokens: 1, DoneOutput: 1}
	n, problems, err := CheckAgainstS10([]Cell{bad}, "../../s10/results")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(problems) != 4 {
		t.Errorf("compared %d, %d problems; want 1 and one per column (4): %v", n, len(problems), problems)
	}
}

// The pass rule on made-up cells, worked out by hand. Three options are
// built so the rule has something to decide:
//
//	c=1.25: peaks at 40 in every full-membership regime, hit 0.50 after the
//	        drain and 0.60 when scarce.
//	c=2.0: peaks at 71 (above 48), the best hit rates (0.80, 0.70). It must be
//	       excluded however good it looks.
//	c=2.0+cap48: peaks at 48 (the limit itself is not above it), hit 0.75
//	             after the drain and 0.65 when scarce.
//
// Qualifying are c=1.25 and the capped one; the capped one is best on both,
// so it passes. Then c=1.25's drain score is raised to 0.9 and its scarce hit
// lowered below the capped one's: the rankings split and nobody is named.
// Last, with every option above the limit, nobody passes.
func TestPassRule(t *testing.T) {
	mk := func(opt string, peak int, above float64, drain, scarce float64) []Cell {
		var cs []Cell
		for _, rg := range Regimes {
			c := Cell{Regime: rg.Name, Key: string(key), Option: opt, Seed: 1, DoneOutput: 1, PostDone: 1}
			c.PeakRun, c.AboveRunS = peak, above
			if rg.Name == Drain.Name {
				c.Hit05, c.Hit515 = drain, drain
			}
			if rg.Name == Scarce.Name {
				c.HitRate = scarce
			}
			cs = append(cs, c)
		}
		return cs
	}
	rr := func() []Cell {
		var cs []Cell
		for _, rg := range Regimes {
			cs = append(cs, Cell{Regime: rg.Name, Key: "-", Option: RoundRobin.Name, Seed: 1, DoneOutput: 1, PostDone: 1, HitRate: 0.3})
		}
		return cs
	}
	build := func(a, b, c []Cell) []Cell { return append(append(append(rr(), a...), b...), c...) }
	cond := func(v OptionVerdict) bool { return v.Cond1All }

	cells := build(mk("c=1.25", 40, 0, 0.50, 0.60), mk("c=2.0", 71, 100, 0.80, 0.70), mk("c=2.0+cap48", 48, 0, 0.75, 0.65))
	q, bd, bs, w := Pass(Evaluate(cells, string(key)), cond)
	if strings.Join(q, ",") != "c=1.25,c=2.0+cap48" || bd != "c=2.0+cap48" || bs != "c=2.0+cap48" || w != "c=2.0+cap48" {
		t.Errorf("qualifying %v, best drain %q, best scarce %q, winner %q", q, bd, bs, w)
	}

	cells = build(mk("c=1.25", 40, 0, 0.90, 0.60), mk("c=2.0", 71, 100, 0.80, 0.70), mk("c=2.0+cap48", 48, 0, 0.75, 0.65))
	_, bd, bs, w = Pass(Evaluate(cells, string(key)), cond)
	if bd != "c=1.25" || bs != "c=2.0+cap48" || w != "" {
		t.Errorf("split case: best drain %q, best scarce %q, winner %q; want c=1.25, c=2.0+cap48 and no winner", bd, bs, w)
	}

	cells = build(mk("c=1.25", 49, 0, 0.5, 0.5), mk("c=2.0", 71, 100, 0.8, 0.7), mk("c=2.0+cap48", 48, 3, 0.7, 0.6))
	q, _, _, w = Pass(Evaluate(cells, string(key)), cond)
	if len(q) != 0 || w != "" {
		t.Errorf("one past the limit, and time above it: qualifying %v, winner %q; want none", q, w)
	}
}

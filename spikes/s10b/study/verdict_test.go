package study

// A verdict is issued only for a complete sweep. The cells are made up; what
// the rule must say is worked out from the rule's wording.

import (
	"strings"
	"testing"
)

// completeCells has round robin and all four options in all four regimes on
// every required seed. Only option 3 stays at the engine limit (48), so on
// the rule it is the one qualifier and passes by default.
func completeCells() []Cell {
	var cells []Cell
	for _, rg := range Regimes {
		for _, s := range RequiredSeeds {
			cells = append(cells, Cell{Regime: rg.Name, Key: "-", Option: RoundRobin.Name, Seed: s, DoneOutput: 1, PostDone: 1, HitRate: 0.3})
			for _, o := range Options {
				c := Cell{Regime: rg.Name, Key: string(key), Option: o.Name, Seed: s, DoneOutput: 1, PostDone: 1, PeakRun: 49, AboveRunS: 5, HitRate: 0.4}
				if o.ID == 3 {
					c.PeakRun, c.AboveRunS = EngineLimit, 0
				}
				cells = append(cells, c)
			}
		}
	}
	return cells
}

func without(cells []Cell, drop func(Cell) bool) []Cell {
	var out []Cell
	for _, c := range cells {
		if !drop(c) {
			out = append(out, c)
		}
	}
	return out
}

func TestACompleteSweepGetsAVerdict(t *testing.T) {
	cells := completeCells()
	if m := Missing(cells, string(key)); len(m) != 0 {
		t.Fatalf("a complete sweep is missing %v", m)
	}
	md := VerdictMarkdown(cells, string(key))
	if strings.Contains(md, "INCOMPLETE") || !strings.Contains(md, "Qualifying on condition 1 in all three full-membership regimes: [c=2.0+cap48]. It is the only qualifier, so it passes by default") {
		t.Errorf("complete sweep:\n%s", md)
	}
}

// The reviewer's case: -regimes tight,scarce,half-prefill leaves no drain
// results, every drain score is 0, and option 3 used to pass by default.
func TestASweepWithoutTheDrainRegimeGetsNoVerdict(t *testing.T) {
	cells := without(completeCells(), func(c Cell) bool { return c.Regime == Drain.Name })
	if m := Missing(cells, string(key)); len(m) != 1 || m[0] != "regime drain: no runs" {
		t.Errorf("missing %v, want just the drain regime", m)
	}
	md := VerdictMarkdown(cells, string(key))
	for _, bad := range []string{"passes", "Qualifying", "| option |"} {
		if strings.Contains(md, bad) {
			t.Errorf("an incomplete sweep's verdict contains %q:\n%s", bad, md)
		}
	}
	if !strings.Contains(md, "INCOMPLETE") || !strings.Contains(md, "regime drain: no runs") {
		t.Errorf("incomplete sweep:\n%s", md)
	}
}

// One group short of one seed is enough to withhold the verdict.
func TestASweepMissingOneSeedGetsNoVerdict(t *testing.T) {
	cells := without(completeCells(), func(c Cell) bool {
		return c.Regime == Scarce.Name && c.Option == "c=2.0+cap48" && c.Seed == 2
	})
	m := Missing(cells, string(key))
	if len(m) != 1 || m[0] != "scarce / c=2.0+cap48: seeds 2" {
		t.Errorf("missing %v, want just seed 2 of the capped option in the scarce regime", m)
	}
	md := VerdictMarkdown(cells, string(key))
	if !strings.Contains(md, "INCOMPLETE") || strings.Contains(md, "passes") {
		t.Errorf("sweep short of a seed:\n%s", md)
	}
	// A run on other seeds only is incomplete too: the rule is fixed on 1-3.
	other := without(completeCells(), func(c Cell) bool { return c.Seed != 1 })
	if m := Missing(other, string(key)); len(m) != 4*5 {
		t.Errorf("seed 1 alone: %d groups missing seeds, want 20 (4 regimes x 5 options)", len(m))
	}
}

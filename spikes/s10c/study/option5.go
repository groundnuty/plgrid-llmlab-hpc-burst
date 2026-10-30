// Package study runs spike S10c: option 5 of issue #45's follow-up, the
// adaptive bound with a hard cap that is active only at full membership,
// on S10b's harness. Options 1-4 and round robin are not run again; their
// rows are read from S10b's committed results.
package study

import (
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// Option5 is the adaptive bound plus a cap that follows it: while every
// desired Instance is ready, c = 1.25 and a hard cap of 48 in flight per
// Instance, spilling to the next Instance on the ring (queueing at the front
// when all are full); while fewer are ready, c = 2.0 and no cap. The
// deployment checks readiness every 10 s, as for option 4.
var Option5 = b.Option{ID: 5, Name: "adaptive+cap48 (cap at full only)", Policy: func(k sim.KeyKind) sim.Policy {
	return sim.Policy{
		Kind: sim.CHWBL, Key: k, Percent: 125, ReleaseOnDrain: true,
		Cap: b.EngineLimit, AdaptiveLoose: 200, ControlStep: sim.DefaultControlStep, CapFullOnly: true,
	}
}}

// ControlCapKept is option 5 with the cap kept after the drain: the adaptive
// bound with option 3's always-on cap. It is not one of the options. It is
// the control for "no spill after the switch" (it must spill, so zero for
// option 5 means something) and the isolating run for the drain: it differs
// from option 5 in the cap alone.
var ControlCapKept = b.Option{ID: 6, Name: "control: adaptive bound and cap kept", Policy: func(k sim.KeyKind) sim.Policy {
	p := Option5.Policy(k)
	p.CapFullOnly = false
	return p
}}

// Key is the key of the issue: the session header, falling back to the
// hash of the opening.
const Key = sim.KeyHeaderOpening

// Extra holds what only option 5 is asked about, from the run's raw
// metrics. It is not part of S10b's cell, so S10b's columns stay as they
// were.
type Extra struct {
	Switches           int
	SwitchT            float64 // time of the first switch, -1 for none
	BoundFrom, BoundTo int     // MeanLoadPercentage across the first switch
	CapFrom, CapTo     int     // the hard cap across the first switch (0 for none)
	SpillsBefore       int     // spills before the first switch (all, when none)
	SpillsAfter        int     // spills from the first switch on
	HeldBefore         int     // requests held at the front before it
	HeldAfter          int     // and from it on
}

// NewExtra reads the extras from a run's metrics.
func NewExtra(m *sim.Metrics) Extra {
	x := Extra{Switches: len(m.Switches), SwitchT: -1}
	end := 1e18
	cut := end
	if len(m.Switches) > 0 {
		sw := m.Switches[0]
		x.SwitchT, x.BoundFrom, x.BoundTo, x.CapFrom, x.CapTo = sw.T, sw.From, sw.To, sw.CapFrom, sw.CapTo
		cut = sw.T
	}
	x.SpillsBefore, x.SpillsAfter = m.SpillsBetween(0, cut), m.SpillsBetween(cut, end)
	x.HeldBefore, x.HeldAfter = m.HeldBetween(0, cut), m.HeldBetween(cut, end)
	return x
}

// Result is one run's cell and extras.
type Result struct {
	Run   b.Run
	Cell  b.Cell
	Extra Extra
}

// Plan lists option 5's runs: every regime and seed under the issue's key.
func Plan(regimes []b.Regime, seeds []uint64) []b.Run {
	var runs []b.Run
	for _, rg := range regimes {
		for _, s := range seeds {
			runs = append(runs, b.Run{Regime: rg, Opt: Option5, Key: Key, Seed: s})
		}
	}
	return runs
}

// PlanOptions lists the given options' runs on one seed in every regime:
// the re-run that the regression check compares with S10b's rows.
func PlanOptions(opts []b.Option, regimes []b.Regime, seed uint64) []b.Run {
	var runs []b.Run
	for _, rg := range regimes {
		for _, o := range opts {
			runs = append(runs, b.Run{Regime: rg, Opt: o, Key: Key, Seed: seed})
		}
	}
	return runs
}

// Execute runs the plan, par at a time, and returns the results in plan
// order.
func Execute(runs []b.Run, par int) ([]Result, error) {
	out := make([]Result, len(runs))
	errs := make([]error, len(runs))
	sem := make(chan struct{}, max(1, par))
	var wg sync.WaitGroup
	for i, r := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			m, err := r.Metrics()
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = Result{Run: r, Cell: b.NewCell(r, m), Extra: NewExtra(m)}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("s10c: %w", err)
		}
	}
	return out, nil
}

// Cells returns the results' cells.
func Cells(rs []Result) []b.Cell {
	cells := make([]b.Cell, len(rs))
	for i, r := range rs {
		cells[i] = r.Cell
	}
	return cells
}

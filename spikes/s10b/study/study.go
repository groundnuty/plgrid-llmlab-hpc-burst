package study

import (
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

// S10's measurement setup, unchanged.
const (
	WinStart    = 1200.0 // warm-up ends
	WinEnd      = 4800.0 // arrivals stop
	DrainT      = 3000.0 // S10's membership event
	EngineLimit = 48     // S10's MaxRunning: the engine's maximum concurrent sequences
)

// RequiredSeeds are the workload seeds the pass rule is fixed on (issue #45:
// "in every seed"). A verdict needs every run of the study on each of them.
var RequiredSeeds = []uint64{1, 2, 3}

// Regime is one of the issue's four conditions.
type Regime struct {
	Name    string
	KV      int     // KV cache tokens per Instance
	Prefill float64 // uncached prompt tokens per second per Instance
	Events  []sim.Event
}

// The four regimes, with S10's parameters. "Tight" is full membership with
// a cache that holds about the working set; "drain" is the same cluster
// losing instance-1 at 3,000 s with no replacement.
var (
	Tight       = Regime{"tight", 1_500_000, 25000, nil}
	Drain       = Regime{"drain", 1_500_000, 25000, []sim.Event{{Time: DrainT, Kind: sim.Drain, Instance: 1}}}
	Scarce      = Regime{"scarce", 750_000, 25000, nil}
	HalfPrefill = Regime{"half-prefill", 1_500_000, 12500, nil}
	Regimes     = []Regime{Tight, Drain, Scarce, HalfPrefill}
	// FullMembership are the regimes in which no Instance ever leaves.
	FullMembership = []Regime{Tight, Scarce, HalfPrefill}
)

// Option is one load-bound policy under test.
type Option struct {
	ID     int
	Name   string
	Policy func(key sim.KeyKind) sim.Policy
}

func chwbl(key sim.KeyKind, pct int) sim.Policy {
	// ReleaseOnDrain is HBEP-5's requirement since S10; it matters only in
	// the drain regime.
	return sim.Policy{Kind: sim.CHWBL, Key: key, Percent: pct, ReleaseOnDrain: true}
}

// RoundRobin is the throughput baseline in every regime.
var RoundRobin = Option{0, "round-robin", func(sim.KeyKind) sim.Policy { return sim.Policy{Kind: sim.RoundRobin} }}

// Options are the four options of issue #45.
var Options = []Option{
	{1, "c=1.25", func(k sim.KeyKind) sim.Policy { return chwbl(k, 125) }},
	{2, "c=2.0", func(k sim.KeyKind) sim.Policy { return chwbl(k, 200) }},
	{3, "c=2.0+cap48", func(k sim.KeyKind) sim.Policy {
		p := chwbl(k, 200)
		p.Cap = EngineLimit
		return p
	}},
	{4, "adaptive 1.25/2.0", func(k sim.KeyKind) sim.Policy {
		p := chwbl(k, 125)
		p.AdaptiveLoose, p.ControlStep = 200, sim.DefaultControlStep
		return p
	}},
}

// Run is one simulation.
type Run struct {
	Regime Regime
	Opt    Option
	Key    sim.KeyKind
	Seed   uint64
}

// Metrics runs the simulation and returns its raw metrics.
func (r Run) Metrics() (*sim.Metrics, error) {
	cl := sim.DefaultCluster(r.Regime.KV)
	cl.PrefillRate = r.Regime.Prefill
	e := sim.NewEngine(cl, r.Opt.Policy(r.Key), sim.DefaultWorkload(r.Seed), nil, r.Regime.Events)
	e.M.WinStart, e.M.WinEnd = WinStart, WinEnd
	if err := e.Run(); err != nil {
		return nil, fmt.Errorf("%s %s %s seed %d: %w", r.Regime.Name, r.Opt.Name, r.Key, r.Seed, err)
	}
	return e.M, nil
}

// Plan lists the runs for the given regimes and keys: every option under
// every key, and round robin once per regime and seed.
func Plan(regimes []Regime, keys []sim.KeyKind, seeds []uint64) []Run {
	var runs []Run
	for _, rg := range regimes {
		for _, s := range seeds {
			runs = append(runs, Run{rg, RoundRobin, "", s})
			for _, k := range keys {
				for _, o := range Options {
					runs = append(runs, Run{rg, o, k, s})
				}
			}
		}
	}
	return runs
}

// Execute runs every plan entry, par at a time, and returns the cells in
// plan order.
func Execute(runs []Run, par int) ([]Cell, error) {
	cells := make([]Cell, len(runs))
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
			cells[i] = NewCell(r, m)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return cells, nil
}

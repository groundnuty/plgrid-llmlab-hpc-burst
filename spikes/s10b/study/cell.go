package study

import (
	"fmt"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

// Cell is one run's measures.
type Cell struct {
	Regime, Key, Option string
	OptionID            int
	Seed                uint64

	// Window (1,200-4,800 s) totals, as in S10.
	Requests     int
	HitTokens    int64
	PromptTokens int64
	HitRate      float64
	DoneOutput   int64 // output of requests completed in the window: the throughput measure
	P95          float64

	// Load against the engine limit. Peak is the most in flight, queued at
	// the Instance included, on any Instance when a request was placed.
	PeakRun, PeakWin int
	AboveRunS        float64 // time with some Instance above the limit, whole run
	AboveWinS        float64 // the same in the window
	AboveInstS       float64 // Instance-seconds above the limit, whole run

	// The hottest Instance of the window: the one with the highest peak.
	HotInst    int
	HotPeak    int
	HotHit     float64
	MinInstHit float64 // lowest hit rate on any Instance that served requests

	// The bound. AboveBound counts placements where the chosen Instance's
	// load before the request exceeded c*(T+1)/n, c in effect.
	Placements, AboveBound int
	// Option 3: placements the cap moved off a full choice, and how many of
	// those were above the bound; requests that waited at the front.
	Spills, SpillsWin, SpillAboveBound int
	FrontHeld, MaxFrontQ               int
	FrontWaitMean, FrontWaitP95        float64
	FrontWaitMax                       float64
	// Option 4: switches of the bound, the first one's time (-1 for none) and
	// the bounds that placed requests (MeanLoadPercentage).
	Switches   int
	SwitchT    float64
	BoundsUsed string
	// PctSpan: for each bound in use, the times of its first and last
	// placement. Not in the CSV; the switch test reads it.
	PctSpan map[int][2]float64

	// Windows from the drain time (3,000 s), also taken in runs without a
	// drain, where they are that option's no-event baseline.
	Hit05, Hit515 float64 // hit rate over 0-5 and 5-15 min after
	PostP95       float64 // p95 latency of requests arriving at 3,000 s or later
	PostPeak      int     // peak in flight on any Instance from 3,000 s
	PostAboveS    float64 // time above the engine limit from 3,000 s
	PostDone      int64   // output of requests completed from 3,000 s
}

// NewCell computes the measures of one run.
func NewCell(r Run, m *sim.Metrics) Cell {
	c := Cell{Regime: r.Regime.Name, Option: r.Opt.Name, OptionID: r.Opt.ID, Seed: r.Seed, Key: string(r.Key)}
	if c.Key == "" {
		c.Key = "-"
	}
	c.Requests, c.HitTokens, c.PromptTokens, c.HitRate = m.Requests, m.HitTokens, m.PromptTokens, m.HitRate()
	c.DoneOutput, c.P95 = m.DoneOutputTokens, m.Latency(0.95)
	c.PeakRun, c.AboveRunS, c.AboveWinS, c.AboveInstS = m.PeakLoad, m.AboveEngine, m.AboveEngineWin, m.AboveEngineInst
	c.MinInstHit = 1
	for i, st := range m.PerInst {
		if st.PeakInFlight > c.PeakWin {
			c.PeakWin = st.PeakInFlight
		}
		if st.PeakInFlight > c.HotPeak {
			c.HotInst, c.HotPeak = i, st.PeakInFlight
			c.HotHit = ratio(st.Hit, st.Prompt)
		}
		if st.Prompt > 0 {
			c.MinInstHit = min(c.MinInstHit, ratio(st.Hit, st.Prompt))
		}
	}
	c.Placements, c.AboveBound = m.Placements, m.KubeRuleViol
	c.Spills, c.SpillsWin, c.SpillAboveBound = m.Spills, m.SpillsWin, m.SpillRuleViol
	c.FrontHeld, c.MaxFrontQ = m.FrontHeld, m.MaxFrontQ
	_, c.FrontWaitMean, c.FrontWaitMax = m.FrontWaitStats()
	c.FrontWaitP95 = m.FrontWait(0.95)
	c.Switches, c.SwitchT = len(m.Switches), -1
	if len(m.Switches) > 0 {
		c.SwitchT = m.Switches[0].T
	}
	var used []int
	for p := range m.PctSpan {
		used = append(used, p)
	}
	sort.Ints(used)
	var parts []string
	for _, p := range used {
		parts = append(parts, fmt.Sprint(p))
	}
	c.BoundsUsed = strings.Join(parts, ";")
	c.PctSpan = m.PctSpan
	c.Hit05 = m.BucketHitRate(DrainT, DrainT+300)
	c.Hit515 = m.BucketHitRate(DrainT+300, DrainT+900)
	c.PostP95 = m.LatencyBetween(DrainT, WinEnd, 0.95)
	c.PostPeak = m.PeakBetween(DrainT, WinEnd)
	c.PostAboveS = m.AboveEngineBetween(DrainT, WinEnd)
	c.PostDone = m.DoneOutputBetween(DrainT, WinEnd)
	return c
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

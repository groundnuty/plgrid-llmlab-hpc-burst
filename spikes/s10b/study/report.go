package study

import (
	"fmt"
	"math"
	"strings"
)

type col struct {
	name string
	get  func(Cell) string
}

var cellCols = []col{
	{"regime", func(c Cell) string { return c.Regime }},
	{"key", func(c Cell) string { return c.Key }},
	{"option", func(c Cell) string { return c.Option }},
	{"seed", func(c Cell) string { return fmt.Sprint(c.Seed) }},
	{"requests", func(c Cell) string { return fmt.Sprint(c.Requests) }},
	{"prompt_tokens", func(c Cell) string { return fmt.Sprint(c.PromptTokens) }},
	{"hit_tokens", func(c Cell) string { return fmt.Sprint(c.HitTokens) }},
	{"hit_rate", func(c Cell) string { return fmt.Sprintf("%.5f", c.HitRate) }},
	{"done_output_tokens", func(c Cell) string { return fmt.Sprint(c.DoneOutput) }},
	{"p95_latency_s", func(c Cell) string { return fmt.Sprintf("%.3f", c.P95) }},
	{"peak_inflight_run", func(c Cell) string { return fmt.Sprint(c.PeakRun) }},
	{"peak_inflight_window", func(c Cell) string { return fmt.Sprint(c.PeakWin) }},
	{"above_engine_limit_run_s", func(c Cell) string { return fmt.Sprintf("%.1f", c.AboveRunS) }},
	{"above_engine_limit_window_s", func(c Cell) string { return fmt.Sprintf("%.1f", c.AboveWinS) }},
	{"above_engine_limit_instance_s", func(c Cell) string { return fmt.Sprintf("%.1f", c.AboveInstS) }},
	{"hot_instance", func(c Cell) string { return fmt.Sprint(c.HotInst) }},
	{"hot_peak_inflight", func(c Cell) string { return fmt.Sprint(c.HotPeak) }},
	{"hot_hit_rate", func(c Cell) string { return fmt.Sprintf("%.5f", c.HotHit) }},
	{"min_instance_hit_rate", func(c Cell) string { return fmt.Sprintf("%.5f", c.MinInstHit) }},
	{"placements", func(c Cell) string { return fmt.Sprint(c.Placements) }},
	{"placements_above_bound", func(c Cell) string { return fmt.Sprint(c.AboveBound) }},
	{"spills", func(c Cell) string { return fmt.Sprint(c.Spills) }},
	{"spills_window", func(c Cell) string { return fmt.Sprint(c.SpillsWin) }},
	{"spills_above_bound", func(c Cell) string { return fmt.Sprint(c.SpillAboveBound) }},
	{"front_held", func(c Cell) string { return fmt.Sprint(c.FrontHeld) }},
	{"front_queue_max", func(c Cell) string { return fmt.Sprint(c.MaxFrontQ) }},
	{"front_wait_mean_s", func(c Cell) string { return fmt.Sprintf("%.3f", c.FrontWaitMean) }},
	{"front_wait_p95_s", func(c Cell) string { return fmt.Sprintf("%.3f", c.FrontWaitP95) }},
	{"front_wait_max_s", func(c Cell) string { return fmt.Sprintf("%.3f", c.FrontWaitMax) }},
	{"bound_switches", func(c Cell) string { return fmt.Sprint(c.Switches) }},
	{"first_switch_time_s", func(c Cell) string { return fmt.Sprintf("%.1f", c.SwitchT) }},
	{"bounds_used_pct", func(c Cell) string { return c.BoundsUsed }},
	{"hit_rate_0_5_after_3000s", func(c Cell) string { return fmt.Sprintf("%.5f", c.Hit05) }},
	{"hit_rate_5_15_after_3000s", func(c Cell) string { return fmt.Sprintf("%.5f", c.Hit515) }},
	{"p95_latency_from_3000s", func(c Cell) string { return fmt.Sprintf("%.3f", c.PostP95) }},
	{"peak_inflight_from_3000s", func(c Cell) string { return fmt.Sprint(c.PostPeak) }},
	{"above_engine_limit_from_3000s_s", func(c Cell) string { return fmt.Sprintf("%.1f", c.PostAboveS) }},
	{"done_output_tokens_from_3000s", func(c Cell) string { return fmt.Sprint(c.PostDone) }},
}

// CellsCSV is one row per run.
func CellsCSV(cells []Cell) string {
	var b strings.Builder
	for i, c := range cellCols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c.name)
	}
	b.WriteByte('\n')
	for _, cell := range cells {
		for i, c := range cellCols {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(c.get(cell))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// Group is the cells of one regime, key and option, one per seed.
type Group struct {
	Regime, Key, Option string
	Cells               []Cell
}

// Groups groups cells in first-seen order.
func Groups(cells []Cell) []Group {
	var out []Group
	idx := map[string]int{}
	for _, c := range cells {
		k := c.Regime + "|" + c.Key + "|" + c.Option
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, Group{Regime: c.Regime, Key: c.Key, Option: c.Option})
		}
		out[i].Cells = append(out[i].Cells, c)
	}
	return out
}

func (g Group) mean(f func(Cell) float64) float64 {
	var s float64
	for _, c := range g.Cells {
		s += f(c)
	}
	return s / float64(len(g.Cells))
}

func (g Group) sum(f func(Cell) float64) float64 { return g.mean(f) * float64(len(g.Cells)) }

func (g Group) max(f func(Cell) float64) float64 {
	m := math.Inf(-1)
	for _, c := range g.Cells {
		m = math.Max(m, f(c))
	}
	return m
}

func (g Group) min(f func(Cell) float64) float64 {
	m := math.Inf(1)
	for _, c := range g.Cells {
		m = math.Min(m, f(c))
	}
	return m
}

// Baseline finds the round-robin group of a regime.
func Baseline(gs []Group, regime string) (Group, bool) {
	for _, g := range gs {
		if g.Regime == regime && g.Option == RoundRobin.Name {
			return g, true
		}
	}
	return Group{}, false
}

// Find returns the group of a regime, key and option.
func Find(gs []Group, regime, key, option string) (Group, bool) {
	for _, g := range gs {
		if g.Regime == regime && g.Key == key && g.Option == option {
			return g, true
		}
	}
	return Group{}, false
}

func pct(v float64) string { return fmt.Sprintf("%.1f%%", 100*v) }

// SummaryCSV is one row per regime, key and option: means over the seeds,
// and sums or maxima where the column says so.
func SummaryCSV(cells []Cell) string {
	gs := Groups(cells)
	var b strings.Builder
	b.WriteString("regime,key,option,seeds,hit_rate_mean,hit_rate_min,hit_rate_max,throughput_vs_rr,p95_latency_s_mean,peak_inflight_run_max,peak_inflight_window_max,above_engine_limit_run_s_sum,hot_peak_inflight_mean,hot_peak_inflight_max,hot_hit_rate_mean,min_instance_hit_rate_mean,placements_above_bound_sum,placements_sum,spills_sum,spills_above_bound_sum,front_held_sum,front_wait_max_s,bound_switches_sum,hit_rate_0_5_after_3000s_mean,hit_rate_5_15_after_3000s_mean,throughput_vs_rr_from_3000s,p95_latency_from_3000s_mean,peak_inflight_from_3000s_max,above_engine_limit_from_3000s_s_sum\n")
	for _, g := range gs {
		rr, _ := Baseline(gs, g.Regime)
		thr := g.mean(func(c Cell) float64 { return float64(c.DoneOutput) }) / rr.mean(func(c Cell) float64 { return float64(c.DoneOutput) })
		postThr := g.mean(func(c Cell) float64 { return float64(c.PostDone) }) / rr.mean(func(c Cell) float64 { return float64(c.PostDone) })
		fmt.Fprintf(&b, "%s,%s,%s,%d,%.5f,%.5f,%.5f,%.3f,%.2f,%.0f,%.0f,%.1f,%.1f,%.0f,%.5f,%.5f,%.0f,%.0f,%.0f,%.0f,%.0f,%.3f,%.0f,%.5f,%.5f,%.3f,%.2f,%.0f,%.1f\n",
			g.Regime, g.Key, g.Option, len(g.Cells),
			g.mean(func(c Cell) float64 { return c.HitRate }),
			g.min(func(c Cell) float64 { return c.HitRate }), g.max(func(c Cell) float64 { return c.HitRate }),
			thr,
			g.mean(func(c Cell) float64 { return c.P95 }),
			g.max(func(c Cell) float64 { return float64(c.PeakRun) }),
			g.max(func(c Cell) float64 { return float64(c.PeakWin) }),
			g.sum(func(c Cell) float64 { return c.AboveRunS }),
			g.mean(func(c Cell) float64 { return float64(c.HotPeak) }),
			g.max(func(c Cell) float64 { return float64(c.HotPeak) }),
			g.mean(func(c Cell) float64 { return c.HotHit }),
			g.mean(func(c Cell) float64 { return c.MinInstHit }),
			g.sum(func(c Cell) float64 { return float64(c.AboveBound) }),
			g.sum(func(c Cell) float64 { return float64(c.Placements) }),
			g.sum(func(c Cell) float64 { return float64(c.Spills) }),
			g.sum(func(c Cell) float64 { return float64(c.SpillAboveBound) }),
			g.sum(func(c Cell) float64 { return float64(c.FrontHeld) }),
			g.max(func(c Cell) float64 { return c.FrontWaitMax }),
			g.sum(func(c Cell) float64 { return float64(c.Switches) }),
			g.mean(func(c Cell) float64 { return c.Hit05 }),
			g.mean(func(c Cell) float64 { return c.Hit515 }),
			postThr,
			g.mean(func(c Cell) float64 { return c.PostP95 }),
			g.max(func(c Cell) float64 { return float64(c.PostPeak) }),
			g.sum(func(c Cell) float64 { return c.PostAboveS }))
	}
	return b.String()
}

package study

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// ReadCells parses a cells.csv written by S10b's CellsCSV back into cells. It
// fills every column the file has; the 300 s window series is in another
// file and is left empty. Round-tripping through CellsCSV reproduces the
// file byte for byte (TestReaderRoundTripsS10bCells), so a cell that was
// written and read again is rounded the same way as the committed rows.
func ReadCells(path string) ([]b.Cell, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(recs) < 1 {
		return nil, fmt.Errorf("%s: empty", path)
	}
	idx := map[string]int{}
	for i, h := range recs[0] {
		idx[h] = i
	}
	var cells []b.Cell
	for n, r := range recs[1:] {
		p := &rowParser{path: path, line: n + 2, row: r, idx: idx}
		c := b.Cell{
			Regime: p.str("regime"), Key: p.str("key"), Option: p.str("option"),
			Seed:     uint64(p.int("seed")),
			Requests: int(p.int("requests")), PromptTokens: p.int("prompt_tokens"), HitTokens: p.int("hit_tokens"),
			HitRate: p.float("hit_rate"), DoneOutput: p.int("done_output_tokens"), P95: p.float("p95_latency_s"),
			PeakRun: int(p.int("peak_inflight_run")), PeakWin: int(p.int("peak_inflight_window")),
			AboveRunS: p.float("above_engine_limit_run_s"), AboveWinS: p.float("above_engine_limit_window_s"),
			AboveInstS: p.float("above_engine_limit_instance_s"),
			HotInst:    int(p.int("hot_instance")), HotPeak: int(p.int("hot_peak_inflight")),
			HotHit: p.float("hot_hit_rate"), MinInstHit: p.float("min_instance_hit_rate"),
			Placements: int(p.int("placements")), AboveBound: int(p.int("placements_above_bound")),
			Spills: int(p.int("spills")), SpillsWin: int(p.int("spills_window")), SpillAboveBound: int(p.int("spills_above_bound")),
			FrontHeld: int(p.int("front_held")), MaxFrontQ: int(p.int("front_queue_max")),
			FrontWaitMean: p.float("front_wait_mean_s"), FrontWaitP95: p.float("front_wait_p95_s"), FrontWaitMax: p.float("front_wait_max_s"),
			Switches: int(p.int("bound_switches")), SwitchT: p.float("first_switch_time_s"), BoundsUsed: p.str("bounds_used_pct"),
			Hit05: p.float("hit_rate_0_5_after_3000s"), Hit515: p.float("hit_rate_5_15_after_3000s"), Hit1530: p.float("hit_rate_15_30_after_3000s"),
			PostP95: p.float("p95_latency_from_3000s"), PostPeak: int(p.int("peak_inflight_from_3000s")),
			PostAboveS: p.float("above_engine_limit_from_3000s_s"), PostDone: p.int("done_output_tokens_from_3000s"),
		}
		if p.err != nil {
			return nil, p.err
		}
		c.OptionID = optionID(c.Option)
		cells = append(cells, c)
	}
	return cells, nil
}

func optionID(name string) int {
	if name == b.RoundRobin.Name {
		return b.RoundRobin.ID
	}
	for _, o := range append([]b.Option{Option5}, b.Options...) {
		if o.Name == name {
			return o.ID
		}
	}
	return -1
}

// rowParser reads typed columns by name and keeps the first error, which
// names the file, line and column.
type rowParser struct {
	path string
	line int
	row  []string
	idx  map[string]int
	err  error
}

func (p *rowParser) str(col string) string {
	i, ok := p.idx[col]
	if !ok || i >= len(p.row) {
		if p.err == nil {
			p.err = fmt.Errorf("%s:%d: no column %q", p.path, p.line, col)
		}
		return ""
	}
	return p.row[i]
}

func (p *rowParser) int(col string) int64 {
	s := p.str(col)
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s:%d: column %q is %q, not an integer", p.path, p.line, col, s)
	}
	return v
}

func (p *rowParser) float(col string) float64 {
	s := p.str(col)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s:%d: column %q is %q, not a number", p.path, p.line, col, s)
	}
	return v
}

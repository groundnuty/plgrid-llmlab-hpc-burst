package study

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// SummaryRow is one row of a summary.csv, by column name.
type SummaryRow map[string]string

// ParseSummary reads the text of a summary.csv (S10b's SummaryCSV).
func ParseSummary(text string) ([]SummaryRow, error) {
	recs, err := csv.NewReader(strings.NewReader(text)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) < 1 {
		return nil, fmt.Errorf("empty summary")
	}
	var rows []SummaryRow
	for _, r := range recs[1:] {
		row := SummaryRow{}
		for i, h := range recs[0] {
			row[h] = r[i]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// label is the option's name in the tables, as in S10b's README.
func label(option string) string {
	switch option {
	case b.RoundRobin.Name:
		return "round robin"
	case "c=1.25":
		return "1. c = 1.25"
	case "c=2.0":
		return "2. c = 2.0"
	case "c=2.0+cap48":
		return "3. c = 2.0 + cap 48"
	case "adaptive 1.25/2.0":
		return "4. adaptive 1.25 / 2.0"
	case Option5.Name:
		return "5. adaptive 1.25 / 2.0, cap 48 at full membership"
	}
	return option
}

func (r SummaryRow) f(col string) float64 {
	v, err := strconv.ParseFloat(r[col], 64)
	if err != nil {
		panic(fmt.Sprintf("summary column %q is %q: %v", col, r[col], err))
	}
	return v
}

func (r SummaryRow) pct(col string) string { return fmt.Sprintf("%.1f%%", 100*r.f(col)) }

// find returns the row of a regime, key and option; round robin has no key.
func find(rows []SummaryRow, regime, key, option string) (SummaryRow, bool) {
	for _, r := range rows {
		if r["regime"] == regime && r["option"] == option && (option == b.RoundRobin.Name || r["key"] == key) {
			return r, true
		}
	}
	return nil, false
}

// CombinedTables renders S10b's three full-membership tables and its drain
// table with option 5 added, from summary rows: round robin and options 1-4
// as S10b committed them, option 5's from this study's run. The row layout
// is S10b's README's; TestTablesReproduceS10bREADME checks that each
// S10b row appears there verbatim.
func CombinedTables(rows []SummaryRow, key string) (string, error) {
	opts := append([]string{b.RoundRobin.Name}, optionNames(AllOptions())...)
	var sb strings.Builder
	for _, t := range []struct{ title, regime string }{
		{"Tight (1.5 M KV per Instance), full membership", b.Tight.Name},
		{"Scarce (750 k KV per Instance)", b.Scarce.Name},
		{"Half prefill (12,500 tokens/s)", b.HalfPrefill.Name},
	} {
		fmt.Fprintf(&sb, "**%s**\n\n", t.title)
		sb.WriteString("| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |\n|---|---|---|---|---|---|---|---|---|\n")
		for _, o := range opts {
			r, ok := find(rows, t.regime, key, o)
			if !ok {
				return "", fmt.Errorf("no summary row for %s / %s", t.regime, o)
			}
			above, spills := fmt.Sprintf("%.0f", r.f("placements_above_bound_sum")), fmt.Sprintf("%.0f", r.f("spills_sum"))
			if o == b.RoundRobin.Name {
				above, spills = "-", "-"
			}
			fmt.Fprintf(&sb, "| %s | %s [%s, %s] | %.2fx | %.1f | %.0f | %.0f | %.1f / %s | %s | %s |\n",
				label(o), r.pct("hit_rate_mean"), r.pct("hit_rate_min"), r.pct("hit_rate_max"), r.f("throughput_vs_rr"),
				r.f("p95_latency_s_mean"), r.f("peak_inflight_run_max"), r.f("above_engine_limit_run_s_sum"),
				r.f("hot_peak_inflight_mean"), r.pct("hot_hit_rate_mean"), above, spills)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("**After an unreplaced drain (instance-1 drained at 3,000 s)**\n\n")
	sb.WriteString("| option | hit 0-5 min after (no drain) | hit 5-15 min after (no drain) | hit 15-30 min after (no drain) | throughput vs RR from 3,000 s | p95 latency from 3,000 s (s) | peak in flight after the drain | time above 48 after the drain (s, sum over seeds) | held at the front (sum) |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, o := range opts {
		d, ok := find(rows, b.Drain.Name, key, o)
		base, ok2 := find(rows, b.Tight.Name, key, o)
		if !ok || !ok2 {
			return "", fmt.Errorf("no summary row for the drain regime / %s", o)
		}
		held := fmt.Sprintf("%.0f", d.f("front_held_sum"))
		if o == b.RoundRobin.Name {
			held = "-"
		}
		fmt.Fprintf(&sb, "| %s | %s (%s) | %s (%s) | %s (%s) | %.2fx | %.1f | %.0f | %.0f | %s |\n", label(o),
			d.pct("hit_rate_0_5_after_3000s_mean"), base.pct("hit_rate_0_5_after_3000s_mean"),
			d.pct("hit_rate_5_15_after_3000s_mean"), base.pct("hit_rate_5_15_after_3000s_mean"),
			d.pct("hit_rate_15_30_after_3000s_mean"), base.pct("hit_rate_15_30_after_3000s_mean"),
			d.f("throughput_vs_rr_from_3000s"), d.f("p95_latency_from_3000s_mean"), d.f("peak_inflight_from_3000s_max"),
			d.f("above_engine_limit_from_3000s_s_sum"), held)
	}
	return sb.String(), nil
}

func optionNames(opts []b.Option) []string {
	var out []string
	for _, o := range opts {
		out = append(out, o.Name)
	}
	return out
}

// Command s18table gathers spike S18's cell summaries (every summary.json under a results directory)
// into results.csv and results.md, one row per cell, sorted by fault, stream mode and arm.
//
//	s18table <results dir> <out dir>
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: s18table <results dir> <out dir>")
		os.Exit(2)
	}
	var rows []rig.Summary
	err := filepath.WalkDir(os.Args[1], func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "summary.json" {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var s rig.Summary
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		rows = append(rows, s)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sort.Slice(rows, func(a, b int) bool { return key(rows[a]) < key(rows[b]) })
	if err := os.MkdirAll(os.Args[2], 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeCSV(filepath.Join(os.Args[2], "results.csv"), rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(os.Args[2], "results.md"), []byte(markdown(rows)), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%d cells\n", len(rows))
}

func key(s rig.Summary) string {
	return fmt.Sprintf("%s|%v|%v|%s|%05.1f|%05.1f|%06.2f", s.Cell.Fault, s.Cell.Stream, s.Cell.ClientRetries, s.Cell.Arm, s.Cell.Burst, s.Cell.Refill, s.Cell.Rate)
}

var header = []string{"cell", "fault", "stream", "sdk_retries", "arm", "burst", "refill", "rate", "requests", "marked", "fired",
	"success", "success_fired", "success_unmarked", "duplicates", "reexecutions", "deaths", "restarts", "false_deaths", "false_stops",
	"ttfb_fired_p50_ms", "ttfb_fired_p95_ms", "ttfb_unmarked_p50_ms", "ttfb_unmarked_p95_ms", "corr_front", "corr_ctrl_30s",
	"corr_ctrl_2m", "corr_ctrl_10m", "poison_deaths_mean", "poison_deaths_max", "poison_final", "poison_corr_front", "poison_corr_ctrl"}

func record(s rig.Summary) []string {
	c := s.Cell
	pm, px, finals, cf, cc := poison(s.Poison)
	return []string{c.Name, c.Fault, fmt.Sprint(c.Stream), fmt.Sprint(c.ClientRetries), c.Arm, g(c.Burst), g(c.Refill), g(c.Rate),
		fmt.Sprint(s.Requests), fmt.Sprint(s.Marked), fmt.Sprint(s.Fired),
		f4(s.Success), f4(s.SuccessFired), f4(s.SuccessUnmarked), fmt.Sprint(s.Duplicates), fmt.Sprint(s.ReExecutions),
		fmt.Sprint(s.Deaths), fmt.Sprint(s.Restarts), fmt.Sprint(s.FalseDeaths), fmt.Sprint(s.FalseStops),
		f1(s.TTFBFiredP50), f1(s.TTFBFiredP95), f1(s.TTFBUnmarkedP50), f1(s.TTFBUnmarkedP95), fmt.Sprint(s.CorrFront),
		fmt.Sprint(s.CorrController["30s"]), fmt.Sprint(s.CorrController["2m0s"]), fmt.Sprint(s.CorrController["10m0s"]),
		pm, px, finals, cf, cc}
}

func poison(ps []rig.PoisonTrial) (mean, mx, finals, corrFront, corrCtrl string) {
	if len(ps) == 0 {
		return "", "", "", "", ""
	}
	sum, m, f, c := 0, 0, 0, 0
	st := map[int]int{}
	for _, p := range ps {
		sum += p.Deaths
		m = max(m, p.Deaths)
		st[p.Final]++
		if p.CorrFront {
			f++
		}
		if p.CorrController {
			c++
		}
	}
	var parts []string
	for k, v := range st {
		parts = append(parts, fmt.Sprintf("%dx%d", v, k))
	}
	sort.Strings(parts)
	n := len(ps)
	return fmt.Sprintf("%.2f", float64(sum)/float64(n)), fmt.Sprint(m), strings.Join(parts, " "), fmt.Sprintf("%d/%d", f, n), fmt.Sprintf("%d/%d", c, n)
}

func writeCSV(path string, rows []rig.Summary) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		return err
	}
	for _, s := range rows {
		if err := w.Write(record(s)); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func markdown(rows []rig.Summary) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(header, " | ") + " |\n|" + strings.Repeat("---|", len(header)) + "\n")
	for _, s := range rows {
		b.WriteString("| " + strings.Join(record(s), " | ") + " |\n")
	}
	return b.String()
}

func g(x float64) string {
	if x == 0 {
		return ""
	}
	return fmt.Sprintf("%g", x)
}
func f1(x float64) string { return fmt.Sprintf("%.1f", x) }
func f4(x float64) string { return fmt.Sprintf("%.4f", x) }

// Command s10csim runs spike S10c: option 5 (the adaptive bound with a hard
// cap that is active only at full membership) in S10b's four regimes on
// seeds 1-3, re-runs options 3 and 4 on one seed to compare them with S10b's
// committed rows byte for byte, and writes the tables and the verdict that
// put option 5 beside S10b's committed rows for round robin and options 1-4.
//
// Exit status: 0 on success; 1 when the re-run of options 3 and 4 differs
// from S10b's committed rows (the results are still written); 2 on bad usage
// (a repeated seed included), a failed run or a file error. A set of seeds
// other than the default ones is run, but verdict.md then says INCOMPLETE or
// INVALID and gives no verdict; the exit status is still 0.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10c/study"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type config struct {
	seeds              []uint64
	out, s10b          string
	par                int
	regressionSeed     uint64
	regressionOptionID []int
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseArgs(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "s10csim:", err)
		return 2
	}
	problems, err := execute(stdout, cfg)
	if err != nil {
		fmt.Fprintln(stderr, "s10csim:", err)
		return 2
	}
	if problems > 0 {
		fmt.Fprintf(stderr, "s10csim: %d differences or missing rows against S10b's committed rows; see %s\n",
			problems, filepath.Join(cfg.out, "regression-vs-s10b.txt"))
		return 1
	}
	return 0
}

func parseArgs(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("s10csim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var def []string
	for _, s := range b.RequiredSeeds {
		def = append(def, fmt.Sprint(s))
	}
	seedsF := fs.String("seeds", strings.Join(def, ","), "workload seeds, each once; the verdict needs exactly the default ones and gives none for a missing, repeated or extra seed")
	out := fs.String("out", "results", "output directory")
	s10b := fs.String("s10b-results", "../s10b/results", "S10b's committed results: options 1-4 and round robin are read from here, not run")
	par := fs.Int("parallel", 2, "simulations run at once")
	rseed := fs.Uint64("regression-seed", 1, "the seed on which options 3 and 4 are run again and compared with S10b's rows")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg := config{out: *out, s10b: *s10b, par: *par, regressionSeed: *rseed, regressionOptionID: []int{3, 4}}
	seen := map[uint64]bool{}
	for _, f := range strings.Split(*seedsF, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64)
		if err != nil {
			return config{}, fmt.Errorf("bad seed %q: %w", f, err)
		}
		if seen[v] { // a seed run twice is weighted twice in every mean and sum
			return config{}, fmt.Errorf("duplicate seed %d in -seeds", v)
		}
		seen[v] = true
		cfg.seeds = append(cfg.seeds, v)
	}
	return cfg, nil
}

func execute(stdout io.Writer, cfg config) (problems int, err error) {
	committedCells := filepath.Join(cfg.s10b, "cells.csv")
	committedText, err := os.ReadFile(committedCells)
	if err != nil {
		return 0, err
	}
	summaryText, err := os.ReadFile(filepath.Join(cfg.s10b, "summary.csv"))
	if err != nil {
		return 0, err
	}
	s10bCells, err := study.ReadCells(committedCells)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return 0, err
	}

	// Option 5, every regime and seed.
	runs := study.Plan(b.Regimes, cfg.seeds)
	fmt.Fprintf(stdout, "%d runs of option 5, %s\n", len(runs), time.Now().UTC().Format(time.RFC3339))
	t0 := time.Now()
	res, err := study.Execute(runs, cfg.par)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "done in %s\n", time.Since(t0).Round(time.Second))

	// Options 3 and 4 again, one seed, for the byte-for-byte comparison.
	var opts []b.Option
	for _, o := range b.Options {
		for _, id := range cfg.regressionOptionID {
			if o.ID == id {
				opts = append(opts, o)
			}
		}
	}
	reruns := study.PlanOptions(opts, b.Regimes, cfg.regressionSeed)
	fmt.Fprintf(stdout, "%d re-runs of options 3 and 4 on seed %d\n", len(reruns), cfg.regressionSeed)
	t0 = time.Now()
	rres, err := study.Execute(reruns, cfg.par)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(stdout, "done in %s\n", time.Since(t0).Round(time.Second))
	n, found := study.CompareWithS10b(study.Cells(rres), string(committedText))
	reg := regressionReport(n, cfg.regressionSeed, found)

	// The control: option 5 with the cap kept after the drain, drain regime.
	var ctlRuns []b.Run
	for _, s := range cfg.seeds {
		ctlRuns = append(ctlRuns, b.Run{Regime: b.Drain, Opt: study.ControlCapKept, Key: study.Key, Seed: s})
	}
	ctl, err := study.Execute(ctlRuns, cfg.par)
	if err != nil {
		return 0, err
	}

	// Option 5's summary, computed as S10b's was (in memory), against the
	// committed round-robin cells for the throughput ratio.
	cells5 := study.Cells(res)
	var rr []b.Cell
	for _, c := range s10bCells {
		if c.Option == b.RoundRobin.Name {
			rr = append(rr, c)
		}
	}
	sum5, err := optionRows(b.SummaryCSV(append(rr, cells5...)), study.Option5.Name)
	if err != nil {
		return 0, err
	}
	cells5CSV := b.CellsCSV(cells5)
	for _, f := range []struct{ name, body string }{
		{"cells.csv", cells5CSV},
		{"summary.csv", sum5},
		{"extras.csv", study.ExtraCSV(res)},
		{"rerun-options-3-4.csv", b.CellsCSV(study.Cells(rres))},
		{"control-cap-kept.csv", b.CellsCSV(study.Cells(ctl))},
		{"regression-vs-s10b.txt", reg},
	} {
		if err := write(stdout, cfg.out, f.name, f.body); err != nil {
			return 0, err
		}
	}

	// The verdict and the combined tables: every option through the same
	// CSV serialise-and-parse path, S10b's from its committed file and
	// option 5's from the file just written.
	cells5Read, err := study.ReadCells(filepath.Join(cfg.out, "cells.csv"))
	if err != nil {
		return 0, err
	}
	all := append(append([]b.Cell(nil), s10bCells...), cells5Read...)
	key := string(study.Key)
	verdict := fmt.Sprintf("## %s\n\n%s", key, study.VerdictMarkdown(all, key))
	if missing, invalid := study.Missing(all, key), study.Invalid(all, key); len(missing)+len(invalid) > 0 {
		fmt.Fprintf(stdout, "no verdict: %d groups are missing runs and %d cells are repeated or not asked for; see verdict.md\n", len(missing), len(invalid))
	}
	rows, err := study.ParseSummary(string(summaryText))
	if err != nil {
		return 0, err
	}
	rows5, err := study.ParseSummary(sum5)
	if err != nil {
		return 0, err
	}
	tables, err := study.CombinedTables(append(rows, rows5...), key)
	if err != nil {
		fmt.Fprintln(stdout, "combined tables not written:", err)
		tables = "No combined tables: " + err.Error() + "\n"
	}
	for _, f := range []struct{ name, body string }{{"verdict.md", verdict}, {"combined-tables.md", tables}} {
		if err := write(stdout, cfg.out, f.name, f.body); err != nil {
			return 0, err
		}
	}
	fmt.Fprintln(stdout, strings.SplitN(reg, "\n", 2)[0])
	return len(found), nil
}

// regressionReport is regression-vs-s10b.txt: the summary line, then one
// entry per difference or missing row, and exactly one newline at the end
// (a file with none, or with a blank last line, is a file git diff --check
// would flag).
func regressionReport(compared int, seed uint64, found []string) string {
	head := fmt.Sprintf("%d re-run cells (options 3 and 4, seed %d, four regimes) compared byte for byte with S10b's committed rows in cells.csv; %d differences or missing rows",
		compared, seed, len(found))
	return strings.Join(append([]string{head}, found...), "\n") + "\n"
}

// optionRows returns the summary header and the rows of one option.
func optionRows(summary, option string) (string, error) {
	lines := strings.Split(strings.TrimSuffix(summary, "\n"), "\n")
	out := []string{lines[0]}
	for _, l := range lines[1:] {
		if f := strings.Split(l, ","); len(f) > 2 && f[2] == option {
			out = append(out, l)
		}
	}
	if len(out) == 1 {
		return "", fmt.Errorf("no summary rows for %s", option)
	}
	return strings.Join(out, "\n") + "\n", nil
}

func write(stdout io.Writer, dir, name, body string) error {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "wrote", p)
	return nil
}

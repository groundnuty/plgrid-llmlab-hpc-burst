// Command s10bsim runs spike S10b: four load-bound options (c = 1.25,
// c = 2.0, c = 2.0 with a hard cap, an adaptive c) in four regimes, on
// S10's simulator, and writes CSV tables to the results directory.
//
// Exit status: 0 on success; 1 when the regression check against S10's
// committed results finds a mismatch or a missing row (the results and the
// diagnostic report are still written); 2 on bad usage, a failed run or a
// file error.
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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// executor runs the planned simulations, par at a time.
type executor func(runs []study.Run, par int) ([]study.Cell, error)

type config struct {
	seeds    []uint64
	keys     []sim.KeyKind
	regimes  []study.Regime
	out, s10 string
	par      int
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, study.Execute))
}

func run(args []string, stdout, stderr io.Writer, execute executor) int {
	cfg, err := parseArgs(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "s10bsim:", err)
		return 2
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		fmt.Fprintln(stderr, "s10bsim:", err)
		return 2
	}
	runs := study.Plan(cfg.regimes, cfg.keys, cfg.seeds)
	fmt.Fprintf(stdout, "%d runs, %s\n", len(runs), time.Now().UTC().Format(time.RFC3339))
	t0 := time.Now()
	cells, err := execute(runs, cfg.par)
	if err != nil {
		fmt.Fprintln(stderr, "s10bsim:", err)
		return 2
	}
	fmt.Fprintf(stdout, "done in %s\n", time.Since(t0).Round(time.Second))
	problems, err := writeResults(stdout, cfg, cells)
	if err != nil {
		fmt.Fprintln(stderr, "s10bsim:", err)
		return 2
	}
	if problems > 0 {
		fmt.Fprintf(stderr, "s10bsim: %d mismatches or missing rows against S10's committed results; see %s\n",
			problems, filepath.Join(cfg.out, "regression-vs-s10.txt"))
		return 1
	}
	return 0
}

func parseArgs(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("s10bsim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var seedParts []string
	for _, s := range study.RequiredSeeds {
		seedParts = append(seedParts, fmt.Sprint(s))
	}
	seedsF := fs.String("seeds", strings.Join(seedParts, ","), "workload seeds; the verdict needs all of the default ones")
	keysF := fs.String("keys", "header>opening,opening", "conversation keys; the first is the issue's")
	regimesF := fs.String("regimes", "tight,drain,scarce,half-prefill", "regimes to run; the verdict needs all four")
	out := fs.String("out", "results", "output directory")
	s10 := fs.String("s10-results", "../s10/results", "S10's committed results, for the regression check")
	par := fs.Int("parallel", 2, "simulations run at once")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg := config{out: *out, s10: *s10, par: *par}
	for _, f := range strings.Split(*seedsF, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64)
		if err != nil {
			return config{}, fmt.Errorf("bad seed %q: %w", f, err)
		}
		cfg.seeds = append(cfg.seeds, v)
	}
	for _, f := range strings.Split(*keysF, ",") {
		cfg.keys = append(cfg.keys, sim.KeyKind(strings.TrimSpace(f)))
	}
	for _, f := range strings.Split(*regimesF, ",") {
		found := false
		for _, rg := range study.Regimes {
			if rg.Name == strings.TrimSpace(f) {
				cfg.regimes, found = append(cfg.regimes, rg), true
			}
		}
		if !found {
			return config{}, fmt.Errorf("unknown regime %q", f)
		}
	}
	return cfg, nil
}

// writeResults writes every table and the verdict, then the regression
// report, and returns the number of regression mismatches and missing rows.
// The report is written whether or not there are any.
func writeResults(stdout io.Writer, cfg config, cells []study.Cell) (problems int, err error) {
	var verdict strings.Builder
	for _, k := range cfg.keys {
		fmt.Fprintf(&verdict, "## %s\n\n%s\n", k, study.VerdictMarkdown(cells, string(k)))
		if missing := study.Missing(cells, string(k)); len(missing) > 0 {
			fmt.Fprintf(stdout, "key %s: incomplete sweep (%d groups missing runs), so verdict.md gives no verdict\n", k, len(missing))
		}
	}
	for _, f := range []struct{ name, body string }{
		{"cells.csv", study.CellsCSV(cells)},
		{"summary.csv", study.SummaryCSV(cells)},
		{"windows.csv", study.SeriesCSV(cells)},
		{"verdict.md", verdict.String()},
	} {
		if err := write(stdout, cfg.out, f.name, f.body); err != nil {
			return 0, err
		}
	}
	n, found, err := study.CheckAgainstS10(cells, cfg.s10)
	if err != nil {
		return 0, err
	}
	reg := fmt.Sprintf("%d cells compared with S10's committed rows (requests, prompt tokens, hit tokens, completed output); %d mismatches or missing rows\n%s",
		n, len(found), strings.Join(found, "\n"))
	if err := write(stdout, cfg.out, "regression-vs-s10.txt", reg+"\n"); err != nil {
		return 0, err
	}
	fmt.Fprintln(stdout, strings.SplitN(reg, "\n", 2)[0])
	return len(found), nil
}

func write(stdout io.Writer, dir, name, body string) error {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "wrote", p)
	return nil
}

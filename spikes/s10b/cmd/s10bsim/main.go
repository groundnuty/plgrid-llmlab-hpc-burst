// Command s10bsim runs spike S10b: four load-bound options (c = 1.25,
// c = 2.0, c = 2.0 with a hard cap, an adaptive c) in four regimes, on
// S10's simulator, and writes CSV tables to the results directory.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

func main() {
	seedsF := flag.String("seeds", "1,2,3", "workload seeds")
	keysF := flag.String("keys", "header>opening,opening", "conversation keys; the first is the issue's")
	regimesF := flag.String("regimes", "tight,drain,scarce,half-prefill", "regimes to run")
	out := flag.String("out", "results", "output directory")
	s10 := flag.String("s10-results", "../s10/results", "S10's committed results, for the regression check")
	par := flag.Int("parallel", 2, "simulations run at once")
	flag.Parse()

	var seeds []uint64
	for _, f := range strings.Split(*seedsF, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64)
		if err != nil {
			log.Fatalf("bad seed %q: %v", f, err)
		}
		seeds = append(seeds, v)
	}
	var keys []sim.KeyKind
	for _, f := range strings.Split(*keysF, ",") {
		keys = append(keys, sim.KeyKind(strings.TrimSpace(f)))
	}
	var regimes []study.Regime
	for _, f := range strings.Split(*regimesF, ",") {
		found := false
		for _, rg := range study.Regimes {
			if rg.Name == strings.TrimSpace(f) {
				regimes, found = append(regimes, rg), true
			}
		}
		if !found {
			log.Fatalf("unknown regime %q", f)
		}
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	runs := study.Plan(regimes, keys, seeds)
	fmt.Printf("%d runs, %s\n", len(runs), time.Now().UTC().Format(time.RFC3339))
	t0 := time.Now()
	cells, err := study.Execute(runs, *par)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("done in %s\n", time.Since(t0).Round(time.Second))

	write(*out, "cells.csv", study.CellsCSV(cells))
	write(*out, "summary.csv", study.SummaryCSV(cells))
	var verdict strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&verdict, "## %s\n\n%s\n", k, study.VerdictMarkdown(cells, string(k)))
	}
	write(*out, "verdict.md", verdict.String())

	n, problems, err := study.CheckAgainstS10(cells, *s10)
	if err != nil {
		log.Fatal(err)
	}
	reg := fmt.Sprintf("%d cells compared with S10's committed rows (requests, prompt tokens, hit tokens, completed output); %d mismatches or missing rows\n%s",
		n, len(problems), strings.Join(problems, "\n"))
	write(*out, "regression-vs-s10.txt", reg+"\n")
	fmt.Println(strings.SplitN(reg, "\n", 2)[0])
}

func write(dir, name, body string) {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", p)
}

package alpha

// The live rows of the alpha runbook (issue #194). Each test is the gate of its
// rows: it skips unless hack/live-checks.sh (or a dry run) names one of them in
// HPCBURST_ALPHA_ROW, and the script counts a skip as a failure. The scenario
// that drives the stack comes from stepsFor: the stand-in steps of a dry run, or
// the live steps (steps_live_test.go).

import (
	"context"
	"os"
	"testing"
)

// gate is the variable that asks for a row, as hack/live-checks.sh's table names it.
const gate = "HPCBURST_ALPHA_ROW"

// runRow runs one row, live or dry, and reports its verdict.
func runRow(t *testing.T, family string, rows ...string) {
	t.Helper()
	if gate != envRow {
		t.Fatalf("the table's gate %s is not the one the config reads, %s", gate, envRow)
	}
	cfg, skip, err := ConfigFromEnv(os.Getenv, rows...)
	if skip != "" {
		t.Skip(skip + " (this test needs a real cluster)")
	}
	if err != nil {
		t.Fatalf("the settings cannot run %s: %v", cfg.Row.Name, err)
	}
	t.Logf("%s: %s", cfg.Row.Name, cfg.Mode())
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	var st Steps
	if cfg.Dry {
		st = newDrySteps(t, cfg, family)
	} else {
		st = liveSteps(t, cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+cfg.Row.Budget)
	defer cancel()
	failures, err := Run(ctx, cfg, st, rec, t.Logf)
	if err != nil {
		t.Fatalf("%s did not finish: %v", cfg.Row.Name, err)
	}
	if len(failures) > 0 {
		t.Fatalf("%s failed: %s", cfg.Row.Name, Summary(failures))
	}
	t.Logf("%s passed; the text to post is in %s", cfg.Row.Name, PostsFile)
}

// TestAlphaAthenaHandover is HBEP-7's and HBEP-5's alpha criterion on Athena
// plgrid-gpu-a100: a handover with a stream in flight cuts nothing, with one
// restart at the worker rung.
//
//livecheck:alpha-athena-handover
func TestAlphaAthenaHandover(t *testing.T) {
	runRow(t, "handover", "alpha-athena-handover")
}

// TestAlphaHeliosNoOverlap is HBEP-7's alpha criterion on Helios
// plgrid-gpu-gh200 under QOS now: no-overlap mode with the replacement queued
// ahead, no request cut, and the gap measured. It needs #174.
//
//livecheck:alpha-helios-nooverlap
func TestAlphaHeliosNoOverlap(t *testing.T) {
	runRow(t, "nooverlap", "alpha-helios-nooverlap")
}

// TestTunnelHour is HBEP-6's alpha criterion: S1's one-hour run over this
// wiring, on the cluster of the row, with the path rate measured.
//
//livecheck:tunnel-hour-helios,tunnel-hour-athena
func TestTunnelHour(t *testing.T) {
	runRow(t, "tunnel", "tunnel-hour-helios", "tunnel-hour-athena")
}

package alpha

// The live rows of the alpha runbook (issue #194). Each test is the gate of its
// rows: it skips unless hack/live-checks.sh (or a dry run) names one of them in
// HPCBURST_ALPHA_ROW, and the script counts a skip as a failure. The scenario
// that drives the stack comes from stepsFor: the stand-in steps of a dry run, or
// the live steps (steps_live_test.go).

import (
	"context"
	"os"
	"strings"
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
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Row.RunDeadline(cfg.PendingMax))
	defer cancel()
	failures, err := Run(ctx, cfg, st, rec, t.Logf)
	if NotRun(cfg.Row, err) {
		// The protocol of #210: the marker, then the test's one top-level skip. The scenario has cancelled
		// the run's jobs by their tags; no record and no text to post were written.
		note := NotRunNote(cfg.Row, cfg.PendingMax, err)
		if merr := rec.SaveText(NotRunFile, note); merr != nil {
			t.Fatalf("%s: a job did not start in time, but the not-run marker could not be written: %v", cfg.Row.Name, merr)
		}
		t.Skipf("NOT RUN: %s", strings.TrimSpace(note))
	}
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
// ahead, no request cut, and the gap measured.
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

// TestAlphaAthenaNowServe is row A of issue #376 on Athena plgrid-now under profiles/athena-now.yaml: the long stream
// with attribution, the deployment ID under its tag, HBEP-8's events and engine gauges, one worker-rung restart, and
// the deployment's end with its job seen ended (no requeue: #503).
//
//livecheck:alpha-athena-now-serve
func TestAlphaAthenaNowServe(t *testing.T) {
	runRow(t, "nowserve", "alpha-athena-now-serve")
}

// TestBetaAthenaNowKeeper is the keeper row of issue #521 (beta's tracker, #522) on Athena plgrid-now under profiles/athena-now.yaml, in host-dialed
// mode with the relay keeper: the controller is killed through a pinned pidfd while a stream is served, and the stream finishes whole, the relays
// the same processes, the agent undisturbed. A context that cannot hold a stream for the window is NOT RUN.
//
//livecheck:beta-athena-now-keeper
func TestBetaAthenaNowKeeper(t *testing.T) {
	runRow(t, "betakeeper", BetaKeeperName)
}

// TestBetaAthenaNowRouter is the router row of issue #521 (beta's tracker, #522) on Athena plgrid-now under profiles/athena-now.yaml: Envoy's
// first run on a compute node, run bare from the build staged by its pinned digest, the node's glibc, one request through the front, S13's
// cases through the real path as a response-level reading on vLLM (not bytes), and one worker-rung restart with the router in front. A node
// whose glibc is under 2.30 is NOT RUN, naming the version.
//
//livecheck:beta-athena-now-router
func TestBetaAthenaNowRouter(t *testing.T) {
	runRow(t, "betarouter", BetaRouterName)
}

// TestAlphaAthenaNowNoOverlap is row B of issue #376 on Athena plgrid-now under profiles/athena-now.yaml: HBEP-7's
// no-overlap mode on the one-slot path, two jobs in turn, both warm, with a stream across the drain start, the
// successor's Allocation only after the predecessor's job has left the slot, and the gap measured.
//
//livecheck:alpha-athena-now-nooverlap
func TestAlphaAthenaNowNoOverlap(t *testing.T) {
	runRow(t, "nownooverlap", "alpha-athena-now-nooverlap")
}

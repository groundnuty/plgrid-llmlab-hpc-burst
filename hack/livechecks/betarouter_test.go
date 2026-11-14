package livechecks

// The router row on plgrid-now, beta-athena-now-router (issue #521), through the script. Its NOT RUNs are two: a job past the row's start
// wait, as on every row, and a compute node whose glibc is under the 2.30 Envoy needs, read on the node or from the agent's own log. The
// second is run with the real row's test in dry mode (internal/alpha, dry_betarouter_test.go), so what the script prints is what the test
// wrote. It exits 3, says NOT RUN with the version and nothing else of the node, and never PASS or FAIL; its cleanup leaves nothing, and no
// text is posted, since the run sent nothing. The lines are written out here from the issue's criteria, not read from the script.

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestTheRouterRowsNotRunsThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	const row = "beta-athena-now-router"
	for _, c := range []struct{ fault, line string }{
		{"pend-past-bound", row + ": NOT RUN (a job did not start within 10 min of the deployment's creation, the row's start wait, and was cancelled by its tag; not a failure)"},
		// The probe on the node says it; the agent's own log of a refused start says it; the minimum's neighbour below says it.
		{"glibc-old-probe", row + ": NOT RUN (the compute node's glibc is older than the 2.30 Envoy needs (glibc 2.28); Envoy cannot run bare there; the run sent no request through the front, and the run's jobs were cancelled by tag; not a failure)"},
		{"glibc-old-agent", row + ": NOT RUN (the compute node's glibc is older than the 2.30 Envoy needs (glibc 2.28); Envoy cannot run bare there; the run sent no request through the front, and the run's jobs were cancelled by tag; not a failure)"},
		{"glibc-one-under", row + ": NOT RUN (the compute node's glibc is older than the 2.30 Envoy needs (glibc 2.29); Envoy cannot run bare there; the run sent no request through the front, and the run's jobs were cancelled by tag; not a failure)"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, row+": tests: 0 passed, 0 failed, 1 skipped", c.line, "cleanup: left behind 0")
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			if m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a run that sent nothing left a text to post: %v", m)
			}
		})
	}
	// The control: a node at exactly the minimum is no NOT RUN and no failure, through the same script.
	t.Run("glibc-at-the-minimum", func(t *testing.T) {
		t.Parallel()
		w := newAlphaWorld(t)
		w.installDry(dryWrapper, real, realDir)
		res := w.run([]string{"STUB_DRY_FAULT=glibc-at-the-minimum"}, "--run", row, "--yes")
		if res.code != 0 {
			t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
		}
		// The row's own verdict line: the owner's-go note above it says "NOT RUN" of the start wait, whatever the verdict.
		if strings.Contains(res.out, row+": NOT RUN (") || !strings.Contains(res.out, row+": PASS") {
			t.Errorf("a node at the minimum did not read as a pass:\n%s", res.out)
		}
	})
}

// The glibc marker on another row is no NOT RUN: only the router row has a glibc to read. And a marker whose parentheses hold more than a
// version prints no more than the fixed words, never what it also carried.
func TestTheGlibcMarkerOnAnotherRowIsAFailureAndCarriesOnlyTheVersion(t *testing.T) {
	for _, row := range []string{"alpha-athena-now-serve", "alpha-athena-now-nooverlap", "tunnel-hour-athena"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-glibc"}, "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("a glibc marker on %s read as NOT RUN:\n%s", row, res.out)
			}
		})
	}
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run-glibc-words"}, "--run", "beta-athena-now-router", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	if strings.Contains(res.out, "host-x.example.org") {
		t.Errorf("the script printed what the marker carried beside the version:\n%s", res.out)
	}
	w.contains(res, "the version could not be read from the marker")
}

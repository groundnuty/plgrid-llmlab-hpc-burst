package livechecks

// Row A on plgrid-now, alpha-athena-now-serve (issue #376), through the script: its three NOT RUNs, each with the real
// row's test in dry mode (internal/alpha, dry_test.go). A job past the row's start wait is NOT RUN with no text to post,
// as on every row; a refused requeue and a warm start the agent's own log did not show are NOT RUN after the rest of the
// run passed, with its text to post saying so. Each exits 3, says NOT RUN with its own words and never PASS or FAIL, and
// its cleanup leaves nothing. The lines are written out here from the science agent's rulings, not read from the script.

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRowAsNotRunsThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	const row = "alpha-athena-now-serve"
	for _, c := range []struct {
		fault, line string
		posts       bool
	}{
		{"pend-past-bound", row + ": NOT RUN (a job did not start within 10 min of the deployment's creation, the row's start wait, and was cancelled by its tag; not a failure)", false},
		{"requeue-refused", row + ": NOT RUN (the scheduler refused the requeue of the run's own job: the scheduler says the operation is disabled for this job; the run's other checks passed; not a failure)", true},
		{"warm-no-image", row + ": NOT RUN (the agent's own log did not show the warm start: the image already staged and verified: false; checkpoint files present and matching: 3 of 3; the run's other checks passed; not a failure)", true},
		{"warm-cold-then-retry", row + ": NOT RUN (the agent's own log did not show the warm start: the first staging was built, failed or tried again: not the first attempt's evidence; the run's other checks passed; not a failure)", true},
		{"warm-no-start", row + ": NOT RUN (the agent's own log did not show the warm start: the log read is not the whole of the agent's first start to its first worker; the run's other checks passed; not a failure)", true},
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
			m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md"))
			if !c.posts {
				if len(m) != 0 {
					t.Errorf("a run whose job never started left a text to post: %v", m)
				}
				return
			}
			if p := w.postsOf(row); !strings.Contains(p, row+": NOT RUN") || strings.Contains(p, row+": PASS") {
				t.Errorf("the text to post does not say NOT RUN:\n%s", p)
			}
		})
	}
}

// A marker of row A's NOT RUNs on another row is no NOT RUN: only row A requeues its job and reads its agent's log.
func TestARowAMarkerOnAnotherRowIsAFailure(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run-row-a"}, "--run", "tunnel-hour-athena", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	if strings.Contains(res.out, "NOT RUN") {
		t.Errorf("a row A marker on another row read as NOT RUN:\n%s", res.out)
	}
}

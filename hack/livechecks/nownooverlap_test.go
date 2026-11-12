package livechecks

// Row B on plgrid-now, alpha-athena-now-nooverlap (issue #376), through the script: its NOT RUNs, each with the real row's
// test in dry mode (internal/alpha, nownooverlap_dry_test.go). Its first job past its start wait, and its second past its
// own start wait from its Allocation's creation, are NOT RUN with no text to post, as on every row; the owner's job in the
// slot between the jobs (Q7) and either job's warm start not shown are NOT RUN after the rest of the run passed, with its
// text to post saying so. Each exits 3, says NOT RUN with its own words and never PASS or FAIL, and its cleanup leaves
// nothing. The lines are written out here from the science agent's rulings, not read from the script.

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRowBsNotRunsThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	const row = "alpha-athena-now-nooverlap"
	for _, c := range []struct {
		fault string
		lines []string
		posts bool
	}{
		{"pend-past-bound", []string{row + ": NOT RUN (a job did not start within 10 min of the deployment's creation, the row's start wait, and was cancelled by its tag; not a failure)"}, false},
		{"second-never-started", []string{row + ": NOT RUN (the first job ran and the second never started within its start wait of 10 min from its Allocation's creation, and was cancelled by its tag; not a failure)",
			row + ": ReplacementLate for its deployment: not raised (information, not a check)"}, false},
		{"slot-taken", []string{row + ": NOT RUN (a job that is not the run's held the one-job slot between the row's two jobs: the owner's own work, left untouched; the run ended its deployment; the run's other checks passed; not a failure)"}, true},
		{"slot-taken-refused", []string{row + ": NOT RUN (a job that is not the run's held the one-job slot between the row's two jobs: the owner's own work, left untouched; the run ended its deployment; the run's other checks passed; not a failure)"}, true},
		{"warm-pre-unshown", []string{row + ": NOT RUN (the agent's own log did not show the warm start: the predecessor's job: the agent's log of the job could not be read; the run's other checks passed; not a failure)"}, true},
		{"warm-suc-unshown", []string{row + ": NOT RUN (the agent's own log did not show the warm start: the successor's job: the agent's log of the job could not be read; the run's other checks passed; not a failure)"}, true},
	} {
		t.Run(c.fault, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, append([]string{row + ": tests: 0 passed, 0 failed, 1 skipped", "cleanup: left behind 0"}, c.lines...)...)
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
			if p := w.postsOf(row); !strings.Contains(p, row+": NOT RUN") || strings.Contains(p, row+": PASS") || strings.Contains(p, "owner-other-project-job") {
				t.Errorf("the text to post does not say NOT RUN, or names the owner's job:\n%s", p)
			}
		})
	}
}

// A marker of row B's slot NOT RUN on another row is no NOT RUN: only row B reads the slot between two jobs. Nor is a warm
// start's marker on a row that does not start warm.
func TestARowBMarkerOnAnotherRowIsAFailure(t *testing.T) {
	for _, c := range []struct{ mode, row string }{
		{"not-run-row-b-slot", "alpha-athena-now-serve"},
		{"not-run-row-b-slot", "tunnel-hour-athena"},
		{"not-run-row-b-warm", "tunnel-hour-athena"},
	} {
		t.Run(c.mode+"/"+c.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=" + c.mode}, "--run", c.row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, c.row+": NOT RUN") {
				t.Errorf("a row B marker on %s read as NOT RUN:\n%s", c.row, res.out)
			}
		})
	}
	// The control: the same slot marker on row B is its NOT RUN.
	w := newAlphaWorld(t)
	if res := w.run([]string{"STUB_ALPHA_MODE=not-run-row-b-slot"}, "--run", "alpha-athena-now-nooverlap", "--yes"); res.code != 3 {
		t.Errorf("row B's own slot marker: exit %d, want 3:\n%s", res.code, res.out)
	}
}

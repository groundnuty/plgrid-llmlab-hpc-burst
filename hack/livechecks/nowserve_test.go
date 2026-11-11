package livechecks

// Row A on plgrid-now, alpha-athena-now-serve (issue #376), through the script: its three NOT RUNs, each with the real
// row's test in dry mode (internal/alpha, dry_test.go). A job past the row's start wait is NOT RUN with no text to post,
// as on every row; a refused requeue and a warm start the agent's own log did not show are NOT RUN after the rest of the
// run passed, with its text to post saying so. Each exits 3, says NOT RUN with its own words and never PASS or FAIL, and
// its cleanup leaves nothing. The lines are written out here from the science agent's rulings, not read from the script.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
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
		{"warm-cold-then-retry", row + ": NOT RUN (the agent's own log did not show the warm start: the first staging was built, failed a check or was tried again: not the first attempt's evidence; the run's other checks passed; not a failure)", true},
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

// The pre-staging row's shape is the package's row (internal/alpha, NowStageRow): athena-now on plgrid-now, its slot, row
// A's per-GPU size, the 30-minute time limit that is its lease, its timeout its budget and its start wait of 10 minutes.
func TestTheScriptsStageRowIsThePackagesRow(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	r := alpha.NowStageRow()
	var f []string
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		if g := strings.Split(l, "|"); g[0] == r.Name {
			f = g
		}
	}
	if len(f) != 14 {
		t.Fatalf("the script has no shape of 14 fields for %s: %q", r.Name, f)
	}
	// name | host | profile | partition | qos | slot | account key | GPUs | CPUs | memory | time limit | timeout | extra | start wait
	want := []string{r.Name, r.Cluster, r.ProfileName(), r.Partition, "-", "plgrid-now", "ACCOUNT_ATHENA_A100", "1", "16", "125Gi", "00:30:00",
		fmt.Sprint(int(r.Budget / time.Minute)), "-", fmt.Sprint(int(r.StartWait / time.Minute))}
	if strings.Join(f, "|") != strings.Join(want, "|") {
		t.Errorf("the script's shape is %q, the package's row gives %q", f, want)
	}
	if r.Lease != 30*time.Minute || r.SlotHold != 42*time.Minute || r.MostJobs != 1 || r.OwnerGo != alpha.OwnerGoAthenaNow {
		t.Errorf("the package's pre-staging row is %+v", r)
	}
}

// The pre-staging row builds its helper before its slot, and its test gets the build, the profile and the start wait, and
// none of the stack rows' settings.
func TestTheStageRowBuildsItsHelperAndGivesTheTestItsEnvironment(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run(nil, "--run", "alpha-athena-now-stage", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	env := w.stateFile("test-env-TestNowStage")
	for _, want := range []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=athena-now", "HPCBURST_CONF_PARTITION=plgrid-now", "HPCBURST_CONF_START_WAIT=10m", "HPCBURST_CONF_STAGE_HELPER="} {
		if !strings.Contains(env, want) {
			t.Errorf("the test's environment lacks %s:\n%s", want, env)
		}
	}
	for _, not := range []string{"HPCBURST_ALPHA_RUN_ID", "HPCBURST_ALPHA_DIALHOME_MODE", "HPCBURST_CONF_QOS"} {
		if strings.Contains(env, not) {
			t.Errorf("the test's environment has %s:\n%s", not, env)
		}
	}
	ev := w.stateFile("events")
	if i, j := strings.Index(ev, "build-stage-helper "), strings.Index(ev, "testbin "); i < 0 || j < 0 || i > j {
		t.Errorf("the helper was not built before the test ran:\n%s", ev)
	}
}

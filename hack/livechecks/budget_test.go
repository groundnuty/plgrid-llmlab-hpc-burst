package livechecks

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// The margin of a test's own waits: they give up this long before its -test.timeout (internal/rehearsal/stack, CleanupMargin).
const waitsGiveUp = 2 * time.Minute

// The margin a timeout keeps beyond the waits it covers, for the calls each poll makes and the run's own cleanup.
const budgetMargin = 3 * time.Minute

var (
	testTimeout = regexp.MustCompile(`-test\.timeout (\d+)m`)
	leaseEnv    = regexp.MustCompile(`(?m)^HPCBURST_ALPHA_LEASE_MINUTES=(\d+)$`)
)

// stackRowTests are the tests of the rows internal/alpha runs.
var stackRowTests = map[string]string{"alpha-athena-handover": "TestAlphaAthenaHandover", "alpha-helios-nooverlap": "TestAlphaHeliosNoOverlap",
	"tunnel-hour-helios": "TestTunnelHour", "tunnel-hour-athena": "TestTunnelHour"}

// B5 and B6 of the review of PR 284: a row's timeout must let its bound fire first, so a test fails when it can fire
// before them. The sum comes from the row's own bounds and the real waits, never from a number kept beside the table.
// Every row of the table has a case: a new row fails here until some test derives its timeout.
//   - A stack row: below, at each wait an operator can ask for.
//   - real-engine: no timeout wraps it, since its own two clocks end it, so its column is a statement; it must still hold
//     the table's time limit and the margin its poll really uses.
//   - The conformance, accounting and reach rows: internal/providerctl/conformance adds up their tests' own source.
func TestEveryRowsTimeoutIsDerivedFromItsBounds(t *testing.T) {
	raw, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range block(t, string(raw), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		name := f[0]
		switch {
		case stackRowTests[name] != "":
			if _, ok := alpha.RowByName(name); !ok {
				t.Errorf("%s has a test here, and no row in internal/alpha", name)
			}
		case strings.HasPrefix(name, "conformance-"), strings.HasPrefix(name, "gpu-accounting-"), strings.HasPrefix(name, "reach-"):
			// internal/providerctl/conformance, TestEveryRowsTimeoutIsAboveTheWorstCaseItsSourceAddsUpTo.
		case name == "real-engine":
			timeout, err := strconv.Atoi(f[11])
			if err != nil {
				t.Fatal(err)
			}
			limit, err := time.ParseDuration(strings.Replace(strings.Replace(f[10], ":", "h", 1), ":", "m", 1) + "s")
			if err != nil {
				t.Fatal(err)
			}
			m := regexp.MustCompile(`margin=\$\{LIVE_RUN_MARGIN_SECONDS:-(\d+)\}`).FindStringSubmatch(string(raw))
			if m == nil {
				t.Fatal("real-engine's margin is not in the script")
			}
			seconds, _ := strconv.Atoi(m[1])
			if got, want := time.Duration(timeout)*time.Minute, limit+time.Duration(seconds)*time.Second+budgetMargin; got < want {
				t.Errorf("real-engine: its timeout %s is below its time limit, its poll's margin of %d s and the margin of %s", got, seconds, budgetMargin)
			}
		default:
			t.Errorf("%s is a row no test derives a timeout for: say how its bounds and its waits add up", name)
		}
	}
}

// Through the script: the timeout is the flag it really passes, the wait in the queue and the lease what it really hands the
// test, and the worst case the steps' own waits (internal/alpha, Row.WorstCase, whose sum is checked by hand there). It
// must be above: the waits give up before the test's deadline, so a shorter timeout would cut one short and report what
// the row's bound never decided.
func TestEveryStackRowsTimeoutIsAboveItsWorstCase(t *testing.T) {
	for _, row := range alpha.Rows() {
		test, ok := stackRowTests[row.Name]
		if !ok {
			t.Errorf("%s has no case here: name the test the row runs", row.Name)
			continue
		}
		leases := []int{0}
		if row.MostJobs == 1 {
			leases = []int{0, 106, 240}
		}
		// 0 is the row's own default.
		for _, seconds := range []int{0, 59, 600, 1800, 5400, 7200, 21600} {
			for _, lease := range leases {
				t.Run(fmt.Sprintf("%s/wait %d s/lease %d", row.Name, seconds, lease), func(t *testing.T) {
					t.Parallel()
					w := newAlphaWorld(t)
					if lease != 0 {
						w.alphaSettings(func(m map[string]string) { m["LIVE_TUNNEL_LEASE_MINUTES"] = strconv.Itoa(lease) })
					}
					var env []string
					if seconds != 0 {
						env = []string{"LIVE_PENDING_MAX_SECONDS=" + strconv.Itoa(seconds)}
					}
					if res := w.run(env, "--run", row.Name, "--yes"); res.code != 0 {
						t.Fatalf("exit %d:\n%s", res.code, res.out)
					}
					delivered := w.stateFile("test-env-" + test)
					p := pendingEnv.FindStringSubmatch(delivered)
					if p == nil {
						t.Fatalf("the test got no wait in the queue:\n%s", delivered)
					}
					pendingSeconds, _ := strconv.Atoi(p[1])
					r := row
					if m := leaseEnv.FindStringSubmatch(delivered); m != nil {
						minutes, _ := strconv.Atoi(m[1])
						var err error
						if r, err = alpha.WithTunnelLease(row, time.Duration(minutes)*time.Minute); err != nil {
							t.Fatal(err)
						}
					}
					m := testTimeout.FindStringSubmatch(w.stateFile("test-args-" + test))
					if m == nil {
						t.Fatalf("no -test.timeout in %q", w.stateFile("test-args-"+test))
					}
					minutes, _ := strconv.Atoi(m[1])
					giveUp := time.Duration(minutes)*time.Minute - waitsGiveUp
					if worst := r.WorstCase(time.Duration(pendingSeconds) * time.Second); worst >= giveUp {
						t.Errorf("-test.timeout %dm: the waits give up at %s, and the row's worst case is %s, so the timeout fires before its bound can", minutes, giveUp, worst)
					}
				})
			}
		}
	}
}

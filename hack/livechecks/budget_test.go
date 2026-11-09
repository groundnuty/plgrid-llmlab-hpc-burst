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

// waitsGiveUp is internal/rehearsal/stack's CleanupMargin: a test's waits give up this long before its -test.timeout.
const waitsGiveUp = 2 * time.Minute

var testTimeout = regexp.MustCompile(`-test\.timeout (\d+)m`)

// B5 of the review of PR 284: a row's timeout must let its bound fire first. The worst case comes from the waits the live
// steps use (internal/alpha, Row.WorstCase; a figure in the shape's table would prove nothing) and the timeout from the
// flag the script really passes, at each wait in the queue an operator can ask for and each lease. It must be above: the
// waits themselves give up before the test's deadline, so a shorter timeout would cut a wait short and report what
// the row's bound never decided. Row.WorstCase's sum is checked by hand in internal/alpha.
func TestEveryStackRowsTimeoutIsAboveItsWorstCase(t *testing.T) {
	tests := map[string]string{"alpha-athena-handover": "TestAlphaAthenaHandover", "alpha-helios-nooverlap": "TestAlphaHeliosNoOverlap",
		"tunnel-hour-helios": "TestTunnelHour", "tunnel-hour-athena": "TestTunnelHour"}
	for _, row := range alpha.Rows() {
		test, ok := tests[row.Name]
		if !ok {
			t.Errorf("%s has no case here: name the test the row runs", row.Name)
			continue
		}
		leases := []int{0}
		if row.MostJobs == 1 {
			leases = []int{0, 106, 240}
		}
		// 0 is the row's own default: 30 minutes in a slot and 2 hours with none.
		for _, seconds := range []int{0, 59, 600, 1800, 5400, 7200, 21600} {
			for _, lease := range leases {
				t.Run(fmt.Sprintf("%s/wait %d s/lease %d", row.Name, seconds, lease), func(t *testing.T) {
					t.Parallel()
					w := newAlphaWorld(t)
					r := row
					if lease != 0 {
						var err error
						if r, err = alpha.WithTunnelLease(row, time.Duration(lease)*time.Minute); err != nil {
							t.Fatal(err)
						}
						w.alphaSettings(func(m map[string]string) { m["LIVE_TUNNEL_LEASE_MINUTES"] = strconv.Itoa(lease) })
					}
					var env []string
					pending := 2 * time.Hour
					if r.SlotHold > 0 {
						pending = 30 * time.Minute
					}
					if seconds != 0 {
						env = []string{"LIVE_PENDING_MAX_SECONDS=" + strconv.Itoa(seconds)}
						pending = time.Duration(seconds) * time.Second
					}
					if res := w.run(env, "--run", r.Name, "--yes"); res.code != 0 {
						t.Fatalf("exit %d:\n%s", res.code, res.out)
					}
					m := testTimeout.FindStringSubmatch(w.stateFile("test-args-" + test))
					if m == nil {
						t.Fatalf("no -test.timeout in %q", w.stateFile("test-args-"+test))
					}
					minutes, _ := strconv.Atoi(m[1])
					giveUp := time.Duration(minutes)*time.Minute - waitsGiveUp
					if worst := r.WorstCase(pending); worst >= giveUp {
						t.Errorf("-test.timeout %dm: the waits give up at %s, and the row's worst case is %s, so the timeout fires before its bound can", minutes, giveUp, worst)
					}
				})
			}
		}
	}
}

// statedWorstCase is what the script's comment states for a row: the waits in the queue, and the minutes beside them.
var statedWorstCase = regexp.MustCompile(`(?m)^# worst case of ([a-z0-9-]+): (the wait \+ |2 waits \+ )?(\d+) min`)

// Every row of the cluster table states its worst case, and its timeout column is above it by the margin of 3 minutes. A row
// added later fails here until it does. A stated figure proves nothing by itself, so each is recomputed where the code
// is: the stack rows' below, from the steps' waits; conformance, accounting and reach from their test's source
// (internal/providerctl/conformance, TestEveryRowsTimeoutIsAboveTheWorstCaseItsSourceAddsUpTo); real-engine's from its
// time limit and the margin of its own poll in the script, since no timeout wraps it.
func TestEveryRowsTimeoutIsAboveItsStatedWorstCase(t *testing.T) {
	raw, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	type figure struct{ waits, minutes int }
	stated := map[string]figure{}
	for _, m := range statedWorstCase.FindAllStringSubmatch(string(raw), -1) {
		n, _ := strconv.Atoi(m[3])
		stated[m[1]] = figure{map[string]int{"": 0, "the wait + ": 1, "2 waits + ": 2}[m[2]], n}
	}
	seen := map[string]bool{}
	for _, l := range block(t, string(raw), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		name := f[0]
		seen[name] = true
		fig, ok := stated[name]
		if !ok {
			t.Errorf("%s states no worst case in the script's comment", name)
			continue
		}
		timeout, err := strconv.Atoi(f[11])
		if err != nil {
			t.Fatal(err)
		}
		if timeout < fig.minutes+3 {
			t.Errorf("%s: its timeout of %d min is not above its worst case of %d min by the margin of 3", name, timeout, fig.minutes)
		}
		if name == "real-engine" {
			margin := regexp.MustCompile(`margin=\$\{LIVE_RUN_MARGIN_SECONDS:-(\d+)\}`).FindStringSubmatch(string(raw))
			if margin == nil {
				t.Fatal("real-engine's margin is not in the script")
			}
			limit, err := time.ParseDuration(strings.Replace(strings.Replace(f[10], ":", "h", 1), ":", "m", 1) + "s")
			if err != nil {
				t.Fatal(err)
			}
			seconds, _ := strconv.Atoi(margin[1])
			if want := int(limit/time.Minute) + seconds/60; fig.minutes != want || fig.waits != 1 {
				t.Errorf("real-engine states %+v, and its time limit and its poll's margin add up to the wait and %d", fig, want)
			}
		}
		if row, ok := alpha.RowByName(name); ok && (strings.HasPrefix(name, "alpha-") || strings.HasPrefix(name, "tunnel-hour-")) {
			// WorstCase is linear in the wait up to a tunnel row's cap: two waits under it give the figure's two numbers.
			a, b := row.WorstCase(30*time.Minute), row.WorstCase(60*time.Minute)
			if waits := int((b - a) / (30 * time.Minute)); waits != fig.waits || a != time.Duration(fig.waits*30+fig.minutes)*time.Minute {
				t.Errorf("%s states %+v, and its steps' waits give %s at a wait of 30 min and %s at 60", name, fig, a, b)
			}
		}
	}
	for name := range stated {
		if !seen[name] {
			t.Errorf("the script's comment states a worst case for %s, which is no row of the cluster table", name)
		}
	}
}

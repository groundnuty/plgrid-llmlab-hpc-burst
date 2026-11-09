package livechecks

import (
	"fmt"
	"regexp"
	"strconv"
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

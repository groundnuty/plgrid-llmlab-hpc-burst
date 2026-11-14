package livechecks

// The bulk row's INVALID (issue #521; the science agent's ruling on #521): a run judged, recorded and posted with no failure and no saturation
// witness. Its test writes internal/alpha's InvalidNote to the marker and skips once, and the script reads that as INVALID on the bulk row alone.
// The bulk row is pending in the script's tables, so --run refuses it (exit 2) before its test could run: the positive case, INVALID and exit 3
// through the script, waits for the row to be made ready (beta_rows_test.go, pendingAlphaRows). What runs now is the guard: the marker on any other
// row is no INVALID and no NOT RUN but a failure, with the words the Go test writes, not a copy of them.

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

func TestTheInvalidMarkerOnAnotherRowIsAFailure(t *testing.T) {
	for _, row := range []string{"alpha-athena-now-serve", "beta-athena-now-router", "beta-athena-now-keeper", "tunnel-hour-athena"} {
		t.Run(row, func(t *testing.T) {
			// The control first: the same mode with the plain start-wait marker reads NOT RUN on this row, so the failure below is the INVALID words'.
			for _, c := range []struct {
				name, marker string
				code         int
			}{
				{"a start-wait marker", row + ": a job did not start within the start wait of 10m0s from the deployment's creation; this run's jobs were cancelled by tag\n", 3},
				{"the INVALID marker", alpha.InvalidNote(alpha.Row{Name: row}), 1},
			} {
				w := newAlphaWorld(t)
				if row == "beta-athena-now-keeper" {
					hostDialed(w)
				}
				res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + c.marker}, "--run", row, "--yes")
				if res.code != c.code {
					t.Fatalf("%s: exit %d, want %d:\n%s", c.name, res.code, c.code, res.out)
				}
				if strings.Contains(res.out, row+": INVALID") {
					t.Errorf("%s on %s read as INVALID:\n%s", c.name, row, res.out)
				}
				if c.code == 1 && strings.Contains(res.out, row+": NOT RUN") {
					t.Errorf("%s on %s read as NOT RUN:\n%s", c.name, row, res.out)
				}
			}
		})
	}
}

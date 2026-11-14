package livechecks

// The bulk row's INVALID (issue #521; the science agent's ruling on #521): a run judged, recorded and posted with no failure and no saturation
// witness. Its test writes internal/alpha's InvalidNote to the marker and skips once, and the script reads that as INVALID on the bulk row alone.
// The row is ready since its test shim (option (A)): the positive case runs the real test in dry mode through the script, INVALID and exit 3 with its
// text to post; the guard is that the marker on any other row is no INVALID and no NOT RUN but a failure, with the words the Go test writes, not a
// copy of them.

import (
	"path/filepath"
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
				if hostDialedOnly(row) {
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

// The positive INVALID, now that the row can run: the real test in dry mode, with a control too short of gaps to measure the transfer, is judged,
// recorded and posted, and the script reads it as INVALID, exit 3, apart from PASS, FAIL and NOT RUN. The control is the same run with its gaps: PASS.
func TestTheBulkRowsInvalidThroughTheScript(t *testing.T) {
	const row = "beta-athena-now-bulk"
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	for _, c := range []struct {
		fault string
		code  int
		says  string
	}{
		{"", 0, row + ": PASS"},
		{"control-few-gaps", 3, row + ": INVALID (not a NOT RUN"},
	} {
		w := newAlphaWorld(t)
		w.installDry(dryWrapper, real, realDir)
		res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
		if res.code != c.code {
			t.Fatalf("fault %q: exit %d, want %d:\n%s", c.fault, res.code, c.code, res.out)
		}
		w.contains(res, c.says)
		if c.code == 3 {
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") || strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("an INVALID run was also reported as a pass, a failure or a NOT RUN:\n%s", res.out)
			}
			if p := w.postsOf(row); !strings.Contains(p, "INVALID") {
				t.Errorf("the text to post does not say INVALID:\n%s", p)
			}
		}
	}
}

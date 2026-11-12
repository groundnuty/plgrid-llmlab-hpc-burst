package livechecks

// The NOT RUN of a handover row whose streams' lead does not fit the deployment's max-model-len (issue #447, the science
// agent's ruling (a), 12 Nov 2026), through the script. The three rows that open a stream across a drain start have it; the
// lines are written out from the ruling, not read from the script, and the marker is the one internal/alpha's NotRunNote
// writes (handoverlead_test.go there pins its text).

import (
	"path/filepath"
	"strings"
	"testing"
)

// leadLine is what the script prints for the marker of a rate of 600 tokens a second on a context of 32768, whose fitting lead is
// 26 s: NOT RUN, exit 3, with the numbers and no other text from the marker.
func leadLine(row string) string {
	return row + ": NOT RUN (no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate: " +
		"at 600 tokens/s with max-model-len 32768 the lead that fits is 26s, under the floor of 30s; the run opened no stream, and its jobs were cancelled by tag; not a failure)"
}

func TestAHandoverRowsLeadThatDoesNotFitIsNotRunThroughTheScript(t *testing.T) {
	for _, row := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "alpha-athena-now-nooverlap"} {
		t.Run(row, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-lead"}, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, row+": tests: 0 passed, 0 failed, 1 skipped", "cleanup: left behind 0", leadLine(row))
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			// No record and no text to post: the run opened no stream, so it holds nothing to post.
			if m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a run that opened no stream left a text to post: %v", m)
			}
		})
	}
}

// The marker's numbers are the only text of it that reaches the screen: words where the rate goes are not printed.
func TestTheLeadMarkersReasonIsPrintedOnlyAsNumbers(t *testing.T) {
	const row = "alpha-athena-now-nooverlap"
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run-lead-words"}, "--run", row, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, row+": NOT RUN (no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate: "+
		"the rate and the context could not be read from the marker; the run opened no stream, and its jobs were cancelled by tag; not a failure)")
	if strings.Contains(res.out, "owner-private-host") {
		t.Errorf("text of the marker's reason reached the screen:\n%s", res.out)
	}
}

// Only a row that opens a stream across a drain start sizes a lead: the marker on any other row is no NOT RUN, so a marker that a
// row cannot write is a failure, not a quiet retry.
func TestALeadMarkerOnARowThatSizesNoLeadIsAFailure(t *testing.T) {
	for _, row := range []string{"tunnel-hour-athena", "tunnel-hour-helios", "alpha-athena-now-serve", "gpu-accounting-athena"} {
		t.Run(row, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-lead"}, "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, row+": NOT RUN") || !strings.Contains(res.out, row+": FAIL") {
				t.Errorf("a lead marker on %s read as NOT RUN, or was not a FAIL:\n%s", row, res.out)
			}
		})
	}
}

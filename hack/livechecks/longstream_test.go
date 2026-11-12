package livechecks

// The NOT RUN of a tunnel row whose long stream does not fit the deployment's max-model-len (issue #451, the science agent's ruling
// (b), 12 Nov 2026), through the script. Both tunnel rows have it; the lines are written out from the ruling, not read from the script,
// and the marker is the one internal/alpha's NotRunNote writes (longstream_test.go there pins its text).

import (
	"path/filepath"
	"strings"
	"testing"
)

// longLine is what the script prints for the marker of a rate of 300 tokens a second on a context of 32768, whose fitting stream is
// 1 min 47 s against a floor of 2 minutes: NOT RUN, exit 3, with the numbers and no other text from the marker.
func longLine(row string) string {
	return row + ": NOT RUN (the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate: " +
		"at 300 tokens/s with max-model-len 32768 the stream that fits is 1m47s, under the floor of 2m0s; the hour was not started, and the run's jobs were cancelled by tag; not a failure)"
}

func TestATunnelRowsLongStreamThatDoesNotFitIsNotRunThroughTheScript(t *testing.T) {
	for _, row := range []string{"tunnel-hour-athena", "tunnel-hour-helios"} {
		t.Run(row, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-long"}, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, row+": tests: 0 passed, 0 failed, 1 skipped", "cleanup: left behind 0", longLine(row))
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			// No record and no text to post: the hour was not started.
			if m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a run that did not start the hour left a text to post: %v", m)
			}
		})
	}
}

// The marker's numbers are the only text of it that reaches the screen: words where the rate goes are not printed.
func TestTheLongMarkersReasonIsPrintedOnlyAsNumbers(t *testing.T) {
	const row = "tunnel-hour-athena"
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run-long-words"}, "--run", row, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, row+": NOT RUN (the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate: "+
		"the rate and the context could not be read from the marker; the hour was not started, and the run's jobs were cancelled by tag; not a failure)")
	if strings.Contains(res.out, "owner-private-host") {
		t.Errorf("text of the marker's reason reached the screen:\n%s", res.out)
	}
}

// Only a tunnel row sizes a long stream: the marker on any other row is no NOT RUN, so a marker that a row cannot write is a failure,
// not a quiet retry.
func TestALongMarkerOnARowThatSizesNoLongStreamIsAFailure(t *testing.T) {
	for _, row := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "alpha-athena-now-nooverlap", "alpha-athena-now-serve", "gpu-accounting-athena"} {
		t.Run(row, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-long"}, "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, row+": NOT RUN") || !strings.Contains(res.out, row+": FAIL") {
				t.Errorf("a long-stream marker on %s read as NOT RUN, or was not a FAIL:\n%s", row, res.out)
			}
		})
	}
}

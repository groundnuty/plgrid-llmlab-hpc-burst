package alpha

// The stand-in proof of the hour's long stream (issue #451): the LIVE steps of the tunnel rows over the real stack and the fake Slurm
// (standin_test.go), run with HPCBURST_ALPHA_STANDIN=1 under the stand-in's rules (launcher.lock, a memory cap; heavy runs on a hosted
// runner). The stand-in's deployment has the context 32768 (the large bodies need it), its engine double streams 50 tokens a second, and
// its clock is scaled: a deadline of 75 s, a NearDeadline of 60 s (a cap of 67.5 s) and a floor of 30 s. What fits, 32256 / 50, is over
// ten minutes, so the cap binds: the stand-in's long stream is its cap, as before the stream was sized to the context, and the hour passes
// with it. The fit between the floor and the cap, at the A100's rate on 32768, is out of the stand-in's reach: the burst needs 45 s of
// stream in the same context (about 716 tokens a second at most), so no engine double leaves a fit under the cap. That case is proved in
// the unit tests (longstream_test.go), at the A100's numbers.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStandInTheTunnelHoursLongStreamIsItsCapWhenTheContextHoldsIt is a tunnel row over the stand-in with the context holding far more
// than the cap: the hour passes its own verdict, and the plan the record carries is the cap, 67.5 s, sized at the measured rate (at most the
// engine double's 50 tokens a second) on the context 32768.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInTheTunnelHoursLongStreamIsItsCapWhenTheContextHoldsIt(t *testing.T) {
	s := standInLeadRun(t, "tunnel-hour-athena", "", nil)
	if err := s.stop.problem(); err != nil {
		t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
	}
	if s.err != nil || len(s.failures) > 0 {
		t.Fatalf("%s over the stand-in: failures %v, err %v; want a PASS", s.row.Name, s.failures, s.err)
	}
	record, err := os.ReadFile(filepath.Join(s.cfg.Out, RecordFile))
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Observation struct {
			LongPlan LongPlan `json:"longPlan"`
		} `json:"observation"`
	}
	if err := json.Unmarshal(record, &rec); err != nil {
		t.Fatalf("the record does not parse: %v", err)
	}
	p := rec.Observation.LongPlan
	t.Logf("%s: the long stream was sized for %s, %d tokens, at %.1f tokens/s on a context of %d", s.row.Name, p.Length, p.Tokens, p.Rate, p.MaxModelLen)
	if p.Length != 67500*time.Millisecond || p.MaxModelLen != 32768 || p.Rate <= 0 || p.Rate > 50 || p.Tokens <= 0 {
		t.Errorf("the plan is %+v, want a length of the stand-in's cap, 67.5 s, on the context 32768, at a rate over 0 and at most the double's 50 tokens a second", p)
	}
	post, _ := os.ReadFile(filepath.Join(s.cfg.Out, PostsFile))
	if !strings.Contains(string(post), "- the long stream: ") || !strings.Contains(string(post), "sized for 1m7.5s at the measured ") {
		t.Errorf("the text to post lacks the long stream's line:\n%s", post)
	}
}

// TestStandInATunnelRowWhoseLongStreamDoesNotFitIsNotRun is the other side, on both tunnel rows: the floor raised to 20 minutes, above the
// 10.75 minutes that 32256 tokens last at the double's 50 tokens a second at the very least, so the stream that fits is under it. The row
// is NOT RUN before any request of the hour (the script's exit 3), no failure, no record and no text to post, nothing left running, and the
// marker names the context 32768 and the floor.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInATunnelRowWhoseLongStreamDoesNotFitIsNotRun(t *testing.T) {
	for _, name := range []string{"tunnel-hour-athena", "tunnel-hour-helios"} {
		t.Run(name, func(t *testing.T) {
			s := standInLeadRun(t, name, "", func(r Row) Row {
				r.Timing.LongFloor = 20 * time.Minute
				return r
			})
			if err := s.stop.problem(); err != nil {
				t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
			}
			if len(s.failures) != 0 || !errors.Is(s.err, ErrLongTooShort) || !NotRun(s.row, s.err) {
				t.Fatalf("failures %v, err %v; want the NOT RUN of a long stream that does not fit", s.failures, s.err)
			}
			var lt *LongTooShortError
			if !errors.As(s.err, &lt) || lt.MaxModelLen != 32768 || lt.Floor != 20*time.Minute || lt.Rate <= 0 || lt.Rate > 50 || lt.Fit >= lt.Floor {
				t.Errorf("the error is %+v, want the context 32768, the floor 20m, a rate over 0 and at most 50 tokens a second and a fit under the floor", lt)
			}
			note := NotRunNote(s.row, s.cfg.PendingMax, s.err)
			t.Logf("the marker would say: %s", strings.TrimSpace(note))
			if !strings.Contains(note, "with max-model-len 32768 the stream that fits is ") || !strings.Contains(note, "under the floor of 20m0s") {
				t.Errorf("the marker %q does not name the context and the floor", note)
			}
			s.wroteNothing(t, "a NOT RUN")
			s.nothingLeftRunning(t)
		})
	}
}

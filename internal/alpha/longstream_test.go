package alpha

// The hour's long stream (issue #451; longstream.go). Every expected value is typed from the science agent's ruling (b) on #451
// (as long as the context allows at the measured rate, capped at 4 min 30 s, at least 2 minutes) worked by hand, never computed by
// FitLong or by a formula that copies it.

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The live clock of the tunnel rows: a deadline of 5 minutes, a NearDeadline of 4, so a cap of 4 min 30 s, and a floor of 2 minutes.
func liveTunnelClock(t *testing.T) Timing {
	t.Helper()
	tm := rowOf("tunnel-hour-athena").Clock()
	if tm.LongCap() != 270*time.Second || tm.LongFloor != 2*time.Minute {
		t.Fatalf("the tunnel row's clock has a cap of %s and a floor of %s, want 4m30s and 2m", tm.LongCap(), tm.LongFloor)
	}
	return tm
}

// The case the issue names: Athena's A100 at 199 tokens a second on a context of 32768. (32768 - 512) / 199 = 162.09 s, which is 162 s,
// 2 min 42 s, once rounded down: the row runs with a long stream of that length, where 4 min 30 s was 53,730 tokens. A mutant that drops
// the fit and keeps the cap (4 min 30 s) fails here.
func TestFitLongAtAnA100sRateOnA32kContextIs2Minutes42Seconds(t *testing.T) {
	got, err := FitLong(liveTunnelClock(t), 32768, 199)
	if err != nil {
		t.Fatalf("FitLong(live, 32768, 199) = error %v, want 2m42s: the row runs", err)
	}
	if want := 162 * time.Second; got != want {
		t.Errorf("FitLong(live, 32768, 199) = %s, want %s", got, want)
	}
}

func TestFitLongIsWhatFitsCappedAtFourAndAHalfMinutesAndAtLeastTwo(t *testing.T) {
	live := liveTunnelClock(t)
	// The stand-ins' clock: a deadline of 75 s, a NearDeadline of 60 s (a cap of 67.5 s) and a floor of 30 s.
	scaled := Timing{MaxRequestDuration: 75 * time.Second, NearDeadline: 60 * time.Second, LongFloor: 30 * time.Second}
	for _, c := range []struct {
		name        string
		tm          Timing
		maxModelLen int
		rate        float64
		want        time.Duration
	}{
		// A slow engine: 32256 / 100 = 322 s, over the cap, so the cap: what the long stream was before it was sized.
		{"a rate of 100 tokens a second", live, 32768, 100, 270 * time.Second},
		{"a rate of 20 tokens a second", live, 32768, 20, 270 * time.Second},
		// The cap exactly: (27512 - 512) / 100 = 270 s.
		{"a fit of exactly the cap", live, 27512, 100, 270 * time.Second},
		// One token short of it: 26999 / 100 = 269.99 s, rounded down to 269 s.
		{"a fit one token under the cap", live, 27511, 100, 269 * time.Second},
		// The floor exactly: (12512 - 512) / 100 = 120 s runs.
		{"a fit of exactly the floor", live, 12512, 100, 120 * time.Second},
		// 11999 / 100 = 119.99 s is under the floor: see the NOT RUN cases below.
		// The reserve is 512: (13512 - 512) / 100 = 130 s, where 256 would give 132 s.
		{"the prompt's reserve", live, 13512, 100, 130 * time.Second},
		// The stand-ins' clock: a cap of 67.5 s that is no whole second, returned exactly, and not rounded down to 67 s.
		{"the stand-ins' cap", scaled, 32768, 50, 67500 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitLong(c.tm, c.maxModelLen, c.rate)
			if err != nil {
				t.Fatalf("FitLong(%d, %v) = error %v, want %s", c.maxModelLen, c.rate, err, c.want)
			}
			if got != c.want {
				t.Errorf("FitLong(%d, %v) = %s, want %s", c.maxModelLen, c.rate, got, c.want)
			}
			// The stream fits the context less the reserve: the stream the row opens is one tokensFor sizes without error.
			if n := int(c.rate * got.Seconds()); n+512 > c.maxModelLen {
				t.Errorf("a stream of %d tokens (%s at %v tokens/s) does not fit max-model-len %d with 512 for the prompt", n, got, c.rate, c.maxModelLen)
			}
		})
	}
}

// A fit under the floor is NOT RUN, never a weaker pass.
func TestFitLongUnderTheFloorIsLongTooShort(t *testing.T) {
	live := liveTunnelClock(t)
	scaled := Timing{MaxRequestDuration: 75 * time.Second, NearDeadline: 60 * time.Second, LongFloor: 30 * time.Second}
	for _, c := range []struct {
		name        string
		tm          Timing
		maxModelLen int
		rate        float64
		wantFit     time.Duration
	}{
		// 32256 / 300 = 107.52 s, rounded down to 107 s: 1 min 47 s.
		{"a rate of 300 on a 32k context", live, 32768, 300, 107 * time.Second},
		// One token under the floor: 11999 / 100 = 119.99 s, rounded down to 119 s.
		{"a fit one token under the floor", live, 12511, 100, 119 * time.Second},
		// The stand-ins' 4096 context at the A100's rate: 3584 / 199 = 18.01 s.
		{"a 4096 context at the A100's rate", live, 4096, 199, 18 * time.Second},
		{"a context of the reserve alone", live, 512, 100, 0},
		{"a context under the reserve", live, 100, 100, 0},
		// The stand-ins' floor of 30 s: (2047 - 512) / 100 = 15.35 s is under it, rounded down to 15 s.
		{"a fit under the stand-ins' floor", scaled, 2047, 100, 15 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitLong(c.tm, c.maxModelLen, c.rate)
			if !errors.Is(err, ErrLongTooShort) {
				t.Fatalf("FitLong(%d, %v) = %s, %v; want ErrLongTooShort", c.maxModelLen, c.rate, got, err)
			}
			var lt *LongTooShortError
			if !errors.As(err, &lt) || lt.Rate != c.rate || lt.MaxModelLen != c.maxModelLen || lt.Fit != c.wantFit || lt.Floor != c.tm.LongFloor {
				t.Errorf("the error is %+v, want the rate %v, the context %d, a fit of %s and the floor %s", lt, c.rate, c.maxModelLen, c.wantFit, c.tm.LongFloor)
			}
			if got != 0 {
				t.Errorf("a stream of %s came with the error", got)
			}
		})
	}
}

// The error names the rate and the context: the message a run stops on.
func TestLongTooShortErrorNamesTheRateAndTheContext(t *testing.T) {
	_, err := FitLong(liveTunnelClock(t), 32768, 300)
	want := "the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate " +
		"(at 300 tokens/s with max-model-len 32768 the stream that fits is 1m47s, under the floor of 2m0s)"
	if err == nil || err.Error() != want {
		t.Errorf("the error is %v, want %q", err, want)
	}
}

// A rate that is not a positive finite number measured nothing a stream can be sized from: a plain failure, never a NOT RUN.
func TestFitLongRefusesRatesThatMeasureNothing(t *testing.T) {
	for name, rate := range map[string]float64{"zero": 0, "negative": -3, "not a number": math.NaN(), "infinite": math.Inf(1)} {
		t.Run(name, func(t *testing.T) {
			got, err := FitLong(liveTunnelClock(t), 32768, rate)
			if err == nil || errors.Is(err, ErrLongTooShort) || got != 0 {
				t.Errorf("FitLong = %s, %v; want a plain error and no length", got, err)
			}
		})
	}
}

// What check 2 asks of the stream a plan sized: NearDeadline for a stream sized at the cap, as ever (4 minutes of 4 min 30 s), and the
// same share of a shorter one (2 min 42 s asks for 2 min 24 s: 162 of 270 is 0.6, and 0.6 of 240 is 144). No plan is judged on NearDeadline.
func TestACheck2AsksForTheSameShareOfWhatWasSized(t *testing.T) {
	live := liveTunnelClock(t)
	scaled := Timing{MaxRequestDuration: 75 * time.Second, NearDeadline: 60 * time.Second, LongFloor: 30 * time.Second}
	for _, c := range []struct {
		name string
		tm   Timing
		plan time.Duration
		want time.Duration
	}{
		{"a stream sized at the cap", live, 270 * time.Second, 240 * time.Second},
		{"the A100's 2 min 42 s", live, 162 * time.Second, 144 * time.Second},
		{"the floor, 2 minutes", live, 120 * time.Second, 106667 * time.Millisecond},
		{"no plan", live, 0, 240 * time.Second},
		{"a plan over the cap", live, 300 * time.Second, 240 * time.Second},
		{"the stand-ins' cap", scaled, 67500 * time.Millisecond, 60 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.tm.withLongPlan(LongPlan{Length: c.plan}).NearDeadline; got != c.want {
				t.Errorf("a plan of %s asks for a stream of %s, want %s", c.plan, got, c.want)
			}
		})
	}
}

// The verdict reads the plan: on the scaled clock (a deadline of 60 s, NearDeadline 48 s, a cap of 54 s), an hour whose longest completed
// streams last 47 s has no stream of NearDeadline and fails check 2; the same hour passes when its long stream was sized for 50 s (which asks
// for 44.4 s) or for 40 s (35.6 s), and still fails when it was sized at the cap, 54 s (which asks for 48 s).
func TestTheVerdictJudgesCheck2OnWhatTheLongStreamWasSizedFor(t *testing.T) {
	r := scaledTunnel()
	_, jobs := scaledHour(r)
	hour := func(plan time.Duration) TunnelHourObs {
		o := scaledLiveHour(r)
		o.Requests = append([]Stream(nil), o.Requests...)
		for i, q := range o.Requests {
			if q.ID == "a0" || q.ID == "a1" || q.ID == "a2" || q.ID == "b0" {
				o.Requests[i].Ended = q.Opened.Add(47 * time.Second)
			}
		}
		o.LongPlan = LongPlan{Length: plan}
		return o
	}
	for _, c := range []struct {
		name string
		plan time.Duration
		want []string
	}{
		{"no plan", 0, []string{longStreamFailure}},
		{"sized at the cap", 54 * time.Second, []string{longStreamFailure}},
		{"sized for 50 s", 50 * time.Second, nil},
		{"sized for 40 s", 40 * time.Second, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := VerifyTunnelHour(r, hour(c.plan), jobs)
			if !slices.Equal(f, c.want) {
				t.Errorf("failures %v, want %v", f, c.want)
			}
		})
	}
	if want := "the hour held no completed long stream, as long as the context allows at the measured rate, within the deadline"; longStreamFailure != want {
		t.Errorf("check 2's failure reads %q, want %q", longStreamFailure, want)
	}
}

// The plan is posted with the result: its tokens, its length, the rate and the context it was sized at, the cap and the floor, and the
// longest completed stream of the hour within the deadline. A run with no plan (a dry run) has no such line.
func TestTheTextToPostCarriesTheLongStreamsPlan(t *testing.T) {
	r := scaledTunnel()
	o := scaledLiveHour(r)
	if post := tunnelHourPost(r, o, nil); strings.Contains(post, "the long stream") {
		t.Errorf("a run with no plan posted a long stream's line:\n%s", post)
	}
	o.LongPlan = LongPlan{Length: 50 * time.Second, Tokens: 9950, Rate: 199, MaxModelLen: 32768}
	want := "- the long stream: 9950 tokens, sized for 50s at the measured 199 tokens/s on a context of 32768 (cap 54s, floor 2m0s); the longest completed stream within the deadline: 50s\n"
	if post := tunnelHourPost(r, o, nil); !strings.Contains(post, want) {
		t.Errorf("the text to post lacks %q:\n%s", want, post)
	}
}

// NOT RUN on the tunnel rows, through Run: the long stream's ErrLongTooShort stops the run before the hour's first request and ends it
// NOT RUN (exit 3 at the script), with no record and no text to post, and the marker says what the script reads: the rate and the
// context. A close that failed may have left a job, which is a failure, never a NOT RUN.
func TestATunnelRowWhoseLongStreamDoesNotFitEndsNotRun(t *testing.T) {
	long := &LongTooShortError{Rate: 300, MaxModelLen: 32768, Fit: 107 * time.Second, Floor: 2 * time.Minute}
	for _, name := range []string{"tunnel-hour-athena", "tunnel-hour-helios"} {
		row := rowOf(name)
		for _, c := range []struct {
			close      string
			closeErr   error
			wantNotRun bool
		}{
			{"closes cleanly", nil, true},
			{"fails to close", errors.New("scancel did not answer"), false},
		} {
			t.Run(name+"/"+c.close, func(t *testing.T) {
				cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), FrontLabel: testFrontLabel}
				rec, err := NewRecorder(cfg.Out)
				if err != nil {
					t.Fatal(err)
				}
				failures, err := Run(context.Background(), cfg, notStarted{failedSteps: failedSteps{err: long}, closeErr: c.closeErr}, rec, t.Logf)
				if len(failures) != 0 || err == nil {
					t.Fatalf("Run = %v, %v; want no verdict and an error", failures, err)
				}
				if got := NotRun(row, err); got != c.wantNotRun {
					t.Errorf("NotRun(%v) = %t, want %t", err, got, c.wantNotRun)
				}
				for _, f := range []string{RecordFile, PostsFile} {
					if _, serr := os.Stat(filepath.Join(cfg.Out, f)); serr == nil {
						t.Errorf("a run that did not start the hour wrote %s", f)
					}
				}
				if !c.wantNotRun {
					return
				}
				want := name + ": the deployment's max-model-len does not hold a long stream of the length the hour needs at the engine's measured rate " +
					"(at 300 tokens/s with max-model-len 32768 the stream that fits is 1m47s, under the floor of 2m0s); the hour was not started; this run's jobs were cancelled by tag\n"
				if got := NotRunNote(row, 10*time.Minute, err); got != want {
					t.Errorf("the marker is\n%q\nwant\n%q", got, want)
				}
			})
		}
	}
}

// The bare sentinel is a NOT RUN on every row (the rule reads no property of the row), and the other too-long cases are untouched: the
// handover's lead keeps its own marker, and a request that does not fit, where nothing sizes it, stays a failure.
func TestLongTooShortIsNotRunOnEveryRowWithItsOwnMarker(t *testing.T) {
	for _, r := range allRows(t) {
		for name, err := range map[string]error{
			"the bare sentinel": ErrLongTooShort,
			"with its counts":   &LongTooShortError{Rate: 300, MaxModelLen: 32768, Fit: 107 * time.Second, Floor: 2 * time.Minute},
			"wrapped":           errors.Join(errors.New("the scenario"), ErrLongTooShort),
		} {
			if !NotRun(r, err) {
				t.Errorf("%s: NotRun(%s) = false, want true", r.Name, name)
			}
		}
	}
	bare := NotRunNote(rowOf("tunnel-hour-athena"), time.Minute, ErrLongTooShort)
	if strings.Contains(bare, "(") || !strings.HasSuffix(bare, "; the hour was not started; this run's jobs were cancelled by tag\n") || strings.Contains(bare, "long enough to tell a cut from an end") {
		t.Errorf("the bare sentinel's marker is %q: its own words, with no counts to name", bare)
	}
	if lead := NotRunNote(rowOf("alpha-athena-handover"), time.Minute, ErrLeadTooShort); strings.Contains(lead, "does not hold a long stream") {
		t.Errorf("the handover's lead marker %q carries the long stream's words", lead)
	}
}

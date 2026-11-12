package alpha

// The lead of a handover row's streams (issue #447; handoverlead.go). Every expected value below is typed from the science
// agent's ruling (a) on #447 (lead = min(MaxRequestDuration/3, (max-model-len - 512) / rate / 2), a floor of 30 s) worked by
// hand, never computed by FitLead or by a formula that copies it.

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The incident, 12 Nov 2026: alpha-athena-now-nooverlap on Athena's A100 stopped before its streams opened: 199 tokens a
// second, max-model-len 32768, a 5-minute deadline. (32768 - 512) / 199 / 2 = 81.04 s, which is 81 s once rounded down; the
// deadline's third, 100 s, is the longer. This is the test a mutant that goes back to MaxRequestDuration/3 fails (100 s).
func TestFitLeadAtAnA100sRateOnA32kContextIs81Seconds(t *testing.T) {
	got, err := FitLead(5*time.Minute, 32768, 199)
	if err != nil {
		t.Fatalf("FitLead(5m, 32768, 199) = error %v, want 81 s: the row runs", err)
	}
	if want := 81 * time.Second; got != want {
		t.Errorf("FitLead(5m, 32768, 199) = %s, want %s", got, want)
	}
}

func TestFitLeadIsTheSmallerOfTheDeadlinesThirdAndWhatFits(t *testing.T) {
	for _, c := range []struct {
		name        string
		maxRequest  time.Duration
		maxModelLen int
		rate        float64
		want        time.Duration
	}{
		// A slow engine: what fits is 806 s, so the deadline's third, 100 s, is the lead.
		{"a slow rate", 5 * time.Minute, 32768, 20, 100 * time.Second},
		// The issue's threshold: 161 tokens a second is the rate at which 32256 tokens fill 200 s (the deadline's third twice).
		// 32256 / 161 / 2 = 100.17 s: the deadline's third is still the lead, to the second.
		{"just under the issue's threshold", 5 * time.Minute, 32768, 161, 100 * time.Second},
		// 32256 / 162 / 2 = 99.55 s, rounded down to 99 s.
		{"just over it", 5 * time.Minute, 32768, 162, 99 * time.Second},
		// The context's reserve is 512, not 256 or 1024: (8512 - 512) / 100 / 2 = 40 s exactly, where a reserve of 256 gives 41 s.
		{"the prompt's reserve", 5 * time.Minute, 8512, 100, 40 * time.Second},
		// The floor itself: (30512 - 512) / 500 / 2 = 30 s exactly runs.
		{"exactly the floor", 5 * time.Minute, 30512, 500, 30 * time.Second},
		// A deadline whose third is exactly the floor runs with it: 90 s / 3 = 30 s (the stand-ins' deadline), where (32768 - 512) / 100 / 2 = 161 s fits.
		{"a deadline whose third is exactly the floor", 90 * time.Second, 32768, 100, 30 * time.Second},
		// A rate so slow that the seconds that fit do not fit a Duration: the deadline's third.
		{"a vanishing rate", 5 * time.Minute, 32768, 1e-12, 100 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitLead(c.maxRequest, c.maxModelLen, c.rate)
			if err != nil {
				t.Fatalf("FitLead(%s, %d, %v) = error %v, want %s", c.maxRequest, c.maxModelLen, c.rate, err, c.want)
			}
			if got != c.want {
				t.Errorf("FitLead(%s, %d, %v) = %s, want %s", c.maxRequest, c.maxModelLen, c.rate, got, c.want)
			}
			// A stream of two leads at that rate fits the context less the reserve: the stream the row opens is one tokensFor
			// would size without error.
			if n := int(c.rate * 2 * got.Seconds()); n+512 > c.maxModelLen {
				t.Errorf("a stream of %d tokens (two leads of %s at %v tokens/s) does not fit max-model-len %d with 512 for the prompt", n, got, c.rate, c.maxModelLen)
			}
		})
	}
}

// A fit under the floor is NOT RUN, never a weaker pass, whatever the deadline.
func TestFitLeadUnderTheFloorIsLeadTooShort(t *testing.T) {
	for _, c := range []struct {
		name        string
		maxRequest  time.Duration
		maxModelLen int
		rate        float64
		wantFit     time.Duration
	}{
		// 32256 / 600 / 2 = 26.88 s, rounded down to 26 s.
		{"a rate of 600 on a 32k context", 5 * time.Minute, 32768, 600, 26 * time.Second},
		// One token a second over the floor's rate: 30000 / 501 / 2 = 29.94 s, rounded down to 29 s.
		{"one token a second past the floor", 5 * time.Minute, 30512, 501, 29 * time.Second},
		// The stand-ins' context, 4096, at the A100's rate: 3584 / 199 / 2 = 9.0 s.
		{"a 4096 context at the A100's rate", 5 * time.Minute, 4096, 199, 9 * time.Second},
		// A context that is all reserve has no room for a stream.
		{"a context of the reserve alone", 5 * time.Minute, 512, 100, 0},
		{"a context under the reserve", 5 * time.Minute, 100, 100, 0},
		// The floor judges the FINAL lead, so it is the deadline's third that is named when that is the shorter term: a deadline of 45 s
		// has a third of 15 s, where (32768 - 512) / 600 / 2 = 26 s fits (the science agent's ruling (a) on #451, 12 Nov 2026).
		{"a short deadline on a fast rate", 45 * time.Second, 32768, 600, 15 * time.Second},
		// A deadline whose third is under the floor is NOT RUN on its own, whatever the context fits: (32768 - 512) / 100 / 2 = 161 s.
		{"a short deadline on a slow rate", 45 * time.Second, 32768, 100, 15 * time.Second},
		// 89 s / 3 = 29.67 s, named in whole seconds, as the marker prints it.
		{"a deadline whose third is a hair under the floor", 89 * time.Second, 32768, 100, 29 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitLead(c.maxRequest, c.maxModelLen, c.rate)
			if !errors.Is(err, ErrLeadTooShort) {
				t.Fatalf("FitLead(%s, %d, %v) = %s, %v; want ErrLeadTooShort", c.maxRequest, c.maxModelLen, c.rate, got, err)
			}
			var lt *LeadTooShortError
			if !errors.As(err, &lt) || lt.Rate != c.rate || lt.MaxModelLen != c.maxModelLen || lt.Fit != c.wantFit {
				t.Errorf("the error is %+v, want the rate %v, the context %d and a fit of %s", lt, c.rate, c.maxModelLen, c.wantFit)
			}
			if got != 0 {
				t.Errorf("a lead of %s came with the error", got)
			}
		})
	}
}

// The error names the rate and the context: the message a run stops on.
func TestLeadTooShortErrorNamesTheRateAndTheContext(t *testing.T) {
	_, err := FitLead(5*time.Minute, 32768, 600)
	want := "no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate " +
		"(at 600 tokens/s with max-model-len 32768 the lead that fits is 26s, under the floor of 30s)"
	if err == nil || err.Error() != want {
		t.Errorf("the error is %v, want %q", err, want)
	}
}

// A rate that is not a positive finite number measured nothing a lead can be sized from: a plain failure, never a NOT RUN
// (the run did not find the deployment too small), and a deadline that is not positive is no row's.
func TestFitLeadRefusesInputsThatMeasureNothing(t *testing.T) {
	for _, c := range []struct {
		name       string
		maxRequest time.Duration
		rate       float64
	}{
		{"a zero rate", 5 * time.Minute, 0},
		{"a negative rate", 5 * time.Minute, -3},
		{"a rate that is not a number", 5 * time.Minute, math.NaN()},
		{"an infinite rate", 5 * time.Minute, math.Inf(1)},
		{"no deadline", 0, 199},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitLead(c.maxRequest, 32768, c.rate)
			if err == nil || errors.Is(err, ErrLeadTooShort) || got != 0 {
				t.Errorf("FitLead = %s, %v; want a plain error and no lead", got, err)
			}
		})
	}
}

// The live steps read the deployment's own max-model-len and the row's deadline: streamLead is FitLead over both, with
// the engine's measured rate. A deployment with no max-model-len flag has vLLM's 4096 here, as tokensFor has.
func TestStreamLeadReadsTheDeploymentsContextAndTheRowsDeadline(t *testing.T) {
	md := func(flags ...v1alpha1.EngineFlag) *v1alpha1.ModelDeployment {
		return &v1alpha1.ModelDeployment{Spec: v1alpha1.ModelDeploymentSpec{Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{Flags: flags}}}}
	}
	ctx32k := v1alpha1.EngineFlag{Name: "max-model-len", Value: "32768"}
	for _, c := range []struct {
		name     string
		deadline time.Duration
		md       *v1alpha1.ModelDeployment
		rate     float64
		want     time.Duration
		notRun   bool
	}{
		{"row B's deployment at the A100's rate", 5 * time.Minute, md(ctx32k), 199, 81 * time.Second, false},
		{"the same deployment on a slower engine", 5 * time.Minute, md(ctx32k), 100, 100 * time.Second, false},
		{"the stand-ins' deadline, a 50 tokens a second engine", 90 * time.Second, md(v1alpha1.EngineFlag{Name: "max-model-len", Value: "4096"}), 50, 30 * time.Second, false},
		{"a flag among others", 5 * time.Minute, md(v1alpha1.EngineFlag{Name: "dtype", Value: "bfloat16"}, ctx32k), 199, 81 * time.Second, false},
		{"no max-model-len flag: 4096", 5 * time.Minute, md(), 199, 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &liveRun{tm: Timing{MaxRequestDuration: c.deadline}, w: world{md: c.md}}
			got, err := r.streamLead(c.rate)
			if errors.Is(err, ErrLeadTooShort) != c.notRun || (err == nil) == c.notRun {
				t.Fatalf("streamLead(%v) = %s, %v; want NOT RUN %t", c.rate, got, err, c.notRun)
			}
			if got != c.want {
				t.Errorf("streamLead(%v) = %s, want %s", c.rate, got, c.want)
			}
		})
	}
}

// Every timing that uses the lead still holds at a shorter one (issue #447). The three leads are the A100's 81 s, the
// deadline's third, 100 s (the lead before this change), and the floor, 30 s. The offsets are written out from what the
// steps do: the handover stops waiting for its replacement half a lead before its streams open (untilBefore waits for them,
// a lead before the drain start); a stream of two leads, sized at the measured rate, ends a lead after it. The relations
// are checked against the deadline and the drain window written out (5 minutes, and 7 minutes, the shortest of the rows'
// drain windows: plgrid-now's, DrainWindow(5m, 2m)), not against the lead.
func TestEveryTimingThatUsesTheLeadHoldsAtTheShorterLead(t *testing.T) {
	const (
		deadline    = 5 * time.Minute
		drainWindow = 7 * time.Minute
	)
	for _, c := range []struct {
		name                string
		lead                time.Duration
		waitEnds, open, end time.Duration
	}{
		{"the A100's lead", 81 * time.Second, -121500 * time.Millisecond, -81 * time.Second, 81 * time.Second},
		{"the deadline's third", 100 * time.Second, -150 * time.Second, -100 * time.Second, 100 * time.Second},
		{"the floor", 30 * time.Second, -45 * time.Second, -30 * time.Second, 30 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := LeadTimesFor(c.lead)
			if got.SuccessorWaitEnds != c.waitEnds || got.StreamsOpen != c.open || got.StreamsEnd != c.end {
				t.Fatalf("LeadTimesFor(%s) = %+v, want the successor wait ending at the drain start%+v, the streams opening at%+v and ending at%+v",
					c.lead, got, c.waitEnds, c.open, c.end)
			}
			switch {
			case !(got.StreamsOpen < 0 && got.StreamsEnd > 0):
				t.Errorf("the stream is not in flight at the drain start and ending after it: opens at %s, ends at %s from it", got.StreamsOpen, got.StreamsEnd)
			case -got.StreamsOpen < LeadFloor || got.StreamsEnd < LeadFloor:
				t.Errorf("the stream straddles the drain start by less than the floor: %s before, %s after", -got.StreamsOpen, got.StreamsEnd)
			case got.StreamsEnd-got.StreamsOpen >= deadline:
				t.Errorf("the stream lasts %s, not within the %s deadline", got.StreamsEnd-got.StreamsOpen, deadline)
			case got.StreamsEnd >= drainWindow:
				t.Errorf("the stream ends %s after the drain start, not within the %s drain window", got.StreamsEnd, drainWindow)
			case got.SuccessorWaitEnds >= got.StreamsOpen:
				t.Errorf("the successor wait ends at%+v, not before the streams open at%+v", got.SuccessorWaitEnds, got.StreamsOpen)
			}
		})
	}
}

// NOT RUN on every row, through Run: the three rows that open a stream across a drain start stop on ErrLeadTooShort before
// any stream and end NOT RUN (exit 3 at the script), with no record and no text to post, and the marker says what the
// script reads: the rate and the context. A close that failed may have left a job, which is a failure, never a NOT RUN.
func TestAHandoverRowWhoseLeadDoesNotFitEndsNotRun(t *testing.T) {
	lead := &LeadTooShortError{Rate: 600, MaxModelLen: 32768, Fit: 26 * time.Second}
	for _, name := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "alpha-athena-now-nooverlap"} {
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
				failures, err := Run(context.Background(), cfg, notStarted{failedSteps: failedSteps{err: lead}, closeErr: c.closeErr}, rec, t.Logf)
				if len(failures) != 0 || err == nil {
					t.Fatalf("Run = %v, %v; want no verdict and an error", failures, err)
				}
				if got := NotRun(row, err); got != c.wantNotRun {
					t.Errorf("NotRun(%v) = %t, want %t", err, got, c.wantNotRun)
				}
				for _, f := range []string{RecordFile, PostsFile} {
					if _, serr := os.Stat(filepath.Join(cfg.Out, f)); serr == nil {
						t.Errorf("a run that opened no stream wrote %s", f)
					}
				}
				if !c.wantNotRun {
					return
				}
				want := name + ": no stream across the drain start that is long enough to tell a cut from an end fits the deployment's max-model-len at the engine's measured rate " +
					"(at 600 tokens/s with max-model-len 32768 the lead that fits is 26s, under the floor of 30s); the run opened no stream across the drain start; this run's jobs were cancelled by tag\n"
				if got := NotRunNote(row, 10*time.Minute, err); got != want {
					t.Errorf("the marker is\n%q\nwant\n%q", got, want)
				}
			})
		}
	}
}

// The bare sentinel (an error that carries no counts) is a NOT RUN too, on every row: the rule reads no property of the row.
// The too-long request of a row that does not size a lead (the tunnel hour's) stays a failure.
func TestLeadTooShortIsNotRunOnEveryRowAndTooLongARequestStaysAFailure(t *testing.T) {
	for _, r := range allRows(t) {
		for name, err := range map[string]error{
			"the bare sentinel": ErrLeadTooShort,
			"with its counts":   &LeadTooShortError{Rate: 600, MaxModelLen: 32768, Fit: 26 * time.Second},
			"wrapped":           errors.Join(errors.New("the scenario"), ErrLeadTooShort),
		} {
			if !NotRun(r, err) {
				t.Errorf("%s: NotRun(%s) = false, want true", r.Name, name)
			}
		}
		if NotRun(r, ErrRequestTooLong) {
			t.Errorf("%s: NotRun(ErrRequestTooLong) = true: a request that does not fit, where no lead is sized, is a failure", r.Name)
		}
	}
	if got := NotRunNote(rowOf("alpha-athena-handover"), time.Minute, ErrLeadTooShort); strings.Contains(got, "(") || !strings.HasSuffix(got, "; the run opened no stream across the drain start; this run's jobs were cancelled by tag\n") {
		t.Errorf("the bare sentinel's marker is %q: its words, with no counts to name", got)
	}
}

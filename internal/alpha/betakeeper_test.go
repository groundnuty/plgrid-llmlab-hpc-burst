package alpha

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
)

// The keeper row's logic (betakeeper.go), each check with its words written out and its boundary.

func TestTheKeeperRowIsRowAsHoldOfThePostedSlot(t *testing.T) {
	r, a := BetaKeeperRow(), NowServeRow()
	if r.Name != "beta-athena-now-keeper" || r.PostsTo != "#522" {
		t.Errorf("the row is %q posting to %q, want beta-athena-now-keeper posting to #522", r.Name, r.PostsTo)
	}
	b := r
	b.Name, b.PostsTo = a.Name, a.PostsTo
	if b != a {
		t.Errorf("the row differs from row A beyond its name and tracker:\n%+v\n%+v", r, a)
	}
}

// The keeper's epoch base is the keeper's own: this file writes it out, since it may not import an internal package that is test support, and
// it must stay what relaykeeper says.
func TestTheKeeperEpochBaseIsRelaykeepers(t *testing.T) {
	if KeeperEpochBase != relaykeeper.EpochBase {
		t.Errorf("KeeperEpochBase = %d, relaykeeper.EpochBase = %d", KeeperEpochBase, relaykeeper.EpochBase)
	}
}

func TestTheKeeperWindowIsItsThreeParts(t *testing.T) {
	// Written out from the ruling (b) on #543, not derived: the window is 10 s to the kill, 60 s for the keeper's outcome and 30 s of margin, 100 s;
	// the verdict's bar is 2 minutes; the stream is planned at the window and 30 s more.
	if KeeperKillAfter != 10*time.Second || KeeperSizingOutcome != 60*time.Second || KeeperMargin != 30*time.Second || KeeperWindow != 100*time.Second {
		t.Errorf("the window is %s: the kill's offset %s, the outcome's term %s and the margin %s; want 100 s, 10 s, 60 s and 30 s", KeeperWindow, KeeperKillAfter, KeeperSizingOutcome, KeeperMargin)
	}
	if KeeperOutcomeBound != 2*time.Minute || KeeperStreamExtra != 30*time.Second {
		t.Errorf("the verdict's bar is %s and the stream's extra %s; want 2 minutes and 30 s", KeeperOutcomeBound, KeeperStreamExtra)
	}
	// The outcome's term is a margin over the 35 s the loopback rehearsal measured.
	if KeeperSizingOutcome <= 35*time.Second {
		t.Errorf("the window's outcome term %s is not over the 35 s the rehearsal measured", KeeperSizingOutcome)
	}
	// The loopback rehearsal measured the outcome at about 35 s after the SIGKILL; the bound has room for it more than three times over.
	if KeeperOutcomeBound < 3*35*time.Second {
		t.Errorf("the outcome's bound %s is under three times the 35 s the rehearsal measured", KeeperOutcomeBound)
	}
}

func TestFitKeeperSizesTheStreamToTheContextAndTheWindow(t *testing.T) {
	tm := BetaKeeperRow().Clock()
	const rate = 100.0
	for _, c := range []struct {
		name        string
		maxModelLen int
		rate        float64
		want        time.Duration
		notRun      bool
	}{
		// 100 s at 100 tokens/s is 10,000 tokens: a context of 10,000 + PromptReserve holds exactly the window.
		{"exactly the window", 10000 + PromptReserve, rate, KeeperWindow, false},
		{"one token under the window", 10000 + PromptReserve - 1, rate, 0, true},
		{"a context that holds a minute", 6000 + PromptReserve, rate, 0, true},
		{"between the window and the plan", 11500 + PromptReserve, rate, 115 * time.Second, false},
		// The plan is the window and 30 s more, 130 s: a larger context is not asked for whole.
		{"exactly the plan", 13000 + PromptReserve, rate, 130 * time.Second, false},
		{"a larger context", 1 << 20, rate, 130 * time.Second, false},
		// Athena's slot (#543's review): 32768 at its measured 199 tokens/s fits 162 s, planned at 130 s, about 26,000 tokens; and a rate a few
		// tokens a second higher, which the 160 s floor made NOT RUN, is planned the same.
		{"Athena at 199 tokens/s", 32768, 199, 130 * time.Second, false},
		{"Athena at 203 tokens/s", 32768, 203, 130 * time.Second, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FitKeeper(tm, c.maxModelLen, c.rate)
			if c.notRun {
				var k *KeeperStreamTooShortError
				if !errors.Is(err, ErrKeeperStreamTooShort) || !errors.As(err, &k) || k.Floor != KeeperWindow || k.Fit >= KeeperWindow || got != 0 {
					t.Fatalf("FitKeeper = %s, %v; want a stream too short for the window of %s", got, err, KeeperWindow)
				}
				if !NotRun(BetaKeeperRow(), err) {
					t.Error("the error does not end the run NOT RUN")
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("FitKeeper = %s, %v; want %s", got, err, c.want)
			}
		})
	}
	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := FitKeeper(tm, 32768, bad); err == nil || errors.Is(err, ErrKeeperStreamTooShort) {
			t.Errorf("a rate of %v: err = %v, want a plain error that is no NOT RUN", bad, err)
		}
	}
}

func goodKeeperObs(row Row) KeeperObs {
	rel := dryKeeperRelays()
	opened := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	killed := opened.Add(KeeperKillAfter)
	outcome := killed.Add(35 * time.Second)
	return KeeperObs{
		Tags: []string{"tag-1"}, Deployment: row.Plan(), ReadyAt: opened.Add(-time.Minute), DrainStart: opened.Add(30 * time.Minute), Rate: 40, Asked: 6400,
		Before: rel, After: rel, KilledAt: killed, OutcomeAt: outcome, ControllerReplaced: true, LeaseBefore: 3, LeaseAfter: 4, HolderChanged: true, FenceAfter: 4,
		AgentReported: true, AgentSame: true, Planned: 160 * time.Second, Stream: Stream{ID: "k", Opened: opened, Ended: outcome.Add(KeeperMargin + time.Second), Complete: true, Tokens: 6400},
		FrontCuts: 0, Responses: 3, Attributed: 3,
	}
}

func TestEachKeeperCheckNamesItsOwnFailure(t *testing.T) {
	row := BetaKeeperRow()
	// The checks apart from the cluster's record of the jobs, which the dry runs judge through Run.
	judge := func(o KeeperObs) []string {
		var f []string
		f = append(f, relaysChecks("before the restart", o.Before)...)
		f = append(f, relaysChecks("after the restart", o.After)...)
		f = append(f, controllerChecks(o)...)
		f = append(f, sameRelayChecks(o.Before, o.After)...)
		f = append(f, agentChecks(o)...)
		f = append(f, keeperStreamChecks(row, o)...)
		return f
	}
	if f := judge(goodKeeperObs(row)); len(f) != 0 {
		t.Fatalf("a good run fails: %q", f)
	}
	for _, c := range []struct {
		name string
		mut  func(o *KeeperObs)
		want string
	}{
		{"relays unread before", func(o *KeeperObs) { o.Before.Read = false }, "the keeper's relays were not read before the restart"},
		{"relays unread after", func(o *KeeperObs) { o.After.Read = false }, "the keeper's relays were not read after the restart"},
		{"E under 2^31 by one", func(o *KeeperObs) {
			o.Before.KeeperEpoch = KeeperEpochBase - 1
			o.After.KeeperEpoch = KeeperEpochBase - 1
		}, "the keeper's E before the restart is below 2^31"},
		{"controller relays before", func(o *KeeperObs) { o.Before.ControllerOwn = 1 }, "the controller runs relays of its own before the restart: in keeper mode they are the keeper's"},
		{"controller relays after", func(o *KeeperObs) { o.After.ControllerOwn = 1 }, "the controller runs relays of its own after the restart: in keeper mode they are the keeper's"},
		{"control relay replaced", func(o *KeeperObs) { o.After.Control.PID++ }, "the control relay's ssh is not the process it was before the restart: a relay was replaced"},
		{"control relay start time", func(o *KeeperObs) { o.After.Control.StartTicks++ }, "the control relay's ssh is not the process it was before the restart: a relay was replaced"},
		{"serving relay replaced", func(o *KeeperObs) { o.After.Serving.PID++ }, "the serving relay's ssh is not the process it was before the restart: a relay was replaced"},
		{"serving relay epoch", func(o *KeeperObs) { o.After.Serving.Epoch++ }, "the serving relay's E or N changed across the restart"},
		{"control relay seq", func(o *KeeperObs) { o.After.Control.Seq++ }, "the control relay's E or N changed across the restart"},
		{"keeper's E", func(o *KeeperObs) { o.After.KeeperEpoch++ }, "the keeper's E changed across the controller's restart"},
		{"never killed", func(o *KeeperObs) { o.KilledAt = time.Time{} }, "the controller was not killed"},
		{"outcome unseen", func(o *KeeperObs) { o.OutcomeAt = time.Time{} }, "the outcome of the controller's restart was not seen within 2m0s"},
		{"outcome one nanosecond past the bound", func(o *KeeperObs) { o.OutcomeAt = o.KilledAt.Add(KeeperOutcomeBound + time.Nanosecond) }, "the outcome of the controller's restart was not seen within 2m0s"},
		{"outcome before the kill", func(o *KeeperObs) { o.OutcomeAt = o.KilledAt }, "the outcome was seen before the kill"},
		{"same controller", func(o *KeeperObs) { o.ControllerReplaced = false }, "the controller that leads after the kill is the one that was killed"},
		{"same holder", func(o *KeeperObs) { o.HolderChanged = false }, "the Provider's Lease has the holder it had before the kill"},
		{"lease not risen", func(o *KeeperObs) { o.LeaseAfter = o.LeaseBefore }, "the Provider's Lease's transitions did not rise by one across the restart"},
		{"lease risen by two", func(o *KeeperObs) { o.LeaseAfter = o.LeaseBefore + 2 }, "the Provider's Lease's transitions did not rise by one across the restart"},
		{"fence behind", func(o *KeeperObs) { o.FenceAfter = o.LeaseAfter - 1 }, "the keeper's fence is not the new leader's epoch: the keeper did not accept it"},
		{"fence ahead", func(o *KeeperObs) { o.FenceAfter = o.LeaseAfter + 1 }, "the keeper's fence is not the new leader's epoch: the keeper did not accept it"},
		{"agent unreported", func(o *KeeperObs) { o.AgentReported = false }, "the new controller holds no report of the agent"},
		{"agent registration", func(o *KeeperObs) { o.AgentSame = false }, "the agent's registration changed across the restart"},
		{"unreachable", func(o *KeeperObs) { o.Unreachable = true }, "the Instance was judged unreachable across the restart"},
		{"restarted", func(o *KeeperObs) { o.RestartsAfter = o.RestartsBefore + 1 }, "the Instance restarted across the controller's restart"},
		{"stream cut", func(o *KeeperObs) { o.Stream.CutBy = "tunnel"; o.Stream.Complete = false }, "the stream across the restart was cut"},
		{"stream incomplete", func(o *KeeperObs) { o.Stream.Complete = false }, "the stream across the restart did not finish by its completion rule"},
		{"one token short", func(o *KeeperObs) { o.Stream.Tokens = o.Asked - 1 }, "the stream across the restart did not deliver exactly the tokens asked for"},
		{"one token over", func(o *KeeperObs) { o.Stream.Tokens = o.Asked + 1 }, "the stream across the restart did not deliver exactly the tokens asked for"},
		{"none asked", func(o *KeeperObs) { o.Asked = 0; o.Stream.Tokens = 0 }, "the stream across the restart did not deliver exactly the tokens asked for"},
		{"past the deadline", func(o *KeeperObs) { o.Stream.Ended = o.Stream.Opened.Add(row.Clock().MaxRequestDuration + time.Second) }, "the stream across the restart did not end within the deployment's deadline"},
		{"opened at the kill", func(o *KeeperObs) { o.Stream.Opened = o.KilledAt }, "the stream was not open before the controller was killed"},
		{"a front cut", func(o *KeeperObs) { o.FrontCuts = 1 }, "the front counted a cut during the restart"},
		{"cuts unread", func(o *KeeperObs) { o.FrontCuts = -1 }, "the front's cuts could not be counted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodKeeperObs(row)
			c.mut(&o)
			if f := judge(o); !slices.Contains(f, c.want) {
				t.Errorf("the checks say %q, want them to hold %q", f, c.want)
			}
		})
	}
	// Relays that were not read are reported by their own check and compared by none: a missing reading is not "a relay was replaced".
	for _, unread := range []func(o *KeeperObs){func(o *KeeperObs) { o.Before.Read = false }, func(o *KeeperObs) { o.After.Read = false }} {
		o := goodKeeperObs(row)
		unread(&o)
		o.After.Control.PID += 7
		if f := sameRelayChecks(o.Before, o.After); len(f) != 0 {
			t.Errorf("relays that were not read are compared: %q", f)
		}
	}
	// The outcome's bound is a boundary too: an outcome at exactly the bound passes the controller's check.
	{
		o := goodKeeperObs(row)
		o.OutcomeAt = o.KilledAt.Add(KeeperOutcomeBound)
		if f := controllerChecks(o); len(f) != 0 {
			t.Errorf("an outcome at exactly the bound fails: %q", f)
		}
	}
	// A stream that does not outlast the outcome by the margin fails no check: it is the run's NOT RUN (keeperEndedFirst), never a failure.
	o := goodKeeperObs(row)
	o.Stream.Ended = o.OutcomeAt.Add(KeeperMargin - time.Nanosecond)
	if f := judge(o); len(f) != 0 {
		t.Errorf("a stream that ends inside the margin fails: %q", f)
	}
}

// keeperEndedFirst (the ruling (b) on #543): the stream must outlast the keeper's outcome by the margin, else the run is NOT RUN, with the times
// from the stream's first token; at exactly the margin it is not; an outcome or a stream end not seen is the verdict's to fail, not this.
func TestAStreamThatDoesNotOutlastTheOutcomeByTheMarginIsNotRun(t *testing.T) {
	row := BetaKeeperRow()
	for _, c := range []struct {
		name          string
		outcome, end  time.Duration // after the stream opened; the kill is at 10 s
		notRun        bool
		wantOutcome   string
		wantEndedSays string
	}{
		{"the ruling's case: an outcome 90 s after the kill, a 100 s stream ended first", 100 * time.Second, 100 * time.Second, true, "1m40s", "1m40s"},
		{"a stream ended before the outcome", 100 * time.Second, 90 * time.Second, true, "1m40s", "1m30s"},
		{"an outcome 90 s after the kill, the 130 s plan: exactly the margin", 100 * time.Second, 130 * time.Second, false, "", ""},
		{"an outcome 100 s after the kill, the 130 s plan: inside the margin", 110 * time.Second, 130 * time.Second, true, "1m50s", "2m10s"},
		{"one nanosecond inside the margin", 100 * time.Second, 130*time.Second - time.Nanosecond, true, "1m40s", "2m10s"},
		{"an outcome 60 s after the kill, the 130 s plan", 70 * time.Second, 130 * time.Second, false, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodKeeperObs(row)
			o.KilledAt = o.Stream.Opened.Add(KeeperKillAfter)
			o.OutcomeAt, o.Stream.Ended = o.Stream.Opened.Add(c.outcome), o.Stream.Opened.Add(c.end)
			err := keeperEndedFirst(o)
			if (err != nil) != c.notRun {
				t.Fatalf("keeperEndedFirst = %v, want NOT RUN %t", err, c.notRun)
			}
			if !c.notRun {
				return
			}
			var e *KeeperStreamEndedFirstError
			if !errors.Is(err, ErrKeeperStreamEndedFirst) || !errors.As(err, &e) || !NotRun(row, err) {
				t.Fatalf("err = %v, want a KeeperStreamEndedFirstError that is NOT RUN", err)
			}
			if want := "the outcome at " + c.wantOutcome + " after the stream opened, the stream's end at " + c.wantEndedSays + ", the margin 30s"; e.counts() != want {
				t.Errorf("counts = %q, want %q", e.counts(), want)
			}
		})
	}
	for _, unseen := range []func(o *KeeperObs){func(o *KeeperObs) { o.OutcomeAt = time.Time{} }, func(o *KeeperObs) { o.Stream.Ended = time.Time{} }} {
		o := goodKeeperObs(row)
		o.Stream.Ended = o.OutcomeAt
		unseen(&o)
		if err := keeperEndedFirst(o); err != nil {
			t.Errorf("a time not seen is the run's NOT RUN: %v", err)
		}
	}
}

// Through Run, a stream that did not outlast the outcome by the margin is NOT RUN with nothing failed: the record says "not run", the text to post
// is headed NOT RUN with the times, and the marker's note says them. A cut stream beside it is a failure, which wins (betaKeeperCases).
func TestAKeeperStreamThatEndedBeforeTheOutcomeIsNotRunThroughRun(t *testing.T) {
	row := BetaKeeperRow()
	for _, c := range []struct{ fault, counts string }{
		{"ended-before-outcome", "the outcome at 1m40s after the stream opened, the stream's end at 1m40s, the margin 30s"},
		{"outcome-in-the-margin", "the outcome at 1m50s after the stream opened, the stream's end at 2m10s, the margin 30s"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betakeeper"), rec, t.Logf)
			if !errors.Is(err, ErrKeeperStreamEndedFirst) || !NotRun(row, err) || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want the NOT RUN of a stream that ended before the outcome", failures, err)
			}
			if v := keeperRecordVerdict(t, cfg.Out); v != "not run" {
				t.Errorf("the record's verdict is %q, want not run", v)
			}
			post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			head := "beta-athena-now-keeper: NOT RUN (posts to #522): the stream ended before the keeper's outcome and its margin (" + c.counts + "); the controller was killed and the run's other checks passed\n"
			if !strings.HasPrefix(string(post), head) {
				t.Errorf("the text to post is not headed\n%q:\n%s", head, post)
			}
			note := NotRunNote(row, cfg.PendingMax, err)
			want := "beta-athena-now-keeper: the stream ended before the keeper's outcome and its margin (" + c.counts + "); the controller was killed and the run's other checks passed; this run's jobs were ended with its deployment\n"
			if note != want {
				t.Errorf("the marker's note is\n%q\nwant\n%q", note, want)
			}
		})
	}
}

// The text to post is counts and times: no PID, no start time, no job ID.
func TestTheKeeperPostSaysWhatWasSeenAndNamesNoProcess(t *testing.T) {
	row := BetaKeeperRow()
	o := goodKeeperObs(row)
	post := betaKeeperPost(row, o, nil)
	for _, want := range []string{
		"beta-athena-now-keeper: PASS (posts to #522)",
		"a controller restart (SIGKILL through a pinned pidfd)",
		"the stream delivered 6400 tokens",
		"the front counted 0 cuts",
		"35s after the kill (the bar: within 2m0s)",
		"- the window: 1m40s (10s to the kill, 1m0s for the keeper's outcome, 30s of stream after it); the stream planned at 2m40s (the window and 30s more, capped at what the context holds), actual 1m16s\n",
		"- the keeper's outcome landed 45s after the stream opened and 31s before the stream's end (the stream must outlast it by 30s)\n",
		"the Lease's transitions 3 to 4",
		"control the same process at the same E and N, serving the same process at the same E and N",
		"restarts before and after: 0 and 0",
	} {
		if !strings.Contains(post, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, post)
		}
	}
	for _, leak := range []string{"4100", "4101", "5000", "5001"} {
		if strings.Contains(post, leak) {
			t.Errorf("the text to post names %s, a PID or a start time:\n%s", leak, post)
		}
	}
	o.After.Control.PID++
	if p := betaKeeperPost(row, o, []string{"x"}); !strings.Contains(p, "FAIL (posts to #522)") || !strings.Contains(p, "control NOT the same") {
		t.Errorf("a failed run's text lacks its verdict or the replaced relay:\n%s", p)
	}
}

// A context that cannot hold a stream for the window ends the run NOT RUN, naming the rate and the context and nothing else; one that holds
// exactly the window is no NOT RUN and no failure. No record and no text are written for the run that sent nothing.
func TestAKeeperStreamTheContextCannotHoldIsNotRun(t *testing.T) {
	row := BetaKeeperRow()
	cfg := Config{Row: row, Dry: true, DryFault: "window-too-short", Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betakeeper"), rec, t.Logf)
	var k *KeeperStreamTooShortError
	if !errors.Is(err, ErrKeeperStreamTooShort) || !errors.As(err, &k) || !NotRun(row, err) || len(failures) != 0 {
		t.Fatalf("failures %v, err %v; want the NOT RUN of a stream the context cannot hold for the window", failures, err)
	}
	note := NotRunNote(row, cfg.PendingMax, err)
	want := "beta-athena-now-keeper: the deployment's max-model-len does not hold a stream long enough to span a controller restart at the engine's measured rate (at 40 tokens/s with max-model-len 2912 the stream that fits is 1m0s, under the window of 1m40s); the controller was not killed; this run's jobs were cancelled by tag\n"
	if note != want {
		t.Errorf("the marker's note is\n%q\nwant\n%q", note, want)
	}
	for _, f := range []string{RecordFile, PostsFile} {
		if _, err := os.Stat(filepath.Join(cfg.Out, f)); err == nil {
			t.Errorf("%s was written for a run that sent nothing", f)
		}
	}
}

// A late outcome, past the 2-minute bar, after the stream had already ended is the bar's FAIL, never the NOT RUN of a stream that ended first:
// Run returns the NOT RUN only when nothing failed (#543's delta review). The fixture's stream ends at its 130 s plan, before an outcome 2 min and
// 1 s after the kill at 10 s.
func TestALateOutcomeAfterTheStreamEndedIsAFailureNotANotRun(t *testing.T) {
	if planEnd, outcome := KeeperWindow+KeeperStreamExtra, KeeperKillAfter+KeeperOutcomeBound+time.Second; !(planEnd < outcome) {
		t.Fatalf("the fixture's stream ends at %s, not before the outcome at %s: the case would not have the stream ended first", planEnd, outcome)
	}
	row := BetaKeeperRow()
	cfg := Config{Row: row, Dry: true, DryFault: "outcome-late-ended", Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "betakeeper"), rec, t.Logf)
	if err != nil || NotRun(row, err) || !slices.Contains(failures, "the outcome of the controller's restart was not seen within 2m0s") {
		t.Fatalf("failures %v, err %v; want the bar's failure and no NOT RUN", failures, err)
	}
	if v := keeperRecordVerdict(t, cfg.Out); v != "fail" {
		t.Errorf("the record's verdict is %q, want fail", v)
	}
	post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
	if !strings.HasPrefix(string(post), "beta-athena-now-keeper: FAIL (posts to #522)\n") || strings.Contains(string(post), "NOT RUN") {
		t.Errorf("the text to post is not headed FAIL, or says NOT RUN:\n%s", post)
	}
}

// The keeper row runs in host-dialed mode alone: the relay keeper runs the relays of that mode, and another mode has none to read.
func TestTheKeeperRowRefusesAnotherDialHomeMode(t *testing.T) {
	const row = BetaKeeperName
	run := func(mode string) (Config, error) {
		m := liveSettings(row)
		m[envProfile] = BetaKeeperRow().ProfileName() // the shipped profile the plgrid-now rows run under, which liveSettings does not name
		m[envMode] = mode
		if mode == "host-dialed" {
			delete(m, envAddr)
		}
		if mode == "ssh" {
			m[envKey] = "/private/key"
		}
		cfg, skip, err := ConfigFromEnv(env(m), row)
		if skip != "" {
			t.Fatalf("skip %q", skip)
		}
		return cfg, err
	}
	if cfg, err := run("host-dialed"); err != nil || cfg.DialHomeMode != "host-dialed" {
		t.Errorf("host-dialed: mode %q, err %v; want it accepted", cfg.DialHomeMode, err)
	}
	for _, mode := range []string{"direct", "ssh"} {
		if _, err := run(mode); err == nil || !strings.Contains(err.Error(), "must be host-dialed for "+BetaKeeperName) {
			t.Errorf("%s: err = %v, want a refusal that names the keeper row and host-dialed", mode, err)
		}
	}
	// Another row is not held to it.
	m := liveSettings("beta-athena-now-router")
	m[envProfile] = BetaRouterRow().ProfileName()
	if _, skip, err := ConfigFromEnv(env(m), "beta-athena-now-router"); skip != "" || err != nil {
		t.Errorf("the router row in direct mode: skip %q err %v, want it accepted", skip, err)
	}
}

// The keeper row's stack runs the relay keeper, and no other row's does.
func TestOnlyTheKeeperRowsStackRunsTheRelayKeeper(t *testing.T) {
	opts := hostOptionsFor(BetaKeeperName)
	if len(opts) != 1 || reflect.ValueOf(opts[0]).Pointer() != reflect.ValueOf(stack.RelayKeeper).Pointer() {
		t.Fatalf("the keeper row's host options are %d, want exactly stack.RelayKeeper", len(opts))
	}
	for _, name := range []string{BetaRouterName, BetaBulkName} {
		for _, o := range hostOptionsFor(name) {
			if reflect.ValueOf(o).Pointer() == reflect.ValueOf(stack.RelayKeeper).Pointer() {
				t.Errorf("%s runs the relay keeper", name)
			}
		}
	}
}

// keeperRecordVerdict is the verdict a run's record holds.
func keeperRecordVerdict(t *testing.T, out string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, RecordFile))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Verdict string `json:"verdict"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r.Verdict
}

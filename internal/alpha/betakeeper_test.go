package alpha

import (
	"context"
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
	// Written out, not derived: the window is 10 s to the kill, 2 minutes for the outcome and 30 s of margin.
	if KeeperKillAfter != 10*time.Second || KeeperOutcomeBound != 2*time.Minute || KeeperMargin != 30*time.Second || KeeperWindow != 160*time.Second {
		t.Errorf("the window is %s: the kill's offset %s, the outcome's bound %s and the margin %s; want 160 s, 10 s, 2 minutes and 30 s", KeeperWindow, KeeperKillAfter, KeeperOutcomeBound, KeeperMargin)
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
		// 160 s at 100 tokens/s is 16,000 tokens: a context of 16,000 + PromptReserve holds exactly the window.
		{"exactly the window", 16000 + PromptReserve, rate, KeeperWindow, false},
		{"one token under the window", 16000 + PromptReserve - 1, rate, 0, true},
		{"a context that holds a minute", 6000 + PromptReserve, rate, 0, true},
		{"the cap of the stream", 1 << 20, rate, tm.LongCap(), false},
		{"between the window and the cap", 20000 + PromptReserve, rate, 200 * time.Second, false},
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
		AgentReported: true, AgentSame: true, Stream: Stream{ID: "k", Opened: opened, Ended: outcome.Add(KeeperMargin + time.Second), Complete: true, Tokens: 6400},
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
		{"ends inside the margin", func(o *KeeperObs) { o.Stream.Ended = o.OutcomeAt.Add(KeeperMargin - time.Nanosecond) }, "the stream did not flow on for the margin after the outcome"},
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
	// The margin is a boundary: ending at exactly the margin passes.
	o := goodKeeperObs(row)
	o.Stream.Ended = o.OutcomeAt.Add(KeeperMargin)
	if f := judge(o); len(f) != 0 {
		t.Errorf("a stream that ends exactly at the margin fails: %q", f)
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
		"35s after the kill",
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
	want := "beta-athena-now-keeper: the deployment's max-model-len does not hold a stream long enough to span a controller restart at the engine's measured rate (at 40 tokens/s with max-model-len 2912 the stream that fits is 1m0s, under the window of 2m40s); the controller was not killed; this run's jobs were cancelled by tag\n"
	if note != want {
		t.Errorf("the marker's note is\n%q\nwant\n%q", note, want)
	}
	for _, f := range []string{RecordFile, PostsFile} {
		if _, err := os.Stat(filepath.Join(cfg.Out, f)); err == nil {
			t.Errorf("%s was written for a run that sent nothing", f)
		}
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
	for _, o := range hostOptionsFor(BetaRouterName) {
		if reflect.ValueOf(o).Pointer() == reflect.ValueOf(stack.RelayKeeper).Pointer() {
			t.Error("the router row runs the relay keeper")
		}
	}
}

package agent_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func warmUpChecks(gen int64, sessions int32) *agentapi.Checks {
	return &agentapi.Checks{VerificationGeneration: gen, Verify: &agentapi.VerifyRequest{Files: defaultFiles}, WarmUp: &agentapi.WarmUpRequest{Shapes: hShapes, TargetSessions: sessions}}
}

func warmRounds(n int) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.WarmUp != nil && len(c.WarmUp.Rounds) >= n
	}
}

func warmDone(r agentapi.Report) bool {
	c := checksOf(r)
	return c != nil && c.WarmUp != nil && c.WarmUp.Done
}

// firstOf returns the Total of a round's first successful request of a shape.
func firstOf(t *testing.T, round agentapi.WarmUpRound, s agentapi.Shape) time.Duration {
	t.Helper()
	for _, q := range round.Requests {
		if q.Shape == s && q.Error == "" {
			return time.Duration(q.Total)
		}
	}
	t.Fatalf("round %d has no successful %s request", round.Round, s)
	return 0
}

// engineEvents reads the engine double's log as its events.
func engineEvents(t *testing.T, h *harness) []enginedouble.LogEvent {
	t.Helper()
	var out []enginedouble.LogEvent
	for _, line := range readLines(t, h.engLog) {
		var ev enginedouble.LogEvent
		if json.Unmarshal([]byte(line), &ev) == nil {
			out = append(out, ev)
		}
	}
	return out
}

// chatRequests returns the engine's chat request records, grouped by shape (prompt tokens and max
// tokens, the key of slow-first and never-settle), each group in the order the requests ended.
func chatRequests(t *testing.T, h *harness) map[[2]int][]enginedouble.LogEvent {
	t.Helper()
	groups := map[[2]int][]enginedouble.LogEvent{}
	for _, ev := range engineEvents(t, h) {
		if ev.Event == "request" && ev.Path == "/v1/chat/completions" {
			k := [2]int{ev.PromptTokens, ev.MaxTokens}
			groups[k] = append(groups[k], ev)
		}
	}
	return groups
}

// A round sends the shapes HBEP-7 lists, in order: the three context lengths
// with the typical output, the typical context with the longest output, then
// targetSessions concurrent typical requests, each with the output length it
// asked for.
func TestWarmUpRoundSendsTheWorkloadShapes(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, warmUpChecks(1, 3)))
	r := h.waitReport("one warm-up round", 60*time.Second, warmRounds(1))
	round := r.Instance.Checks.WarmUp.Rounds[0]
	var shapes []string
	for _, q := range round.Requests {
		shapes = append(shapes, string(q.Shape))
		if q.Error != "" {
			t.Errorf("%s request failed: %s", q.Shape, q.Error)
		}
		want := hShapes.OutputTypical
		if q.Shape == agentapi.ShapeLongOutput {
			want = hShapes.OutputLongest
		}
		if q.OutputTokens != want {
			t.Errorf("%s request produced %d tokens, want %d", q.Shape, q.OutputTokens, want)
		}
		if q.TimeToFirstToken <= 0 || q.Total < q.TimeToFirstToken {
			t.Errorf("%s request timing %v / %v", q.Shape, q.TimeToFirstToken, q.Total)
		}
	}
	want := "short typical longest longOutput concurrent concurrent concurrent"
	if got := strings.Join(shapes, " "); got != want {
		t.Fatalf("shapes %q, want %q", got, want)
	}
}

// An engine slow on the first request of each shape is slow in round 1 and not
// in round 2, so a controller finds it settled at round 2; once the controller
// drops warm-up from the desired state, the agent reports warm-up done.
func TestWarmUpSlowFirstRequestOfEachShape(t *testing.T) {
	const slow = 400 * time.Millisecond
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("slow-first=400ms", 1, 0, warmUpChecks(1, 2)))
	r := h.waitReport("two warm-up rounds", 60*time.Second, warmRounds(2))
	rounds := r.Instance.Checks.WarmUp.Rounds
	for _, s := range []agentapi.Shape{agentapi.ShapeShort, agentapi.ShapeTypical, agentapi.ShapeLongest, agentapi.ShapeLongOutput} {
		r1, r2 := firstOf(t, rounds[0], s), firstOf(t, rounds[1], s)
		if r1-r2 < slow*3/4 {
			t.Errorf("%s: round 1 took %s and round 2 %s; the cold first request did not show", s, r1, r2)
		}
	}
	// The controller has judged: warm-up is dropped.
	h.set(h.runState("slow-first=400ms", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	h.waitReport("warm-up done", 20*time.Second, warmDone)
}

// An engine whose latency never settles is warmed up for five rounds, and the agent stops there by
// itself and reports warm-up done. That each round is slower than the one before by more than the 10%
// the controller tolerates is a premise of the scenario, and it is shown from what the engine double
// injected (its log), never from the wall-clock latencies the agent measured, which a loaded machine
// skews by tens of milliseconds either way.
//
// Round r sends two requests of the typical shape (the typical one and the one concurrent session), so
// the typical request of round r is the shape's request number 2(r-1), and never-settle=60 makes request
// n wait n * 60% of token-delay * tokens before its first token: 0 for round 1, then 1.2, 2.4, 3.6 and
// 4.8 times the 128 ms (64 tokens at 2 ms) that a request takes to generate. The nominal latency of a
// round is that time plus the wait, so a round is at least 26% slower than the one before (round 5
// against 4: 5.8 against 4.6 times 128 ms), against the controller's 10%.
func TestWarmUpNeverSettlingStopsAfterFiveRounds(t *testing.T) {
	const (
		delay    = 2 * time.Millisecond
		percent  = 60
		perRound = 2 // typical-shape requests in a round: the typical one and the one concurrent session
	)
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("never-settle=60,token-delay=2ms", 1, 0, warmUpChecks(1, 1)))
	r := h.waitReport("warm-up done", 120*time.Second, warmDone)
	rounds := r.Instance.Checks.WarmUp.Rounds
	if len(rounds) != agentapi.MaxWarmUpRounds {
		t.Fatalf("%d rounds, want %d", len(rounds), agentapi.MaxWarmUpRounds)
	}
	for _, round := range rounds {
		firstOf(t, round, agentapi.ShapeTypical) // each round was run, and its typical request succeeded
	}

	// The engine's record of the typical shape: the one whose requests are perRound for each round. The
	// last ones are written as the agent receives the answers, so the record is waited for.
	var typical []enginedouble.LogEvent
	eventually(t, 30*time.Second, "the engine's record of every typical-shape request", func() bool {
		typical = nil
		for _, g := range chatRequests(t, h) {
			if len(g) == perRound*agentapi.MaxWarmUpRounds {
				typical = g
			}
		}
		return typical != nil
	})
	base := time.Duration(hShapes.OutputTypical) * delay
	share := time.Duration(float64(percent) / 100 * float64(delay) * float64(hShapes.OutputTypical))
	var injected []time.Duration
	for _, ev := range typical {
		injected = append(injected, time.Duration(ev.InjectedNS))
	}
	slices.Sort(injected)
	for n, got := range injected {
		if want := time.Duration(n) * share; got < want-time.Microsecond || got > want+time.Microsecond {
			t.Fatalf("typical-shape request %d was made to wait %v, want %v (all: %v)", n, got, want, injected)
		}
	}
	for round := 2; round <= agentapi.MaxWarmUpRounds; round++ {
		prev, cur := injected[perRound*(round-2)], injected[perRound*(round-1)]
		prevLatency := base + prev
		// More than the 10% the controller tolerates, with a margin: at least 20%, twice that.
		if (cur-prev)*5 <= prevLatency {
			t.Errorf("round %d was made %v slower than round %d, nominal latency %v: not 20%% more", round, cur-prev, round-1, prevLatency)
		}
	}

	// The agent stops by itself: after warm-up is reported done, five more reports (each a full pass of
	// its loop) show the same five rounds, and the engine is sent no further typical-shape request.
	reports := len(h.timed.reportTimes())
	eventually(t, 30*time.Second, "five more reports after warm-up done", func() bool { return len(h.timed.reportTimes()) >= reports+5 })
	last := checksOf(mustLast(t, h))
	if last == nil || last.WarmUp == nil || !last.WarmUp.Done || len(last.WarmUp.Rounds) != agentapi.MaxWarmUpRounds {
		t.Fatalf("after five more reports the warm-up is %+v, want done with %d rounds", last, agentapi.MaxWarmUpRounds)
	}
	for k, g := range chatRequests(t, h) {
		if len(g) > perRound*agentapi.MaxWarmUpRounds {
			t.Errorf("shape %v was sent %d requests, more than %d rounds hold", k, len(g), agentapi.MaxWarmUpRounds)
		}
	}
}

// When the desired state drops warm-up while a round is in flight, the agent
// cancels the requests still in flight and reports warm-up done, so no live
// request meets a warm-up round (agentapi's documentation, Warm-up gates
// ready).
func TestWarmUpDroppedCancelsTheRoundInFlight(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("stall", 1, 0, warmUpChecks(1, 2)))
	h.waitReport("warm-up started", 30*time.Second, func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.WarmUp != nil
	})
	// The first request is in flight, stalled: the engine has admitted a streaming request (warm-up
	// requests stream; verification's one does not) that has no record of an end yet.
	eventually(t, 30*time.Second, "the engine to hold a warm-up request in flight", func() bool {
		var admitted, ended int
		for _, ev := range engineEvents(t, h) {
			switch {
			case ev.Event == "admitted" && ev.Stream:
				admitted++
			case ev.Event == "request" && ev.Stream:
				ended++
			}
		}
		return admitted > ended
	})
	h.set(h.runState("stall", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	r := h.waitReport("warm-up done", 20*time.Second, warmDone)
	if n := len(r.Instance.Checks.WarmUp.Rounds); n != 0 {
		t.Fatalf("%d rounds reported; the stalled round must be dropped, not reported", n)
	}
	// The engine saw the request cancelled by its client.
	eventually(t, 30*time.Second, "the engine to log a warm-up request cancelled by its client", func() bool {
		for _, ev := range engineEvents(t, h) {
			if ev.Event == "request" && ev.Stream && ev.Outcome == "cancelled" {
				return true
			}
		}
		return false
	})
}

// Check results carry their verification generation, and a new generation
// never shows the old one's results: while the restarted worker comes up,
// generation 2 has no verification, and its verification comes from the new
// process.
func TestCheckResultsNeverCrossVerificationGenerations(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("start-delay=800ms", 1, 0, verifyChecks(1, nil)))
	h.waitReport("verification 1", 30*time.Second, verified(1))
	h.set(h.runState("start-delay=800ms", 1, 1, verifyChecks(2, nil)))
	h.waitReport("verification 2", 30*time.Second, verified(2))
	for _, r := range h.backend.Reports(hAllocUID) {
		c := checksOf(r)
		if c == nil || c.VerificationGeneration != 2 || c.Verify == nil {
			continue
		}
		if w := worker0(r); w == nil || w.Generation != 1 || w.Phase != agentapi.PartRunning {
			t.Fatalf("a report shows generation 2's verification without the generation-1 process running: worker %+v", w)
		}
	}
}

package agent_test

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func warmUpChecks(gen int64, sessions int32) *agentapi.Checks {
	return &agentapi.Checks{VerificationGeneration: gen, WarmUp: &agentapi.WarmUpRequest{Shapes: hShapes, TargetSessions: sessions}}
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

// An engine whose latency never settles is warmed up for five rounds, and the
// agent stops there by itself and reports warm-up done; each round is slower
// than the one before by more than the 10% the controller tolerates.
func TestWarmUpNeverSettlingStopsAfterFiveRounds(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("never-settle=60,token-delay=2ms", 1, 0, warmUpChecks(1, 1)))
	r := h.waitReport("warm-up done", 120*time.Second, warmDone)
	rounds := r.Instance.Checks.WarmUp.Rounds
	if len(rounds) != agentapi.MaxWarmUpRounds {
		t.Fatalf("%d rounds, want %d", len(rounds), agentapi.MaxWarmUpRounds)
	}
	for i := 1; i < len(rounds); i++ {
		prev, cur := firstOf(t, rounds[i-1], agentapi.ShapeTypical), firstOf(t, rounds[i], agentapi.ShapeTypical)
		if cur*10 <= prev*11 {
			t.Errorf("round %d took %s, round %d %s: not more than 10%% slower", i+1, cur, i, prev)
		}
	}
	time.Sleep(500 * time.Millisecond)
	if n := len(checksOf(mustLast(t, h)).WarmUp.Rounds); n != agentapi.MaxWarmUpRounds {
		t.Fatalf("the agent ran %d rounds", n)
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
	time.Sleep(300 * time.Millisecond) // the first request is in flight, stalled
	h.set(h.runState("stall", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	r := h.waitReport("warm-up done", 20*time.Second, warmDone)
	if n := len(r.Instance.Checks.WarmUp.Rounds); n != 0 {
		t.Fatalf("%d rounds reported; the stalled round must be dropped, not reported", n)
	}
	// The engine saw the request cancelled by its client.
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(strings.Join(readLines(t, h.engLog), "\n"), `"outcome":"cancelled"`) {
		if time.Now().After(deadline) {
			t.Fatalf("the engine never saw a warm-up request cancelled; its log:\n%s", strings.Join(readLines(t, h.engLog), "\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
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

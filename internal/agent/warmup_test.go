package agent_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
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

// roundStarts records the warm-up rounds the agent started, by their number, at the moment it calls the
// engine: the count of round STARTS, which no engine record can give, since a round the agent launches may
// not have sent a request yet, and one that is cancelled sends none.
type roundStarts struct {
	mu sync.Mutex
	ns []int32
}

func (r *roundStarts) add(n int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ns = append(r.ns, n)
}

func (r *roundStarts) all() []int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ns)
}

// startsCountingEngine is the agent's engine client with every WarmUpRound call counted before it runs.
type startsCountingEngine struct {
	agent.Engine
	starts *roundStarts
}

func (e startsCountingEngine) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest, s agentapi.Sampling) agentapi.WarmUpRound {
	e.starts.add(n)
	return e.Engine.WarmUpRound(ctx, n, req, s)
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

// warmUpRequests returns the engine's chat request records of the agent's warm-up requests that
// completed, grouped by shape (prompt tokens and max tokens, the key of slow-first and never-settle). The
// typical shape's group also holds the concurrent sessions' requests, which have the same key. A request
// the double did not complete may not have reached the point where it decides its wait.
func warmUpRequests(t *testing.T, h *harness) map[[2]int][]enginedouble.LogEvent {
	t.Helper()
	groups := map[[2]int][]enginedouble.LogEvent{}
	for _, ev := range engineEvents(t, h) {
		if ev.Event == "request" && ev.Path == "/v1/chat/completions" && ev.Outcome == "completed" && requestCheck(ev.Tail) == "warmUp" {
			k := [2]int{ev.PromptTokens, ev.MaxTokens}
			groups[k] = append(groups[k], ev)
		}
	}
	return groups
}

// warmUpShapeKeys tells the engine's four warm-up groups apart by what the agent asked for: the long-output
// shape is the one with the longest output, and the other three ask for the typical output with the short,
// the typical and the longest context, so their prompt lengths order them. ok is false until the engine has
// logged all four.
func warmUpShapeKeys(groups map[[2]int][]enginedouble.LogEvent) (keys map[agentapi.Shape][2]int, ok bool) {
	if len(groups) != 4 {
		return nil, false
	}
	var typicalOutput [][2]int
	keys = map[agentapi.Shape][2]int{}
	for k := range groups {
		if k[1] == int(hShapes.OutputLongest) {
			keys[agentapi.ShapeLongOutput] = k
		} else {
			typicalOutput = append(typicalOutput, k)
		}
	}
	if len(keys) != 1 || len(typicalOutput) != 3 {
		return nil, false
	}
	slices.SortFunc(typicalOutput, func(a, b [2]int) int { return a[0] - b[0] })
	keys[agentapi.ShapeShort], keys[agentapi.ShapeTypical], keys[agentapi.ShapeLongest] = typicalOutput[0], typicalOutput[1], typicalOutput[2]
	return keys, true
}

// loggedThroughRound says whether the engine's record holds every request of the first rounds rounds, one
// by one: for each of the four shapes, the requests numbered 0 up to the end of that round. A round sends
// one request of each shape, and the typical shape's group also holds the sessions concurrent requests. It
// asks for the records themselves, never for how many there are: a record of a later round cannot stand in
// for one of an earlier round that the double has not logged yet (the double logs a request when it ends,
// after the agent has its answer).
func loggedThroughRound(groups map[[2]int][]enginedouble.LogEvent, rounds, sessions int) bool {
	keys, ok := warmUpShapeKeys(groups)
	if !ok {
		return false
	}
	for shape, k := range keys {
		perRound := 1
		if shape == agentapi.ShapeTypical {
			perRound += sessions
		}
		logged := map[int]bool{}
		for _, ev := range groups[k] {
			logged[ev.Nth] = true
		}
		for n := 0; n < rounds*perRound; n++ {
			if !logged[n] {
				return false
			}
		}
	}
	return true
}

// TestLoggedThroughRound: the wait of the slow-first test below is on the records themselves. Round 2's
// long-output request is unlogged while a request of round 3 is, which brings the total to what two rounds
// make (12 for 2 sessions), and the wait must not be satisfied by it.
func TestLoggedThroughRound(t *testing.T) {
	const sessions, perRound = 2, 3 + 3 // requests of a round: short, typical, longest, long output, 2 concurrent
	// The keys, in any increasing prompt lengths; the engine's own counts (Nth) number each key's requests.
	short := [2]int{25, int(hShapes.OutputTypical)}
	typical := [2]int{73, int(hShapes.OutputTypical)}
	longest := [2]int{4032, int(hShapes.OutputTypical)}
	longOut := [2]int{73, int(hShapes.OutputLongest)}
	logged := func(rounds int) map[[2]int][]enginedouble.LogEvent {
		g := map[[2]int][]enginedouble.LogEvent{}
		for _, k := range [][2]int{short, longest, longOut} {
			for n := 0; n < rounds; n++ {
				g[k] = append(g[k], enginedouble.LogEvent{Nth: n})
			}
		}
		for n := 0; n < rounds*(1+sessions); n++ {
			g[typical] = append(g[typical], enginedouble.LogEvent{Nth: n})
		}
		return g
	}
	without := func(g map[[2]int][]enginedouble.LogEvent, k [2]int, nth int) map[[2]int][]enginedouble.LogEvent {
		var kept []enginedouble.LogEvent
		for _, ev := range g[k] {
			if ev.Nth != nth {
				kept = append(kept, ev)
			}
		}
		out := map[[2]int][]enginedouble.LogEvent{}
		for kk, v := range g {
			out[kk] = v
		}
		out[k] = kept
		return out
	}
	total := func(g map[[2]int][]enginedouble.LogEvent) (n int) {
		for _, v := range g {
			n += len(v)
		}
		return n
	}

	two := logged(2)
	if total(two) != 2*perRound {
		t.Fatalf("the fixture of two rounds holds %d records, want %d", total(two), 2*perRound)
	}
	if !loggedThroughRound(two, 2, sessions) {
		t.Error("two complete rounds are not logged through round 2")
	}
	// Round 3's short request is logged (Nth 2) while round 2's long-output request (Nth 1) is not: the
	// total is the same as for two complete rounds.
	delayed := without(two, longOut, 1)
	delayed[short] = append(delayed[short], enginedouble.LogEvent{Nth: 2})
	if total(delayed) != 2*perRound {
		t.Fatalf("the delayed-log fixture holds %d records, want %d to match a complete two rounds", total(delayed), 2*perRound)
	}
	if loggedThroughRound(delayed, 2, sessions) {
		t.Error("round 3's record stood in for round 2's unlogged long-output request")
	}
	for _, c := range []struct {
		what string
		g    map[[2]int][]enginedouble.LogEvent
	}{
		{"the second concurrent session of round 2 unlogged", without(two, typical, 5)},
		{"the typical request of round 2 unlogged", without(two, typical, 3)},
		{"a round 1 record unlogged", without(two, short, 0)},
		{"only round 1", logged(1)},
	} {
		if loggedThroughRound(c.g, 2, sessions) {
			t.Errorf("logged through round 2 with %s", c.what)
		}
	}
	if !loggedThroughRound(logged(3), 2, sessions) {
		t.Error("three complete rounds are not logged through round 2")
	}
	three := logged(3)
	delete(three, longOut) // a shape the engine has not seen at all: three groups
	if loggedThroughRound(three, 2, sessions) {
		t.Error("logged through round 2 with a shape missing")
	}
}

// roundHold is the state shared by the engine clients of a test that holds warm-up rounds from holdFrom
// on (the agent asks for a client for each unit it starts, so the clients share it): such a round sends
// nothing, and returns only when the agent cancels it. So the agent cannot run its rounds to the end by
// itself, and only the controller's drop (a desired state without WarmUp) can end warm-up. entered is
// closed when the first held round has been called, and released when a held round has returned.
type roundHold struct {
	holdFrom int32
	entered  chan struct{}
	released chan struct{}
	once     sync.Once
	back     sync.Once
}

func newRoundHold(holdFrom int32) *roundHold {
	return &roundHold{holdFrom: holdFrom, entered: make(chan struct{}), released: make(chan struct{})}
}

// holdingEngine is the agent's engine client with the rounds roundHold names held.
type holdingEngine struct {
	agent.Engine
	hold *roundHold
}

func (e holdingEngine) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest, s agentapi.Sampling) agentapi.WarmUpRound {
	if n < e.hold.holdFrom {
		return e.Engine.WarmUpRound(ctx, n, req, s)
	}
	e.hold.once.Do(func() { close(e.hold.entered) })
	<-ctx.Done()
	e.hold.back.Do(func() { close(e.hold.released) })
	return agentapi.WarmUpRound{Round: n}
}

// An engine slow on the first request of each shape is slow in round 1 and not
// in round 2, so a controller finds it settled at round 2; once the controller
// drops warm-up from the desired state, the agent reports warm-up done.
//
// That the engine was slow on the first request of each shape, and on no other, is shown from what the
// engine double injected (its log, as TestWarmUpNeverSettlingStopsAfterFiveRounds shows its premise),
// never from the wall-clock latencies the agent measured: a loaded machine skews those by hundreds of
// milliseconds either way, and a comparison of round 1 with round 2 fails although the double behaved as
// configured (#234). The double numbers each request in its shape (Nth) and decides its wait before it
// answers, so the request numbered 0 is the shape's first, which the agent sends in round 1 (a round
// finishes before the next starts), and every request after it, round 2's among them, is not slowed.
// What the agent reports is its rounds: round 1 and round 2, each with the shapes HBEP-7 lists, in order,
// every request successful.
//
// Round 3 is held by the engine client until the agent cancels it, so the test controls what ends
// warm-up: the agent cannot reach its fifth round by itself, the engine's log stays as it is while it is
// read, and the "done" that follows is the agent's answer to the controller's drop, which cancels the round
// in flight and reports Done with the two rounds that ran. The log is waited for record by record
// (loggedThroughRound), not by how many records there are.
func TestWarmUpSlowFirstRequestOfEachShape(t *testing.T) {
	const (
		slow     = 400 * time.Millisecond
		sessions = 2
	)
	hold := newRoundHold(3)
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return holdingEngine{e, hold} }})
	h.set(h.runState("slow-first="+slow.String(), 1, 0, warmUpChecks(1, sessions)))
	r := h.waitReport("two warm-up rounds", 60*time.Second, warmRounds(2))
	if w := r.Instance.Checks.WarmUp; w.Done || len(w.Rounds) != 2 {
		t.Fatalf("with round 3 held, the report shows %d rounds and done=%v, want 2 rounds and not done: the agent finished warm-up before the controller dropped it", len(w.Rounds), w.Done)
	}
	rounds := r.Instance.Checks.WarmUp.Rounds
	const wantShapes = "short typical longest longOutput concurrent concurrent"
	for i, round := range rounds {
		if round.Round != int32(i+1) {
			t.Errorf("report round %d is numbered %d", i+1, round.Round)
		}
		var shapes []string
		for _, q := range round.Requests {
			shapes = append(shapes, string(q.Shape))
			if q.Error != "" {
				t.Errorf("round %d: the %s request failed: %s", round.Round, q.Shape, q.Error)
			}
		}
		if got := strings.Join(shapes, " "); got != wantShapes {
			t.Errorf("round %d: shapes %q, want %q", round.Round, got, wantShapes)
		}
	}

	// The engine's record of what it did. Its last records are written as the agent receives the answers,
	// so each record of the two reported rounds is waited for itself.
	var groups map[[2]int][]enginedouble.LogEvent
	eventually(t, 30*time.Second, "the engine's record of every request of both reported rounds", func() bool {
		groups = warmUpRequests(t, h)
		return loggedThroughRound(groups, 2, sessions)
	})
	for k, g := range groups {
		firsts := 0
		for _, ev := range g {
			want := time.Duration(0)
			if ev.Nth == 0 {
				firsts++
				want = slow
			}
			if got := time.Duration(ev.InjectedNS); got != want {
				t.Errorf("shape of %d prompt tokens and %d max tokens: request number %d was made to wait %v, want %v", k[0], k[1], ev.Nth, got, want)
			}
		}
		if firsts != 1 {
			t.Errorf("shape of %d prompt tokens and %d max tokens: %d requests numbered 0, want exactly the shape's first", k[0], k[1], firsts)
		}
	}

	// The controller has judged: warm-up is dropped, with round 3 in flight (held). The agent cancels that
	// round and reports Done with the two rounds that ran, and a report before the drop was not done.
	eventually(t, 30*time.Second, "round 3 to be started and held", func() bool { return closed(hold.entered) })
	h.set(h.runState("slow-first="+slow.String(), 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	done := h.waitReport("warm-up done", 20*time.Second, warmDone)
	if w := done.Instance.Checks.WarmUp; len(w.Rounds) != 2 {
		t.Errorf("warm-up done with %d rounds, want the 2 that ran before the drop (fewer than %d)", len(w.Rounds), agentapi.MaxWarmUpRounds)
	}
	if !closed(hold.released) {
		t.Error("warm-up is done, but the agent did not cancel the round in flight")
	}
}

// An engine whose latency never settles is warmed up for five rounds, and the agent stops there by itself
// and reports warm-up done. That each round is slower than the one before by more than the 10% the
// controller tolerates is a premise of the scenario, and it is shown from what the engine double injected
// (its log), never from the wall-clock latencies the agent measured, which a loaded machine skews by tens
// of milliseconds either way.
//
// Round r sends two requests of the typical shape (the typical one and the one concurrent session), so the
// typical request of round r is the shape's request number 2(r-1), and never-settle=60 makes request n wait
// n * 60% of token-delay * tokens before its first token: 0 for round 1, then 1.2, 2.4, 3.6 and 4.8 times
// the 128 ms (64 tokens at 2 ms) that a request takes to generate. The nominal latency of a round is that
// time plus the wait, so a round is at least 26% slower than the one before (round 5 against 4: 5.8
// against 4.6 times 128 ms), against the controller's 10%. The double numbers each request in its shape
// (Nth), so a wait is tied to its round by that number and not by sorting the waits.
//
// That the agent starts no sixth round is shown at a synchronization point: the agent is stopped, and
// Run returns only when every unit it started has returned. The engine client counts each WarmUpRound call
// as it is made, so a round started at any time before the stop is counted, whether it had sent a request
// or not, before or after the Done report; the engine's admissions count the requests the same way.
func TestWarmUpNeverSettlingStopsAfterFiveRounds(t *testing.T) {
	const (
		delay    = 2 * time.Millisecond
		percent  = 60
		perRound = 2 // typical-shape requests in a round: the typical one and the one concurrent session
		perShape = agentapi.MaxWarmUpRounds
		inRound  = 5 // requests in a round: short, typical, longest, long output and the one concurrent session
	)
	starts := &roundStarts{}
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return startsCountingEngine{e, starts} }})
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
			if len(g) == perRound*perShape {
				typical = g
			}
		}
		return typical != nil
	})
	base := time.Duration(hShapes.OutputTypical) * delay
	share := time.Duration(float64(percent) / 100 * float64(delay) * float64(hShapes.OutputTypical))
	// The waits by the request's place in its shape's sequence, as the double numbered it: place 2(r-1) is
	// round r's typical request. Every place from 0 to 9 is there once, and each wait is its place times the
	// share, so a wait that belongs to another place (waits 0 1 2 3 9 8 7 6 5 4) is caught as well.
	waits := make([]time.Duration, len(typical))
	seen := make([]bool, len(typical))
	for _, ev := range typical {
		if ev.Nth < 0 || ev.Nth >= len(typical) || seen[ev.Nth] {
			t.Fatalf("typical-shape requests numbered %v: want each of 0 to %d once", nths(typical), len(typical)-1)
		}
		seen[ev.Nth] = true
		waits[ev.Nth] = time.Duration(ev.InjectedNS)
		if want := time.Duration(ev.Nth) * share; waits[ev.Nth] < want-time.Microsecond || waits[ev.Nth] > want+time.Microsecond {
			t.Fatalf("typical-shape request number %d was made to wait %v, want %v", ev.Nth, waits[ev.Nth], want)
		}
	}
	for round := 2; round <= agentapi.MaxWarmUpRounds; round++ {
		prev, cur := waits[perRound*(round-2)], waits[perRound*(round-1)]
		prevLatency := base + prev
		// More than the 10% the controller tolerates, with a margin: at least 20%, twice that.
		if (cur-prev)*5 <= prevLatency {
			t.Errorf("round %d was made %v slower than round %d, nominal latency %v: not 20%% more", round, cur-prev, round-1, prevLatency)
		}
	}

	// The agent stops by itself. After warm-up is reported done, the engine is quiet: every streaming request
	// it admitted has ended, five rounds' worth (5 requests each). Five more reports show the same rounds,
	// and the agent's loop has run many passes (its health probes post an event every 100 ms) in which a
	// round could have been started. Then the agent is stopped, which waits for every unit it started.
	eventually(t, 30*time.Second, "the engine to be quiet after warm-up done", func() bool {
		admitted, ended := streamingCounts(t, h)
		return admitted == ended && admitted == inRound*agentapi.MaxWarmUpRounds
	})
	reports := len(h.timed.reportTimes())
	eventually(t, 30*time.Second, "five more reports after warm-up done", func() bool { return len(h.timed.reportTimes()) >= reports+5 })
	h.stop()
	if got, want := starts.all(), []int32{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("the agent started warm-up rounds %v, want exactly %v", got, want)
	}
	perShapeAdmitted := map[[2]int]int{}
	for _, ev := range engineEvents(t, h) {
		if ev.Event == "admitted" && ev.Stream {
			perShapeAdmitted[[2]int{ev.PromptTokens, ev.MaxTokens}]++
		}
	}
	// Four shapes of fixed length: short, typical (with the concurrent session's), longest and long output.
	counts := []int{}
	for _, n := range perShapeAdmitted {
		counts = append(counts, n)
	}
	slices.Sort(counts)
	if want := []int{perShape, perShape, perShape, perRound * perShape}; !slices.Equal(counts, want) {
		t.Errorf("the engine admitted warm-up requests per shape %v, want %v: exactly %d for each shape and %d for the typical one", counts, want, perShape, perRound*perShape)
	}
}

// nths lists the places in their shape that the records name.
func nths(evs []enginedouble.LogEvent) []int {
	var out []int
	for _, ev := range evs {
		out = append(out, ev.Nth)
	}
	return out
}

// streamingCounts counts the engine's admitted streaming requests (the warm-up's) and those that have ended.
func streamingCounts(t *testing.T, h *harness) (admitted, ended int) {
	t.Helper()
	for _, ev := range engineEvents(t, h) {
		switch {
		case ev.Event == "admitted" && ev.Stream:
			admitted++
		case ev.Event == "request" && ev.Stream:
			ended++
		}
	}
	return admitted, ended
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
		admitted, ended := streamingCounts(t, h)
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

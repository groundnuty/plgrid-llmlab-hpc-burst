package managed

import (
	"context"
	"net/http"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

// goroutines is the stack of every goroutine (debug 2, the form of a test
// binary's time-out dump).
func goroutines() string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return b.String()
}

// startFrame is how a goroutine inside Controller.Start shows in goroutines.
const startFrame = "managed.(*Controller).Start("

// TestStartStopsWhenToldWhereverItIs: the catalog loop stops within a bound
// when its context ends, so a managed Provider's manager stops when told
// (hpcburst down, the supervisor's restart; issue #136). It has no work
// queue, so controller-runtime's priority-queue deadlock cannot reach it;
// what can hold it is a pass that does not end. Each of stopRounds starts is
// cancelled at one of the two places a pass waits: between passes (Start
// waits on the clock), or inside a catalog read that the gateway double holds
// in a stall. Start must return within stopLimit, far more than a stop takes;
// one that does not fails the test with every goroutine's stack. Each round
// stops its Start on every path, a failed one too, and the test ends with no
// more goroutines in Start than it began with.
func TestStartStopsWhenToldWhereverItIs(t *testing.T) {
	const stopRounds = 200
	before := strings.Count(goroutines(), startFrame)
	t.Cleanup(func() {
		if n := strings.Count(goroutines(), startFrame); n > before {
			t.Errorf("%d goroutines are still in Controller.Start after the test, %d before it; goroutines:\n%s", n, before, goroutines())
		}
	})
	fc := clocktesting.NewFakeClock(at(12, 0, 0))
	e := newEnv(t, withClock(fc), withModels(model("example-a")))
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		stopRound(t, e, fc, round)
	}
}

// stopRound runs one round of TestStartStopsWhenToldWhereverItIs: an odd
// round cancels Start inside a stalled catalog read, an even one while it
// waits between passes. Its deferred stop runs on every path: it cancels,
// waits once, bounded, for Start, and then ends the stall.
func stopRound(t *testing.T, e *env, fc *clocktesting.FakeClock, round int) {
	t.Helper()
	const (
		stopLimit = 10 * time.Second
		heldLimit = 10 * time.Second
	)
	// Two minutes: past the last pass's floor (a minute and a second after
	// its read), and past every wait a stopped Start left on the clock, so
	// HasWaiters is the new Start's own wait.
	fc.Step(2 * CatalogInterval)
	stall := round%2 == 1
	e.gw.Stall(stall)
	arrived := e.gw.Arrived(http.MethodGet, "/v1/models")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.ctl.Start(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("round %d: Start returned %v after its context ended, want nil", round, err)
			}
		case <-time.After(stopLimit):
			t.Errorf("round %d (stalled read %v): Start did not return within %s of its cancel; goroutines:\n%s", round, stall, stopLimit, goroutines())
			return
		}
		if stall {
			// The held read ends with its request; the next round counts
			// only its own.
			deadline := time.Now().Add(heldLimit)
			for e.gw.Waiting() != 0 {
				if time.Now().After(deadline) {
					t.Errorf("round %d: the held read did not end within %s of Start's return", round, heldLimit)
					break
				}
				time.Sleep(time.Millisecond)
			}
			e.gw.Stall(false)
		}
	}()
	if stall {
		waitFor(t, "the catalog read to meet the stall", func() bool { return e.gw.Waiting() == 1 })
	} else {
		waitFor(t, "the pass to end and Start to wait", fc.HasWaiters)
	}
	if got := e.gw.Arrived(http.MethodGet, "/v1/models"); got != arrived+1 {
		t.Fatalf("round %d: %d catalog reads arrived in the round, want 1", round, got-arrived)
	}
}

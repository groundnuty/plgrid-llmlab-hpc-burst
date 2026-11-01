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

// TestStartStopsWhenToldWhereverItIs: the catalog loop stops within a bound
// when its context ends, so a managed Provider's manager stops when told
// (hpcburst down, the supervisor's restart; issue #136). It has no work
// queue, so controller-runtime's priority-queue deadlock cannot reach it;
// what can hold it is a pass that does not end. Each of stopRounds starts is
// cancelled at one of the two places a pass waits: between passes (Start
// waits on the clock), or inside a catalog read that the gateway double holds
// in a stall. Start must return within stopLimit, far more than a stop takes;
// one that does not fails the test with every goroutine's stack.
func TestStartStopsWhenToldWhereverItIs(t *testing.T) {
	const (
		stopRounds = 200
		stopLimit  = 10 * time.Second
	)
	fc := clocktesting.NewFakeClock(at(12, 0, 0))
	e := newEnv(t, withClock(fc), withModels(model("example-a")))
	for round := 0; round < stopRounds; round++ {
		// Two minutes: past the last pass's floor (a minute and a second
		// after its read), and past every wait a stopped Start left on the
		// clock, so HasWaiters is the new Start's own wait.
		fc.Step(2 * CatalogInterval)
		stall := round%2 == 1
		e.gw.Stall(stall)
		arrived := e.gw.Arrived(http.MethodGet, "/v1/models")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.ctl.Start(ctx) }()
		if stall {
			waitFor(t, "the catalog read to meet the stall", func() bool { return e.gw.Waiting() == 1 })
		} else {
			waitFor(t, "the pass to end and Start to wait", fc.HasWaiters)
		}
		if got := e.gw.Arrived(http.MethodGet, "/v1/models"); got != arrived+1 {
			cancel()
			t.Fatalf("round %d: %d catalog reads arrived in the round, want 1", round, got-arrived)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: Start returned %v after its context ended, want nil", round, err)
			}
		case <-time.After(stopLimit):
			var b strings.Builder
			_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
			t.Fatalf("round %d (stalled read %v): Start did not return within %s of its cancel; goroutines:\n%s", round, stall, stopLimit, b.String())
		}
		if stall {
			// The held read ends with its request; the next round counts
			// only its own.
			waitFor(t, "the held read to end", func() bool { return e.gw.Waiting() == 0 })
			e.gw.Stall(false)
		}
	}
}

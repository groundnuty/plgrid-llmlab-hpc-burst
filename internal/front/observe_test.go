package front

import (
	"testing"
	"time"
)

// Waits on observable state for the tests that wait on concurrency or time
// (IMPL-COMMON, Timing tests): a generous deadline, never a fixed window.

// stateLimit bounds a wait for state the test expects to reach; it is far
// above the time the state takes, so a loaded machine cannot reach it.
const stateLimit = 30 * time.Second

// waitState polls cond until it holds, or fails the test after stateLimit.
func waitState(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(stateLimit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", stateLimit, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

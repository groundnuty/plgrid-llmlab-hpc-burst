package front

import (
	"runtime/pprof"
	"strings"
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

// goroutineStacks returns every goroutine's stack, one per element, in the
// form of a test binary's time-out dump (debug 2): a header line such as
// "goroutine 42 [IO wait]:", then a function line and a file line per frame.
func goroutineStacks() []string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Split(b.String(), "\n\n")
}

// waitGoroutine waits until some goroutine's stack satisfies match, or fails
// the test after stateLimit.
func waitGoroutine(t *testing.T, what string, match func(stack string) bool) {
	t.Helper()
	deadline := time.Now().Add(stateLimit)
	for {
		for _, g := range goroutineStacks() {
			if match(g) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", stateLimit, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// blockedWritingToClient reports whether a goroutine stack is the front's
// handler parked in a socket write: waiting for the poller ("IO wait") inside
// internal/poll's write, under one of the Proxy's methods. The poller parks a
// write only when the kernel's send buffer is full, so this is the front
// blocked on a client that does not read.
func blockedWritingToClient(stack string) bool {
	head, _, _ := strings.Cut(stack, "\n")
	return strings.Contains(head, "[IO wait") &&
		strings.Contains(stack, "internal/poll.(*FD).Write(") &&
		strings.Contains(stack, "/internal/front.(*Proxy).")
}

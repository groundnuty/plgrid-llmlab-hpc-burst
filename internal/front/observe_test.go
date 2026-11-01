package front

import (
	"context"
	"net/http"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// Waits on observable state for the tests that wait on concurrency or time
// (IMPL-COMMON, Timing tests): a generous deadline, never a fixed window.
// waitState, the wait for a condition with the same 30 s bound, is in
// probe_report_test.go.

// stateLimit bounds a wait for state the test expects to reach; it is far
// above the time the state takes, so a loaded machine cannot reach it.
const stateLimit = 30 * time.Second

// goroutineStacks returns every goroutine's stack, one per element, in the
// form of a test binary's time-out dump (debug 2): a header line such as
// "goroutine 42 [IO wait]:", then a function line and a file line per frame.
func goroutineStacks() []string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Split(b.String(), "\n\n")
}

// waitGoroutine waits until some goroutine's stack satisfies match, or fails
// the test after stateLimit with the stacks of the front's goroutines, so a
// change in how Go prints a stack shows in the failure.
func waitGoroutine(t *testing.T, what string, match func(stack string) bool) {
	t.Helper()
	deadline := time.Now().Add(stateLimit)
	for {
		stacks := goroutineStacks()
		for _, g := range stacks {
			if match(g) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s; the front's goroutines:\n%s", stateLimit, what, frontStacks(stacks))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// frontStacks joins the stacks that run the front's code, cut to 32 KiB.
func frontStacks(stacks []string) string {
	var b strings.Builder
	for _, g := range stacks {
		if strings.Contains(g, "/internal/front.") {
			b.WriteString(g)
			b.WriteString("\n\n")
		}
	}
	if out := b.String(); len(out) > 32<<10 {
		return out[:32<<10] + "\n... cut at 32 KiB"
	}
	return b.String()
}

// requestLimit bounds a whole exchange whose end the test holds back (an
// engine at its gate, a timer the test moves): headers, first event and the
// rest. It is far above the seconds such an exchange takes, so a cut that
// never comes fails the test within it instead of hanging the package until
// its time-out (PR #144 review, T1).
const requestLimit = time.Minute

// postBounded posts body to path with the client key, on a client that never
// reuses a connection, under a context that ends after requestLimit. It
// returns the response and that context: a read that fails once the context
// has ended is the bound's failure.
func postBounded(t *testing.T, h *harness, path, body string) (*http.Response, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestLimit)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := errClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("no response headers within %s: %v", requestLimit, err)
		}
		t.Fatalf("the request: %v", err)
	}
	return resp, ctx
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

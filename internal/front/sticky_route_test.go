package front

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// Sticky routes (HBEP-5, Workloads, Sticky routes; HBEP-764 R9): one row per rule. A row that shows a conversation stays put first makes a fresh decision
// differ from the pin (the key's home is full, or the pin was made by a spill), since the ring alone already sends a key to one place.

// The key (Sticky routes, The key): a request of a Workload with sessionAffinity Route must carry a session header, or it is 400 invalid_request with detail
// session_header_required, before anything is chosen or journalled. An empty header is none. The same request with a header is served, and a Workload with no
// sessionAffinity needs none.
func TestStickyWorkloadRefusesARequestWithNoSessionHeader(t *testing.T) {
	f := newSkFix(t, skOpts{})
	for _, hdr := range [][]string{nil, {"x-session-id", ""}} {
		resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), hdr...)
		body := readAll(t, resp)
		checkError(t, resp, body, wantError{status: 400, code: "invalid_request", detail: "session_header_required"})
		wantWorkload(t, "refused for no header", f.record(resp.Header.Get("hpcburst-request-id")), str("alpha"))
	}
	if n, j := f.totalSaw(), len(f.journal()); n != 0 || j != 0 {
		t.Errorf("the engines saw %d requests and the journal holds %d records after two refusals, want none of either", n, j)
	}
	if got, resp, body := f.chatAt("any-session"); got == "" {
		t.Fatalf("the same Workload refused a request with a header: %d %s", resp.StatusCode, body)
	}

	g := newSkFix(t, skOpts{noAffinity: true})
	if got, resp, body := g.chat(t, g.alpha(), ""); got == "" {
		t.Errorf("a Workload without sessionAffinity refused a request with no session header: %d %s", resp.StatusCode, body)
	}
}

// The pin is journalled before the request is dispatched (Sticky routes, The pin). A first request is held between the choice of its target and its pin
// (pinStore.beforeCreate): the engines have seen nothing and the journal holds no pin. Released, the request is served by the key's home, and the journal
// holds exactly one pin record: alpha's UID, the SHA-256 of the key, the ModelDeployment's UID and the Instance's full deployment ID, and the pin's TTL.
func TestStickyPinIsJournalledBeforeTheRequestIsDispatched(t *testing.T) {
	f := newSkFix(t, skOpts{})
	gate, entered := make(chan struct{}), make(chan struct{}, 4)
	f.router.pins.beforeCreate = func() { entered <- struct{}{}; <-gate }
	t.Cleanup(func() { close(gate) })
	s := f.session("ns/a-0", 0)
	call, _ := f.start(s)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first request never reached its pin")
	}
	if n, j := f.totalSaw(), len(f.journal()); n != 0 || j != 0 {
		t.Fatalf("before its pin was written the engines saw %d requests and the journal holds %d records, want none of either", n, j)
	}
	gate <- struct{}{}
	resp, body := call.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("the first request: %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	pins := f.journalOf("pin", s)
	if len(pins) != 1 || len(f.journal()) != 1 {
		t.Fatalf("the journal holds %d records, %d of them this key's pin, want exactly that one", len(f.journal()), len(pins))
	}
	p := pins[0]
	if p.Workload != f.alphaUID() || p.MD != f.mdUID("md-a") || p.ID != f.idOf("ns/a-0") || p.Provider != "" || p.ManagedID != "" || p.TTL.D() != 12*time.Hour {
		t.Errorf("the pin is %+v, want workload %s, ModelDeployment %s, deployment ID %s, TTL 12h and no managed target", p, f.alphaUID(), f.mdUID("md-a"), f.idOf("ns/a-0"))
	}
	if want := f.start0().Add(12 * time.Hour); !p.Expiry.Equal(want) {
		t.Errorf("the pin expires at %v, want %v: 12 h after the first request", p.Expiry, want)
	}
}

// start0 is the time on the front's clock at the harness's start.
func (f *skFix) start0() time.Time { return f.harness.start }

// A pin that can't be written refuses the request and dispatches nothing (Sticky routes, The pin): 503 capacity with detail pins_unavailable and
// Retry-After 10, the engines see no request, the journal holds nothing, and the Instance the request had taken is free again: once the journal is writable
// the same key is served by its home, which the held slot would have blocked (the cap is 1, so a leaked slot would send it to b-0).
func TestStickyFailedJournalWriteRefusesTheRequestAndDispatchesNothing(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	f.setFailWrites(errors.New("no space left on the device"))
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
	body := readAll(t, resp)
	checkError(t, resp, body, wantError{status: 503, code: "capacity", detail: "pins_unavailable", retryAfter: "10"})
	if n, j := f.totalSaw(), len(f.journal()); n != 0 || j != 0 {
		t.Errorf("after the refusal the engines saw %d requests and the journal holds %d records, want none of either", n, j)
	}
	f.setFailWrites(nil)
	if got, resp, body := f.chatAt(s); got != "ns/a-0" {
		t.Fatalf("with the journal writable again the key went to %q (%d %s), want its home ns/a-0: the refused request leaked its slot", got, resp.StatusCode, body)
	}
}

// Two first requests of one key are decided ONCE, under the key's lock (Sticky routes, The pin). The home a-0 and b-0 each take one request. The first request
// is held after it has taken a-0 and before its pin; the second then arrives and is seen waiting at the key's lock, so neither has decided twice. Released,
// both are served by a-0, the second after the first, and the journal holds one pin. Without the lock the second would find a-0 full and spill to b-0 while
// the first held the pin back.
func TestStickyTwoFirstRequestsOfOneKeyAreDecidedOnce(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(time.Minute) }})
	gate, entered := make(chan struct{}), make(chan struct{}, 4)
	var first atomic.Bool
	f.router.pins.beforeCreate = func() {
		entered <- struct{}{}
		// Only the first decision is held: a second decision, which the lock must prevent, would otherwise wait on the gate as well and hide its target.
		if first.CompareAndSwap(false, true) {
			<-gate
		}
	}
	s := f.session("ns/a-0", 0)
	one, _ := f.start(s)
	<-entered // the first request holds a-0 and the key's lock, and waits for its pin
	two, _ := f.start(s)
	errWaitFor(t, "the second request to reach the key's lock", func() bool { return f.keyLockRefs(s) == 2 })
	close(gate)
	for i, c := range []*scCall{one, two} {
		resp, body := c.wait(t)
		if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
			t.Errorf("request %d of the key: %d from %q, want 200 from ns/a-0: %s", i+1, resp.StatusCode, scInstanceOf(resp), body)
		}
	}
	if a, b := f.engineSaw("ns/a-0"), f.engineSaw("ns/b-0"); a != 2 || b != 0 {
		t.Errorf("a-0 saw %d requests and b-0 saw %d, want 2 and 0: the key was split", a, b)
	}
	if n := len(f.journalOf("pin", s)); n != 1 {
		t.Errorf("the journal holds %d pins of the key, want 1", n)
	}
}

// A pinned conversation never moves (Sticky routes, Requests after the pin). The key is pinned to a-0 by its first request. a-0 is then full, held by one
// request of the key, while b-0 is idle: a later request of the key waits in the Workload's FIFO, and after queueTimeout is 503 capacity with Retry-After.
// b-0 sees nothing. The control, a conversation with no pin yet whose home is the same full a-0, IS sent to b-0 at once: the fixture does move a request
// that is free to move.
func TestStickyPinnedRequestWaitsForItsTargetAndNeverMovesToAnotherWithRoom(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }})
	s, other := f.session("ns/a-0", 0), f.session("ns/a-0", 1)
	if got, resp, body := f.chatAt(s); got != "ns/a-0" {
		t.Fatalf("the first request went to %q (%d %s), want its home ns/a-0", got, resp.StatusCode, body)
	}
	release := f.hold("ns/a-0")
	held, _ := f.start(s)
	errWaitFor(t, "the held request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })

	waiter, _ := f.start(s)
	errWaitFor(t, "the request to wait in the Workload's FIFO", func() bool { return f.waiting() == 1 })
	resp, body := waiter.wait(t) // past queueTimeout
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	if n := f.engineSaw("ns/b-0"); n != 0 {
		t.Fatalf("b-0 saw %d requests of a pinned conversation whose target was full, want none", n)
	}
	if got, resp, body := f.chatAt(other); got != "ns/b-0" {
		t.Errorf("the control, a conversation with no pin and a full home, went to %q (%d %s), want b-0: b-0 had room all along", got, resp.StatusCode, body)
	}
	release()
	if r, b := held.wait(t); r.StatusCode != 200 {
		t.Errorf("the held request: %d %s", r.StatusCode, b)
	}
	if got, _, body := f.chatAt(s); got != "ns/a-0" {
		t.Errorf("after a-0 freed, the pinned key went to %q: %s", got, body)
	}
}

// A retry or resend stays in the target (Sticky routes, The pin and Requests after the pin). a-0 closes every connection before its response headers, which
// the front retries once, to another Instance. With a-1 beside a-0 in the pinned ModelDeployment the retry goes there, never to b-0. With a-0 the only
// Instance of its ModelDeployment the first failure is relayed to the client and b-0 sees nothing. The control, a request that names no Workload, is retried to
// b-0 as before. The first request of the conversation is the one retried, so the pin of a first request is shown to bind its own retry.
func TestStickyRetryAndResendStayInTheTarget(t *testing.T) {
	t.Run("a sibling Instance of the pinned ModelDeployment takes the retry", func(t *testing.T) {
		f := newSkFix(t, skOpts{deps: []pwDep{{name: "md-a", instances: []string{"a-0", "a-1"}, cap: 1}, {name: "md-b", instances: []string{"b-0"}, cap: 1}}})
		s := f.session("ns/a-0", 0)
		f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
		if got, resp, body := f.postAt(s, f.alpha()); got != "ns/a-1" || resp.StatusCode != 200 {
			t.Fatalf("the retry of the first request went to %q (%d %s), want a-1 of the pinned ModelDeployment", got, resp.StatusCode, body)
		}
		if n := f.engineSaw("ns/b-0"); n != 0 {
			t.Errorf("b-0 saw %d requests of a conversation pinned to md-a", n)
		}
	})
	t.Run("with no sibling the failure is relayed and b-0 sees nothing, where an unnamed request is retried to b-0", func(t *testing.T) {
		f := newSkFix(t, skOpts{})
		s := f.session("ns/a-0", 0)
		f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
		_, resp, body := f.postAt(s, f.alpha())
		if resp.StatusCode != 502 {
			t.Errorf("the pinned request got %d (%s), want the first failure, 502", resp.StatusCode, body)
		}
		if n := f.engineSaw("ns/b-0"); n != 0 {
			t.Fatalf("b-0 saw %d requests of a conversation pinned to a-0", n)
		}
		if got, r, b := f.postAt(s, scModel); got != "ns/b-0" || r.StatusCode != 200 {
			t.Errorf("the control, a request that names no Workload, went to %q (%d %s), want the retry on b-0", got, r.StatusCode, b)
		}
	})
}

// A request that names no Workload, and a Workload with no sessionAffinity, are routed as before (Sticky routes: absent, a conversation is routed as
// before): no journal is made, and a conversation whose home is full is sent to the other ModelDeployment, then back to its home once it is free.
func TestWorkloadWithoutSessionAffinityRoutesAsBefore(t *testing.T) {
	f := newSkFix(t, skOpts{noAffinity: true, edit: func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }})
	s := f.session("ns/a-0", 0)
	release := f.hold("ns/a-0")
	held, _ := f.start(s)
	errWaitFor(t, "the held request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	f.engines["ns/b-0"].Set(enginetest.Behaviour{})
	if got, resp, body := f.chatAt(s); got != "ns/b-0" {
		t.Errorf("with its home full the conversation went to %q (%d %s), want b-0", got, resp.StatusCode, body)
	}
	release()
	held.wait(t)
	if got, resp, body := f.chatAt(s); got != "ns/a-0" {
		t.Errorf("with its home free again the conversation went to %q (%d %s), want a-0", got, resp.StatusCode, body)
	}
	if _, err := os.Stat(f.journalPath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a Workload with no sessionAffinity made a pin journal (stat: %v)", err)
	}
}

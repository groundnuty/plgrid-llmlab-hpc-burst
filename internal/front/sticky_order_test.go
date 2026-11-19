package front

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Waiting, fairness and the deadline of a sticky Workload (HBEP-5, Workloads, Sticky routes: Requests after the pin; Waiting). Every row holds its requests
// where the test wants them on the engines, and reads the order of arrival at an engine from the engine's own log.

// addBeta adds the Workload ns/beta of scModel, which has no sessionAffinity, beside alpha.
func (f *skFix) addBeta() {
	w := f.workload("ns", "beta", scModel)
	w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 100, 25
	f.apply()
}

// startBeta sends one chat request of beta under the session key on its own goroutine.
func (f *skFix) startBeta(session string) *scCall {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.t.Cleanup(cancel)
	return tbCall(f.harness, ctx, errChatBody(scModel+"#beta", false, "hello there"), "x-session-id", session)
}

// sessionsAt is the x-session-id of each request the engine of inst has received, in the order it received them.
func (f *skFix) sessionsAt(inst string) []string {
	var out []string
	for _, r := range f.engines[inst].Requests() {
		out = append(out, r.Header.Get("X-Session-Id"))
	}
	return out
}

// A pinned waiter is not starved by new conversations (Requests after the pin). a-0 holds one request of the conversation pinned to it, and a second request of
// the conversation waits for it. The first request ends and frees a-0, and the wake pass that would place the waiter is held. A request of a new conversation
// whose home is a-0, which arrives in that window, is sent to b-0, not to the slot the waiter is waiting for; released, the pass places the waiter on a-0. A
// route order that took the free slot for the new conversation would send it to a-0, and could do so again for each new conversation that came.
func TestStickyNewConversationLeavesAPinnedWaitersSlotToIt(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(time.Minute) }})
	gate := newFifoGate(t, f.router, true)
	s, fresh := f.session("ns/a-0", 0), f.session("ns/a-0", 1)
	f.at(s, "ns/a-0", "the first request of the pinned conversation")
	release := f.hold("ns/a-0")
	held, _ := f.start(s)
	errWaitFor(t, "a-0 to hold a request", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	waiter, _ := f.start(s)
	errWaitFor(t, "the pinned request to wait for a-0", func() bool { return f.waiting() == 1 })

	gate.arm()
	release() // the held request ends, a-0 is free, and the wake pass is held at the gate
	gate.waitEntered(t, 1)
	arrival, _ := f.start(fresh)
	resp, body := arrival.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/b-0" {
		t.Errorf("the new conversation, arriving while a-0 was free and a pinned request waited for it: %d from %q, want 200 from b-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	gate.release()
	for i, c := range []*scCall{held, waiter} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
			t.Errorf("request %d of the pinned conversation: %d from %q, want 200 from a-0: %s", i+1, resp.StatusCode, scInstanceOf(resp), body)
		}
	}
}

// A sticky Workload's scan past its blocked pinned head leaves the order across Workloads as it was (Waiting). alpha is sticky and beta is not; they share a-0
// and b-0, each at cap 1 and held full. alpha's request A, pinned to a-0, waits first; beta's request N waits second, for any route; alpha's request B, pinned
// to b-0, waits third. When b-0 frees, N, the older, takes it, and B after it. A scan that placed B past A without looking at beta's older head would give the
// slot to B and leave beta's strict FIFO behind it.
func TestStickyScanPastABlockedHeadDoesNotPassAnOlderWaiterOfAnotherWorkload(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(time.Minute) }})
	f.addBeta()
	sa, sb := f.session("ns/a-0", 0), f.session("ns/b-0", 0)
	f.at(sa, "ns/a-0", "alpha's conversation on a-0")
	f.at(sb, "ns/b-0", "alpha's conversation on b-0")
	releaseA, releaseB := f.hold("ns/a-0"), f.hold("ns/b-0")
	holdA, _ := f.start(sa)
	holdB, _ := f.start(sb)
	errWaitFor(t, "both Instances to hold a request", func() bool { return f.engines["ns/a-0"].Open() == 1 && f.engines["ns/b-0"].Open() == 1 })
	a, _ := f.start(sa)
	errWaitFor(t, "A to wait", func() bool { return f.waiting() == 1 })
	n := f.startBeta("beta-n")
	errWaitFor(t, "N to wait behind A", func() bool { return f.waiting() == 2 })
	b, _ := f.start(sb)
	errWaitFor(t, "B to wait behind N", func() bool { return f.waiting() == 3 })

	releaseB()
	for _, c := range []*scCall{holdB, n, b} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/b-0" {
			t.Fatalf("a request served by b-0: %d from %q: %s", resp.StatusCode, scInstanceOf(resp), body)
		}
	}
	// b-0 received alpha's first request, the held one, and then, by hand: beta's older N before alpha's B.
	if got := f.sessionsAt("ns/b-0"); len(got) != 4 || got[2] != "beta-n" || got[3] != sb {
		t.Errorf("b-0 received %v, want the first two of %s, then beta-n, then %s: the older head of the other Workload goes first", got, sb, sb)
	}
	releaseA()
	for _, c := range []*scCall{holdA, a} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
			t.Errorf("an a-0 request: %d from %q: %s", resp.StatusCode, scInstanceOf(resp), body)
		}
	}
}

// A request that waited for its conversation's key lock waits in the FIFO only for what is left of its queueTimeout (Waiting). Two first requests of one key
// arrive: the first is held between its target and its pin, and the second waits at the key's lock. The arrival clock reads an hour later once the second has
// been seen at the lock, so a deadline taken when it leaves the lock would be an hour later than its arrival's. The second then finds a-0 full, held by the
// first, and waits in the FIFO: the deadline it is handed is asserted itself, its arrival's reading plus queueTimeout.
func TestStickyKeyLockWaitIsPartOfQueueTimeout(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(10 * time.Minute) }})
	t0 := time.Now()
	var late atomic.Bool
	f.router.arrivalClock = func() time.Time {
		if late.Load() {
			return t0.Add(time.Hour)
		}
		return t0
	}
	deadlines := make(chan time.Time, 4)
	f.router.onQueueDeadline = func(d time.Time) { deadlines <- d }
	gate, entered := make(chan struct{}), make(chan struct{}, 4)
	var first atomic.Bool
	f.router.pins.beforeCreate = func() {
		entered <- struct{}{}
		if first.CompareAndSwap(false, true) {
			<-gate
		}
	}
	var open sync.Once
	t.Cleanup(func() { open.Do(func() { close(gate) }) })
	f.hold("ns/a-0") // the first request is held on a-0 once it is dispatched, and the second then finds a-0 full
	s := f.session("ns/a-0", 0)
	f.start(s)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first request never reached its pin")
	}
	f.start(s)
	errWaitFor(t, "the second request to reach the key's lock", func() bool { return f.keyLockRefs(s) == 2 })
	late.Store(true)
	open.Do(func() { close(gate) })
	select {
	case d := <-deadlines:
		if want := t0.Add(10 * time.Minute); !d.Equal(want) {
			t.Errorf("the second request's wait in the FIFO has the deadline %v, want its arrival's %v plus queueTimeout, %v", d, t0, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second request never began to wait in the FIFO")
	}
}

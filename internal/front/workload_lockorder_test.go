package front

import (
	"sync/atomic"
	"testing"
	"time"
)

// The lock order of the Workloads' FIFOs (the science agent's ruling Q6 on #764; workload_route.go, LOCK ORDER). From the outermost: passMu, a Workload's
// mutex, Router.mu, a group's mutex. A Workload's mutex is taken BEFORE any group's mutex, never the reverse, so a group delivers its wake to the Workload
// FIFOs (Group.OnFree) only AFTER it has released its own mutex. The row below forces the case with the router's hooks, with no timing window: a request
// holds alpha's mutex, a completion's wake needs it and waits, and the group's mutex must be free meanwhile.

// A completion on a group frees its slot and then wakes the Workload FIFOs, whose wake needs a Workload's mutex that another request holds: the wake waits in
// the completing goroutine, and the group's mutex is free while it waits.
//
// a-0 (cap 1) is held by H. W waits in alpha's FIFO (so a wake has a head to try, and takes alpha's mutex for it). A2, a second request of alpha, enters
// the choice and stops in Router.beforePick, which runs under alpha's mutex (A2 holds it there). H is cut: its completion releases a-0's slot, and its
// wake goes to alpha's head and finds the mutex held (onWlContended says so). At that moment the test takes a-0's group mutex with Group.Open, which must
// return within 5 s: had the wake run under the group's mutex it would hold it for ever, since the Workload's mutex is held until the gate opens. Then
// the gate opens: A2 queues behind W (strict FIFO: a younger request never passes the head), the wake places W on the freed slot, and when W is let go A2
// is placed on it. Both end 200 from a-0, and no count drifted.
func TestACompletionsWakeWaitsForAWorkloadsMutexWithTheGroupsMutexFree(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: func(c *Config) { c.QueueTimeout = Duration(10 * time.Minute) }})
	g := f.scGroup("ns/md-a")

	_, cutH := f.start(f.alpha(), "lo-h")
	wkSettledOurs(f, 1, 0)
	w, _ := f.start(f.alpha(), "lo-w")
	wkSettledOurs(f, 1, 1) // W is alpha's head

	inCritical, gate, contended := make(chan struct{}), make(chan struct{}), make(chan struct{}, 8)
	var first, once atomic.Bool
	t.Cleanup(func() {
		if once.CompareAndSwap(false, true) {
			close(gate)
		}
	})
	f.router.beforePick = func() { // A2's choice, under alpha's mutex: the only request that chooses from here on
		if first.CompareAndSwap(false, true) {
			close(inCritical)
			<-gate
		}
	}
	f.router.onWlContended = func() {
		select {
		case contended <- struct{}{}:
		default:
		}
	}
	a2, _ := f.start(f.alpha(), "lo-a2")
	select {
	case <-inCritical:
	case <-time.After(5 * time.Second):
		t.Fatal("A2 did not reach its choice")
	}

	cutH() // H's completion: the group's mutex is taken and released for its slot, and then the wake goes to alpha's head
	select {
	case <-contended:
	case <-time.After(5 * time.Second):
		t.Fatal("the completion's wake never found alpha's mutex held: H was not completed, or the wake did not try alpha's head")
	}
	// The wake waits for alpha's mutex now, in H's goroutine. Alpha's mutex is held by A2 until the gate opens, so the group's mutex is free only if the
	// wake was delivered after the group released it: the Workload's mutex is taken before the group's, never after.
	opened := make(chan int, 1)
	go func() { opened <- g.Open("ns/a-0") }()
	select {
	case n := <-opened:
		if n != 0 {
			t.Errorf("a-0 holds %d reservations after H's slot was released and nothing was placed, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the group's mutex is held while the wake waits for the Workload's mutex: the lock order is reversed")
	}

	if once.CompareAndSwap(false, true) {
		close(gate) // A2 goes on: it finds W in the FIFO and queues behind it
	}
	errWaitFor(t, "W to be placed on the freed slot", func() bool { return len(f.engines["ns/a-0"].Requests()) == 2 })
	wkWantRequests(t, "a-0 after the gate", f.engines["ns/a-0"], "lo-h", "lo-w")
	wkSettledOurs(f, 1, 1) // W holds a-0's one, and A2 waits behind it
	f.freeOurs()           // W and then A2 are let go
	for name, c := range map[string]*scCall{"W": w, "A2": a2} {
		resp, body := c.wait(t)
		if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
			t.Errorf("%s: status %d from %q, want 200 from ns/a-0: %s", name, resp.StatusCode, scInstanceOf(resp), body)
		}
	}
	wkWantRequests(t, "a-0 at the end", f.engines["ns/a-0"], "lo-h", "lo-w", "lo-a2")
	f.scNoDrift()
}

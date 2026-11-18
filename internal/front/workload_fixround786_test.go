package front

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The fix round of the reviews of #786 (PR 5 of HBEP-764): six rows, each with its hand mutant in the hand-back. Every expected value is typed by hand from
// the rule it names, and every wait is on observable state (the doubles' counts, the FIFO's length, the scraped series), never a window.

// r2Once closes a channel once, however many tests' cleanups and steps ask.
func r2Once(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// r2Sessions waits until the engine of inst has seen exactly these conversation keys, in this order.
func r2Sessions(t *testing.T, f *wrFix, inst string, want ...string) {
	t.Helper()
	errWaitFor(t, inst+" to have seen "+strings.Join(want, ", "), func() bool {
		got := fifoSessions(f, inst)
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	})
}

// A request for which no route may free is answered at once, as it would be alone, even behind a waiter (point 1, the reviews of Astra, Gemini and Opus).
// One Instance, a-0, at cap 1. R1 is held on it and its engine will answer 500 before any body byte, which the front retries to another Instance. Two
// requests, W1 and W2, wait in alpha's FIFO. R1's engine answers: R1 releases a-0, which goes to W1 (the head), and R1's retry excludes a-0, the only
// Instance: no route may free for it. It must relay its 500 as it came (HBEP-5, open question 5: with no other Instance ready the first failure is
// relayed, with no x-should-retry), not queue behind W2 to a 503 capacity at queueTimeout while the Instance sits idle. W2 then takes a-0 after W1, in order.
func TestARetryThatExcludesTheOnlyInstanceRelaysItsFailureEvenBehindWaiters(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, noManaged: true, edit: wkQueueLong})
	e := f.engines["ns/a-0"]
	failFirst, hold2 := make(chan struct{}), make(chan struct{})
	openFail, openHold2 := r2Once(failFirst), r2Once(hold2)
	t.Cleanup(openFail)
	t.Cleanup(openHold2)
	e.Set(enginetest.Behaviour{Hold: failFirst, Status: 500, ErrorBody: `{"error":{"message":"engine failed","type":"server_error"}}`})
	r1, _ := f.start(f.alpha(), "r2-r1")
	errWaitFor(t, "R1 to reach a-0", func() bool { return e.Open() == 1 })
	e.Set(enginetest.Behaviour{Hold: hold2}) // every later request is held until hold2
	w1, _ := f.start(f.alpha(), "r2-w1")
	fifoWaitState(t, f, 1)
	w2, _ := f.start(f.alpha(), "r2-w2")
	ws := fifoWaitState(t, f, 2)

	openFail() // R1's engine answers 500; its retry finds a-0 excluded and nothing else
	resp, body := r1.wait(t)
	if resp.StatusCode != 500 || !strings.Contains(body, "engine failed") {
		t.Fatalf("R1: status %d, body %s, want its 500 relayed as the engine sent it", resp.StatusCode, body)
	}
	if h := resp.Header.Get("X-Should-Retry"); h != "" {
		t.Errorf("R1: x-should-retry %q, want none: no retry was decided", h)
	}
	r2Sessions(t, f, "ns/a-0", "r2-r1", "r2-w1") // a-0 went to W1, the head, as R1 released it
	if got := ws.waiting.Load(); got != 1 {
		t.Errorf("alpha's FIFO holds %d requests, want 1 (W2)", got)
	}
	openHold2() // W1 completes, and a-0 goes to W2
	for i, c := range []*scCall{w1, w2} {
		if resp, body := c.wait(t); resp.StatusCode != 200 {
			t.Errorf("waiter %d: status %d: %s", i+1, resp.StatusCode, body)
		}
	}
	r2Sessions(t, f, "ns/a-0", "r2-r1", "r2-w1", "r2-w2")
}

// A request pinned to a provider with nothing eligible on it is refused no_ready_target at once, behind a non-empty FIFO as alone (point 1). a-0, on
// helios-a, is held, so a plain request waits. helios-b exists and has no Instance, so a request for model@helios-b#alpha has no route that may free.
func TestAProviderPinnedRequestWithNothingEligibleThereIsRefusedAtOnceBehindWaiters(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.batchProvider("helios-b", "helios-gh200")
	f.apply()
	f.start(f.alpha(), "r2-holder")
	wkSettledOurs(f, 1, 0)
	f.start(f.alpha(), "r2-waiter")
	ws := fifoWaitState(t, f, 1)
	pinned, _ := f.start(scModel+"@helios-b#alpha", "r2-pinned")
	resp, body := pinned.wait(t)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	if got := ws.waiting.Load(); got != 1 {
		t.Errorf("alpha's FIFO holds %d requests, want 1: the pinned request was never queued", got)
	}
}

// A head that no route can take any more leaves with no_ready_target and the next waiter is tried in the same pass (point 1). One ModelDeployment of two
// Instances, a-0 on helios-a and b-0 on helios-b, each at cap 1. b-0 is held. P, pinned to helios-b, waits as the head. a-0 is free, but U, a plain
// request, arrives behind P and queues (strict). b-0 then leaves: nothing is eligible for P any more, so P is answered no_ready_target and U is placed
// on the free a-0 in the same pass.
func TestAHeadThatNoRouteCanTakeLeavesWithNoReadyTargetAndTheNextWaiterIsPlaced(t *testing.T) {
	f := newFifoTwoProviderFix(t)
	w := f.workload("ns", "alpha", scModel)
	w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 100, 25
	f.apply()
	holdB := f.engines["ns/b-0"]
	f.engines["ns/a-0"].Set(enginetest.Behaviour{}) // a-0 answers at once
	holdBChan := make(chan struct{})
	t.Cleanup(r2Once(holdBChan))
	holdB.Set(enginetest.Behaviour{Hold: holdBChan})
	hb, _ := f.start(scModel+"@helios-b#alpha", "r2-hb")
	errWaitFor(t, "the holder to reach b-0", func() bool { return holdB.Open() == 1 })
	_ = hb
	p, _ := f.start(scModel+"@helios-b#alpha", "r2-p")
	ws := fifoWaitState(t, f, 1)
	u, _ := f.start(f.alpha(), "r2-u")
	ws = fifoWaitState(t, f, 2)
	if n := len(f.engines["ns/a-0"].Requests()); n != 0 {
		t.Fatalf("a-0 saw %d requests before b-0 left, want 0: U must wait behind P although a-0 is free", n)
	}

	srDropInstance(t, f, f.uids["ns/b-0"])
	f.apply() // b-0 leaves its group, with P pinned to it as the head
	resp, body := p.wait(t)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	if resp, body := u.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("U: status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	if got := ws.waiting.Load(); got != 0 {
		t.Errorf("alpha's FIFO holds %d requests, want 0", got)
	}
}

// A head whose serving set has gone leaves with no_ready_target at once (point 1): its ModelDeployment is deleted while it waits.
func TestAHeadWhoseServingSetHasGoneLeavesWithNoReadyTarget(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.start(f.alpha(), "r2-holder")
	wkSettledOurs(f, 1, 0)
	waiter, _ := f.start(f.alpha(), "r2-waiter")
	fifoWaitState(t, f, 1)
	f.snap.Deployments, f.snap.Instances = nil, nil // md-a and its Instance are deleted
	f.apply()
	resp, body := waiter.wait(t)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
}

// Q5 after a cancel (point 2): with arrivals A1 (alpha), B2 (beta) and A3 (alpha) waiting, a pass orders alpha before beta on A1's arrival. A1 gives
// up between that order and the lock of alpha's FIFO, so alpha's oldest request is A3 now, younger than B2: B2 must take the one free slot, not A3.
// The seam holds the pass between its ordering and its first lock, deterministically.
func TestAFreedSlotGoesToTheOlderFifoWhenTheOrderWentStaleThroughACancel(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	b := f.workload("ns", "beta", scModel)
	b.Spec.Shape.ContextTokens.Typical, b.Spec.Shape.OutputTokens.Typical = 100, 25
	f.apply()
	_, cutHolder := f.start(f.alpha(), "r2-holder")
	wkSettledOurs(f, 1, 0)
	_, cutA1 := f.start(f.alpha(), "r2-a1")
	alpha := fifoWaitState(t, f, 1)
	f.start(scModel+"#beta", "r2-b2")
	errWaitFor(t, "beta's FIFO to hold B2", func() bool { return f.waiting() == 2 })
	a3, cutA3 := f.start(f.alpha(), "r2-a3")
	_ = a3
	fifoWaitState(t, f, 2)

	var once sync.Once
	f.router.afterSort = func() {
		once.Do(func() {
			cutA1() // A1 gives up after the order was taken
			errWaitFor(t, "A1 to leave alpha's FIFO", func() bool { return alpha.waiting.Load() == 1 })
		})
	}
	cutHolder() // a-0 frees: the pass orders alpha (A1) before beta (B2)
	r2Sessions(t, f, "ns/a-0", "r2-holder", "r2-b2")
	if got := alpha.waiting.Load(); got != 1 {
		t.Errorf("alpha's FIFO holds %d requests, want 1 (A3)", got)
	}
	cutA3()
}

// A Workload deleted and created again under the same name starts with none of the deleted one's state (point 3). alpha is OwnFirst with managedSessions 1;
// a-0 is full and a request M1 overflows to the managed target, so alpha's count is 1. alpha is deleted and created again (a new UID, managedSessions 1):
// a request N1 of the replacement must take its own idle managed route at once, and M1's release must not touch the replacement's count.
func TestAReplacementWorkloadStartsWithNoneOfTheDeletedOnesManagedCount(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1, edit: wkQueueLong})
	f.start(f.alpha(), "r2-holder")
	f.settled(0, 1, 0)
	_, cutM1 := f.start(f.alpha(), "r2-m1")
	f.settled(1, 1, 0) // M1 is on the managed target: the old Workload's count is 1
	oldState := f.wlState()

	f.dropWorkload("ns", "alpha")
	nw := f.workload("ns", "alpha", scModel)
	nw.Spec.Shape.ContextTokens.Typical, nw.Spec.Shape.OutputTokens.Typical = 100, 25
	one := int32(1)
	nw.Spec.ManagedSessions = &one
	f.apply()
	f.start(f.alpha(), "r2-n1")
	f.settled(2, 1, 0) // N1 reached the managed target at once: the replacement's count was 0
	if f.wlState() == oldState {
		t.Fatalf("the replacement Workload shares the deleted one's state")
	}
	if got := f.managedCount(); got != 1 {
		t.Fatalf("the replacement's count is %d, want 1 (N1)", got)
	}

	cutM1() // M1 ends: the deleted Workload's count falls, the replacement's does not
	errWaitFor(t, "the deleted Workload's state to be dropped", func() bool {
		f.router.wlMu.Lock()
		defer f.router.wlMu.Unlock()
		return len(f.router.wls) == 1
	})
	if got := f.managedCount(); got != 1 {
		t.Errorf("the replacement's count is %d after M1 ended, want 1", got)
	}
}

// A retry holding a deleted Workload's resolution recreates no live state (point 4). R is held on a-0, which will close its connection before any byte, so
// R is retried to a-1. Meanwhile alpha is deleted and its state pruned. R's retry asks for the state again: it is made an orphan, so the pass that ends the
// retry drops it, leaving no registry entry and no waiting series for alpha.
func TestARetryOfADeletedWorkloadsRequestLeavesNoStateAndNoWaitingSeriesBehind(t *testing.T) {
	f := newWrFix(t, wrOpts{instances: []string{"a-0", "a-1"}, noManaged: true, edit: wkQueueLong})
	ring := []string{"ns/a-0", "ns/a-1"}
	session := scKeys(t, ring, "ns/a-0", 1)[0]
	closeIt := make(chan struct{})
	openClose := r2Once(closeIt)
	t.Cleanup(openClose)
	f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseAfterHold: closeIt})
	r, _ := f.start(f.alpha(), session)
	errWaitFor(t, "R to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	if f.wlState() == nil {
		t.Fatal("alpha has no state although R named it")
	}

	f.dropWorkload("ns", "alpha")
	f.apply()
	errWaitFor(t, "alpha's idle state to be pruned", func() bool { return f.wlState() == nil })

	openClose() // a-0 closes: R is retried to a-1, holding alpha's resolution
	resp, body := r.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-1" {
		t.Fatalf("R: status %d from %q, want 200 from ns/a-1: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	errWaitFor(t, "the state of the deleted Workload not to be left behind", func() bool { return f.router.wlN.Load() == 0 })
	if f.wlState() != nil {
		t.Errorf("alpha's state is registered after its retry ended")
	}
	srNoLabel(t, scrape(t, f.metrics), srWaiting, "workload", "alpha")
}

// The queue deadline is taken at arrival (point 5). A request that no route can take is queued, and a wake that came meanwhile makes it run a pass of its
// own, which the seam holds. The arrival clock advances an hour at each reading, so a deadline taken after the pass is an hour later than the arrival's:
// the deadline the wait is handed is asserted itself, equal to the first reading plus queueTimeout, never an elapsed time.
func TestTheQueueDeadlineIsTakenAtArrivalBeforeTheRequestsOwnWakePass(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	f.start(f.alpha(), "r2-holder")
	wkSettledOurs(f, 1, 0)

	t0 := time.Now()
	var reads atomic.Int64
	f.router.arrivalClock = func() time.Time { return t0.Add(time.Duration(reads.Add(1)-1) * time.Hour) }
	deadlines := make(chan time.Time, 4)
	f.router.onQueueDeadline = func(d time.Time) { deadlines <- d }
	entered, gate := make(chan struct{}), make(chan struct{})
	openGate := r2Once(gate)
	t.Cleanup(openGate)
	var armed atomic.Bool
	armed.Store(true)
	f.router.beforeEnqueue = func() { f.router.wakeGen.Add(1) } // a wake request came while the arrival decided
	f.router.beforeWake = func() {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-gate
		}
	}
	_, cut := f.start(f.alpha(), "r2-late")
	t.Cleanup(cut)
	<-entered // the arrival's own pass is held, after it queued and before it waits
	openGate()
	select {
	case d := <-deadlines:
		if want := t0.Add(10 * time.Minute); !d.Equal(want) {
			t.Errorf("the wait's deadline is %v, want the arrival's %v plus queueTimeout, %v", d, t0, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the request never began to wait")
	}
}

// The in-flight gauge releases under the labels its reservation was counted under (point 6). R1 is held on ns/a-0 at UID u1. The Instance is replaced by
// one of the same name and UID u2, and R2 is held on it. R1's release brings u1's series to 0 and drops it, and leaves u2's at 1, never changing it; R2's
// release brings u2's to 0, never -1.
func TestAReleaseOfAReplacedInstancesRequestLeavesTheReplacementsSeriesAlone(t *testing.T) {
	f := newWrFix(t, wrOpts{holdOurs: true, noManaged: true, edit: srLongQueue})
	u1 := f.srUID("ns/a-0")
	labels := func(uid string) []string {
		return []string{"workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid}
	}
	c1, _ := f.start(f.alpha(), "r2-1")
	errWaitFor(t, "R1 to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	wantSample(t, scrape(t, f.metrics), 1, srInFlight, labels(u1)...)

	srDropInstance(t, f, f.uids["ns/a-0"])
	key := newKey(t)
	e2 := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: f.clk})
	hold2 := make(chan struct{})
	openHold2 := r2Once(hold2)
	t.Cleanup(openHold2)
	e2.Set(enginetest.Behaviour{Hold: hold2})
	u2 := string(f.instance("md-a", "a-0", "helios-a", e2, key, instanceOpts{}))
	f.apply() // the same member name, another UID
	c2, _ := f.start(f.alpha(), "r2-2")
	errWaitFor(t, "R2 to reach the replacement", func() bool { return e2.Open() == 1 })
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, labels(u1)...)
	wantSample(t, ss, 1, srInFlight, labels(u2)...)

	f.freeOurs() // R1, on the old Instance, ends
	srDone(t, f, c1)
	ss = scrape(t, f.metrics)
	srNoLabel(t, ss, srInFlight, "instance_uid", u1)
	wantSample(t, ss, 1, srInFlight, labels(u2)...)

	openHold2()
	srDone(t, f, c2)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srInFlight, labels(u2)...)
	srNoLabel(t, ss, srInFlight, "instance_uid", u1)
}

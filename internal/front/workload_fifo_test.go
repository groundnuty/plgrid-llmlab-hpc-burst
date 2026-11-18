package front

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The Workload's own FIFO (HBEP-5, Workloads, Route order and Waiting; HBEP-764's PR 5, the science agent's rulings Q5, Q9 and Q10 on #764): a request that
// names a Workload and finds no route waits in the Workload's FIFO, in arrival order, only its head is placed, and a request that arrives while the FIFO
// holds one queues behind it without trying a route, however free a route is at that instant. A request that names no Workload keeps the
// ModelDeployment's group queue, which is not strict. Every request holds its own session key, which the front sends to the engine as x-session-id, so the
// engine's own log says which request a freed Instance was given: no row reads the front's bookkeeping for where a request went. Waits are on the engine's
// log, the router's count of waiters and the series, never on a window; queueTimeout is ten minutes except in the timeout row. Every name here starts with
// fifo so that it cannot meet another file's.

// fifoLong sets queueTimeout to ten minutes, so no waiter in a row gives up by itself.
func fifoLong(c *Config) { c.QueueTimeout = Duration(10 * time.Minute) }

// fifoWaitingSeries is hpcburst_front_waiting (HBEP-8, Front metrics): its samples carry workload and model_deployment, and exactly one of the two is empty.
const fifoWaitingSeries = "hpcburst_front_waiting"

// fifoSessions is the x-session-id of each request the engine of inst has received, in the order it received them.
func fifoSessions(f *wrFix, inst string) []string {
	var out []string
	for _, r := range f.engines[inst].Requests() {
		out = append(out, r.Header.Get("X-Session-Id"))
	}
	return out
}

// fifoWaitSessions waits until the engine of inst has received n requests, and returns the sessions of them. A request that never comes is the failure.
func fifoWaitSessions(t *testing.T, f *wrFix, inst string, n int) []string {
	t.Helper()
	errWaitFor(t, "the engine of "+inst+" to receive request number "+string(rune('0'+n)), func() bool { return len(f.engines[inst].Requests()) >= n })
	return fifoSessions(f, inst)
}

// fifoSettled waits until open requests are open on our engines and waiting wait at the front. Unlike wrFix.settled it reads nothing of the LLMLab double,
// which a fixture with no managed target does not have.
func fifoSettled(t *testing.T, f *wrFix, open, waiting int) {
	t.Helper()
	errWaitFor(t, "the requests to settle", func() bool { return f.openOurs() == open && f.waiting() == waiting })
}

// fifoWaitState waits until alpha's state exists and has n requests in its FIFO, and returns the state.
func fifoWaitState(t *testing.T, f *wrFix, n int64) *wlState {
	t.Helper()
	var ws *wlState
	errWaitFor(t, "alpha's FIFO to hold the requests", func() bool {
		ws = f.wlState()
		return ws != nil && ws.waiting.Load() == n
	})
	return ws
}

// fifoGate blocks a wake pass of the router at its start, before the pass takes any lock (Router.beforeWake). It is installed before the first request of a
// test, so no goroutine of the front reads the seam while the test writes it, and does nothing until armed. With once set, only the first pass that finds
// it armed blocks, and later passes go on; with once clear every pass that begins while it is armed blocks until the gate opens.
type fifoGate struct {
	armed   atomic.Bool
	once    bool
	entered atomic.Int32
	open    chan struct{}
	opened  sync.Once
}

func newFifoGate(t *testing.T, r *Router, once bool) *fifoGate {
	t.Helper()
	g := &fifoGate{once: once, open: make(chan struct{})}
	r.beforeWake = func() {
		if !g.armed.Load() {
			return
		}
		if g.once && !g.armed.CompareAndSwap(true, false) {
			return
		}
		g.entered.Add(1)
		<-g.open
	}
	t.Cleanup(g.release) // a test that fails with a pass blocked must not leave it so
	return g
}

func (g *fifoGate) arm()     { g.armed.Store(true) }
func (g *fifoGate) release() { g.opened.Do(func() { g.armed.Store(false); close(g.open) }) }

// waitEntered waits until n wake passes are blocked at the gate.
func (g *fifoGate) waitEntered(t *testing.T, n int32) {
	t.Helper()
	errWaitFor(t, "the wake passes to reach the gate", func() bool { return g.entered.Load() >= n })
}

// Arrival order (HBEP-5, Workloads, Waiting: the Workload's own FIFO; Q5). One Instance at cap 1 holds H, and three more requests of alpha arrive one at a
// time, each started only once the FIFO shows the one before it: W2, W3, W4. Each completion frees the Instance, and it goes to the head: by hand the
// Instance is given H, W2, W3, W4 in that order. A FIFO with one waiter proves nothing, so three wait.
func TestTheWorkloadFIFOGivesAFreedInstanceToTheOldestWaiterEachTime(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: fifoLong})
	_, cutH := f.start(f.alpha(), "fifo-h")
	fifoSettled(t, f, 1, 0) // H holds a-0's one slot
	var cuts []func()
	for i, session := range []string{"fifo-w2", "fifo-w3", "fifo-w4"} {
		_, cut := f.start(f.alpha(), session)
		cuts = append(cuts, cut)
		fifoWaitState(t, f, int64(i+1)) // the next one is started only once this one sits in the FIFO
	}
	if got := fifoSessions(f, "ns/a-0"); len(got) != 1 || got[0] != "fifo-h" {
		t.Fatalf("a-0 has received %v with three requests waiting, want only [fifo-h]", got)
	}

	cutH() // the holder is cut by its client: a-0 is free, and its oldest waiter takes it
	if got := fifoWaitSessions(t, f, "ns/a-0", 2); got[1] != "fifo-w2" {
		t.Fatalf("a-0 was given %v after H ended, want fifo-w2 second: the oldest waiter takes it", got)
	}
	fifoWaitState(t, f, 2)
	cuts[0]() // W2 ends
	if got := fifoWaitSessions(t, f, "ns/a-0", 3); got[2] != "fifo-w3" {
		t.Fatalf("a-0 was given %v after W2 ended, want fifo-w3 third", got)
	}
	fifoWaitState(t, f, 1)
	cuts[1]() // W3 ends
	if got := fifoWaitSessions(t, f, "ns/a-0", 4); got[3] != "fifo-w4" {
		t.Fatalf("a-0 was given %v after W3 ended, want fifo-w4 fourth", got)
	}
	fifoWaitState(t, f, 0)
	if got, want := strings.Join(fifoSessions(f, "ns/a-0"), ","), "fifo-h,fifo-w2,fifo-w3,fifo-w4"; got != want {
		t.Errorf("a-0 received %s, want %s", got, want)
	}
	cuts[2]()
	fifoSettled(t, f, 0, 0)
}

// Strictness at the instant a route frees (Q5: strict per-Workload FIFO, only the head is placed). H holds a-0 (cap 1) and W2 waits. A wake pass is held
// at its start: H is cut, its completion frees a-0 and its goroutine blocks before it looks at the FIFO, so for a while a-0 is FREE and W2 has not been
// placed. A3 of the same Workload arrives then. By the rule it queues behind W2 without trying a route, though a-0 is free: the FIFO holds two. A FIFO
// that was not strict would give A3 the free slot at once, the count would never reach two, and the engine would see A3 before W2. When the pass is let
// go, W2 is placed first, and A3 is placed when W2 ends: by hand, a-0 is given H, W2, A3.
func TestARequestThatArrivesWhileARouteIsFreeStillQueuesBehindTheOlderWaiter(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: fifoLong})
	gate := newFifoGate(t, f.router, true)
	_, cutH := f.start(f.alpha(), "fifo-h")
	fifoSettled(t, f, 1, 0)
	_, cutW2 := f.start(f.alpha(), "fifo-w2")
	ws := fifoWaitState(t, f, 1)

	gate.arm()
	cutH()
	gate.waitEntered(t, 1) // the completion has freed a-0 and its goroutine is held before the FIFO is looked at
	errWaitFor(t, "a-0 to be free", func() bool { return f.openOurs() == 0 })
	if n := f.scLoad("ns/md-a", "ns/a-0"); n != 0 {
		t.Fatalf("a-0 carries %d requests with the pass held, want 0: its slot is free", n)
	}
	if got := fifoSessions(f, "ns/a-0"); len(got) != 1 {
		t.Fatalf("a-0 has received %v, want only fifo-h: W2 is not placed while the pass is held", got)
	}

	f.start(f.alpha(), "fifo-a3")
	// The wait ends when A3 is queued behind W2 or, in a front that is not strict, when the engine has received A3.
	errWaitFor(t, "A3 to be queued or placed", func() bool { return ws.waiting.Load() == 2 || len(fifoSessions(f, "ns/a-0")) > 1 })
	if got := fifoSessions(f, "ns/a-0"); len(got) != 1 {
		t.Fatalf("a-0 has received %v while W2 waits: A3 took the free slot past the older waiter, so the FIFO is not strict", got)
	}
	if n := ws.waiting.Load(); n != 2 {
		t.Fatalf("the FIFO holds %d, want 2: W2 and A3", n)
	}
	if n := f.scLoad("ns/md-a", "ns/a-0"); n != 0 {
		t.Fatalf("a-0 carries %d requests, want 0: A3 queued behind W2 and took nothing", n)
	}

	gate.release()
	if got := fifoWaitSessions(t, f, "ns/a-0", 2); got[1] != "fifo-w2" {
		t.Fatalf("a-0 was given %v when the pass went on, want fifo-w2 second: the head is placed first", got)
	}
	fifoWaitState(t, f, 1) // A3 waits
	cutW2()
	if got := fifoWaitSessions(t, f, "ns/a-0", 3); got[2] != "fifo-a3" {
		t.Fatalf("a-0 was given %v after W2 ended, want fifo-a3 third", got)
	}
}

// The timeout (HBEP-5, Waiting; Q10): the one row where a timer is the thing under test, queueTimeout 300 ms. a-0 is full, one request of alpha waits in
// the Workload's FIFO and is refused with 503 capacity and Retry-After 10, its message naming the Workload alpha and no ModelDeployment. Its usage record
// carries workload alpha, a modelDeployment of none and status 503, and the front's requests series has workload "alpha" and an empty model_deployment.
// Afterwards nothing waits, in the Workload's FIFO or at the front.
func TestAWorkloadRequestThatWaitsPastQueueTimeoutIsRefusedAsCapacityAndLeavesTheFIFO(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }})
	_, cutH := f.start(f.alpha(), "fifo-h")
	fifoSettled(t, f, 1, 0)
	waiter, _ := f.start(f.alpha(), "fifo-w2")
	ws := fifoWaitState(t, f, 1)

	resp, body := waiter.wait(t) // past queueTimeout
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	if !strings.Contains(body, "Workload alpha") || strings.Contains(body, "md-a") {
		t.Errorf("the error does not name the Workload alpha, or names a ModelDeployment: %s", body)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	wantWorkload(t, "refused after the wait", rec, str("alpha"))
	if rec.Status != 503 || rec.ModelDeployment != "" {
		t.Errorf("record: status %d modelDeployment %q, want 503 and none", rec.Status, rec.ModelDeployment)
	}
	wantSample(t, scrape(t, f.metrics), 1, mRequests, "code", "503", "endpoint", "chat", "provider", "", "model_deployment", "", "workload", "alpha")
	errWaitFor(t, "the FIFO to be empty", func() bool { return ws.waiting.Load() == 0 && f.waiting() == 0 })
	if n := ws.waiting.Load(); n != 0 {
		t.Errorf("alpha's FIFO holds %d after the refusal, want 0", n)
	}
	if n := f.router.Status().Waiting; n != 0 {
		t.Errorf("Status().Waiting is %d after the refusal, want 0", n)
	}
	cutH()
	fifoSettled(t, f, 0, 0)
}

// Control, the group queue is unchanged (HBEP-5, Workloads, Waiting): a request that names no Workload waits in its ModelDeployment's group queue, never
// in a Workload's FIFO. a-0 is at cap 1 holding one unnamed request and there is no managed target to overflow to; the second unnamed request waits. By
// hand: the group ns/md-a has one waiter, no Workload state was made, and the waiting series has one sample, workload "" with model_deployment ns/md-a.
func TestAnUnnamedWaiterSitsInTheModelDeploymentsGroupQueueAndMakesNoWorkloadState(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: fifoLong})
	f.start(scModel, "fifo-h")
	fifoSettled(t, f, 1, 0)
	f.start(scModel, "fifo-u2")
	g := f.scGroup("ns/md-a")
	errWaitFor(t, "the unnamed request to wait", func() bool { return g.Waiting() == 1 })
	if n := g.Waiting(); n != 1 {
		t.Errorf("the group queue holds %d, want 1", n)
	}
	if ws := f.wlState(); ws != nil {
		t.Errorf("an unnamed request made a Workload state with %d waiting", ws.waiting.Load())
	}
	if n := f.waiting(); n != 1 {
		t.Errorf("Status().Waiting is %d, want 1", n)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, fifoWaitingSeries, "workload", "", "model_deployment", "ns/md-a")
}

// Control: a request that names a Workload waits in the Workload's FIFO and not in the ModelDeployment's group queue. With the same fixture, one request
// of alpha holds a-0 and the second waits: by hand alpha's FIFO has one and the group queue has none, and the waiting series has one sample, workload
// "alpha" with an empty model_deployment, beside the group's own, which is 0.
func TestANamedWaiterSitsInTheWorkloadFIFOAndNotInTheGroupQueue(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: fifoLong})
	f.start(f.alpha(), "fifo-h")
	fifoSettled(t, f, 1, 0)
	f.start(f.alpha(), "fifo-w2")
	ws := fifoWaitState(t, f, 1)
	g := f.scGroup("ns/md-a")
	if n := g.Waiting(); n != 0 {
		t.Errorf("the group queue holds %d, want 0: the waiter names a Workload", n)
	}
	if n := ws.waiting.Load(); n != 1 {
		t.Errorf("alpha's FIFO holds %d, want 1", n)
	}
	if n := f.waiting(); n != 1 {
		t.Errorf("Status().Waiting is %d, want 1", n)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, fifoWaitingSeries, "workload", "alpha", "model_deployment", "")
	wantSample(t, ss, 0, fifoWaitingSeries, "workload", "", "model_deployment", "ns/md-a")
}

// Control, the group queue is still not strict (queue.go placeWaitersLocked: a younger waiter whose filter admits a member with room is placed past an
// older one that cannot be). One ModelDeployment, ns/md-a, has two Instances at cap 1, a-0 on Provider helios-a and b-0 on Provider helios-b, each held
// by a request pinned to its Provider (H-a, H-b). Two unnamed requests then wait in the group's queue, oldest first: A names the level helios-a, so only
// a-0 may take it, and B names no level, so either may. H-b is cut: b-0 is free. By hand: A cannot use b-0 and keeps its place, B is placed on b-0 past it,
// the group queue holds one (A); when H-a is cut, a-0 goes to A.
func TestTheGroupQueueStillPlacesAYoungerWaiterPastAnOlderOneThatCannotUseTheFreeInstance(t *testing.T) {
	f := newFifoTwoProviderFix(t)
	g := f.scGroup("ns/md-a")
	_, cutHa := f.start(scModel+"@helios-a", "fifo-ha")
	errWaitFor(t, "H-a to hold a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	_, cutHb := f.start(scModel+"@helios-b", "fifo-hb")
	errWaitFor(t, "H-b to hold b-0", func() bool { return f.engines["ns/b-0"].Open() == 1 })
	f.start(scModel+"@helios-a", "fifo-A")
	errWaitFor(t, "A to wait", func() bool { return g.Waiting() == 1 })
	f.start(scModel, "fifo-B")
	errWaitFor(t, "B to wait behind A", func() bool { return g.Waiting() == 2 })
	for inst, want := range map[string]int{"ns/a-0": 1, "ns/b-0": 1} {
		if got := len(f.engines[inst].Requests()); got != want {
			t.Fatalf("%s has received %d requests with both Instances full, want %d", inst, got, want)
		}
	}

	cutHb() // b-0 is free; A may not use it, B may
	errWaitFor(t, "B to reach b-0", func() bool { return len(f.engines["ns/b-0"].Requests()) == 2 })
	if got := f.engines["ns/b-0"].Requests()[1].Header.Get("X-Session-Id"); got != "fifo-B" {
		t.Fatalf("b-0 was given %q after H-b ended, want fifo-B: the younger waiter is placed past A", got)
	}
	errWaitFor(t, "A to wait alone", func() bool { return g.Waiting() == 1 })
	if got := len(f.engines["ns/a-0"].Requests()); got != 1 {
		t.Errorf("a-0 has received %d requests, want only H-a's: A still waits", got)
	}

	cutHa() // a-0 is free, and A takes it
	errWaitFor(t, "A to reach a-0", func() bool { return len(f.engines["ns/a-0"].Requests()) == 2 })
	if got := f.engines["ns/a-0"].Requests()[1].Header.Get("X-Session-Id"); got != "fifo-A" {
		t.Errorf("a-0 was given %q after H-a ended, want fifo-A", got)
	}
	if n := g.Waiting(); n != 0 {
		t.Errorf("the group queue holds %d, want 0", n)
	}
}

// newFifoTwoProviderFix is ns/md-a with two Instances at routing.instanceCap 1, a-0 on the batch Provider helios-a and b-0 on helios-b, each on its own
// engine double that holds every request until the test is over. It is a wrFix with no managed target and no Workload, whose start sends a request.
func newFifoTwoProviderFix(t *testing.T) *wrFix {
	t.Helper()
	h := newHarness(t, fifoLong)
	pw := &pwFix{harness: h, engines: map[string]*enginetest.Engine{}, uids: map[string]types.UID{}}
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.batchProvider("helios-b", "helios-gh200")
	md := h.servedMD("md-a", scModel, 2, "helios-a", "helios-b")
	one := int32(1)
	md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &one}
	hold := make(chan struct{})
	for _, in := range []struct{ name, provider string }{{"a-0", "helios-a"}, {"b-0", "helios-b"}} {
		key := newKey(t)
		e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
		e.Set(enginetest.Behaviour{Hold: hold})
		pw.uids["ns/"+in.name] = h.instance("md-a", in.name, in.provider, e, key, instanceOpts{})
		pw.engines["ns/"+in.name] = e
	}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(hold) }) })
	h.apply()
	return &wrFix{pwFix: pw, hold: hold}
}

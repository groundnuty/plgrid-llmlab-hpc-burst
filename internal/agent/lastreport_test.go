package agent_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The last report must not be overtaken by a periodic report already in
// flight (issue #220): on 2 Nov 2026 the endpoint kept worker phase stopping
// after the agent had stopped, because a report sent just before the stop
// reached the endpoint just after the last one. The agent lets that send
// finish, without cancelling it, and only then sends the last report, for as
// long as finalReportTimeout; past the bound it sends the last report anyway
// and logs a warning (science agent's ruling, 2 Nov 2026: option 2 for alpha).
// What the wait cannot catch, a send that outlasts the bound, is caught by the
// count on every report (issue #221): the Backend drops the older report.
//
// heldBackend is the double that makes the race deterministic. It holds one
// Report at the "server": the caller's context ending makes the call return at
// once, as a client that gives up does, but the request has already left, so
// the held report lands only when the test releases it. A double that blocked
// until released regardless of the context would make the old code wait too,
// and the test would pass without the fix.
type heldBackend struct {
	agentapi.Backend
	mu      sync.Mutex
	armed   bool
	held    bool
	heldCtx context.Context
	// holding is closed when a report is held; release lets it land.
	holding chan struct{}
	release chan struct{}
	// cancelledAtRelease says whether the held report's context had ended when
	// the test released it: the agent let it finish, or it did not.
	cancelledAtRelease bool
	released           bool
	// failure, when set at release, is what the held call returns: the server
	// does not take the report.
	failure error
	// entered carries every Report call that came in while one was held.
	entered chan agentapi.Report
	// lateLanded is closed when the held report has reached the Backend after
	// the release, whether its caller waited for the release or had given up
	// (the two can both be ready when the held call first looks).
	lateLanded chan struct{}
	lateOnce   sync.Once
	// failing, once set by failFrom, is what every other Report call returns,
	// and failed counts them.
	failing error
	failed  int
}

func newHeldBackend() *heldBackend {
	return &heldBackend{holding: make(chan struct{}), release: make(chan struct{}), entered: make(chan agentapi.Report, 8), lateLanded: make(chan struct{})}
}

// arm makes the next Report call the held one.
func (b *heldBackend) arm() {
	b.mu.Lock()
	b.armed = true
	b.mu.Unlock()
}

// open says whether a report is held now.
func (b *heldBackend) open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.held
}

// releaseHeld lets the held report land. It does nothing when none is held or
// it was released.
func (b *heldBackend) releaseHeld() { b.releaseWith(nil) }

// releaseWith ends the hold: with err nil the report lands, otherwise the call
// fails with err and nothing lands.
func (b *heldBackend) releaseWith(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.held || b.released {
		return
	}
	b.released, b.failure = true, err
	b.cancelledAtRelease = b.heldCtx.Err() != nil
	b.held = false
	close(b.release)
}

func (b *heldBackend) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	b.mu.Lock()
	switch {
	case b.armed:
		b.armed, b.held, b.heldCtx = false, true, ctx
		release := b.release
		close(b.holding)
		b.mu.Unlock()
		select {
		case <-release:
			b.mu.Lock()
			err := b.failure
			b.mu.Unlock()
			if err != nil {
				return err
			}
			defer b.lateOnce.Do(func() { close(b.lateLanded) })
			return b.Backend.Report(ctx, c, r)
		case <-ctx.Done():
			// The caller gave up; the request had left, so the server takes it
			// when it can.
			go func() {
				<-release
				b.mu.Lock()
				err := b.failure
				b.mu.Unlock()
				if err == nil {
					b.Backend.Report(context.Background(), c, r)
				}
				b.lateOnce.Do(func() { close(b.lateLanded) })
			}()
			return ctx.Err()
		}
	case b.held:
		select {
		case b.entered <- r:
		default:
		}
	}
	if err := b.failing; err != nil {
		b.failed++
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()
	return b.Backend.Report(ctx, c, r)
}

// failFrom makes every Report call after this one that is not held fail with err.
func (b *heldBackend) failFrom(err error) {
	b.mu.Lock()
	b.failing = err
	b.mu.Unlock()
}

// failedCalls says how many calls failFrom failed.
func (b *heldBackend) failedCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failed
}

// logRecorder records the agent's log records, for tests that assert on what
// the agent logged, not on its text output. seen(msg) is closed once a record
// with that message has come in.
type logRecorder struct {
	mu   sync.Mutex
	recs []logRecord
	sig  map[string]chan struct{}
}

type logRecord struct {
	level slog.Level
	msg   string
}

func newLogRecorder() *logRecorder { return &logRecorder{sig: map[string]chan struct{}{}} }

func (r *logRecorder) signal(msg string) chan struct{} {
	c, ok := r.sig[msg]
	if !ok {
		c = make(chan struct{})
		r.sig[msg] = c
	}
	return c
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, logRecord{rec.Level, rec.Message})
	if c := r.signal(rec.Message); !closed(c) {
		close(c)
	}
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

func closed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// seen returns a channel that is closed once msg has been logged.
func (r *logRecorder) seen(msg string) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.signal(msg)
}

// count returns how many records at level carried msg.
func (r *logRecorder) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.level == level && rec.msg == msg {
			n++
		}
	}
	return n
}

// teeHandler sends each record to every handler.
type teeHandler struct{ hs []slog.Handler }

func teeHandlers(a, b slog.Handler) slog.Handler {
	if b == nil {
		return a
	}
	return &teeHandler{hs: []slog.Handler{a, b}}
}

func (t *teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range t.hs {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (t *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range t.hs {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (t *teeHandler) WithAttrs(as []slog.Attr) slog.Handler {
	out := &teeHandler{}
	for _, h := range t.hs {
		out.hs = append(out.hs, h.WithAttrs(as))
	}
	return out
}

func (t *teeHandler) WithGroup(name string) slog.Handler {
	out := &teeHandler{}
	for _, h := range t.hs {
		out.hs = append(out.hs, h.WithGroup(name))
	}
	return out
}

// fakeTimers puts the test in control of the timers of the waits at the
// agent's end (agent.SetTimerForTest): the agent's timer never fires by
// itself. Each timer the agent starts is handed to the test with the duration
// it was asked for, so the test pins the bound; its channel has no buffer, so
// fire completes only if the agent is waiting on it, and a wait that ignores
// the timer fails the test there.
type fakeTimers struct{ arms chan *armedTimer }

type armedTimer struct {
	d       time.Duration
	c       chan time.Time
	stopped atomic.Bool
}

func installFakeTimers(t *testing.T) *fakeTimers {
	f := &fakeTimers{arms: make(chan *armedTimer, 32)}
	t.Cleanup(agent.SetTimerForTest(func(d time.Duration) (<-chan time.Time, func()) {
		at := &armedTimer{d: d, c: make(chan time.Time)}
		select {
		case f.arms <- at:
		default:
		}
		return at.c, func() { at.stopped.Store(true) }
	}))
	return f
}

// fire makes the timer go off.
func (at *armedTimer) fire(t *testing.T) {
	t.Helper()
	select {
	case at.c <- time.Now():
	case <-time.After(10 * time.Second):
		t.Fatalf("nothing waits on the %v timer: the agent does not wait for it", at.d)
	}
}

// The bounds the agent is to wait for a periodic send in flight, per end.
const (
	instructionWait = 30 * time.Second // finalReportTimeout
	jobEndWait      = 2500 * time.Millisecond
	jobEndBudget    = 5 * time.Second
)

func waitBound(via string) time.Duration {
	if via == byJobEnd {
		return jobEndWait
	}
	return instructionWait
}

// lastReportRig is a running agent whose next report is held at the endpoint.
type lastReportRig struct {
	h      *harness
	held   *heldBackend
	rec    *logRecorder
	timers *fakeTimers
	grace  time.Duration
	// landed is how many reports the endpoint had taken when the hold began.
	landed int
}

// The two ways the agent ends its reporter before a last report.
const (
	byInstruction = "instruction" // the desired state says stop
	byJobEnd      = "job end"     // the agent's context ends
)

var bothEnds = []string{byInstruction, byJobEnd}

type rigOptions struct {
	modes string        // the engine double's modes
	grace time.Duration // the stop grace (1 s when 0)
}

// newLastReportRig runs the Instance, waits for its worker to run, then holds
// the next report at the endpoint and waits until it is held.
func newLastReportRig(t *testing.T, o rigOptions) *lastReportRig {
	t.Helper()
	g := &lastReportRig{held: newHeldBackend(), rec: newLogRecorder(), grace: o.grace}
	if g.grace == 0 {
		g.grace = time.Second
	}
	g.timers = installFakeTimers(t) // before the harness, so it is undone after the harness stops
	g.h = newHarness(t, harnessOptions{logTee: g.rec, backendWrap: func(b agentapi.Backend) agentapi.Backend {
		g.held.Backend = b
		return g.held
	}})
	// Registered after the harness's clean-up, so it runs before it: a held
	// report never keeps the agent from stopping.
	t.Cleanup(g.held.releaseHeld)
	d := g.h.runState(o.modes, 1, 0, nil)
	d.Settings.StopGrace = agentapi.Duration(g.grace)
	g.h.set(d)
	g.h.waitReport("worker-0 running", 20*time.Second, running)
	g.held.arm()
	select {
	case <-g.held.holding:
	case <-time.After(20 * time.Second):
		t.Fatal("no report was held: the agent sent none")
	}
	g.landed = len(g.h.backend.Reports(hAllocUID))
	return g
}

// end begins the stop, as an instruction or as the end of the job.
func (g *lastReportRig) end(via string) {
	switch via {
	case byInstruction:
		g.h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(g.grace)}})
	case byJobEnd:
		g.h.cancel()
	}
}

// waitArmed waits until the agent starts a timer for its wait, and checks the
// duration it asked for. If a report comes in at the endpoint first, the last
// report did not wait for the periodic one held there.
func (g *lastReportRig) waitArmed(t *testing.T, want time.Duration) *armedTimer {
	t.Helper()
	select {
	case r := <-g.held.entered:
		t.Fatalf("a report came in at the endpoint while a periodic report was held there: worker-0 %+v", worker0(r))
	case at := <-g.timers.arms:
		if at.d != want {
			t.Fatalf("the agent started a %v timer, want %v", at.d, want)
		}
		return at
	case <-time.After(30 * time.Second):
		t.Fatalf("the agent started no %v timer and sent no report", want)
	}
	return nil
}

// nothingBeforeTheBound checks that, with the bound not reached, no report
// has landed or come in at the endpoint, its latest report is not the last
// one, and the agent has not warned.
func (g *lastReportRig) nothingBeforeTheBound(t *testing.T) {
	t.Helper()
	if n := len(g.held.entered); n != 0 {
		t.Errorf("%d reports came in at the endpoint while the periodic one was held, before the bound", n)
	}
	if n := len(g.h.backend.Reports(hAllocUID)); n != g.landed {
		t.Errorf("%d reports landed while the periodic one was held, want none", n-g.landed)
	}
	if p := g.phaseOfLast(t); p == agentapi.PartStopped {
		t.Errorf("the endpoint's latest report says %s before the bound", p)
	}
	if n := g.rec.count(slog.LevelWarn, agent.LogOpenSendAtLast); n != 0 {
		t.Errorf("the agent warned of an open periodic send %d times before the bound", n)
	}
}

// waitRun waits until Run has returned, and checks what it returned: nil
// after a stop instruction, the job's error at the end of the job.
func (g *lastReportRig) waitRun(t *testing.T, via string) {
	t.Helper()
	select {
	case err := <-g.h.done:
		g.h.done <- err // for the clean-up's wait
		if via == byInstruction && err != nil || via == byJobEnd && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v after a stop by %s", err, via)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the agent did not exit after a stop by %s", via)
	}
}

// phaseOfLast returns worker-0's phase in the endpoint's latest report.
func (g *lastReportRig) phaseOfLast(t *testing.T) agentapi.PartPhase {
	t.Helper()
	w := worker0(mustLast(t, g.h))
	if w == nil {
		t.Fatal("the latest report has no worker-0")
	}
	return w.Phase
}

// With a periodic report held at the endpoint, the last report is not sent:
// it waits for that report, which lands first, so the endpoint's latest report
// is the last one and says stopped. The wait is pinned by the timer the agent
// starts for it (its duration, and that it is stopped once the send is over);
// the waits are on observable state: that timer, and the endpoint's reports.
func TestLastReportWaitsForAPeriodicReportInFlight(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			g := newLastReportRig(t, rigOptions{})
			g.end(via)
			at := g.waitArmed(t, waitBound(via))
			g.nothingBeforeTheBound(t)

			g.held.releaseHeld()
			g.waitRun(t, via)
			if g.held.cancelledAtRelease {
				t.Error("the agent cancelled the periodic send it was to let finish")
			}
			if !at.stopped.Load() {
				t.Error("the wait's timer was left running after the periodic send finished")
			}
			reps := g.h.backend.Reports(hAllocUID)
			if len(reps) != g.landed+2 {
				t.Fatalf("%d reports landed after the hold began, want the held one and the last one", len(reps)-g.landed)
			}
			if w := worker0(reps[g.landed]); w == nil || w.Phase == agentapi.PartStopped {
				t.Errorf("the report that landed first after the release was %+v, want the held periodic one", w)
			}
			if p := g.phaseOfLast(t); p != agentapi.PartStopped {
				t.Errorf("the endpoint's latest report says worker-0 %s, want %s", p, agentapi.PartStopped)
			}
			if n := g.rec.count(slog.LevelWarn, agent.LogOpenSendAtLast); n != 0 {
				t.Errorf("the agent warned of an open periodic send %d times though it finished", n)
			}
		})
	}
}

// A periodic report held to the bound does not keep the agent from sending the
// last report. Before the bound the last report is not sent and nothing is
// logged; the test makes the bound come (the timer fires, which completes only
// if the agent waits on it), and then the last report goes, at the bound, with
// the warning. The held report then lands after it, and the Backend drops it
// as older: the last report stands (issue #221).
func TestLastReportGoesAtTheBoundWhenAPeriodicSendStaysOpen(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			g := newLastReportRig(t, rigOptions{})
			g.end(via)
			at := g.waitArmed(t, waitBound(via))
			g.nothingBeforeTheBound(t)

			at.fire(t)
			g.waitRun(t, via)
			if !g.held.open() {
				t.Fatal("the periodic report was released: the test did not hold it to the bound")
			}
			if p := g.phaseOfLast(t); p != agentapi.PartStopped {
				t.Errorf("the endpoint's latest report says worker-0 %s, want %s: the last report did not go", p, agentapi.PartStopped)
			}
			if n := g.rec.count(slog.LevelWarn, agent.LogOpenSendAtLast); n != 1 {
				t.Errorf("the agent logged the open periodic send %d times at Warn, want once", n)
			}
			if n := g.rec.count(slog.LevelInfo, agent.LogWaitingForSend); n != 1 {
				t.Errorf("the agent logged that it waits %d times, want once", n)
			}

			// The held report reaches the Backend now, after the last one. The
			// wait is on the double's own signal that it landed.
			g.held.releaseHeld()
			select {
			case <-g.held.lateLanded:
			case <-time.After(30 * time.Second):
				t.Fatal("the held report never reached the Backend")
			}
			if p := g.phaseOfLast(t); p != agentapi.PartStopped {
				t.Errorf("after the held report landed the endpoint's latest report says worker-0 %s, want %s: the older report overwrote the last", p, agentapi.PartStopped)
			}
			if n := g.h.backend.Dropped(hAllocUID); n != 1 {
				t.Errorf("the Backend dropped %d reports, want the held one", n)
			}
		})
	}
}

// A periodic report that fails while the stop waits for it is not retried: the
// last report supersedes it, so only the last report lands after the hold.
// CodeNotRegistered is the error send answers by registering again and sending
// the old snapshot once more, which it must not do for a periodic report once
// the stop has begun.
func TestAPeriodicReportThatFailsDuringTheStopIsNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"a transport error", errors.New("read: connection reset by peer")},
		{"not registered", agentapi.Errorf(agentapi.CodeNotRegistered, "agent instance is not registered")},
	} {
		for _, via := range bothEnds {
			t.Run(tc.name+"/"+via, func(t *testing.T) {
				g := newLastReportRig(t, rigOptions{})
				g.end(via)
				g.waitArmed(t, waitBound(via))
				g.held.releaseWith(tc.err)
				g.waitRun(t, via)
				reps := g.h.backend.Reports(hAllocUID)
				if len(reps) != g.landed+1 {
					t.Fatalf("%d reports landed after the hold began, want only the last one", len(reps)-g.landed)
				}
				if p := g.phaseOfLast(t); p != agentapi.PartStopped {
					t.Errorf("the endpoint's latest report says worker-0 %s, want %s", p, agentapi.PartStopped)
				}
			})
		}
	}
}

// A job cancellation that comes while a stop instruction waits for the
// periodic send in flight cuts the wait to the job-end budget: Allocation
// deletion sends the stop and cancels the job (issue #223, review B1), and
// Slurm kills the agent KillWait after its TERM. The first timer is stopped
// and a second is started for what the job-end path waits, which fires.
func TestAJobCancellationDuringTheStopWaitCutsItToTheJobEndBudget(t *testing.T) {
	g := newLastReportRig(t, rigOptions{})
	g.end(byInstruction)
	first := g.waitArmed(t, instructionWait)
	g.h.cancel()
	second := g.waitArmed(t, jobEndWait)
	if !first.stopped.Load() {
		t.Error("the stop's wait timer was left running after the cancellation")
	}
	second.fire(t)
	g.waitRun(t, byInstruction)
	if p := g.phaseOfLast(t); p != agentapi.PartStopped {
		t.Errorf("the endpoint's latest report says worker-0 %s, want %s: the last report did not go", p, agentapi.PartStopped)
	}
	if n := g.rec.count(slog.LevelWarn, agent.LogOpenSendAtLast); n != 1 {
		t.Errorf("the agent logged the open periodic send %d times at Warn, want once", n)
	}
}

// A job cancellation that comes while the last report is being tried cuts the
// tries to the job-end budget, jobEndBudget from the cancellation: the last
// report fails here until it does, and the cut is a timer that fires.
func TestAJobCancellationDuringTheLastReportCutsItsTries(t *testing.T) {
	g := newLastReportRig(t, rigOptions{})
	g.end(byInstruction)
	g.waitArmed(t, instructionWait)
	g.held.failFrom(errors.New("dial: no route to host"))
	g.held.releaseHeld() // the periodic report lands; the last one is tried and fails
	deadline := time.Now().Add(30 * time.Second)
	for g.held.failedCalls() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the last report was not tried again after it failed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.h.cancel()
	cut := g.waitArmed(t, jobEndBudget)
	cut.fire(t)
	g.waitRun(t, byInstruction)
	if n := g.rec.count(slog.LevelWarn, "last report not sent"); n != 1 {
		t.Errorf("the agent logged the unsent last report %d times, want once", n)
	}
	if n := len(g.h.backend.Reports(hAllocUID)); n != g.landed+1 {
		t.Errorf("%d reports landed after the hold began, want only the periodic one", n-g.landed)
	}
}

// The watch that cuts the last report's tries at a job cancellation has
// returned by the time it is ended, with its timer stopped. Issue #262: the
// watch's goroutine used to outlive the end of the watch, so a rig that ran
// after an agent stopped on instruction could read the 5 s timer that goroutine
// armed through the timer hook, in place of the 2.5 s wait it was waiting for
// (and a test waiting for the 5 s cut could take the same stray one and then
// wait for it to fire).
//
// The test gates the goroutine's cleanup: the stop function of the timer it
// armed blocks until the test opens the gate. In a synctest bubble, Wait
// returns only when every goroutine is blocked or gone, so after it the ending
// of the watch has either returned (a stop that does not wait for the
// goroutine) or is still waiting (one that does), whatever the schedule: no
// timing, and no window for the goroutine to finish its cleanup first. Then the
// gate opens and the end of the watch must return, with the timer stopped.
func TestTheWatchOnTheLastReportsTriesHasReturnedWhenItIsEnded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		arms := make(chan *armedTimer, 8)
		defer agent.SetTimerForTest(func(d time.Duration) (<-chan time.Time, func()) {
			at := &armedTimer{d: d, c: make(chan time.Time)} // never fires
			arms <- at
			return at.c, func() {
				<-gate // the goroutine's cleanup waits here
				at.stopped.Store(true)
			}
		})()

		jobCtx, endJob := context.WithCancel(context.Background())
		var cut atomic.Bool
		stop := sync.OnceFunc(agent.ClampAtTermForTest(jobCtx, func() { cut.Store(true) }))
		defer func() {
			// However the test ends, the goroutine must leave the bubble: open the
			// gate, and end the watch if the test did not (stop is single-use).
			release()
			endJob()
			stop()
		}()

		endJob() // the job's cancellation: the watch arms its cut
		synctest.Wait()
		var at *armedTimer
		select {
		case at = <-arms:
		default:
			t.Fatal("the watch armed no timer after the job's context ended")
		}
		if at.d != jobEndBudget {
			t.Fatalf("the watch armed a %v timer, want %v", at.d, jobEndBudget)
		}

		returned := make(chan struct{})
		go func() {
			stop()
			close(returned)
		}()
		synctest.Wait() // the end of the watch has done all it can with the gate shut
		select {
		case <-returned:
			t.Error("the end of the watch returned while the watch's goroutine was still cleaning up: the goroutine outlives the end of the watch")
		default:
		}

		release()
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Error("the end of the watch did not return after the watch's goroutine finished its cleanup")
		}
		if !at.stopped.Load() {
			t.Error("the watch's timer was still running when its end returned")
		}
		if cut.Load() {
			t.Error("the watch cut the tries though it was ended before its timer fired")
		}
		if n := len(arms); n != 0 {
			t.Errorf("the watch armed %d more timers", n)
		}
	})
}

// Periodic reports close when the stop begins, not when the workers have
// stopped (issue #223, review B3). A worker that ignores TERM keeps the agent
// stopping for the whole stop grace: no report from that time says stopping,
// and the last one says stopped.
func TestNoPeriodicReportGoesWhileTheWorkersStopOnInstruction(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	d := h.runState("ignore-term", 1, 0, nil)
	d.Settings.StopGrace = agentapi.Duration(1500 * time.Millisecond)
	h.set(d)
	h.waitReport("worker-0 running", 20*time.Second, running)
	h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: d.Settings})
	select {
	case err := <-h.done:
		h.done <- err
		if err != nil {
			t.Fatalf("Run returned %v after a stop", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the agent did not exit after a stop")
	}
	for i, r := range h.backend.Reports(hAllocUID) {
		if w := worker0(r); w != nil && w.Phase == agentapi.PartStopping {
			t.Fatalf("report %d of %d says worker-0 stopping: a periodic report went after the stop began", i+1, len(h.backend.Reports(hAllocUID)))
		}
	}
	if w := worker0(mustLast(t, h)); w == nil || w.Phase != agentapi.PartStopped {
		t.Fatalf("the last report's worker: %+v", w)
	}
}

// The same at the end of the job: the report queued behind the one held does
// not start while the workers stop. The periodic reports must have closed by
// the time the workers are being stopped, which the test sees as the worker
// still alive once the agent says they are closed; then the held report is
// released, and only it and the last report land.
func TestNoQueuedReportStartsWhileTheWorkersStopAtJobEnd(t *testing.T) {
	g := newLastReportRig(t, rigOptions{modes: "ignore-term", grace: 2 * time.Second})
	pids := enginePIDs(t, g.h)
	deadline := time.Now().Add(20 * time.Second)
	for !agent.QueuedReportForTest(g.h.agent) { // the report tick queues one behind the held one
		if time.Now().After(deadline) {
			t.Fatal("no report was queued behind the held one")
		}
		time.Sleep(5 * time.Millisecond)
	}
	g.h.cancel()
	for !agent.PeriodicClosedForTest(g.h.agent) {
		if time.Now().After(deadline) {
			t.Fatal("the periodic reports did not close")
		}
		time.Sleep(time.Millisecond)
	}
	if !anyAlive(pids) {
		t.Fatal("the periodic reports closed only after the workers had stopped")
	}
	g.held.releaseHeld()
	g.waitRun(t, byJobEnd)
	if n := len(g.h.backend.Reports(hAllocUID)); n != g.landed+2 {
		t.Errorf("%d reports landed after the hold began, want the held one and the last one", n-g.landed)
	}
}

// anyAlive says whether any of the processes exists.
func anyAlive(pids []int) bool {
	for _, p := range pids {
		if syscall.Kill(p, 0) == nil {
			return true
		}
	}
	return false
}

package agent_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
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
// and logs a warning (science agent's ruling, 2 Nov 2026: option 2 for alpha;
// the full fix, a sequence number on every report, is issue #221).
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
}

func newHeldBackend() *heldBackend {
	return &heldBackend{holding: make(chan struct{}), release: make(chan struct{}), entered: make(chan agentapi.Report, 8)}
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
			}()
			return ctx.Err()
		}
	case b.held:
		select {
		case b.entered <- r:
		default:
		}
	}
	b.mu.Unlock()
	return b.Backend.Report(ctx, c, r)
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

// lastReportRig is a running agent whose next report is held at the endpoint.
type lastReportRig struct {
	h    *harness
	held *heldBackend
	rec  *logRecorder
	// landed is how many reports the endpoint had taken when the hold began.
	landed int
}

// The two ways the agent ends its reporter before a last report.
const (
	byInstruction = "instruction" // the desired state says stop
	byJobEnd      = "job end"     // the agent's context ends
)

// newLastReportRig runs the Instance, waits for its worker to run, then holds
// the next report at the endpoint and waits until it is held.
func newLastReportRig(t *testing.T) *lastReportRig {
	t.Helper()
	g := &lastReportRig{held: newHeldBackend(), rec: newLogRecorder()}
	g.h = newHarness(t, harnessOptions{logTee: g.rec, backendWrap: func(b agentapi.Backend) agentapi.Backend {
		g.held.Backend = b
		return g.held
	}})
	// Registered after the harness's clean-up, so it runs before it: a held
	// report never keeps the agent from stopping.
	t.Cleanup(g.held.releaseHeld)
	g.h.set(g.h.runState("", 1, 0, nil))
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
		g.h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(time.Second)}})
	case byJobEnd:
		g.h.cancel()
	}
}

// waitRun waits until Run has returned, and checks what it returned.
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

var bothEnds = []string{byInstruction, byJobEnd}

// With a periodic report held at the endpoint, the last report is not sent:
// it waits for that report, which lands first, so the endpoint's latest report
// is the last one and says stopped. The waits are on observable state: the
// endpoint's reports, and the agent's own log record that it waits.
func TestLastReportWaitsForAPeriodicReportInFlight(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			g := newLastReportRig(t)
			g.end(via)
			// Either the last report comes in while the periodic one is held
			// (the old code: it cancelled the periodic send and went on), or the
			// agent says it is waiting.
			select {
			case r := <-g.held.entered:
				t.Fatalf("a report came in at the endpoint while a periodic report was held there: worker-0 %+v", worker0(r))
			case <-g.rec.seen(agent.LogWaitingForSend):
			case <-time.After(30 * time.Second):
				t.Fatal("the agent neither sent the last report nor said it waits for the periodic one")
			}
			if n := len(g.h.backend.Reports(hAllocUID)); n != g.landed {
				t.Fatalf("%d reports landed while the periodic one was held, want none", n-g.landed)
			}
			if p := g.phaseOfLast(t); p == agentapi.PartStopped {
				t.Fatalf("the endpoint's latest report says %s before the periodic report landed", p)
			}

			g.held.releaseHeld()
			g.waitRun(t, via)
			if g.held.cancelledAtRelease {
				t.Error("the agent cancelled the periodic send it was to let finish")
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

// The known limit: a periodic report held past the bound does not keep the
// agent from sending the last report. It sends it, logs a warning, and the
// held report can still land after it (issue #221 tracks the full fix). The
// bound is shortened through the test seam; the hold stays open throughout.
func TestLastReportGoesWhenAPeriodicSendStaysOpen(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			t.Cleanup(agent.SetFinalWaitForTest(200 * time.Millisecond))
			g := newLastReportRig(t)
			g.end(via)
			g.waitRun(t, via)
			if !g.held.open() {
				t.Fatal("the periodic report was released: the test did not hold it past the bound")
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
		})
	}
}

// A periodic report that fails while the stop waits for it is not retried: the
// last report supersedes it, so only the last report lands after the hold.
func TestAPeriodicReportThatFailsDuringTheStopIsNotRetried(t *testing.T) {
	for _, via := range bothEnds {
		t.Run(via, func(t *testing.T) {
			g := newLastReportRig(t)
			g.end(via)
			select {
			case r := <-g.held.entered:
				t.Fatalf("a report came in at the endpoint while a periodic report was held there: worker-0 %+v", worker0(r))
			case <-g.rec.seen(agent.LogWaitingForSend):
			case <-time.After(30 * time.Second):
				t.Fatal("the agent neither sent the last report nor said it waits for the periodic one")
			}
			g.held.releaseWith(errors.New("read: connection reset by peer"))
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

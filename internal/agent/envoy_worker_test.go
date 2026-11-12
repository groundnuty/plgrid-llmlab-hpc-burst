package agent_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// The worker double the router tests put behind the real Envoy: a recording HTTP/1.1 server on the
// worker's Unix socket (<RunDir>/worker-0.sock, the path the agent's config names). It stands in for a
// vLLM worker only; Envoy is never doubled. It records what each request carried and what became of
// it, can stream, delay, hold and refuse, and tells the test about its connections' states with the
// time of each, so a test waits on those events and never on a sleep.

// workerSock is the worker's socket in a run directory.
func workerSock(dir string) string { return filepath.Join(dir, "worker-0.sock") }

// workerRequest is one request as the worker saw it.
type workerRequest struct {
	Method, Path string
	Header       http.Header // as the worker's server parsed it: names canonicalized, values as sent
	Body         []byte
	Conn         int       // the worker-side connection that carried it, numbered from 1
	At           time.Time // when it arrived
	Outcome      string    // "" while it is being answered, then "answered", "unauthorized", "cut" or "held"
}

// connEvent is a change of state of a connection the worker accepted.
type connEvent struct {
	Conn  int
	State http.ConnState
	At    time.Time
}

// workerSpec is how the worker answers.
type workerSpec struct {
	Key         string        // when set, a request without "Authorization: Bearer "+Key gets 401
	Events      int           // SSE events to stream, then "data: [DONE]"; 0 answers 200 with workerBody
	Every       time.Duration // between the events (and before [DONE])
	First       time.Duration // wait this long before writing any response header or byte
	Hold        *holdGate     // when set, every request waits here before it is answered
	IdleTimeout time.Duration // the server's IdleTimeout: how long it keeps an idle connection; 0 for none
}

const (
	workerBody         = "worker: ok"
	workerUnauthorized = "worker: unauthorized"
)

// holdGate holds requests open until all `want` of them have arrived, or its deadline passes, or the
// test releases it.
type holdGate struct {
	want     int
	deadline time.Duration

	mu      sync.Mutex
	arrived int
	how     string // "", then "count", "deadline" or "manual"
	open    chan struct{}
	started bool
}

// newHoldGate holds each request until want have arrived (want 0: until released), but for at most
// deadline from the first arrival.
func newHoldGate(want int, deadline time.Duration) *holdGate {
	return &holdGate{want: want, deadline: deadline, open: make(chan struct{})}
}

// Release opens the gate; the first reason to open it is the one kept.
func (g *holdGate) Release() { g.openFor("manual") }

func (g *holdGate) openFor(how string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.how == "" {
		g.how = how
		close(g.open)
	}
}

// Arrived is the number of requests that have reached the gate.
func (g *holdGate) Arrived() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.arrived
}

// How says what opened the gate: "count", "deadline", "manual", or "" while it is closed.
func (g *holdGate) How() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.how
}

// enter counts the request in and waits for the gate; false means the request ended first.
func (g *holdGate) enter(ctx context.Context) bool {
	g.mu.Lock()
	g.arrived++
	if !g.started {
		g.started = true
		time.AfterFunc(g.deadline, func() { g.openFor("deadline") })
	}
	full := g.want > 0 && g.arrived >= g.want
	g.mu.Unlock()
	if full {
		g.openFor("count")
	}
	select {
	case <-g.open:
		return true
	case <-ctx.Done():
		return false
	}
}

type connKey struct{}

// testWorker is the running double.
type testWorker struct {
	spec workerSpec

	mu      sync.Mutex
	reqs    []*workerRequest
	events  []connEvent
	ids     map[net.Conn]int
	next    int
	changed chan struct{} // closed and replaced on every change a test may wait for
}

// startWorker serves the double on dir's worker socket (mode 0600) until the test ends.
func startWorker(t *testing.T, dir string, spec workerSpec) *testWorker {
	t.Helper()
	w := &testWorker{spec: spec, ids: map[net.Conn]int{}, changed: make(chan struct{})}
	ln, err := net.Listen("unix", workerSock(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(workerSock(dir), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		Handler:     http.HandlerFunc(w.handle),
		IdleTimeout: spec.IdleTimeout,
		ConnState:   w.onConnState,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, w.connID(c))
		},
	}
	served := make(chan struct{})
	go func() { defer close(served); _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close() // ends the connections, which cancels every request still being held or streamed
		<-served
	})
	return w
}

// connID numbers a connection on first sight.
func (w *testWorker) connID(c net.Conn) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	id, ok := w.ids[c]
	if !ok {
		w.next++
		id = w.next
		w.ids[c] = id
	}
	return id
}

func (w *testWorker) onConnState(c net.Conn, s http.ConnState) {
	id := w.connID(c)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, connEvent{Conn: id, State: s, At: time.Now()})
	if s == http.StateClosed || s == http.StateHijacked {
		delete(w.ids, c)
	}
	w.signalLocked()
}

// signalLocked wakes every waiter; the caller holds w.mu.
func (w *testWorker) signalLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

// sleepCtx waits d, or until the request ends; false means it ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *testWorker) handle(rw http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := io.ReadAll(r.Body)
	rec := &workerRequest{Method: r.Method, Path: r.URL.RequestURI(), Header: r.Header.Clone(), Body: body, At: time.Now()}
	rec.Conn, _ = ctx.Value(connKey{}).(int)
	w.mu.Lock()
	w.reqs = append(w.reqs, rec)
	w.signalLocked()
	w.mu.Unlock()
	setOutcome := func(s string) {
		w.mu.Lock()
		rec.Outcome = s
		w.signalLocked()
		w.mu.Unlock()
	}
	outcome := "cut" // what a handler that returns before it answers means: the other end ended it
	defer func() { setOutcome(outcome) }()

	if w.spec.Key != "" && r.Header.Get("Authorization") != "Bearer "+w.spec.Key {
		outcome = "unauthorized"
		http.Error(rw, workerUnauthorized, http.StatusUnauthorized)
		return
	}
	if g := w.spec.Hold; g != nil && !g.enter(ctx) {
		return
	}
	if !sleepCtx(ctx, w.spec.First) {
		return
	}
	if w.spec.Events == 0 {
		outcome = "answered"
		_, _ = io.WriteString(rw, workerBody)
		return
	}
	fl := rw.(http.Flusher)
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.WriteHeader(http.StatusOK)
	fl.Flush()
	for i := 0; i < w.spec.Events; i++ {
		if _, err := fmt.Fprintf(rw, "data: {\"i\":%d}\n\n", i); err != nil {
			return
		}
		fl.Flush()
		if !sleepCtx(ctx, w.spec.Every) {
			return
		}
	}
	outcome = "answered"
	_, _ = io.WriteString(rw, "data: [DONE]\n\n")
	fl.Flush()
}

// Requests is a copy of the requests so far.
func (w *testWorker) Requests() []workerRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]workerRequest, len(w.reqs))
	for i, r := range w.reqs {
		out[i] = *r
	}
	return out
}

// Events is a copy of the connection events so far, in the order they happened.
func (w *testWorker) Events() []connEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]connEvent(nil), w.events...)
}

// waitFor waits until cond holds of the worker's state, which it reads under the lock, or the
// deadline passes. It wakes on each change the worker records, never on a poll.
func (w *testWorker) waitFor(deadline time.Duration, cond func(reqs []*workerRequest, events []connEvent) bool) bool {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		w.mu.Lock()
		ok := cond(w.reqs, w.events)
		ch := w.changed
		w.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-ch:
		case <-timer.C:
			return false
		}
	}
}

// waitConnState waits for connection conn to reach state and returns that event.
func (w *testWorker) waitConnState(conn int, state http.ConnState, deadline time.Duration) (connEvent, bool) {
	var got connEvent
	ok := w.waitFor(deadline, func(_ []*workerRequest, evs []connEvent) bool {
		for _, e := range evs {
			if e.Conn == conn && e.State == state {
				got = e
				return true
			}
		}
		return false
	})
	return got, ok
}

// waitOutcome waits until the i-th request (counting from 0) has an outcome and returns it.
func (w *testWorker) waitOutcome(i int, deadline time.Duration) (string, bool) {
	var out string
	ok := w.waitFor(deadline, func(rs []*workerRequest, _ []connEvent) bool {
		if i < len(rs) && rs[i].Outcome != "" {
			out = rs[i].Outcome
			return true
		}
		return false
	})
	return out, ok
}

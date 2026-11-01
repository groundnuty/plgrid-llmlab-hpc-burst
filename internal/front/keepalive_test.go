package front

import (
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Keep-alive: "Idle the front's connection to an Instance
// past 5 s and send again: no request fails (L-2.5)." vLLM closes a
// connection that was idle for 5 s (its keep-alive default); the front drops
// its idle upstream connections after upstreamIdleTimeout, 4 s, so it never
// writes into a connection the engine is closing (Time-outs and keep-alive).
//
// A second request on a new connection proves little by itself: the engine
// closes the old one at 5 s, and Go's pool would then open a new one even with
// no timer in the front. So the tests look at who closed the first
// connection: a relay between the front and the engine (kaRelay) sees which
// side ended it first. With the 4 s timer the front closes it, before the
// engine's own 5 s; the control, with the front's timer raised above 5 s, sees
// the engine close it, so the first test can fail. The time from the engine's
// idle to the close is checked from below only: a timer never fires early,
// and a loaded machine only makes it later.
//
// The tests used to read who closed from when it closed, 3.5 to 4.6 s or 4.8
// to 5.6 s after the idle, windows a loaded machine could leave (PR #137
// review, Astra). What is left to load is the order of two timers 1 s apart in
// the same process (the front's 4 s, the engine's 5 s): a stall of the whole
// test binary across both could still let the engine's run first.
//
// These tests run on the wall clock and take about 5.5 s and 5 s.

const (
	kaEngineIdle = 5 * time.Second // vLLM's keep-alive default, which the double uses
)

// kaFix is a front on the wall clock with one Instance on an engine double
// that closes idle connections after 5 s, reached through a kaRelay.
type kaFix struct {
	*harness
	e     *enginetest.Engine
	relay *kaRelay
}

func newKaFix(t *testing.T, edit func(*Config)) *kaFix {
	t.Helper()
	h := newHarnessReal(t, edit)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: nil, IdleTimeout: kaEngineIdle})
	relay := startKaRelay(t, e.Addr())
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	// A lease end after the wall clock's now, past the drain window.
	uid := h.instance("md", "md-0", "helios-a", e, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	h.trans.Set(uid, relay.addr()) // the front reaches the engine through the relay
	h.apply()
	return &kaFix{harness: h, e: e, relay: relay}
}

// chat sends one request and returns its status and body.
func (k *kaFix) chat(t *testing.T) (int, string) {
	t.Helper()
	resp := stPost(t, k.harness, "/v1/chat/completions", `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	return resp.StatusCode, string(b)
}

// firstClosed waits for the first connection to be closed, and returns who
// closed it, "front" or "engine" (kaRelay), and how long after it went idle
// on the engine's side the engine saw it close.
func (k *kaFix) firstClosed(t *testing.T) (string, time.Duration) {
	t.Helper()
	var by string
	waitState(t, "the first connection to close", func() bool {
		by = k.relay.closedBy(0)
		cs := k.e.Conns()
		return by != "" && len(cs) > 0 && !cs[0].Closed.IsZero()
	})
	cs := k.e.Conns()
	return by, cs[0].Closed.Sub(cs[0].Idle)
}

// TestKeepAliveFrontDropsItsIdleConnectionBeforeTheEnginesTimeout: the front
// closes its idle connection, before the engine's 5 s; after an idle past
// those 5 s a request succeeds, on a new connection.
func TestKeepAliveFrontDropsItsIdleConnectionBeforeTheEnginesTimeout(t *testing.T) {
	k := newKaFix(t, nil)
	if status, body := k.chat(t); status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	if cs := k.e.Conns(); len(cs) != 1 || cs[0].Requests != 1 {
		t.Fatalf("after the first request the engine has connections %+v, want one that carried one request", cs)
	}

	by, d := k.firstClosed(t)
	// The measurement that can fail: closed by the front's upstreamIdleTimeout,
	// not by the engine's 5 s, and by a timer, not at once.
	if by != "front" {
		t.Errorf("the first connection was closed by the %s, %v after it went idle; want the front, by upstreamIdleTimeout (4 s), before the engine's %v", by, d, kaEngineIdle)
	}
	if d < 3500*time.Millisecond {
		t.Errorf("the first connection closed %v after it went idle, want upstreamIdleTimeout's 4 s or later", d)
	}
	// Idle past the engine's 5 s, then send again.
	idle := kaEngineIdle + 500*time.Millisecond
	time.Sleep(time.Until(k.e.Conns()[0].Idle.Add(idle)))

	status, body := k.chat(t)
	if status != http.StatusOK {
		t.Fatalf("the request after an idle of %v failed: status %d: %s", idle, status, body)
	}
	cs := k.e.Conns()
	if len(cs) != 2 || cs[0].Requests != 1 || cs[1].Requests != 1 {
		t.Fatalf("the engine's connections %+v, want two, one request each: the second request arrived on a new connection", cs)
	}
	for _, r := range k.records() {
		if r.Status != http.StatusOK {
			t.Errorf("usage record %s: status %d, want 200", r.RequestID, r.Status)
		}
	}
	if strings.Contains(k.logs.String(), "level=WARN") || strings.Contains(k.logs.String(), "level=ERROR") {
		t.Errorf("the front logged a warning or error:\n%s", k.logs.String())
	}
}

// TestKeepAliveControlLongerUpstreamIdleLetsTheEngineClose: the same
// observation with upstreamIdleTimeout at 30 s sees the engine close the
// connection, at its own 5 s. Without this the front's close above could be
// the relay's habit.
func TestKeepAliveControlLongerUpstreamIdleLetsTheEngineClose(t *testing.T) {
	k := newKaFix(t, func(c *Config) { c.UpstreamIdleTimeout = Duration(30 * time.Second) })
	if status, body := k.chat(t); status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	by, d := k.firstClosed(t)
	if by != "engine" {
		t.Errorf("with a front that keeps idle connections 30 s, the first connection was closed by the %s, %v after it went idle; want the engine, at its %v", by, d, kaEngineIdle)
	}
	if d < 4800*time.Millisecond {
		t.Errorf("the engine closed the first connection %v after it went idle, want its %v or later", d, kaEngineIdle)
	}
}

// kaRelay passes bytes between the front and the engine, one engine
// connection per front connection, and records for each pair which side
// ended it first: the side whose read reached the end of its stream first is
// the side that closed the connection. It serves until the test ends.
type kaRelay struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	by     []string // per accepted connection: "" while open, then "front" or "engine"
	conns  []net.Conn
}

func startKaRelay(t *testing.T, target string) *kaRelay {
	t.Helper()
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	r := &kaRelay{ln: ln, target: target}
	go r.accept()
	t.Cleanup(func() {
		ln.Close()
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, c := range r.conns {
			c.Close()
		}
	})
	return r
}

func (r *kaRelay) addr() string { return r.ln.Addr().String() }

func (r *kaRelay) accept() {
	for {
		front, err := r.ln.Accept()
		if err != nil {
			return
		}
		engine, err := net.Dial("tcp", r.target)
		if err != nil {
			front.Close()
			continue
		}
		r.mu.Lock()
		i := len(r.by)
		r.by = append(r.by, "")
		r.conns = append(r.conns, front, engine)
		r.mu.Unlock()
		go r.pipe(i, engine, front, "front")
		go r.pipe(i, front, engine, "engine")
	}
}

// pipe copies src to dst until src ends, records src's side as the closer if
// the other direction has not ended first, and closes both.
func (r *kaRelay) pipe(i int, dst, src net.Conn, side string) {
	_, _ = io.Copy(dst, src)
	r.mu.Lock()
	if r.by[i] == "" {
		r.by[i] = side
	}
	r.mu.Unlock()
	src.Close()
	dst.Close()
}

// closedBy is who closed the i-th connection, or "" while it is open or not
// yet accepted.
func (r *kaRelay) closedBy(i int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.by) {
		return ""
	}
	return r.by[i]
}

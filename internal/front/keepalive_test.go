package front

import (
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Keep-alive: "Idle the front's connection to an Instance
// past 5 s and send again: no request fails (L-2.5)." vLLM closes a
// connection that was idle for 5 s (its keep-alive default); the front drops
// its idle upstream connections after upstreamIdleTimeout, 4 s, so it never
// writes into a connection the engine is closing (Time-outs and keep-alive).
//
// No assertion here depends on which of two real timers fires first (PR #144
// review, T2). The property is checked in three parts:
//   - the front's Instance transport drops idle connections after
//     upstreamIdleTimeout, and its default is below vLLM's 5 s
//     (TestKeepAliveIdleTimeoutIsBelowTheEngines, no timer at all);
//   - with an engine that never closes an idle connection, the front closes
//     it, by its own timer, and a request after the idle succeeds;
//   - with the front's timer far beyond the wait, the engine's 5 s close is
//     seen as the engine's, so the observation can tell the two apart.
//
// Who closed is seen by a relay between the front and the engine (kaRelay):
// the side whose stream ended first. In each case only one side can close
// within the wait, so the record cannot depend on which copy goroutine wins
// a lock. Times are measured from just before the client sent its request,
// which comes before either side's idle timer starts, so the lower bounds
// hold however late a callback runs, and a loaded machine only makes the
// close later.
//
// These tests run on the wall clock and take about 5.5 s and 5 s.

const (
	kaEngineIdle = 5 * time.Second // vLLM's keep-alive default
	kaNever      = 0               // an engine double that never closes an idle connection
)

// kaFix is a front on the wall clock with one Instance on an engine double
// that closes idle connections after engineIdle (kaNever: never), reached
// through a kaRelay.
type kaFix struct {
	*harness
	e     *enginetest.Engine
	relay *kaRelay
}

func newKaFix(t *testing.T, edit func(*Config), engineIdle time.Duration) *kaFix {
	t.Helper()
	h := newHarnessReal(t, edit)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: nil, IdleTimeout: engineIdle})
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

// chat sends one request and returns its status, its body and the time just
// before it was sent.
func (k *kaFix) chat(t *testing.T) (int, string, time.Time) {
	t.Helper()
	sent := time.Now()
	resp := stPost(t, k.harness, "/v1/chat/completions", `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	return resp.StatusCode, string(b), sent
}

// firstClosed waits for the relay to see the first connection end, and
// returns who closed it, "front" or "engine", and when the relay saw it.
func (k *kaFix) firstClosed(t *testing.T) (string, time.Time) {
	t.Helper()
	var by string
	var at time.Time
	waitState(t, "the first connection to close", func() bool {
		by, at = k.relay.closed(0)
		return by != ""
	})
	return by, at
}

// TestKeepAliveIdleTimeoutIsBelowTheEngines: the transport the front builds
// for an Instance drops idle connections after upstreamIdleTimeout, and the
// default, 4 s, is below vLLM's 5 s keep-alive (HBEP-5, Parameters).
// TestClientServerHasNoDeadlineBelowTheFronts checks upstreamTransport's.
func TestKeepAliveIdleTimeoutIsBelowTheEngines(t *testing.T) {
	cfg := Config{}.WithDefaults()
	if d := cfg.UpstreamIdleTimeout.D(); d != 4*time.Second || d >= kaEngineIdle {
		t.Errorf("upstreamIdleTimeout defaults to %s, want 4s, below the engine's %s", d, kaEngineIdle)
	}
	for _, want := range []time.Duration{cfg.UpstreamIdleTimeout.D(), 7 * time.Second} {
		c := cfg
		c.UpstreamIdleTimeout = Duration(want)
		src := NewStaticTransports()
		uid := types.UID("ka-uid")
		src.Set(uid, "127.0.0.1:9")
		tr, ok := newInstanceTransports(c, src).get(uid)
		if !ok {
			t.Fatal("no transport for a registered Instance")
		}
		if tr.IdleConnTimeout != want {
			t.Errorf("with upstreamIdleTimeout %s the Instance's transport has IdleConnTimeout %s", want, tr.IdleConnTimeout)
		}
	}
}

// TestKeepAliveFrontDropsItsIdleUpstreamConnection: with an engine that never
// closes an idle connection, the front closes its first connection by its
// upstreamIdleTimeout, no sooner than 4 s after the request; after an idle
// past vLLM's 5 s a request succeeds, on a new connection.
func TestKeepAliveFrontDropsItsIdleUpstreamConnection(t *testing.T) {
	k := newKaFix(t, nil, kaNever)
	status, body, sent := k.chat(t)
	if status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	if cs := k.e.Conns(); len(cs) != 1 || cs[0].Requests != 1 {
		t.Fatalf("after the first request the engine has connections %+v, want one that carried one request", cs)
	}

	by, at := k.firstClosed(t)
	if by != "front" {
		t.Fatalf("the first connection was closed by the %s; the engine never closes one, want the front", by)
	}
	if d, min := at.Sub(sent), k.cfg.UpstreamIdleTimeout.D(); d < min {
		t.Errorf("the front closed the first connection %v after the request, before upstreamIdleTimeout's %v", d, min)
	}
	// Idle past the engine's 5 s, then send again.
	idle := kaEngineIdle + 500*time.Millisecond
	time.Sleep(time.Until(sent.Add(idle)))

	status, body, _ = k.chat(t)
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

// TestKeepAliveControlTheEngineClosesFirst: with the front keeping idle
// connections far longer than the test waits, the engine's own 5 s close is
// seen as the engine's, no sooner than 5 s after the request. Without this,
// the front's close above could be the relay's habit.
func TestKeepAliveControlTheEngineClosesFirst(t *testing.T) {
	k := newKaFix(t, func(c *Config) { c.UpstreamIdleTimeout = Duration(10 * time.Minute) }, kaEngineIdle)
	status, body, sent := k.chat(t)
	if status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	by, at := k.firstClosed(t)
	if by != "engine" {
		t.Fatalf("with a front that keeps idle connections 10 min, the first connection was closed by the %s; want the engine, at its %v", by, kaEngineIdle)
	}
	if d := at.Sub(sent); d < kaEngineIdle {
		t.Errorf("the engine closed the first connection %v after the request, before its %v", d, kaEngineIdle)
	}
}

// kaRelay passes bytes between the front and the engine, one engine
// connection per front connection, and records for each pair which side
// ended it first, and when: the side whose read reached the end of its stream
// first is the side that closed the connection. It serves until the test
// ends.
type kaRelay struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	by     []string    // per accepted connection: "" while open, then "front" or "engine"
	at     []time.Time // when the closer's end was read
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
		r.at = append(r.at, time.Time{})
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
	now := time.Now()
	r.mu.Lock()
	if r.by[i] == "" {
		r.by[i], r.at[i] = side, now
	}
	r.mu.Unlock()
	src.Close()
	dst.Close()
}

// closed is who closed the i-th connection and when the relay saw it, or ""
// while it is open or not yet accepted.
func (r *kaRelay) closed(i int) (string, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.by) {
		return "", time.Time{}
	}
	return r.by[i], r.at[i]
}

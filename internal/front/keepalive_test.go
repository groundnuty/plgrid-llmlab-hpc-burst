package front

import (
	"io"
	"net/http"
	"strings"
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
// no timer in the front. So the tests look at when the first connection
// closed, from the engine's side: with the 4 s timer it closes about 4 s after
// the engine finished the first request, before the engine's own 5 s; and the
// control, with the front's timer raised above 5 s, shows the same
// measurement reading about 5 s, so the first test can fail.
//
// These tests run on the wall clock and take about 5.5 s and 5 s.

const (
	kaEngineIdle = 5 * time.Second // vLLM's keep-alive default, which the double uses
)

// kaFix is a front on the wall clock with one Instance on an engine double
// that closes idle connections after 5 s.
type kaFix struct {
	*harness
	e *enginetest.Engine
}

func newKaFix(t *testing.T, edit func(*Config)) *kaFix {
	t.Helper()
	h := newHarnessReal(t, edit)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: nil, IdleTimeout: kaEngineIdle})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	// A lease end after the wall clock's now, past the drain window.
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	h.apply()
	return &kaFix{harness: h, e: e}
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

// waitClosed waits for the engine's first connection to close, and returns how
// long after it went idle.
func (k *kaFix) firstClosedAfterIdle(t *testing.T, within time.Duration) time.Duration {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cs := k.e.Conns(); len(cs) > 0 && !cs[0].Closed.IsZero() {
			return cs[0].Closed.Sub(cs[0].Idle)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the engine's first connection did not close within %v", within)
	return 0
}

// TestKeepAliveFrontDropsItsIdleConnectionBeforeTheEnginesTimeout: idle past
// the engine's 5 s, then send again: the request succeeds, on a new
// connection, and the first connection was closed at about 4 s, by the
// front's upstreamIdleTimeout, before the engine's 5 s.
func TestKeepAliveFrontDropsItsIdleConnectionBeforeTheEnginesTimeout(t *testing.T) {
	k := newKaFix(t, nil)
	if status, body := k.chat(t); status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	if cs := k.e.Conns(); len(cs) != 1 || cs[0].Requests != 1 {
		t.Fatalf("after the first request the engine has connections %+v, want one that carried one request", cs)
	}

	time.Sleep(kaEngineIdle + 500*time.Millisecond) // idle past 5 s

	status, body := k.chat(t)
	if status != http.StatusOK {
		t.Fatalf("the request after an idle of %v failed: status %d: %s", kaEngineIdle+500*time.Millisecond, status, body)
	}
	cs := k.e.Conns()
	if len(cs) != 2 || cs[0].Requests != 1 || cs[1].Requests != 1 {
		t.Fatalf("the engine's connections %+v, want two, one request each: the second request arrived on a new connection", cs)
	}
	if cs[0].Closed.IsZero() {
		t.Fatalf("the first connection is still open after the idle")
	}
	// The measurement that can fail: closed by the front at 4 s, not by the
	// engine at 5 s. The bounds leave 0.4 s each way.
	if d := cs[0].Closed.Sub(cs[0].Idle); d < 3500*time.Millisecond || d > 4600*time.Millisecond {
		t.Errorf("the first connection closed %v after it went idle, want about 4 s (upstreamIdleTimeout), before the engine's %v", d, kaEngineIdle)
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
// measurement with upstreamIdleTimeout at 30 s reads about 5 s, the engine's
// own close. Without this the 4 s above could be the measurement's habit.
func TestKeepAliveControlLongerUpstreamIdleLetsTheEngineClose(t *testing.T) {
	k := newKaFix(t, func(c *Config) { c.UpstreamIdleTimeout = Duration(30 * time.Second) })
	if status, body := k.chat(t); status != http.StatusOK {
		t.Fatalf("the first request: status %d: %s", status, body)
	}
	d := k.firstClosedAfterIdle(t, kaEngineIdle+3*time.Second)
	if d < 4800*time.Millisecond || d > 5600*time.Millisecond {
		t.Errorf("with a front that keeps idle connections 30 s, the first connection closed %v after it went idle, want about the engine's %v", d, kaEngineIdle)
	}
}

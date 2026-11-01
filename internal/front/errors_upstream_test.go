package front

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The rows of HBEP-5's Errors table that name a target: 502 and the two 504s,
// where the front raises the error about a target; and the errors a target
// raises, which pass through unchanged.

// refusedAddr is a loopback address nothing listens on. The front's tests may
// not free a port of their block and dial it, because a parallel run of
// another package could bind the freed port first; port 1 is reserved and
// refuses.
const refusedAddr = "127.0.0.1:1"

// TestErrorUpstreamUnreachable is row 7: no byte reached the target, after
// the one resend, is 502 upstream_unreachable. It names the front and the
// target it tried.
func TestErrorUpstreamUnreachable(t *testing.T) {
	if c, err := net.DialTimeout("tcp", refusedAddr, time.Second); err == nil {
		c.Close()
		t.Fatalf("something listens on %s; this test needs a refused address", refusedAddr)
	}
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.trans.Set(s.uid, refusedAddr)
	s.apply()

	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
	got := readAll(t, resp)
	checkError(t, resp, got, wantError{status: 502, code: "upstream_unreachable"})
	id := checkAttribution(t, resp, s.instanceAttr("front"))
	rec := s.record(id)
	if rec.Status != 502 || rec.Emitter != "front" || rec.Instance != "ns/md-0" {
		t.Errorf("usage record: status %d emitter %q instance %q, want 502 front ns/md-0", rec.Status, rec.Emitter, rec.Instance)
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests, want 0: it was not the address dialled", n)
	}
}

// holdThenAdvance sends a chat request that the engine holds before any
// byte, waits until the engine has it, then moves the fake clock by d. The
// timers of the request were armed before it was sent, so the move is a
// deterministic cut of a request that has had no byte.
func holdThenAdvance(t *testing.T, s *errFix, d time.Duration) (*http.Response, string) {
	t.Helper()
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	s.e.Set(enginetest.Behaviour{Hold: hold})
	type result struct {
		resp *http.Response
		body string
	}
	done := make(chan result, 1)
	go func() {
		resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		done <- result{resp, readAll(t, resp)}
	}()
	errWaitFor(t, "the engine to hold the request", func() bool { return s.e.Open() == 1 })
	s.clk.Advance(d)
	r := <-done
	return r.resp, r.body
}

// TestErrorRequestTimeoutDeadline is row 8: maxRequestDuration reached before
// the first byte is 504 request_timeout with detail deadline. It names the
// front and the target.
func TestErrorRequestTimeoutDeadline(t *testing.T) {
	// The idle time-out is set above the deadline so that the deadline is the
	// cut that fires first.
	s := newErrFix(t, func(c *Config) {
		c.MaxRequestDuration = Duration(time.Hour)
		c.IdleTimeout = Duration(3 * time.Hour)
	}, instanceOpts{}, "")
	resp, got := holdThenAdvance(t, s, 61*time.Minute)
	checkError(t, resp, got, wantError{status: 504, code: "request_timeout", detail: "deadline"})
	id := checkAttribution(t, resp, s.instanceAttr("front"))
	rec := s.record(id)
	if rec.Status != 504 || rec.Emitter != "front" || rec.CutBy == nil || *rec.CutBy != "front_deadline" {
		t.Errorf("usage record: status %d emitter %q cutBy %v, want 504 front front_deadline", rec.Status, rec.Emitter, rec.CutBy)
	}
}

// TestErrorRequestTimeoutIdle is row 9: idleTimeout reached before the first
// byte is 504 request_timeout with detail idle. It names the front and the
// target.
func TestErrorRequestTimeoutIdle(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	// idleTimeout is 30 minutes by default; the deadline, 2 hours, is not near.
	resp, got := holdThenAdvance(t, s, 31*time.Minute)
	checkError(t, resp, got, wantError{status: 504, code: "request_timeout", detail: "idle"})
	id := checkAttribution(t, resp, s.instanceAttr("front"))
	rec := s.record(id)
	if rec.Status != 504 || rec.Emitter != "front" || rec.CutBy == nil || *rec.CutBy != "front_idle" {
		t.Errorf("usage record: status %d emitter %q cutBy %v, want 504 front front_idle", rec.Status, rec.Emitter, rec.CutBy)
	}
}

// TestErrorFromEnginePassesThrough: an error from a target passes through
// unchanged, status and body, with emitter instance (HBEP-5, Errors).
func TestErrorFromEnginePassesThrough(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	engineBody := `{"error":{"message":"CUDA out of memory in the engine","type":"server_error","param":null,"code":500}}`
	s.e.Set(enginetest.Behaviour{Status: 500, ErrorBody: engineBody})

	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
	got := readAll(t, resp)
	if resp.StatusCode != 500 {
		t.Errorf("status = %d, want the engine's 500", resp.StatusCode)
	}
	if got != engineBody {
		t.Errorf("body changed on the way:\n got %s\nwant %s", got, engineBody)
	}
	id := checkAttribution(t, resp, s.instanceAttr("instance"))
	rec := s.record(id)
	if rec.Status != 500 || rec.Emitter != "instance" {
		t.Errorf("usage record: status %d emitter %q, want 500 instance", rec.Status, rec.Emitter)
	}
	if rec.Finish == nil || *rec.Finish != "error" {
		t.Errorf("usage record finish = %v, want error", rec.Finish)
	}
}

// TestErrorFromLLMLabPassesThrough: LLMLab's errors reach the client as it
// sent them, status and body, with emitter provider (HBEP-5, The LLMLab
// route). The expected bytes come from asking the LLMLab double directly.
func TestErrorFromLLMLabPassesThrough(t *testing.T) {
	t.Run("503 for an inactive model", func(t *testing.T) {
		m := newErrManagedFix(t, nil)
		m.llm.SetActive(efCatalog, false)
		direct := errChatBody(efCatalog, false, "hello there")
		wantStatus, wantBody := errDirect(t, m.llm.BaseURL()+"/chat/completions", m.key, direct)
		if wantStatus != 503 {
			t.Fatalf("the LLMLab double answers %d for an inactive model, want 503", wantStatus)
		}

		resp := m.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		got := readAll(t, resp)
		if resp.StatusCode != wantStatus || got != wantBody {
			t.Errorf("through the front: %d %s\nfrom LLMLab:       %d %s", resp.StatusCode, got, wantStatus, wantBody)
		}
		id := checkAttribution(t, resp, efManagedAttr)
		rec := m.record(id)
		if rec.Status != 503 || rec.Emitter != "provider" || rec.Provider != "llmlab-a" {
			t.Errorf("usage record: status %d emitter %q provider %q, want 503 provider llmlab-a", rec.Status, rec.Emitter, rec.Provider)
		}
	})
	t.Run("401 for a key LLMLab rejects", func(t *testing.T) {
		m := newErrManagedFix(t, nil)
		m.llm.RejectKey(m.key, 401)
		wantStatus, wantBody := errDirect(t, m.llm.BaseURL()+"/chat/completions", m.key, errChatBody(efCatalog, false, "hello there"))
		if wantStatus != 401 {
			t.Fatalf("the LLMLab double answers %d for a rejected key, want 401", wantStatus)
		}

		resp := m.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		got := readAll(t, resp)
		if resp.StatusCode != http.StatusUnauthorized || got != wantBody {
			t.Errorf("through the front: %d %s\nfrom LLMLab:       %d %s", resp.StatusCode, got, wantStatus, wantBody)
		}
		id := checkAttribution(t, resp, efManagedAttr)
		if rec := m.record(id); rec.Status != 401 || rec.Emitter != "provider" || rec.Provider != "llmlab-a" {
			t.Errorf("usage record: status %d emitter %q provider %q, want 401 provider llmlab-a", rec.Status, rec.Emitter, rec.Provider)
		}
	})
}

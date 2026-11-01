package front

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestStreamHeadersDeadline (PR #95 review: Astra B7; HBEP-5, Time-outs and
// keep-alive): an Instance that reads a streamed request and sends no
// headers gets 504 request_timeout, detail headers, at streamHeadersTimeout
// (30 s) after the request's last byte; it is not resent, the record says
// cutBy front_headers, and it counts toward the pause. A non-streamed
// request is exempt.
//
// "Not cut before 30 s" is read from the fake clock, not from a real wait:
// Advance runs every timer that falls due on the test's goroutine, so a timer
// due by 29 s has fired, and is no longer pending, when Advance returns. The
// request's timers all still pending after Advance(29 s) is therefore the
// state that a cut before 30 s would have changed first. (The test used to
// wait 200 ms of real time for an answer that should not come, which a slow
// machine could pass wrongly; PR #137 review, Astra.)
func TestStreamHeadersDeadline(t *testing.T) {
	for _, stream := range []bool{true, false} {
		name := map[bool]string{true: "streamed", false: "not streamed (exempt)"}[stream]
		t.Run(name, func(t *testing.T) {
			f := newScFix(t, scOpts{cfg: func(c *Config) { c.StreamHeadersTimeout = Duration(DefaultStreamHeaders) }})
			e := f.engines[0]
			hold := make(chan struct{})
			e.Set(enginetest.Behaviour{Hold: hold})
			base := f.clk.Pending()
			call := scGo(f.harness, errChatBody(scModel, stream, "hi"))
			// The deadline, idle and lease timers come with the target; a
			// streamed request's headers timer comes at its last byte.
			want := base + 3
			if stream {
				want++
			}
			waitState(t, "the request's timers", func() bool { return e.Open() == 1 && f.clk.Pending() == want })
			f.clk.Advance(29 * time.Second)
			if n := f.clk.Pending(); n != want || doneNow(call) {
				t.Fatalf("after 29 s %d timers are pending, want %d, and the request finished %v: a timer fell due before streamHeadersTimeout", n, want, doneNow(call))
			}
			f.clk.Advance(1 * time.Second)
			if !stream {
				// No headers timer: the deadline (2 h), idle (30 min) and
				// lease timers are all still pending at 30 s, and the 200
				// once the engine answers shows the request was never cut.
				if n := f.clk.Pending(); n != want || doneNow(call) {
					t.Fatalf("after 30 s %d timers are pending, want %d, and the request finished %v: a non-streamed request was cut", n, want, doneNow(call))
				}
				close(hold)
				if resp, body := call.wait(t); resp.StatusCode != 200 {
					t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
				}
				return
			}
			resp, body := call.wait(t)
			checkErrorBody(t, body, codeRequestTimeout, "headers")
			if resp.StatusCode != 504 {
				t.Errorf("status %d, want 504", resp.StatusCode)
			}
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			if rec.CutBy == nil || *rec.CutBy != cutFrontHeaders {
				t.Errorf("cutBy %v, want front_headers", rec.CutBy)
			}
			if n := len(e.Requests()); n != 1 {
				t.Errorf("the engine received %d requests, want 1: not resent", n)
			}
			f.wantFailures(t, "a missed streamed-headers deadline", 1)
			close(hold)
		})
	}
}

// doneNow reports whether the call has finished, without waiting.
func doneNow(c *scCall) bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// TestStreamHeadersTimeoutDefault: HBEP-5's Parameters give
// streamHeadersTimeout 30 s.
func TestStreamHeadersTimeoutDefault(t *testing.T) {
	if got := (Config{}).WithDefaults().StreamHeadersTimeout.D(); got != 30*time.Second {
		t.Errorf("streamHeadersTimeout default %v, want 30s", got)
	}
}

package front

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// Row 10 of HBEP-5's Errors table: a stream that fails after its headers were
// sent ends with one data: event holding the error body, then the end of the
// stream, never [DONE]. The code is stream_cut; the detail is upstream,
// deadline, idle or lease_end; the names are the target's, as in the headers
// already sent (emitter instance, with provider, deployment ID and Instance).

// splitEvents splits an SSE body into its events (blocks ended by a blank
// line), without the blank line.
func splitEvents(body string) []string {
	var out []string
	for _, e := range strings.Split(body, "\n\n") {
		if strings.TrimSpace(e) != "" {
			out = append(out, e)
		}
	}
	return out
}

// checkStreamCut asserts that the last event of body is the stream_cut error
// with the detail, that no event is [DONE], and returns the events before it.
func checkStreamCut(t *testing.T, body, detail string) []string {
	t.Helper()
	events := splitEvents(body)
	if len(events) == 0 {
		t.Fatalf("the stream is empty, want a stream_cut event at its end")
	}
	for _, e := range events {
		if strings.Contains(e, "[DONE]") {
			t.Errorf("the stream carries [DONE] (event %q), want it never after a cut", e)
		}
	}
	last := events[len(events)-1]
	data, ok := strings.CutPrefix(last, "data: ")
	if !ok {
		t.Fatalf("the last event %q is not a data: event", last)
	}
	checkErrorBody(t, data, "stream_cut", detail)
	return events[:len(events)-1]
}

// midStreamCut sends a streaming chat request to an engine that sends one
// token at once and the next after 10 s of real time, reads the first event,
// then moves the fake clock by d: the timers armed for the request fire with
// one token delivered. It returns the request ID and the whole body.
func midStreamCut(t *testing.T, s *errFix, d time.Duration) (string, string) {
	t.Helper()
	s.e.Set(enginetest.Behaviour{Tokens: 3, PerToken: 10 * time.Second})
	resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200: the headers were sent before the cut", resp.StatusCode)
	}
	id := checkAttribution(t, resp, s.instanceAttr("instance"))
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	first := readEvent(t, br)
	s.clk.Advance(d)
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("reading the rest of the stream: %v", err)
	}
	return id, first + string(rest)
}

// TestErrorStreamCut covers row 10, one subtest per detail.
func TestErrorStreamCut(t *testing.T) {
	t.Run("upstream: the engine kills the connection", func(t *testing.T) {
		s := newErrFix(t, nil, instanceOpts{}, "")
		s.e.Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})
		resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
		id := checkAttribution(t, resp, s.instanceAttr("instance"))
		body := readAll(t, resp)
		before := checkStreamCut(t, body, "upstream")
		if len(before) != 2 {
			t.Errorf("%d events before the error, want the 2 tokens the engine sent: %q", len(before), before)
		}
		rec := s.record(id)
		if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "upstream" {
			t.Errorf("usage record finish %v cutBy %v, want cut upstream", rec.Finish, rec.CutBy)
		}
	})
	t.Run("upstream: the stream ends without its end", func(t *testing.T) {
		s := newErrFix(t, nil, instanceOpts{}, "")
		s.e.Set(enginetest.Behaviour{Tokens: 3, NoFinish: true, NoDone: true})
		resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
		id := checkAttribution(t, resp, s.instanceAttr("instance"))
		body := readAll(t, resp)
		before := checkStreamCut(t, body, "upstream")
		if len(before) != 3 {
			t.Errorf("%d events before the error, want the 3 tokens the engine sent: %q", len(before), before)
		}
		rec := s.record(id)
		if rec.Finish == nil || *rec.Finish != "truncated" || rec.CutBy == nil || *rec.CutBy != "upstream" {
			t.Errorf("usage record finish %v cutBy %v, want truncated upstream", rec.Finish, rec.CutBy)
		}
	})
	t.Run("deadline: maxRequestDuration passes mid-stream", func(t *testing.T) {
		s := newErrFixClock(t, func(c *Config) {
			c.MaxRequestDuration = Duration(time.Hour)
			c.IdleTimeout = Duration(3 * time.Hour)
		}, instanceOpts{}, "", true)
		id, body := midStreamCut(t, s, 61*time.Minute)
		if before := checkStreamCut(t, body, "deadline"); len(before) != 1 {
			t.Errorf("%d events before the error, want the 1 token read before the cut: %q", len(before), before)
		}
		rec := s.record(id)
		if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "front_deadline" {
			t.Errorf("usage record finish %v cutBy %v, want cut front_deadline", rec.Finish, rec.CutBy)
		}
	})
	t.Run("idle: no byte for idleTimeout mid-stream", func(t *testing.T) {
		s := newErrFixClock(t, nil, instanceOpts{}, "", true)
		// idleTimeout is 30 minutes by default; the deadline, 2 hours, is not near.
		id, body := midStreamCut(t, s, 31*time.Minute)
		if before := checkStreamCut(t, body, "idle"); len(before) != 1 {
			t.Errorf("%d events before the error, want the 1 token read before the cut: %q", len(before), before)
		}
		rec := s.record(id)
		if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "front_idle" {
			t.Errorf("usage record finish %v cutBy %v, want cut front_idle", rec.Finish, rec.CutBy)
		}
	})
	t.Run("lease_end: the Instance's lease ends mid-stream", func(t *testing.T) {
		// The lease ends at t0 + 1 h; a 10-minute drain window puts the drain
		// start at 50 minutes, which the request in flight survives; the
		// lease end at 60 minutes cuts it. The idle time-out is raised so
		// that it does not fire first.
		s := newErrFixClock(t, func(c *Config) { c.IdleTimeout = Duration(3 * time.Hour) },
			instanceOpts{leaseEnd: t0.Add(time.Hour)}, "10m", true)
		id, body := midStreamCut(t, s, 61*time.Minute)
		if before := checkStreamCut(t, body, "lease_end"); len(before) != 1 {
			t.Errorf("%d events before the error, want the 1 token read before the cut: %q", len(before), before)
		}
		rec := s.record(id)
		if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "lease_end" {
			t.Errorf("usage record finish %v cutBy %v, want cut lease_end", rec.Finish, rec.CutBy)
		}
	})
}

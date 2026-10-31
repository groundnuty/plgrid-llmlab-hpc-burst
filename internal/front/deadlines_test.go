package front

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Deadlines:
//   - a 7,000-token stream at one token per second (about 117 min) completes
//     under the default 2 h maxRequestDuration;
//   - a first token after 1,100 s completes, because idleTimeout is 30 min;
//   - positive controls: with maxRequestDuration at 60 s, a 150 s stream is
//     cut at about 60 s with cutBy front_deadline, so the test can see a cut.
//
// A deadline test that never reaches its deadline proves nothing, so each
// completing case has a control that is cut by the same timer at a smaller
// setting. Expected values are worked from the engine double's pace and the
// defaults of HBEP-5 (Parameters); the front's output is never the reference.

// TestDeadlinesLongStreamCompletesUnderTheDefault: the engine double sends
// 7,000 tokens, pausing 1 s before every token after the first: 6,999 pauses,
// 6,999 s = 116 min 39 s on the fake clock, below the default 2 h (7,200 s).
// The idle time-out (30 min) is reset by every token.
func TestDeadlinesLongStreamCompletesUnderTheDefault(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 7000, PerToken: time.Second})

	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	id := resp.Header.Get("hpcburst-request-id")
	body := readAll(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	// The fake clock moved by 6,999 s: the front's real 2 h deadline had
	// more than 3 minutes left, and the test did pass the 1 h 56 min mark.
	if got := f.clk.Now().Sub(t0); got < 116*time.Minute || got >= 2*time.Hour {
		t.Fatalf("the fake clock moved by %v, want about 117 min and below 2 h", got)
	}
	events := splitEvents(body)
	if n := scContentEvents(body); n != 7000 {
		t.Errorf("the client got %d token events, want 7000", n)
	}
	finish := false
	for _, e := range events {
		finish = finish || strings.Contains(e, `"finish_reason":"stop"`)
		if strings.Contains(e, "stream_cut") {
			t.Errorf("a stream_cut event on a stream that completed: %q", e)
		}
	}
	if !finish {
		t.Errorf("no event carries a finish reason")
	}
	if last := events[len(events)-1]; last != "data: [DONE]" {
		t.Errorf("the stream ends with %q, want data: [DONE]", last)
	}

	rec := f.record(id)
	if rec.Finish == nil || *rec.Finish != "stop" {
		t.Errorf("finish = %v, want stop", rec.Finish)
	}
	if rec.CutBy != nil {
		t.Errorf("cutBy = %q, want none", *rec.CutBy)
	}
	// "About 7,000,000": 6,999,000 ms by the pace above.
	if d := rec.DurationMs; d < 6_990_000 || d > 7_010_000 {
		t.Errorf("durationMs = %d, want about 7,000,000", d)
	}
}

// TestDeadlinesFirstTokenAfter1100sCompletes: a first byte after 1,100 s is
// inside idleTimeout (30 min = 1,800 s), so the front lets it through. The
// second half of HBEP-5's item, that the lifecycle controller does not
// restart the worker (HBEP-7's no-progress watchdog), needs the lifecycle
// controller (#61) and is pending there.
func TestDeadlinesFirstTokenAfter1100sCompletes(t *testing.T) {
	const wait = 1100 * time.Second
	t.Run("a reply with no byte at all for 1,100 s", func(t *testing.T) {
		f := newScFix(t, scOpts{})
		f.engines[0].Set(enginetest.Behaviour{Tokens: 3, FirstToken: wait})
		resp := f.errPost(scChat, errChatBody(scModel, false, "hello there"))
		id := resp.Header.Get("hpcburst-request-id")
		body := readAll(t, resp)
		if resp.StatusCode != 200 {
			t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
		}
		if got := f.clk.Now().Sub(t0); got != wait {
			t.Fatalf("the fake clock moved by %v, want the engine's %v", got, wait)
		}
		rec := f.record(id)
		if rec.Finish == nil || *rec.Finish != "stop" || rec.CutBy != nil {
			t.Errorf("finish %v cutBy %v, want stop and none", rec.Finish, rec.CutBy)
		}
	})
	t.Run("a stream whose first token comes after 1,100 s", func(t *testing.T) {
		f := newScFix(t, scOpts{})
		f.engines[0].Set(enginetest.Behaviour{Tokens: 3, FirstToken: wait})
		resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
		id := resp.Header.Get("hpcburst-request-id")
		body := readAll(t, resp)
		if resp.StatusCode != 200 || scContentEvents(body) != 3 || !strings.HasSuffix(body, "data: [DONE]\n\n") {
			t.Fatalf("status %d, %d token events, body %q: want 200, 3 tokens and [DONE]", resp.StatusCode, scContentEvents(body), body)
		}
		if got := f.clk.Now().Sub(t0); got != wait {
			t.Errorf("the fake clock moved by %v, want %v", got, wait)
		}
		if rec := f.record(id); rec.CutBy != nil || rec.Finish == nil || *rec.Finish != "stop" {
			t.Errorf("finish %v cutBy %v, want stop and none", rec.Finish, rec.CutBy)
		}
	})
	// The same wait is cut when idleTimeout is below it: the 1,100 s are a
	// real test of the 30 min, not a wait no setting would cut. The engine
	// holds the request and the test moves the clock (scHoldThenAdvance), so
	// the engine cannot write its reply in the instant the timer fires.
	t.Run("control: idleTimeout 1,000 s cuts a reply that is silent for 1,100 s", func(t *testing.T) {
		f := newScFix(t, scOpts{cfg: func(c *Config) { c.IdleTimeout = Duration(1000 * time.Second) }})
		resp, body := scHoldThenAdvance(t, f, wait)
		checkError(t, resp, body, wantError{status: 504, code: "request_timeout", detail: "idle"})
	})
}

// scHoldThenAdvance sends a non-streamed chat request that the engine holds
// before any byte, waits until the engine has it (the front armed its timers
// before sending), then moves the fake clock by d. The cut of a request that
// has had no byte is then certain: the engine writes nothing until the test
// ends. (An engine that paced itself on the fake clock would write its reply
// as soon as the timer fired, and the front's select between the cancelled
// context and that reply could go either way.)
func scHoldThenAdvance(t *testing.T, f *scFix, d time.Duration) (*http.Response, string) {
	t.Helper()
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	f.setAll(enginetest.Behaviour{Tokens: 3, Hold: hold})
	call := scGo(f.harness, errChatBody(scModel, false, "hello there"))
	errWaitFor(t, "the engine to hold the request", func() bool { return f.engines[0].Open() == 1 })
	f.clk.Advance(d)
	return call.wait(t)
}

// TestDeadlinesPositiveControlOnTheFakeClock: with maxRequestDuration at 60 s,
// a stream that would run 150 s (150 tokens, one per second) is cut at 60 s.
// The engine runs on the wall clock and the test moves the fake clock one
// second after each token it reads (scLockstep), 60 times and no more, so the
// request lives exactly 60 s of fake time: the deadline timer falls due at the
// 60th move, and durationMs is 60,000 by that arithmetic alone.
func TestDeadlinesPositiveControlOnTheFakeClock(t *testing.T) {
	f := newScFix(t, scOpts{engineReal: true, cfg: func(c *Config) { c.MaxRequestDuration = Duration(60 * time.Second) }})
	// 50 ms between tokens on the wall clock: the test must read 60 tokens
	// and move the fake clock before the engine sends the other 90 (4.5 s of
	// slack), or the stream would end before the cut. A tighter pace raced on
	// a loaded machine.
	f.engines[0].Set(enginetest.Behaviour{Tokens: 150, PerToken: 50 * time.Millisecond})

	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200: the headers were sent before the cut", resp.StatusCode)
	}
	id := resp.Header.Get("hpcburst-request-id")
	content, body := scLockstep(t, f.harness, resp, time.Second, 60*time.Second)

	before := checkStreamCut(t, body, "deadline") // the last event is stream_cut, detail deadline; no [DONE]
	scOneCut(t, body)
	if content < 60 || content >= 150 {
		t.Errorf("the client got %d token events, want at least the 60 read before the cut and fewer than the 150 of the whole stream", content)
	}
	for _, e := range before {
		if strings.Contains(e, `"finish_reason":"stop"`) {
			t.Errorf("a finish reason arrived on a stream that was cut: %q", e)
		}
	}
	rec := f.record(id)
	if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "front_deadline" {
		t.Errorf("finish %v cutBy %v, want cut and front_deadline", rec.Finish, rec.CutBy)
	}
	if rec.DurationMs != 60_000 {
		t.Errorf("durationMs = %d, want 60,000: 60 moves of 1 s", rec.DurationMs)
	}
}

// TestDeadlinesPositiveControlInRealTime: the same cut on the wall clock.
// maxRequestDuration is 2 s, and the engine would send 50 tokens 100 ms apart
// (4.9 s), so the front cuts it at about 2 s, after about 21 tokens.
func TestDeadlinesPositiveControlInRealTime(t *testing.T) {
	f := newScFix(t, scOpts{realClock: true, cfg: func(c *Config) { c.MaxRequestDuration = Duration(2 * time.Second) }})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 50, PerToken: 100 * time.Millisecond})

	start := time.Now()
	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	id := resp.Header.Get("hpcburst-request-id")
	body := readAll(t, resp)
	elapsed := time.Since(start)

	checkStreamCut(t, body, "deadline")
	scOneCut(t, body)
	if elapsed < 1900*time.Millisecond || elapsed > 4500*time.Millisecond {
		t.Errorf("the stream ended after %v, want about 2 s and well before the 4.9 s of the whole stream", elapsed)
	}
	// A token every 100 ms for 2 s is at most 21 or 22 tokens.
	if n := scContentEvents(body); n < 10 || n > 25 {
		t.Errorf("the client got %d token events, want about 21", n)
	}
	rec := f.record(id)
	if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "front_deadline" {
		t.Errorf("finish %v cutBy %v, want cut and front_deadline", rec.Finish, rec.CutBy)
	}
	if rec.DurationMs < 1900 || rec.DurationMs > 4500 {
		t.Errorf("durationMs = %d, want about 2,000", rec.DurationMs)
	}
}

// TestDeadlinesIdleBeforeTheFirstByte: with idleTimeout at 30 s, a reply whose
// first byte is 60 s away is cut at 30 s, before any byte: 504 request_timeout
// with detail idle (Errors, row 9) and no stream started. The 60 s are the
// test's own move of the fake clock while the engine holds the request
// (scHoldThenAdvance): the engine double's FirstToken on the fake clock would
// let it write in the instant the timer fires. A non-streamed request, because
// the engine double sends a stream's headers at once, and a 200 with a
// stream_cut event would be the answer to a stream.
func TestDeadlinesIdleBeforeTheFirstByte(t *testing.T) {
	f := newScFix(t, scOpts{cfg: func(c *Config) { c.IdleTimeout = Duration(30 * time.Second) }})

	resp, body := scHoldThenAdvance(t, f, 60*time.Second)
	checkError(t, resp, body, wantError{status: 504, code: "request_timeout", detail: "idle"})
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "event-stream") {
		t.Errorf("Content-Type = %q: a stream was started", ct)
	}
	id := checkAttribution(t, resp, wantAttr{emitter: "front", provider: "helios-a", deployID: scDeployDisplay(f, 0), instance: f.names[0], uid: string(f.uids[0])})
	rec := f.record(id)
	if rec.Status != 504 || rec.CutBy == nil || *rec.CutBy != "front_idle" {
		t.Errorf("record: status %d cutBy %v, want 504 and front_idle", rec.Status, rec.CutBy)
	}
	// The control for the 30 s: the same wait under the default 30 min goes
	// through (TestDeadlinesFirstTokenAfter1100sCompletes).
}

// scDeployDisplay is the display form of the i-th Instance's deployment ID.
func scDeployDisplay(f *scFix, i int) string {
	return string(f.inst(f.uids[i]).Spec.DeploymentID.Display())
}

package front

// The front's one retry before the first byte (HBEP-5, open question 5, decided for beta by spike S18,
// #528): toward an Instance, a request written and then closed before the response headers, or answered
// with a 5xx before any body byte, is sent once more, to another ready Instance. Every fault here is
// proven by the engine doubles' own records (the requests each received) and the client's answer, and
// every retry's target is fixed by the ring's own walk from the request's key, so it is the other
// Instance by exclusion, never by chance.

import (
	"net/http"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// retryFix is n Instances and a session key homed on the first, with the ring's walk from it.
func retryFix(t *testing.T, n int, o scOpts) (*scFix, string, []string) {
	t.Helper()
	o.instances, o.desired = n, int32(n)
	f := newScFix(t, o)
	key := scKeys(t, f.names, f.names[0], 1)[0]
	walk := scWalk(f.names, key)
	if walk[0] != f.names[0] {
		t.Fatalf("the key's walk %v does not start at %s", walk, f.names[0])
	}
	return f, key, walk
}

// wantCounts checks how many requests each engine received, in f.names' order.
func wantCounts(t *testing.T, f *scFix, want ...int) {
	t.Helper()
	got := f.counts()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the engines received %v requests, want %v", got, want)
		}
	}
}

// wantWhole checks a whole 200 from Instance name, with no x-should-retry header.
func wantWhole(t *testing.T, resp *http.Response, body, name string) {
	t.Helper()
	if resp.StatusCode != 200 || scInstanceOf(resp) != name || scEmitterOf(resp) != "instance" {
		t.Fatalf("status %d from %q (emitter %q), want 200 from %s: %s", resp.StatusCode, scInstanceOf(resp), scEmitterOf(resp), name, body)
	}
	if v := resp.Header.Get(headerShouldRetry); v != "" {
		t.Errorf("x-should-retry %q on a success, want none", v)
	}
}

// A request written and then closed before the headers is retried to the next Instance on the key's
// walk. One usage record names it, with its 200 and no cut.
func TestARequestClosedBeforeItsHeadersIsRetriedToTheOtherInstance(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "not streamed", true: "streamed"}[stream], func(t *testing.T) {
			f, key, walk := retryFix(t, 2, scOpts{})
			f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
			resp, body := scGo(f.harness, errChatBody(scModel, stream, "hello there"), "x-session-id", key).wait(t)
			wantWhole(t, resp, body, walk[1])
			wantCounts(t, f, 1, 1)
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			if rec.Instance != walk[1] || rec.Status != 200 || rec.CutBy != nil {
				t.Errorf("usage record: Instance %q status %d cutBy %v, want %s, 200 and no cut", rec.Instance, rec.Status, rec.CutBy, walk[1])
			}
			n := 0
			for _, r := range f.records() {
				if r.RequestID == rec.RequestID {
					n++
				}
			}
			if n != 1 {
				t.Errorf("%d usage records for the request, want one", n)
			}
		})
	}
}

// A 5xx before any body byte is held and retried: the client gets the other Instance's 200, never the
// 500.
func TestA5xxBeforeAnyBodyByteIsRetriedAndNeverReachesTheClient(t *testing.T) {
	for _, status := range []int{500, 502, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			f, key, walk := retryFix(t, 2, scOpts{})
			f.engines[0].Set(enginetest.Behaviour{Status: status, ErrorBody: `{"error":{"message":"the engine failed","type":"server_error","param":null,"code":500}}`})
			resp, body := scChatOnce(t, f.harness, key)
			wantWhole(t, resp, body, walk[1])
			wantCounts(t, f, 1, 1)
		})
	}
}

// A stream that failed after its headers is not retried: the client has its headers already. Its
// control, a stream closed before its headers, is retried (the test above).
func TestAStreamPastItsHeadersIsNotRetried(t *testing.T) {
	f, key, walk := retryFix(t, 2, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 5, KillAfter: 1})
	resp, body := scGo(f.harness, errChatBody(scModel, true, "hello there"), "x-session-id", key).wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != walk[0] {
		t.Fatalf("status %d from %q, want the first Instance's 200 stream: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	wantCounts(t, f, 1, 0)
}

// The front's own time-out is never retried: a stream whose headers missed streamHeadersTimeout gets 504,
// and the other Instance receives nothing.
func TestAFrontTimeOutIsNotRetried(t *testing.T) {
	f, key, _ := retryFix(t, 2, scOpts{cfg: func(c *Config) { c.StreamHeadersTimeout = Duration(DefaultStreamHeaders) }})
	hold := make(chan struct{})
	defer close(hold)
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	base := f.clk.Pending()
	call := scGo(f.harness, errChatBody(scModel, true, "hi"), "x-session-id", key)
	// The deadline, idle and lease timers come with the target, and the headers timer at the last byte.
	waitState(t, "the request's timers", func() bool { return f.engines[0].Open() == 1 && f.clk.Pending() == base+4 })
	f.clk.Advance(DefaultStreamHeaders)
	resp, body := call.wait(t)
	checkErrorBody(t, body, codeRequestTimeout, "headers")
	if resp.StatusCode != 504 {
		t.Fatalf("status %d, want 504", resp.StatusCode)
	}
	wantCounts(t, f, 1, 0)
	// The time-out is the first Instance's: no other target was taken for the request.
	if got := scInstanceOf(resp); got != f.names[0] {
		t.Errorf("the 504 names %q, want %s", got, f.names[0])
	}
	if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.Instance != f.names[0] {
		t.Errorf("the usage record names %q, want %s", rec.Instance, f.names[0])
	}
}

// The failed Instance is skipped although it is still eligible after one failure: the retry goes to
// the next Instance on the key's walk, not back to it, and not to the one after.
func TestTheFailedInstanceIsSkippedThoughStillEligible(t *testing.T) {
	f, key, walk := retryFix(t, 3, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	resp, body := scChatOnce(t, f.harness, key)
	wantWhole(t, resp, body, walk[1])
	got := map[string]int{}
	for i, n := range f.names {
		got[n] = f.counts()[i]
	}
	if got[walk[0]] != 1 || got[walk[1]] != 1 || got[walk[2]] != 0 {
		t.Errorf("requests by Instance %v, want 1 on %s, 1 on %s and none on %s", got, walk[0], walk[1], walk[2])
	}
	if f.router.Paused(walk[0]) {
		t.Errorf("%s paused after one failure; it must still be eligible (pauseAfterFailures 3)", walk[0])
	}
}

// The first attempt's failure counts toward its own Instance's pause, once, and the retry's success on
// the other Instance doesn't reset it: after three rescued requests the first Instance is paused.
func TestTheFirstFailureCountsTowardItsOwnPause(t *testing.T) {
	f, key, walk := retryFix(t, 2, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	for k := 1; k <= 3; k++ {
		resp, body := scChatOnce(t, f.harness, key)
		wantWhole(t, resp, body, walk[1])
		if got := f.router.failures(walk[0]); got != k && !(k == 3 && f.router.Paused(walk[0])) {
			t.Fatalf("after %d rescued requests: %d failures in a row on %s, want %d", k, got, walk[0], k)
		}
	}
	if !f.router.Paused(walk[0]) {
		t.Errorf("%s not paused after three failures", walk[0])
	}
	if got := f.router.failures(walk[1]); got != 0 {
		t.Errorf("%s has %d failures, want none: it answered every retry", walk[1], got)
	}
}

// The give-up answers after the front's own retry carry x-should-retry: false: both Instances close
// before their headers; both answer 5xx; the other Instance is at its cap past queueTimeout; the other
// Instance's dial is refused.
func TestTheGiveUpAfterTheRetryCarriesTheHeader(t *testing.T) {
	cases := []struct {
		name   string
		o      scOpts
		set    func(f *scFix, walk []string)
		status int
		code   string
	}{
		{"both closed", scOpts{}, func(f *scFix, _ []string) { f.setAll(enginetest.Behaviour{CloseBeforeHeaders: true}) }, 502, codeUpstreamClosed},
		{"both 5xx", scOpts{}, func(f *scFix, _ []string) {
			f.setAll(enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"failed","type":"server_error","param":null,"code":500}}`})
		}, 500, ""},
		{"the other refused", scOpts{}, func(f *scFix, _ []string) {
			f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
			f.engines[1].Close()
		}, 502, codeUpstreamUnreachable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, key, _ := retryFix(t, 2, c.o)
			c.set(f, nil)
			resp, body := scChatOnce(t, f.harness, key)
			if resp.StatusCode != c.status || resp.Header.Get(headerShouldRetry) != "false" {
				t.Fatalf("status %d, x-should-retry %q, want %d and false: %s", resp.StatusCode, resp.Header.Get(headerShouldRetry), c.status, body)
			}
			if c.code != "" {
				checkErrorBody(t, body, c.code, map[string]string{codeUpstreamClosed: "tunnel", codeUpstreamUnreachable: ""}[c.code])
			}
			if c.name != "the other refused" {
				wantCounts(t, f, 1, 1)
			} else {
				wantCounts(t, f, 1, 0)
			}
		})
	}
	t.Run("the other at its cap", func(t *testing.T) {
		f, key, walk := retryFix(t, 2, scOpts{
			cfg:  func(c *Config) { c.QueueTimeout = Duration(200 * time.Millisecond) },
			inst: func(i int, o *instanceOpts) { o.maxNumSeqs = 1 },
		})
		// The second Instance holds one request, its cap; a session homed on it keeps it there.
		hold := make(chan struct{})
		defer close(hold)
		f.engines[1].Set(enginetest.Behaviour{Hold: hold})
		busy := scGo(f.harness, errChatBody(scModel, false, "busy"), "x-session-id", scKeys(t, f.names, walk[1], 1)[0])
		waitState(t, "the other Instance at its cap", func() bool { return f.engines[1].Open() == 1 })
		f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
		resp, body := scChatOnce(t, f.harness, key)
		if resp.StatusCode != 503 || resp.Header.Get(headerShouldRetry) != "false" {
			t.Fatalf("status %d, x-should-retry %q, want 503 capacity after the FIFO, and false: %s", resp.StatusCode, resp.Header.Get(headerShouldRetry), body)
		}
		checkErrorBody(t, body, codeCapacity, "")
		wantCounts(t, f, 1, 1) // the second Instance's one is the busy request, not the retry
		_ = busy
	})
}

// With one Instance there is nothing to retry to: alpha's answer, unchanged, and no header.
func TestWithOneInstanceNothingIsRetriedAndNoHeaderIsSet(t *testing.T) {
	f, key, _ := retryFix(t, 1, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	resp, body := scChatOnce(t, f.harness, key)
	checkErrorBody(t, body, codeUpstreamClosed, "tunnel")
	if resp.StatusCode != 502 || resp.Header.Get(headerShouldRetry) != "" {
		t.Fatalf("status %d, x-should-retry %q, want 502 and none", resp.StatusCode, resp.Header.Get(headerShouldRetry))
	}
	wantCounts(t, f, 1)
}

// Alpha's resend and the retry are separate: a dial refused on the first Instance goes to the second,
// whose close after the write is retried to the third, skipping both.
func TestAResendAndARetryInOneRequest(t *testing.T) {
	f, key, walk := retryFix(t, 3, scOpts{})
	idx := map[string]int{}
	for i, n := range f.names {
		idx[n] = i
	}
	f.engines[idx[walk[0]]].Close()
	f.engines[idx[walk[1]]].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	resp, body := scChatOnce(t, f.harness, key)
	wantWhole(t, resp, body, walk[2])
	got := f.counts()
	if got[idx[walk[0]]] != 0 || got[idx[walk[1]]] != 1 || got[idx[walk[2]]] != 1 {
		t.Errorf("requests %v, want none on %s (refused), one on %s and one on %s", got, walk[0], walk[1], walk[2])
	}
}

// timerStep runs one request whose first attempt, on the first Instance, closes before its headers after
// first on the front's clock, and whose retry, on the second, is held: it returns the call, the second
// engine's hold, and the timers pending once the retry is written (the base before the request, plus
// the deadline, idle and lease timers, plus a streamed request's headers timer).
func timerStep(t *testing.T, f *scFix, key string, stream bool, first time.Duration) (*scCall, chan struct{}) {
	t.Helper()
	closeA, holdB := make(chan struct{}), make(chan struct{})
	f.engines[0].Set(enginetest.Behaviour{CloseAfterHold: closeA})
	f.engines[1].Set(enginetest.Behaviour{Hold: holdB})
	want := f.clk.Pending() + 3
	if stream {
		want++
	}
	call := scGo(f.harness, errChatBody(scModel, stream, "hi"), "x-session-id", key)
	waitState(t, "the first attempt's timers", func() bool { return f.engines[0].Open() == 1 && f.clk.Pending() == want })
	f.clk.Advance(first)
	if doneNow(call) {
		t.Fatalf("the request ended %s into its first attempt", first)
	}
	close(closeA)
	waitState(t, "the retry's timers", func() bool { return f.engines[1].Open() == 1 && f.clk.Pending() == want })
	return call, holdB
}

// streamHeadersTimeout and idleTimeout start again at the retry's write: a first attempt that used 0.8 of
// the time-out and a retry that uses 0.8 more both stand (1.6 in all). And the retry has its own: held
// past the whole time-out from its write, it is cut.
func TestTheHeadersAndIdleTimeOutsStartAgainAtTheRetry(t *testing.T) {
	for _, c := range []struct {
		name   string
		stream bool
		cfg    func(*Config)
		d      time.Duration
		detail string
	}{
		{"streamHeadersTimeout", true, func(c *Config) { c.StreamHeadersTimeout = Duration(DefaultStreamHeaders) }, DefaultStreamHeaders, "headers"},
		{"idleTimeout", false, func(c *Config) { c.IdleTimeout = Duration(time.Minute) }, time.Minute, "idle"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, key, walk := retryFix(t, 2, scOpts{cfg: c.cfg})
			call, holdB := timerStep(t, f, key, c.stream, c.d*8/10)
			f.clk.Advance(c.d * 8 / 10)
			if doneNow(call) {
				t.Fatalf("cut 1.6 x %s after arrival, 0.8 x it after the retry's write: the time-out did not start again", c.name)
			}
			close(holdB)
			resp, body := call.wait(t)
			wantWhole(t, resp, body, walk[1])

			f2, key2, _ := retryFix(t, 2, scOpts{cfg: c.cfg})
			call2, holdB2 := timerStep(t, f2, key2, c.stream, c.d*8/10)
			defer close(holdB2)
			f2.clk.Advance(c.d)
			resp2, body2 := call2.wait(t)
			if resp2.StatusCode != 504 {
				t.Fatalf("held past %s after the retry's write: status %d, want 504: %s", c.name, resp2.StatusCode, body2)
			}
			checkErrorBody(t, body2, codeRequestTimeout, c.detail)
		})
	}
}

// maxRequestDuration counts from the request's arrival, not from the retry: a first attempt that used
// 0.6 of it leaves the retry 0.4.
func TestMaxRequestDurationCountsFromArrivalAcrossTheRetry(t *testing.T) {
	const m = time.Hour
	// idleTimeout above it, so only the deadline can cut the request.
	f, key, _ := retryFix(t, 2, scOpts{cfg: func(c *Config) { c.MaxRequestDuration, c.IdleTimeout = Duration(m), Duration(2*m) }})
	call, holdB := timerStep(t, f, key, false, m*6/10)
	defer close(holdB)
	f.clk.Advance(m * 4 / 10)
	resp, body := call.wait(t)
	if resp.StatusCode != 504 {
		t.Fatalf("status %d at maxRequestDuration after arrival, want 504: %s", resp.StatusCode, body)
	}
	checkErrorBody(t, body, codeRequestTimeout, "deadline")
}

// The first attempt's idle timer stops while the retry waits in the FIFO: the other Instance at its
// cap, the front's clock moved past idleTimeout during the wait, and the retry is then served whole.
func TestARetryWaitingInTheFIFOIsNotCutByTheFirstAttemptsIdleTimer(t *testing.T) {
	const idle = time.Minute
	f, key, walk := retryFix(t, 2, scOpts{
		cfg:  func(c *Config) { c.IdleTimeout = Duration(idle); c.QueueTimeout = Duration(30 * time.Second) },
		inst: func(i int, o *instanceOpts) { o.maxNumSeqs = 1 },
	})
	hold := make(chan struct{})
	f.engines[1].Set(enginetest.Behaviour{Hold: hold})
	busy := scGo(f.harness, errChatBody(scModel, false, "busy"), "x-session-id", scKeys(t, f.names, walk[1], 1)[0])
	waitState(t, "the other Instance at its cap", func() bool { return f.engines[1].Open() == 1 })
	f.engines[0].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	call := scGo(f.harness, errChatBody(scModel, false, "hi"), "x-session-id", key)
	waitState(t, "the retry waiting in the FIFO", func() bool { return len(f.engines[0].Requests()) == 1 && f.router.Status().Waiting == 1 })
	f.clk.Advance(2 * idle)
	if doneNow(call) {
		t.Fatal("the retry was cut while it waited in the FIFO: the first attempt's idle timer ran on")
	}
	f.engines[1].Set(enginetest.Behaviour{})
	close(hold)
	busy.wait(t)
	resp, body := call.wait(t)
	wantWhole(t, resp, body, walk[1])
}

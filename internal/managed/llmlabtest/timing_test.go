package llmlabtest

import (
	"net/http"
	"testing"
	"time"
)

// The no-byte cut (L-6.1). Each case is the cut on against the cut off.
func TestNoByteCut(t *testing.T) {
	const cut = 200 * time.Millisecond

	for _, route := range []struct{ name, body string }{
		{"whole", chatReq(modelA)},
		{"stream", streamReq(modelA)},
	} {
		t.Run("504 at the cut/"+route.name, func(t *testing.T) {
			s := startS(t)
			s.SetNoByteCut(cut)
			s.SetLatency(Latency{FirstByte: 600 * time.Millisecond})
			a := ask(t, s, http.MethodPost, routeChat, keyOne, route.body)
			requireStatus(t, a, http.StatusGatewayTimeout, "first byte 600ms, cut 200ms")
			if a.Took < 150*time.Millisecond || a.Took >= 500*time.Millisecond {
				t.Errorf("504 after %v, want it near the 200ms cut, well before the 600ms first byte", a.Took)
			}
			if r := waitLogged(t, s, 1)[0]; r.Status != http.StatusGatewayTimeout {
				t.Errorf("logged status %d, want 504", r.Status)
			}
		})
	}

	t.Run("a first byte under the cut is answered", func(t *testing.T) {
		s := startS(t)
		s.SetNoByteCut(cut)
		s.SetLatency(Latency{FirstByte: 50 * time.Millisecond})
		a := postChat(t, s, modelA)
		requireStatus(t, a, http.StatusOK, "first byte 50ms, cut 200ms")
		if a.Took < 40*time.Millisecond {
			t.Errorf("answered after %v: the 50ms first byte was not applied", a.Took)
		}
	})

	t.Run("off by default", func(t *testing.T) {
		s := startS(t)
		s.SetLatency(Latency{FirstByte: 600 * time.Millisecond})
		a := postChat(t, s, modelA)
		requireStatus(t, a, http.StatusOK, "first byte 600ms, no cut")
		if a.Took < 550*time.Millisecond {
			t.Errorf("answered after %v: the 600ms first byte was not applied", a.Took)
		}
	})

	t.Run("off again", func(t *testing.T) {
		s := startS(t)
		s.SetLatency(Latency{FirstByte: 600 * time.Millisecond})
		s.SetNoByteCut(cut)
		s.SetNoByteCut(0)
		requireStatus(t, postChat(t, s, modelA), http.StatusOK, "cut switched on and off")
	})

	t.Run("a stream gap above the cut ends the stream", func(t *testing.T) {
		s := startS(t)
		s.SetNoByteCut(cut)
		s.SetLatency(Latency{PerChunk: 600 * time.Millisecond})
		a := ask(t, s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
		requireStatus(t, a, http.StatusOK, "stream, gap 600ms, cut 200ms") // the status line had gone out
		if a.Took >= 500*time.Millisecond {
			t.Errorf("stream ended after %v, want it cut near 200ms", a.Took)
		}
		st := parseSSE(t, a.Body)
		if st.Done || len(st.contents()) != 0 || len(st.finishes()) != 0 {
			t.Errorf("cut stream: done %v contents %v finishes %v, want none of them", st.Done, st.contents(), st.finishes())
		}
		r := waitLogged(t, s, 1)[0]
		if !r.Truncated || r.Status != http.StatusOK {
			t.Errorf("logged Truncated %v Status %d, want true and 200", r.Truncated, r.Status)
		}
	})

	t.Run("the cut counts each gap, not the whole stream", func(t *testing.T) {
		s := startS(t)
		s.SetReply("a b c d e f g h")
		s.SetNoByteCut(300 * time.Millisecond)
		s.SetLatency(Latency{PerChunk: 50 * time.Millisecond}) // nine gaps, 450ms in all
		a := ask(t, s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
		requireStatus(t, a, http.StatusOK, "slow stream, small gaps")
		st := parseSSE(t, a.Body)
		if !st.Done || len(st.contents()) != 8 || a.Took < 400*time.Millisecond {
			t.Errorf("stream: done %v, %d pieces, %v long; want [DONE], 8 pieces and over 400ms", st.Done, len(st.contents()), a.Took)
		}
		if r := waitLogged(t, s, 1)[0]; r.Truncated {
			t.Error("logged Truncated for a stream that finished")
		}
	})

	t.Run("a stream without the cut finishes", func(t *testing.T) {
		s := startS(t)
		s.SetReply("a b")
		s.SetLatency(Latency{PerChunk: 250 * time.Millisecond}) // above the cut used elsewhere
		a := ask(t, s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
		st := parseSSE(t, a.Body)
		if a.Status != http.StatusOK || !st.Done || len(st.contents()) != 2 {
			t.Errorf("stream: status %d done %v pieces %v, want 200, [DONE] and 2 pieces", a.Status, st.Done, st.contents())
		}
		if r := waitLogged(t, s, 1)[0]; r.Truncated {
			t.Error("logged Truncated with no cut")
		}
	})
}

// The cut reads the Server's clock, so a manual clock decides it with no
// sleeping beyond a check that the request is still waiting.
func TestNoByteCutOnTheManualClock(t *testing.T) {
	setup := func(t *testing.T, firstByte time.Duration) (*manualClock, *Server, <-chan answer) {
		clock := newManualClock()
		s := startWith(t, clock.Now)
		s.SetNoByteCut(180 * time.Second)
		s.SetLatency(Latency{FirstByte: firstByte})
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))
		requirePending(t, ch, held, "chat before the clock moves")
		return clock, s, ch
	}

	t.Run("cut", func(t *testing.T) {
		clock, s, ch := setup(t, 10*time.Minute)
		clock.Advance(179 * time.Second)
		requirePending(t, ch, 100*time.Millisecond, "chat 1s before the cut")
		clock.Advance(2 * time.Second)
		requireStatus(t, requireAnswer(t, ch, 5*time.Second, "chat past the cut"), http.StatusGatewayTimeout, "past the 180s cut")
		if r := waitLogged(t, s, 1)[0]; r.Status != http.StatusGatewayTimeout {
			t.Errorf("logged status %d, want 504", r.Status)
		}
	})

	t.Run("first byte under the cut", func(t *testing.T) {
		clock, _, ch := setup(t, 170*time.Second)
		clock.Advance(171 * time.Second)
		requireStatus(t, requireAnswer(t, ch, 5*time.Second, "chat past its first byte"), http.StatusOK, "first byte at 170s, cut 180s")
	})
}

var stallRoutes = []struct{ name, method, path, body string }{
	{"listing", http.MethodGet, routeModels, ""},
	{"entry", http.MethodGet, routeModels + "/" + modelA, ""},
	{"chat", http.MethodPost, routeChat, chatReq(modelA)},
	{"completions", http.MethodPost, routeCompletions, `{"model":"example-9b","prompt":"one two"}`},
	{"responses", http.MethodPost, routeResponses, `{"model":"example-9b","input":"one two"}`},
	{"embeddings", http.MethodPost, routeEmbeddings, `{"model":"example-9b","input":"one two"}`},
}

// A manual stall (L-6.2) holds a request until it ends, on every route.
func TestManualStall(t *testing.T) {
	for _, rt := range stallRoutes {
		t.Run(rt.name, func(t *testing.T) {
			s := startS(t)
			requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "no stall")

			s.Stall(true)
			ch := askAsync(s, rt.method, rt.path, keyOne, rt.body)
			requirePending(t, ch, held, rt.name+" in a stall")
			releasedAt := time.Now()
			s.Stall(false)
			requireStatus(t, requireAnswer(t, ch, 5*time.Second, rt.name+" after the stall"), http.StatusOK, "after the stall")

			if r := waitLogged(t, s, 2)[1]; r.FirstByte.Before(releasedAt) {
				t.Errorf("first byte at %v, before the stall ended at %v", r.FirstByte, releasedAt)
			}
		})
	}
}

// A stall that outlasts the no-byte cut ends in 504 (L-6.1 with L-6.2).
func TestStallAgainstTheCut(t *testing.T) {
	for _, rt := range []struct{ name, method, path, body string }{stallRoutes[0], stallRoutes[2]} {
		t.Run("504 while stalled/"+rt.name, func(t *testing.T) {
			s := startS(t)
			s.Stall(true)
			s.SetNoByteCut(200 * time.Millisecond)
			a := ask(t, s, rt.method, rt.path, keyOne, rt.body)
			requireStatus(t, a, http.StatusGatewayTimeout, "stalled past the cut")
			if a.Took < 150*time.Millisecond || a.Took >= 500*time.Millisecond {
				t.Errorf("504 after %v, want it near the 200ms cut", a.Took)
			}
			if r := waitLogged(t, s, 1)[0]; r.Status != http.StatusGatewayTimeout {
				t.Errorf("logged status %d, want 504", r.Status)
			}
		})
	}

	t.Run("a stall that ends before the cut", func(t *testing.T) {
		s := startS(t)
		s.Stall(true)
		s.SetNoByteCut(2 * time.Second)
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))
		requirePending(t, ch, held, "chat in a stall")
		s.Stall(false)
		requireStatus(t, requireAnswer(t, ch, 5*time.Second, "chat after the stall"), http.StatusOK, "stall ended before the cut")
	})
}

// The periodic window, by Stalled(t) and a manual clock. Each expectation is
// a literal from the window's definition: [Start, Start+Duration) of every
// Period.
func TestPeriodicStallWindows(t *testing.T) {
	const period, window = 30 * time.Minute, 5 * time.Minute
	clock := newManualClock()
	t0 := clock.Now()

	t.Run("off by default", func(t *testing.T) {
		s := startWith(t, clock.Now)
		for _, off := range []time.Duration{0, window / 2, period, 10 * period} {
			if s.Stalled(t0.Add(off)) {
				t.Errorf("a fresh Server is stalled at Start+%v", off)
			}
		}
	})

	t.Run("windows", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.SetStalls(Stalls{Period: period, Duration: window, Start: t0})
		for _, c := range []struct {
			off  time.Duration
			want bool
		}{
			{-time.Hour, false},
			{-time.Nanosecond, false}, // before Start
			{0, true},                 // Start
			{time.Nanosecond, true},
			{window - time.Nanosecond, true}, // last instant inside
			{window, false},                  // the window is half-open
			{15 * time.Minute, false},
			{period - time.Nanosecond, false},
			{period, true}, // next period
			{period + window - time.Nanosecond, true},
			{period + window, false},
			{3 * period, true},
			{3*period + window, false},
		} {
			if got := s.Stalled(t0.Add(c.off)); got != c.want {
				t.Errorf("Stalled(Start+%v) = %v, want %v", c.off, got, c.want)
			}
		}
	})

	t.Run("start in the future", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.SetStalls(Stalls{Period: period, Duration: window, Start: t0.Add(time.Hour)})
		if s.Stalled(t0) || s.Stalled(t0.Add(time.Hour-time.Nanosecond)) || !s.Stalled(t0.Add(time.Hour)) {
			t.Error("windows must open at the given Start, not before")
		}
	})

	t.Run("zero start means when SetStalls is called", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.SetStalls(Stalls{Period: period, Duration: window})
		if !s.Stalled(t0) || s.Stalled(t0.Add(-time.Nanosecond)) || s.Stalled(t0.Add(window)) || !s.Stalled(t0.Add(period)) {
			t.Error("with a zero Start the first window must open at the clock's time of the call")
		}
	})

	t.Run("switched off again", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.SetStalls(Stalls{Period: period, Duration: window, Start: t0})
		if !s.Stalled(t0) {
			t.Fatal("not stalled at Start")
		}
		s.SetStalls(Stalls{})
		if s.Stalled(t0) {
			t.Error("still stalled after SetStalls with a zero Period")
		}
	})

	t.Run("a zero Duration never stalls", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.SetStalls(Stalls{Period: period, Start: t0})
		if s.Stalled(t0) || s.Stalled(t0.Add(period)) {
			t.Error("stalled with a zero Duration")
		}
	})

	t.Run("a manual stall covers every instant", func(t *testing.T) {
		s := startWith(t, clock.Now)
		s.Stall(true)
		if !s.Stalled(t0) || !s.Stalled(t0.Add(-100*time.Hour)) {
			t.Error("not stalled during a manual stall")
		}
		s.Stall(false)
		if s.Stalled(t0) {
			t.Error("still stalled after the manual stall ended")
		}
	})
}

// A request that arrives in a periodic window waits until the clock leaves
// it; real time passing releases nothing.
func TestPeriodicStallReleasedByTheClock(t *testing.T) {
	const period, window = 30 * time.Minute, 5 * time.Minute
	for _, rt := range []struct{ name, method, path, body string }{stallRoutes[0], stallRoutes[2]} {
		t.Run(rt.name, func(t *testing.T) {
			clock := newManualClock()
			t0 := clock.Now()
			s := startWith(t, clock.Now)
			s.SetStalls(Stalls{Period: period, Duration: window, Start: t0})

			ch := askAsync(s, rt.method, rt.path, keyOne, rt.body)
			requirePending(t, ch, held, rt.name+" in the window, clock unmoved")
			clock.Advance(4 * time.Minute) // still inside
			requirePending(t, ch, 100*time.Millisecond, rt.name+" 1 min before the window ends")
			clock.Advance(time.Minute) // Start+5min: outside
			requireStatus(t, requireAnswer(t, ch, 5*time.Second, rt.name+" after the window"), http.StatusOK, "after the window")
			if r := waitLogged(t, s, 1)[0]; !r.Time.Equal(t0) {
				t.Errorf("logged arrival %v, want %v: the request arrived after the window, so it never waited", r.Time, t0)
			}

			// Outside a window nothing waits.
			requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "outside the window")

			// The next period stalls again.
			clock.Set(t0.Add(period))
			ch = askAsync(s, rt.method, rt.path, keyOne, rt.body)
			requirePending(t, ch, held, rt.name+" in the next window")
			clock.Set(t0.Add(period + window))
			requireStatus(t, requireAnswer(t, ch, 5*time.Second, rt.name+" after the next window"), http.StatusOK, "after the next window")
		})
	}
}

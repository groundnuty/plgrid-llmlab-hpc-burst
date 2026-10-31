package llmlabtest

import (
	"net/http"
	"testing"
	"time"
)

// Requests and Count see a request only when its answer ends, so a request
// held by a stall is invisible to them. Arrived counts it at arrival (Opus B3).
func TestArrivedSeesAHeldRequest(t *testing.T) {
	s := startS(t)
	s.Stall(true)
	ch := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))

	pollUntil(t, "Arrived(POST, chat) == 1", func() bool { return s.Arrived(http.MethodPost, routeChat) == 1 })
	// The request is held: the log does not have it, though it has arrived.
	if n := s.Count(http.MethodPost, routeChat); n != 0 {
		t.Errorf("Count = %d while the request is held, want 0: the log sees a request when its answer ends", n)
	}
	if n := len(s.Requests()); n != 0 {
		t.Errorf("%d requests logged while the only one is held, want 0", n)
	}
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, routeChat, 1},
		{"", "", 1}, // an empty method and path match any
		{http.MethodPost, "", 1},
		{"", routeChat, 1},
		{http.MethodGet, "", 0},
		{"", routeModels, 0},
		{http.MethodPost, routeModels, 0},
		{http.MethodGet, routeChat, 0},
		{http.MethodPost, routeCompletions, 0},
	} {
		if got := s.Arrived(c.method, c.path); got != c.want {
			t.Errorf("Arrived(%q, %q) = %d, want %d", c.method, c.path, got, c.want)
		}
	}

	s.Stall(false)
	requireStatus(t, requireAnswer(t, ch, 5*time.Second, "chat after the stall"), http.StatusOK, "after the stall")
	waitLogged(t, s, 1)
	if a, c, l := s.Arrived(http.MethodPost, routeChat), s.Count(http.MethodPost, routeChat), len(s.Requests()); a != 1 || c != 1 || l != 1 {
		t.Errorf("after the answer Arrived %d, Count %d, Requests %d, want 1, 1 and 1: one request is counted once", a, c, l)
	}
	if got := s.Arrived("", ""); got != 1 {
		t.Errorf("Arrived(\"\", \"\") = %d after the answer, want 1", got)
	}
}

// Every request counts, whatever it gets: another route, a wrong method, a bad
// body, a refused key.
func TestArrivedCountsEveryRequest(t *testing.T) {
	s := startS(t)
	ask(t, s, http.MethodGet, routeModels, keyOne, "")
	ask(t, s, http.MethodGet, routeModels, "", "")                 // no key: 401
	ask(t, s, http.MethodGet, "/v1/unknown", keyOne, "")           // 404
	ask(t, s, http.MethodGet, routeChat, keyOne, "")               // 405
	ask(t, s, http.MethodPost, routeChat, keyOne, "not json")      // 400
	ask(t, s, http.MethodPost, routeChat, keyOne, chatReq(modelA)) // 200
	waitLogged(t, s, 6)

	for _, c := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, routeModels, 2},
		{http.MethodGet, "/v1/unknown", 1},
		{http.MethodGet, routeChat, 1},
		{http.MethodPost, routeChat, 2},
		{http.MethodGet, "", 4},
		{http.MethodPost, "", 2},
		{"", routeChat, 3},
		{"", "", 6},
	} {
		if got := s.Arrived(c.method, c.path); got != c.want {
			t.Errorf("Arrived(%q, %q) = %d, want %d", c.method, c.path, got, c.want)
		}
	}
	if got, logged := s.Arrived("", ""), len(s.Requests()); got != logged {
		t.Errorf("Arrived = %d, Requests = %d: once every answer has ended they agree", got, logged)
	}
}

// Waiting is the barrier a test needs before it ends a stall: it counts a
// request only once the request has met the wait.
func TestWaiting(t *testing.T) {
	t.Run("a manual stall", func(t *testing.T) {
		s := startS(t)
		if n := s.Waiting(); n != 0 {
			t.Fatalf("Waiting = %d on a fresh Server, want 0", n)
		}
		requireStatus(t, ask(t, s, http.MethodGet, routeModels, keyOne, ""), http.StatusOK, "no stall")
		if n := s.Waiting(); n != 0 {
			t.Errorf("Waiting = %d after an answered request, want 0: a request that was never held is not counted", n)
		}

		s.Stall(true)
		first := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))
		requireWaiting(t, s, 1, "one request in the stall")
		second := askAsync(s, http.MethodGet, routeModels, keyOne, "")
		requireWaiting(t, s, 2, "two requests in the stall")

		s.Stall(false)
		requireStatus(t, requireAnswer(t, first, 5*time.Second, "chat after the stall"), http.StatusOK, "chat after the stall")
		requireStatus(t, requireAnswer(t, second, 5*time.Second, "listing after the stall"), http.StatusOK, "listing after the stall")
		// An answer is written after the request left its wait, so no polling.
		if n := s.Waiting(); n != 0 {
			t.Errorf("Waiting = %d after both answers, want 0", n)
		}
	})

	t.Run("the first-byte latency", func(t *testing.T) {
		clock := newManualClock()
		s := startWith(t, clock.Now)
		s.SetLatency(Latency{FirstByte: 10 * time.Minute})
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))
		requireHeld(t, s, ch, "chat in its first-byte latency")
		clock.Advance(10 * time.Minute)
		requireStatus(t, requireAnswer(t, ch, 5*time.Second, "chat after the latency"), http.StatusOK, "after the latency")
		if n := s.Waiting(); n != 0 {
			t.Errorf("Waiting = %d after the answer, want 0", n)
		}
	})

	t.Run("a stream's pacing", func(t *testing.T) {
		clock := newManualClock()
		s := startWith(t, clock.Now)
		s.SetReply("a b")
		s.SetLatency(Latency{PerChunk: time.Minute})
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
		requireHeld(t, s, ch, "stream between chunks")

		deadline := time.After(5 * time.Second)
		var a answer
		for got := false; !got; {
			clock.Advance(time.Minute)
			select {
			case a = <-ch:
				got = true
			case <-deadline:
				t.Fatal("the stream did not end within 5s of moving the clock")
			case <-time.After(10 * time.Millisecond):
			}
		}
		if a.Err != nil || !parseSSE(t, a.Body).Done {
			t.Fatalf("stream: err %v, body %s", a.Err, a.Body)
		}
		if n := s.Waiting(); n != 0 {
			t.Errorf("Waiting = %d after the stream ended, want 0", n)
		}
	})

	t.Run("a Server that closes", func(t *testing.T) {
		s := startS(t)
		s.Stall(true)
		ch := askAsync(s, http.MethodGet, routeModels, keyOne, "")
		requireHeld(t, s, ch, "listing in a stall")
		s.Close()
		<-ch
		pollUntil(t, "Waiting() == 0 after Close", func() bool { return s.Waiting() == 0 })
	})
}

package llmlabtest

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// keyRoutes are the two routes a key guards in these tests: the listing the
// controller reads and a chat request.
var keyRoutes = []struct{ name, method, path, body string }{
	{"listing", http.MethodGet, routeModels, ""},
	{"chat", http.MethodPost, routeChat, chatReq(modelA)},
}

// A missing, a wrong and a removed key get 401 (L-6.4).
func TestKeyRefusal(t *testing.T) {
	for _, rt := range keyRoutes {
		t.Run(rt.name, func(t *testing.T) {
			s := startS(t)
			requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "the key from Options.Keys")

			for name, auth := range map[string]string{
				"no header":    "",
				"wrong key":    "Bearer example-key-wrong",
				"empty bearer": "Bearer ",
				"other scheme": "Basic " + keyOne,
				"bare key":     keyOne,
			} {
				requireStatus(t, askAuth(t, s, rt.method, rt.path, auth, rt.body), http.StatusUnauthorized, name)
			}

			s.AddKey(keyTwo)
			requireStatus(t, ask(t, s, rt.method, rt.path, keyTwo, rt.body), http.StatusOK, "an added key")
			s.RemoveKey(keyTwo)
			requireStatus(t, ask(t, s, rt.method, rt.path, keyTwo, rt.body), http.StatusUnauthorized, "a removed key")
			s.AddKey(keyTwo)
			requireStatus(t, ask(t, s, rt.method, rt.path, keyTwo, rt.body), http.StatusOK, "a key added back")

			// The log: the good key, five refusals, then keyTwo's three.
			for i, r := range waitLogged(t, s, 9) {
				want := 401
				if i == 0 || i == 6 || i == 8 {
					want = 200
				}
				if r.Status != want {
					t.Errorf("request %d logged status %d, want %d", i, r.Status, want)
				}
			}
		})
	}
}

// ExpireKeyAt: the key works until t on the Server's clock, then fails with
// 401, on the listing as well (L-6.4).
func TestExpireKeyAt(t *testing.T) {
	for _, rt := range keyRoutes {
		t.Run(rt.name, func(t *testing.T) {
			clock := newManualClock()
			t0 := clock.Now()
			s := startWith(t, clock.Now)
			s.AddKey(keyTwo)
			exp := t0.Add(time.Hour)

			// No expiry by default, however far the clock goes.
			clock.Set(t0.AddDate(20, 0, 0))
			requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "a key with no expiry, 20 years on")
			clock.Set(t0)

			s.ExpireKeyAt(keyOne, exp)
			for _, c := range []struct {
				name string
				at   time.Time
				want int
			}{
				{"now", t0, 200},
				{"1ns before the expiry", exp.Add(-time.Nanosecond), 200},
				{"at the expiry", exp, 401},
				{"1ns after", exp.Add(time.Nanosecond), 401},
				{"a day after", exp.Add(24 * time.Hour), 401},
			} {
				clock.Set(c.at)
				requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), c.want, c.name)
			}
			requireStatus(t, ask(t, s, rt.method, rt.path, keyTwo, rt.body), http.StatusOK, "another key, same instant")

			s.ExpireKeyAt(keyOne, time.Time{}) // cleared
			requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "expiry cleared")
		})
	}
}

// RejectKey answers every request with one status, 401 or 403; 0 undoes it.
func TestRejectKey(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, rt := range keyRoutes {
			t.Run(http.StatusText(code)+"/"+rt.name, func(t *testing.T) {
				s := startS(t)
				s.AddKey(keyTwo)
				requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "before RejectKey")

				s.RejectKey(keyOne, code)
				requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), code, "rejected key")
				requireStatus(t, ask(t, s, rt.method, rt.path, keyTwo, rt.body), http.StatusOK, "another key")

				s.RejectKey(keyOne, 0)
				requireStatus(t, ask(t, s, rt.method, rt.path, keyOne, rt.body), http.StatusOK, "RejectKey(key, 0)")
			})
		}
	}

	t.Run("other statuses panic", func(t *testing.T) {
		s := startS(t)
		for _, code := range []int{http.StatusInternalServerError, http.StatusNotFound, http.StatusOK, -1} {
			mustPanic(t, "RejectKey("+http.StatusText(code)+")", func() { s.RejectKey(keyOne, code) })
		}
		requireStatus(t, postChat(t, s, modelA), http.StatusOK, "the key after the refused calls")
	})
}

// ExpireKeyAt and RejectKey act on a key the Server has. On one it never had,
// or one a test removed, they must not make it valid again.
func TestKeyChangesNeedAKnownKey(t *testing.T) {
	clock := newManualClock()
	s := startWith(t, clock.Now)
	future := clock.Now().Add(time.Hour)

	const never = "example-key-9999"
	mustPanic(t, "ExpireKeyAt on a key never added", func() { s.ExpireKeyAt(never, future) })
	mustPanic(t, "RejectKey(0) on a key never added", func() { s.RejectKey(never, 0) })
	mustPanic(t, "RejectKey(401) on a key never added", func() { s.RejectKey(never, http.StatusUnauthorized) })
	requireStatus(t, ask(t, s, http.MethodGet, routeModels, never, ""), http.StatusUnauthorized, "a key never added")

	s.AddKey(keyTwo)
	s.RemoveKey(keyTwo)
	mustPanic(t, "ExpireKeyAt on a removed key", func() { s.ExpireKeyAt(keyTwo, future) })
	mustPanic(t, "RejectKey(0) on a removed key", func() { s.RejectKey(keyTwo, 0) })
	requireStatus(t, ask(t, s, http.MethodGet, routeModels, keyTwo, ""), http.StatusUnauthorized, "a removed key")
}

// EchoKey puts the request's Authorization header into every error body
// (L-6.4's test aid): "Bearer <key>", exactly as sent.
func TestEchoKey(t *testing.T) {
	const wrong = "example-key-wrong"
	cases := []struct {
		name    string
		status  int
		arrange func(*Server)
		do      func(t *testing.T, s *Server) answer
		key     string // the key the request sends
	}{
		{"401 wrong key", 401, nil, func(t *testing.T, s *Server) answer {
			return ask(t, s, http.MethodGet, routeModels, wrong, "")
		}, wrong},
		{"401 chat", 401, nil, func(t *testing.T, s *Server) answer {
			return ask(t, s, http.MethodPost, routeChat, wrong, chatReq(modelA))
		}, wrong},
		{"403 rejected", 403, func(s *Server) { s.RejectKey(keyOne, 403) }, func(t *testing.T, s *Server) answer {
			return postChat(t, s, modelA)
		}, keyOne},
		{"503 inactive", 503, func(s *Server) { s.SetActive(modelA, false) }, func(t *testing.T, s *Server) answer {
			return postChat(t, s, modelA)
		}, keyOne},
		{"400 unknown model", 400, nil, func(t *testing.T, s *Server) answer {
			return postChat(t, s, "example-unknown")
		}, keyOne},
		{"404 unknown route", 404, nil, func(t *testing.T, s *Server) answer {
			return ask(t, s, http.MethodGet, "/v1/unknown", keyOne, "")
		}, keyOne},
		{"422 rewriting", 422, func(s *Server) { s.SetRewriting(&Rewriting{}) }, func(t *testing.T, s *Server) answer {
			return ask(t, s, http.MethodPost, routeChat, keyOne, `{"model":"example-9b","cache_salt":"s","messages":[]}`)
		}, keyOne},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := startS(t)
			if c.arrange != nil {
				c.arrange(s)
			}
			sent := "Bearer " + c.key

			off := c.do(t, s)
			requireStatus(t, off, c.status, "echo off")
			if strings.Contains(string(off.Body), c.key) || strings.Contains(string(off.Body), "Bearer") {
				t.Errorf("EchoKey off, but the body carries the header: %s", off.Body)
			}

			s.EchoKey(true)
			on := c.do(t, s)
			requireStatus(t, on, c.status, "echo on")
			if !strings.Contains(string(on.Body), sent) {
				t.Errorf("EchoKey on, but the body lacks %q: %s", sent, on.Body)
			}

			s.EchoKey(false)
			again := c.do(t, s)
			if strings.Contains(string(again.Body), c.key) {
				t.Errorf("EchoKey off again, but the body carries the key: %s", again.Body)
			}
		})
	}
}

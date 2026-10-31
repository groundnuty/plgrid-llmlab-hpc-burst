package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
)

// recordingOrigin is an "external" origin that serves the fixtures' content
// and records the Authorization header of every request.
func recordingOrigin(t *testing.T) *treeServer {
	t.Helper()
	content := map[string]string{}
	for _, f := range fixtures {
		content["/"+f.path] = f.content
	}
	return newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(content[r.URL.Path]))
	})
}

// TestCallersCheckRedirectCannotRestoreTheTokenOnAnotherOrigin: a callback
// that rebuilds the headers from the first request (a common way to keep
// custom headers across a redirect) restores the Authorization header that
// net/http stripped. The package's own check runs after the callback.
func TestCallersCheckRedirectCannotRestoreTheTokenOnAnotherOrigin(t *testing.T) {
	external := recordingOrigin(t)
	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.redirectTo = external.URL
	var (
		mu    sync.Mutex
		calls int
	)
	hc := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		mu.Lock()
		calls++
		mu.Unlock()
		req.Header = via[0].Header.Clone() // brings the Authorization header back
		return nil
	}}
	path := writeToken(t, testToken, 0o600)
	if _, err := h.client(t, Config{TokenFile: path, HTTPClient: hc}).Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatalf("Files: %v", err)
	}
	mu.Lock()
	ran := calls
	mu.Unlock()
	if ran != len(fixtures) {
		t.Fatalf("the caller's CheckRedirect ran %d times, want once per redirected download (%d)", ran, len(fixtures))
	}
	got := external.authHeaders()
	if len(got) != len(fixtures) {
		t.Fatalf("the external origin served %d downloads, want %d", len(got), len(fixtures))
	}
	for i, a := range got {
		if a != "" {
			t.Errorf("request %d to the external origin carried Authorization %q", i, a)
		}
	}
	// Prove the callback does restore the header, so the check above can fail:
	// the same callback on a plain client sends the token to the other origin.
	plain := &http.Client{CheckRedirect: hc.CheckRedirect}
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/"+testRepo+"/resolve/"+testRev+"/config.json", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := plain.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if last := external.authHeaders(); last[len(last)-1] == "" {
		t.Fatal("the callback no longer restores Authorization; the test above proves nothing")
	}
}

func TestCallersCheckRedirectIsHonoured(t *testing.T) {
	external := recordingOrigin(t)
	h := newFakeHub(t, allFiles())
	h.redirectTo = external.URL

	refuse := errors.New("redirects are not allowed here")
	_, err := h.client(t, Config{HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return refuse }}}).
		Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, refuse) {
		t.Errorf("err = %v, want the callback's error", err)
	}
	if external.requests() != 0 {
		t.Errorf("a refused redirect was followed (%d requests)", external.requests())
	}

	// ErrUseLastResponse hands back the 302 itself, which is not a download.
	_, err = h.client(t, Config{HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}).
		Files(context.Background(), testRepo, testRev)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusFound {
		t.Errorf("err = %v, want an HTTPError with status 302", err)
	}
	if external.requests() != 0 {
		t.Errorf("ErrUseLastResponse still followed the redirect")
	}
}

func TestOriginKeyNormalisesDefaultPorts(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		same bool
	}{
		{"https://hub.example.org", "https://hub.example.org:443", true},
		{"https://Hub.Example.org:443", "https://hub.example.org", true},
		{"http://127.0.0.1:80", "http://127.0.0.1", true},
		{"https://hub.example.org:8443", "https://hub.example.org", false},
		{"https://hub.example.org:80", "https://hub.example.org", false}, // 80 is not https's default
		{"http://hub.example.org:443", "http://hub.example.org", false},  // 443 is not http's default
		{"http://hub.example.org", "https://hub.example.org", false},
		{"https://[::1]:443", "https://[::1]", true},
		{"https://[::1]:8443", "https://[::1]", false},
		{"https://hub.example.org.", "https://hub.example.org", false},
	} {
		a, _ := url.Parse(tc.a)
		b, _ := url.Parse(tc.b)
		if got := sameOrigin(a, b); got != tc.same {
			t.Errorf("sameOrigin(%s, %s) = %v, want %v (keys %s, %s)", tc.a, tc.b, got, tc.same, originKey(a), originKey(b))
		}
	}
}

// roundTripFunc serves a handler as an http.RoundTripper, so a test can use
// the Hub's real host name, with and without ":443", with no network.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func handlerTransport(h http.HandlerFunc) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h(rec, r)
		resp := rec.Result()
		resp.Request = r
		return resp, nil
	})
}

// TestAnExplicitDefaultPortIsNotACrossHostLink: "https://hub:443/..." and
// "https://hub/..." are one origin, in the base URL and in a Link.
func TestAnExplicitDefaultPortIsNotACrossHostLink(t *testing.T) {
	for name, tc := range map[string]struct{ base, linkHost string }{
		"port in the base URL, none in the link": {"https://hub.example.org:443", "https://hub.example.org"},
		"port in the link, none in the base URL": {"https://hub.example.org", "https://hub.example.org:443"},
		"port in both":                           {"https://hub.example.org:443", "https://hub.example.org:443"},
	} {
		var seenHosts []string
		rt := handlerTransport(func(w http.ResponseWriter, r *http.Request) {
			seenHosts = append(seenHosts, r.URL.Host)
			if r.URL.Query().Get("cursor") != "" {
				_, _ = w.Write([]byte(listOf(lfsJSON("b.safetensors"))))
				return
			}
			w.Header().Set("Link", "<"+tc.linkHost+treePath+`?cursor=2&recursive=true>; rel="next"`)
			_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
		})
		c, err := New(Config{BaseURL: tc.base, HTTPClient: &http.Client{Transport: rt}})
		if err != nil {
			t.Fatal(err)
		}
		entries, err := c.Files(context.Background(), testRepo, testRev)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got, want := paths(entries), []string{"a.safetensors", "b.safetensors"}; !slices.Equal(got, want) {
			t.Errorf("%s: files = %q, want %q", name, got, want)
		}
		if len(seenHosts) != 2 {
			t.Errorf("%s: hosts %q", name, seenHosts)
		}
	}
}

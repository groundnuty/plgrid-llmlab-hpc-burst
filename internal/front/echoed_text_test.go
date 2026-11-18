package front

// The bound on the client's text that an error message repeats (echoed_text.go; issue #647). What is checked, for each of the five messages that
// repeat it (GET /v1/models/{id}, an unknown provider, a model that is not attached, a model with no ready target, an unknown deployment ID): an
// over-long text draws the SAME status, code, detail, Retry-After and attribution as a short one, and a message whose repeated part is the first 1,355
// bytes and the marker, in a body and a log within the bound; a text of exactly 1,355 bytes is repeated whole and one of 1,356 is cut. The usage
// record's own bound is recorded_text_test.go's.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// echoSite is one of the messages that repeat the client's text: how to provoke it with a text, what it answers, and the message around the repeated part.
type echoSite struct {
	name   string
	do     func(s *errFix, text string) (*http.Response, string)
	want   wantError
	before string // the message up to the repeated part
	after  string // the message after it
	// maxText is the longest text the site can be sent: a path is bounded by the header limit, a body by MaxBodyBytes.
	maxText int
}

// escapeUnreserved writes every byte of s that is not an ASCII letter or digit as %XX, so that a path of letters keeps its length and any other text is
// sent as the decoded text it is.
func escapeUnreserved(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			b.WriteByte(c)
		} else {
			b.WriteString(percentEncoded(s[i : i+1]))
		}
	}
	return b.String()
}

var echoSites = []echoSite{
	{
		name: "GET /v1/models/{id}",
		do: func(s *errFix, id string) (*http.Response, string) {
			resp := s.errGet("/v1/models/" + escapeUnreserved(id))
			return resp, readAll(s.t, resp)
		},
		want:   wantError{status: http.StatusNotFound, code: "model_not_found"},
		before: "the model ", after: " is not listed",
		maxText: 900 << 10,
	},
	{
		name: "an unknown provider",
		do: func(s *errFix, provider string) (*http.Response, string) {
			resp := s.post("/v1/chat/completions", errChatBody(efModel+"@"+provider, false, "hello there"))
			return resp, readAll(s.t, resp)
		},
		want:   wantError{status: http.StatusBadRequest, code: "invalid_model_name", detail: "unknown_provider"},
		before: "the model name names an unknown provider: ", after: "",
		maxText: 5 << 20,
	},
	{
		name: "a model that is not attached",
		do: func(s *errFix, model string) (*http.Response, string) {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			return resp, readAll(s.t, resp)
		},
		want:   wantError{status: http.StatusServiceUnavailable, code: "model_inactive", detail: "not_attached", retryAfter: "30"},
		before: "the model ", after: " is not active: no provider is attached to it",
		maxText: 5 << 20,
	},
	{
		name: "an unknown deployment ID",
		do: func(s *errFix, id string) (*http.Response, string) {
			return s.getRecords(escapeUnreserved(id))
		},
		want:   wantError{status: http.StatusNotFound, code: "deployment_not_found"},
		before: "no identity record holds the deployment ID ", after: "",
		maxText: 900 << 10,
	},
}

// echoedMessage is the message of an error answer, and fails the test unless the answer is a front error body.
func echoedMessage(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("the answer is not an error body: %v: %.200s", err, body)
	}
	return doc.Error.Message
}

// longestLine is the length of the longest line of the harness's log.
func (h *harness) longestLogLine() int {
	n := 0
	for _, l := range strings.Split(h.logs.String(), "\n") {
		if len(l) > n {
			n = len(l)
		}
	}
	return n
}

// Each message repeats the text whole up to the bound and its first bytes and the marker beyond it, and nothing else about the answer changes: the status,
// the code, the detail, Retry-After and the attribution are those of a short text's, and the answer and the log are within the bound however long the text.
func TestAnErrorMessageRepeatsABoundedCopyOfTheClientsText(t *testing.T) {
	for _, site := range echoSites {
		t.Run(site.name, func(t *testing.T) {
			s := newErrFix(t, nil, instanceOpts{}, "")
			ascii := func(n int) string { return strings.Repeat("n", n) }
			for name, c := range map[string]struct{ text, echoed string }{
				"a short text":                               {"zz-short", "zz-short"},
				"exactly the bound":                          {ascii(1355), ascii(1355)},
				"one byte over the bound":                    {ascii(1356), ascii(1355) + cutMarker},
				"the longest it can be sent, with a newline": {ascii(site.maxText-2) + "\n" + "n", ascii(1355) + cutMarker},
			} {
				t.Run(name, func(t *testing.T) {
					resp, body := site.do(s, c.text)
					checkError(t, resp, body, site.want)
					checkAttribution(t, resp, efFrontOnly)
					if got, want := echoedMessage(t, body), site.before+c.echoed+site.after; got != want {
						t.Errorf("the message has %d bytes (a text of %d was sent), want %d: the text whole up to 1,355 bytes, else its first 1,355 and the marker", len(got), len(c.text), len(want))
					}
					if len(body) > 4096 && len(c.echoed) <= 1355+len(cutMarker) {
						t.Errorf("the answer is %d bytes for a text of %d", len(body), len(c.text))
					}
					if n := s.longestLogLine(); n > 8192 {
						t.Errorf("the log has a line of %d bytes after a request with a text of %d", n, len(c.text))
					}
				})
			}
		})
	}
}

// The message of a model with no ready target repeats its name bounded too. The name is an attached model's, at most 256 characters, in the answers a
// running front gives; only before the router has a view does it repeat any name, and that state is not one a fixture can hold, so the constructors are
// called with a long name: the status, code and detail are the same, and the message holds the bounded copy. The other two are checked the same way.
func TestTheConstructorsOfTheEchoingErrorsBoundTheirCopy(t *testing.T) {
	long := strings.Repeat("n", 5<<20)
	want := strings.Repeat("n", 1355) + cutMarker
	for name, c := range map[string]struct {
		e                  *routeError
		status             int
		code, detail, mess string
	}{
		"no ready target":  {errNoReadyTarget(long), http.StatusServiceUnavailable, "model_inactive", "no_ready_target", "the model " + want + " has no ready target at the requested level"},
		"not attached":     {errNotAttached(long), http.StatusServiceUnavailable, "model_inactive", "not_attached", "the model " + want + " is not active: no provider is attached to it"},
		"unknown provider": {errUnknownProvider(long), http.StatusBadRequest, "invalid_model_name", "unknown_provider", "the model name names an unknown provider: " + want},
	} {
		if c.e.status != c.status || c.e.code != c.code || c.e.detail != c.detail || c.e.message != c.mess {
			t.Errorf("%s: status %d code %q detail %q and a message of %d bytes, want %d %q %q and %d bytes", name, c.e.status, c.e.code, c.e.detail, len(c.e.message), c.status, c.code, c.detail, len(c.mess))
		}
	}
	for _, m := range []*routeError{errNoReadyTarget("example-27b"), errNotAttached("example-27b"), errUnknownProvider("helios-a")} {
		if len(m.message) > 200 {
			t.Errorf("a message that repeats a short name is %d bytes: %q", len(m.message), m.message)
		}
	}
}

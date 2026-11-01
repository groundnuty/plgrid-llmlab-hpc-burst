package front

import (
	"regexp"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, "x-session-id": every request reaching an engine carries
// it, with the client's header value or the opening key; a request without the
// header and without messages carries an r- key (Conversation key).
//
// The opening keys below are o1- plus the SHA-256 of the hand-written RFC 8785
// bytes of the opening messages, taken with sha256sum outside our code, e.g.
//
//	$ printf '%s' '[{"content":"hello there","role":"user"}]' | sha256sum
//	910a1cdfedbcba67619132cd5b34ead261b5466c88093027edb47354d8e718ad
//
// The key rule itself has its golden vectors in internal/front/convkey; these
// tests show that the request path applies it and that the header reaches the
// engine on every endpoint.

var (
	skOpening = regexp.MustCompile(`^o1-[0-9a-f]{64}$`)
	skRandom  = regexp.MustCompile(`^r-[0-9a-f]{32}$`)
)

// skLast is the x-session-id of the engine's latest request, and whether the
// engine got the header at all.
func skLast(t *testing.T, e *enginetest.Engine, wantRequests int) (string, bool) {
	t.Helper()
	reqs := e.Requests()
	if len(reqs) != wantRequests {
		t.Fatalf("the engine saw %d requests, want %d", len(reqs), wantRequests)
	}
	h := reqs[len(reqs)-1].Header
	_, present := h["X-Session-Id"]
	return h.Get("X-Session-Id"), present
}

// TestSessionKeyOnEveryEndpoint: the client's header value arrives unchanged
// on every endpoint, streamed or not.
func TestSessionKeyOnEveryEndpoint(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	bodies := []struct{ path, body string }{
		{"/v1/chat/completions", `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`},
		{"/v1/chat/completions", `{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`},
		{"/v1/completions", `{"model":"example-27b","prompt":"hello there"}`},
		{"/v1/responses", `{"model":"example-27b","input":"hello there"}`},
		{"/v1/responses", `{"model":"example-27b","stream":true,"input":"hello there"}`},
		{"/v1/embeddings", `{"model":"example-27b","input":"hello there"}`},
		{"/v1/other", `{"model":"example-27b","anything":1}`},
	}
	for i, tc := range bodies {
		want := "conv-" + string(rune('a'+i))
		resp := stPost(t, f.harness, tc.path, tc.body, "x-session-id", want)
		_ = readAll(t, resp)
		if got, _ := skLast(t, f.e, i+1); got != want {
			t.Errorf("%s: the engine got x-session-id %q, want the client's %q", tc.path, got, want)
		}
	}
}

// TestOpeningKeyForARequestWithMessagesAndNoHeader: hashed by the rule of
// Conversation key, whatever else the request holds.
func TestOpeningKeyForARequestWithMessagesAndNoHeader(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	cases := []struct{ name, body, key string }{
		{"one user message",
			`{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`,
			"o1-910a1cdfedbcba67619132cd5b34ead261b5466c88093027edb47354d8e718ad"},
		{"a system and a user message, and a later turn that is not hashed",
			`{"model":"example-27b","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hello there"},{"role":"assistant","content":"hi"},{"role":"user","content":"and then"}]}`,
			"o1-cbd24e4c9bc52678db1ea8d1ab9dcfea38c219e2992d5151245e85da0d2882ab"},
		{"content as an array of parts",
			`{"model":"example-27b","messages":[{"role":"user","content":[{"type":"text","text":"look"}]}]}`,
			"o1-a8094c51de02d0588789024d4aeb18a46ddd28ec5d586a9451dd63dc4652a406"},
		{"a stream",
			`{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`,
			"o1-910a1cdfedbcba67619132cd5b34ead261b5466c88093027edb47354d8e718ad"},
	}
	for i, tc := range cases {
		resp := stPost(t, f.harness, "/v1/chat/completions", tc.body)
		_ = readAll(t, resp)
		got, _ := skLast(t, f.e, i+1)
		if !skOpening.MatchString(got) {
			t.Errorf("%s: x-session-id %q is not o1- and 64 lowercase hex characters", tc.name, got)
		}
		if got != tc.key {
			t.Errorf("%s: x-session-id %q, want %q", tc.name, got, tc.key)
		}
	}
}

// TestHeaderWinsOverTheOpeningKeyAndAnEmptyHeaderDoesNot: the key is the
// header's value when it is non-empty (Conversation key, Session header).
func TestHeaderWinsOverTheOpeningKeyAndAnEmptyHeaderDoesNot(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	const body = `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`
	_ = readAll(t, stPost(t, f.harness, "/v1/chat/completions", body, "x-session-id", "mine"))
	if got, _ := skLast(t, f.e, 1); got != "mine" {
		t.Errorf("with the header and messages: x-session-id %q, want mine", got)
	}
	_ = readAll(t, stPost(t, f.harness, "/v1/chat/completions", body, "x-session-id", ""))
	if got, _ := skLast(t, f.e, 2); got != "o1-910a1cdfedbcba67619132cd5b34ead261b5466c88093027edb47354d8e718ad" {
		t.Errorf("with an empty header: x-session-id %q, want the opening key", got)
	}
}

// TestRandomKeyForARequestWithNeitherHeaderNorMessages: /v1/completions,
// /v1/responses and /v1/embeddings without the header get r- and 32 random hex
// characters, a fresh one per request, so they have no affinity.
func TestRandomKeyForARequestWithNeitherHeaderNorMessages(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	bodies := []struct{ path, body string }{
		{"/v1/completions", `{"model":"example-27b","prompt":"hello there"}`},
		{"/v1/responses", `{"model":"example-27b","input":"hello there"}`},
		{"/v1/embeddings", `{"model":"example-27b","input":"hello there"}`},
		{"/v1/chat/completions", `{"model":"example-27b"}`}, // chat, but no messages array
	}
	seen := map[string]string{}
	n := 0
	for round := 0; round < 2; round++ { // the same request twice: two keys
		for _, tc := range bodies {
			resp := stPost(t, f.harness, tc.path, tc.body)
			_ = readAll(t, resp)
			n++
			got, present := skLast(t, f.e, n)
			if !present || !skRandom.MatchString(got) {
				t.Errorf("%s: x-session-id %q (present %v), want r- and 32 lowercase hex characters", tc.path, got, present)
			}
			if prev, dup := seen[got]; dup {
				t.Errorf("%s: key %q repeats the one of %s; each request gets a fresh key", tc.path, got, prev)
			}
			seen[got] = tc.path
		}
	}
}

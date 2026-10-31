package llmlabtest

import (
	"bytes"
	"net/http"
	"slices"
	"testing"
)

// rig sends POSTs and checks, for each, that the Server logged the body byte
// for byte: the rewriting is the gateway's, the log keeps what arrived.
type rig struct {
	t *testing.T
	s *Server
	n int // requests sent so far
}

// newRig starts a Server whose modelA has a context window of 100 tokens and
// whose modelB has none.
func newRig(t *testing.T) *rig {
	return &rig{t: t, s: startS(t, Model{ID: modelA, ContextWindow: 100}, Model{ID: modelB})}
}

func (r *rig) post(route, body string) answer {
	r.t.Helper()
	a := ask(r.t, r.s, http.MethodPost, route, keyOne, body)
	logged := waitLogged(r.t, r.s, r.n+1)[r.n]
	r.n++
	if !bytes.Equal(logged.Body, []byte(body)) {
		r.t.Errorf("logged body differs from the one sent:\nsent:   %q\nlogged: %q", body, logged.Body)
	}
	if logged.Status != a.Status {
		r.t.Errorf("logged status %d, client got %d", logged.Status, a.Status)
	}
	return a
}

// chat sends a chat body of three words with the extra members (each starting
// with a comma). The body has spacing, member order and escapes that a
// re-encoder would change.
func (r *rig) chat(model, extra string) answer {
	r.t.Helper()
	return r.post(routeChat, "{ \"messages\" : [ {\"role\":\"user\",\"content\":\"one two three\"} ],\n\t\"model\" : \""+model+"\" "+extra+" }\n")
}

const (
	saltMember   = `, "cache_salt" : "s<>é"`
	windowMember = `, "max_tokens" : 100` // modelA's context window
	belowMember  = `,"max_tokens":99`
)

// usage reads one number of the answer's "usage" object.
func usage(t *testing.T, a answer, field string) int {
	t.Helper()
	var u struct{ Usage map[string]int }
	decode(t, a.Body, &u)
	n, ok := u.Usage[field]
	if !ok {
		t.Fatalf("no usage.%s in %s", field, a.Body)
	}
	return n
}

func errorParam(t *testing.T, a answer) string {
	t.Helper()
	e := parseError(t, a.Body)
	if e.Error.Param == nil {
		return ""
	}
	return *e.Error.Param
}

func TestRewritingOff(t *testing.T) {
	r := newRig(t)
	a := r.chat(modelA, saltMember)
	requireStatus(t, a, http.StatusOK, "cache_salt, rewriting off")
	if got := usage(t, a, "prompt_tokens"); got != 3 {
		t.Errorf("prompt_tokens = %d, want the 3 words of the request", got)
	}
	requireStatus(t, r.chat(modelA, windowMember), http.StatusOK, "max_tokens at the context window, rewriting off")
}

// Each case sends the same request with rewriting off, then on.
func TestRewritingOn(t *testing.T) {
	t.Run("a fixed segment is added to the prompt", func(t *testing.T) {
		r := newRig(t)
		if got := usage(t, r.chat(modelA, ""), "prompt_tokens"); got != 3 {
			t.Fatalf("prompt_tokens = %d with rewriting off, want 3", got)
		}
		r.s.SetRewriting(&Rewriting{})
		a := r.chat(modelA, "")
		requireStatus(t, a, http.StatusOK, "plain request")
		if got := usage(t, a, "prompt_tokens"); got != 15 { // 3 words and the 12-token segment
			t.Errorf("prompt_tokens = %d, want 15", got)
		}
	})

	t.Run("a member outside the allowlist gets 422", func(t *testing.T) {
		r := newRig(t)
		requireStatus(t, r.chat(modelA, saltMember), http.StatusOK, "cache_salt with rewriting off")
		r.s.SetRewriting(&Rewriting{})
		a := r.chat(modelA, saltMember)
		requireStatus(t, a, http.StatusUnprocessableEntity, "cache_salt")
		if p := errorParam(t, a); p != "cache_salt" {
			t.Errorf("param = %q, want cache_salt", p)
		}
		a = r.chat(modelA, `, "foo_bar" : 1`)
		requireStatus(t, a, http.StatusUnprocessableEntity, "foo_bar")
		if p := errorParam(t, a); p != "foo_bar" {
			t.Errorf("param = %q, want foo_bar", p)
		}
		requireStatus(t, r.chat(modelA, `, "temperature" : 0.5, "max_tokens" : 5`), http.StatusOK, "members of the default allowlist")
	})

	t.Run("max_tokens equal to the context window gets 400", func(t *testing.T) {
		r := newRig(t)
		requireStatus(t, r.chat(modelA, windowMember), http.StatusOK, "max_tokens = window with rewriting off")
		r.s.SetRewriting(&Rewriting{})
		requireStatus(t, r.chat(modelA, windowMember), http.StatusBadRequest, "max_tokens = window")
		requireStatus(t, r.chat(modelA, belowMember), http.StatusOK, "max_tokens = window - 1")
		requireStatus(t, r.chat(modelB, windowMember), http.StatusOK, "a model with no context window")
	})

	t.Run("the other routes", func(t *testing.T) {
		r := newRig(t)
		for _, c := range []struct {
			name, route, body, field string
			off, on                  int
		}{
			{"completions", routeCompletions, `{"model":"example-9b", "prompt" : "one two three"}`, "prompt_tokens", 3, 15},
			{"responses", routeResponses, `{"model":"example-9b","instructions":"be brief","input":"one two three"}`, "input_tokens", 5, 17},
			{"embeddings", routeEmbeddings, `{"model":"example-9b","input":["one two","three"]}`, "prompt_tokens", 3, 3}, // nothing is added to an embedding
		} {
			if got := usage(t, r.post(c.route, c.body), c.field); got != c.off {
				t.Errorf("%s %s = %d with rewriting off, want %d", c.name, c.field, got, c.off)
			}
		}
		streamBody := `{"model":"example-9b","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"one two three"}]}`
		if got := streamedPromptTokens(t, r.post(routeChat, streamBody)); !slices.Equal(got, []int{3}) {
			t.Errorf("stream usage = %v with rewriting off, want [3]", got)
		}

		r.s.SetRewriting(&Rewriting{})
		for _, c := range []struct {
			name, route, body, field string
			off, on                  int
		}{
			{"completions", routeCompletions, `{"model":"example-9b", "prompt" : "one two three"}`, "prompt_tokens", 3, 15},
			{"responses", routeResponses, `{"model":"example-9b","instructions":"be brief","input":"one two three"}`, "input_tokens", 5, 17},
			{"embeddings", routeEmbeddings, `{"model":"example-9b","input":["one two","three"]}`, "prompt_tokens", 3, 3},
		} {
			if got := usage(t, r.post(c.route, c.body), c.field); got != c.on {
				t.Errorf("%s %s = %d with rewriting on, want %d", c.name, c.field, got, c.on)
			}
		}
		if got := streamedPromptTokens(t, r.post(routeChat, streamBody)); !slices.Equal(got, []int{15}) {
			t.Errorf("stream usage = %v with rewriting on, want [15]", got)
		}
	})
}

// streamedPromptTokens returns the prompt_tokens of every usage chunk.
func streamedPromptTokens(t *testing.T, a answer) []int {
	t.Helper()
	requireStatus(t, a, http.StatusOK, "stream")
	var out []int
	for _, c := range parseSSE(t, a.Body).Chunks {
		if c.Usage != nil {
			out = append(out, c.Usage.PromptTokens)
		}
	}
	return out
}

func TestRewritingSwitchedOff(t *testing.T) {
	r := newRig(t)
	r.s.SetRewriting(&Rewriting{})
	requireStatus(t, r.chat(modelA, saltMember), http.StatusUnprocessableEntity, "rewriting on")

	r.s.SetRewriting(nil)
	a := r.chat(modelA, saltMember)
	requireStatus(t, a, http.StatusOK, "cache_salt, rewriting off again")
	if got := usage(t, a, "prompt_tokens"); got != 3 {
		t.Errorf("prompt_tokens = %d, want 3", got)
	}
	requireStatus(t, r.chat(modelA, windowMember), http.StatusOK, "max_tokens = window, rewriting off again")
}

func TestRewritingInjectedTokens(t *testing.T) {
	r := newRig(t)
	if got := usage(t, r.chat(modelA, ""), "prompt_tokens"); got != 3 {
		t.Fatalf("prompt_tokens = %d with rewriting off, want 3", got)
	}
	r.s.SetRewriting(&Rewriting{InjectedTokens: 7})
	if got := usage(t, r.chat(modelA, ""), "prompt_tokens"); got != 10 {
		t.Errorf("prompt_tokens = %d, want 3 words and 7 injected", got)
	}
}

func TestRewritingAllowlist(t *testing.T) {
	r := newRig(t)
	// The default list refuses cache_salt.
	r.s.SetRewriting(&Rewriting{})
	requireStatus(t, r.chat(modelA, saltMember), http.StatusUnprocessableEntity, "default allowlist")

	r.s.SetRewriting(&Rewriting{Allowlist: []string{"model", "messages", "cache_salt"}})
	a := r.chat(modelA, saltMember)
	requireStatus(t, a, http.StatusOK, "cache_salt on a custom allowlist")
	if got := usage(t, a, "prompt_tokens"); got != 15 {
		t.Errorf("prompt_tokens = %d, want 15: a custom allowlist keeps the injected segment", got)
	}
	a = r.chat(modelA, `, "temperature" : 0.5`)
	requireStatus(t, a, http.StatusUnprocessableEntity, "temperature, not on the custom allowlist")
	if p := errorParam(t, a); p != "temperature" {
		t.Errorf("param = %q, want temperature", p)
	}
}

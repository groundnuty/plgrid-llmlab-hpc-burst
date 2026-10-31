package llmlabtest

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

// An inactive model stays in the listing and answers 503 (L-6.5).
func TestInactiveModel(t *testing.T) {
	s := startS(t, Model{ID: modelA}, Model{ID: modelB})
	requireStatus(t, postChat(t, s, modelA), http.StatusOK, "active by default")

	s.SetActive(modelA, false)
	requireListed(t, s, modelA, modelB)
	requireStatus(t, postChat(t, s, modelA), http.StatusServiceUnavailable, "inactive model")
	requireStatus(t, postChat(t, s, modelB), http.StatusOK, "another model, still active")

	s.SetActive(modelA, true)
	requireStatus(t, postChat(t, s, modelA), http.StatusOK, "active again")
}

// An unknown, a blocked and a removed model get one 400 body; an inactive
// one joins them only with SetAmbiguous (L-6.5). The bodies are compared
// across requests, never with the double's own output.
func TestAmbiguousErrors(t *testing.T) {
	s := startS(t, Model{ID: modelA}, Model{ID: modelB}, Model{ID: modelC})
	requireStatus(t, postChat(t, s, modelB), http.StatusOK, "before blocking")
	requireStatus(t, postChat(t, s, modelC), http.StatusOK, "before removal")
	s.SetBlocked(modelB, true)
	s.RemoveModel(modelC)

	unknown := postChat(t, s, "example-unknown")
	blocked := postChat(t, s, modelB)
	removed := postChat(t, s, modelC)
	for name, a := range map[string]answer{"unknown": unknown, "blocked": blocked, "removed": removed} {
		requireStatus(t, a, http.StatusBadRequest, name+" model")
		parseError(t, a.Body) // an empty body would be "identical" too
	}
	if !bytes.Equal(unknown.Body, blocked.Body) || !bytes.Equal(unknown.Body, removed.Body) {
		t.Errorf("400 bodies differ:\nunknown: %s\nblocked: %s\nremoved: %s", unknown.Body, blocked.Body, removed.Body)
	}
	for _, name := range []string{"example-unknown", modelB, modelC} {
		if strings.Contains(string(unknown.Body), name) {
			t.Errorf("the 400 body names %q, so it would tell the causes apart", name)
		}
	}

	s.SetBlocked(modelB, false)
	requireStatus(t, postChat(t, s, modelB), http.StatusOK, "unblocked")

	s.SetActive(modelA, false)
	plain := postChat(t, s, modelA)
	requireStatus(t, plain, http.StatusServiceUnavailable, "inactive, ambiguity off")
	if bytes.Equal(plain.Body, unknown.Body) {
		t.Error("the 503 body equals the 400 body with ambiguity off")
	}

	s.SetAmbiguous(true)
	amb := postChat(t, s, modelA)
	requireStatus(t, amb, http.StatusBadRequest, "inactive, ambiguity on")
	if !bytes.Equal(amb.Body, unknown.Body) {
		t.Errorf("inactive 400 body %s differs from the unknown model's %s", amb.Body, unknown.Body)
	}
	requireStatus(t, postChat(t, s, modelB), http.StatusOK, "an active model under ambiguity")

	s.SetAmbiguous(false)
	requireStatus(t, postChat(t, s, modelA), http.StatusServiceUnavailable, "ambiguity off again")
}

// Listing changes: removal, addition, return, and what survives them.
func TestListingChanges(t *testing.T) {
	s := startS(t)
	unknown := postChat(t, s, "example-unknown")
	requireStatus(t, unknown, http.StatusBadRequest, "unknown model")

	s.SetModels(Model{ID: modelA}, Model{ID: modelB})
	requireListed(t, s, modelA, modelB)

	s.SetActive(modelA, false)
	s.RemoveModel(modelA)
	requireListed(t, s, modelB)
	gone := postChat(t, s, modelA)
	requireStatus(t, gone, http.StatusBadRequest, "removed model")
	if !bytes.Equal(gone.Body, unknown.Body) {
		t.Errorf("removed model's body %s differs from the unknown model's %s", gone.Body, unknown.Body)
	}
	requireStatus(t, ask(t, s, http.MethodGet, routeModels+"/"+modelA, keyOne, ""), http.StatusNotFound, "entry of a removed model")

	s.AddModel(Model{ID: modelC})
	requireListed(t, s, modelB, modelC)
	requireStatus(t, postChat(t, s, modelC), http.StatusOK, "added model")

	s.AddModel(Model{ID: modelA})
	requireListed(t, s, modelB, modelC, modelA)
	requireStatus(t, postChat(t, s, modelA), http.StatusServiceUnavailable, "returned model keeps its inactive state")
	s.SetActive(modelA, true)
	requireStatus(t, postChat(t, s, modelA), http.StatusOK, "returned model, active again")

	// The blocked state survives a removal and return as well.
	s.SetBlocked(modelB, true)
	s.RemoveModel(modelB)
	s.AddModel(Model{ID: modelB})
	requireStatus(t, postChat(t, s, modelB), http.StatusBadRequest, "returned model keeps its blocked state")
}

func TestSetModels(t *testing.T) {
	s := startS(t, Model{ID: modelA}, Model{ID: modelB})
	s.SetActive(modelB, false)

	s.SetModels(Model{ID: modelC}, Model{ID: modelB})
	requireListed(t, s, modelC, modelB) // new order, a gone
	requireStatus(t, postChat(t, s, modelA), http.StatusBadRequest, "model dropped by SetModels")
	requireStatus(t, postChat(t, s, modelC), http.StatusOK, "model added by SetModels")
	requireStatus(t, postChat(t, s, modelB), http.StatusServiceUnavailable, "a model SetModels kept keeps its state")

	s.SetModels()
	a := ask(t, s, http.MethodGet, routeModels, keyOne, "")
	if a.Status != http.StatusOK || !strings.Contains(string(a.Body), `"data":[]`) {
		t.Errorf("empty listing = %d %s, want an empty data array, not null", a.Status, a.Body)
	}
}

// A model's Revision is the listing's "revision", and absent when empty.
func TestRevision(t *testing.T) {
	s := startS(t, Model{ID: modelA, Revision: "2026-09"}, Model{ID: modelB})
	revisions := func(t *testing.T) map[string]any {
		t.Helper()
		a := ask(t, s, http.MethodGet, routeModels, keyOne, "")
		var l struct{ Data []map[string]any }
		decode(t, a.Body, &l)
		out := map[string]any{}
		for _, m := range l.Data {
			rev, ok := m["revision"]
			if !ok {
				rev = "<absent>"
			}
			out[m["id"].(string)] = rev
		}
		return out
	}
	got := revisions(t)
	if got[modelA] != "2026-09" || got[modelB] != "<absent>" {
		t.Errorf("listing revisions = %v, want %s: 2026-09 and %s absent", got, modelA, modelB)
	}

	s.AddModel(Model{ID: modelA, Revision: "2026-10"})
	if got := revisions(t); got[modelA] != "2026-10" {
		t.Errorf("revision after AddModel = %v, want 2026-10", got[modelA])
	}
	s.AddModel(Model{ID: modelA})
	if got := revisions(t); got[modelA] != "<absent>" {
		t.Errorf("revision cleared by AddModel = %v, want absent", got[modelA])
	}
}

// GET /v1/models/{id}: LLMLab has no retrieve route, so every name, listed
// or not, with a slash, an at sign or an escaped slash, and an empty one,
// answers 404 with FastAPI's body {"detail":"Not Found"} (#69).
func TestModelEntryIsNotFound(t *testing.T) {
	ids := []string{"plain-9b", "org/example-27b", "example@2026-09"}
	models := make([]Model, len(ids))
	for i, id := range ids {
		models[i] = Model{ID: id}
	}
	s := startS(t, models...)
	for _, p := range []string{"plain-9b", "org/example-27b", "example@2026-09", "org%2Fexample-27b", "example-unknown", ""} {
		a := ask(t, s, http.MethodGet, routeModels+"/"+p, keyOne, "")
		requireStatus(t, a, http.StatusNotFound, "entry "+p)
		var v map[string]any
		decode(t, a.Body, &v)
		if len(v) != 1 || v["detail"] != "Not Found" {
			t.Errorf("entry %q: body %s, want {\"detail\":\"Not Found\"}", p, a.Body)
		}
	}
}

// MaxTokensCap is a hidden cap (L-6.5): the output ends after the cap with
// finish reason "length", though the request asked for no limit.
func TestHiddenCap(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		stream     bool
	}{
		{"whole", chatReq(modelA), false},
		{"stream", streamReq(modelA), true},
		{"whole asking for more", `{"model":"example-9b","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`, false},
		{"stream asking for more", `{"model":"example-9b","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startS(t, Model{ID: modelA, MaxTokensCap: 2}, Model{ID: modelB})
			uncapped := strings.ReplaceAll(tc.body, modelA, modelB)

			// The model with no cap answers in full: the cap is the model's.
			full := ask(t, s, http.MethodPost, routeChat, keyOne, uncapped)
			text, finish, done := chatText(t, full, tc.stream)
			if text != DefaultReply || finish != "stop" || (tc.stream && !done) {
				t.Errorf("uncapped model: text %q finish %q done %v, want the full reply and stop", text, finish, done)
			}

			a := ask(t, s, http.MethodPost, routeChat, keyOne, tc.body)
			text, finish, done = chatText(t, a, tc.stream)
			if text != "This is" || finish != "length" || (tc.stream && !done) {
				t.Errorf("capped model: text %q finish %q done %v, want %q and length", text, finish, done, "This is")
			}
		})
	}
}

// chatText returns the generated text, the finish reason and whether [DONE]
// came, of a chat answer streamed or whole.
func chatText(t *testing.T, a answer, stream bool) (text, finish string, done bool) {
	t.Helper()
	requireStatus(t, a, http.StatusOK, "chat")
	if stream {
		st := parseSSE(t, a.Body)
		if f := st.finishes(); len(f) == 1 {
			finish = f[0]
		}
		return strings.Join(st.contents(), ""), finish, st.Done
	}
	var c struct {
		Choices []struct {
			Message      struct{ Content string }
			FinishReason string `json:"finish_reason"`
		}
	}
	decode(t, a.Body, &c)
	if len(c.Choices) != 1 {
		t.Fatalf("chat answer has %d choices: %s", len(c.Choices), a.Body)
	}
	return c.Choices[0].Message.Content, c.Choices[0].FinishReason, false
}

// TruncateAfter cuts a stream (L-6.7): the content chunks stop, and neither a
// finish reason nor [DONE] follows.
func TestTruncateAfter(t *testing.T) {
	s := startS(t, Model{ID: modelA, TruncateAfter: 2}, Model{ID: modelB})

	a := ask(t, s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
	requireStatus(t, a, http.StatusOK, "truncated stream")
	st := parseSSE(t, a.Body)
	if got := strings.Join(st.contents(), "|"); got != "This| is" {
		t.Errorf("content chunks = %q, want the first two words only", got)
	}
	if len(st.finishes()) != 0 || st.Done {
		t.Errorf("finish reasons %v, done %v: a truncated stream has neither", st.finishes(), st.Done)
	}

	full := ask(t, s, http.MethodPost, routeChat, keyOne, streamReq(modelB))
	fst := parseSSE(t, full.Body)
	if len(fst.contents()) != 5 || len(fst.finishes()) != 1 || !fst.Done {
		t.Errorf("model without TruncateAfter: %d chunks, finishes %v, done %v, want 5, one, true", len(fst.contents()), fst.finishes(), fst.Done)
	}

	// Only streams are cut.
	whole := postChat(t, s, modelA)
	if text, finish, _ := chatText(t, whole, false); text != DefaultReply || finish != "stop" {
		t.Errorf("non-streamed answer of the truncating model: %q %q, want the full reply", text, finish)
	}

	logs := waitLogged(t, s, 3)
	for i, want := range []bool{true, false, false} {
		if logs[i].Truncated != want {
			t.Errorf("request %d logged Truncated %v, want %v", i, logs[i].Truncated, want)
		}
	}
}

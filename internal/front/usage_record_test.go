package front

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Usage records: the counts and their source, and the privacy rule.
// These tests need the harness, so they live in package front; the sink's own
// tests (counter, fsync, crash gap) are in internal/front/usage.
//
// The expected counts come from what the engine double was told to send: its
// Behaviour.Tokens completion tokens, and the whitespace-separated words of
// the request's text as prompt tokens (enginetest's documented rule).

// rawRecord returns the usage file's JSON object for a request ID, as
// written, so that null members can be told from absent ones.
func rawRecord(t *testing.T, h *harness, id string) map[string]any {
	t.Helper()
	h.record(id) // wait until it is written
	b := readUsageFiles(t, h.cfg.UsageDir)
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("usage line %q: %v", line, err)
		}
		if m["requestID"] == id {
			return m
		}
	}
	t.Fatalf("no usage line for %s", id)
	return nil
}

func ptrIs(p *int64, want int64) bool { return p != nil && *p == want }

// ptrStr shows a count for a failure message: its value, or null.
func ptrStr(p *int64) string {
	if p == nil {
		return "null"
	}
	return strconv.FormatInt(*p, 10)
}

// strStr shows a string member for a failure message: its value, or null.
func strStr(p *string) string {
	if p == nil {
		return "null"
	}
	return *p
}

// TestUsageCountsOfAStreamWithoutUsage: a stream whose request did not ask for
// stream_options.include_usage carries no counts, so they are null, and the
// record holds the front's count of content events (countsSource
// front_events).
func TestUsageCountsOfAStreamWithoutUsage(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Tokens: 7})
	resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "one two three four five"))
	readAll(t, resp)
	id := resp.Header.Get("Hpcburst-Request-Id")
	rec := s.record(id)
	if rec.PromptTokens != nil || rec.CachedTokens != nil || rec.CompletionTokens != nil {
		t.Errorf("counts = %v %v %v, want all null: the client did not ask for usage", ptrStr(rec.PromptTokens), ptrStr(rec.CachedTokens), ptrStr(rec.CompletionTokens))
	}
	if rec.CountsSource == nil || *rec.CountsSource != "front_events" {
		t.Errorf("countsSource = %v, want front_events", strStr(rec.CountsSource))
	}
	if !ptrIs(rec.ContentEvents, 7) {
		t.Errorf("contentEvents = %v, want 7: the engine sent 7 tokens", ptrStr(rec.ContentEvents))
	}
	raw := rawRecord(t, s.harness, id)
	for _, k := range []string{"promptTokens", "cachedTokens", "completionTokens"} {
		if v, ok := raw[k]; !ok || v != nil {
			t.Errorf("%s = %v (present %v) in the file, want an explicit null", k, v, ok)
		}
	}
	if rec.Finish == nil || *rec.Finish != "stop" {
		t.Errorf("finish = %v, want stop", strStr(rec.Finish))
	}
}

// TestUsageCountsOfAStreamWithUsage: a stream whose request asked for
// include_usage gets countsSource engine and the engine's counts.
func TestUsageCountsOfAStreamWithUsage(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Tokens: 7})
	body := `{"model": "example-27b", "stream": true, "stream_options": {"include_usage": true},` +
		` "messages": [{"role": "user", "content": "one two three four five"}]}`
	resp := s.post("/v1/chat/completions", body)
	got := readAll(t, resp)
	rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
	if rec.CountsSource == nil || *rec.CountsSource != "engine" {
		t.Errorf("countsSource = %v, want engine", strStr(rec.CountsSource))
	}
	if !ptrIs(rec.PromptTokens, 5) || !ptrIs(rec.CompletionTokens, 7) || !ptrIs(rec.CachedTokens, 0) {
		t.Errorf("counts prompt %v completion %v cached %v, want 5 (the request's words), 7 (the engine's tokens) and 0", ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens), ptrStr(rec.CachedTokens))
	}
	if !strings.Contains(got, `"usage"`) {
		t.Errorf("the client's stream has no usage chunk, although it asked for one: %s", got)
	}
}

// TestUsageCountsOfWholeReplies: a non-streamed reply's counts come from its
// usage member (countsSource engine).
func TestUsageCountsOfWholeReplies(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Tokens: 7})
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "one two three four five"))
	readAll(t, resp)
	rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
	if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, 5) || !ptrIs(rec.CompletionTokens, 7) {
		t.Errorf("countsSource %v prompt %v completion %v, want engine 5 7", strStr(rec.CountsSource), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens))
	}
	if rec.Stream {
		t.Errorf("stream = true for a whole reply")
	}
}

// TestUsageCountsOnResponsesStreams: on /v1/responses the counts come from the
// response that response.completed carries; a stream that never completes has
// none, and the record holds the content events.
func TestUsageCountsOnResponsesStreams(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	body := `{"model": "example-27b", "stream": true, "input": "alpha beta gamma"}`
	t.Run("completed", func(t *testing.T) {
		s.e.Set(enginetest.Behaviour{Tokens: 7})
		resp := s.post("/v1/responses", body)
		readAll(t, resp)
		rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
		if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, 3) || !ptrIs(rec.CompletionTokens, 7) {
			t.Errorf("countsSource %v prompt %v completion %v, want engine 3 7", strStr(rec.CountsSource), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens))
		}
		if rec.Finish == nil || *rec.Finish != "completed" {
			t.Errorf("finish = %v, want the final response's status, completed", strStr(rec.Finish))
		}
	})
	t.Run("never completed", func(t *testing.T) {
		s.e.Set(enginetest.Behaviour{Tokens: 7, NoFinish: true})
		resp := s.post("/v1/responses", body)
		readAll(t, resp)
		rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
		if rec.PromptTokens != nil || rec.CompletionTokens != nil || rec.CountsSource == nil || *rec.CountsSource != "front_events" || !ptrIs(rec.ContentEvents, 7) {
			t.Errorf("counts %v %v source %v contentEvents %v, want null null front_events 7", ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens), strStr(rec.CountsSource), ptrStr(rec.ContentEvents))
		}
		if rec.Finish == nil || *rec.Finish != "truncated" {
			t.Errorf("finish = %v, want truncated", strStr(rec.Finish))
		}
	})
}

// TestUsageFileHoldsNoKeyNorPrompt: no record contains the client key, the
// Instance key, LLMLab's upstream key, a rejected key a client sent, a
// prompt, or a reply (HBEP-5, Privacy). The usage directory is searched for
// each secret after requests of every kind: a whole reply, a stream, an
// engine error, a rejected key and an LLMLab reply.
func TestUsageFileHoldsNoKeyNorPrompt(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{efUpstreamKey},
		Models: []llmlabtest.Model{{ID: efCatalog}}})
	s.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	s.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), efUpstreamKey, efCatalog)
	s.managedMD("md-managed", "managed-model", "llmlab", efCatalog, "llmlab-a")
	s.apply()

	const canary = "zebra-prompt-canary"
	const wrongKey = "wrong-client-key-9f3a51"
	var ids []string
	send := func(model string, stream bool, hdr ...string) {
		resp := s.post("/v1/chat/completions", errChatBody(model, stream, canary+" hello there"), hdr...)
		readAll(t, resp)
		ids = append(ids, resp.Header.Get("Hpcburst-Request-Id"))
	}
	send(efModel, false)
	send(efModel, true)
	s.e.Set(enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"engine fault"}}`})
	send(efModel, false)
	send("managed-model", false)
	send("managed-model", true)
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, canary), "Authorization", "Bearer "+wrongKey)
	readAll(t, resp)
	ids = append(ids, resp.Header.Get("Hpcburst-Request-Id"))

	for _, id := range ids {
		s.record(id) // every record is written before the files are read
	}
	var haystack strings.Builder
	files, err := os.ReadDir(s.cfg.UsageDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(s.cfg.UsageDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		haystack.Write(b)
	}
	all := haystack.String()

	// Positive controls: the files are not empty, and hold what they should.
	for _, id := range ids {
		if id == "" || !strings.Contains(all, id) {
			t.Errorf("the usage files do not hold request ID %q", id)
		}
	}
	if !strings.Contains(all, `"clientKeyID":"tester"`) {
		t.Errorf("the usage files do not name the client key by its ID")
	}
	// The secrets.
	for name, secret := range map[string]string{
		"the client key": s.clientKey, "the Instance key": s.key, "LLMLab's upstream key": efUpstreamKey,
		"a rejected client key": wrongKey, "the prompt": canary,
		"an LLMLab reply": "LLMLab double", "an authorization header": "Bearer",
	} {
		if strings.Contains(all, secret) {
			t.Errorf("the usage files contain %s (%q)", name, secret)
		}
	}
}

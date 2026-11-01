package front

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestUsageCachedTokensAreNullFromAnInstanceEngine: the framework does not
// start vLLM with --enable-prompt-tokens-details, so an Instance engine's
// usage names no cached count (spike S94: usage with prompt_tokens,
// completion_tokens and no prompt_tokens_details). The engine double does the
// same by default, and the record holds an explicit null: unknown, not 0.
// A scenario that sets CachedTokens gets that count in the record.
func TestUsageCachedTokensAreNullFromAnInstanceEngine(t *testing.T) {
	const streamUsage = `{"model": "` + efModel + `", "stream": true, "stream_options": {"include_usage": true},` +
		` "messages": [{"role": "user", "content": "one two three"}]}`
	const respStream = `{"model": "` + efModel + `", "stream": true, "input": "one two three"}`
	const respWhole = `{"model": "` + efModel + `", "input": "one two three"}`
	for _, c := range []struct{ name, path, body string }{
		{"chat stream", "/v1/chat/completions", streamUsage},
		{"chat whole reply", "/v1/chat/completions", errChatBody(efModel, false, "one two three")},
		{"responses stream", "/v1/responses", respStream},
		{"responses whole reply", "/v1/responses", respWhole},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newErrFix(t, nil, instanceOpts{}, "")
			s.e.Set(enginetest.Behaviour{Tokens: 4})
			resp := s.post(c.path, c.body)
			got := readAll(t, resp)
			for _, name := range []string{"cached_tokens", "prompt_tokens_details", "input_tokens_details"} {
				if strings.Contains(got, name) {
					t.Errorf("the engine double sent %s by default: %s", name, got)
				}
			}
			id := resp.Header.Get("Hpcburst-Request-Id")
			rec := s.record(id)
			if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, 3) || !ptrIs(rec.CompletionTokens, 4) {
				t.Fatalf("countsSource %s prompt %s completion %s, want engine 3 4: the null below must not come from missing counts",
					strStr(rec.CountsSource), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens))
			}
			if rec.CachedTokens != nil {
				t.Errorf("cachedTokens = %s, want null: the engine sent no cached count", ptrStr(rec.CachedTokens))
			}
			if v, ok := rawRecord(t, s.harness, id)["cachedTokens"]; !ok || v != nil {
				t.Errorf("cachedTokens = %v (present %v) in the file, want an explicit null", v, ok)
			}
		})
	}
}

// TestUsageCachedTokensOfAnEngineThatReportsThem: an engine started with
// --enable-prompt-tokens-details sends a count, 0 included, and the record
// holds it as sent.
func TestUsageCachedTokensOfAnEngineThatReportsThem(t *testing.T) {
	const streamUsage = `{"model": "` + efModel + `", "stream": true, "stream_options": {"include_usage": true},` +
		` "messages": [{"role": "user", "content": "one two three"}]}`
	const respStream = `{"model": "` + efModel + `", "stream": true, "input": "one two three"}`
	for _, c := range []struct {
		name, path, body string
		cached           int64
	}{
		{"chat stream", "/v1/chat/completions", streamUsage, 2},
		{"chat whole reply", "/v1/chat/completions", errChatBody(efModel, false, "one two three"), 2},
		{"responses stream", "/v1/responses", respStream, 2},
		{"chat stream, zero", "/v1/chat/completions", streamUsage, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newErrFix(t, nil, instanceOpts{}, "")
			s.e.Set(enginetest.Behaviour{Tokens: 4, CachedTokens: i64p(c.cached)})
			resp := s.post(c.path, c.body)
			readAll(t, resp)
			rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
			if !ptrIs(rec.CachedTokens, c.cached) {
				t.Errorf("cachedTokens = %s, want %d as the engine sent it", ptrStr(rec.CachedTokens), c.cached)
			}
		})
	}
}

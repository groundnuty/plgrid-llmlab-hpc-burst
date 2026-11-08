package enginedouble

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

// The expected values in this file come from three places, never from the
// double's own output: the OpenAI completions shape that vLLM v0.32.0 serves
// (the field lists below are those of CompletionResponse,
// CompletionResponseChoice, CompletionStreamResponse and
// CompletionResponseStreamChoice in vllm/entrypoints/openai/completion/
// protocol.py, and the stream order is completion_stream_generator in
// serving.py), the double's documented generation rules (doc.go: a token is a
// word, a model stops after 24 tokens of its own accord, min_tokens at
// max_tokens fixes the length, and so on), and hash/fnv for token ids.

// completionBody is a completions request whose prompt is n distinct words.
func completionBody(words int, extra map[string]any) map[string]any {
	m := map[string]any{"model": "m", "prompt": nWords(words)}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

type usageJSON struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// completionResult is the part of a non-streamed completion the tests read.
type completionResult struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int     `json:"index"`
		Text         string  `json:"text"`
		FinishReason string  `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage             usageJSON `json:"usage"`
	SystemFingerprint *string   `json:"system_fingerprint"`
}

func (s *testServer) completion(body map[string]any) completionResult {
	s.t.Helper()
	r := s.post("/v1/completions", body)
	if r.status != 200 {
		s.t.Fatalf("completion: HTTP %d: %s", r.status, r.body)
	}
	var out completionResult
	r.json(s.t, &out)
	if len(out.Choices) != 1 {
		s.t.Fatalf("completion: %d choices: %s", len(out.Choices), r.body)
	}
	return out
}

// completionChunk is one event of a streamed completion.
type completionChunkJSON struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Choices []struct {
		Index        int     `json:"index"`
		Text         string  `json:"text"`
		FinishReason *string `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage             *usageJSON `json:"usage"`
	SystemFingerprint string     `json:"system_fingerprint"`
}

func completionChunks(t *testing.T, r streamResult) []completionChunkJSON {
	t.Helper()
	var out []completionChunkJSON
	for _, ev := range r.events {
		if ev == "[DONE]" {
			continue
		}
		var c completionChunkJSON
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			t.Fatalf("event %q is not JSON: %v", ev, err)
		}
		out = append(out, c)
	}
	return out
}

func rawKeys(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("not a JSON object (%v): %s", err, b)
	}
	return m
}

// checkKeys fails unless every required key is present and every key present
// is one the vLLM model has.
func checkKeys(t *testing.T, what string, got map[string]json.RawMessage, required, allowed []string) {
	t.Helper()
	ok := map[string]bool{}
	for _, k := range allowed {
		ok[k] = true
	}
	for _, k := range required {
		ok[k] = true
		if _, has := got[k]; !has {
			t.Errorf("%s lacks %q", what, k)
		}
	}
	var extra []string
	for k := range got {
		if !ok[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("%s has keys vLLM's model does not: %v", what, extra)
	}
}

// Field lists of vLLM v0.32.0's completion models (completion/protocol.py:
// CompletionResponse 682-699, CompletionResponseChoice 659-680,
// CompletionStreamResponse 724-733, CompletionResponseStreamChoice 702-722).
var (
	responseFields = []string{"id", "object", "created", "model", "choices", "service_tier", "system_fingerprint", "usage", "kv_transfer_params", "ec_transfer_params", "metrics"}
	choiceFields   = []string{"index", "text", "logprobs", "finish_reason", "stop_reason", "token_ids", "prompt_logprobs", "prompt_token_ids", "routed_experts"}
	chunkFields    = []string{"id", "object", "created", "model", "choices", "usage", "system_fingerprint", "metrics"}
	chunkChoices   = []string{"index", "text", "logprobs", "finish_reason", "stop_reason", "prompt_token_ids", "token_ids"}
)

func TestCompletionReplyShape(t *testing.T) {
	s := serve(t, cfgFor())
	before := time.Now().Unix()
	r := s.post("/v1/completions", completionBody(4, map[string]any{"max_tokens": 5, "min_tokens": 5, "return_token_ids": true}))
	after := time.Now().Unix()
	if r.status != 200 || !strings.HasPrefix(r.header.Get("Content-Type"), "application/json") {
		t.Fatalf("HTTP %d, Content-Type %q: %s", r.status, r.header.Get("Content-Type"), r.body)
	}
	top := rawKeys(t, r.body)
	checkKeys(t, "the reply", top, []string{"id", "object", "created", "model", "choices", "usage"}, responseFields)
	var choices []json.RawMessage
	if err := json.Unmarshal(top["choices"], &choices); err != nil || len(choices) != 1 {
		t.Fatalf("choices = %s: want a list of one", top["choices"])
	}
	choice := rawKeys(t, choices[0])
	checkKeys(t, "choices[0]", choice, []string{"index", "text", "logprobs", "finish_reason"}, choiceFields)
	if string(choice["logprobs"]) != "null" {
		t.Errorf("logprobs = %s, want null", choice["logprobs"])
	}
	checkKeys(t, "usage", rawKeys(t, top["usage"]), []string{"prompt_tokens", "completion_tokens", "total_tokens"}, nil)

	var res completionResult
	r.json(t, &res)
	if res.Object != "text_completion" || !strings.HasPrefix(res.ID, "cmpl-") || res.Model != "m" {
		t.Errorf("object %q, id %q, model %q; want text_completion, a cmpl- id and m", res.Object, res.ID, res.Model)
	}
	if res.Created < before || res.Created > after {
		t.Errorf("created = %d, want a Unix time between %d and %d", res.Created, before, after)
	}
	c := res.Choices[0]
	if c.Index != 0 || c.FinishReason != "length" {
		t.Errorf("index %d, finish_reason %q; want 0 and length", c.Index, c.FinishReason)
	}
	// 4 prompt words (no template), 5 tokens fixed by min_tokens = max_tokens
	if want := (usageJSON{4, 5, 9}); res.Usage != want {
		t.Errorf("usage = %+v, want %+v", res.Usage, want)
	}
	words := strings.Fields(c.Text)
	if len(words) != 5 || len(c.TokenIDs) != 5 {
		t.Fatalf("text %q has %d words and %d token ids, want 5 and 5", c.Text, len(words), len(c.TokenIDs))
	}
	for i, w := range words {
		if refID(w) != c.TokenIDs[i] {
			t.Errorf("token %d: hash/fnv of %q is %d, the reply says %d", i, w, refID(w), c.TokenIDs[i])
		}
	}
	if res.SystemFingerprint == nil || *res.SystemFingerprint != "fp-enginedouble" {
		t.Errorf("system_fingerprint = %v, want fp-enginedouble", res.SystemFingerprint)
	}
	if _, has := top["system_fingerprint"]; !has {
		t.Errorf("no system_fingerprint in %s", r.body)
	}

	// without return_token_ids the ids are not sent
	if r := s.post("/v1/completions", completionBody(2, map[string]any{"max_tokens": 3})); r.status != 200 || strings.Contains(string(r.body), "token_ids") {
		t.Errorf("HTTP %d %s, want 200 without token_ids unless asked", r.status, r.body)
	}
}

// A prompt has no template: prompt_tokens is its word count, and it is what
// /tokenize counts for the same prompt.
func TestCompletionPromptTokensAreItsWords(t *testing.T) {
	s := serve(t, cfgFor())
	for _, words := range []int{1, 7, 40} {
		got := s.completion(completionBody(words, map[string]any{"max_tokens": 2})).Usage.PromptTokens
		if got != words {
			t.Errorf("%d-word prompt: prompt_tokens %d, want %d", words, got, words)
		}
		if n := s.tokenize(map[string]any{"prompt": nWords(words)}).Count; n != words {
			t.Errorf("%d-word prompt: /tokenize counts %d", words, n)
		}
	}
	// extra white space between words is not a token
	if got := s.completion(map[string]any{"model": "m", "prompt": " a  b\tc\n", "max_tokens": 2}).Usage.PromptTokens; got != 3 {
		t.Errorf("prompt \" a  b\\tc\\n\": prompt_tokens %d, want 3", got)
	}
}

// The length and finish reason follow doc.go: the model stops after 24 tokens
// of its own accord ("stop") unless max_tokens comes first or min_tokens asks
// for more; CompletionRequest.max_tokens defaults to 16 when absent
// (completion/protocol.py:63) and null means the rest of the context. The
// usage adds up, and the streamed answer says the same.
func TestCompletionLengthsAndFinishReasons(t *testing.T) {
	s := serve(t, cfgFor())
	cases := []struct {
		name       string
		extra      map[string]any
		wantTokens int
		wantFinish string
	}{
		{"max_tokens absent: the default 16, below the natural 24", map[string]any{}, 16, "length"},
		{"max_tokens null: the rest of the context", map[string]any{"max_tokens": nil}, 24, "stop"},
		{"max_tokens one", map[string]any{"max_tokens": 1}, 1, "length"},
		{"max_tokens below the natural length", map[string]any{"max_tokens": 10}, 10, "length"},
		{"max_tokens at the natural length", map[string]any{"max_tokens": 24}, 24, "length"},
		{"max_tokens above it: the model stops", map[string]any{"max_tokens": 100}, 24, "stop"},
		{"ignore_eos alone does not fix the length", map[string]any{"max_tokens": 57, "ignore_eos": true}, 24, "stop"},
		{"min_tokens at max_tokens", map[string]any{"max_tokens": 40, "min_tokens": 40}, 40, "length"},
		{"min_tokens with ignore_eos, as a warm-up sends them", map[string]any{"max_tokens": 57, "min_tokens": 57, "ignore_eos": true}, 57, "length"},
		{"min_tokens above the natural length", map[string]any{"max_tokens": 100, "min_tokens": 30}, 30, "stop"},
	}
	for _, tc := range cases {
		extra := map[string]any{"return_token_ids": true, "stream_options": map[string]any{"include_usage": true}}
		for k, v := range tc.extra {
			extra[k] = v
		}
		whole := s.completion(completionBody(5, extra))
		c := whole.Choices[0]
		if whole.Usage.CompletionTokens != tc.wantTokens || len(c.TokenIDs) != tc.wantTokens || len(strings.Fields(c.Text)) != tc.wantTokens || c.FinishReason != tc.wantFinish {
			t.Errorf("%s: %d completion tokens, %d token ids, %d words, finish %q; want %d, %d, %d, %q",
				tc.name, whole.Usage.CompletionTokens, len(c.TokenIDs), len(strings.Fields(c.Text)), c.FinishReason, tc.wantTokens, tc.wantTokens, tc.wantTokens, tc.wantFinish)
		}
		if want := (usageJSON{5, tc.wantTokens, 5 + tc.wantTokens}); whole.Usage != want {
			t.Errorf("%s: usage %+v, want %+v", tc.name, whole.Usage, want)
		}

		chunks := completionChunks(t, s.streamAt("/v1/completions", completionBody(5, extra)))
		if len(chunks) != tc.wantTokens+1 { // a chunk per token, then the usage chunk
			t.Errorf("%s: %d streamed chunks, want %d", tc.name, len(chunks), tc.wantTokens+1)
			continue
		}
		for i, ch := range chunks[:tc.wantTokens] {
			last := i == tc.wantTokens-1
			if got := ch.Choices[0].FinishReason; (got != nil) != last || (last && *got != tc.wantFinish) {
				t.Errorf("%s: chunk %d finish_reason %v, want %q on the last chunk only", tc.name, i, got, tc.wantFinish)
			}
		}
		if u := chunks[tc.wantTokens].Usage; u == nil || *u != (usageJSON{5, tc.wantTokens, 5 + tc.wantTokens}) {
			t.Errorf("%s: streamed usage %+v, want %+v", tc.name, u, usageJSON{5, tc.wantTokens, 5 + tc.wantTokens})
		}
	}
}

// vLLM's AuthenticationMiddleware guards the /v1 prefix: the completions route
// needs the key as the chat route does, and answers 200 with it.
func TestCompletionKeyGuard(t *testing.T) {
	cfg := cfgFor()
	cfg.APIKey = "k-123"
	s := serve(t, cfg)
	body := completionBody(2, map[string]any{"max_tokens": 2})
	streamed := completionBody(2, map[string]any{"max_tokens": 2, "stream": true})
	for _, key := range []string{"", "wrong", "k-12", "k-1234"} {
		for name, b := range map[string]map[string]any{"whole": body, "streamed": streamed} {
			if r := s.do(http.MethodPost, "/v1/completions", key, b); r.status != 401 || strings.TrimSpace(string(r.body)) != `{"error":"Unauthorized"}` {
				t.Errorf("%s request with key %q: HTTP %d %s, want 401 {\"error\":\"Unauthorized\"}", name, key, r.status, r.body)
			}
		}
	}
	r := s.do(http.MethodPost, "/v1/completions", "k-123", body)
	if r.status != 200 {
		t.Fatalf("with the key: HTTP %d: %s", r.status, r.body)
	}
	var res completionResult
	r.json(t, &res)
	if res.Object != "text_completion" || res.Usage.CompletionTokens != 2 {
		t.Errorf("with the key: %s, want a text_completion of 2 tokens", r.body)
	}
	if r := s.do(http.MethodPost, "/v1/completions", "k-123", streamed); r.status != 200 || !bytes.HasSuffix(r.body, []byte("data: [DONE]\n\n")) {
		t.Errorf("streamed with the key: HTTP %d %q", r.status, r.body)
	}
	// the guard is not a reason to refuse a body: a refused request with the key says why
	if r := s.do(http.MethodPost, "/v1/completions", "k-123", map[string]any{"model": "m"}); r.status != 400 {
		t.Errorf("no prompt, with the key: HTTP %d, want 400", r.status)
	}
}

func TestCompletionRefusals(t *testing.T) {
	cfg := cfgFor()
	cfg.MaxModelLen = 100
	s := serve(t, cfg)
	cases := []struct {
		name   string
		body   any
		status int
		typ    string
		in     string // the message says
	}{
		{"no prompt", map[string]any{"model": "m"}, 400, "BadRequestError", "needs a prompt"},
		{"a null prompt", map[string]any{"model": "m", "prompt": nil}, 400, "BadRequestError", "needs a prompt"},
		{"a list of strings", map[string]any{"model": "m", "prompt": []string{"a b"}}, 400, "BadRequestError", "must be a string"},
		{"a number", map[string]any{"model": "m", "prompt": 7}, 400, "BadRequestError", "must be a string"},
		{"an empty prompt", map[string]any{"model": "m", "prompt": ""}, 400, "BadRequestError", "The decoder prompt cannot be empty"},
		{"a prompt of white space", map[string]any{"model": "m", "prompt": " \t\n"}, 400, "BadRequestError", "The decoder prompt cannot be empty"},
		{"another model", completionBody(2, map[string]any{"model": "other"}), 404, "NotFoundError", "`other` does not exist"},
		{"min_tokens above max_tokens", completionBody(2, map[string]any{"max_tokens": 10, "min_tokens": 11}), 400, "BadRequestError", "min_tokens must be"},
		{"a negative min_tokens", completionBody(2, map[string]any{"max_tokens": 10, "min_tokens": -1}), 400, "BadRequestError", "min_tokens must be"},
		{"one token over the context", completionBody(60, map[string]any{"max_tokens": 41}), 400, "BadRequestError", "maximum context length is 100"},
		{"not an object", "just a string", 400, "BadRequestError", "JSON decode error"},
	}
	for _, tc := range cases {
		r := s.post("/v1/completions", tc.body)
		if r.status != tc.status {
			t.Errorf("%s: HTTP %d, want %d: %s", tc.name, r.status, tc.status, r.body)
			continue
		}
		var e struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    int    `json:"code"`
			} `json:"error"`
		}
		r.json(t, &e)
		if e.Error.Type != tc.typ || e.Error.Code != tc.status || !strings.Contains(e.Error.Message, tc.in) {
			t.Errorf("%s: error %+v, want type %s, code %d and a message with %q", tc.name, e.Error, tc.typ, tc.status, tc.in)
		}
	}
	// the limit itself is fine: 59 prompt words + 41 = 100
	if r := s.post("/v1/completions", completionBody(59, map[string]any{"max_tokens": 41})); r.status != 200 {
		t.Errorf("a request that fits exactly: HTTP %d: %s", r.status, r.body)
	}
	if r := s.get("/v1/completions"); r.status != 405 {
		t.Errorf("GET /v1/completions: HTTP %d, want 405", r.status)
	}
}

package enginedouble

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestAuthSplit(t *testing.T) {
	cfg := cfgFor()
	cfg.APIKey = "k-123"
	s := serve(t, cfg)

	// the guarded prefixes need the key
	for _, path := range []string{"/v1/models", "/v1/chat/completions", "/v2/rerank", "/inference/x", "/cohere/x"} {
		if r := s.do(http.MethodGet, path, "", nil); r.status != 401 || strings.TrimSpace(string(r.body)) != `{"error":"Unauthorized"}` {
			t.Errorf("GET %s without a key: HTTP %d %s, want 401 {\"error\":\"Unauthorized\"}", path, r.status, r.body)
		}
	}
	if r := s.do(http.MethodGet, "/v1/models", "wrong", nil); r.status != 401 {
		t.Errorf("/v1/models with a wrong key: HTTP %d, want 401", r.status)
	}
	if r := s.do(http.MethodGet, "/v1/models", "k-123", nil); r.status != 200 {
		t.Errorf("/v1/models with the key: HTTP %d, want 200", r.status)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://engine/v1/models", nil)
	req.Header.Set("Authorization", "bearer k-123") // the scheme is case-insensitive
	if resp, err := s.hc.Do(req); err != nil || resp.StatusCode != 200 {
		t.Errorf("lower-case bearer scheme: %v %v, want 200", resp, err)
	}

	// every other path is open
	for _, path := range []string{"/health", "/version", "/metrics"} {
		if r := s.get(path); r.status != 200 {
			t.Errorf("GET %s without a key: HTTP %d, want 200", path, r.status)
		}
	}
	if r := s.post("/tokenize", map[string]any{"prompt": "a b"}); r.status != 200 {
		t.Errorf("/tokenize without a key: HTTP %d, want 200: %s", r.status, r.body)
	}
}

func TestNoKeyMeansNoAuth(t *testing.T) {
	s := serve(t, cfgFor())
	if r := s.get("/v1/models"); r.status != 200 {
		t.Errorf("/v1/models with no API key set: HTTP %d, want 200", r.status)
	}
}

func TestSimpleEndpoints(t *testing.T) {
	cfg := cfgFor()
	cfg.ModelPath, cfg.MaxModelLen = "/ckpt/x", 4096
	s := serve(t, cfg)

	if r := s.get("/health"); r.status != 200 || len(r.body) != 0 {
		t.Errorf("/health: HTTP %d, body %q, want 200 and an empty body", r.status, r.body)
	}
	var v map[string]string
	r := s.get("/version")
	r.json(t, &v)
	if !reflect.DeepEqual(v, map[string]string{"version": "0.32.0"}) {
		t.Errorf("/version = %v, want {version: 0.32.0}", v)
	}
	var models struct {
		Object string `json:"object"`
		Data   []struct {
			ID, Object, Root string
			MaxModelLen      int `json:"max_model_len"`
		}
	}
	s.get("/v1/models").json(t, &models)
	if models.Object != "list" || len(models.Data) != 1 {
		t.Fatalf("/v1/models = %+v, want a list of one model", models)
	}
	if d := models.Data[0]; d.ID != "m" || d.Object != "model" || d.Root != "/ckpt/x" || d.MaxModelLen != 4096 {
		t.Errorf("model card = %+v, want m, model, /ckpt/x, 4096", d)
	}
	if r := s.get("/nope"); r.status != 404 {
		t.Errorf("unknown path: HTTP %d, want 404", r.status)
	}
	if r := s.post("/v1/models", nil); r.status != 405 {
		t.Errorf("POST /v1/models: HTTP %d, want 405", r.status)
	}
}

type tokenizeResult struct {
	Count       int     `json:"count"`
	MaxModelLen int     `json:"max_model_len"`
	Tokens      []int32 `json:"tokens"`
}

func (s *testServer) tokenize(body map[string]any) tokenizeResult {
	s.t.Helper()
	r := s.post("/tokenize", body)
	if r.status != 200 {
		s.t.Fatalf("/tokenize: HTTP %d: %s", r.status, r.body)
	}
	var out tokenizeResult
	r.json(s.t, &out)
	return out
}

func msgs(contents ...string) []map[string]string {
	var out []map[string]string
	for i, c := range contents {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		out = append(out, map[string]string{"role": role, "content": c})
	}
	return out
}

// The count of a conversation is written out by hand from the template: each
// message is "<|role|> content <|assistant|>", two tokens besides its words,
// and the generation prompt is 7 tokens with the default overhead of 9.
func TestChatTemplateOverhead(t *testing.T) {
	s := serve(t, cfgFor())
	cases := []struct {
		name string
		body map[string]any
		want int
	}{
		{"one empty message with the generation prompt", map[string]any{"messages": msgs("")}, 9},
		{"one empty message, no generation prompt", map[string]any{"messages": msgs(""), "add_generation_prompt": false}, 2},
		{"one message of three words", map[string]any{"messages": msgs("a b c")}, 12},
		{"two messages of 1 and 2 words", map[string]any{"messages": msgs("hello", "world there")}, 14},
		{"a prompt is its words", map[string]any{"prompt": "a b c d"}, 4},
		{"an empty prompt", map[string]any{"prompt": ""}, 0},
	}
	for _, tc := range cases {
		if got := s.tokenize(tc.body); got.Count != tc.want || len(got.Tokens) != tc.want || got.MaxModelLen != 32768 {
			t.Errorf("%s: count %d, %d tokens, max_model_len %d; want %d, %d, 32768", tc.name, got.Count, len(got.Tokens), got.MaxModelLen, tc.want, tc.want)
		}
	}
}

func TestTemplateOverheadIsConfigurable(t *testing.T) {
	cfg := cfgFor()
	cfg.TemplateOverhead = 20
	s := serve(t, cfg)
	if got := s.tokenize(map[string]any{"messages": msgs("a b c")}).Count; got != 23 {
		t.Errorf("count with TemplateOverhead 20 and 3 words = %d, want 23", got)
	}
}

// Token ids: 2221 is the FNV-1a 32-bit hash of "a" (0xe40c292c) modulo 100000,
// plus 1, and the others are the same computation for "b", "hello" and "world".
func TestTokenIDs(t *testing.T) {
	s := serve(t, cfgFor())
	got := s.tokenize(map[string]any{"prompt": "a b hello  world"}).Tokens
	if want := []int32{2221, 35078, 31724, 88788}; !reflect.DeepEqual(got, want) {
		t.Errorf("tokens of \"a b hello  world\" = %v, want %v", got, want)
	}
	for i, w := range []string{"a", "b", "hello", "world"} {
		if refID(w) != got[i] {
			t.Errorf("hash/fnv gives %d for %q, the double %d", refID(w), w, got[i])
		}
	}
	// the template markers are words too
	tokens := s.tokenize(map[string]any{"messages": msgs("a"), "add_generation_prompt": false}).Tokens
	if want := []int32{4637, 2221, 91566}; !reflect.DeepEqual(tokens, want) {
		t.Errorf("tokens of one message \"a\" = %v, want %v (<|user|>, a, <|assistant|>)", tokens, want)
	}
}

func TestChatPromptTokensEqualTokenizeCount(t *testing.T) {
	s := serve(t, cfgFor())
	conv := msgs("tell me a story", "once upon a time", "and then")
	count := s.tokenize(map[string]any{"messages": conv}).Count
	got := s.chat(map[string]any{"model": "m", "messages": conv, "max_tokens": 3}).Usage.PromptTokens
	// 3 messages of 2 template tokens each, 4+4+2 words, and the 7-token generation prompt
	if got != count || got != 3*2+10+7 {
		t.Errorf("prompt_tokens = %d, /tokenize count = %d, want both %d", got, count, 3*2+10+7)
	}
}

func TestOutputLength(t *testing.T) {
	s := serve(t, cfgFor())
	cases := []struct {
		name       string
		extra      map[string]any
		wantTokens int
		wantFinish string
	}{
		{"ignore_eos alone does not fix the length", map[string]any{"max_tokens": 57, "ignore_eos": true}, 24, "stop"},
		{"min_tokens at max_tokens gives exactly max_tokens", map[string]any{"max_tokens": 40, "min_tokens": 40}, 40, "length"},
		{"min_tokens with ignore_eos, as the warm-up sends them", map[string]any{"max_tokens": 57, "min_tokens": 57, "ignore_eos": true}, 57, "length"},
		{"min_tokens below max_tokens stops at the longer of it and the natural length", map[string]any{"max_tokens": 57, "min_tokens": 40, "ignore_eos": true}, 40, "stop"},
		{"the model stops after 24 tokens", map[string]any{"max_tokens": 100}, 24, "stop"},
		{"max_tokens below that is reached first", map[string]any{"max_tokens": 10}, 10, "length"},
		{"min_tokens above the natural length", map[string]any{"max_tokens": 100, "min_tokens": 30}, 30, "stop"},
		{"max_completion_tokens wins over max_tokens", map[string]any{"max_tokens": 5, "max_completion_tokens": 8, "ignore_eos": true}, 8, "length"},
		{"max_tokens equal to the natural length", map[string]any{"max_tokens": 24}, 24, "length"},
	}
	for _, tc := range cases {
		extra := tc.extra
		extra["return_token_ids"] = true
		res := s.chat(chatBody(5, extra))
		c := res.Choices[0]
		if res.Usage.CompletionTokens != tc.wantTokens || len(c.TokenIDs) != tc.wantTokens || c.FinishReason != tc.wantFinish {
			t.Errorf("%s: %d completion tokens, %d token_ids, finish %q; want %d, %d, %q",
				tc.name, res.Usage.CompletionTokens, len(c.TokenIDs), c.FinishReason, tc.wantTokens, tc.wantTokens, tc.wantFinish)
		}
		if want := res.Usage.PromptTokens + tc.wantTokens; res.Usage.TotalTokens != want {
			t.Errorf("%s: total_tokens %d, want %d", tc.name, res.Usage.TotalTokens, want)
		}
	}
	if res := s.chat(chatBody(5, map[string]any{"max_tokens": 3})); res.Choices[0].TokenIDs != nil {
		t.Errorf("token_ids sent without return_token_ids: %v", res.Choices[0].TokenIDs)
	}
}

// early-stop=<n>: the reply stops after n tokens with "stop", even under
// ignore_eos, unless min_tokens asks for more.
func TestEarlyStop(t *testing.T) {
	cfg, err := ParseModes("early-stop=10")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServedName, cfg.TokenDelay = "m", 0
	s := serve(t, cfg)
	for _, tc := range []struct {
		name       string
		extra      map[string]any
		wantTokens int
		wantFinish string
	}{
		{"no flags", map[string]any{"max_tokens": 100}, 10, "stop"},
		{"ignore_eos does not prevent it", map[string]any{"max_tokens": 100, "ignore_eos": true}, 10, "stop"},
		{"min_tokens below it changes nothing", map[string]any{"max_tokens": 100, "min_tokens": 5, "ignore_eos": true}, 10, "stop"},
		{"min_tokens above it asks for more", map[string]any{"max_tokens": 100, "min_tokens": 30, "ignore_eos": true}, 30, "stop"},
		{"min_tokens at max_tokens gives the full length", map[string]any{"max_tokens": 100, "min_tokens": 100, "ignore_eos": true}, 100, "length"},
		{"max_tokens below it is reached first", map[string]any{"max_tokens": 6, "ignore_eos": true}, 6, "length"},
	} {
		res := s.chat(chatBody(5, tc.extra))
		if got := res.Usage.CompletionTokens; got != tc.wantTokens || res.Choices[0].FinishReason != tc.wantFinish {
			t.Errorf("%s: %d tokens, finish %q; want %d, %q", tc.name, got, res.Choices[0].FinishReason, tc.wantTokens, tc.wantFinish)
		}
	}
}

// vLLM refuses min_tokens above max_tokens (SamplingParams._verify_args,
// sampling_params.py:694-701), and so does the double.
func TestMinTokensAboveMaxTokensIs400(t *testing.T) {
	s := serve(t, cfgFor())
	for name, extra := range map[string]map[string]any{
		"min above max": {"max_tokens": 10, "min_tokens": 11},
		"negative min":  {"max_tokens": 10, "min_tokens": -1},
	} {
		r := s.post("/v1/chat/completions", chatBody(3, extra))
		if r.status != 400 || !strings.Contains(string(r.body), "min_tokens must be") {
			t.Errorf("%s: HTTP %d %s, want 400 saying min_tokens must be less than or equal to max_tokens", name, r.status, r.body)
		}
	}
	if r := s.post("/v1/chat/completions", chatBody(3, map[string]any{"max_tokens": 10, "min_tokens": 10})); r.status != 200 {
		t.Errorf("min_tokens equal to max_tokens: HTTP %d, want 200", r.status)
	}
}

func TestNaturalLenIsConfigurable(t *testing.T) {
	cfg := cfgFor()
	cfg.NaturalLen = 7
	if res := serve(t, cfg).chat(chatBody(2, map[string]any{"max_tokens": 50})); res.Usage.CompletionTokens != 7 || res.Choices[0].FinishReason != "stop" {
		t.Errorf("with NaturalLen 7: %d tokens, finish %q; want 7, stop", res.Usage.CompletionTokens, res.Choices[0].FinishReason)
	}
}

func TestOverlongPromptIs400(t *testing.T) {
	cfg := cfgFor()
	cfg.MaxModelLen = 100
	s := serve(t, cfg)
	cases := []struct {
		name   string
		words  int
		max    int
		status int
	}{
		{"fits exactly: 59 prompt tokens + 41", 50, 41, 200},
		{"one token over", 50, 42, 400},
		{"the prompt alone over", 95, 1, 400},
		{"no max_tokens: the rest of the context", 50, 0, 200},
	}
	for _, tc := range cases {
		extra := map[string]any{}
		if tc.max > 0 {
			extra["max_tokens"] = tc.max
		}
		r := s.post("/v1/chat/completions", chatBody(tc.words, extra))
		if r.status != tc.status {
			t.Errorf("%s: HTTP %d, want %d: %s", tc.name, r.status, tc.status, r.body)
			continue
		}
		if tc.status == 400 {
			var e struct {
				Error map[string]any `json:"error"`
			}
			r.json(t, &e)
			if e.Error["type"] != "BadRequestError" || e.Error["code"] != float64(400) || e.Error["message"] == "" {
				t.Errorf("%s: error = %v, want type BadRequestError, code 400 and a message", tc.name, e.Error)
			}
		}
	}
}

func TestUnknownModelIs404(t *testing.T) {
	r := serve(t, cfgFor()).post("/v1/chat/completions", chatBody(1, map[string]any{"model": "other"}))
	if r.status != 404 || !strings.Contains(string(r.body), "NotFoundError") {
		t.Errorf("unknown model: HTTP %d %s, want 404 NotFoundError", r.status, r.body)
	}
}

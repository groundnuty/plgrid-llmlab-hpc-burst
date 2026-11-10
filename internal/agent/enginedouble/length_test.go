package enginedouble

import (
	"encoding/json"
	"net/http"
	"testing"
)

// Issue 353: the double's length handling is vLLM v0.32.0's, on both endpoints.
// What v0.32.0 does, in the order it does it (raw.githubusercontent.com/
// vllm-project/vllm/v0.32.0/vllm/...):
//
//  1. A limit over the whole window is refused before the prompt is read:
//     build_tok_params (entrypoints/openai/chat_completion/protocol.py:657-675,
//     completion/protocol.py:289-300) builds TokenizeParams, whose __post_init__
//     (renderers/params.py:256-268) raises "{field}={N} cannot be greater than
//     max_model_len={M}. Please request fewer output tokens." when the limit is
//     over max_model_len. The field is max_completion_tokens when a chat request
//     sets it (protocol.py:658-663), else max_tokens.
//  2. A prompt over the context is refused (params.py:515-535, _token_len_check).
//     The tokenizer is asked for max_model_len+1 tokens at most (get_encode_kwargs,
//     params.py:351-376), so the message always says "at least {M+1}".
//  3. The limit is clamped: get_max_tokens (entrypoints/serve/utils/api_utils.py:
//     188-205) returns min(M - prompt, limit), or M - prompt when there is no limit.
//     A completions null or absent limit is 16 (completion/protocol.py:63, :430-436).
//  4. A result under 1 is refused by SamplingParams (sampling_params.py:688-693,
//     "max_tokens must be at least 1, got {N}."), and min_tokens above the result
//     by :698-702, both on the clamped value.
//
// Every expected number below is a literal worked from those rules, with the
// arithmetic in the comment: M is the context (100 here), a prompt's tokens are
// its words (a completions prompt has no template) or its words plus the chat
// template's overhead (set to 9 in these tests), and the natural length of an
// answer is 24 (set below), so any limit under 24 is what ends the answer. None
// of it is read back from the double's own helpers.

const (
	lenContext  = 100 // M: the MaxModelLen these tests run with
	lenOverhead = 9   // the chat template's tokens around a message
	lenNatural  = 24  // the double's answer length when no limit cuts it
)

func lengthCfg() Config {
	cfg := cfgFor()
	cfg.MaxModelLen, cfg.TemplateOverhead, cfg.NaturalLen = lenContext, lenOverhead, lenNatural
	return cfg
}

// errorBody is the 400 a refusal answers with.
type errorBody struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error"`
}

// promptRefusal is vLLM's refusal of a prompt over the context (params.py:
// 523-534) for M = 100: the count is M+1, with "at least", whatever the
// prompt's own count.
const promptRefusal = "This model's maximum context length is 100 tokens. However, your prompt contains at least 101 input tokens, which exceeds this limit even without any output tokens. Please reduce the length of the input prompt."

// A fitting limit is served, clamped to the rest of the context, on both
// endpoints. The record's limit, the completion's token count and its finish
// reason are all the clamp's.
func TestFittingLimitIsClampedToTheRestOfTheContext(t *testing.T) {
	type tc struct {
		name   string
		words  int            // prompt words
		extra  map[string]any // the limit fields
		prompt int            // prompt tokens: the words, plus 9 for chat
		limit  int            // the limit served, from min(100 - prompt, limit) or the rest
	}
	// A completions prompt has no template: prompt tokens = words.
	completions := []tc{
		{"limit over the rest: min(100-85, 50)", 85, map[string]any{"max_tokens": 50}, 85, 15},
		{"limit one over the rest: min(15, 16)", 85, map[string]any{"max_tokens": 16}, 85, 15},
		{"limit equal to the rest: min(15, 15)", 85, map[string]any{"max_tokens": 15}, 85, 15},
		{"limit under the rest is kept: min(15, 10)", 85, map[string]any{"max_tokens": 10}, 85, 10},
		{"limit equal to the window is not refused, only clamped: min(15, 100)", 85, map[string]any{"max_tokens": 100}, 85, 15},
		{"the rest is one token: min(1, 50)", 99, map[string]any{"max_tokens": 50}, 99, 1},
		{"absent: the default 16 over the rest, min(15, 16)", 85, map[string]any{}, 85, 15},
		{"null: the default 16 over the rest, min(15, 16)", 85, map[string]any{"max_tokens": nil}, 85, 15},
		{"absent, the rest above the default: min(30, 16)", 70, map[string]any{}, 70, 16},
		{"null, the rest above the default: min(30, 16)", 70, map[string]any{"max_tokens": nil}, 70, 16},
		{"absent, the rest equal to the default: min(16, 16)", 84, map[string]any{}, 84, 16},
	}
	for _, c := range completions {
		post, events := engineRecord(t, lengthCfg())
		r := post("/v1/completions", completionBody(c.words, c.extra))
		if r.Code != http.StatusOK {
			t.Errorf("completions, %s: HTTP %d, want 200: %s", c.name, r.Code, r.Body)
			continue
		}
		var res completionResult
		if err := json.Unmarshal(r.Body.Bytes(), &res); err != nil || len(res.Choices) != 1 {
			t.Errorf("completions, %s: %v: %s", c.name, err, r.Body)
			continue
		}
		got := requestEvents(events())
		if len(got) != 1 || got[0].PromptTokens != c.prompt || got[0].MaxTokens != c.limit {
			t.Errorf("completions, %s: record %+v, want prompt %d and limit %d", c.name, got, c.prompt, c.limit)
		}
		if res.Usage.PromptTokens != c.prompt || res.Usage.CompletionTokens != c.limit || res.Choices[0].FinishReason != "length" {
			t.Errorf("completions, %s: usage %+v, finish %q; want prompt %d, %d tokens, finish length (the limit is under the natural 24)",
				c.name, res.Usage, res.Choices[0].FinishReason, c.prompt, c.limit)
		}
	}

	// A chat prompt has the template: prompt tokens = words + 9. Chat's absent
	// or null limit is no limit at all, so the rest applies, not 16.
	chat := []tc{
		{"max_tokens over the rest: min(100-85, 50)", 76, map[string]any{"max_tokens": 50}, 85, 15},
		{"max_completion_tokens over the rest: min(15, 50)", 76, map[string]any{"max_completion_tokens": 50}, 85, 15},
		{"max_completion_tokens wins over max_tokens (protocol.py:658-663, serving.py:343-347): min(15, 50), not min(15, 5)",
			76, map[string]any{"max_tokens": 5, "max_completion_tokens": 50}, 85, 15},
		{"max_completion_tokens wins over max_tokens: min(15, 5)",
			76, map[string]any{"max_tokens": 50, "max_completion_tokens": 5}, 85, 5},
		{"max_completion_tokens null falls to max_tokens: min(15, 50)",
			76, map[string]any{"max_tokens": 50, "max_completion_tokens": nil}, 85, 15},
		{"limit equal to the window is not refused, only clamped: min(15, 100)", 76, map[string]any{"max_tokens": 100}, 85, 15},
		{"no limit: the rest, 100-79 = 21, not the completions default 16", 70, map[string]any{}, 79, 21},
		{"both null: the rest, 100-79 = 21", 70, map[string]any{"max_tokens": nil, "max_completion_tokens": nil}, 79, 21},
	}
	for _, c := range chat {
		post, events := engineRecord(t, lengthCfg())
		r := post("/v1/chat/completions", chatBody(c.words, c.extra))
		if r.Code != http.StatusOK {
			t.Errorf("chat, %s: HTTP %d, want 200: %s", c.name, r.Code, r.Body)
			continue
		}
		var res chatResult
		if err := json.Unmarshal(r.Body.Bytes(), &res); err != nil || len(res.Choices) != 1 {
			t.Errorf("chat, %s: %v: %s", c.name, err, r.Body)
			continue
		}
		got := requestEvents(events())
		if len(got) != 1 || got[0].PromptTokens != c.prompt || got[0].MaxTokens != c.limit {
			t.Errorf("chat, %s: record %+v, want prompt %d and limit %d", c.name, got, c.prompt, c.limit)
		}
		if res.Usage.PromptTokens != c.prompt || res.Usage.CompletionTokens != c.limit || res.Choices[0].FinishReason != "length" {
			t.Errorf("chat, %s: usage %+v, finish %q; want prompt %d, %d tokens, finish length (the limit is under the natural 24)",
				c.name, res.Usage, res.Choices[0].FinishReason, c.prompt, c.limit)
		}
	}
}

// A clamped limit is the one served on a stream too: the same limitFor feeds
// both. 85 prompt tokens (76 words + 9), a limit of 50: min(100-85, 50) = 15
// tokens, so the stream is a role chunk, 15 content chunks (the last with
// finish_reason length) and the usage chunk.
func TestClampedLimitOnAStream(t *testing.T) {
	s := serve(t, lengthCfg())
	st := s.stream(chatBody(76, map[string]any{"max_tokens": 50, "stream_options": map[string]any{"include_usage": true}})).chunks(t)
	if len(st) != 17 { // role, 15 tokens, usage
		t.Fatalf("%d chunks, want 17 (role, 15 tokens, usage)", len(st))
	}
	if fr := st[15].Choices[0].FinishReason; fr == nil || *fr != "length" {
		t.Errorf("last token chunk finish_reason = %v, want length", fr)
	}
	if u := st[16].Usage; u == nil || u.PromptTokens != 85 || u.CompletionTokens != 15 {
		t.Errorf("usage = %+v, want 85 prompt and 15 completion tokens", u)
	}
}

// A prompt over the context is refused with vLLM's message: "at least" and
// the count M+1 = 101, whatever the prompt's own count (a prompt of 150 tokens
// is refused as "at least 101", because the tokenizer stops there,
// params.py:351-376). The limit does not matter: this prompt cannot be served
// with any.
func TestPromptOverTheContextIsRefusedAtLeast(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		body     map[string]any
		wantCode int
	}{
		// completions: tokens = words
		{"completions: 101 tokens, one over", "/v1/completions", completionBody(101, map[string]any{"max_tokens": 1}), 400},
		{"completions: 150 tokens, the message still says 101", "/v1/completions", completionBody(150, map[string]any{"max_tokens": 1}), 400},
		{"completions: 150 tokens, no limit", "/v1/completions", completionBody(150, nil), 400},
		// chat: tokens = words + 9
		{"chat: 101 tokens, one over (92 words + 9)", "/v1/chat/completions", chatBody(92, map[string]any{"max_tokens": 1}), 400},
		{"chat: 149 tokens, the message still says 101 (140 words + 9)", "/v1/chat/completions", chatBody(140, map[string]any{"max_tokens": 1}), 400},
		{"chat: 104 tokens, no limit (95 words + 9)", "/v1/chat/completions", chatBody(95, nil), 400},
	}
	s := serve(t, lengthCfg())
	for _, c := range cases {
		r := s.post(c.path, c.body)
		var e errorBody
		r.json(t, &e)
		if r.status != c.wantCode || e.Error.Type != "BadRequestError" || e.Error.Code != 400 || e.Error.Message != promptRefusal {
			t.Errorf("%s: HTTP %d, error %+v; want 400, BadRequestError and %q", c.name, r.status, e.Error, promptRefusal)
		}
	}
}

// A prompt of exactly the context passes the prompt check (params.py:515-535
// refuses only len(tokens) > max_model_len), and get_max_tokens gives it
// 100 - 100 = 0 tokens, which SamplingParams refuses (sampling_params.py:
// 688-693): a different message from the prompt's, with the clamped value 0.
// A limit of 0 is refused the same way, and a negative one with its own value:
// min(rest, -3) = -3. A limit the clamp would take under 1 is the clamp's, not
// the request's: a prompt of 100 with a limit of 5 is "got 0", not "got 5".
func TestLimitUnderOneIsRefusedWithTheClampedValue(t *testing.T) {
	cases := []struct {
		name string
		path string
		body map[string]any
		want string
	}{
		{"completions: a prompt of 100 tokens, no limit: min(0, 16) = 0", "/v1/completions", completionBody(100, nil), "max_tokens must be at least 1, got 0."},
		{"completions: a prompt of 100 tokens, a limit of 5: min(0, 5) = 0", "/v1/completions", completionBody(100, map[string]any{"max_tokens": 5}), "max_tokens must be at least 1, got 0."},
		{"completions: a limit of 0: min(30, 0) = 0", "/v1/completions", completionBody(70, map[string]any{"max_tokens": 0}), "max_tokens must be at least 1, got 0."},
		{"completions: a limit of -3: min(30, -3) = -3", "/v1/completions", completionBody(70, map[string]any{"max_tokens": -3}), "max_tokens must be at least 1, got -3."},
		{"chat: a prompt of 100 tokens (91 words + 9), no limit: the rest, 0", "/v1/chat/completions", chatBody(91, nil), "max_tokens must be at least 1, got 0."},
		{"chat: a prompt of 100 tokens, max_completion_tokens 5: min(0, 5) = 0", "/v1/chat/completions", chatBody(91, map[string]any{"max_completion_tokens": 5}), "max_tokens must be at least 1, got 0."},
		{"chat: max_tokens 0: min(21, 0) = 0", "/v1/chat/completions", chatBody(70, map[string]any{"max_tokens": 0}), "max_tokens must be at least 1, got 0."},
		{"chat: max_completion_tokens 0 wins over max_tokens 50: min(21, 0) = 0", "/v1/chat/completions", chatBody(70, map[string]any{"max_tokens": 50, "max_completion_tokens": 0}), "max_tokens must be at least 1, got 0."},
	}
	s := serve(t, lengthCfg())
	for _, c := range cases {
		r := s.post(c.path, c.body)
		var e errorBody
		r.json(t, &e)
		if r.status != 400 || e.Error.Type != "BadRequestError" || e.Error.Code != 400 || e.Error.Message != c.want {
			t.Errorf("%s: HTTP %d, error %+v; want 400, BadRequestError and %q", c.name, r.status, e.Error, c.want)
		}
	}
}

// A limit over the whole window is refused (params.py:256-268), by the name of
// the field it came in: max_completion_tokens when a chat request sets it. A
// limit equal to the window is not over it (TestFitting...: served, clamped).
// The refusal comes before the prompt is read, so a prompt over the context
// with a limit over the window gets the window's message. When both chat
// fields are set the one vLLM reads is max_completion_tokens
// (protocol.py:658-663): an over-window max_tokens is not refused beside an
// in-window max_completion_tokens, and an in-window max_tokens is no help
// beside an over-window max_completion_tokens.
func TestLimitOverTheWindowIsRefused(t *testing.T) {
	cases := []struct {
		name string
		path string
		body map[string]any
		want string // the refusal's message; "" for served
	}{
		{"completions: 101 over the window of 100", "/v1/completions", completionBody(10, map[string]any{"max_tokens": 101}),
			"max_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"completions: far over the window, a tiny prompt", "/v1/completions", completionBody(1, map[string]any{"max_tokens": 40000}),
			"max_tokens=40000 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"completions: the window's message comes before the prompt's (150 tokens)", "/v1/completions", completionBody(150, map[string]any{"max_tokens": 101}),
			"max_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"chat: max_tokens 101", "/v1/chat/completions", chatBody(10, map[string]any{"max_tokens": 101}),
			"max_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"chat: max_completion_tokens 101 is named by its own field", "/v1/chat/completions", chatBody(10, map[string]any{"max_completion_tokens": 101}),
			"max_completion_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"chat: max_completion_tokens 101 beside max_tokens 5: the field vLLM reads is max_completion_tokens", "/v1/chat/completions",
			chatBody(10, map[string]any{"max_tokens": 5, "max_completion_tokens": 101}),
			"max_completion_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"chat: the window's message comes before the prompt's (149 tokens)", "/v1/chat/completions", chatBody(140, map[string]any{"max_tokens": 101}),
			"max_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
		{"chat: max_tokens 101 beside max_completion_tokens 5: max_tokens is not read, so served", "/v1/chat/completions",
			chatBody(10, map[string]any{"max_tokens": 101, "max_completion_tokens": 5}), ""},
		{"chat: max_tokens 101 beside a null max_completion_tokens: max_tokens is read", "/v1/chat/completions",
			chatBody(10, map[string]any{"max_tokens": 101, "max_completion_tokens": nil}),
			"max_tokens=101 cannot be greater than max_model_len=100. Please request fewer output tokens."},
	}
	s := serve(t, lengthCfg())
	for _, c := range cases {
		r := s.post(c.path, c.body)
		if c.want == "" {
			if r.status != 200 {
				t.Errorf("%s: HTTP %d, want 200: %s", c.name, r.status, r.body)
			}
			continue
		}
		var e errorBody
		r.json(t, &e)
		if r.status != 400 || e.Error.Type != "BadRequestError" || e.Error.Code != 400 || e.Error.Message != c.want {
			t.Errorf("%s: HTTP %d, error %+v; want 400, BadRequestError and %q", c.name, r.status, e.Error, c.want)
		}
	}
}

// With a small context a completions request with no limit can be over the
// window by the default alone: 16 over 10 is refused, "max_tokens=16 cannot be
// greater than max_model_len=10", as vLLM does, since the null is rewritten to
// 16 before build_tok_params reads it (completion/protocol.py:430-436, :292).
// A chat request with no limit is not (its limit is "or 0").
func TestDefaultCompletionLimitOverASmallWindow(t *testing.T) {
	cfg := lengthCfg()
	cfg.MaxModelLen = 10
	s := serve(t, cfg)
	r := s.post("/v1/completions", completionBody(2, nil))
	var e errorBody
	r.json(t, &e)
	if want := "max_tokens=16 cannot be greater than max_model_len=10. Please request fewer output tokens."; r.status != 400 || e.Error.Message != want {
		t.Errorf("completions with no limit, a window of 10: HTTP %d, %q; want 400 and %q", r.status, e.Error.Message, want)
	}
	// chat: 1 word + 9 = 10 tokens fills the window of 10 exactly, and no limit
	// is "or 0", so the window does not refuse it; SamplingParams does ("got 0").
	r = s.post("/v1/chat/completions", chatBody(1, nil))
	r.json(t, &e)
	if want := "max_tokens must be at least 1, got 0."; r.status != 400 || e.Error.Message != want {
		t.Errorf("chat with no limit, a prompt filling a window of 10: HTTP %d, %q; want 400 and %q", r.status, e.Error.Message, want)
	}
}

// min_tokens is checked against the clamped limit (sampling_params.py:698-702,
// after get_max_tokens), and its message states that value. 85 prompt tokens,
// a limit of 50: the limit is 15, so min_tokens 20 is refused as
// "max_tokens=15", and min_tokens 15 gets exactly 15 tokens, finish length.
// This is the shape a warm-up of a fixed length sends (min_tokens = max_tokens).
func TestMinTokensIsCheckedAgainstTheClampedLimit(t *testing.T) {
	s := serve(t, lengthCfg())
	r := s.post("/v1/completions", completionBody(85, map[string]any{"max_tokens": 50, "min_tokens": 20}))
	var e errorBody
	r.json(t, &e)
	if want := "min_tokens must be less than or equal to max_tokens=15, got 20."; r.status != 400 || e.Error.Message != want {
		t.Errorf("min_tokens 20 over a clamped 15: HTTP %d, %q; want 400 and %q", r.status, e.Error.Message, want)
	}
	res := s.completion(completionBody(85, map[string]any{"max_tokens": 50, "min_tokens": 15}))
	if res.Usage.CompletionTokens != 15 || res.Choices[0].FinishReason != "length" {
		t.Errorf("min_tokens 15, clamped 15: %d tokens, finish %q; want 15, length", res.Usage.CompletionTokens, res.Choices[0].FinishReason)
	}
	// chat, max_completion_tokens: 76 words + 9 = 85, the same 15.
	r = s.post("/v1/chat/completions", chatBody(76, map[string]any{"max_completion_tokens": 50, "min_tokens": 20}))
	r.json(t, &e)
	if want := "min_tokens must be less than or equal to max_tokens=15, got 20."; r.status != 400 || e.Error.Message != want {
		t.Errorf("chat, min_tokens 20 over a clamped 15: HTTP %d, %q; want 400 and %q", r.status, e.Error.Message, want)
	}
}

// A request refused for its length is in the engine's log as refused, with the
// prompt it carried; its limit is 0 when it did not resolve. The record of a
// min_tokens refusal carries the clamped limit it resolved.
func TestRefusedLengthIsRecorded(t *testing.T) {
	post, events := engineRecord(t, lengthCfg())
	post("/v1/completions", completionBody(150, map[string]any{"max_tokens": 1}))  // prompt over the context
	post("/v1/completions", completionBody(10, map[string]any{"max_tokens": 101})) // limit over the window
	post("/v1/completions", completionBody(85, map[string]any{"max_tokens": 50, "min_tokens": 20}))
	got := requestEvents(events())
	if len(got) != 3 {
		t.Fatalf("%d request records, want 3: %+v", len(got), got)
	}
	for i, want := range []struct{ prompt, limit int }{{150, 0}, {10, 0}, {85, 15}} {
		if got[i].Outcome != "refused" || got[i].PromptTokens != want.prompt || got[i].MaxTokens != want.limit {
			t.Errorf("record %d: %+v, want refused, prompt %d, limit %d", i, got[i], want.prompt, want.limit)
		}
	}
}

package enginedouble

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// The engine's counters, its log and its modes, as they apply to completions.

// The engine's counters count a completions request as they count a chat one:
// tokens, finish reasons, requests running, and the instrumentator's request
// count under the route's own handler label.
func TestCompletionIsCountedInTheMetrics(t *testing.T) {
	s := serve(t, cfgFor())
	s.completion(completionBody(3, map[string]any{"max_tokens": 5, "min_tokens": 5}))   // 3 prompt, 5 generated, length
	s.streamAt("/v1/completions", completionBody(3, map[string]any{"max_tokens": 100})) // 3 prompt, 24 generated, stop
	s.post("/v1/completions", map[string]any{"model": "m"})                             // no prompt: 400
	m := s.metrics()
	for _, line := range []string{
		`vllm:num_requests_running{engine="0",model_name="m"} 0.0`,
		`vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.0`,
		`vllm:prompt_tokens_total{engine="0",model_name="m"} 6.0`,
		`vllm:generation_tokens_total{engine="0",model_name="m"} 29.0`,
		`vllm:prefix_cache_queries_total{engine="0",model_name="m"} 6.0`,
		`vllm:request_success_total{engine="0",finished_reason="length",model_name="m"} 1.0`,
		`vllm:request_success_total{engine="0",finished_reason="stop",model_name="m"} 1.0`,
		`vllm:request_success_total{engine="0",finished_reason="abort",model_name="m"} 0.0`,
		`vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 2.0`,
		`http_requests_total{handler="/v1/completions",method="POST",status="2xx"} 2.0`,
		`http_requests_total{handler="/v1/completions",method="POST",status="4xx"} 1.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing line %s\nin:\n%s", line, m)
		}
	}
	if strings.Contains(m, `handler="none"`) {
		t.Errorf("the completions route was counted as an unknown path:\n%s", m)
	}
}

// engineRecord returns a handler that keeps the engine log's events, and a
// function that serves one request to it in process.
func engineRecord(t *testing.T, cfg Config) (func(path string, body map[string]any) *httptest.ResponseRecorder, func() []LogEvent) {
	t.Helper()
	var mu sync.Mutex
	var events []LogEvent
	h := newHandler(cfg, func(ev LogEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() })
	post := func(path string, body map[string]any) *httptest.ResponseRecorder {
		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b)))
		return rec
	}
	return post, func() []LogEvent { mu.Lock(); defer mu.Unlock(); return append([]LogEvent(nil), events...) }
}

// A completions request is in the engine's log (HB_ENGINE_DOUBLE_LOG) under
// its own path, with the prompt and limit it was run with.
func TestCompletionIsInTheEngineLog(t *testing.T) {
	post, events := engineRecord(t, cfgFor())
	post("/v1/completions", completionBody(3, map[string]any{"max_tokens": 5, "min_tokens": 5}))
	post("/v1/completions", completionBody(2, map[string]any{"max_tokens": 4, "min_tokens": 4, "stream": true}))
	want := []LogEvent{
		{Event: "request", Path: "/v1/completions", PromptTokens: 3, MaxTokens: 5, Outcome: "completed", Tokens: 5},
		{Event: "request", Path: "/v1/completions", PromptTokens: 2, MaxTokens: 4, Stream: true, Outcome: "completed", Tokens: 4},
	}
	if got := events(); !reflect.DeepEqual(got, want) {
		t.Errorf("engine log = %+v, want %+v", got, want)
	}
}

func modeCfg(t *testing.T, modes string) Config {
	t.Helper()
	cfg, err := ParseModes(modes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServedName, cfg.TokenDelay = "m", 0
	return cfg
}

// The modes that change what the model says act on a completions request as
// they do on a chat one (doc.go).
func TestCompletionModes(t *testing.T) {
	longBody := func() map[string]any {
		return completionBody(5, map[string]any{"max_tokens": 10, "min_tokens": 10, "return_token_ids": true})
	}
	plain := serve(t, cfgFor()).completion(longBody()).Choices[0]

	t.Run("early-stop", func(t *testing.T) {
		res := serve(t, modeCfg(t, "early-stop=10")).completion(completionBody(5, map[string]any{"max_tokens": 100, "ignore_eos": true}))
		if res.Usage.CompletionTokens != 10 || res.Choices[0].FinishReason != "stop" {
			t.Errorf("%d tokens, finish %q; want 10, stop", res.Usage.CompletionTokens, res.Choices[0].FinishReason)
		}
	})
	t.Run("greedy-repeat", func(t *testing.T) {
		words := strings.Fields(serve(t, modeCfg(t, "greedy-repeat")).completion(longBody()).Choices[0].Text)
		if len(words) != 10 {
			t.Fatalf("%d words, want 10", len(words))
		}
		for i, w := range words {
			if w != words[0] {
				t.Errorf("word %d is %q, want the first word %q repeated", i, w, words[0])
			}
		}
	})
	t.Run("reasoning-first", func(t *testing.T) {
		// 3 tokens of reasoning, then 7: the same 10 tokens, finish length, and,
		// as the completion is one walk, the tokens of the model that does not
		// reason. There is no reasoning field on this endpoint: all of it is text.
		res := serve(t, modeCfg(t, "reasoning-first=3")).completion(longBody())
		c := res.Choices[0]
		if res.Usage.CompletionTokens != 10 || c.FinishReason != "length" || len(strings.Fields(c.Text)) != 10 {
			t.Errorf("%d tokens, finish %q, text %q; want 10, length and 10 words", res.Usage.CompletionTokens, c.FinishReason, c.Text)
		}
		if c.Text != plain.Text || !reflect.DeepEqual(c.TokenIDs, plain.TokenIDs) {
			t.Errorf("text %q ids %v differ from the model that does not reason: %q %v", c.Text, c.TokenIDs, plain.Text, plain.TokenIDs)
		}
	})
	t.Run("reasoning-forever", func(t *testing.T) {
		// the model reasons until max_tokens, where the plain one stops at 24
		res := serve(t, modeCfg(t, "reasoning-forever")).completion(completionBody(5, map[string]any{"max_tokens": 50}))
		if res.Usage.CompletionTokens != 50 || res.Choices[0].FinishReason != "length" || len(strings.Fields(res.Choices[0].Text)) != 50 {
			t.Errorf("%d tokens, finish %q, %d words; want 50, length, 50", res.Usage.CompletionTokens, res.Choices[0].FinishReason, len(strings.Fields(res.Choices[0].Text)))
		}
	})
	t.Run("tool modes change nothing", func(t *testing.T) {
		// a completions request has no tools, so these chat modes have no effect
		for _, m := range []string{"tool-malformed", "tool-args-null", "tool-args-badtype", "no-tool-call"} {
			c := serve(t, modeCfg(t, m)).completion(longBody()).Choices[0]
			if c.Text != plain.Text || c.FinishReason != "length" {
				t.Errorf("mode %s: text %q finish %q, want the plain answer", m, c.Text, c.FinishReason)
			}
		}
	})
	t.Run("reports-nothing", func(t *testing.T) {
		s := serve(t, modeCfg(t, "reports-nothing"))
		r := s.post("/v1/completions", longBody())
		if r.status != 200 || strings.Contains(string(r.body), "system_fingerprint") {
			t.Errorf("HTTP %d %s, want 200 without a system_fingerprint", r.status, r.body)
		}
		stream := s.streamAt("/v1/completions", completionBody(2, map[string]any{"max_tokens": 3, "min_tokens": 3, "stream_options": map[string]any{"include_usage": true}}))
		if strings.Contains(string(stream.raw), "system_fingerprint") {
			t.Errorf("a stream without a system_fingerprint expected: %s", stream.raw)
		}
	})
	t.Run("errors-only", func(t *testing.T) {
		s := serve(t, modeCfg(t, "errors-only"))
		for _, stream := range []bool{false, true} {
			r := s.post("/v1/completions", completionBody(2, map[string]any{"max_tokens": 2, "stream": stream}))
			if r.status != 500 || !strings.Contains(string(r.body), `"type":"InternalServerError"`) {
				t.Errorf("stream=%v: HTTP %d %s, want 500 InternalServerError", stream, r.status, r.body)
			}
		}
		if m := s.metrics(); !has(m, `http_requests_total{handler="/v1/completions",method="POST",status="5xx"} 2.0`) {
			t.Errorf("the two 500s are not counted:\n%s", m)
		}
	})
}

// A mode that makes the engine slow is observed by what it cannot do early:
// a request cannot finish before the waits it was promised, whatever the load.
// These tests assert lower bounds only.
func TestCompletionWaitsForTheTokenDelayAndSlowFirst(t *testing.T) {
	t.Run("token-delay", func(t *testing.T) {
		cfg := cfgFor()
		cfg.TokenDelay = 20 * time.Millisecond
		s := serve(t, cfg)
		start := time.Now()
		s.completion(completionBody(2, map[string]any{"max_tokens": 5, "min_tokens": 5}))
		if d := time.Since(start); d < 5*20*time.Millisecond {
			t.Errorf("5 tokens at 20 ms took %v, want at least 100ms", d)
		}
		start = time.Now()
		s.streamAt("/v1/completions", completionBody(2, map[string]any{"max_tokens": 4, "min_tokens": 4}))
		if d := time.Since(start); d < 4*20*time.Millisecond {
			t.Errorf("4 streamed tokens at 20 ms took %v, want at least 80ms", d)
		}
	})
	t.Run("slow-first", func(t *testing.T) {
		cfg := cfgFor()
		cfg.SlowFirst = 200 * time.Millisecond
		s := serve(t, cfg)
		start := time.Now()
		s.completion(completionBody(3, map[string]any{"max_tokens": 4, "min_tokens": 4}))
		if d := time.Since(start); d < 200*time.Millisecond {
			t.Errorf("the first request of a shape took %v, want at least 200ms", d)
		}
	})
}

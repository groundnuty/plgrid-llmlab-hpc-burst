package enginedouble

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// longRequest asks for n tokens exactly, so no natural stop hides a difference.
func longRequest(n int, extra map[string]any) map[string]any {
	m := map[string]any{"max_tokens": n, "ignore_eos": true, "return_token_ids": true}
	for k, v := range extra {
		m[k] = v
	}
	return chatBody(6, m)
}

func (s *testServer) tokens(body map[string]any) []int32 {
	s.t.Helper()
	return s.chat(body).Choices[0].TokenIDs
}

func TestGreedyOutputsAreIdentical(t *testing.T) {
	s := serve(t, cfgFor())
	a, b, c := s.tokens(longRequest(64, nil)), s.tokens(longRequest(64, nil)), s.tokens(longRequest(64, map[string]any{"temperature": 0}))
	if len(a) != 64 || !reflect.DeepEqual(a, b) || !reflect.DeepEqual(a, c) {
		t.Errorf("identical greedy requests gave different outputs:\n%v\n%v\n%v", a, b, c)
	}
	other := s.tokens(chatBody(7, map[string]any{"max_tokens": 64, "ignore_eos": true, "return_token_ids": true}))
	if reflect.DeepEqual(a, other) {
		t.Error("two different prompts gave the same output")
	}
	// a longer output extends a shorter one
	if long := s.tokens(longRequest(100, nil)); !reflect.DeepEqual(long[:64], a) {
		t.Error("the 100-token output does not start with the 64-token output")
	}
}

func TestSampledOutputsDiffer(t *testing.T) {
	s := serve(t, cfgFor())
	first := s.tokens(longRequest(32, map[string]any{"temperature": 0.8}))
	for i := 0; i < 4; i++ {
		if !reflect.DeepEqual(first, s.tokens(longRequest(32, map[string]any{"temperature": 0.8}))) {
			return
		}
	}
	t.Error("five sampled requests gave the same 32 tokens")
}

// The healthy output is no loop: no token repeats next to itself, not one
// token fills the output, and the walk has no period up to 512 tokens.
func TestOutputIsNotARepetition(t *testing.T) {
	s := serve(t, cfgFor())
	toks := s.tokens(longRequest(3000, nil))
	if len(toks) != 3000 {
		t.Fatalf("%d tokens, want 3000", len(toks))
	}
	distinct := map[int32]bool{}
	for i, id := range toks {
		distinct[id] = true
		if i > 0 && id == toks[i-1] {
			t.Fatalf("token %d repeats token %d (id %d)", i, i-1, id)
		}
	}
	if len(distinct) < 200 {
		t.Errorf("%d distinct tokens in 3000, want at least 200", len(distinct))
	}
	for period := 1; period <= 512; period++ {
		same := true
		for i := 0; i+period < len(toks); i++ {
			if toks[i] != toks[i+period] {
				same = false
				break
			}
		}
		if same {
			t.Fatalf("the output has period %d", period)
		}
	}
}

// greedy-diverge=40: the repeats of an identical greedy request share the
// first 40 tokens with the first and differ from index 40 on. The requests use
// ignore_eos, since a 24-token natural stop would end before index 40.
func TestGreedyDivergeAt40(t *testing.T) {
	cfg := cfgFor()
	modes, err := ParseModes("greedy-diverge=40")
	if err != nil {
		t.Fatal(err)
	}
	cfg.GreedyDiverge, cfg.GreedyDivergeAt = modes.GreedyDiverge, modes.GreedyDivergeAt
	s := serve(t, cfg)

	first := s.tokens(longRequest(60, nil))
	if len(first) != 60 {
		t.Fatalf("%d tokens, want 60", len(first))
	}
	for k := 1; k <= 3; k++ {
		rep := s.tokens(longRequest(60, nil))
		if len(rep) != 60 {
			t.Fatalf("repeat %d: %d tokens, want 60", k, len(rep))
		}
		if !reflect.DeepEqual(rep[:40], first[:40]) {
			t.Errorf("repeat %d differs within the first 40 tokens:\n%v\n%v", k, first[:40], rep[:40])
		}
		for i := 40; i < 60; i++ {
			if rep[i] == first[i] {
				t.Errorf("repeat %d has the first request's token at index %d (id %d)", k, i, rep[i])
			}
			if i > 0 && rep[i] == rep[i-1] {
				t.Errorf("repeat %d repeats a token next to itself at index %d", k, i)
			}
		}
	}
	// a different request is a different identity: its own first one is not a repeat
	a1 := s.tokens(chatBody(9, map[string]any{"max_tokens": 60, "ignore_eos": true, "return_token_ids": true}))
	a2 := s.tokens(chatBody(9, map[string]any{"max_tokens": 60, "ignore_eos": true, "return_token_ids": true}))
	if !reflect.DeepEqual(a1[:40], a2[:40]) || a1[40] == a2[40] {
		t.Error("the second request of another prompt did not diverge at 40")
	}
}

func TestGreedyDivergeAtZero(t *testing.T) {
	cfg := cfgFor()
	cfg.GreedyDiverge, cfg.GreedyDivergeAt = true, 0
	s := serve(t, cfg)
	a, b := s.tokens(longRequest(8, nil)), s.tokens(longRequest(8, nil))
	for i := range a {
		if a[i] == b[i] {
			t.Errorf("index %d is the same in the repeat", i)
		}
	}
}

func TestGreedyRepeatIsOneToken(t *testing.T) {
	cfg := cfgFor()
	cfg.GreedyRepeat = true
	s := serve(t, cfg)
	toks := s.tokens(longRequest(20, nil))
	if len(toks) != 20 {
		t.Fatalf("%d tokens, want 20", len(toks))
	}
	for i, id := range toks {
		if id != toks[0] {
			t.Fatalf("token %d is %d, token 0 is %d: want one token repeated", i, id, toks[0])
		}
	}
}

func TestLoopAbove(t *testing.T) {
	cfg := cfgFor()
	cfg.LoopAbove = 50
	s := serve(t, cfg)

	// a prompt of 9+60 tokens, above 50, without ignore_eos: a 3-token cycle
	res := s.chat(chatBody(60, map[string]any{"max_tokens": 30, "return_token_ids": true}))
	toks := res.Choices[0].TokenIDs
	if len(toks) != 30 || res.Choices[0].FinishReason != "length" {
		t.Fatalf("looping answer: %d tokens, finish %q; want 30, length", len(toks), res.Choices[0].FinishReason)
	}
	if toks[0] == toks[1] || toks[1] == toks[2] || toks[0] == toks[2] {
		t.Errorf("the cycle %v is not 3 distinct tokens", toks[:3])
	}
	for i := 3; i < 30; i++ {
		if toks[i] != toks[i-3] {
			t.Fatalf("token %d is %d, token %d is %d: not a 3-token cycle", i, toks[i], i-3, toks[i-3])
		}
	}
	// with ignore_eos the check does not trigger
	if got := s.tokens(chatBody(60, map[string]any{"max_tokens": 30, "ignore_eos": true, "return_token_ids": true})); got[3] == got[0] && got[4] == got[1] && got[5] == got[2] {
		t.Error("ignore_eos still looped")
	}
	// a short prompt answers normally
	if res := s.chat(chatBody(10, map[string]any{"max_tokens": 100})); res.Usage.CompletionTokens != 24 || res.Choices[0].FinishReason != "stop" {
		t.Errorf("short prompt: %d tokens, finish %q; want 24, stop", res.Usage.CompletionTokens, res.Choices[0].FinishReason)
	}
}

var weatherTools = []map[string]any{{
	"type":     "function",
	"function": map[string]any{"name": "get_weather", "parameters": map[string]any{"type": "object"}},
}, {
	"type":     "function",
	"function": map[string]any{"name": "other_tool"},
}}

func TestToolCall(t *testing.T) {
	s := serve(t, cfgFor())
	res := s.chat(chatBody(4, map[string]any{"tools": weatherTools, "max_tokens": 50}))
	c := res.Choices[0]
	if c.FinishReason != "tool_calls" || c.Message.Content != nil || len(c.Message.ToolCalls) != 1 {
		t.Fatalf("finish %q, content %v, %d tool calls; want tool_calls, null, 1", c.FinishReason, c.Message.Content, len(c.Message.ToolCalls))
	}
	tc := c.Message.ToolCalls[0]
	if tc.ID != "call_0" || tc.Type != "function" || tc.Function.Name != "get_weather" || tc.Function.Arguments != `{"city": "Krakow"}` {
		t.Errorf("tool call = %+v", tc)
	}
	if res.Usage.CompletionTokens < 1 {
		t.Error("a tool call has no completion tokens")
	}

	// tool_choice none: text; naming a function: that function
	none := s.chat(chatBody(4, map[string]any{"tools": weatherTools, "tool_choice": "none", "max_tokens": 50}))
	if none.Choices[0].FinishReason != "stop" || none.Choices[0].Message.Content == nil || len(none.Choices[0].Message.ToolCalls) != 0 {
		t.Errorf("tool_choice none gave %+v", none.Choices[0])
	}
	named := s.chat(chatBody(4, map[string]any{"tools": weatherTools, "tool_choice": map[string]any{"type": "function", "function": map[string]any{"name": "other_tool"}}}))
	if got := named.Choices[0].Message.ToolCalls[0].Function.Name; got != "other_tool" {
		t.Errorf("named tool_choice gave %q, want other_tool", got)
	}
}

func TestToolMalformedAndNoToolCall(t *testing.T) {
	cfg := cfgFor()
	cfg.ToolMalformed = true
	args := serve(t, cfg).chat(chatBody(4, map[string]any{"tools": weatherTools})).Choices[0].Message.ToolCalls[0].Function.Arguments
	if args != `{"city": ` || json.Valid([]byte(args)) {
		t.Errorf("malformed arguments = %q, want {\"city\":  cut short, invalid JSON", args)
	}

	cfg = cfgFor()
	cfg.NoToolCall = true
	c := serve(t, cfg).chat(chatBody(4, map[string]any{"tools": weatherTools, "max_tokens": 50})).Choices[0]
	if len(c.Message.ToolCalls) != 0 || c.Message.Content == nil || c.FinishReason != "stop" {
		t.Errorf("no-tool-call mode gave %+v", c)
	}
}

func TestReportsNothing(t *testing.T) {
	modes, err := ParseModes("reports-nothing")
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgFor()
	cfg.ReportsNothing = modes.ReportsNothing
	s := serve(t, cfg)
	if r := s.get("/version"); r.status != 404 {
		t.Errorf("/version: HTTP %d, want 404", r.status)
	}
	var models struct {
		Data []map[string]any
	}
	s.get("/v1/models").json(t, &models)
	if _, has := models.Data[0]["max_model_len"]; has {
		t.Errorf("/v1/models has max_model_len: %v", models.Data[0])
	}
	if m := string(s.get("/metrics").body); strings.Contains(m, "cache_config_info") {
		t.Error("/metrics has cache_config_info")
	}
	if res := s.chat(chatBody(2, nil)); res.SystemFingerprint != nil {
		t.Errorf("system_fingerprint = %q, want none", *res.SystemFingerprint)
	}
	// and the default engine reports all of it
	d := serve(t, cfgFor())
	if res := d.chat(chatBody(2, nil)); res.SystemFingerprint == nil || *res.SystemFingerprint != "fp-enginedouble" {
		t.Errorf("default system_fingerprint = %v, want fp-enginedouble", res.SystemFingerprint)
	}
}

func TestReportMaxModelLenContradictsTheFlag(t *testing.T) {
	cfg := cfgFor()
	cfg.MaxModelLen, cfg.ReportedMaxModelLen = 8192, 4096
	s := serve(t, cfg)
	var models struct {
		Data []struct {
			MaxModelLen int `json:"max_model_len"`
		}
	}
	s.get("/v1/models").json(t, &models)
	if models.Data[0].MaxModelLen != 4096 {
		t.Errorf("/v1/models max_model_len = %d, want the reported 4096", models.Data[0].MaxModelLen)
	}
	if got := s.tokenize(map[string]any{"prompt": "a"}).MaxModelLen; got != 4096 {
		t.Errorf("/tokenize max_model_len = %d, want 4096", got)
	}
	// but the enforced limit is the real one: 59 + 5000 fits 8192 and not 4096
	if r := s.post("/v1/chat/completions", chatBody(50, map[string]any{"max_tokens": 5000})); r.status != 200 {
		t.Errorf("a request within MaxModelLen: HTTP %d: %s", r.status, r.body)
	}
}

func TestHealthFailAndErrorsOnly(t *testing.T) {
	cfg := cfgFor()
	cfg.HealthFail = true
	if r := serve(t, cfg).get("/health"); r.status != 503 || len(r.body) != 0 {
		t.Errorf("health-fail: HTTP %d %q, want 503 and an empty body", r.status, r.body)
	}

	cfg = cfgFor()
	cfg.ErrorsOnly = true
	s := serve(t, cfg)
	r := s.post("/v1/chat/completions", chatBody(1, nil))
	var e struct {
		Error map[string]any `json:"error"`
	}
	r.json(t, &e)
	if r.status != 500 || e.Error["code"] != float64(500) || e.Error["message"] == "" {
		t.Errorf("errors-only: HTTP %d %s, want 500 with error.code 500", r.status, r.body)
	}
	if h := s.get("/health"); h.status != 200 {
		t.Errorf("errors-only /health: HTTP %d, want 200", h.status)
	}
}

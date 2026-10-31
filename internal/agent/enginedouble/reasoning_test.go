package enginedouble

import (
	"reflect"
	"strings"
	"testing"
)

// reasoningCfg is a double whose model reasons for n tokens before it answers.
func reasoningCfg(n int) Config {
	c := cfgFor()
	c.ReasoningFirst = n
	return c
}

// reasoning-first=n: n reasoning tokens in message.reasoning, then the answer
// in message.content; usage.completion_tokens counts both, as vLLM's does.
func TestReasoningFirst(t *testing.T) {
	body := func() map[string]any {
		return chatBody(5, map[string]any{"max_tokens": 100, "return_token_ids": true})
	}
	plain := serve(t, cfgFor()).chat(body()).Choices[0]
	res := serve(t, reasoningCfg(10)).chat(body())
	c := res.Choices[0]

	// 10 reasoning tokens, then the 24 tokens the model says of its own accord
	if res.Usage.CompletionTokens != 34 || len(c.TokenIDs) != 34 || c.FinishReason != "stop" {
		t.Fatalf("%d completion tokens, %d token ids, finish %q; want 34, 34, stop", res.Usage.CompletionTokens, len(c.TokenIDs), c.FinishReason)
	}
	if c.Message.Reasoning == nil || c.Message.Content == nil {
		t.Fatalf("reasoning %v, content %v: want both", c.Message.Reasoning, c.Message.Content)
	}
	reasoning, content := strings.Fields(*c.Message.Reasoning), strings.Fields(*c.Message.Content)
	if len(reasoning) != 10 || len(content) != 24 {
		t.Fatalf("%d reasoning words and %d content words, want 10 and 24", len(reasoning), len(content))
	}
	for i, w := range append(append([]string(nil), reasoning...), content...) {
		if refID(w) != c.TokenIDs[i] {
			t.Errorf("token %d: hash/fnv of %q is %d, the answer says %d", i, w, refID(w), c.TokenIDs[i])
		}
	}
	// the completion is one walk: the first 24 ids are those of the model that
	// does not reason, so a greedy check of the first tokens still agrees
	if !reflect.DeepEqual(c.TokenIDs[:24], plain.TokenIDs) {
		t.Errorf("the first 24 token ids differ from the model that does not reason:\n%v\n%v", c.TokenIDs[:24], plain.TokenIDs)
	}
	if res.Usage.TotalTokens != res.Usage.PromptTokens+34 {
		t.Errorf("total_tokens %d, want prompt %d + 34", res.Usage.TotalTokens, res.Usage.PromptTokens)
	}
}

// A model that does not reason has "reasoning": null, not a missing key, as
// vLLM writes its unset fields.
func TestNoReasoningIsNull(t *testing.T) {
	r := serve(t, cfgFor()).post("/v1/chat/completions", chatBody(3, map[string]any{"max_tokens": 4}))
	if !strings.Contains(string(r.body), `"reasoning":null`) {
		t.Errorf("a message without reasoning must carry \"reasoning\":null: %s", r.body)
	}
}

// The reasoning of a streamed reply is delta.reasoning, one chunk per token,
// and the answer's chunks follow it; a chunk has one or the other.
func TestReasoningFirstStreamed(t *testing.T) {
	res := serve(t, reasoningCfg(10)).stream(chatBody(5, map[string]any{
		"max_tokens": 100, "stream_options": map[string]any{"include_usage": true},
	}))
	chunks := res.chunks(t)
	// role, 10 reasoning, 24 content, usage
	if len(chunks) != 36 {
		t.Fatalf("%d chunks, want 36", len(chunks))
	}
	var reasoning, content strings.Builder
	for i, ch := range chunks[1:35] {
		d := ch.Choices[0].Delta
		switch {
		case i < 10 && d.Reasoning != nil && d.Content == nil:
			reasoning.WriteString(*d.Reasoning)
		case i >= 10 && d.Content != nil && d.Reasoning == nil:
			content.WriteString(*d.Content)
		default:
			t.Fatalf("chunk %d delta = %+v: want reasoning for the first 10 and content after", i+1, d)
		}
		if (ch.Choices[0].FinishReason != nil) != (i == 33) {
			t.Errorf("chunk %d finish_reason = %v: only the last token chunk carries one", i+1, ch.Choices[0].FinishReason)
		}
	}
	if got := len(strings.Fields(reasoning.String())); got != 10 {
		t.Errorf("%d reasoning words streamed, want 10", got)
	}
	if got := len(strings.Fields(content.String())); got != 24 {
		t.Errorf("%d content words streamed, want 24", got)
	}
	if u := chunks[35].Usage; u == nil || u.CompletionTokens != 34 {
		t.Errorf("usage = %+v, want 34 completion tokens: the reasoning counts", u)
	}
}

// reasoning-forever: the reply reasons until max_tokens runs out, with no
// content and the finish reason "length".
func TestReasoningForever(t *testing.T) {
	cfg := cfgFor()
	cfg.ReasoningForever = true
	s := serve(t, cfg)

	res := s.chat(chatBody(5, map[string]any{"max_tokens": 30}))
	c := res.Choices[0]
	if c.Message.Content != nil || c.Message.Reasoning == nil || len(strings.Fields(*c.Message.Reasoning)) != 30 ||
		c.FinishReason != "length" || res.Usage.CompletionTokens != 30 {
		t.Errorf("content %v, reasoning %v, finish %q, %d completion tokens; want no content, 30 reasoning words, length, 30",
			c.Message.Content, c.Message.Reasoning, c.FinishReason, res.Usage.CompletionTokens)
	}

	st := s.stream(chatBody(5, map[string]any{"max_tokens": 30, "stream_options": map[string]any{"include_usage": true}})).chunks(t)
	if len(st) != 32 { // role, 30 reasoning, usage
		t.Fatalf("%d chunks, want 32", len(st))
	}
	for i, ch := range st[1:31] {
		d := ch.Choices[0].Delta
		if d.Reasoning == nil || d.Content != nil || d.ToolCalls != nil {
			t.Fatalf("chunk %d delta = %+v, want reasoning only", i+1, d)
		}
	}
	if fr := st[30].Choices[0].FinishReason; fr == nil || *fr != "length" {
		t.Errorf("last chunk finish_reason = %v, want length", fr)
	}
	if st[31].Usage == nil || st[31].Usage.CompletionTokens != 30 {
		t.Errorf("usage = %+v, want 30 completion tokens", st[31].Usage)
	}
}

// Reasoning that outlasts max_tokens leaves no room for the answer.
func TestReasoningFirstBeyondMaxTokens(t *testing.T) {
	res := serve(t, reasoningCfg(50)).chat(chatBody(5, map[string]any{"max_tokens": 20}))
	c := res.Choices[0]
	if c.Message.Content != nil || len(strings.Fields(*c.Message.Reasoning)) != 20 || c.FinishReason != "length" || res.Usage.CompletionTokens != 20 {
		t.Errorf("content %v, reasoning %v, finish %q, %d tokens; want no content, 20 reasoning words, length, 20",
			c.Message.Content, c.Message.Reasoning, c.FinishReason, res.Usage.CompletionTokens)
	}
}

// A tool call follows the reasoning, and the reasoning counts in usage.
func TestReasoningThenToolCall(t *testing.T) {
	s := serve(t, reasoningCfg(5))
	body := func() map[string]any { return toolBody(map[string]any{"max_tokens": 50}) }
	res := s.chat(body())
	c := res.Choices[0]
	if c.FinishReason != "tool_calls" || len(c.Message.ToolCalls) != 1 || c.Message.Content != nil ||
		c.Message.Reasoning == nil || len(strings.Fields(*c.Message.Reasoning)) != 5 {
		t.Fatalf("finish %q, %d tool calls, content %v, reasoning %v; want tool_calls, 1, null, 5 words",
			c.FinishReason, len(c.Message.ToolCalls), c.Message.Content, c.Message.Reasoning)
	}
	if res.Usage.CompletionTokens != 7 { // 5 reasoning + the two tokens of {"city": "Krakow"}
		t.Errorf("%d completion tokens, want 7", res.Usage.CompletionTokens)
	}

	st := s.stream(toolBody(map[string]any{"max_tokens": 50})).chunks(t)
	var kinds []string
	for _, ch := range st[1:] {
		d := ch.Choices[0].Delta
		switch {
		case d.Reasoning != nil:
			kinds = append(kinds, "reasoning")
		case d.ToolCalls != nil:
			kinds = append(kinds, "call")
		default:
			kinds = append(kinds, "other")
		}
	}
	if got, want := strings.Join(kinds, " "), "reasoning reasoning reasoning reasoning reasoning call call call"; got != want {
		t.Errorf("delta kinds %q, want %q", got, want)
	}
}

// A tool call cut off by max_tokens carries part of its arguments and the
// finish reason "length", not "tool_calls".
func TestToolCallCutOffByMaxTokens(t *testing.T) {
	res := serve(t, cfgFor()).chat(toolBody(map[string]any{"max_tokens": 1}))
	c := res.Choices[0]
	if c.FinishReason != "length" || len(c.Message.ToolCalls) != 1 || c.Message.ToolCalls[0].Function.Arguments != `{"city":` || res.Usage.CompletionTokens != 1 {
		t.Errorf("finish %q, calls %+v, %d tokens; want length, the first piece {\"city\":, 1", c.FinishReason, c.Message.ToolCalls, res.Usage.CompletionTokens)
	}
}

// The pieces a call's arguments stream in: one per token, white space kept,
// and the pieces joined are the arguments.
func TestArgumentPieces(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{`{"city": "Krakow"}`, []string{`{"city":`, ` "Krakow"}`}},
		{`null`, []string{`null`}},
		{`{"city": `, []string{`{"city": `}}, // the trailing space stays
		{`  a  b `, []string{`  a`, `  b `}},
		{"{\"a\":\n 1}", []string{`{"a":`, "\n 1}"}},
	} {
		got := argumentPieces(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("argumentPieces(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Join(got, "") != tc.in {
			t.Errorf("the pieces of %q join to %q", tc.in, strings.Join(got, ""))
		}
		if len(got) != len(strings.Fields(tc.in)) {
			t.Errorf("%q: %d pieces for %d words", tc.in, len(got), len(strings.Fields(tc.in)))
		}
	}
}

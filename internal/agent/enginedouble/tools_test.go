package enginedouble

import (
	"encoding/json"
	"strings"
	"testing"
)

// toolBody is a chat request offering the weather tools whose user message asks
// for a call of get_weather by name.
func toolBody(extra map[string]any) map[string]any {
	m := map[string]any{
		"model":    "m",
		"messages": []map[string]string{{"role": "user", "content": "What is the weather in Krakow? Call the get_weather tool."}},
		"tools":    weatherTools,
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// The model calls a tool when the last user message names it, or when
// tool_choice forces one; offered tools and a prompt that does not ask get text
// (Astra B9: a double that called on any tools masked a prompt that never asked).
func TestToolCallNeedsThePromptToNameTheTool(t *testing.T) {
	s := serve(t, cfgFor())
	calls := func(body map[string]any) string {
		t.Helper()
		c := s.chat(body).Choices[0]
		if len(c.Message.ToolCalls) == 0 {
			if c.Message.Content == nil || c.FinishReason != "stop" {
				t.Fatalf("no call and no text: %+v", c)
			}
			return ""
		}
		return c.Message.ToolCalls[0].Function.Name
	}
	user := func(text string) map[string]any {
		return map[string]any{"model": "m", "tools": weatherTools, "max_tokens": 60,
			"messages": []map[string]string{{"role": "user", "content": text}}}
	}
	with := func(base map[string]any, k string, v any) map[string]any { base[k] = v; return base }
	namedChoice := map[string]any{"type": "function", "function": map[string]any{"name": "other_tool"}}

	for _, tc := range []struct {
		name string
		body map[string]any
		want string
	}{
		{"the prompt names the tool", toolBody(nil), "get_weather"},
		{"the prompt does not name any tool", user("Continue the list of words above."), ""},
		{"the prompt names the second tool", user("please use other_tool now"), "other_tool"},
		{"tool_choice auto, the prompt does not ask", with(user("hello there"), "tool_choice", "auto"), ""},
		{"tool_choice none beats the name", toolBody(map[string]any{"tool_choice": "none"}), ""},
		{"tool_choice required forces the first tool", with(user("hello there"), "tool_choice", "required"), "get_weather"},
		{"a named tool_choice forces that tool", with(user("hello there"), "tool_choice", namedChoice), "other_tool"},
		{"a named tool_choice beats the name in the prompt", toolBody(map[string]any{"tool_choice": namedChoice}), "other_tool"},
		{"only the last user message counts", map[string]any{"model": "m", "tools": weatherTools, "max_tokens": 60, "messages": []map[string]string{
			{"role": "user", "content": "call get_weather"}, {"role": "assistant", "content": "ok"}, {"role": "user", "content": "thanks"}}}, ""},
		{"the last user message asks, an assistant message follows", map[string]any{"model": "m", "tools": weatherTools, "max_tokens": 60, "messages": []map[string]string{
			{"role": "user", "content": "call get_weather"}, {"role": "assistant", "content": "ok"}}}, "get_weather"},
	} {
		if got := calls(tc.body); got != tc.want {
			t.Errorf("%s: called %q, want %q", tc.name, got, tc.want)
		}
	}
}

// tool-args-null and tool-args-badtype: arguments the weather tool's schema
// rejects, in the non-streamed message and in the streamed pieces.
func TestToolArgumentModes(t *testing.T) {
	for _, tc := range []struct {
		mode string
		set  func(*Config)
		want string
	}{
		{"tool-args-null", func(c *Config) { c.ToolArgsNull = true }, `null`},
		{"tool-args-badtype", func(c *Config) { c.ToolArgsBadType = true }, `{"city": 42}`},
	} {
		cfg := cfgFor()
		tc.set(&cfg)
		s := serve(t, cfg)
		c := s.chat(toolBody(nil)).Choices[0]
		if got := c.Message.ToolCalls[0].Function.Arguments; got != tc.want || c.FinishReason != "tool_calls" {
			t.Errorf("%s: arguments %q, finish %q; want %q, tool_calls", tc.mode, got, c.FinishReason, tc.want)
		}
		var args strings.Builder
		for _, ch := range s.stream(toolBody(nil)).chunks(t)[2:] { // after the role chunk and the name delta
			var calls []struct {
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			}
			if len(ch.Choices) > 0 && json.Unmarshal(ch.Choices[0].Delta.ToolCalls, &calls) == nil && len(calls) == 1 {
				args.WriteString(calls[0].Function.Arguments)
			}
		}
		if args.String() != tc.want {
			t.Errorf("%s: streamed pieces join to %q, want %q", tc.mode, args.String(), tc.want)
		}
	}
}

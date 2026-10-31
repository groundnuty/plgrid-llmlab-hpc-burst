package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// toolDelta is a streamed tool-call chunk as vLLM v0.32.0 writes it: the first
// delta of a call has its index, id, type and function name; the later ones
// only the index and a piece of the arguments (DeltaToolCall leaves unset
// fields out).
func toolDelta(index int, id, typ, name, args string) string {
	call := fmt.Sprintf(`{"index":%d`, index)
	if id != "" {
		call += fmt.Sprintf(`,"id":%q`, id)
	}
	if typ != "" {
		call += fmt.Sprintf(`,"type":%q`, typ)
	}
	fn := ""
	if name != "" {
		fn += fmt.Sprintf(`"name":%q`, name)
	}
	if args != "" {
		if fn != "" {
			fn += ","
		}
		fn += fmt.Sprintf(`"arguments":%q`, args)
	}
	if fn != "" {
		call += `,"function":{` + fn + `}`
	}
	return `{"tool_calls":[` + call + `}]}`
}

func toolTimed(t *testing.T, events ...sseEvent2) agentapi.RequestTiming {
	t.Helper()
	c := New(serveOn(t, sseHandler(events)), "key")
	return c.timed(context.Background(), warmShape{shape: agentapi.ShapeToolCall, context: 0, output: 64, tools: true})
}

func ev(payload string) sseEvent2 { return sseEvent2{payload: payload} }

// The tool-call shape assembles the call from its deltas: the name from the
// first, the arguments from the pieces, however vLLM splits them, and needs the
// stream to finish with "tool_calls". Each stream is written by hand.
func TestToolShapeAssemblesTheStreamedCall(t *testing.T) {
	role := ev(chunk(`{"role":"assistant","content":""}`, `null`))
	done := []sseEvent2{ev(usageChunk(7)), ev("[DONE]")}
	stream := func(deltas ...string) []sseEvent2 {
		out := []sseEvent2{role}
		for i, d := range deltas {
			finish := `null`
			if i == len(deltas)-1 {
				finish = `"tool_calls"`
			}
			out = append(out, ev(chunk(d, finish)))
		}
		return append(out, done...)
	}
	for _, tc := range []struct {
		name    string
		events  []sseEvent2
		wantErr string
	}{
		{"split the way vLLM splits it", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `{"city": `),
			toolDelta(0, "", "", "", `"Krakow"}`)), ""},
		{"all at once", stream(toolDelta(0, "call_1", "function", "get_weather", `{"city":"Krakow"}`)), ""},
		{"arguments in one-character pieces", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `{`), toolDelta(0, "", "", "", `"city"`), toolDelta(0, "", "", "", `:`),
			toolDelta(0, "", "", "", `"Krakow"`), toolDelta(0, "", "", "", `}`)), ""},
		{"the arguments are cut off", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `{"city": `)), "not valid JSON"},
		{"null arguments", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `null`)), "null, not a JSON object"},
		{"an empty object", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `{}`)), `miss the required "city"`},
		{"a number for the city", stream(
			toolDelta(0, "call_1", "function", "get_weather", ""),
			toolDelta(0, "", "", "", `{"city": 42}`)), "not string"},
		{"another tool", stream(toolDelta(0, "call_1", "function", "get_time", `{"city":"Krakow"}`)), `calls "get_time"`},
		{"no type", stream(toolDelta(0, "call_1", "", "get_weather", `{"city":"Krakow"}`)), `type is ""`},
		{"a second call that is bad", stream(
			toolDelta(0, "call_1", "function", "get_weather", `{"city":"Krakow"}`),
			toolDelta(1, "call_2", "function", "get_weather", `{`)), "not valid JSON"},
		{"text and no call", []sseEvent2{role, ev(chunk(`{"content":"It is sunny."}`, `"stop"`)), ev(usageChunk(5)), ev("[DONE]")}, "calls no tool"},
		{"a complete call that is cut by the limit", []sseEvent2{role,
			ev(chunk(toolDelta(0, "call_1", "function", "get_weather", `{"city":"Krakow"}`), `"length"`)), ev(usageChunk(64)), ev("[DONE]")}, "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := toolTimed(t, tc.events...)
			switch {
			case tc.wantErr == "" && got.Error != "":
				t.Errorf("timing %+v: want no error", got)
			case tc.wantErr != "" && !strings.Contains(got.Error, tc.wantErr):
				t.Errorf("timing %+v: want an error containing %q", got, tc.wantErr)
			}
			if got.TimeToFirstToken <= 0 || got.OutputTokens <= 0 {
				t.Errorf("timing %+v: want the first-token time and the output tokens filled in, error or not", got)
			}
		})
	}
}

// Each failing tool-call double fails the tool-call shape, and the shapes that
// need no call stay clean: the failure is the double's, not the harness's.
// Written by hand: each double and the part of the message it must give.
func TestToolShapeAgainstTheFailingDoubles(t *testing.T) {
	for _, tc := range []struct {
		modes, wantErr string
	}{
		{"", ""},
		{"no-tool-call", "calls no tool"},
		{"tool-malformed", "not valid JSON"},
		{"tool-args-null", "null, not a JSON object"},
		{"tool-args-badtype", "not string"},
	} {
		t.Run("modes="+tc.modes, func(t *testing.T) {
			c := double(t, tc.modes, nil)
			round := c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 1})
			var sawTool bool
			for _, q := range round.Requests {
				if q.Shape != agentapi.ShapeToolCall {
					if q.Error != "" {
						t.Errorf("%s request failed: %q", q.Shape, q.Error)
					}
					continue
				}
				sawTool = true
				if (tc.wantErr == "") != (q.Error == "") || !strings.Contains(q.Error, tc.wantErr) {
					t.Errorf("tool-call request error %q, want one containing %q", q.Error, tc.wantErr)
				}
			}
			if !sawTool {
				t.Error("no tool-call request in the round")
			}
		})
	}
}

// The tool-call shape asks for the call in words: its last user message ends
// with the instruction naming the tool, and no other shape names it. The
// healthy double calls a tool only when the prompt names it, so a prompt that
// merely offers tools would fail the shape (Astra B9).
func TestToolShapePromptAsksForTheCall(t *testing.T) {
	c, rec := doubleWith(t, "", nil, nil)
	round := c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 1})
	for _, q := range round.Requests {
		if q.Error != "" {
			t.Errorf("%s request: %q", q.Shape, q.Error)
		}
	}
	var withTools, without int
	for _, req := range rec.sent() {
		msgs := req["messages"].([]any)
		text := msgs[len(msgs)-1].(map[string]any)["content"].(string)
		_, offered := req["tools"]
		switch {
		case offered && strings.HasSuffix(text, " Call the get_weather tool for the city of Krakow."):
			withTools++
			if req["min_tokens"] != nil || req["ignore_eos"] != nil {
				t.Errorf("the tool-call request sent min_tokens %v, ignore_eos %v: it ends with its call, not at a length", req["min_tokens"], req["ignore_eos"])
			}
		case !offered && !strings.Contains(text, "get_weather"):
			without++
		default:
			t.Errorf("request with tools %v ends %q", offered, text[max(0, len(text)-60):])
		}
	}
	if withTools != 1 || without != 5 { // 4 fixed shapes + 1 concurrent, then the tool request
		t.Errorf("%d tool-call requests and %d others, want 1 and 5", withTools, without)
	}
}

// With a reasoning parser the tool-call shape asks for its output plus the
// reasoning budget (provisional): a model that reasons for 500 tokens before
// its call passes with the option and fails without it.
func TestToolShapeReasoningBudget(t *testing.T) {
	req := agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 1}
	toolTiming := func(c *Client) agentapi.RequestTiming {
		for _, q := range c.WarmUpRound(context.Background(), 1, req).Requests {
			if q.Shape == agentapi.ShapeToolCall {
				return q
			}
		}
		t.Fatal("no tool-call request")
		return agentapi.RequestTiming{}
	}
	on, rec := doubleWith(t, "reasoning-first=500", nil, nil, WithReasoning(true))
	if q := toolTiming(on); q.Error != "" || q.OutputTokens != 502 { // 500 reasoning + the two tokens of the call
		t.Errorf("reasoning on: %+v, want no error and 502 output tokens", q)
	}
	if got := rec.maxTokensOf(true); len(got) != 1 || got[0] != int(shapes.OutputTypical)+4096 {
		t.Errorf("max_tokens of the tool-call request = %v, want [%d]", got, shapes.OutputTypical+4096)
	}
	off, _ := doubleWith(t, "reasoning-first=500", nil, nil)
	if q := toolTiming(off); q.Error == "" {
		t.Errorf("reasoning off: %+v, want an error: the 64 tokens go to reasoning", q)
	}
}

// Qualification's tool-call checks (the smoke test, and the behaviour check
// with tool calls on) fail against the doubles whose calls are invalid, and the
// checks that need no call pass. null and {"city": 42} used to pass.
func TestToolCallChecksAgainstTheFailingDoubles(t *testing.T) {
	for _, tc := range []struct {
		modes, wantErr string
	}{
		{"tool-malformed", "not valid JSON"},
		{"tool-args-null", "null, not a JSON object"},
		{"tool-args-badtype", `"city" is a number, not string`},
		{"no-tool-call", "calls no tool"},
	} {
		t.Run(tc.modes, func(t *testing.T) {
			c := double(t, tc.modes, nil)
			res := c.Qualify(context.Background(), agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true})
			for _, name := range []agentapi.CheckName{agentapi.CheckToolCall, agentapi.CheckBehaviour} {
				if o := outcomeOf(t, res, name); o.Passed || !strings.Contains(o.Message, tc.wantErr) {
					t.Errorf("%s: passed %v, %q; want a failure containing %q", name, o.Passed, o.Message, tc.wantErr)
				}
			}
			for _, name := range []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat, agentapi.CheckCorruption} {
				if o := outcomeOf(t, res, name); !o.Passed {
					t.Errorf("%s failed too: %q", name, o.Message)
				}
			}
		})
	}
}

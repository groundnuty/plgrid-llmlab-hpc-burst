package engine

import (
	"context"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Real vLLM can send its finish chunk with a non-empty delta: Qwen3's capture
// before-stream-length.sse line 7 is "delta":{"reasoning":","} beside
// "finish_reason":"length" (issue #125, item 10, from Astra's review of the
// #94 spike, confirmed by the science agent on 1 Nov 2026). The finish chunk's
// delta is ordinary content or reasoning: here it is a one-chunk reply's only
// token, so it is the first token, and without it the stream would carry none.
func TestFinishChunkDeltaIsTheFirstToken(t *testing.T) {
	role := ev(chunk(`{"role":"assistant","content":""}`, `null`))
	for _, tc := range []struct{ name, delta string }{
		{"content", `{"content":"word"}`},
		{"reasoning, as in the Qwen3 capture", `{"reasoning":","}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(serveOn(t, sseHandler([]sseEvent2{role, ev(chunk(tc.delta, `"length"`)), ev(usageChunk(1)), ev("[DONE]")})), "key")
			got, _ := c.timedCall(context.Background(), warmShape{shape: agentapi.ShapeShort, context: 0, output: 1})
			if got.Error != "" || got.TimeToFirstToken <= 0 || got.OutputTokens != 1 {
				t.Errorf("timing %+v: want no error, a first-token time and 1 output token", got)
			}
		})
	}
}

// A tool call whose last piece of arguments arrives in the finish chunk is
// whole: the finish chunk's delta goes into the call like any other. Without
// it the arguments would end at {"city": and not parse.
func TestFinishChunkDeltaCompletesTheToolCall(t *testing.T) {
	role := ev(chunk(`{"role":"assistant","content":""}`, `null`))
	got, answer := toolTimed(t, role,
		ev(chunk(toolDelta(0, "call_1", "function", "get_weather", `{"city": `), `null`)),
		ev(chunk(toolDelta(0, "", "", "", `"Krakow"}`), `"tool_calls"`)),
		ev(usageChunk(9)), ev("[DONE]"))
	if answer != nil || got.Error != "" {
		t.Errorf("timing %+v, answer %v: want a whole, valid call", got, answer)
	}
}

// The engine double sends the finish reason on the chunk of the last token, so
// every warm-up and check against it reads a non-empty finish delta; this pins
// it, so a double that split the finish into its own empty chunk could not
// drop that coverage unseen.
func TestDoubleSendsANonEmptyFinishDelta(t *testing.T) {
	c, rec := doubleWith(t, "", nil, nil)
	round := c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 1})
	for _, q := range round.Requests {
		if q.Error != "" {
			t.Errorf("%s: %q", q.Shape, q.Error)
		}
	}
	if n := rec.finishDeltas(); n == 0 {
		t.Errorf("no stream from the double had a finish chunk with a non-empty delta")
	}
}

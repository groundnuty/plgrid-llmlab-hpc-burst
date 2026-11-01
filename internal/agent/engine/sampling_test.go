package engine

import (
	"context"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Every request the agent sends the engine sets every sampling parameter its
// checks rely on: temperature 0, top_p 1, repetition_penalty 1,
// presence_penalty 0 and frequency_penalty 0, so a model's generation_config
// (Qwen2.5's repetition_penalty 1.1, #94 finding 6) changes no check
// (science-agent rulings, 1 Nov 2026, issue #125). Verification, every
// qualification check and every warm-up shape are sent here and recorded.
func TestEveryRequestSetsTheSamplingParameters(t *testing.T) {
	c, rec := doubleWith(t, "", nil, nil)
	if v := c.Verify(context.Background()); v.Error != "" {
		t.Fatalf("verify: %s", v.Error)
	}
	c.Qualify(context.Background(), agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true})
	c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 2})
	sent := rec.sent()
	// verify's chat, chat, tool call, corruption x3, behaviour; warm-up 4 + tool call + 2 concurrent
	if len(sent) < 14 {
		t.Fatalf("%d chat requests recorded, want at least 14", len(sent))
	}
	want := map[string]float64{"temperature": 0, "top_p": 1, "repetition_penalty": 1, "presence_penalty": 0, "frequency_penalty": 0}
	for i, req := range sent {
		for k, v := range want {
			if got, ok := req[k].(float64); !ok || got != v {
				t.Errorf("request %d (stream %v, tools %v): %s = %v, want %v", i, req["stream"], req["tools"] != nil, k, req[k], v)
			}
		}
	}
}

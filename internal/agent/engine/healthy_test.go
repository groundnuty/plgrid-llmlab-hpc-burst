package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The positive control: the healthy double passes every check and warms up
// with no failed request, with reasoning off and with a model that reasons for
// 50 tokens before it answers. The failing doubles of the other tests fail
// because of what they do, not because a check always fails.
func TestHealthyEngineQualifiesAndWarmsUp(t *testing.T) {
	for _, tc := range []struct {
		name, modes string
		opts        []Option
		// the tokens of the tool-call warm-up request: the two tokens of the
		// call, after the reasoning if any
		toolTokens int32
	}{
		{"reasoning off", "", nil, 2},
		{"reasoning on, reasoning-first=50", "reasoning-first=50", []Option{WithReasoning(true)}, 52},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := double(t, tc.modes, nil, tc.opts...)

			res := c.Qualify(ctx, agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true}, testGreedy)
			for _, name := range []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat, agentapi.CheckToolCall, agentapi.CheckCorruption, agentapi.CheckBehaviour} {
				want := ""
				if name == agentapi.CheckBehaviour && tc.opts != nil {
					want = "ran at a context of 200 tokens" // a reasoning parser is declared: the context is named
				}
				if o := outcomeOf(t, res, name); !o.Passed || o.Message != want {
					t.Errorf("%s: %+v, want passed with message %q", name, o, want)
				}
			}
			if res.Fingerprint != "fp-enginedouble" || res.KVCapacityTokens != 1<<20 || len(res.ServedModelNames) != 1 || res.ServedModelNames[0] != "m" {
				t.Errorf("fingerprint %q, KV capacity %d, names %q", res.Fingerprint, res.KVCapacityTokens, res.ServedModelNames)
			}

			round := c.WarmUpRound(ctx, 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 2}, testGreedy)
			var order []string
			for _, q := range round.Requests {
				order = append(order, string(q.Shape))
				want := shapes.OutputTypical
				switch q.Shape {
				case agentapi.ShapeLongOutput:
					want = shapes.OutputLongest
				case agentapi.ShapeToolCall:
					want = tc.toolTokens
				}
				if q.Error != "" || q.OutputTokens != want || q.TimeToFirstToken <= 0 || q.Total < q.TimeToFirstToken {
					t.Errorf("%s request: %+v, want no error and %d output tokens", q.Shape, q, want)
				}
			}
			if got, want := strings.Join(order, " "), "short typical longest longOutput toolCall concurrent concurrent"; got != want {
				t.Errorf("shapes %q, want %q", got, want)
			}
		})
	}
}

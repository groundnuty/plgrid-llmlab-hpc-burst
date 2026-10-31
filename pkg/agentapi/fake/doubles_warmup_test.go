package fake_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

type timing struct {
	shape        agentapi.Shape
	first, total time.Duration
}

func timingsOf(round agentapi.WarmUpRound) []timing {
	var out []timing
	for _, r := range round.Requests {
		out = append(out, timing{r.Shape, time.Duration(r.TimeToFirstToken), time.Duration(r.Total)})
	}
	return out
}

func warmUpRounds(t *testing.T, round func(int32, agentapi.WarmUpRequest) agentapi.WarmUpRound, steps int) agentapi.WarmUpResult {
	t.Helper()
	r := newRig(t, fake.Engine{WarmUpRound: round})
	r.set(run(instance(warmUpChecks(agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 3}))))
	r.steps(1 + steps) // the worker starts; then one round per Step
	return *checksOf(t, r).WarmUp
}

func TestWarmUpSlowFirstRoundIsSlowOnTheFirstRequestOfEachShapeInRound1Only(t *testing.T) {
	w := warmUpRounds(t, fake.WarmUpSlowFirstRound, 2)
	eq(t, "rounds", len(w.Rounds), 2)

	const (
		fast      = 100 * time.Millisecond
		fastTotal = 2 * time.Second
		slow      = 5 * time.Second
		slowTotal = 20 * time.Second
	)
	eq(t, "round 1", timingsOf(w.Rounds[0]), []timing{
		{"short", slow, slowTotal}, {"typical", slow, slowTotal}, {"longest", slow, slowTotal},
		{"longOutput", slow, slowTotal}, {"toolCall", slow, slowTotal},
		{"concurrent", slow, slowTotal}, {"concurrent", fast, fastTotal}, {"concurrent", fast, fastTotal},
	})
	eq(t, "round 2", timingsOf(w.Rounds[1]), []timing{
		{"short", fast, fastTotal}, {"typical", fast, fastTotal}, {"longest", fast, fastTotal},
		{"longOutput", fast, fastTotal}, {"toolCall", fast, fastTotal},
		{"concurrent", fast, fastTotal}, {"concurrent", fast, fastTotal}, {"concurrent", fast, fastTotal},
	})
	// The controller's rule, by hand: round 2 is no shape's more than 10% slower
	// than round 1, so warm-up has settled in two rounds.
	for k, r2 := range w.Rounds[1].Requests {
		r1 := w.Rounds[0].Requests[k]
		if r2.TimeToFirstToken*10 > r1.TimeToFirstToken*11 || r2.Total*10 > r1.Total*11 {
			t.Errorf("request %d (%s) got slower from round 1 to round 2", k, r2.Shape)
		}
	}
	// Output sizes are DefaultWarmUpRound's.
	eq(t, "longOutput tokens", w.Rounds[0].Requests[3].OutputTokens, int32(1024))
	eq(t, "typical tokens", w.Rounds[0].Requests[1].OutputTokens, int32(256))
}

func TestWarmUpNeverSettlesGrowsEveryRoundByAQuarterForFiveRounds(t *testing.T) {
	w := warmUpRounds(t, fake.WarmUpNeverSettles, agentapi.MaxWarmUpRounds+2)
	eq(t, "rounds", len(w.Rounds), 5)
	eq(t, "Done after five rounds", w.Done, true)

	// 100 ms and 2 s, times 5/4 per round: worked out by hand, exact in ns.
	wantFirst := []time.Duration{
		100 * time.Millisecond, 125 * time.Millisecond, 156250 * time.Microsecond,
		195312500 * time.Nanosecond, 244140625 * time.Nanosecond,
	}
	wantTotal := []time.Duration{
		2 * time.Second, 2500 * time.Millisecond, 3125 * time.Millisecond,
		3906250 * time.Microsecond, 4882812500 * time.Nanosecond,
	}
	for n, round := range w.Rounds {
		eq(t, fmt.Sprintf("round %d number", n+1), round.Round, int32(n+1))
		eq(t, fmt.Sprintf("round %d request count", n+1), len(round.Requests), 8)
		for k, r := range round.Requests {
			eq(t, fmt.Sprintf("round %d request %d TimeToFirstToken", n+1, k), time.Duration(r.TimeToFirstToken), wantFirst[n])
			eq(t, fmt.Sprintf("round %d request %d Total", n+1, k), time.Duration(r.Total), wantTotal[n])
			if n > 0 {
				prev := w.Rounds[n-1].Requests[k]
				// More than 10% slower than the round before, to the first
				// token and in all.
				if r.TimeToFirstToken*10 <= prev.TimeToFirstToken*11 || r.Total*10 <= prev.Total*11 {
					t.Errorf("round %d request %d is not more than 10%% slower than round %d", n+1, k, n)
				}
			}
		}
	}
	eq(t, "round 1 is the default round", timingsOf(w.Rounds[0])[0], timing{"short", 100 * time.Millisecond, 2 * time.Second})
}

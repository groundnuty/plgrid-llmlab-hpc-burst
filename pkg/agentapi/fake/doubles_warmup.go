package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Cold timings of WarmUpSlowFirstRound, against DefaultWarmUpRound's 100 ms
// to the first token and 2 s in all.
const (
	coldTimeToFirstToken = 5 * time.Second
	coldTotal            = 20 * time.Second
)

// WarmUpSlowFirstRound is an engine that is slow on the first request of each
// shape in round 1 (cold caches: 5 s to the first token and 20 s in all) and
// as DefaultWarmUpRound from round 2 on. Of the concurrent requests only the
// first is slow. The controller finds it settled at round 2, because no
// shape is slower than in the round before.
func WarmUpSlowFirstRound(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	round := DefaultWarmUpRound(n, req)
	if n != 1 {
		return round
	}
	seen := map[agentapi.Shape]bool{}
	for k, r := range round.Requests {
		if seen[r.Shape] {
			continue
		}
		seen[r.Shape] = true
		round.Requests[k].TimeToFirstToken = agentapi.Duration(coldTimeToFirstToken)
		round.Requests[k].Total = agentapi.Duration(coldTotal)
	}
	return round
}

// WarmUpNeverSettles is an engine whose latency never settles: in round n every
// request takes 25% longer than in round n-1, to the first token and in all,
// from DefaultWarmUpRound's 100 ms and 2 s in round 1. That is more than the
// 10% the controller tolerates, in every round, so it fails after five. The
// times are exact in nanoseconds up to round 5.
func WarmUpNeverSettles(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	round := DefaultWarmUpRound(n, req)
	for k := int32(1); k < n; k++ {
		for i := range round.Requests {
			r := &round.Requests[i]
			r.TimeToFirstToken = r.TimeToFirstToken / 4 * 5
			r.Total = r.Total / 4 * 5
		}
	}
	return round
}

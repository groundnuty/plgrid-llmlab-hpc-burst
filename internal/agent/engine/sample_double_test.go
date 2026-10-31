package engine

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// sampleFromTheDouble runs requests through the engine double and returns the
// Client's sample of its /metrics. Three requests of three tokens and one of one
// token, each with the same two-word prompt (11 tokens with the 9-token
// template), at the double's TokenDelay of 30 ms.
func sampleFromTheDouble(t *testing.T, modes string) agentapi.Health {
	t.Helper()
	c := double(t, modes, func(cfg *enginedouble.Config) { cfg.TokenDelay = 30 * time.Millisecond })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	chat := func(extra map[string]any) {
		body := map[string]any{"model": "m", "messages": []map[string]string{{"role": "user", "content": "hello world"}}}
		for k, v := range extra {
			body[k] = v
		}
		if _, err := c.do(ctx, http.MethodPost, "/v1/chat/completions", body); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		chat(map[string]any{"max_tokens": 3, "min_tokens": 3})
	}
	chat(map[string]any{"max_tokens": 1})
	h, err := c.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Issue 110, the engine double behind the real Client. Present: each field is
// the double's value. The expected numbers are worked out from the requests
// above, not read from the Sample: 4 requests x 11 prompt tokens = 44 queried
// tokens (a prompt under one 16-token block never hits the cache, so 0 hits);
// 4 finished requests, the three of 3 tokens each observing 0.03 s and the
// 1-token one observing 0 (vLLM's rule), so a sum of 0.09; the double never
// preempts.
func TestSampleFromTheDoubleGivesEachValue(t *testing.T) {
	h := sampleFromTheDouble(t, "")
	if h.Preemptions == nil || *h.Preemptions != 0 {
		t.Errorf("preemptions %v, want a known 0", h.Preemptions)
	}
	if h.PrefixCacheQueriedTokens == nil || *h.PrefixCacheQueriedTokens != 44 || h.PrefixCacheHitTokens == nil || *h.PrefixCacheHitTokens != 0 {
		t.Errorf("prefix cache queried %v hit %v, want 44 and a known 0", h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens)
	}
	if h.TimePerOutputTokenCount == nil || *h.TimePerOutputTokenCount != 4 {
		t.Errorf("time per output token count %v, want 4", h.TimePerOutputTokenCount)
	}
	if h.TimePerOutputTokenSecondsSum == nil || math.Abs(*h.TimePerOutputTokenSecondsSum-0.09) > 1e-9 {
		t.Errorf("time per output token sum %v, want 0.09", h.TimePerOutputTokenSecondsSum)
	}
}

// Omitted: an engine whose /metrics lacks the prefix-cache lines gives those
// two nil, an engine without the histogram gives the pair nil, and the other
// readings stay known (HBEP-8, Test plan).
func TestSampleFromTheDoubleThatOmitsLinesGivesUnknownNotZero(t *testing.T) {
	h := sampleFromTheDouble(t, "no-prefix-cache-metrics")
	if h.PrefixCacheQueriedTokens != nil || h.PrefixCacheHitTokens != nil {
		t.Errorf("prefix cache queried %v hit %v, want both nil", h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens)
	}
	if h.Preemptions == nil || h.TimePerOutputTokenCount == nil || *h.TimePerOutputTokenCount != 4 || h.TimePerOutputTokenSecondsSum == nil || h.RunningRequests == nil {
		t.Errorf("the other series are not all present: %+v", h)
	}

	h = sampleFromTheDouble(t, "no-tpot")
	if h.TimePerOutputTokenSecondsSum != nil || h.TimePerOutputTokenCount != nil {
		t.Errorf("without the histogram: sum %v count %v, want both nil", h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount)
	}
	if h.PrefixCacheQueriedTokens == nil || *h.PrefixCacheQueriedTokens != 44 || h.Preemptions == nil {
		t.Errorf("the other series are not all present: %+v", h)
	}
}

// Half of the pair omitted: the other half is on the wire and valid, and both
// are nil (`@hpc-burst-science-agent`, 31 Oct 2026).
func TestSampleFromTheDoubleThatOmitsHalfTheTimePerOutputTokenPairGivesBothNil(t *testing.T) {
	for _, modes := range []string{"no-tpot-sum", "no-tpot-count"} {
		h := sampleFromTheDouble(t, modes)
		if h.TimePerOutputTokenSecondsSum != nil || h.TimePerOutputTokenCount != nil {
			t.Errorf("%s: sum %v count %v, want both nil", modes, h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount)
		}
		if h.PrefixCacheQueriedTokens == nil || *h.PrefixCacheQueriedTokens != 44 {
			t.Errorf("%s: the prefix-cache readings are not known: %+v", modes, h)
		}
	}
}

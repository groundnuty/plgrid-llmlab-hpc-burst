package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// perEngine writes one series of the metric for each value, with engine labels
// 0, 1, ... in the form of vLLM's scrape. A value is written as text so that a
// test can give NaN or +Inf. An empty value writes no series for that engine.
func perEngine(metric string, vals ...string) string {
	var b strings.Builder
	for i, v := range vals {
		if v == "" {
			continue
		}
		fmt.Fprintf(&b, "%s{engine=\"%d\",model_name=\"m\"} %s\n", metric, i, v)
	}
	return b.String()
}

const (
	tpotSumName   = "vllm:request_time_per_output_token_seconds_sum"
	tpotCountName = "vllm:request_time_per_output_token_seconds_count"
)

// HBEP-8: an unknown reading is never 0, and a partial total is not the whole.
// With two engines, one holding a good value (a 0, the case where dropping the
// bad series gives a known 0) and the other a value that is not a finite number,
// every counter is unknown, whichever engine is the bad one
// (`@hpc-burst-science-agent`, 31 Oct 2026; Astra B1, PR 115).
func TestSampleACounterWithANonFiniteEngineIsUnknown(t *testing.T) {
	for _, c := range []struct {
		metric string
		get    func(agentapi.Health) *int64
	}{
		{"vllm:num_requests_running", func(h agentapi.Health) *int64 { return h.RunningRequests }},
		{"vllm:num_requests_waiting", func(h agentapi.Health) *int64 { return h.WaitingRequests }},
		{"vllm:generation_tokens_total", func(h agentapi.Health) *int64 { return h.GeneratedTokens }},
		{"vllm:num_preemptions_total", func(h agentapi.Health) *int64 { return h.Preemptions }},
		{"vllm:prefix_cache_queries_total", func(h agentapi.Health) *int64 { return h.PrefixCacheQueriedTokens }},
		{"vllm:prefix_cache_hits_total", func(h agentapi.Health) *int64 { return h.PrefixCacheHitTokens }},
	} {
		for _, bad := range []string{"NaN", "+Inf", "-Inf"} {
			for _, vals := range [][]string{{"0.0", bad}, {bad, "0.0"}} {
				name := fmt.Sprintf("%s %v", c.metric, vals)
				if got := c.get(sampleOf(t, perEngine(c.metric, vals...))); got != nil {
					t.Errorf("%s: %d, want nil (unknown): a known total from some engines is not the total", name, *got)
				}
			}
		}
		// The positive control: the same two engines, both good, are known.
		if got := c.get(sampleOf(t, perEngine(c.metric, "0.0", "0.0"))); got == nil || *got != 0 {
			t.Errorf("%s with two good engines: %v, want a known 0", c.metric, got)
		}
	}
}

// The KV cache's use is the fullest engine's, and unknown when any engine's
// series is not finite: a maximum over the engines that could be read may be
// lower than the real one.
func TestSampleAKVGaugeWithANonFiniteEngineIsUnknown(t *testing.T) {
	for _, vals := range [][]string{{"0.25", "NaN", "0.5"}, {"NaN", "0.5"}, {"0.5", "+Inf"}, {"0.5", "-Inf"}} {
		if got := sampleOf(t, perEngine("vllm:kv_cache_usage_perc", vals...)).KVCacheUsage; got != nil {
			t.Errorf("KV gauges %v: %v, want nil (unknown)", vals, *got)
		}
	}
}

// Counts are summed across the engine label and the KV cache's use, a fraction,
// takes the largest (`@hpc-burst-science-agent`, 31 Oct 2026). The two engines
// are at 0.4 and 0.7 in both orders, so a first, last, sum or mean would not
// give 0.7.
func TestSampleSumsTheCountsAcrossEnginesAndTakesTheLargestKV(t *testing.T) {
	for _, kv := range [][]string{{"0.4", "0.7"}, {"0.7", "0.4"}} {
		text := perEngine("vllm:num_requests_running", "2.0", "3.0") +
			perEngine("vllm:num_requests_waiting", "7.0", "1.0") +
			perEngine("vllm:generation_tokens_total", "100.0", "200.0") +
			perEngine("vllm:kv_cache_usage_perc", kv...)
		h := sampleOf(t, text)
		if h.RunningRequests == nil || *h.RunningRequests != 5 || h.WaitingRequests == nil || *h.WaitingRequests != 8 ||
			h.GeneratedTokens == nil || *h.GeneratedTokens != 300 {
			t.Errorf("running %v waiting %v generated %v, want 5 8 300", h.RunningRequests, h.WaitingRequests, h.GeneratedTokens)
		}
		if h.KVCacheUsage == nil || *h.KVCacheUsage != 0.7 {
			t.Errorf("KV gauges %v: %v, want 0.7", kv, h.KVCacheUsage)
		}
	}
}

// The sum and the count of the time per output token are usable only together:
// whatever is wrong with either half, in any engine's series, both are nil
// (`@hpc-burst-science-agent`, 31 Oct 2026). Engine 1 is the one that is wrong.
func TestSampleTheTimePerOutputTokenPairIsBothOrNeither(t *testing.T) {
	good := perEngine(tpotSumName, "1.5", "2.25") + perEngine(tpotCountName, "30.0", "45.0")
	if h := sampleOf(t, good); h.TimePerOutputTokenSecondsSum == nil || *h.TimePerOutputTokenSecondsSum != 3.75 ||
		h.TimePerOutputTokenCount == nil || *h.TimePerOutputTokenCount != 75 {
		t.Fatalf("two good engines: %+v, want 3.75 and 75", h)
	}
	for name, text := range map[string]string{
		"engine 1's sum is NaN, its count is valid":  perEngine(tpotSumName, "1.5", "NaN") + perEngine(tpotCountName, "30.0", "45.0"),
		"engine 1's sum is +Inf":                     perEngine(tpotSumName, "1.5", "+Inf") + perEngine(tpotCountName, "30.0", "45.0"),
		"engine 1's count is NaN, its sum is valid":  perEngine(tpotSumName, "1.5", "2.25") + perEngine(tpotCountName, "30.0", "NaN"),
		"engine 1 has a sum and no count":            perEngine(tpotSumName, "1.5", "2.25") + perEngine(tpotCountName, "30.0"),
		"engine 1 has a count and no sum":            perEngine(tpotSumName, "1.5") + perEngine(tpotCountName, "30.0", "45.0"),
		"no sum at all":                              perEngine(tpotCountName, "30.0", "45.0"),
		"no count at all":                            perEngine(tpotSumName, "1.5", "2.25"),
		"the count is above the integer range":       perEngine(tpotSumName, "1.5") + perEngine(tpotCountName, "1e19"),
		"the sums add up past the largest float":     perEngine(tpotSumName, "1.7e308", "1.7e308") + perEngine(tpotCountName, "4.0", "4.0"),
		"the counts add up past the integer range":   perEngine(tpotSumName, "1.5", "2.25") + perEngine(tpotCountName, "9e18", "9e18"),
		"the sum is NaN and no engine has the count": perEngine(tpotSumName, "NaN"),
	} {
		h := sampleOf(t, text)
		if h.TimePerOutputTokenSecondsSum != nil || h.TimePerOutputTokenCount != nil {
			t.Errorf("%s: sum %v, count %v, want both nil", name, h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount)
		}
	}
}

// A response count that is not finite makes the whole map unknown, not just its
// status: a map without "5xx" reads as no server errors (agentapi.ServerErrors
// is 0), and Health cannot say that a key is missing. A series with no status
// label is not counted, so a bad value there changes nothing.
func TestSampleAResponseCountThatIsNotFiniteMakesTheMapUnknown(t *testing.T) {
	const ok = `http_requests_total{handler="/v1/chat/completions",method="POST",status="2xx"} 9.0
http_requests_total{handler="/v1/models",method="GET",status="2xx"} 1.0
http_requests_total{handler="/v1/chat/completions",method="POST",status="5xx"} 3.0
`
	h := sampleOf(t, ok)
	if len(h.ResponsesByStatus) != 2 || h.ResponsesByStatus["2xx"] != 10 || h.ResponsesByStatus["5xx"] != 3 {
		t.Fatalf("responses by status %v, want 2xx 10 (two handlers added) and 5xx 3", h.ResponsesByStatus)
	}
	if h := sampleOf(t, ok+"http_requests_total{handler=\"/x\",method=\"GET\"} NaN\n"); len(h.ResponsesByStatus) != 2 {
		t.Errorf("a series without a status changed the map: %v", h.ResponsesByStatus)
	}
	for _, bad := range []string{"NaN", "+Inf", "-Inf"} {
		text := ok + `http_requests_total{handler="/v1/chat/completions",method="POST",status="4xx"} ` + bad + "\n"
		if h := sampleOf(t, text); h.ResponsesByStatus != nil {
			t.Errorf("a %s count for 4xx: %v, want a nil map (unknown), not a map without 4xx", bad, h.ResponsesByStatus)
		}
	}
	// A status whose total does not fit an int64 also leaves the map unknown.
	over := ok + `http_requests_total{handler="/a",method="POST",status="4xx"} 9e18
http_requests_total{handler="/b",method="POST",status="4xx"} 9e18
`
	if h := sampleOf(t, over); h.ResponsesByStatus != nil {
		t.Errorf("a 4xx total of 1.8e19: %v, want a nil map", h.ResponsesByStatus)
	}
}

// A counter's total is checked before it becomes an int64: not finite, or out of
// the integer range, is unknown, never a wrapped number (Astra nit, PR 115). The
// values in range are exact.
func TestSampleACounterOutOfTheIntegerRangeIsUnknown(t *testing.T) {
	const metric = "vllm:num_preemptions_total"
	for name, vals := range map[string][]string{
		"two finite values that add up to +Inf": {"1.7e308", "1.7e308"},
		"two values that each fit and do not":   {"9e18", "9e18"},
		"one value above the range":             {"9.3e18"},
		"one value below the range":             {"-9.3e18"},
		"2^63 itself":                           {"9223372036854775808"},
	} {
		if got := sampleOf(t, perEngine(metric, vals...)).Preemptions; got != nil {
			t.Errorf("%s: %d, want nil", name, *got)
		}
	}
	for want, vals := range map[int64][]string{
		9000000000000000000: {"9e18"},
		9007199254740992:    {"9007199254740992"}, // 2^53
		0:                   {"0.0"},
	} {
		if got := sampleOf(t, perEngine(metric, vals...)).Preemptions; got == nil || *got != want {
			t.Errorf("%v: %v, want %d", vals, got, want)
		}
	}
}

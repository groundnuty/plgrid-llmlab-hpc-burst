package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The hand-written vLLM v0.32.0 scrape the tests below read (its header says
// where each line comes from).
const fixturePath = "testdata/vllm-0.32.0-metrics.txt"

func readFixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// sampleOf serves text as an engine's /metrics and returns what Sample makes of
// it.
func sampleOf(t *testing.T, text string) agentapi.Health {
	t.Helper()
	sock := serveOn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(text))
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, err := New(sock, "key").Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// without drops every line that starts with one of the prefixes, comments
// included, as an engine that does not export the metric would.
func without(text string, prefixes ...string) string {
	var keep []string
lines:
	for _, l := range strings.Split(text, "\n") {
		for _, p := range prefixes {
			if strings.HasPrefix(l, p) || strings.HasPrefix(l, "# HELP "+p) || strings.HasPrefix(l, "# TYPE "+p) {
				continue lines
			}
		}
		keep = append(keep, l)
	}
	return strings.Join(keep, "\n")
}

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }

// counters is what the fixture says about the five fields, written by hand from
// the fixture's lines (never from the parser's output).
type counters struct {
	preemptions, queried, hit, tpotCount *int64
	tpotSum                              *float64
}

func countersOf(h agentapi.Health) counters {
	return counters{h.Preemptions, h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens, h.TimePerOutputTokenCount, h.TimePerOutputTokenSecondsSum}
}

func fixtureCounters() counters {
	return counters{preemptions: i64(7), queried: i64(3310655), hit: i64(2647024), tpotCount: i64(250), tpotSum: f64(12.5)}
}

func (c counters) String() string {
	i := func(p *int64) string {
		if p == nil {
			return "nil"
		}
		return fmt.Sprint(*p)
	}
	s := "nil"
	if c.tpotSum != nil {
		s = fmt.Sprint(*c.tpotSum)
	}
	return fmt.Sprintf("{preemptions %s, queried %s, hit %s, tpotCount %s, tpotSum %s}", i(c.preemptions), i(c.queried), i(c.hit), i(c.tpotCount), s)
}

func sameCounters(a, b counters) bool { return a.String() == b.String() }

// Each of the five fields is the value of its own line in the scrape: the
// fixture has a distinct value for each, and neighbours that a careless match
// would pick up (the time-to-first-token histogram, whose _sum is 99.5, the
// time-per-output-token buckets, whose +Inf bucket equals the count, and
// prompt_tokens_total, which equals the prefix-cache queries).
func TestSampleReadsTheEngineCountersOfHBEP8(t *testing.T) {
	h := sampleOf(t, readFixture(t))
	if got, want := countersOf(h), fixtureCounters(); !sameCounters(got, want) {
		t.Errorf("counters %v, want %v", got, want)
	}
	// The gauges and counters read before still read.
	if h.RunningRequests == nil || *h.RunningRequests != 2 || h.WaitingRequests == nil || *h.WaitingRequests != 5 ||
		h.GeneratedTokens == nil || *h.GeneratedTokens != 27072 || h.KVCacheUsage == nil || *h.KVCacheUsage != 0.75 {
		t.Errorf("running %v waiting %v generated %v kv %v, want 2 5 27072 0.75", h.RunningRequests, h.WaitingRequests, h.GeneratedTokens, h.KVCacheUsage)
	}
	if h.ResponsesByStatus["2xx"] != 245 || h.ResponsesByStatus["5xx"] != 3 {
		t.Errorf("responses by status %v, want 2xx 245 and 5xx 3", h.ResponsesByStatus)
	}
}

// A metric the scrape lacks leaves its own field nil and no other: unknown, not
// zero. Each case drops one metric's lines (the histogram's _sum and _count
// separately, then the whole histogram).
func TestSampleLeavesAMissingMetricUnknownAndTheOthersKnown(t *testing.T) {
	const tpot = "vllm:request_time_per_output_token_seconds"
	for _, c := range []struct {
		name  string
		drop  []string
		nilIt func(*counters)
	}{
		{"preemptions", []string{"vllm:num_preemptions_total"}, func(c *counters) { c.preemptions = nil }},
		{"prefix-cache queries", []string{"vllm:prefix_cache_queries_total"}, func(c *counters) { c.queried = nil }},
		{"prefix-cache hits", []string{"vllm:prefix_cache_hits_total"}, func(c *counters) { c.hit = nil }},
		{"time per output token, sum only", []string{tpot + "_sum"}, func(c *counters) { c.tpotSum = nil }},
		{"time per output token, count only", []string{tpot + "_count"}, func(c *counters) { c.tpotCount = nil }},
		{"time per output token, whole histogram", []string{tpot}, func(c *counters) { c.tpotSum, c.tpotCount = nil, nil }},
	} {
		t.Run(c.name, func(t *testing.T) {
			text := without(readFixture(t), c.drop...)
			for _, p := range c.drop {
				if strings.Contains(text, "\n"+p+"{") {
					t.Fatalf("the double still exports %s", p)
				}
			}
			want := fixtureCounters()
			c.nilIt(&want)
			if got := countersOf(sampleOf(t, text)); !sameCounters(got, want) {
				t.Errorf("counters %v, want %v", got, want)
			}
		})
	}
}

// HBEP-8, Test plan: an engine double that omits the prefix-cache counter lines
// gives those two fields unknown, and the other series are present.
func TestSampleOfAnEngineThatOmitsThePrefixCacheLines(t *testing.T) {
	h := sampleOf(t, without(readFixture(t), "vllm:prefix_cache_queries_total", "vllm:prefix_cache_hits_total"))
	want := fixtureCounters()
	want.queried, want.hit = nil, nil
	if got := countersOf(h); !sameCounters(got, want) {
		t.Errorf("counters %v, want %v", got, want)
	}
	if h.RunningRequests == nil || h.GeneratedTokens == nil || h.KVCacheUsage == nil {
		t.Errorf("the other series are not present: %+v", h)
	}
}

// A real vLLM v0.32.0 with prefix caching off still exports both counters, at
// 0, because it registers them unconditionally (loggers.py, lines 661-679): a
// known zero, which is not what an engine that omits the lines gives.
func TestSampleOfAnEngineWithPrefixCachingOffReadsZeros(t *testing.T) {
	const off = `# TYPE vllm:prefix_cache_queries_total counter
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 0.0
# TYPE vllm:prefix_cache_hits_total counter
vllm:prefix_cache_hits_total{engine="0",model_name="m"} 0.0
`
	h := sampleOf(t, off)
	if h.PrefixCacheQueriedTokens == nil || *h.PrefixCacheQueriedTokens != 0 || h.PrefixCacheHitTokens == nil || *h.PrefixCacheHitTokens != 0 {
		t.Errorf("queried %v hit %v, want two pointers to zero", h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens)
	}
	// And the lines that scrape lacks stay unknown, not zero.
	if h.Preemptions != nil || h.TimePerOutputTokenSecondsSum != nil || h.TimePerOutputTokenCount != nil || h.RunningRequests != nil {
		t.Errorf("a metric the scrape lacks was filled: %+v", h)
	}
}

// A data-parallel deployment exports one series per engine; the Instance's
// counters are their sum, as for the generated tokens.
func TestSampleAddsTheSeriesOfEveryEngine(t *testing.T) {
	const dp = `vllm:num_preemptions_total{engine="0",model_name="m"} 3.0
vllm:num_preemptions_total{engine="1",model_name="m"} 4.0
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 1000.0
vllm:prefix_cache_queries_total{engine="1",model_name="m"} 2000.0
vllm:prefix_cache_hits_total{engine="0",model_name="m"} 100.0
vllm:prefix_cache_hits_total{engine="1",model_name="m"} 200.0
vllm:request_time_per_output_token_seconds_sum{engine="0",model_name="m"} 1.5
vllm:request_time_per_output_token_seconds_sum{engine="1",model_name="m"} 2.25
vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 30.0
vllm:request_time_per_output_token_seconds_count{engine="1",model_name="m"} 45.0
`
	want := counters{preemptions: i64(7), queried: i64(3000), hit: i64(300), tpotCount: i64(75), tpotSum: f64(3.75)}
	if got := countersOf(sampleOf(t, dp)); !sameCounters(got, want) {
		t.Errorf("counters %v, want %v", got, want)
	}
}

// A value that is not a number the report can carry (NaN, an infinity) is
// unknown. A float64 NaN cannot be encoded in JSON, so one such line would
// otherwise fail the whole report.
func TestSampleTreatsANonFiniteValueAsUnknown(t *testing.T) {
	const bad = `vllm:request_time_per_output_token_seconds_sum{engine="0",model_name="m"} NaN
vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 12.0
vllm:num_preemptions_total{engine="0",model_name="m"} +Inf
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 40.0
`
	want := counters{queried: i64(40), tpotCount: i64(12)}
	if got := countersOf(sampleOf(t, bad)); !sameCounters(got, want) {
		t.Errorf("counters %v, want %v", got, want)
	}
}

// The real vLLM 0.32.0 capture of spike S2 phase B (a GH200 worker) holds three
// of the four: its exported names carry _total, and its zero preemption counter
// is a known zero. It holds no time-per-output-token histogram, so that pair is
// unknown here, and only the hand-written fixture covers it.
func TestSampleOfTheRealCapture(t *testing.T) {
	b, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	h := sampleOf(t, string(b))
	want := counters{preemptions: i64(0), queried: i64(3310655), hit: i64(2647024)}
	if got := countersOf(h); !sameCounters(got, want) {
		t.Errorf("counters %v, want %v", got, want)
	}
}

// Health's float64 fields are KVCacheUsage and TimePerOutputTokenSecondsSum
// (checked against pkg/agentapi/types.go; every other number in Health is an
// integer or a time). JSON has no NaN or infinity: a float64 field holding one
// makes json.Marshal fail for the whole report, so the agent could not report
// at all. A non-finite KV gauge is unknown (nil) and the report encodes with the
// other readings intact. The fixture's own KV gauge is replaced by hand-written
// lines; the encoding is the real one the agent's reports go through.
func TestSampleTreatsANonFiniteKVGaugeAsUnknownAndTheReportEncodes(t *testing.T) {
	for _, bad := range []string{"NaN", "+Inf", "-Inf"} {
		t.Run(bad, func(t *testing.T) {
			text := without(readFixture(t), "vllm:kv_cache_usage_perc") +
				`vllm:kv_cache_usage_perc{engine="0",model_name="m"} ` + bad + "\n"
			h := sampleOf(t, text)
			if h.KVCacheUsage != nil {
				t.Errorf("KVCacheUsage = %v, want nil (unknown)", *h.KVCacheUsage)
			}
			if got, want := countersOf(h), fixtureCounters(); !sameCounters(got, want) {
				t.Errorf("counters %v, want %v", got, want)
			}
			if h.RunningRequests == nil || *h.RunningRequests != 2 {
				t.Errorf("running %v, want 2: the other readings must survive", h.RunningRequests)
			}
			rep := agentapi.Report{Instance: &agentapi.InstanceReport{Workers: []agentapi.WorkerReport{{Name: "worker-0", Health: &h}}}}
			b, err := json.Marshal(rep)
			if err != nil {
				t.Fatalf("the report does not encode: %v", err)
			}
			if strings.Contains(string(b), "kvCacheUsage") {
				t.Errorf("the report carries kvCacheUsage for an unknown gauge: %s", b)
			}
		})
	}
}

// With two engines, a non-finite series is left out and the other still counts:
// the fullest finite engine's use, not NaN (math.Max would give NaN).
func TestSampleKVGaugeIsTheFullestFiniteEngine(t *testing.T) {
	h := sampleOf(t, `vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.25
vllm:kv_cache_usage_perc{engine="1",model_name="m"} NaN
vllm:kv_cache_usage_perc{engine="2",model_name="m"} 0.5
`)
	if h.KVCacheUsage == nil || *h.KVCacheUsage != 0.5 {
		t.Errorf("KVCacheUsage = %v, want 0.5", h.KVCacheUsage)
	}
}

// A response count that is not finite is left out, not converted to an integer.
func TestSampleLeavesANonFiniteResponseCountOut(t *testing.T) {
	h := sampleOf(t, `http_requests_total{handler="/v1/chat/completions",method="POST",status="2xx"} 9.0
http_requests_total{handler="/v1/chat/completions",method="POST",status="5xx"} NaN
`)
	if len(h.ResponsesByStatus) != 1 || h.ResponsesByStatus["2xx"] != 9 {
		t.Errorf("responses by status %v, want only 2xx 9", h.ResponsesByStatus)
	}
}

// Two finite series can add up past the largest float64; the total is then
// +Inf, which the report cannot encode, so the sum is unknown.
func TestSampleTreatsAnOverflowingSumAsUnknown(t *testing.T) {
	h := sampleOf(t, `vllm:request_time_per_output_token_seconds_sum{engine="0",model_name="m"} 1.7e308
vllm:request_time_per_output_token_seconds_sum{engine="1",model_name="m"} 1.7e308
vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 4.0
`)
	if h.TimePerOutputTokenSecondsSum != nil {
		t.Errorf("sum = %v, want nil (unknown)", *h.TimePerOutputTokenSecondsSum)
	}
	if h.TimePerOutputTokenCount == nil || *h.TimePerOutputTokenCount != 4 {
		t.Errorf("count = %v, want 4", h.TimePerOutputTokenCount)
	}
	if _, err := json.Marshal(h); err != nil {
		t.Errorf("the sample does not encode: %v", err)
	}
}

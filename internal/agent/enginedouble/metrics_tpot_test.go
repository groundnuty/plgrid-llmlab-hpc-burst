package enginedouble

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// valueOf returns the value of the exposition line that starts with prefix, and
// whether there is one.
func valueOf(t *testing.T, metrics, prefix string) (float64, bool) {
	t.Helper()
	for _, l := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(l, prefix) {
			f := strings.Fields(l)
			v, err := strconv.ParseFloat(f[len(f)-1], 64)
			if err != nil {
				t.Fatalf("line %q: %v", l, err)
			}
			return v, true
		}
	}
	return 0, false
}

// hasLinesOf reports whether any line, comments included, starts with prefix.
func hasLinesOf(metrics, prefix string) bool {
	for _, l := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(l, prefix) || strings.HasPrefix(l, "# HELP "+prefix) || strings.HasPrefix(l, "# TYPE "+prefix) {
			return true
		}
	}
	return false
}

const (
	tpotName       = "vllm:request_time_per_output_token_seconds"
	queriesName    = "vllm:prefix_cache_queries_total"
	hitsName       = "vllm:prefix_cache_hits_total"
	preemptionName = "vllm:num_preemptions_total"
)

// The histogram follows vLLM's observation: one per finished request, the mean
// time per output token after the first, which for this double is TokenDelay,
// and 0 for a request of one token (vllm/v1/metrics/stats.py, lines 607-612).
// Three requests of three tokens at 30 ms and one request of one token give a
// count of 4 and a sum of 3 x 0.03 s. The expected lines are written by hand: 30
// ms falls in the 0.05 s bucket (above 0.025), and 0 in the first.
func TestTPOTHistogramHoldsOneObservationPerFinishedRequest(t *testing.T) {
	cfg := cfgFor()
	cfg.TokenDelay = 30 * time.Millisecond
	s := serve(t, cfg)
	for range 3 {
		s.chat(chatBody(2, map[string]any{"max_tokens": 3, "min_tokens": 3}))
	}
	s.chat(chatBody(2, map[string]any{"max_tokens": 1}))
	m := s.metrics()
	for _, line := range []string{
		`# TYPE vllm:request_time_per_output_token_seconds histogram`,
		`vllm:request_time_per_output_token_seconds_bucket{engine="0",le="0.01",model_name="m"} 1.0`,
		`vllm:request_time_per_output_token_seconds_bucket{engine="0",le="0.025",model_name="m"} 1.0`,
		`vllm:request_time_per_output_token_seconds_bucket{engine="0",le="0.05",model_name="m"} 4.0`,
		`vllm:request_time_per_output_token_seconds_bucket{engine="0",le="80.0",model_name="m"} 4.0`,
		`vllm:request_time_per_output_token_seconds_bucket{engine="0",le="+Inf",model_name="m"} 4.0`,
		`vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 4.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing line %s\nin:\n%s", line, m)
		}
	}
	sum, ok := valueOf(t, m, tpotName+"_sum{")
	if !ok || math.Abs(sum-0.09) > 1e-9 {
		t.Errorf("_sum = %v (present %v), want 0.09", sum, ok)
	}
	if n := strings.Count(m, tpotName+"_bucket{"); n != 20 {
		t.Errorf("%d bucket lines, want vLLM's 19 bounds and +Inf", n)
	}
}

func TestTPOTHistogramStartsAtZero(t *testing.T) {
	m := serve(t, cfgFor()).metrics()
	for _, line := range []string{
		`vllm:request_time_per_output_token_seconds_count{engine="0",model_name="m"} 0.0`,
		`vllm:request_time_per_output_token_seconds_sum{engine="0",model_name="m"} 0.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing line %s\nin:\n%s", line, m)
		}
	}
}

// Each mode leaves out exactly its lines, and the rest of the exposition stays.
func TestMetricsModesLeaveOutTheirLines(t *testing.T) {
	always := []string{
		"vllm:num_requests_running", "vllm:kv_cache_usage_perc", "vllm:generation_tokens_total", preemptionName,
	}
	for _, c := range []struct {
		modes     string
		queries   bool // the prefix-cache lines are present
		histogram bool // the buckets are present
		count     bool
		sum       bool
	}{
		{"", true, true, true, true},
		{"no-prefix-cache-metrics", false, true, true, true},
		{"no-tpot", true, false, false, false},
		{"no-tpot-sum", true, true, true, false},
		{"no-tpot-count", true, true, false, true},
	} {
		cfg, err := ParseModes(c.modes)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ServedName, cfg.TokenDelay = "m", 0
		m := serve(t, cfg).metrics()
		for name, want := range map[string]bool{
			queriesName: c.queries, hitsName: c.queries,
			tpotName + "_bucket": c.histogram, tpotName + "_count": c.count, tpotName + "_sum": c.sum,
		} {
			if got := hasLinesOf(m, name); got != want {
				t.Errorf("modes %q: lines of %s present = %v, want %v\n%s", c.modes, name, got, want, m)
			}
		}
		for _, name := range always {
			if !hasLinesOf(m, name) {
				t.Errorf("modes %q: the exposition lost %s", c.modes, name)
			}
		}
	}
}

// A real vLLM with prefix caching off still exports both counters, at 0; the
// double's PrefixCaching=false does the same, which is not the omitting mode.
func TestPrefixCachingOffStillExportsTheCountersAtZero(t *testing.T) {
	cfg := cfgFor()
	cfg.PrefixCaching = false
	s := serve(t, cfg)
	s.chat(chatBody(40, map[string]any{"max_tokens": 2}))
	m := s.metrics()
	if hits, ok := valueOf(t, m, hitsName+"{"); !ok || hits != 0 {
		t.Errorf("hits = %v (present %v), want a line at 0", hits, ok)
	}
	if q, ok := valueOf(t, m, queriesName+"{"); !ok || q != 49 {
		t.Errorf("queries = %v (present %v), want 49 (9 template tokens + 40 words)", q, ok)
	}
}

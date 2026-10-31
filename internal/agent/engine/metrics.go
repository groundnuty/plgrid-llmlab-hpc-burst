package engine

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Metric names, from vLLM 0.32.0 (the capture in spikes/s2b/results and
// vllm/entrypoints/serve/instrumentator/metrics.py).
//
// The last four groups are HBEP-8's engine gauges (Metrics, Engine gauges per
// Instance), defined in vllm/v1/metrics/loggers.py at v0.32.0. Every name there
// is created without a suffix; prometheus_client adds the suffix the text
// carries (prometheus_client v0.18.0, the oldest vLLM accepts
// (requirements/common.txt, line 21), prometheus_client/metrics.py: a Counter
// exposes its sample as "_total", line 314; a Histogram exposes "_bucket",
// "_count" and "_sum", lines 650-653). The capture of a real vLLM 0.32.0 shows
// the "_total" form for the three counters below.
//   - vllm:num_preemptions: a Counter, name on line 739 (lines 738-745), raised
//     by the number of requests preempted in each iteration (line 1259).
//   - vllm:prefix_cache_queries and vllm:prefix_cache_hits: Counters in tokens,
//     names on lines 662 and 673 (lines 661-679), created whether or not
//     prefix caching is on and raised from the scheduler's stats at every
//     iteration (lines 1167-1172), so an engine with prefix caching off still
//     exports both, at 0.
//   - vllm:request_time_per_output_token_seconds: a Histogram, name on line 951
//     (lines 950-958), observed once per finished request with
//     FinishedRequestStats.mean_time_per_output_token (lines 1324-1326), which
//     is the decode time over the output tokens after the first and is 0 for a
//     request of one token (vllm/v1/metrics/stats.py, lines 607-612). So its
//     "_count" is the finished requests and its "_sum" the sum of their means.
//     Only the "_sum" and "_count" are read: no bucket, no quantile.
const (
	metricRunning     = "vllm:num_requests_running"
	metricWaiting     = "vllm:num_requests_waiting"
	metricGenerated   = "vllm:generation_tokens_total"
	metricKVUsage     = "vllm:kv_cache_usage_perc"
	metricCacheConfig = "vllm:cache_config_info"
	metricHTTP        = "http_requests_total"

	metricPreemptions   = "vllm:num_preemptions_total"
	metricPrefixQueries = "vllm:prefix_cache_queries_total"
	metricPrefixHits    = "vllm:prefix_cache_hits_total"
	metricTPOTSum       = "vllm:request_time_per_output_token_seconds_sum"
	metricTPOTCount     = "vllm:request_time_per_output_token_seconds_count"
)

// sample is one line of the Prometheus text format.
type sample struct {
	labels map[string]string
	value  float64
}

// metrics is a scrape, by metric name.
type metrics map[string][]sample

func (c *Client) metrics(ctx context.Context) (metrics, error) {
	b, err := c.do(ctx, http.MethodGet, "/metrics", nil)
	if err != nil {
		return nil, err
	}
	return parseMetrics(string(b))
}

// finite reports whether v is a number a report can carry. A series whose value
// is NaN, +Inf or -Inf is unknown and is left out of every reading below: JSON
// has no NaN or infinity, so a float64 field holding one makes json.Marshal fail
// for the whole report, and the agent could not report at all; math.Max with a
// NaN is NaN; and an integer converted from one is meaningless.
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// sum adds the values of every series of the metric (one per engine in a data-
// parallel deployment); found is false when the scrape has none that is finite.
func (m metrics) sum(name string) (total float64, found bool) {
	for _, s := range m[name] {
		if !finite(s.value) {
			continue
		}
		total += s.value
		found = true
	}
	return total, found
}

// max is the largest finite value among the series of the metric; found is false
// when the scrape has none.
func (m metrics) max(name string) (top float64, found bool) {
	for _, s := range m[name] {
		if !finite(s.value) {
			continue
		}
		if !found || s.value > top {
			top = s.value
		}
		found = true
	}
	return top, found
}

// Sample reads the engine's metrics into a health sample: the running and
// waiting requests, the generated-token counter, the KV cache's use, the
// preemption counter, the prefix-cache queried and hit tokens, the sum and count
// of the time per output token and the responses by status (HBEP-7, Reports:
// for health; HBEP-8, Engine gauges per Instance). The agent sets the time and
// the probe's result.
func (c *Client) Sample(ctx context.Context) (agentapi.Health, error) {
	m, err := c.metrics(ctx)
	if err != nil {
		return agentapi.Health{}, err
	}
	// A gauge the scrape does not hold stays nil, which means unknown, never
	// zero (agentapi.Health).
	var h agentapi.Health
	if v, ok := m.sum(metricRunning); ok {
		h.RunningRequests = agentapi.Int64(int64(v))
	}
	if v, ok := m.sum(metricWaiting); ok {
		h.WaitingRequests = agentapi.Int64(int64(v))
	}
	if v, ok := m.sum(metricGenerated); ok {
		h.GeneratedTokens = agentapi.Int64(int64(v))
	}
	// The four counters HBEP-8 adds. Each is read from its own exact name, so a
	// line the engine does not export leaves its field nil: an engine double,
	// or a vLLM that omits the prefix-cache lines, is unknown, not 0. A real
	// vLLM with prefix caching off exports both prefix-cache counters at 0,
	// which is a pointer to 0 here.
	h.Preemptions = m.counter(metricPreemptions)
	h.PrefixCacheQueriedTokens = m.counter(metricPrefixQueries)
	h.PrefixCacheHitTokens = m.counter(metricPrefixHits)
	// The sum of finite series can itself overflow to +Inf, which JSON cannot
	// carry either.
	if v, ok := m.sum(metricTPOTSum); ok && finite(v) {
		h.TimePerOutputTokenSecondsSum = agentapi.Float64(v)
	}
	h.TimePerOutputTokenCount = m.counter(metricTPOTCount)
	// The KV cache's use is the fullest engine's. A series that is not finite
	// is unknown, not the maximum: KVCacheUsage is a float64, and a NaN in it
	// would fail the whole report's JSON encoding.
	if kv, ok := m.max(metricKVUsage); ok {
		h.KVCacheUsage = agentapi.Float64(kv)
	}
	// The status label is taken verbatim (agentapi.Health.ResponsesByStatus).
	// vLLM v0.32.0 groups it into classes: serve/instrumentator/metrics.py,
	// lines 64-74, builds its Instrumentator without
	// should_group_status_codes, whose default is True in the versions vLLM
	// accepts (requirements/common.txt, line 23: >= 8.0.0;
	// prometheus-fastapi-instrumentator v8.0.0 and v8.1.0,
	// src/prometheus_fastapi_instrumentator/instrumentation.py, line 39), so
	// the keys are "2xx", "4xx" and "5xx".
	for _, s := range m[metricHTTP] {
		st := s.labels["status"]
		if st == "" || !finite(s.value) {
			continue
		}
		if h.ResponsesByStatus == nil {
			h.ResponsesByStatus = map[string]int64{}
		}
		h.ResponsesByStatus[st] += int64(s.value)
	}
	return h, nil
}

// counter is sum as a Health pointer: nil when the scrape has no such series.
func (m metrics) counter(name string) *int64 {
	v, ok := m.sum(name)
	if !ok {
		return nil
	}
	return agentapi.Int64(int64(v))
}

// cacheConfig returns the labels of vllm:cache_config_info, or nil when the
// engine reports none.
func (m metrics) cacheConfig() map[string]string {
	ss := m[metricCacheConfig]
	if len(ss) == 0 {
		return nil
	}
	return ss[0].labels
}

// kvCapacity is the KV capacity the engine reported at start-up, in tokens:
// cache_config_info's kv_cache_size_tokens, else num_gpu_blocks times
// block_size; 0 when neither is reported.
func kvCapacity(cfg map[string]string) int64 {
	if n, err := strconv.ParseInt(cfg["kv_cache_size_tokens"], 10, 64); err == nil && n > 0 {
		return n
	}
	blocks, err1 := strconv.ParseInt(cfg["num_gpu_blocks"], 10, 64)
	size, err2 := strconv.ParseInt(cfg["block_size"], 10, 64)
	if err1 == nil && err2 == nil && blocks > 0 && size > 0 {
		return blocks * size
	}
	return 0
}

// cacheSettings maps cache_config_info's labels to the engine flags they
// resolve, by flag name (HBEP-3's table). Only these are reported from it,
// and each value verbatim, as the engine gives it: the controller compares a
// declared input such as kv-cache-dtype verbatim (`@hpc-burst-science-agent`,
// 31 Oct 2026). In vLLM v0.32.0 the labels are CacheConfig's fields as text:
// vllm/v1/metrics/loggers.py, lines 1114-1136 and 1355, sets
// vllm:cache_config_info from CacheConfig.metrics_info(), which is
// {key: str(value)} over the config's fields (vllm/config/cache.py, lines
// 322-325). cache_dtype keeps the string it was given, since its validator
// returns it unchanged (cache.py, lines 350-368), and is "auto" when the
// launch omits --kv-cache-dtype (cache.py, line 122). So the engine, not the
// agent, supplies that default, and the agent reports "auto" for it.
var cacheSettings = map[string]string{
	"cache_dtype":            "kv-cache-dtype",
	"block_size":             "block-size",
	"enable_prefix_caching":  "enable-prefix-caching",
	"gpu_memory_utilization": "gpu-memory-utilization",
}

// parseMetrics reads the Prometheus text exposition format: comment lines,
// then "name value" or "name{label="value",...} value [timestamp]". Label
// values may hold \\, \" and \n escapes.
func parseMetrics(text string) (metrics, error) {
	m := metrics{}
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		name, s, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("/metrics line %d: %w", n+1, err)
		}
		m[name] = append(m[name], s)
	}
	return m, nil
}

func parseLine(line string) (string, sample, error) {
	s := sample{labels: map[string]string{}}
	i := strings.IndexAny(line, "{ \t")
	if i <= 0 {
		return "", s, fmt.Errorf("no value in %.80q", line)
	}
	name := line[:i]
	rest := line[i:]
	if rest[0] == '{' {
		j := 1
		for {
			for j < len(rest) && (rest[j] == ' ' || rest[j] == ',') {
				j++
			}
			if j >= len(rest) {
				return "", s, fmt.Errorf("unterminated labels in %.80q", line)
			}
			if rest[j] == '}' {
				j++
				break
			}
			eq := strings.IndexByte(rest[j:], '=')
			if eq < 0 || j+eq+1 >= len(rest) || rest[j+eq+1] != '"' {
				return "", s, fmt.Errorf("malformed label in %.80q", line)
			}
			key := strings.TrimSpace(rest[j : j+eq])
			j += eq + 2
			var b strings.Builder
			closed := false
			for j < len(rest) {
				ch := rest[j]
				if ch == '\\' && j+1 < len(rest) {
					switch rest[j+1] {
					case 'n':
						b.WriteByte('\n')
					default:
						b.WriteByte(rest[j+1])
					}
					j += 2
					continue
				}
				j++
				if ch == '"' {
					closed = true
					break
				}
				b.WriteByte(ch)
			}
			if !closed {
				return "", s, fmt.Errorf("unterminated label value in %.80q", line)
			}
			s.labels[key] = b.String()
		}
		rest = rest[j:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", s, fmt.Errorf("no value in %.80q", line)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", s, fmt.Errorf("value %q: %w", fields[0], err)
	}
	s.value = v
	return name, s, nil
}

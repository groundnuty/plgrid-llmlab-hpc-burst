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
const (
	metricRunning     = "vllm:num_requests_running"
	metricWaiting     = "vllm:num_requests_waiting"
	metricGenerated   = "vllm:generation_tokens_total"
	metricKVUsage     = "vllm:kv_cache_usage_perc"
	metricCacheConfig = "vllm:cache_config_info"
	metricHTTP        = "http_requests_total"
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

// sum adds the values of every series of the metric; found is false when the
// scrape has none.
func (m metrics) sum(name string) (total float64, found bool) {
	for _, s := range m[name] {
		total += s.value
		found = true
	}
	return total, found
}

// Sample reads the engine's metrics into a health sample: the running and
// waiting requests, the generated-token counter, the KV cache's use and the
// responses by status (HBEP-7, Reports: for health). The agent sets the time
// and the probe's result.
func (c *Client) Sample(ctx context.Context) (agentapi.Health, error) {
	m, err := c.metrics(ctx)
	if err != nil {
		return agentapi.Health{}, err
	}
	var h agentapi.Health
	if v, ok := m.sum(metricRunning); ok {
		h.RunningRequests = int64(v)
	}
	if v, ok := m.sum(metricWaiting); ok {
		h.WaitingRequests = int64(v)
	}
	if v, ok := m.sum(metricGenerated); ok {
		h.GeneratedTokens = int64(v)
	}
	if ss := m[metricKVUsage]; len(ss) > 0 {
		for _, s := range ss {
			h.KVCacheUsage = math.Max(h.KVCacheUsage, s.value)
		}
	}
	// The status label is taken verbatim (agentapi.Health.ResponsesByStatus).
	// vLLM v0.32.0 groups it into classes: serve/instrumentator/metrics.py,
	// lines 64-74, builds its Instrumentator without
	// should_group_status_codes, whose default is True
	// (prometheus-fastapi-instrumentator, instrumentation.py, line 39), so
	// the keys are "2xx", "4xx" and "5xx".
	for _, s := range m[metricHTTP] {
		st := s.labels["status"]
		if st == "" {
			continue
		}
		if h.ResponsesByStatus == nil {
			h.ResponsesByStatus = map[string]int64{}
		}
		h.ResponsesByStatus[st] += int64(s.value)
	}
	return h, nil
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

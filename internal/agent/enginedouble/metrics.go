package enginedouble

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// The /metrics text. The vllm: lines follow the capture of a real vLLM 0.32.0
// (spikes/s2b/results/gh200-1gpu/arms/A1-vr-hash/after-w0.prom): the metric
// names, the label order (engine, finished_reason or reason, model_name) and
// the number format of Python's prometheus_client. The capture holds only
// those 16 lines, with no comment lines and no http_requests_total. The
// # HELP and # TYPE lines are the double's own text, written so that a parser
// meets comment lines as it would in a real scrape; they are not copied from
// vLLM. http_requests_total is the default metric of
// prometheus-fastapi-instrumentator, which vLLM mounts
// (vllm/entrypoints/serve/instrumentator/metrics.py): the status is grouped as
// 1xx to 5xx, and the handlers in instrumentatorExcluded are not counted. Its
// line form is the instrumentator's documented one, not read from a capture.

// cacheConfigLabels are the labels of vllm:cache_config_info in the capture,
// in its order. The ones in cacheConfigDerived are computed from the Config.
var cacheConfigLabels = [][2]string{
	{"_block_size_resolved", "True"},
	{"block_size", "16"},
	{"cache_dtype", "auto"},
	{"cache_hit_alignment_tokens", "None"},
	{"effective_attention_block_size", "16"},
	{"enable_mamba_shared_prefix_checkpoint", "False"},
	{"enable_prefix_caching", "True"},
	{"engine", "0"},
	{"gpu_memory_utilization", "0.2"},
	{"hash_block_size", "None"},
	{"is_attention_free", "False"},
	{"kv_cache_dtype_skip_layers", "[]"},
	{"kv_cache_layout", "None"},
	{"kv_cache_max_concurrency", "32.236328125"},
	{"kv_cache_memory_bytes", "None"},
	{"kv_cache_size_tokens", "528160"},
	{"kv_offloading_backend", "native"},
	{"kv_offloading_size", "None"},
	{"kv_sharing_fast_prefill", "False"},
	{"mamba_block_size", "None"},
	{"mamba_cache_dtype", "auto"},
	{"mamba_cache_mode", "none"},
	{"mamba_page_size_padded", "None"},
	{"mamba_ssm_cache_dtype", "auto"},
	{"num_cpu_blocks", "None"},
	{"num_gpu_blocks", "33010"},
	{"num_gpu_blocks_override", "None"},
	{"prefix_cache_retention_interval", "0"},
	{"prefix_caching_hash_algo", "sha256"},
	{"prefix_match_unit", "None"},
	{"replayssm_buffer_len", "16"},
	{"skip_page_size_padded", "None"},
	{"sliding_window", "None"},
	{"swa_bounded_replay", "True"},
	{"use_kda_recoverssm", "False"},
	{"use_replayssm", "False"},
	{"user_specified_block_size", "False"},
}

// cacheConfigDerived returns the labels that follow from the Config. The
// capture's own values would contradict kv-low: 33010 blocks of a 528160-token
// cache.
func (c Config) cacheConfigDerived() map[string]string {
	return map[string]string{
		"block_size":                     strconv.Itoa(c.BlockSize),
		"effective_attention_block_size": strconv.Itoa(c.BlockSize),
		"cache_dtype":                    c.CacheDtype,
		"enable_prefix_caching":          pyBool(c.PrefixCaching),
		"num_gpu_blocks":                 strconv.FormatInt(c.KVCacheTokens/int64(c.BlockSize), 10),
		"kv_cache_size_tokens":           strconv.FormatInt(c.KVCacheTokens, 10),
		"kv_cache_max_concurrency":       pyRepr(float64(c.KVCacheTokens) / float64(c.MaxModelLen)),
	}
}

func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// pyRepr is Python's repr of a float: the shortest form that reads back, with
// ".0" on a whole number, and an exponent below 1e-4 and from 1e16.
func pyRepr(v float64) string {
	if v != 0 && (math.Abs(v) < 1e-4 || math.Abs(v) >= 1e16) {
		return strconv.FormatFloat(v, 'e', -1, 64)
	}
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// promFloat is prometheus_client's floatToGoString: Python's repr, except from
// seven digits before the point on, where the value is written in exponent
// form, 3.310655e+06 for 3310655.0.
func promFloat(v float64) string {
	s := pyRepr(v)
	dot := strings.Index(s, ".")
	if dot > 6 && !strings.ContainsAny(s, "e") {
		mantissa := strings.TrimRight(s[:1]+"."+s[1:dot]+s[dot+1:], "0.")
		return mantissa + "e+0" + strconv.Itoa(dot-1)
	}
	return s
}

func labelValue(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// metrics is GET /metrics.
func (h *handler) metrics(w http.ResponseWriter) {
	e := h.e
	e.mu.Lock()
	running, waiting := e.running, e.waiting
	kv := float64(e.kvTokens) / float64(h.cfg.KVCacheTokens)
	promptTokens, genTokens := e.promptTokens, e.genTokens
	queries, hits := e.prefixQueries, e.prefixHits
	success := map[string]int64{}
	for k, v := range e.success {
		success[k] = v
	}
	counts := make([]struct {
		k httpKey
		n int64
	}, 0, len(e.httpCount))
	for k, n := range e.httpCount {
		counts = append(counts, struct {
			k httpKey
			n int64
		}{k, n})
	}
	e.mu.Unlock()
	kv = math.Min(math.Max(kv, 0), 1)

	model := labelValue(h.cfg.ServedName)
	base := fmt.Sprintf(`engine="0",model_name="%s"`, model)
	var b strings.Builder
	metric := func(name, typ, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	sample := func(name, labels string, v float64) {
		fmt.Fprintf(&b, "%s{%s} %s\n", name, labels, promFloat(v))
	}

	metric("vllm:num_requests_running", "gauge", "Number of requests in model execution batches.")
	sample("vllm:num_requests_running", base, float64(running))
	metric("vllm:num_requests_waiting", "gauge", "Number of requests waiting to be processed.")
	sample("vllm:num_requests_waiting", base, float64(waiting))
	metric("vllm:num_requests_waiting_by_reason", "gauge", "Number of requests waiting to be processed, by reason it is waiting.")
	sample("vllm:num_requests_waiting_by_reason", fmt.Sprintf(`engine="0",model_name="%s",reason="capacity"`, model), float64(waiting))
	sample("vllm:num_requests_waiting_by_reason", fmt.Sprintf(`engine="0",model_name="%s",reason="deferred"`, model), 0)
	metric("vllm:kv_cache_usage_perc", "gauge", "KV-cache usage. 1 means 100 percent usage.")
	sample("vllm:kv_cache_usage_perc", base, kv)
	metric("vllm:prefix_cache_queries_total", "counter", "Prefix cache queries, in terms of number of queried tokens.")
	sample("vllm:prefix_cache_queries_total", base, float64(queries))
	metric("vllm:prefix_cache_hits_total", "counter", "Prefix cache hits, in terms of number of cached tokens.")
	sample("vllm:prefix_cache_hits_total", base, float64(hits))
	metric("vllm:num_preemptions_total", "counter", "Cumulative number of preemption from the engine.")
	sample("vllm:num_preemptions_total", base, 0)
	metric("vllm:prompt_tokens_total", "counter", "Number of prefill tokens processed.")
	sample("vllm:prompt_tokens_total", base, float64(promptTokens))
	metric("vllm:generation_tokens_total", "counter", "Number of generation tokens processed.")
	sample("vllm:generation_tokens_total", base, float64(genTokens))
	metric("vllm:request_success_total", "counter", "Count of successfully processed requests.")
	for _, reason := range []string{"stop", "length", "abort", "error", "repetition"} {
		sample("vllm:request_success_total", fmt.Sprintf(`engine="0",finished_reason="%s",model_name="%s"`, reason, model), float64(success[reason]))
	}
	if !h.cfg.ReportsNothing {
		metric("vllm:cache_config_info", "gauge", "Information of the LLMEngine CacheConfig")
		sample("vllm:cache_config_info", h.cacheConfigLabelText(), 1)
	}

	sort.Slice(counts, func(i, j int) bool {
		a, c := counts[i].k, counts[j].k
		return a.handler+a.method+a.status < c.handler+c.method+c.status
	})
	if len(counts) > 0 {
		metric("http_requests_total", "counter", "Total number of requests by method, status and handler.")
	}
	for _, c := range counts {
		sample("http_requests_total", fmt.Sprintf(`handler="%s",method="%s",status="%s"`, labelValue(c.k.handler), c.k.method, c.k.status), float64(c.n))
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}

func (h *handler) cacheConfigLabelText() string {
	derived := h.cfg.cacheConfigDerived()
	parts := make([]string, 0, len(cacheConfigLabels))
	for _, kv := range cacheConfigLabels {
		v := kv[1]
		if d, ok := derived[kv[0]]; ok {
			v = d
		}
		parts = append(parts, fmt.Sprintf(`%s="%s"`, kv[0], labelValue(v)))
	}
	return strings.Join(parts, ",")
}

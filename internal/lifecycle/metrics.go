package lifecycle

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The lifecycle controller's engine gauges per Instance (HBEP-8, Metrics),
// served by controller-runtime's metrics server on loopback at the launcher's
// port base + 12. A series exists only while its value is known; an unknown
// one is absent, and hpcburst_instance_metrics_unknown says which and why,
// never 0 and never NaN (HBEP-8, Unknown samples).

// Reasons of hpcburst_instance_metrics_unknown (HBEP-8, Unknown samples).
const (
	unknownAgentUnreachable = "agent_unreachable"
	unknownStale            = "stale"
	unknownNotReported      = "not_reported"
)

// The engine metrics' names (HBEP-8, Engine gauges per Instance).
const (
	metricRunning       = "hpcburst_instance_running_requests"
	metricWaiting       = "hpcburst_instance_waiting_requests"
	metricTPOT          = "hpcburst_instance_time_per_output_token_seconds"
	metricPreemptions   = "hpcburst_instance_preemptions_total"
	metricPrefixQueried = "hpcburst_instance_prefix_cache_queried_tokens_total"
	metricPrefixHit     = "hpcburst_instance_prefix_cache_hit_tokens_total"
	metricKVCache       = "hpcburst_instance_kv_cache_usage_ratio"
	metricGenerated     = "hpcburst_instance_generation_tokens_total"
)

var engineLabels = []string{"instance_uid", "worker", "generation", "worker_generation"}

// engineMetric is one engine metric: its descriptor, and how a known value is
// read from a health sample. value returns ok false for an unknown value: nil,
// or not finite, or a negative count.
type engineMetric struct {
	name  string
	desc  *prometheus.Desc
	value func(h *agentapi.Health, labels []string) (prometheus.Metric, bool)
}

func intOf(f func(*agentapi.Health) *int64) func(*agentapi.Health) (float64, bool) {
	return func(h *agentapi.Health) (float64, bool) {
		// A negative count is no count: unknown, as engineMetric says.
		if v := f(h); v != nil && *v >= 0 {
			return float64(*v), true
		}
		return 0, false
	}
}

func constOf(desc *prometheus.Desc, vt prometheus.ValueType, read func(*agentapi.Health) (float64, bool)) func(*agentapi.Health, []string) (prometheus.Metric, bool) {
	return func(h *agentapi.Health, labels []string) (prometheus.Metric, bool) {
		v, ok := read(h)
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, false
		}
		m, err := prometheus.NewConstMetric(desc, vt, v, labels...)
		return m, err == nil
	}
}

func newEngineMetric(name, help string, vt prometheus.ValueType, read func(*agentapi.Health) (float64, bool)) engineMetric {
	d := prometheus.NewDesc(name, help, engineLabels, nil)
	return engineMetric{name: name, desc: d, value: constOf(d, vt, read)}
}

// engineMetrics are HBEP-8's table, in its order.
var engineMetrics = func() []engineMetric {
	tpot := prometheus.NewDesc(metricTPOT, "Time per output token of the engine's finished requests: the sum of their mean times per output token, in seconds, and their count (vLLM's vllm:request_time_per_output_token_seconds, _sum and _count; no quantiles).", engineLabels, nil)
	return []engineMetric{
		newEngineMetric(metricRunning, "Requests the engine is running (vllm:num_requests_running).", prometheus.GaugeValue,
			intOf(func(h *agentapi.Health) *int64 { return h.RunningRequests })),
		newEngineMetric(metricWaiting, "Requests waiting in the engine's queue (vllm:num_requests_waiting).", prometheus.GaugeValue,
			intOf(func(h *agentapi.Health) *int64 { return h.WaitingRequests })),
		{name: metricTPOT, desc: tpot, value: func(h *agentapi.Health, labels []string) (prometheus.Metric, bool) {
			// The sum and the count are known together or not at all (HBEP-8,
			// science-agent rulings for #110).
			s, n := h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount
			if s == nil || n == nil || *n < 0 || math.IsNaN(*s) || math.IsInf(*s, 0) {
				return nil, false
			}
			m, err := prometheus.NewConstSummary(tpot, uint64(*n), *s, nil, labels...)
			return m, err == nil
		}},
		newEngineMetric(metricPreemptions, "Requests the engine preempted since its process started (vllm:num_preemptions_total).", prometheus.CounterValue,
			intOf(func(h *agentapi.Health) *int64 { return h.Preemptions })),
		newEngineMetric(metricPrefixQueried, "Prompt tokens looked up in the engine's prefix cache since its process started (vllm:prefix_cache_queries_total); the hit rate is hits over queries.", prometheus.CounterValue,
			intOf(func(h *agentapi.Health) *int64 { return h.PrefixCacheQueriedTokens })),
		newEngineMetric(metricPrefixHit, "Prompt tokens found in the engine's prefix cache since its process started (vllm:prefix_cache_hits_total).", prometheus.CounterValue,
			intOf(func(h *agentapi.Health) *int64 { return h.PrefixCacheHitTokens })),
		newEngineMetric(metricKVCache, "The engine's KV-cache use, from 0 to 1 (vllm:kv_cache_usage_perc); recorded, not compared across deployments.", prometheus.GaugeValue,
			func(h *agentapi.Health) (float64, bool) {
				if h.KVCacheUsage == nil {
					return 0, false
				}
				return *h.KVCacheUsage, true
			}),
		newEngineMetric(metricGenerated, "Tokens the engine generated since its process started (vllm:generation_tokens_total).", prometheus.CounterValue,
			intOf(func(h *agentapi.Health) *int64 { return h.GeneratedTokens })),
	}
}()

var (
	descInfo = prometheus.NewDesc("hpcburst_instance_info", "One series per Instance that has not ended, with its descriptive labels; join on instance_uid.",
		[]string{"instance_uid", "namespace", "instance", "model_deployment", "provider", "gpu_model"}, nil)
	descReadAt = prometheus.NewDesc("hpcburst_instance_metrics_read_timestamp_seconds", "When the agent last read the worker's engine metrics (Health.metricsReadAt, the agent's clock), in Unix seconds; absent while there has been no read.",
		[]string{"instance_uid", "worker"}, nil)
	descUnknown = prometheus.NewDesc("hpcburst_instance_metrics_unknown", "1 for each engine metric of a worker whose value is unknown, and so absent: metric is its hpcburst_ name, reason agent_unreachable, stale or not_reported.",
		[]string{"instance_uid", "worker", "metric", "reason"}, nil)
)

// instanceEntry is what the last reconcile pass knew of one Instance that has
// not ended: its identity, and how to find and judge its agent's report.
type instanceEntry struct {
	uid                                   types.UID
	namespace, name, md, provider, gpuMdl string
	// hasAgent: an agent registered for the Instance; allocUID and agentID
	// find its report in the Backend, as the pass reads it (pass.agent).
	hasAgent bool
	allocUID types.UID
	agentID  string
	// unreachable is the pass's AgentUnreachable judgement.
	unreachable   bool
	probeInterval time.Duration
}

// inventory holds, per ModelDeployment, the Instances its last pass saw that
// have not ended. The reconcile pass writes it and the collector reads it, so
// it is safe for concurrent use.
type inventory struct {
	mu   sync.Mutex
	byMD map[types.NamespacedName]map[types.UID]instanceEntry
}

// keep drops the deployment's entries that are not in live, the UIDs of its
// Instances that have not ended; an empty live forgets the deployment.
func (v *inventory) keep(md types.NamespacedName, live map[types.UID]bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(live) == 0 {
		delete(v.byMD, md)
		return
	}
	for uid := range v.byMD[md] {
		if !live[uid] {
			delete(v.byMD[md], uid)
		}
	}
}

// setIdentity records an Instance's identity under its deployment, keeping
// what an earlier pass learnt of its agent, so a pass that fails before it
// reaches the agent does not hide the Instance's series.
func (v *inventory) setIdentity(md types.NamespacedName, e instanceEntry) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.byMD == nil {
		v.byMD = map[types.NamespacedName]map[types.UID]instanceEntry{}
	}
	if v.byMD[md] == nil {
		v.byMD[md] = map[types.UID]instanceEntry{}
	}
	if old, ok := v.byMD[md][e.uid]; ok {
		e.hasAgent, e.allocUID, e.agentID, e.unreachable, e.probeInterval = old.hasAgent, old.allocUID, old.agentID, old.unreachable, old.probeInterval
	}
	v.byMD[md][e.uid] = e
}

// setAgent records the pass's view of an Instance's agent: which report is
// its own, whether the agent is unreachable, and the probe interval its
// freshness is judged by.
func (v *inventory) setAgent(md types.NamespacedName, uid, allocUID types.UID, agentID string, unreachable bool, probeInterval time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.byMD[md][uid]
	if !ok {
		return // not recorded by this pass: it ended or is not the deployment's
	}
	e.hasAgent, e.allocUID, e.agentID, e.unreachable, e.probeInterval = true, allocUID, agentID, unreachable, probeInterval
	v.byMD[md][uid] = e
}

// drop forgets one Instance: it ended.
func (v *inventory) drop(md types.NamespacedName, uid types.UID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.byMD[md], uid)
}

// entries returns a copy of every entry, ordered by UID.
func (v *inventory) entries() []instanceEntry {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []instanceEntry
	for _, m := range v.byMD {
		for _, e := range m {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].uid < out[j].uid })
	return out
}

// instanceCollector is a prometheus.Collector that builds the engine series at
// scrape time from each worker's latest health sample in the Backend. The
// sample's freshness is judged at the scrape, by the controller's clock
// (agentapi.Health.MetricsFresh), since passes can be further apart than the
// freshness bound.
type instanceCollector struct {
	inv     *inventory
	backend *Backend
	clock   Clock
}

// Describe implements prometheus.Collector.
func (c *instanceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descInfo
	ch <- descReadAt
	ch <- descUnknown
	for _, m := range engineMetrics {
		ch <- m.desc
	}
}

// Collect implements prometheus.Collector.
func (c *instanceCollector) Collect(ch chan<- prometheus.Metric) {
	now := c.clock.Now().UTC()
	for _, e := range c.inv.entries() {
		ch <- prometheus.MustNewConstMetric(descInfo, prometheus.GaugeValue, 1, string(e.uid), e.namespace, e.name, e.md, e.provider, e.gpuMdl)
		if !e.hasAgent {
			continue
		}
		rep, at, from := c.backend.latest(e.allocUID)
		if rep == nil || from != e.agentID || rep.Instance == nil || rep.Instance.UID != string(e.uid) {
			// No report of this Instance from its agent since this controller
			// started, so no worker is reported: the alpha Instance's one
			// desired worker (the Backend's desiredInstance) stands in, and
			// its metrics are unknown. Without this, an agent that never
			// reports again after a controller restart leaves its Instance
			// with the info series only, and a share-of-unknown query reads
			// 0/0.
			c.collectUnreported(ch, e)
			continue
		}
		av := agentView{report: rep.Instance, reportAt: at, reportTime: rep.Time, unreachable: e.unreachable, now: now, probeInterval: e.probeInterval}
		gen := strconv.FormatInt(rep.Instance.Generation, 10)
		for i := range rep.Instance.Workers {
			w := &rep.Instance.Workers[i]
			c.collectWorker(ch, e, av, w, gen)
		}
	}
}

// collectUnreported sends the unknown series of an Instance whose agent has
// sent no report of it since this controller started, new or after a restart
// (reports are held in memory): every engine metric of its desired state's one
// worker, worker-0 in alpha, is unknown. It has no fresh read, so the reason is
// stale, or agent_unreachable once the pass has judged the agent unreachable
// (health.agentTimeout, with the restart grace); not_reported is for a metric
// the engine does not export (HBEP-8, Unknown samples; `@hpc-burst-science-agent`,
// 1 Nov 2026, for #111).
func (c *instanceCollector) collectUnreported(ch chan<- prometheus.Metric, e instanceEntry) {
	reason := unknownStale
	if e.unreachable {
		reason = unknownAgentUnreachable
	}
	for _, m := range engineMetrics {
		ch <- prometheus.MustNewConstMetric(descUnknown, prometheus.GaugeValue, 1, string(e.uid), agentapi.WorkerName(0), m.name, reason)
	}
}

// collectWorker sends one worker's series: per engine metric, its value when
// known, or an unknown series with the reason, never both and never neither.
func (c *instanceCollector) collectWorker(ch chan<- prometheus.Metric, e instanceEntry, av agentView, w *agentapi.WorkerReport, gen string) {
	uid := string(e.uid)
	if w.Health != nil && w.Health.MetricsReadAt != nil {
		ch <- prometheus.MustNewConstMetric(descReadAt, prometheus.GaugeValue, float64(w.Health.MetricsReadAt.UnixNano())/1e9, uid, w.Name)
	}
	reason := ""
	switch {
	case av.unreachable:
		reason = unknownAgentUnreachable
	case !av.metricsFresh(w):
		reason = unknownStale
	}
	labels := []string{uid, w.Name, gen, strconv.FormatInt(w.Generation, 10)}
	for _, m := range engineMetrics {
		if reason == "" {
			if s, ok := m.value(w.Health, labels); ok {
				ch <- s
				continue
			}
		}
		r := reason
		if r == "" {
			r = unknownNotReported
		}
		ch <- prometheus.MustNewConstMetric(descUnknown, prometheus.GaugeValue, 1, uid, w.Name, m.name, r)
	}
}

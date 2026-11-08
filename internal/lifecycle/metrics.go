package lifecycle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The lifecycle controller's engine gauges per Instance (HBEP-8, Metrics),
// served by controller-runtime's metrics server on loopback at the launcher's
// port base + 12. A series exists only while its value is known; an unknown
// one is absent, and hpcburst_instance_metrics_unknown says which and why,
// never 0 and never NaN (HBEP-8, Unknown samples).
//
// Only the serving copy exports them (stateCollector), and it builds the
// Instances it lists from its informer cache, which HBEP-11 starts before the
// election, not from its reconciles: a new leader lists every live Instance at
// once, each unknown until its first report to this leader, so a
// share-of-unknown query reads them as unknown rather than 0 over 0 (HBEP-8,
// Unknown samples; `@hpc-burst-science-agent`, 2 Nov 2026, for #249). That
// inventory read can fail, and then hpcburst_collector_success{collector=
// "instances"} is 0 and no Instance series is exposed (6 Nov 2026, for #249).

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

// instanceListTimeout bounds a scrape's reads of the manager's cache.
const instanceListTimeout = 10 * time.Second

// collectorInstances is the collector label of the Instance series'
// hpcburst_collector_success series.
const collectorInstances = "instances"

// instanceEntry is what the cache says of one Instance that has not ended: its
// identity, and how to find and judge its agent's report.
type instanceEntry struct {
	uid                                   types.UID
	namespace, name, md, provider, gpuMdl string
	// allocUID and agentID find the Instance's report in the Backend, as the
	// pass reads it (pass.agent): the UID of the Allocation it names, empty
	// while that is not in the cache, and the agent process that registered
	// for it, empty while none has.
	allocUID types.UID
	agentID  string
	// unreachable is the Instance's AgentUnreachable condition as persisted:
	// the judgement of the pass that wrote it, maybe a previous leader's. It
	// stands only while this process holds no report from the agent (collect).
	unreachable bool
	// startInstr is the Instance's start instruction time, zero when unset,
	// and agentTimeout and probeInterval its profile's health.agentTimeout and
	// probe interval: what the pass judges the agent by.
	startInstr    time.Time
	agentTimeout  time.Duration
	probeInterval time.Duration
}

// instanceCollector builds the Instance series at scrape time from the
// Instances in the controller's cache and each worker's latest health sample
// in the Backend. The sample's freshness is judged at the scrape, by the
// controller's clock (agentapi.Health.MetricsFresh), since passes can be
// further apart than the freshness bound. It is one of stateCollector's two
// reads, not a collector registered on its own.
type instanceCollector struct {
	// cache reads Instances, Allocations, Providers and PlatformProfiles: the
	// manager's cached client.
	cache   client.Reader
	backend *Backend
	clock   Clock
	// started is when this process started, as the passes count it (memory
	// started): the base of the agents' restart grace (agentDeadline).
	started func() time.Time
}

// describe sends the descriptors of the Instance series.
func (c *instanceCollector) describe(ch chan<- *prometheus.Desc) {
	ch <- descInfo
	ch <- descReadAt
	ch <- descUnknown
	for _, m := range engineMetrics {
		ch <- m.desc
	}
}

// collect appends to out the Instance series and
// hpcburst_collector_success{collector="instances"}. It lists the Instances
// that have not ended from the cache; one with no report from its agent to
// this process is unknown, also one still pending with no agent (HBEP-8,
// Unknown samples). An inventory read that fails, the list or a cache read of
// an Instance's Allocation, Provider or PlatformProfile other than a
// not-found, gives no Instance series and the gauge at 0, and is logged; one
// that succeeds gives 1 (HBEP-8, Unknown samples; `@hpc-burst-science-agent`,
// 6 Nov 2026, for #249).
//
// A report the agent sent to this process supersedes the persisted
// AgentUnreachable condition, which a previous leader may have written: with
// one, the agent is judged from that report's receipt as the pass judges it
// (agentDeadline), so its first fresh report to a new leader shows real
// values at once, without waiting for a reconcile to clear the condition
// (PR #254 review, B3). Without one the persisted condition stands.
func (c *instanceCollector) collect(out []prometheus.Metric) []prometheus.Metric {
	ctx, cancel := context.WithTimeout(context.Background(), instanceListTimeout)
	defer cancel()
	entries, err := c.live(ctx)
	if err != nil {
		ctrl.Log.WithName("metrics").Error(err, "the Instance series are not exposed for this scrape")
		return append(out, successSample(collectorInstances, false))
	}
	out = append(out, successSample(collectorInstances, true))
	now := c.clock.Now().UTC()
	started := c.started()
	for _, e := range entries {
		out = append(out, prometheus.MustNewConstMetric(descInfo, prometheus.GaugeValue, 1, string(e.uid), e.namespace, e.name, e.md, e.provider, e.gpuMdl))
		var rep *agentapi.Report
		var at time.Time
		var from string
		if e.allocUID != "" {
			rep, at, from = c.backend.latest(e.allocUID)
		}
		unreachable := e.unreachable
		if rep != nil && e.agentID != "" && from == e.agentID {
			unreachable = !now.Before(agentDeadline(started, e.startInstr, at, e.agentTimeout))
		} else {
			rep = nil
		}
		if rep == nil || rep.Instance == nil || rep.Instance.UID != string(e.uid) {
			// No report of this Instance from its agent since this controller
			// started, so no worker is reported: the alpha Instance's one
			// desired worker (the Backend's desiredInstance) stands in, and
			// its metrics are unknown. Without this, an Instance that is
			// pending, or whose agent has not reported to this leader yet or
			// never reports again, has the info series only, and a
			// share-of-unknown query reads 0/0.
			out = collectUnreported(out, e, unreachable)
			continue
		}
		av := agentView{report: rep.Instance, reportAt: at, reportTime: rep.Time, unreachable: unreachable, now: now, probeInterval: e.probeInterval}
		gen := strconv.FormatInt(rep.Instance.Generation, 10)
		for i := range rep.Instance.Workers {
			out = collectWorker(out, e, av, &rep.Instance.Workers[i], gen)
		}
	}
	return out
}

// live reads, from the cache, the Instances that have not ended and are a
// ModelDeployment's, ordered by UID, each with its identity and what the pass
// last judged of its agent (the Instance's AgentUnreachable condition). The
// Instance's Allocation, Provider and PlatformProfile are read once per name.
func (c *instanceCollector) live(ctx context.Context) ([]instanceEntry, error) {
	var il v1alpha1.InstanceList
	if err := c.cache.List(ctx, &il); err != nil {
		return nil, fmt.Errorf("list Instances: %w", err)
	}
	facts := map[string]providerFacts{}
	var out []instanceEntry
	for i := range il.Items {
		in := &il.Items[i]
		md, owned := controllerDeployment(in)
		if !owned || ended(in) {
			continue
		}
		e := instanceEntry{uid: in.UID, namespace: in.Namespace, name: in.Name, md: md, provider: in.Labels[v1alpha1.LabelProvider]}
		if in.Status.Agent != nil {
			e.agentID = in.Status.Agent.AgentInstanceID
		}
		e.unreachable = apimeta.IsStatusConditionTrue(in.Status.Conditions, v1alpha1.ConditionAgentUnreachable)
		if t := in.Status.StartInstructionTime; t != nil {
			e.startInstr = t.Time
		}
		pf, ok := facts[e.provider]
		if !ok {
			var err error
			if pf, err = c.providerFacts(ctx, e.provider); err != nil {
				return nil, err
			}
			facts[e.provider] = pf
		}
		e.gpuMdl, e.agentTimeout, e.probeInterval = pf.gpuModel, pf.agentTimeout, pf.probeInterval
		if name := in.Spec.AllocationRef.Name; name != "" {
			var a v1alpha1.Allocation
			switch err := c.cache.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: name}, &a); {
			case err == nil:
				e.allocUID = a.UID
			case !apierrors.IsNotFound(err):
				return nil, fmt.Errorf("get Allocation %s/%s: %w", in.Namespace, name, err)
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].uid < out[j].uid })
	return out, nil
}

// providerFacts are what an Instance's info series and freshness take from its
// Provider: the GPU model of the partition it allows, and the agent timeout
// and probe interval its profile sets.
type providerFacts struct {
	gpuModel      string
	agentTimeout  time.Duration
	probeInterval time.Duration
}

// providerFacts reads a Provider and its PlatformProfile from the cache. A
// Provider or profile that is gone, or one the pass would not provision on,
// gives no GPU model and the default probe interval, as the pass does
// (settingsOf, loadProviders).
func (c *instanceCollector) providerFacts(ctx context.Context, name string) (providerFacts, error) {
	def, _ := resolveSettings(v1alpha1.PlatformProfileSpec{})
	f := providerFacts{agentTimeout: def.agentTimeout, probeInterval: def.probeInterval}
	var prov v1alpha1.Provider
	if err := c.cache.Get(ctx, client.ObjectKey{Name: name}, &prov); err != nil {
		if apierrors.IsNotFound(err) {
			return f, nil
		}
		return f, fmt.Errorf("get Provider %s: %w", name, err)
	}
	ref := prov.Spec.PlatformProfileRef.Name
	var prof v1alpha1.PlatformProfile
	if err := c.cache.Get(ctx, client.ObjectKey{Name: ref}, &prof); err != nil {
		if apierrors.IsNotFound(err) {
			return f, nil
		}
		return f, fmt.Errorf("get PlatformProfile %s: %w", ref, err)
	}
	s, err := resolveSettings(prof.Spec)
	f.agentTimeout, f.probeInterval = s.agentTimeout, s.probeInterval
	if err != nil || prof.Spec.ProviderKind != v1alpha1.PlatformBatch {
		return f, nil
	}
	if part, err := allowedPartition(&prov, &prof); err == nil {
		f.gpuModel = part.AcceleratorClass.GPUModel
	}
	return f, nil
}

// collectUnreported appends the unknown series of an Instance whose agent has
// sent no report of it since this controller started: pending with no agent
// yet, new, or after a restart or a change of leader (reports are held in
// memory). Every engine metric of its desired state's one worker, worker-0 in
// alpha, is unknown. It has no fresh read, so the reason is stale, or
// agent_unreachable when the agent is judged unreachable (collect); not_reported
// is for a metric the engine does not export (HBEP-8, Unknown samples;
// `@hpc-burst-science-agent`, 1 Nov 2026, for #111).
func collectUnreported(out []prometheus.Metric, e instanceEntry, unreachable bool) []prometheus.Metric {
	reason := unknownStale
	if unreachable {
		reason = unknownAgentUnreachable
	}
	for _, m := range engineMetrics {
		out = append(out, prometheus.MustNewConstMetric(descUnknown, prometheus.GaugeValue, 1, string(e.uid), agentapi.WorkerName(0), m.name, reason))
	}
	return out
}

// collectWorker appends one worker's series: per engine metric, its value when
// known, or an unknown series with the reason, never both and never neither.
func collectWorker(out []prometheus.Metric, e instanceEntry, av agentView, w *agentapi.WorkerReport, gen string) []prometheus.Metric {
	uid := string(e.uid)
	if w.Health != nil && w.Health.MetricsReadAt != nil {
		out = append(out, prometheus.MustNewConstMetric(descReadAt, prometheus.GaugeValue, float64(w.Health.MetricsReadAt.UnixNano())/1e9, uid, w.Name))
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
				out = append(out, s)
				continue
			}
		}
		r := reason
		if r == "" {
			r = unknownNotReported
		}
		out = append(out, prometheus.MustNewConstMetric(descUnknown, prometheus.GaugeValue, 1, uid, w.Name, m.name, r))
	}
	return out
}

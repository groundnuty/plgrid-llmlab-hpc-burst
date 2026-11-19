package front

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage/forward"
)

// ttftBuckets are HBEP-8's buckets for the time to first token, in seconds
// (Metrics, Front metrics): they reach past S2's first token after 1,100 s and
// the 2 h default maxRequestDuration.
var ttftBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200}

// speedBuckets are HBEP-8's buckets for the output speed of one stream, in tokens per second (Metrics, Front metrics).
var speedBuckets = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500}

// FreeBytesFunc returns the free space of the file system that holds dir, in
// bytes available to the front.
type FreeBytesFunc func(dir string) (uint64, error)

// Metrics are the front's Prometheus metrics (HBEP-8, Metrics, Front metrics),
// served at GET /metrics on the health listener. It uses a registry of its
// own, so two fronts in one process do not share series. No label holds a
// client key ID or the client's model name: per-key usage is in the records,
// and the model name's values are unbounded.
//
// The methods of a nil *Metrics do nothing, so a front without metrics (a
// test) needs no checks.
type Metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	cuts     *prometheus.CounterVec
	pauses   *prometheus.CounterVec
	ttft     *prometheus.HistogramVec
	// inFlight, waiting and speed are the series of the Workload build (HBEP-8, Front metrics): the requests in flight per route and per Instance, the
	// requests waiting in a FIFO, and the output tokens per second of each completed stream that carried usage.
	inFlight *prometheus.GaugeVec
	speed    *prometheus.HistogramVec
	// router is the router the waiting series is read from (attach).
	mu     sync.Mutex
	router *Router
	// bulk counts bulk dispatches and fallbacks (enableBulk); nil without the bulk path.
	bulk *prometheus.CounterVec
}

// NewMetrics registers the front's metrics. sink, when set, feeds the three
// usage metrics at each scrape; dir is the usage directory whose free space
// hpcburst_usage_dir_free_bytes reports, read with free (nil: statfs). events,
// when set, is the front's raised-events counter, which its recorder counts
// into (NewKubeRecorder): it is registered here with the front's series
// created at 0, beside the process collector, whose
// process_start_time_seconds tells a restarted front from the same one
// (HBEP-8, Raised events).
func NewMetrics(sink *usage.Sink, dir string, free FreeBytesFunc, events *eventcount.Counter) *Metrics {
	if free == nil {
		free = statfsFree
	}
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_requests_total",
			Help: "Requests the front answered, by the HTTP status the client got. One per usage record. provider and model_deployment are empty for an error raised before a target was chosen; workload is the request's resolved Workload, empty for a request that names none or was refused before one resolved.",
		}, []string{"code", "endpoint", "provider", "model_deployment", "workload"}),
		cuts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_cuts_total",
			Help: "Requests cut before they ended, by the hop that cut them (HBEP-5 cutBy). workload is the request's resolved Workload, empty for a request that names none or was refused before one resolved.",
		}, []string{"cut_by", "provider", "model_deployment", "workload"}),
		pauses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_pauses_total",
			Help: "Pauses of a target after failed connections.",
		}, []string{"provider", "model_deployment"}),
		ttft: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hpcburst_front_time_to_first_token_seconds",
			Help:    "Time from the request's arrival to its first content event, from live traffic. A request without a first token is not observed. workload is the request's resolved Workload, empty for a request that names none.",
			Buckets: ttftBuckets,
		}, []string{"provider", "model_deployment", "endpoint", "workload"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hpcburst_front_in_flight",
			Help: "Requests in flight per route and per Instance, from dispatch to completion, as the balancer counts them. instance_uid is empty for a managed target, and workload for a request that names none. An Instance's series goes when it leaves its group.",
		}, []string{"workload", "provider", "model_deployment", "instance_uid"}),
		speed: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hpcburst_front_output_tokens_per_second",
			Help:    "Output tokens per second of each completed stream that carried usage: its output tokens over the time from its first token to its end. A stream without usage, or one whose first token is its end, is not observed. workload is the request's resolved Workload, empty for a request that names none.",
			Buckets: speedBuckets,
		}, []string{"workload", "provider", "model_deployment"}),
	}
	m.reg.MustRegister(m.requests, m.cuts, m.pauses, m.ttft, m.inFlight, m.speed, &waitingCollector{m: m}, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if events != nil {
		events.Zero(eventcount.ControllerFront, RaisedEvents())
		m.reg.MustRegister(events)
	}
	m.known("", "") // the cuts of a request that never had a target, and named no Workload
	if sink != nil {
		m.reg.MustRegister(&usageCollector{sink: sink, dir: dir, free: free})
	}
	return m
}

// enableUsageLog registers hpcburst_usage_skipped_total, the records passed over for their length,
// and hpcburst_usage_unlogged_records, the usage records the sink has synced
// that the durable log does not hold yet (HBEP-8, The durable log, When it is down): the last synced
// counter less the last counter the log holds. Only a front that runs with the log calls it, so
// without the log /metrics is as before. The series is absent, not 0, while the forwarder has not
// yet read the log's last counter, and while the sink's position is unknown (it could not read the
// files' last counter at its start and has synced nothing since): unknown is never 0 (HBEP-8,
// Unknown samples). A nil forwarder
// registers nothing.
func (m *Metrics) enableUsageLog(f *forward.Forwarder) {
	if m == nil || f == nil {
		return
	}
	m.reg.MustRegister(&usageLogCollector{fwd: f})
}

var (
	descUnlogged = prometheus.NewDesc("hpcburst_usage_unlogged_records", "Usage records the sink has synced that the durable log does not hold yet: the last synced counter less the last counter the log holds. Absent until the forwarder has read the log's last counter and the sink's position is known.", nil, nil)
	descSkipped  = prometheus.NewDesc("hpcburst_usage_skipped_total", "Usage records the forwarder did not copy to the durable log because their line is longer than a message (files written before the front bounded the record's client text, HBEP-5, Usage records, #649, may hold one), each counted once. The files hold them. They are not lag, so they are not in hpcburst_usage_unlogged_records.", nil, nil)
)

// usageLogCollector exports the gauge and the skip counter from one read of the forwarder at each
// scrape. The counter is a real zero until a record is passed over, so it is always exported.
type usageLogCollector struct{ fwd *forward.Forwarder }

// Describe implements prometheus.Collector.
func (c *usageLogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descUnlogged
	ch <- descSkipped
}

// Collect implements prometheus.Collector.
func (c *usageLogCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.fwd.Stats()
	if st.LoggedKnown && st.SyncedKnown {
		ch <- prometheus.MustNewConstMetric(descUnlogged, prometheus.GaugeValue, float64(st.Unlogged))
	}
	ch <- prometheus.MustNewConstMetric(descSkipped, prometheus.CounterValue, float64(st.Skipped))
}

// enableBulk registers the bulk path's two series (HBEP-6, The bulk path, Metrics): requests sent
// through the bulk transport and requests whose bulk dial failed and took the token transport, by
// path, both created at 0; and how many Instances have a registered bulk transport, read from
// registered at each scrape. Only a front with the bulk path calls it, so without the bulk path
// /metrics is as before.
func (m *Metrics) enableBulk(registered func() int) {
	if m == nil {
		return
	}
	m.bulk = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hpcburst_front_bulk_requests_total",
		Help: "Non-streamed requests toward an Instance by the bulk path's outcome: path=bulk, written to the Instance's bulk transport; path=fallback, its bulk dial failed before any byte and it took the token transport.",
	}, []string{"path"})
	m.bulk.WithLabelValues("bulk")
	m.bulk.WithLabelValues("fallback")
	m.reg.MustRegister(m.bulk, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "hpcburst_front_bulk_registered",
		Help: "Instances whose bulk transport counts as registered: a probe through the bulk proxy passed in its current epoch.",
	}, func() float64 { return float64(registered()) }))
}

// bulkSent counts a request written to a bulk transport.
func (m *Metrics) bulkSent() {
	if m != nil && m.bulk != nil {
		m.bulk.WithLabelValues("bulk").Inc()
	}
}

// bulkFellBack counts a request whose bulk dial failed and took the token transport.
func (m *Metrics) bulkFellBack() {
	if m != nil && m.bulk != nil {
		m.bulk.WithLabelValues("fallback").Inc()
	}
}

// Handler serves the metrics in Prometheus's text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// observe counts one finished request from its usage record: the request by
// the status the client got, its cut, and its time to first token. The labels
// of the target come from the record, and are empty when no target was chosen
// (the record's modelDeployment can still name one then, for a capacity error).
// The workload label is the record's workload: its name once the request's
// Workload resolved, so a refusal after that carries it, and empty otherwise.
func (m *Metrics) observe(r usage.Record) {
	if m == nil {
		return
	}
	provider, dep := r.Provider, r.ModelDeployment
	if provider == "" {
		dep = "" // no target was chosen
	}
	workload := ""
	if r.Workload != nil {
		workload = *r.Workload
	}
	endpoint := endpointLabel(r.Path)
	m.requests.WithLabelValues(strconv.Itoa(r.Status), endpoint, provider, dep, workload).Inc()
	if r.CutBy != nil {
		m.cuts.WithLabelValues(*r.CutBy, provider, dep, workload).Inc()
	}
	if r.FirstTokenMs != nil {
		m.ttft.WithLabelValues(provider, dep, endpoint, workload).Observe(float64(*r.FirstTokenMs) / 1000)
	}
	if v, ok := outputSpeed(r); ok {
		m.speed.WithLabelValues(workload, provider, dep).Observe(v)
	}
}

// outputSpeed is the output tokens per second of a completed stream that carried usage: the record's completion tokens over the time from its first
// token to its end (HBEP-8, Front metrics). It is not defined, so the stream is not observed, for a request that is not a stream, one that was cut or
// that failed (its end is not a completion), one with no usage (the engine's counts are the only ones used: a stream without them has none), one with
// no first token, and one whose first token is its end.
func outputSpeed(r usage.Record) (float64, bool) {
	if !r.Stream || r.CutBy != nil || r.Finish == nil || r.CompletionTokens == nil || r.FirstTokenMs == nil || r.CountsSource == nil || *r.CountsSource != "engine" {
		return 0, false
	}
	switch *r.Finish {
	case "cut", "truncated", "error", "failed":
		return 0, false
	}
	span := r.DurationMs - *r.FirstTokenMs
	if span <= 0 || *r.CompletionTokens < 0 {
		return 0, false
	}
	return float64(*r.CompletionTokens) / (float64(span) / 1000), true
}

// attach tells the metrics which router the waiting series is read from.
func (m *Metrics) attach(r *Router) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.router = r
	m.mu.Unlock()
}

// inFlightAdd counts a request on a route: +1 when it is reserved, -1 when it is released. instanceUID is empty for a managed target.
func (m *Metrics) inFlightAdd(workload, provider, deployment, instanceUID string, delta int) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(workload, provider, deployment, instanceUID).Add(float64(delta))
}

// inFlightDropIfEmpty drops the series of an Instance's earlier incarnation when its last request has been released: a replacement under the same name
// has another UID and no leave to drop the old one's series.
func (m *Metrics) inFlightDropIfEmpty(workload, provider, deployment, instanceUID string) {
	if m == nil || instanceUID == "" {
		return
	}
	var d dto.Metric
	if err := m.inFlight.WithLabelValues(workload, provider, deployment, instanceUID).Write(&d); err == nil && d.GetGauge().GetValue() <= 0 {
		m.inFlight.DeleteLabelValues(workload, provider, deployment, instanceUID)
	}
}

// inFlightGone drops every in-flight series of an Instance that has left its group.
func (m *Metrics) inFlightGone(instanceUID string) {
	if m == nil || instanceUID == "" {
		return
	}
	m.inFlight.DeletePartialMatch(prometheus.Labels{"instance_uid": instanceUID})
}

// waitingCollector exports hpcburst_front_waiting from one read of the router at each scrape: a ModelDeployment's group queue has an empty workload
// label, and a Workload's FIFO an empty model_deployment label (the science agent's ruling Q10). A series is exported for each live group and for each
// Workload that has had a request, 0 when its queue is empty, and goes with the group or the Workload.
type waitingCollector struct{ m *Metrics }

var descWaiting = prometheus.NewDesc("hpcburst_front_waiting", "Requests waiting in the front's FIFO: a ModelDeployment's queue, with an empty workload, for a request that names no Workload; a Workload's own FIFO, with an empty model_deployment, for one that does.", []string{"workload", "model_deployment"}, nil)

// Describe implements prometheus.Collector.
func (c *waitingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- descWaiting }

// Collect implements prometheus.Collector.
func (c *waitingCollector) Collect(ch chan<- prometheus.Metric) {
	c.m.mu.Lock()
	r := c.m.router
	c.m.mu.Unlock()
	if r == nil {
		return
	}
	for _, s := range r.waitingSamples() {
		ch <- prometheus.MustNewConstMetric(descWaiting, prometheus.GaugeValue, float64(s.n), s.workload, s.deployment)
	}
}

// cutReasons are HBEP-5's values of cutBy, the cut_by label's values.
var cutReasons = []string{cutFrontDeadline, cutFrontIdle, cutFrontHeaders, cutTunnel, cutUpstream, cutLeaseEnd, cutClient, cutFrontShutdown}

// known creates a target's counters of its own events at 0 (HBEP-8, Unknown
// samples: a count of the front's own events starts at 0, so a healthy front
// shows zeros, and the first event has a zero before it for rate and
// increase): one cut series for each cut_by and one pause series. An empty
// provider and deployment are the cuts of a request that had no target. Only the
// series with an empty workload are created: a Workload's series appear with its
// first request, so a Workload object adds none (HBEP-8, Front metrics). It is
// idempotent.
func (m *Metrics) known(provider, deployment string) {
	if m == nil {
		return
	}
	for _, c := range cutReasons {
		m.cuts.WithLabelValues(c, provider, deployment, "")
	}
	if provider != "" || deployment != "" {
		m.pauses.WithLabelValues(provider, deployment)
	}
}

// paused counts one pause of a target.
func (m *Metrics) paused(provider, deployment string) {
	if m == nil {
		return
	}
	m.pauses.WithLabelValues(provider, deployment).Inc()
}

// endpointLabel is the metrics' endpoint label for a request path (HBEP-8):
// chat, completions, responses, embeddings, models or other.
func endpointLabel(path string) string {
	switch endpointOf(path) {
	case endpointChat:
		return "chat"
	case endpointCompletions:
		return "completions"
	case endpointResponses:
		return "responses"
	case endpointEmbeddings:
		return "embeddings"
	}
	if path == "/v1/models" || strings.HasPrefix(path, "/v1/models/") {
		return "models"
	}
	return "other"
}

// usageCollector exports the usage sink's three metrics from one read of its
// state at each scrape. The free space is absent when it cannot be read:
// unknown is never 0 (HBEP-8, Unknown samples).
type usageCollector struct {
	sink *usage.Sink
	dir  string
	free FreeBytesFunc
}

var (
	descRecords  = prometheus.NewDesc("hpcburst_usage_records_total", "Usage records written and synced.", nil, nil)
	descFailures = prometheus.NewDesc("hpcburst_usage_write_failures_total", "Usage records not written: a failed write or sync of their batch, or a failed counter-block reservation.", nil, nil)
	descFree     = prometheus.NewDesc("hpcburst_usage_dir_free_bytes", "Free space of the usage directory's file system, in bytes.", nil, nil)
)

// Describe implements prometheus.Collector.
func (c *usageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descRecords
	ch <- descFailures
	ch <- descFree
}

// Collect implements prometheus.Collector.
func (c *usageCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.sink.Stats()
	ch <- prometheus.MustNewConstMetric(descRecords, prometheus.CounterValue, float64(st.Records))
	ch <- prometheus.MustNewConstMetric(descFailures, prometheus.CounterValue, float64(st.Lost))
	if free, err := c.free(c.dir); err == nil {
		ch <- prometheus.MustNewConstMetric(descFree, prometheus.GaugeValue, float64(free))
	}
}

// statfsFree is the space available to an unprivileged writer on dir's file
// system.
func statfsFree(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

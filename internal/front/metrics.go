package front

import (
	"net/http"
	"strconv"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// ttftBuckets are HBEP-8's buckets for the time to first token, in seconds
// (Metrics, Front metrics): they reach past S2's first token after 1,100 s and
// the 2 h default maxRequestDuration.
var ttftBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200}

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
}

// NewMetrics registers the front's metrics. sink, when set, feeds the three
// usage metrics at each scrape; dir is the usage directory whose free space
// hpcburst_usage_dir_free_bytes reports, read with free (nil: statfs).
func NewMetrics(sink *usage.Sink, dir string, free FreeBytesFunc) *Metrics {
	if free == nil {
		free = statfsFree
	}
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_requests_total",
			Help: "Requests the front answered, by the HTTP status the client got. One per usage record. provider and model_deployment are empty for an error raised before a target was chosen.",
		}, []string{"code", "endpoint", "provider", "model_deployment"}),
		cuts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_cuts_total",
			Help: "Requests cut before they ended, by the hop that cut them (HBEP-5 cutBy).",
		}, []string{"cut_by", "provider", "model_deployment"}),
		pauses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hpcburst_front_pauses_total",
			Help: "Pauses of a target after failed connections.",
		}, []string{"provider", "model_deployment"}),
		ttft: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hpcburst_front_time_to_first_token_seconds",
			Help:    "Time from the request's arrival to its first content event, from live traffic. A request without a first token is not observed.",
			Buckets: ttftBuckets,
		}, []string{"provider", "model_deployment", "endpoint"}),
	}
	m.reg.MustRegister(m.requests, m.cuts, m.pauses, m.ttft)
	if sink != nil {
		m.reg.MustRegister(&usageCollector{sink: sink, dir: dir, free: free})
	}
	return m
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
func (m *Metrics) observe(r usage.Record) {
	if m == nil {
		return
	}
	provider, dep := r.Provider, r.ModelDeployment
	if provider == "" {
		dep = "" // no target was chosen
	}
	endpoint := endpointLabel(r.Path)
	m.requests.WithLabelValues(strconv.Itoa(r.Status), endpoint, provider, dep).Inc()
	if r.CutBy != nil {
		m.cuts.WithLabelValues(*r.CutBy, provider, dep).Inc()
	}
	if r.FirstTokenMs != nil {
		m.ttft.WithLabelValues(provider, dep, endpoint).Observe(float64(*r.FirstTokenMs) / 1000)
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

package lifecycle

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// descCollectorSuccess is node_exporter's node_scrape_collector_success
// (HBEP-8, Unknown samples): 1 after a successful read, 0 after a failed one,
// in which case the collector exposes none of its series. It tells "none" from
// "could not read". In alpha two reads have one: accounting, the Allocation
// list, and instances, the Instance inventory. One registered collector,
// stateCollector, describes it: client_golang's Registry.Register refuses a
// second collector that describes the same descriptor, so two would keep the
// manager from starting (`@hpc-burst-science-agent`, 6 Nov 2026, for #249).
var descCollectorSuccess = prometheus.NewDesc("hpcburst_collector_success",
	"1 if the collector's last read succeeded, 0 if it failed, in which case none of its series are exposed. collector names the read: accounting (the Allocation list) or instances (the Instance inventory).",
	[]string{"collector"}, nil)

// collectorAccounting is the collector label of the accounting gauges'
// hpcburst_collector_success series.
const collectorAccounting = "accounting"

// successSample is the hpcburst_collector_success sample of one read.
func successSample(collector string, ok bool) prometheus.Metric {
	v := 0.0
	if ok {
		v = 1
	}
	return prometheus.MustNewConstMetric(descCollectorSuccess, prometheus.GaugeValue, v, collector)
}

// stateCollector is the lifecycle controller's one registered collector of
// hpcburst_ series: the series derived from the cluster's state, the
// Instances' (instanceCollector) and the accounting gauges
// (accountingCollector), each read with its hpcburst_collector_success.
//
// A copy exports them only while it serves: its readiness checks "leading"
// and "caches" pass (readiness.serving). Otherwise it exports no hpcburst_
// series, the success gauges and the accounting included; controller-runtime's
// own series stay. So one copy exports at a time, except during the overlap
// of one hand-over, which a Lease does not fence (HBEP-8, Unknown samples;
// `@hpc-burst-science-agent`, 2 and 6 Nov 2026, for #249). serving is read
// before the reads and again after them, and nothing is sent when it has
// turned false meanwhile, so a scrape that began while the copy led does not
// export once it has lost the Lease (PR #254 review, B2).
//
// It exports this copy's raised-events counter the same way (HBEP-8, Raised
// events): the counter is never registered on its own, which would bypass
// this gate, and every controller that raises events in this process runs
// only on the copy that leads the lifecycle Lease.
type stateCollector struct {
	serving    func() bool
	instances  *instanceCollector
	accounting *accountingCollector
	events     *eventcount.Counter
}

// Describe implements prometheus.Collector.
func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCollectorSuccess
	c.instances.describe(ch)
	c.accounting.describe(ch)
	c.events.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.serving() {
		return
	}
	out := c.instances.collect(nil)
	out = c.accounting.collect(out)
	out = collectEvents(out, c.events)
	if !c.serving() {
		return
	}
	for _, m := range out {
		ch <- m
	}
}

// collectEvents appends the counter's series to out.
func collectEvents(out []prometheus.Metric, events *eventcount.Counter) []prometheus.Metric {
	ch := make(chan prometheus.Metric)
	go func() {
		events.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		out = append(out, m)
	}
	return out
}

// metricsCollectors are the collectors NewManager registers: the one
// stateCollector, over the manager's cached client, the agent Backend, the
// controller's clock, this process's start as the passes count it, the
// readiness that says whether the copy serves, and the copy's raised-events
// counter.
func metricsCollectors(cache client.Reader, backend *Backend, clock Clock, started func() time.Time, serving func() bool, events *eventcount.Counter) []prometheus.Collector {
	return []prometheus.Collector{newStateCollector(cache, backend, clock, started, serving, events)}
}

// newStateCollector is the stateCollector of metricsCollectors.
func newStateCollector(cache client.Reader, backend *Backend, clock Clock, started func() time.Time, serving func() bool, events *eventcount.Counter) *stateCollector {
	return &stateCollector{
		serving:    serving,
		instances:  &instanceCollector{cache: cache, backend: backend, clock: clock, started: started},
		accounting: &accountingCollector{allocs: cache},
		events:     events,
	}
}

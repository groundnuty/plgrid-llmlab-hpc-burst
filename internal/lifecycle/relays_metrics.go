package lifecycle

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The relay keeper's series (HBEP-8, Metrics, The relay keeper; beta, issue
// #343). The keeper exports no metrics of its own and binds no port: the
// serving copy of the lifecycle controller reads its status over its socket
// and exports it through keeperCollector, under the same serving gate as
// stateCollector, so a copy that does not serve exports none of it. The
// series exist in keeper mode only (Options.RelayKeeperSocket set): in alpha
// NewManager registers no keeperCollector, so none of them is exported, nor
// hpcburst_collector_success{collector="relays"}.

var (
	descRelays = prometheus.NewDesc("hpcburst_relays",
		"Relays of the relay keeper by Provider, channel and state: up, retrying or final. A relay waiting for its pair of slots is retrying.",
		[]string{"provider", "channel", "state"}, nil)
	descRelayRestarts = prometheus.NewDesc("hpcburst_relay_restarts_total",
		"Relay starts after the first, since the relay keeper started, by Provider and channel.",
		[]string{"provider", "channel"}, nil)
	descRelaySetsRefused = prometheus.NewDesc("hpcburst_relay_sets_refused_total",
		"Desired sets and acknowledgements the relay keeper refused since it started, by Provider and reason: epoch or login_host.",
		[]string{"provider", "reason"}, nil)
	descRelayKeeperEpoch = prometheus.NewDesc("hpcburst_relay_keeper_epoch",
		"The relay keeper's epoch E; it changes when the keeper restarts, which resets its counters.",
		nil, nil)
)

// collectorRelays is the collector label of the relay keeper's
// hpcburst_collector_success series, present in keeper mode only.
const collectorRelays = "relays"

// relayStates are hpcburst_relays's state label values, in the order of
// HBEP-8's text.
var relayStates = []string{wire.StateUp, wire.StateRetrying, wire.StateFinal}

// relayMetricState is the state label of a relay the keeper reports in state
// s: up and final as they are, and anything else (retrying, waiting for its
// pair of slots, or a state this controller does not know) as retrying. HBEP-8
// counts a relay waiting for its pair of slots as retrying, so the series has
// the three states and no fourth (the science agent's ruling on #343's Q8, 10
// Nov 2026).
func relayMetricState(s string) string {
	switch s {
	case wire.StateUp, wire.StateFinal:
		return s
	}
	return wire.StateRetrying
}

// relaysCollector builds the relay keeper's series at scrape time, from one
// `status` exchange with the keeper per scrape. keeperCollector registers and
// gates it; stateCollector describes hpcburst_collector_success.
type relaysCollector struct {
	// socket is the keeper's socket path (Options.RelayKeeperSocket).
	socket string
}

// describe sends the descriptors of the relay keeper's series.
func (c *relaysCollector) describe(ch chan<- *prometheus.Desc) {
	ch <- descRelays
	ch <- descRelayRestarts
	ch <- descRelaySetsRefused
	ch <- descRelayKeeperEpoch
}

// collect appends to out the relay keeper's series and
// hpcburst_collector_success{collector="relays"}. A read that fails gives the
// gauge at 0 and none of the four series, and is logged: a refusal, an
// unreachable keeper, a reply that does not decode, and a reply that would
// make an invalid metric (the same Provider twice, a relay of no known
// channel, a label that is not UTF-8) all count. So a failed read shows as 0
// and not as no relays. It is not reported as an invalid metric:
// controller-runtime serves /metrics with HTTPErrorOnError, so one invalid
// metric would make the whole scrape answer 500 and hide the other series
// with it.
func (c *relaysCollector) collect(out []prometheus.Metric) []prometheus.Metric {
	series, err := c.read()
	if err != nil {
		ctrl.Log.WithName("metrics").Error(err, "the relay keeper's series are not exposed for this scrape")
		return append(out, successSample(collectorRelays, false))
	}
	out = append(out, successSample(collectorRelays, true))
	return append(out, series...)
}

// read asks the keeper for its status, on a connection of its own. The
// exchange is bounded by wire.ExchangeTimeout, so a keeper that accepts and
// never answers delays one scrape by that, and no longer. The series are
// built whole before any is returned, so a read that fails halfway exports
// none.
func (c *relaysCollector) read() ([]prometheus.Metric, error) {
	var rep wire.StatusReport
	if err := wire.Exchange(context.Background(), c.socket, wire.KindStatus, struct{}{}, &rep); err != nil {
		return nil, fmt.Errorf("read the relay keeper's status: %w", err)
	}
	return relaySeries(rep)
}

// relaySeries builds the four series from a status report: for every Provider
// in it, hpcburst_relays for each channel and each state (zeros included),
// the restarts of each channel and the refusals of each counted reason (0
// when absent), and the keeper's epoch. Only wire.RefusedEpoch and
// wire.RefusedLoginHost are counted (HBEP-8); the keeper's other reasons are
// not exported.
func relaySeries(rep wire.StatusReport) ([]prometheus.Metric, error) {
	var b seriesBuilder
	seen := map[string]bool{}
	for _, p := range rep.Providers {
		if seen[p.Provider] {
			return nil, fmt.Errorf("the relay keeper's report names Provider %q twice", p.Provider)
		}
		seen[p.Provider] = true
		counts, err := countRelays(p)
		if err != nil {
			return nil, err
		}
		for _, ch := range agentapi.RelayChannels {
			for _, state := range relayStates {
				b.add(descRelays, prometheus.GaugeValue, float64(counts[ch][state]), p.Provider, string(ch), state)
			}
			b.add(descRelayRestarts, prometheus.CounterValue, float64(p.Restarts[ch]), p.Provider, string(ch))
		}
		for _, reason := range []string{wire.RefusedEpoch, wire.RefusedLoginHost} {
			b.add(descRelaySetsRefused, prometheus.CounterValue, float64(p.Refused[reason]), p.Provider, reason)
		}
	}
	b.add(descRelayKeeperEpoch, prometheus.GaugeValue, float64(rep.KeeperEpoch))
	return b.out, b.err
}

// countRelays counts a Provider's relays by channel and metric state. A relay
// of a channel that is neither control nor serving cannot come from a keeper
// of this release, and counting it nowhere would understate the relays, so it
// fails the read.
func countRelays(p wire.ProviderStatus) (map[agentapi.RelayChannel]map[string]int, error) {
	counts := map[agentapi.RelayChannel]map[string]int{}
	for _, ch := range agentapi.RelayChannels {
		counts[ch] = map[string]int{}
	}
	for _, r := range p.Relays {
		byState, ok := counts[r.Channel]
		if !ok {
			return nil, fmt.Errorf("the relay keeper's report has a relay of Provider %q on channel %q", p.Provider, r.Channel)
		}
		byState[relayMetricState(r.State)]++
	}
	return counts, nil
}

// seriesBuilder collects const metrics and keeps the first error that
// prometheus.NewConstMetric reports, so that a label it refuses fails the
// read and does not panic the scrape.
type seriesBuilder struct {
	out []prometheus.Metric
	err error
}

func (b *seriesBuilder) add(d *prometheus.Desc, t prometheus.ValueType, v float64, labels ...string) {
	if b.err != nil {
		return
	}
	m, err := prometheus.NewConstMetric(d, t, v, labels...)
	if err != nil {
		b.err = fmt.Errorf("the relay keeper's report makes an invalid metric: %w", err)
		return
	}
	b.out = append(b.out, m)
}

// keeperCollector is the relay keeper's own registered collector, beside
// stateCollector, in beta's keeper mode only (keeperCollectors), so alpha's
// registration is the same call as before. It exports the four relay series
// and hpcburst_collector_success{collector="relays"}, only while the copy
// serves: serving is read before the read and again after it, as
// stateCollector does, so a scrape that began while the copy served exports
// nothing once it has stopped. It describes the four relay descriptors only:
// stateCollector describes the shared success gauge, which client_golang lets
// one collector describe and any registered collector export.
type keeperCollector struct {
	serving func() bool
	relays  *relaysCollector
}

// Describe implements prometheus.Collector.
func (c *keeperCollector) Describe(ch chan<- *prometheus.Desc) { c.relays.describe(ch) }

// Collect implements prometheus.Collector.
func (c *keeperCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.serving() {
		return
	}
	out := c.relays.collect(nil)
	if !c.serving() {
		return
	}
	for _, m := range out {
		ch <- m
	}
}

// keeperCollectors are the collectors NewManager registers beside
// metricsCollectors' for the relay keeper at socket, Options.RelayKeeperSocket:
// none for "", alpha.
func keeperCollectors(socket string, serving func() bool) []prometheus.Collector {
	if socket == "" {
		return nil
	}
	return []prometheus.Collector{&keeperCollector{serving: serving, relays: &relaysCollector{socket: socket}}}
}

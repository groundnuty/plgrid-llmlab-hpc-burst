package lifecycle

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The tests in this file cover HBEP-8, Metrics, The relay keeper, for #343:
// the serving copy's collector reads the keeper's status over its socket and
// exports the relays' series. The keeper is a fake at the socket level: each
// test listens on a Unix socket with wire.Listen and answers with wire.Serve,
// so the collector's exchange, framing and decoding are the real ones. The
// expected values are literals written here from the scripted report, never
// computed by the collector. Every scrape goes through a pedantic registry
// and promhttp with HTTPErrorOnError (scrapeCollectors), which fails the test
// on any invalid metric, and on any series whose descriptor the collector did
// not describe.

// The four series the relay keeper's status gives, besides
// hpcburst_collector_success{collector="relays"}.
var relaySeriesNames = []string{
	"hpcburst_relays",
	"hpcburst_relay_restarts_total",
	"hpcburst_relay_sets_refused_total",
	"hpcburst_relay_keeper_epoch",
}

// fakeKeeper is a keeper at a socket that answers each status request with
// what reply says for that request's number, counting from 0.
type fakeKeeper struct {
	socket string
	calls  atomic.Int32
}

// shortSocketDir makes a directory under $TMPDIR with a path short enough for
// a socket (t.TempDir embeds the test's name, which is too long), removed at
// the test's end.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// startFakeKeeper listens on a socket of its own and serves until the test
// ends. A request that is not a status with an empty body fails the test: the
// serving copy's read sends nothing else.
func startFakeKeeper(t *testing.T, reply func(call int) (wire.Kind, any)) *fakeKeeper {
	t.Helper()
	path := filepath.Join(shortSocketDir(t), "k.sock")
	if len(path) > agentapi.MaxRelaySocketPath {
		t.Fatalf("the socket path %q is %d bytes, over the %d a socket takes: shorten $TMPDIR", path, len(path), agentapi.MaxRelaySocketPath)
	}
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	k := &fakeKeeper{socket: path}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- wire.Serve(ctx, ln, func(_ context.Context, req wire.Envelope) (wire.Kind, any) {
			n := int(k.calls.Add(1)) - 1
			if req.Kind != wire.KindStatus || string(req.Body) != "{}" {
				t.Errorf("the fake keeper received a %s with body %q, want a status with body {}", req.Kind, req.Body)
			}
			return reply(n)
		}, nil)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the fake keeper's Serve: %v", err)
		}
	})
	return k
}

// keeperReporting is a keeper that answers every status request with rep.
func keeperReporting(t *testing.T, rep wire.StatusReport) *fakeKeeper {
	t.Helper()
	return startFakeKeeper(t, func(int) (wire.Kind, any) { return wire.KindStatusReport, rep })
}

// socketWithNoListener is a socket path with nothing at it.
func socketWithNoListener(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortSocketDir(t), "none.sock")
}

// relayCollectors are the collectors NewManager registers, as it builds them
// (metricsCollectorsWithKeeper), over a store with one Instance and one
// Allocation whose accounting is known, so that the instances and accounting
// reads have series and the keeper's reads can be told from them. socket is
// Options.RelayKeeperSocket.
func relayCollectors(t *testing.T, socket string, serving func() bool) []prometheus.Collector {
	t.Helper()
	started := time.Now()
	return metricsCollectorsWithKeeper(gateStore(t), &Backend{}, SystemClock{}, func() time.Time { return started }, serving, eventcount.New(), socket)
}

// scrapeRelays serves a copy that serves, with the keeper at socket, as
// /metrics and parses the text.
func scrapeRelays(t *testing.T, socket string) exposition {
	t.Helper()
	return scrapeCollectors(t, relayCollectors(t, socket, always)...)
}

// relayRelay is a relay of the report: the fields that the series do not read
// carry plausible values, so the report is one the keeper could send.
func relayRelay(uid string, channel agentapi.RelayChannel, state string) wire.RelayState {
	return wire.RelayState{UID: uid, Tag: "tag-" + uid, JobID: "1001", Channel: channel, State: state, Epoch: 7, Seq: 1, Starts: 1, LoginHost: "login.example.org", ProviderUID: "puid-1"}
}

// scriptedReport is the keeper's report the series tests script: two
// Providers. example-provider-a has five relays, restarts on both channels,
// and three reasons of refusals, one of them one the series do not count.
// example-provider-b has nothing at all.
func scriptedReport() wire.StatusReport {
	return wire.StatusReport{
		KeeperEpoch: 7,
		Started:     time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC),
		Providers: []wire.ProviderStatus{
			{
				Provider: "example-provider-a",
				Relays: []wire.RelayState{
					relayRelay("uid-1", agentapi.RelayControl, wire.StateUp),
					relayRelay("uid-2", agentapi.RelayControl, wire.StateUp),
					relayRelay("uid-3", agentapi.RelayControl, wire.StateFinal),
					relayRelay("uid-1", agentapi.RelayServing, wire.StateUp),
					relayRelay("uid-2", agentapi.RelayServing, wire.StateRetrying),
				},
				Restarts: map[agentapi.RelayChannel]uint64{agentapi.RelayControl: 3, agentapi.RelayServing: 1},
				Refused:  map[string]uint64{wire.RefusedEpoch: 2, wire.RefusedLoginHost: 1, wire.RefusedVersion: 9},
			},
			{Provider: "example-provider-b"},
		},
	}
}

// floatOf parses a sample's value.
func floatOf(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("sample value %q: %v", s, err)
	}
	return v
}

// seriesValues returns the samples of a name as a map from their label values,
// joined with "/" in the order of labels, to their values.
func seriesValues(t *testing.T, ex exposition, name string, labels ...string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, s := range ex.find(name, nil) {
		if len(s.labels) != len(labels) {
			t.Fatalf("%s has labels %v, want exactly %v", name, s.labels, labels)
		}
		var parts []string
		for _, l := range labels {
			v, ok := s.labels[l]
			if !ok {
				t.Fatalf("%s has no %s label: %v", name, l, s.labels)
			}
			parts = append(parts, v)
		}
		key := strings.Join(parts, "/")
		if _, dup := out[key]; dup {
			t.Fatalf("%s{%s} is exposed twice", name, key)
		}
		out[key] = floatOf(t, s.value)
	}
	return out
}

// wantValues fails unless got is exactly want.
func wantValues(t *testing.T, name string, got, want map[string]float64) {
	t.Helper()
	if !maps.Equal(got, want) {
		t.Errorf("%s:\n got  %v\n want %v", name, got, want)
	}
}

// noRelaySeries fails if the scrape has any of the four series.
func noRelaySeries(t *testing.T, when string, ex exposition) {
	t.Helper()
	for _, name := range relaySeriesNames {
		if got := ex.find(name, nil); len(got) != 0 {
			t.Errorf("%s: %s has %d series, want none: %v", when, name, len(got), got)
		}
	}
}

// checkOtherReadsSucceed fails unless the accounting and instances reads
// still have their success series at 1: the keeper's read neither hides them
// nor changes them.
func checkOtherReadsSucceed(t *testing.T, when string, ex exposition) {
	t.Helper()
	for _, c := range []string{collectorAccounting, collectorInstances} {
		if got := collectorSuccess(t, ex, c); got != "1" {
			t.Errorf("%s: hpcburst_collector_success{collector=%q} %s, want 1", when, c, got)
		}
	}
}

// TestRelaysGaugeCountsRelaysByProviderChannelAndState: hpcburst_relays has,
// for every Provider in the report, a series for each channel and each state,
// zeros included. The scripted report has 12 of them.
func TestRelaysGaugeCountsRelaysByProviderChannelAndState(t *testing.T) {
	ex := scrapeRelays(t, keeperReporting(t, scriptedReport()).socket)
	wantValues(t, "hpcburst_relays", seriesValues(t, ex, "hpcburst_relays", "provider", "channel", "state"), map[string]float64{
		"example-provider-a/control/up":       2,
		"example-provider-a/control/retrying": 0,
		"example-provider-a/control/final":    1,
		"example-provider-a/serving/up":       1,
		"example-provider-a/serving/retrying": 1,
		"example-provider-a/serving/final":    0,
		"example-provider-b/control/up":       0,
		"example-provider-b/control/retrying": 0,
		"example-provider-b/control/final":    0,
		"example-provider-b/serving/up":       0,
		"example-provider-b/serving/retrying": 0,
		"example-provider-b/serving/final":    0,
	})
	if got := ex.types["hpcburst_relays"]; got != "gauge" {
		t.Errorf("hpcburst_relays is a %s, want a gauge", got)
	}
}

// TestRelaysRestartsCounterHasBothChannelsOfEachProvider:
// hpcburst_relay_restarts_total is the report's Restarts, a counter, with 0
// for a channel the report does not name.
func TestRelaysRestartsCounterHasBothChannelsOfEachProvider(t *testing.T) {
	ex := scrapeRelays(t, keeperReporting(t, scriptedReport()).socket)
	wantValues(t, "hpcburst_relay_restarts_total", seriesValues(t, ex, "hpcburst_relay_restarts_total", "provider", "channel"), map[string]float64{
		"example-provider-a/control": 3,
		"example-provider-a/serving": 1,
		"example-provider-b/control": 0,
		"example-provider-b/serving": 0,
	})
	if got := ex.types["hpcburst_relay_restarts_total"]; got != "counter" {
		t.Errorf("hpcburst_relay_restarts_total is a %s, want a counter", got)
	}
}

// TestRelaysSetsRefusedCounterHasTheTwoCountedReasons:
// hpcburst_relay_sets_refused_total has the reasons epoch and login_host for
// each Provider, 0 when absent, and no series for the report's other reasons:
// provider a's report counts 9 refusals for the version, which HBEP-8 does not
// export.
func TestRelaysSetsRefusedCounterHasTheTwoCountedReasons(t *testing.T) {
	ex := scrapeRelays(t, keeperReporting(t, scriptedReport()).socket)
	wantValues(t, "hpcburst_relay_sets_refused_total", seriesValues(t, ex, "hpcburst_relay_sets_refused_total", "provider", "reason"), map[string]float64{
		"example-provider-a/epoch":      2,
		"example-provider-a/login_host": 1,
		"example-provider-b/epoch":      0,
		"example-provider-b/login_host": 0,
	})
	if got := ex.types["hpcburst_relay_sets_refused_total"]; got != "counter" {
		t.Errorf("hpcburst_relay_sets_refused_total is a %s, want a counter", got)
	}
}

// TestRelaysKeeperEpochIsOneGauge: hpcburst_relay_keeper_epoch is one gauge
// with no labels, the report's KeeperEpoch.
func TestRelaysKeeperEpochIsOneGauge(t *testing.T) {
	ex := scrapeRelays(t, keeperReporting(t, scriptedReport()).socket)
	if got := seriesValues(t, ex, "hpcburst_relay_keeper_epoch"); !maps.Equal(got, map[string]float64{"": 7}) {
		t.Errorf("hpcburst_relay_keeper_epoch %v, want one series with value 7", got)
	}
	if got := ex.types["hpcburst_relay_keeper_epoch"]; got != "gauge" {
		t.Errorf("hpcburst_relay_keeper_epoch is a %s, want a gauge", got)
	}
}

// TestRelaysSuccessIsOneAfterAGoodRead: in keeper mode
// hpcburst_collector_success has a relays series, 1 after a read that worked,
// next to the other two reads' at 1, and its help names all three reads.
func TestRelaysSuccessIsOneAfterAGoodRead(t *testing.T) {
	ex := scrapeRelays(t, keeperReporting(t, scriptedReport()).socket)
	if got := collectorSuccess(t, ex, collectorRelays); got != "1" {
		t.Errorf("hpcburst_collector_success{collector=\"relays\"} %s, want 1", got)
	}
	checkOtherReadsSucceed(t, "a good read of the keeper", ex)
	if n := len(ex.find("hpcburst_collector_success", nil)); n != 3 {
		t.Errorf("%d hpcburst_collector_success series, want 3: accounting, instances and relays", n)
	}
	help := ex.help["hpcburst_collector_success"]
	for _, c := range []string{"accounting", "instances", "relays"} {
		if !strings.Contains(help, c) {
			t.Errorf("hpcburst_collector_success's help %q does not name the %s read", help, c)
		}
	}
}

// TestRelaysAWaitingRelayCountsAsRetrying (HBEP-8; #343's Q8): the keeper's
// state for a relay that waits for its pair of slots is "waiting", and
// hpcburst_relays counts it as retrying, as it counts a relay that is
// "retrying". The other two states of its channel stay at 0.
func TestRelaysAWaitingRelayCountsAsRetrying(t *testing.T) {
	rep := wire.StatusReport{KeeperEpoch: 3, Providers: []wire.ProviderStatus{{
		Provider: "example-provider-a",
		Relays: []wire.RelayState{
			relayRelay("uid-1", agentapi.RelayServing, "waiting"),
			relayRelay("uid-1", agentapi.RelayControl, wire.StateRetrying),
		},
	}}}
	ex := scrapeRelays(t, keeperReporting(t, rep).socket)
	wantValues(t, "hpcburst_relays", seriesValues(t, ex, "hpcburst_relays", "provider", "channel", "state"), map[string]float64{
		"example-provider-a/control/up":       0,
		"example-provider-a/control/retrying": 1,
		"example-provider-a/control/final":    0,
		"example-provider-a/serving/up":       0,
		"example-provider-a/serving/retrying": 1,
		"example-provider-a/serving/final":    0,
	})
}

// TestRelaysAStateThatIsNotKnownCountsAsRetrying: a state this controller
// does not know is retrying, not a fourth state, so the series keep HBEP-8's
// three.
func TestRelaysAStateThatIsNotKnownCountsAsRetrying(t *testing.T) {
	rep := wire.StatusReport{KeeperEpoch: 3, Providers: []wire.ProviderStatus{{
		Provider: "example-provider-a",
		Relays: []wire.RelayState{
			relayRelay("uid-1", agentapi.RelayControl, "starting"),
			relayRelay("uid-2", agentapi.RelayControl, ""),
			relayRelay("uid-3", agentapi.RelayControl, wire.StateUp),
		},
	}}}
	ex := scrapeRelays(t, keeperReporting(t, rep).socket)
	wantValues(t, "hpcburst_relays", seriesValues(t, ex, "hpcburst_relays", "provider", "channel", "state"), map[string]float64{
		"example-provider-a/control/up":       1,
		"example-provider-a/control/retrying": 2,
		"example-provider-a/control/final":    0,
		"example-provider-a/serving/up":       0,
		"example-provider-a/serving/retrying": 0,
		"example-provider-a/serving/final":    0,
	})
}

// TestRelaysAFailedReadGivesZeroAndNoRelaySeries: a read of the keeper that
// fails gives hpcburst_collector_success{collector="relays"} 0 and none of the
// four series, so that it shows as 0 and not as no relays; the scrape still
// answers 200 (scrapeCollectors fails otherwise) and the other two reads are
// untouched. Every case runs the same harness, and the first is a keeper that
// works: its success 1 and its series are what a failed read must differ from,
// so a collector that exports nothing for every case, or 0 for every case,
// fails it.
func TestRelaysAFailedReadGivesZeroAndNoRelaySeries(t *testing.T) {
	refuse := func(reason string) func(int) (wire.Kind, any) {
		return func(int) (wire.Kind, any) {
			return wire.KindRefused, wire.Refused{Reason: reason, Message: "refused by the test"}
		}
	}
	reply := func(kind wire.Kind, body any) func(int) (wire.Kind, any) {
		return func(int) (wire.Kind, any) { return kind, body }
	}
	twice := scriptedReport()
	twice.Providers = append(twice.Providers, wire.ProviderStatus{Provider: "example-provider-a"})
	oddChannel := scriptedReport()
	oddChannel.Providers[0].Relays = append(oddChannel.Providers[0].Relays, relayRelay("uid-9", "third", wire.StateUp))
	cases := []struct {
		name   string
		socket func(t *testing.T) string
		ok     bool
	}{
		{"a keeper that works", func(t *testing.T) string { return keeperReporting(t, scriptedReport()).socket }, true},
		{"no listener at the socket", socketWithNoListener, false},
		{"a keeper that refuses with state", func(t *testing.T) string {
			return startFakeKeeper(t, refuse(wire.RefusedState)).socket
		}, false},
		{"a reply with a field that is not the keeper's", func(t *testing.T) string {
			return startFakeKeeper(t, reply(wire.KindStatusReport, map[string]any{"keeperEpoch": 1, "surprise": true})).socket
		}, false},
		{"a reply of the wrong kind", func(t *testing.T) string {
			return startFakeKeeper(t, reply(wire.KindPong, wire.Pong{KeeperEpoch: 1})).socket
		}, false},
		{"the same Provider twice", func(t *testing.T) string { return keeperReporting(t, twice).socket }, false},
		{"a relay on a channel that is not control or serving", func(t *testing.T) string { return keeperReporting(t, oddChannel).socket }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := scrapeRelays(t, tc.socket(t))
			checkOtherReadsSucceed(t, tc.name, ex)
			if tc.ok {
				if got := collectorSuccess(t, ex, collectorRelays); got != "1" {
					t.Errorf("hpcburst_collector_success{collector=\"relays\"} %s, want 1", got)
				}
				if n := len(ex.find("hpcburst_relays", nil)); n != 12 {
					t.Errorf("%d hpcburst_relays series from a keeper that works, want 12", n)
				}
				return
			}
			if got := collectorSuccess(t, ex, collectorRelays); got != "0" {
				t.Errorf("hpcburst_collector_success{collector=\"relays\"} %s, want 0", got)
			}
			noRelaySeries(t, tc.name, ex)
		})
	}
}

// TestRelaysAKeeperThatComesBackIsReadAgain: the read is made at each scrape,
// so after a refusal the next scrape that the keeper answers has the series
// and success 1 again. The first scrape of the same collector, on the same
// socket, is the failure.
func TestRelaysAKeeperThatComesBackIsReadAgain(t *testing.T) {
	k := startFakeKeeper(t, func(call int) (wire.Kind, any) {
		if call == 0 {
			return wire.KindRefused, wire.Refused{Reason: wire.RefusedState, Message: "the state file could not be written"}
		}
		return wire.KindStatusReport, scriptedReport()
	})
	cs := relayCollectors(t, k.socket, always)
	first := scrapeCollectors(t, cs...)
	if got := collectorSuccess(t, first, collectorRelays); got != "0" {
		t.Fatalf("the scrape the keeper refused: success %s, want 0", got)
	}
	noRelaySeries(t, "the scrape the keeper refused", first)
	second := scrapeCollectors(t, cs...)
	if got := collectorSuccess(t, second, collectorRelays); got != "1" {
		t.Fatalf("the next scrape: success %s, want 1", got)
	}
	if n := len(second.find("hpcburst_relays", nil)); n != 12 {
		t.Errorf("the next scrape has %d hpcburst_relays series, want 12", n)
	}
	if got := k.calls.Load(); got != 2 {
		t.Errorf("the keeper received %d status requests for 2 scrapes, want 2", got)
	}
}

// TestRelaysAlphaReadsNoKeeperAndExportsNoRelaySeries: with no socket (alpha)
// the collector exports no relay series and no
// hpcburst_collector_success{collector="relays"}, though the other two reads'
// success series are there, and it makes no exchange. The control is the same
// harness with the socket of a live keeper: it exports all of them, so the
// alpha scrape's silence is the socket's absence and not a harness that never
// shows the series.
func TestRelaysAlphaReadsNoKeeperAndExportsNoRelaySeries(t *testing.T) {
	k := keeperReporting(t, scriptedReport())

	alpha := scrapeRelays(t, "")
	checkOtherReadsSucceed(t, "alpha", alpha)
	noRelaySeries(t, "alpha", alpha)
	if got := alpha.find("hpcburst_collector_success", map[string]string{"collector": collectorRelays}); len(got) != 0 {
		t.Errorf("alpha exports hpcburst_collector_success{collector=\"relays\"}: %v, want none", got)
	}
	if n := len(alpha.find("hpcburst_collector_success", nil)); n != 2 {
		t.Errorf("alpha has %d hpcburst_collector_success series, want 2 (accounting and instances)", n)
	}
	if got := k.calls.Load(); got != 0 {
		t.Errorf("alpha made %d exchanges with a keeper, want none", got)
	}

	beta := scrapeRelays(t, k.socket)
	if got := collectorSuccess(t, beta, collectorRelays); got != "1" {
		t.Errorf("the control, with the keeper's socket: success %s, want 1", got)
	}
	if n := len(beta.find("hpcburst_relays", nil)); n != 12 {
		t.Errorf("the control has %d hpcburst_relays series, want 12", n)
	}
	if got := k.calls.Load(); got != 1 {
		t.Errorf("the control made %d exchanges, want 1", got)
	}
}

// TestRelaysACopyThatDoesNotServeExportsNoRelaySeries: the relay series are
// read inside the serving gate, so a copy that does not serve exports none of
// them, hpcburst_collector_success{collector="relays"} included, and does not
// read the keeper. The control is the same collector once it serves, and it
// stops again.
func TestRelaysACopyThatDoesNotServeExportsNoRelaySeries(t *testing.T) {
	k := keeperReporting(t, scriptedReport())
	var serving atomic.Bool
	cs := relayCollectors(t, k.socket, serving.Load)

	if got := hpcburstSamples(scrapeCollectors(t, cs...)); len(got) != 0 {
		t.Fatalf("a copy that does not serve exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
	if got := k.calls.Load(); got != 0 {
		t.Errorf("a copy that does not serve read the keeper %d times, want 0", got)
	}

	serving.Store(true)
	ex := scrapeCollectors(t, cs...)
	if got := collectorSuccess(t, ex, collectorRelays); got != "1" {
		t.Fatalf("the control, once it serves: success %s, want 1", got)
	}
	if n := len(ex.find("hpcburst_relays", nil)); n != 12 {
		t.Errorf("the control has %d hpcburst_relays series, want 12", n)
	}
	if got := k.calls.Load(); got != 1 {
		t.Errorf("the control read the keeper %d times, want 1", got)
	}

	serving.Store(false)
	if got := hpcburstSamples(scrapeCollectors(t, cs...)); len(got) != 0 {
		t.Errorf("a copy that stopped serving exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
}

// TestRelaysACopyThatStopsServingDuringTheReadExportsNothing: serving is read
// again after the reads, so a copy that lost its Lease while the keeper
// answered exports none of that scrape. The control is the same scrape with
// serving left on.
func TestRelaysACopyThatStopsServingDuringTheReadExportsNothing(t *testing.T) {
	var serving, flip atomic.Bool
	k := startFakeKeeper(t, func(int) (wire.Kind, any) {
		if flip.Load() {
			serving.Store(false)
		}
		return wire.KindStatusReport, scriptedReport()
	})
	cs := relayCollectors(t, k.socket, serving.Load)

	serving.Store(true)
	ex := scrapeCollectors(t, cs...)
	if got := collectorSuccess(t, ex, collectorRelays); got != "1" {
		t.Fatalf("the control, a copy that serves throughout: success %s, want 1", got)
	}

	flip.Store(true)
	serving.Store(true)
	if got := hpcburstSamples(scrapeCollectors(t, cs...)); len(got) != 0 {
		t.Errorf("a copy that stopped serving during the read exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
}

// TestRelaysAKeeperResetShowsInTheEpochGauge: when the keeper restarts its
// counters start again, and a counter that fell is not a reset to a reader who
// cannot tell. hpcburst_relay_keeper_epoch changes with it, so a reset shows.
// Two reads of one collector: the keeper's E is 2147483649, then 2147483650
// (above 2^31, which a 32-bit value would not hold), and the restarts of
// provider a's control channel go from 5 down to 1.
func TestRelaysAKeeperResetShowsInTheEpochGauge(t *testing.T) {
	report := func(epoch int64, control, serving uint64) wire.StatusReport {
		return wire.StatusReport{KeeperEpoch: epoch, Providers: []wire.ProviderStatus{{
			Provider: "example-provider-a",
			Restarts: map[agentapi.RelayChannel]uint64{agentapi.RelayControl: control, agentapi.RelayServing: serving},
		}}}
	}
	k := startFakeKeeper(t, func(call int) (wire.Kind, any) {
		if call == 0 {
			return wire.KindStatusReport, report(2147483649, 5, 2)
		}
		return wire.KindStatusReport, report(2147483650, 1, 0)
	})
	cs := relayCollectors(t, k.socket, always)

	first := scrapeCollectors(t, cs...)
	if got := seriesValues(t, first, "hpcburst_relay_keeper_epoch"); !maps.Equal(got, map[string]float64{"": 2147483649}) {
		t.Errorf("first read: hpcburst_relay_keeper_epoch %v, want 2147483649", got)
	}
	wantValues(t, "first read's hpcburst_relay_restarts_total", seriesValues(t, first, "hpcburst_relay_restarts_total", "provider", "channel"), map[string]float64{
		"example-provider-a/control": 5,
		"example-provider-a/serving": 2,
	})

	second := scrapeCollectors(t, cs...)
	if got := seriesValues(t, second, "hpcburst_relay_keeper_epoch"); !maps.Equal(got, map[string]float64{"": 2147483650}) {
		t.Errorf("second read: hpcburst_relay_keeper_epoch %v, want 2147483650", got)
	}
	wantValues(t, "second read's hpcburst_relay_restarts_total", seriesValues(t, second, "hpcburst_relay_restarts_total", "provider", "channel"), map[string]float64{
		"example-provider-a/control": 1,
		"example-provider-a/serving": 0,
	})
}

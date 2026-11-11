package lifecycle

// The alpha pin of the keeper's metrics (the science agent's ruling of 10 Nov
// 2026 on PR 360): NewManager's registration of metricsCollectors is the same
// call as before, and keeperCollectors adds a collector beside it only in
// keeper mode. So with Options.RelayKeeperSocket "" the collectors NewManager
// registers, metricsCollectors' and keeperCollectors("")'s, must describe and
// export exactly what metricsCollectors alone does, byte for byte. The
// reference is metricsCollectors itself, unchanged; keeper mode, the positive
// control, must add exactly HBEP-8's five keeper series and change nothing
// else.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// pinClock is a clock that does not move, so two scrapes of the same state
// give the same text.
type pinClock struct{ t time.Time }

func (c pinClock) Now() time.Time { return c.t }

// rawScrape serves cs through a pedantic registry as /metrics and returns the
// text as served.
func rawScrape(t *testing.T, cs []prometheus.Collector) string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(cs...)
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: %d", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// describeAll is every descriptor cs describe, as text, sorted.
func describeAll(cs []prometheus.Collector) []string {
	ch := make(chan *prometheus.Desc)
	go func() {
		for _, c := range cs {
			c.Describe(ch)
		}
		close(ch)
	}()
	var out []string
	for d := range ch {
		out = append(out, d.String())
	}
	sort.Strings(out)
	return out
}

// sampleLines are the text's sample lines, without HELP and TYPE.
func sampleLines(text string) map[string]bool {
	out := map[string]bool{}
	for _, l := range strings.Split(text, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			out[l] = true
		}
	}
	return out
}

func TestRelaysAlphaRegistersExactlyMetricsCollectorsAndKeeperModeAddsTheFiveSeries(t *testing.T) {
	k := keeperReporting(t, scriptedReport())
	store := gateStore(t)
	clock := pinClock{time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)}
	started := clock.t.Add(-time.Hour)
	reference := func() []prometheus.Collector {
		return metricsCollectors(store, &Backend{}, clock, func() time.Time { return started }, always, eventcount.New())
	}
	registered := func(socket string) []prometheus.Collector {
		cs := metricsCollectors(store, &Backend{}, clock, func() time.Time { return started }, always, eventcount.New())
		return append(cs, keeperCollectors(socket, always)...)
	}

	// Alpha: byte for byte the reference's.
	if n := len(keeperCollectors("", always)); n != 0 {
		t.Fatalf("alpha registers %d keeper collectors, want none", n)
	}
	refDescs, alphaDescs := describeAll(reference()), describeAll(registered(""))
	if strings.Join(refDescs, "\n") != strings.Join(alphaDescs, "\n") {
		t.Errorf("alpha's descriptors differ from metricsCollectors':\n%s\nwant\n%s", strings.Join(alphaDescs, "\n"), strings.Join(refDescs, "\n"))
	}
	refText, alphaText := rawScrape(t, reference()), rawScrape(t, registered(""))
	if refText != alphaText {
		t.Errorf("alpha's /metrics differs from metricsCollectors':\n%s\nwant\n%s", alphaText, refText)
	}
	if strings.Contains(refText, "hpcburst_relay") || strings.Contains(refText, `collector="relays"`) {
		t.Fatalf("the reference itself has a keeper series:\n%s", refText)
	}
	if k.calls.Load() != 0 {
		t.Errorf("alpha read the keeper %d times", k.calls.Load())
	}

	// Keeper mode, the positive control: the reference's descriptors plus the
	// four keeper descriptors (the success gauge is the reference's), and its
	// samples plus exactly the five keeper series, none of the reference's
	// changed or missing.
	betaDescs := describeAll(registered(k.socket))
	inRef := map[string]bool{}
	for _, d := range refDescs {
		inRef[d] = true
	}
	var added []string
	for _, d := range betaDescs {
		if !inRef[d] {
			added = append(added, d)
		}
	}
	if len(betaDescs) != len(refDescs)+4 || len(added) != 4 {
		t.Errorf("keeper mode describes %d descriptors, %d new, want the reference's %d plus 4: %v", len(betaDescs), len(added), len(refDescs), added)
	}
	for _, name := range []string{"hpcburst_relays", "hpcburst_relay_restarts_total", "hpcburst_relay_sets_refused_total", "hpcburst_relay_keeper_epoch"} {
		found := false
		for _, d := range added {
			found = found || strings.Contains(d, `fqName: "`+name+`"`)
		}
		if !found {
			t.Errorf("keeper mode does not describe %s: %v", name, added)
		}
	}

	ref, beta := sampleLines(refText), sampleLines(rawScrape(t, registered(k.socket)))
	for l := range ref {
		if !beta[l] {
			t.Errorf("keeper mode lost or changed the reference's sample %q", l)
		}
	}
	kinds := map[string]int{}
	for l := range beta {
		if ref[l] {
			continue
		}
		switch name := l[:strings.IndexAny(l, "{ ")]; {
		case name == "hpcburst_collector_success" && strings.Contains(l, `collector="relays"`):
			kinds[`hpcburst_collector_success{collector="relays"}`]++
		case name == "hpcburst_relays", name == "hpcburst_relay_restarts_total", name == "hpcburst_relay_sets_refused_total", name == "hpcburst_relay_keeper_epoch":
			kinds[name]++
		default:
			t.Errorf("keeper mode adds a sample that is not one of the five keeper series: %q", l)
		}
	}
	want := []string{"hpcburst_relays", "hpcburst_relay_restarts_total", "hpcburst_relay_sets_refused_total", "hpcburst_relay_keeper_epoch", `hpcburst_collector_success{collector="relays"}`}
	if len(kinds) != len(want) {
		t.Errorf("keeper mode adds %d kinds of series, want exactly the five: %v", len(kinds), kinds)
	}
	for _, w := range want {
		if kinds[w] == 0 {
			t.Errorf("keeper mode does not add %s", w)
		}
	}
	if kinds[`hpcburst_collector_success{collector="relays"}`] != 1 || kinds["hpcburst_relay_keeper_epoch"] != 1 {
		t.Errorf("keeper mode adds %v; want one success{relays} sample and one epoch sample", kinds)
	}
}

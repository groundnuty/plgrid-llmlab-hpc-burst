package lifecycle

// NewManager itself in both modes (the review of PR 360): the in-memory pin
// (relays_alpha_pin_test.go) compares the collectors NewManager's code builds;
// this one runs NewManager's own registration, on the serving rig's control
// plane, and reads each copy's /metrics as served.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// seriesKeys are a /metrics body's hpcburst_ series, each by its name and
// labels without its value, leaving out the raised-events counter, whose
// series appear at each copy's own first event.
func seriesKeys(body string) map[string]bool {
	out := map[string]bool{}
	for _, l := range hpcburstLines(body) {
		key := l
		if i := strings.LastIndex(l, " "); i > 0 {
			key = l[:i]
		}
		if strings.HasPrefix(key, eventcount.Name) {
			continue
		}
		out[key] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestMetricsKeeperModeThroughNewManager: a copy that NewManager builds with
// no RelayKeeperSocket serves exactly alpha's series, two success series and
// no keeper series, and never reads the keeper; a copy built with the keeper's
// socket, over the same state, serves the same series plus exactly HBEP-8's
// five keeper series. The alpha copy is the reference for the keeper copy, and
// the keeper copy is the positive control for the alpha copy.
func TestMetricsKeeperModeThroughNewManager(t *testing.T) {
	k := keeperReporting(t, scriptedReport())
	g := newServingRig(t)
	defer g.logPorts(t)

	alpha, pa := g.startCopy(t, "the alpha copy", Options{})
	g.awaitServing(t, "the alpha copy", pa)
	_, alphaBody := scrapeText(pa.metrics)
	alphaSeries := seriesKeys(alphaBody)
	for key := range alphaSeries {
		if strings.HasPrefix(key, "hpcburst_relay") || strings.Contains(key, `collector="relays"`) {
			t.Errorf("the alpha copy serves the keeper series %s", key)
		}
	}
	var success []string
	for key := range alphaSeries {
		if strings.HasPrefix(key, "hpcburst_collector_success{") {
			success = append(success, key)
		}
	}
	sort.Strings(success)
	if want := []string{`hpcburst_collector_success{collector="accounting"}`, `hpcburst_collector_success{collector="instances"}`}; fmt.Sprint(success) != fmt.Sprint(want) {
		t.Errorf("the alpha copy's success series are %v, want %v", success, want)
	}
	if n := k.calls.Load(); n != 0 {
		t.Errorf("the alpha copy read the keeper %d times", n)
	}
	if !alpha.stop(t, "the alpha copy") {
		t.FailNow()
	}
	awaitUnregistered(t)

	beta, pb := g.startCopy(t, "the keeper copy", Options{RelayKeeperSocket: k.socket})
	g.awaitServing(t, "the keeper copy", pb)
	var betaBody string
	eventually(t, time.Minute, "the keeper copy serves success{relays} 1", func() (bool, string) {
		_, betaBody = scrapeText(pb.metrics)
		return strings.Contains(betaBody, `hpcburst_collector_success{collector="relays"} 1`), strings.Join(hpcburstLines(betaBody), "\n")
	})
	betaSeries := seriesKeys(betaBody)
	for key := range alphaSeries {
		if !betaSeries[key] {
			t.Errorf("the keeper copy lacks alpha's series %s", key)
		}
	}
	kinds := map[string]int{}
	for _, key := range sortedKeys(betaSeries) {
		if alphaSeries[key] {
			continue
		}
		name := key
		if i := strings.Index(key, "{"); i > 0 {
			name = key[:i]
		}
		switch {
		case key == `hpcburst_collector_success{collector="relays"}`:
			kinds[key]++
		case name == "hpcburst_relays", name == "hpcburst_relay_restarts_total", name == "hpcburst_relay_sets_refused_total", name == "hpcburst_relay_keeper_epoch":
			kinds[name]++
		default:
			t.Errorf("the keeper copy serves a series that is neither alpha's nor one of the five keeper series: %s", key)
		}
	}
	for _, want := range []string{"hpcburst_relays", "hpcburst_relay_restarts_total", "hpcburst_relay_sets_refused_total", "hpcburst_relay_keeper_epoch", `hpcburst_collector_success{collector="relays"}`} {
		if kinds[want] == 0 {
			t.Errorf("the keeper copy does not serve %s; it adds %v", want, kinds)
		}
	}
	if len(kinds) != 5 {
		t.Errorf("the keeper copy adds %d kinds of series, want exactly the five: %v", len(kinds), kinds)
	}
	if !beta.stop(t, "the keeper copy") {
		t.FailNow()
	}
}

package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// TestMetricsCollectorsRegisterOnOneRegistry: the collectors NewManager
// registers (metricsCollectors) register together on one registry. client_golang's
// Registry.Register refuses a second collector that describes the same
// descriptor, and hpcburst_collector_success is shared by the two reads, so
// two collectors that each described it would keep the manager from starting
// (`@hpc-burst-science-agent`, 6 Nov 2026, for #249). Once registered, a
// serving copy's scrape has the gauge for both reads, and its help text names
// both. TestManagerOnControlPlane shows the same on a started manager.
func TestMetricsCollectorsRegisterOnOneRegistry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	started := time.Now()
	for _, c := range metricsCollectors(gateStore(t), &Backend{}, SystemClock{}, func() time.Time { return started }, always, eventcount.New()) {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register the manager's collectors on one registry: %v", err)
		}
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	help := ""
	for _, mf := range mfs {
		if mf.GetName() != "hpcburst_collector_success" {
			continue
		}
		help = mf.GetHelp()
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "collector" {
					got[l.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	if len(got) != 2 || got[collectorInstances] != 1 || got[collectorAccounting] != 1 {
		t.Fatalf("hpcburst_collector_success %v, want instances and accounting, each 1", got)
	}
	if !strings.Contains(help, "accounting") || !strings.Contains(help, "instances") {
		t.Errorf("hpcburst_collector_success's help %q does not name both reads", help)
	}
}

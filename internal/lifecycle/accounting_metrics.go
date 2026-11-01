package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The accounting gauges (HBEP-8, Provider accounting): allocated GPU-seconds
// and CPU-seconds of each live Allocation, from the Provider controller's
// status.atProvider.accounting. Seconds are Prometheus's base unit; people read
// hours. A series is absent while its value is unknown: a pending Allocation,
// which has no accounting yet, or a count or elapsed time the Provider could
// not read. The labels are provider, namespace and allocation: Allocations are
// namespaced, so two of one name in different namespaces have series of their
// own (`@hpc-burst-science-agent`, 1 Nov 2026, for #111).

var accountingLabels = []string{"provider", "namespace", "allocation"}

const (
	metricGPUSeconds = "hpcburst_allocation_allocated_gpu_seconds"
	metricCPUSeconds = "hpcburst_allocation_allocated_cpu_seconds"
)

// unknownListFailed is the reason of hpcburst_allocation_metrics_unknown: the
// Allocations could not be listed, so no Allocation's accounting is known.
const unknownListFailed = "list_failed"

var (
	descGPUSeconds = prometheus.NewDesc(metricGPUSeconds,
		"GPU-seconds the scheduler allocated to the Allocation's job: allocated, not billed. Absent while unknown.", accountingLabels, nil)
	descCPUSeconds = prometheus.NewDesc(metricCPUSeconds,
		"CPU-seconds the scheduler allocated to the Allocation's job: allocated, not billed. Absent while unknown.", accountingLabels, nil)
	// descAccountingUnknown stands in for the accounting gauges when every one
	// of them is unknown at once, which a failed list of the Allocations is,
	// as hpcburst_instance_metrics_unknown stands in for the engine gauges
	// (HBEP-8, Unknown samples). It is a name this controller adds: HBEP-8
	// defines the unknown gauge for the engine metrics only.
	descAccountingUnknown = prometheus.NewDesc("hpcburst_allocation_metrics_unknown",
		"1 for each accounting metric that could not be read for any Allocation, and so has no series: metric is its hpcburst_ name, reason list_failed (the Allocations could not be listed).",
		[]string{"metric", "reason"}, nil)
)

// accountingListTimeout bounds a scrape's list of Allocations, which is read
// from the manager's cache.
const accountingListTimeout = 10 * time.Second

// accountingCollector is a prometheus.Collector that builds the accounting
// gauges at scrape time from the Allocations that exist.
type accountingCollector struct {
	// allocs reads the Allocations: the manager's cached client.
	allocs client.Reader
}

// Describe implements prometheus.Collector.
func (c *accountingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descGPUSeconds
	ch <- descCPUSeconds
	ch <- descAccountingUnknown
}

// Collect implements prometheus.Collector. A list that fails gives no
// accounting series and an unknown series for each of the two gauges, with
// reason list_failed, and is logged. It is not reported as an invalid metric:
// controller-runtime serves /metrics with HTTPErrorOnError, so one invalid
// metric would make the whole scrape answer 500 and hide the engine gauges
// with it.
func (c *accountingCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), accountingListTimeout)
	defer cancel()
	var list v1alpha1.AllocationList
	if err := c.allocs.List(ctx, &list); err != nil {
		ctrl.Log.WithName("metrics").Error(fmt.Errorf("list Allocations: %w", err), "the accounting gauges are unknown for this scrape")
		for _, m := range []string{metricGPUSeconds, metricCPUSeconds} {
			ch <- prometheus.MustNewConstMetric(descAccountingUnknown, prometheus.GaugeValue, 1, m, unknownListFailed)
		}
		return
	}
	for i := range list.Items {
		a := &list.Items[i]
		acct := a.Status.AtProvider.Accounting
		if acct == nil {
			continue // not started: its hours are unknown, never 0
		}
		provider := a.Labels[v1alpha1.LabelProvider]
		if ref := a.Spec.ProviderConfigReference; ref != nil && ref.Name != "" {
			provider = ref.Name
		}
		if s, ok := acct.GPUSeconds(); ok {
			ch <- prometheus.MustNewConstMetric(descGPUSeconds, prometheus.GaugeValue, float64(s), provider, a.Namespace, a.Name)
		}
		if s, ok := acct.CPUSeconds(); ok {
			ch <- prometheus.MustNewConstMetric(descCPUSeconds, prometheus.GaugeValue, float64(s), provider, a.Namespace, a.Name)
		}
	}
}

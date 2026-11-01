package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

var (
	descGPUSeconds = prometheus.NewDesc("hpcburst_allocation_allocated_gpu_seconds",
		"GPU-seconds the scheduler allocated to the Allocation's job: allocated, not billed. Absent while unknown.", accountingLabels, nil)
	descCPUSeconds = prometheus.NewDesc("hpcburst_allocation_allocated_cpu_seconds",
		"CPU-seconds the scheduler allocated to the Allocation's job: allocated, not billed. Absent while unknown.", accountingLabels, nil)
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
}

// Collect implements prometheus.Collector. A list that fails is reported as an
// invalid metric, so the scrape says the gauges could not be read rather than
// showing none.
func (c *accountingCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), accountingListTimeout)
	defer cancel()
	var list v1alpha1.AllocationList
	if err := c.allocs.List(ctx, &list); err != nil {
		ch <- prometheus.NewInvalidMetric(descGPUSeconds, fmt.Errorf("list Allocations: %w", err))
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

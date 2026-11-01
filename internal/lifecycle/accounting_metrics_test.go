package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// accountedAllocation is an Allocation on a Provider with the accounting the
// Provider's controller wrote; nil for a job not started.
func accountedAllocation(ns, name, provider string, acct *v1alpha1.AllocationAccounting) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{v1alpha1.LabelProvider: provider}},
		Spec:       v1alpha1.AllocationSpec{ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: provider}},
	}
	a.Status.AtProvider.Accounting = acct
	return a
}

func i32(v int32) *int32 { return &v }
func i64(v int64) *int64 { return &v }

// TestMetricsAccountingGauges covers HBEP-8, Provider accounting, Metrics: the
// allocated GPU- and CPU-seconds of each live Allocation, with the help text
// "allocated, not billed", absent while unknown, never 0. Two Allocations of
// one name in different namespaces have series of their own
// (`@hpc-burst-science-agent`, 1 Nov 2026, for #111). The expected numbers are
// the counts times the elapsed seconds, worked out here.
func TestMetricsAccountingGauges(t *testing.T) {
	read := metav1.NewTime(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC))
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(
		// 4 GPUs and 64 CPUs for an hour: 14,400 GPU-seconds, 230,400 CPU-seconds.
		accountedAllocation("team-a", "alloc-1", "helios-gpu", &v1alpha1.AllocationAccounting{AllocatedGPUs: i32(4), AllocatedCPUs: i32(64), ElapsedSeconds: i64(3600), ReadTime: read}),
		// The same name in another namespace: 1 GPU and 16 CPUs for 90 s.
		accountedAllocation("team-b", "alloc-1", "athena-gpu", &v1alpha1.AllocationAccounting{AllocatedGPUs: i32(1), AllocatedCPUs: i32(16), ElapsedSeconds: i64(90), ReadTime: read}),
		// Pending: no accounting, so no series.
		accountedAllocation("team-a", "alloc-pending", "helios-gpu", nil),
		// A GPU was asked for and accounting lists none: GPU-seconds unknown.
		accountedAllocation("team-a", "alloc-no-gpu-entry", "helios-gpu", &v1alpha1.AllocationAccounting{AllocatedCPUs: i32(8), ElapsedSeconds: i64(10), ReadTime: read}),
		// Elapsed time unknown (a requeued job): both unknown.
		accountedAllocation("team-a", "alloc-requeued", "helios-gpu", &v1alpha1.AllocationAccounting{AllocatedGPUs: i32(2), AllocatedCPUs: i32(32), ReadTime: read}),
	).Build()
	ex := scrapeCollectors(t, &accountingCollector{allocs: c})
	if got := collectorSuccess(t, ex, collectorAccounting); got != "1" {
		t.Errorf("hpcburst_collector_success{collector=accounting} %s after a successful list, want 1", got)
	}
	const gpu, cpu = "hpcburst_allocation_allocated_gpu_seconds", "hpcburst_allocation_allocated_cpu_seconds"
	for _, tc := range []struct {
		name                string
		provider, ns, alloc string
		want                string // "" is absent
	}{
		{gpu, "helios-gpu", "team-a", "alloc-1", "14400"},
		{cpu, "helios-gpu", "team-a", "alloc-1", "230400"},
		{gpu, "athena-gpu", "team-b", "alloc-1", "90"},
		{cpu, "athena-gpu", "team-b", "alloc-1", "1440"},
		{gpu, "", "team-a", "alloc-pending", ""},
		{cpu, "", "team-a", "alloc-pending", ""},
		{gpu, "", "team-a", "alloc-no-gpu-entry", ""},
		{cpu, "helios-gpu", "team-a", "alloc-no-gpu-entry", "80"},
		{gpu, "", "team-a", "alloc-requeued", ""},
		{cpu, "", "team-a", "alloc-requeued", ""},
	} {
		got := ex.find(tc.name, map[string]string{"namespace": tc.ns, "allocation": tc.alloc})
		if tc.want == "" {
			if len(got) != 0 {
				t.Errorf("%s of %s/%s: %v, want absent (unknown, never 0)", tc.name, tc.ns, tc.alloc, got)
			}
			continue
		}
		if len(got) != 1 || got[0].value != tc.want || got[0].labels["provider"] != tc.provider || len(got[0].labels) != 3 {
			t.Errorf("%s of %s/%s: %v, want one series {provider=%q, namespace, allocation} = %s", tc.name, tc.ns, tc.alloc, got, tc.provider, tc.want)
		}
	}
	if n := len(ex.find(gpu, map[string]string{"allocation": "alloc-1"})); n != 2 {
		t.Errorf("%d GPU series for the two Allocations named alloc-1, want 2 distinct ones", n)
	}
	for _, name := range []string{gpu, cpu} {
		if ex.types[name] != "gauge" || !strings.Contains(ex.help[name], "allocated, not billed") {
			t.Errorf("%s: TYPE %q, HELP %q; want a gauge whose help says allocated, not billed", name, ex.types[name], ex.help[name])
		}
	}
}

// flakyLister is a reader whose lists fail while down is set, as a cache that
// cannot serve one.
type flakyLister struct {
	client.Reader
	down bool
}

func (f *flakyLister) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if f.down {
		return errors.New("the cache cannot list Allocations")
	}
	return f.Reader.List(ctx, list, opts...)
}

// allocationReader is a fake store of the given objects.
func allocationReader(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// collectorSuccess returns the value of hpcburst_collector_success for a
// collector, failing unless it is exposed exactly once with that one label.
func collectorSuccess(t *testing.T, ex exposition, collector string) string {
	t.Helper()
	got := ex.find("hpcburst_collector_success", map[string]string{"collector": collector})
	if len(got) != 1 || len(got[0].labels) != 1 {
		t.Fatalf("hpcburst_collector_success{collector=%q}: %v, want one series with that one label", collector, got)
	}
	return got[0].value
}

// TestMetricsAccountingListFailureKeepsTheScrape (HBEP-8, Unknown samples and
// Test plan, A failed Allocation list): controller-runtime serves /metrics
// with HTTPErrorOnError, so a collector that reports an invalid metric would
// make the whole scrape answer 500, the engine gauges with it. A failed list
// of the Allocations gives instead no accounting series and
// hpcburst_collector_success{collector="accounting"} 0, with the engine gauges
// intact; after the next successful list it is 1 and the series are back.
// scrapeCollectors serves the collectors as /metrics the same way and fails
// the test on any status but 200.
func TestMetricsAccountingListFailureKeepsTheScrape(t *testing.T) {
	rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	read := metav1.NewTime(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC))
	// 4 GPUs and 64 CPUs for an hour: 14,400 GPU-seconds, 230,400 CPU-seconds.
	lister := &flakyLister{Reader: allocationReader(t,
		accountedAllocation("team-a", "alloc-1", "helios-gpu", &v1alpha1.AllocationAccounting{AllocatedGPUs: i32(4), AllocatedCPUs: i32(64), ElapsedSeconds: i64(3600), ReadTime: read}))}
	const gpu, cpu = "hpcburst_allocation_allocated_gpu_seconds", "hpcburst_allocation_allocated_cpu_seconds"
	scrapeBoth := func() exposition {
		return scrapeCollectors(t,
			&instanceCollector{inv: rg.r.inventory(), backend: rg.backend, clock: rg.clock},
			&accountingCollector{allocs: lister})
	}
	check := func(when, success string, series bool) {
		t.Helper()
		ex := scrapeBoth()
		if got := collectorSuccess(t, ex, collectorAccounting); got != success {
			t.Errorf("%s: hpcburst_collector_success{collector=accounting} %s, want %s", when, got, success)
		}
		if n := len(ex.find(gpu, nil)) + len(ex.find(cpu, nil)); (n == 2) != series || (n != 0 && n != 2) {
			t.Errorf("%s: %d accounting series, want present=%v (both gauges of the one Allocation, or neither)", when, n, series)
		}
		if got := len(ex.engineSeries(uid)); got != 9 {
			t.Errorf("%s: %d engine samples beside the accounting collector, want all 9 intact", when, got)
		}
		if n := len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": uid})); n != 1 {
			t.Errorf("%s: %d info series, want 1", when, n)
		}
	}
	check("before the failure", "1", true)
	lister.down = true
	check("while the list fails", "0", false)
	lister.down = false
	check("after the next successful list", "1", true)
}

// TestMetricsAccountingNoAllocationsIsNotAFailure: an empty list gives success
// 1 and no accounting series, which tells "no Allocations" from "could not list
// them" (success 0).
func TestMetricsAccountingNoAllocationsIsNotAFailure(t *testing.T) {
	ex := scrapeCollectors(t, &accountingCollector{allocs: allocationReader(t)})
	if got := collectorSuccess(t, ex, collectorAccounting); got != "1" {
		t.Errorf("hpcburst_collector_success{collector=accounting} %s for an empty list, want 1", got)
	}
	for _, s := range ex.samples {
		if strings.HasPrefix(s.name, "hpcburst_allocation_") {
			t.Errorf("series %v for an empty list, want none", s)
		}
	}
	if ex.types["hpcburst_collector_success"] != "gauge" || ex.help["hpcburst_collector_success"] == "" {
		t.Errorf("hpcburst_collector_success: TYPE %q, HELP %q; want a gauge with help", ex.types["hpcburst_collector_success"], ex.help["hpcburst_collector_success"])
	}
}

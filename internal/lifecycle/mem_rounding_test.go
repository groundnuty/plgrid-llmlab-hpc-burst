package lifecycle

// The aggregate memory checks charge each request in whole MiB, rounded UP, as the plugin sends it (--mem=<MiB>M) and
// Slurm counts it (review of #756, Astra 8): two requests of 1.5 MiB are 2 + 2 = 4 MiB against a cap of 3 MiB, though
// their bytes sum to exactly 3 MiB. Worked by hand.

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestTheOverlapCheckChargesEachRequestInWholeMiB(t *testing.T) {
	size := batch(1, 4, "1536Ki") // 1.5 MiB
	cap3 := v1alpha1.QOSLimits{MaxMemPerUser: memQ("3Mi"), From: map[string]string{v1alpha1.LimitMaxMemPerUser: "example-now"}}
	if got := capsExceeded(&providerView{name: "example-provider", limits: cap3}, "example-instance", size, size); got == "" {
		t.Fatal("capsExceeded = none for two 1.5 MiB requests under a 3 MiB cap; Slurm charges 2 MiB each, 4 MiB in all")
	}
	whole := batch(1, 4, "1Mi")
	if got := capsExceeded(&providerView{name: "example-provider", limits: cap3}, "example-instance", whole, whole); got != "" {
		t.Fatalf("capsExceeded = %q for two 1 MiB requests under a 3 MiB cap, want none: 2 MiB fit", got)
	}
}

func TestTheCapsFitCheckChargesEachRequestInWholeMiB(t *testing.T) {
	running := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "ours"}}
	running.Spec.ProviderConfigReference = providerRef(provB)
	b := batch(1, 4, "1536Ki")
	running.Spec.ForProvider.Batch = &b
	running.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	r := allocsReader{allocs: []v1alpha1.Allocation{running}}
	caps := v1alpha1.QOSLimits{MaxMemPerUser: memQ("3Mi"), From: map[string]string{v1alpha1.LimitMaxMemPerUser: "now"}}
	tab := newSlotTable()
	size := batch(1, 4, "1536Ki")
	a, err := tab.reserveAsk(t.Context(), r, slotAsk{provider: provB, key: rigNS + "/new", params: &size, caps: caps}, fixedAt(hoT(9, 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if a.ok || a.capsWhy == "" {
		t.Fatalf("a second 1.5 MiB request beside a listed one under a 3 MiB cap: ok %v, %q; want refused (2 + 2 = 4 MiB)", a.ok, a.capsWhy)
	}
}

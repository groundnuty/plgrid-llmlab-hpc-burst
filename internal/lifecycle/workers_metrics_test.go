package lifecycle

// HBEP-8's inventory names each of an Instance's launch.workers (HBEP-7, Several workers, Metrics): with
// no report every worker's engine metrics are unknown, stale; with a report that lacks a worker, as while
// it stages, the missing workers' are unknown too, never absent.

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func TestMetricsEveryLaunchWorkerOfAnUnreportedInstanceIsUnknown(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	two := int32(2)
	owner := []metav1.OwnerReference{{Kind: "ModelDeployment", Name: "md", Controller: ptrTo(true)}}
	in := &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "inst-two", UID: "uid-two", OwnerReferences: owner},
		Spec:       v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-two"}, Launch: v1alpha1.Launch{Workers: &two}},
		Status:     v1alpha1.InstanceStatus{Phase: v1alpha1.InstancePending},
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(in).Build()
	ex := scrapeCollectors(t, storeCollector(c, &Backend{}))
	for _, w := range []string{"worker-0", "worker-1"} {
		if u := ex.unknowns("uid-two", w); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
			t.Errorf("unknown series of %s: %v, want every engine metric with reason stale", w, u)
		}
	}
}

func TestMetricsAWorkerTheReportLacksIsUnknown(t *testing.T) {
	rg, ag := twoWorkersRigWith(t, afake.Engine{StagingDuration: time.Hour}, nil)
	startUntil(rg, "md", 10*time.Minute, "the agent reporting while it stages", func() bool {
		rep, ok := ag.LastReport()
		return ok && rep.Instance != nil && len(rep.Instance.Workers) == 0
	})
	uid := string(rg.instances("md")[0].UID)
	ex := scrape(t, rg)
	for _, w := range []string{"worker-0", "worker-1"} {
		if u := ex.unknowns(uid, w); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
			t.Errorf("unknown series of %s, which the report lacks: %v, want every engine metric with reason stale", w, u)
		}
	}
}

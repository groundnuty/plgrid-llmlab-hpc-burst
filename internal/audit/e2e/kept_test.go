package e2e

// Rule 3 of the policy in a log FILE: the lifecycle controller's UPDATE of an Allocation is logged at Request level with its
// request body. md2's one Allocation has STARTED (written as the Provider's controller would), and with the count at zero no
// Instance holds it, so the controller writes the idle-since annotation on it (HBEP-7, Retention): an update of an Allocation by
// the lifecycle identity, which retention's release cause and delete then follow in a run that executes Operations.

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// keep makes md2, starts its Allocation, takes its count to zero, and waits until the controller has marked the Allocation idle.
func (s *scenario) keep() {
	t, w := s.t, s.world
	t.Helper()
	idle := v1alpha1.Duration((30 * time.Minute).String())
	md2 := deployment("md2")
	md2.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &idle}
	if err := w.admin.c.Create(w.ctx, md2); err != nil {
		t.Fatal(err)
	}
	eventually(t, 90*time.Second, "an Instance and its Allocation of md2, made by the lifecycle controller", func() (bool, string) {
		var insts v1alpha1.InstanceList
		var allocs v1alpha1.AllocationList
		labels := client.MatchingLabels{v1alpha1.LabelModelDeployment: "md2"}
		if err := w.admin.c.List(w.ctx, &insts, client.InNamespace(exampleNS), labels); err != nil {
			return false, err.Error()
		}
		if err := w.admin.c.List(w.ctx, &allocs, client.InNamespace(exampleNS), labels); err != nil {
			return false, err.Error()
		}
		if len(insts.Items) != 1 || len(allocs.Items) != 1 {
			return false, fmt.Sprintf("%d Instances, %d Allocations", len(insts.Items), len(allocs.Items))
		}
		s.keptAlloc = allocs.Items[0]
		return true, ""
	})
	w.startAllocation(&s.keptAlloc)
	eventually(t, 30*time.Second, "md2's count at zero", func() (bool, string) {
		var md v1alpha1.ModelDeployment
		if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: "md2"}, &md); err != nil {
			return false, err.Error()
		}
		md.Spec.Instances = 0
		return w.admin.c.Update(w.ctx, &md) == nil, "update conflict"
	})
	eventually(t, 90*time.Second, "md2's Allocation marked idle by the controller", func() (bool, string) {
		var a v1alpha1.Allocation
		if err := w.admin.c.Get(w.ctx, client.ObjectKeyFromObject(&s.keptAlloc), &a); err != nil {
			return false, err.Error()
		}
		return a.Annotations[v1alpha1.AnnotationIdleSince] != "", fmt.Sprintf("annotations %v", a.Annotations)
	})
}

// hasIdleWrite reports whether the log holds the lifecycle identity's update of the Allocation that carries the idle-since
// annotation in its request body.
func hasIdleWrite(s *logSet, lifecycleUser, allocation string) bool {
	for _, e := range s.find("update", "allocations", "", allocation, lifecycleUser) {
		var a v1alpha1.Allocation
		if len(e.RequestObject) > 0 && jsonUnmarshal(e.RequestObject, &a) == nil && a.Annotations[v1alpha1.AnnotationIdleSince] != "" {
			return true
		}
	}
	return false
}

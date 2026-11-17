package e2e

// The helpers of the one run: Operations, their plans and approvals, and the Provider's readiness.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// keepProviderFresh re-marks the Provider ready every 15 s, as its controller would, so that the lifecycle
// controller keeps seeing a recent successful read for as long as the run lasts. It stops with the test.
func (w *world) keepProviderFresh(prov *v1alpha1.Provider) {
	ctx, cancel := context.WithCancel(w.ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Second):
			}
			var cur v1alpha1.Provider
			if err := w.admin.c.Get(ctx, client.ObjectKeyFromObject(prov), &cur); err != nil {
				continue
			}
			apimeta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPolled, LastTransitionTime: metav1.Now()})
			at := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
			cur.Status.LastSuccessfulStatusRead = &at
			_ = w.admin.c.Status().Update(ctx, &cur, client.FieldOwner(v1alpha1.FieldManagerProvider)) // the next tick tries again
		}
	}()
	w.t.Cleanup(func() { cancel(); wg.Wait() })
}

// endOperation builds an End Operation of the deployment, owned by the identity that creates it.
func endOperation(name string, owner who, purpose string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: exampleNS, Annotations: map[string]string{v1alpha1.AnnotationPurpose: purpose}},
		Spec:       v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "md"}, Owner: owner.user},
	}
}

func (w *world) getOperation(name string) *v1alpha1.Operation {
	w.t.Helper()
	var op v1alpha1.Operation
	if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: name}, &op); err != nil {
		w.t.Fatalf("get Operation %s: %v", name, err)
	}
	return &op
}

// waitPlan waits until the controller has written the Operation's plan, and returns the Operation.
func (w *world) waitPlan(name string) *v1alpha1.Operation {
	w.t.Helper()
	var op *v1alpha1.Operation
	eventually(w.t, 90*time.Second, "the plan of Operation "+name, func() (bool, string) {
		var cur v1alpha1.Operation
		if err := w.admin.c.Get(w.ctx, types.NamespacedName{Namespace: exampleNS, Name: name}, &cur); err != nil {
			return false, err.Error()
		}
		op = &cur
		return cur.Status.Phase == v1alpha1.OperationPhasePlanned && cur.Status.Plan != nil, fmt.Sprintf("phase %q, plan %v", cur.Status.Phase, cur.Status.Plan != nil)
	})
	return op
}

func (w *world) approval(name string, op *v1alpha1.Operation, planID string, by who) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: exampleNS},
		Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: planID,
			Approver: v1alpha1.ApproverIdentity{Username: by.user, Groups: by.groups}},
	}
}

// hasPlanWrite reports whether the lifecycle identity's status write of the Operation carries the plan.
func hasPlanWrite(s *logSet, lifecycleUser, name, planID string) bool {
	for _, e := range s.find("update", "operations", "status", name, lifecycleUser) {
		var op v1alpha1.Operation
		if len(e.RequestObject) > 0 && jsonUnmarshal(e.RequestObject, &op) == nil && op.Status.Plan != nil && op.Status.Plan.ID == planID {
			return true
		}
	}
	return false
}

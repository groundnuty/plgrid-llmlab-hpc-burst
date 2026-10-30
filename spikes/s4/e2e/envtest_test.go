package e2e

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// What envtest's API server lacks: it runs no kube-controller-manager, so
// owner references are never garbage-collected and a deleted namespace stays
// Terminating. HBEP-11's "control plane without nodes" needs the controller
// manager for both.
func TestEnvtestHasNoGarbageCollection(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: ns}}
	if err := h.k8s.Create(ctx, owner); err != nil {
		t.Fatal(err)
	}
	dep := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dependent", Namespace: ns,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID}}}}
	if err := h.k8s.Create(ctx, dep); err != nil {
		t.Fatal(err)
	}
	if err := h.k8s.Delete(ctx, owner); err != nil {
		t.Fatal(err)
	}
	nsObj := &corev1.Namespace{}
	if err := h.k8s.Get(ctx, client.ObjectKey{Name: ns}, nsObj); err != nil {
		t.Fatal(err)
	}
	if err := h.k8s.Delete(ctx, nsObj); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
	err := h.k8s.Get(ctx, client.ObjectKeyFromObject(dep), &corev1.ConfigMap{})
	if kerrors.IsNotFound(err) {
		t.Fatalf("dependent was garbage-collected: envtest does run garbage collection")
	} else if err != nil {
		t.Fatal(err)
	}
	if err := h.k8s.Get(ctx, client.ObjectKey{Name: ns}, nsObj); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if nsObj.Status.Phase != corev1.NamespaceTerminating {
		t.Fatalf("namespace phase %q, want Terminating", nsObj.Status.Phase)
	}
	t.Logf("10 s after deletion: the owned ConfigMap still exists and the namespace is still %s", nsObj.Status.Phase)
}

package inventory

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The watch adapter against client-go's fake dynamic client, which serves
// list and watch from an object tracker, so the informers run their real
// list-then-watch path. No API server is needed (issue #64: no envtest block;
// the launcher's real k3s covers the real API in the supervision branch).

func toUnstructured(t *testing.T, obj runtime.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(kind)
	return u
}

func newFakeDynamic(objs ...runtime.Object) *dynfake.FakeDynamicClient {
	lists := map[schema.GroupVersionResource]string{
		GVRModelDeployments: "ModelDeploymentList", GVRInstances: "InstanceList",
		GVRProviders: "ProviderList", GVRPlatformProfiles: "PlatformProfileList",
	}
	return dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, objs...)
}

func TestWatchSeesObjectsAndChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inst := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "i0", UID: "u0"}, Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady}}
	prof := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "p"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch}}
	client := newFakeDynamic(toUnstructured(t, inst, "Instance"), toUnstructured(t, prof, "PlatformProfile"))
	changed := make(chan struct{}, 100)
	w := NewWatch(client, nil, func() { changed <- struct{}{} })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !w.Synced() {
		t.Fatal("not synced after Start")
	}
	s, problems := w.Snapshot()
	if len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	if len(s.Instances) != 1 || s.Instances[0].Status.Phase != v1alpha1.InstanceReady || len(s.Profiles) != 1 {
		t.Fatalf("snapshot %+v", s)
	}
	// A new ModelDeployment arrives by watch.
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "md"}, Spec: v1alpha1.ModelDeploymentSpec{Model: "m"}}
	if _, err := client.Resource(GVRModelDeployments).Namespace("ns").Create(ctx, toUnstructured(t, md, "ModelDeployment"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		s, _ = w.Snapshot()
		if len(s.Deployments) == 1 {
			break
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatal("the watch did not see the new ModelDeployment")
		}
	}
	if s.Deployments[0].Spec.Model != "m" {
		t.Errorf("model = %q", s.Deployments[0].Spec.Model)
	}
	if _, ok := w.LastEvents()["modeldeployments"]; !ok {
		t.Error("no watch age for modeldeployments")
	}
}

// TestWatchReadsUnknownStatusFields: a downgraded front meets status fields
// it does not know and still reads the object (HBEP-5, Compatibility).
func TestWatchReadsUnknownStatusFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inst := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "i0"}}
	u := toUnstructured(t, inst, "Instance")
	if err := unstructured.SetNestedField(u.Object, "later", "status", "fieldFromTheFuture"); err != nil {
		t.Fatal(err)
	}
	w := NewWatch(newFakeDynamic(u), []string{"ns"}, nil)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	s, problems := w.Snapshot()
	if len(problems) != 0 || len(s.Instances) != 1 {
		t.Fatalf("instances %d, problems %v", len(s.Instances), problems)
	}
}

package lifecycle

// The manager on a real API server (internal/testenv's assembled host): the
// Lease, the keyed work queue writing status, identity records as
// ControllerRevisions, the health endpoints, and the server-side apply of
// DialHomeUnreachable beside the Provider controller's own status writes. Run
// it under the envtest lock:
//
//	flock /tmp/hpc-burst-tools/locks/envtest.lock go test ./internal/lifecycle/ -run TestManagerOnControlPlane
//
// It uses #61's envtest block, 29204-29207, unless HPCBURST_TESTENV_PORT_BASE
// is set, so it does not collide with the other packages' blocks when go test
// runs packages in parallel; the health probe binds 127.0.0.1:29301 (#61's
// block).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

const (
	managerHealthAddr = "127.0.0.1:29301"
	// managerPortBase is #61's envtest block, 29204-29207 (IMPL-COMMON.md,
	// Envtest port blocks), used when HPCBURST_TESTENV_PORT_BASE is unset, so
	// go test ./... runs it beside the other packages' control planes.
	managerPortBase = "29204"
)

func installTestCRDs(ctx context.Context, cfg *rest.Config) error {
	s, err := Scheme()
	if err != nil {
		return err
	}
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		return err
	}
	cl, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "config", "crd", "*.yaml"))
	if err != nil || len(files) != 5 {
		return fmt.Errorf("want the 5 committed CRDs in config/crd, found %d (%v)", len(files), err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if err := cl.Create(ctx, crd); err != nil {
			return fmt.Errorf("create %s: %w", crd.Name, err)
		}
	}
	return nil
}

func eventually(t *testing.T, limit time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	last := ""
	for {
		ok, msg := cond()
		if ok {
			return
		}
		last = msg
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s; last: %s", what, limit, last)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestManagerOnControlPlane(t *testing.T) {
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	defer func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	}()
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{v1alpha1.ControllerNamespace, rigNS} {
		if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	prof := &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: rigProfile},
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, Partitions: []v1alpha1.PartitionProfile{{
			Name: rigPartition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch},
			MaxTime: dur(48 * time.Hour), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)), MemPerGPU: ptrTo(resource.MustParse("120Gi")),
		}}},
	}
	if err := cl.Create(ctx, prof); err != nil {
		t.Fatal(err)
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: rigProvider},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
	}
	if err := cl.Create(ctx, prov); err != nil {
		t.Fatal(err)
	}
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionTrue, Reason: "Polled", LastTransitionTime: metav1.Now()})
	if err := cl.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
		t.Fatal(err)
	}
	md := mdSpec("md")
	md.Generation = 0
	if err := cl.Create(ctx, md); err != nil {
		t.Fatalf("create ModelDeployment: %v", err)
	}
	// A second deployment whose identity record name is taken by another ID:
	// its degraded/IdentityUnresolved event must reach the API server as an
	// events.k8s.io Event (a rejected Event would be dropped silently).
	collide := mdSpec("md-collide")
	collide.Generation = 0
	collide.Spec.Source.Served.Flags = append(collide.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "dtype", Value: "bfloat16"})
	tmpl, err := deploymentid.ChatTemplateSHA256("")
	if err != nil {
		t.Fatal(err)
	}
	cid, err := deploymentid.Compute(deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: "example-org/example-model", Revision: rigRevision},
		Engine:             deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest},
		Flags:              []deploymentid.Flag{{Name: "max-model-len", Value: "32768"}, {Name: "dtype", Value: "bfloat16"}},
		ChatTemplateSHA256: tmpl,
	}, deploymentid.Accelerator{GPUModel: rigGPU, CPUArch: rigArch}, rigFiles)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(identityData{Scheme: "v1", SHA256: cid.SHA256[:12] + strings.Repeat("0", 52), Canonical: json.RawMessage(`{}`)})
	if err := cl.Create(ctx, &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "id-" + cid.SHA256[:12], Namespace: rigNS},
		Data:       runtime.RawExtension{Raw: data}, Revision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(ctx, collide); err != nil {
		t.Fatalf("create ModelDeployment: %v", err)
	}

	creds := t.TempDir()
	mgr, backend, err := NewManager(Options{
		Config: cp.Config, CredentialsDir: creds, FrontConfigured: false, HealthAddr: managerHealthAddr,
		LeaderElection: true, Checkpoints: &stubSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	mctx, stopMgr := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	stopped := false
	defer func() {
		if !stopped {
			stopMgr()
			<-done
		}
	}()

	eventually(t, time.Minute, "the lifecycle Lease is held", func() (bool, string) {
		var l coordinationv1.Lease
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
			return false, err.Error()
		}
		return l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != "", "no holder"
	})
	eventually(t, time.Minute, "the Backend serves agents", func() (bool, string) {
		return backend.serving() == nil, "not serving"
	})
	eventually(t, time.Minute, "the ModelDeployment is Degraded with FrontNotConfigured and has its current ID", func() (bool, string) {
		var got v1alpha1.ModelDeployment
		if err := cl.Get(ctx, client.ObjectKeyFromObject(md), &got); err != nil {
			return false, err.Error()
		}
		c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
		if c == nil || c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonFrontNotConfigured || len(got.Status.CurrentIDs) != 1 {
			return false, fmt.Sprintf("conditions %+v, current IDs %+v", got.Status.Conditions, got.Status.CurrentIDs)
		}
		name, _ := v1alpha1.IdentityRecordName(string(got.Status.CurrentIDs[0].ID))
		var rec appsv1.ControllerRevision
		if err := cl.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &rec); err != nil {
			return false, "identity record: " + err.Error()
		}
		return true, ""
	})
	eventually(t, time.Minute, "the collision's event reaches the API server", func() (bool, string) {
		var evs eventsv1.EventList
		if err := cl.List(ctx, &evs, client.InNamespace(rigNS)); err != nil {
			return false, err.Error()
		}
		var seen []string
		for _, e := range evs.Items {
			seen = append(seen, e.Reason+" on "+e.Regarding.Name)
			if e.Reason == eventDegraded+"/"+v1alpha1.ReasonIdentityUnresolved && e.Regarding.Name == "md-collide" && e.Type == corev1.EventTypeWarning {
				return true, ""
			}
		}
		return false, fmt.Sprintf("events: %v", seen)
	})
	var allocs v1alpha1.AllocationList
	if err := cl.List(ctx, &allocs, client.InNamespace(rigNS)); err != nil || len(allocs.Items) != 0 {
		t.Fatalf("with no front configured, want no Allocation; got %d (%v)", len(allocs.Items), err)
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get("http://" + managerHealthAddr + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d", path, resp.StatusCode)
		}
	}

	// DialHomeUnreachable by server-side apply, beside the Provider
	// controller's whole-status updates.
	r := &Reconciler{Client: cl, Reader: cl, Clock: SystemClock{}}
	now := time.Now().UTC().Truncate(time.Second)
	if err := r.setDialHome(ctx, rigProvider, true, now, "an Instance could not register"); err != nil {
		t.Fatalf("apply DialHomeUnreachable: %v", err)
	}
	var got v1alpha1.Provider
	if err := cl.Get(ctx, client.ObjectKey{Name: rigProvider}, &got); err != nil {
		t.Fatal(err)
	}
	if !apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable) ||
		!apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionProviderReady) {
		t.Fatalf("after the apply, want DialHomeUnreachable and Ready true: %+v", got.Status.Conditions)
	}
	owned := false
	for _, mf := range got.ManagedFields {
		if mf.Manager == v1alpha1.FieldManagerLifecycle && mf.Operation == metav1.ManagedFieldsOperationApply && mf.Subresource == "status" {
			owned = true
		}
	}
	if !owned {
		t.Fatalf("no status apply entry of %s in managedFields", v1alpha1.FieldManagerLifecycle)
	}
	// The Provider's controller writes its whole status as it found it.
	got.Status.LastSuccessfulStatusRead = &metav1.Time{Time: now}
	if err := cl.Status().Update(ctx, &got, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
		t.Fatal(err)
	}
	if err := r.setDialHome(ctx, rigProvider, false, now.Add(time.Minute), "an agent registered"); err != nil {
		t.Fatalf("apply DialHomeUnreachable false: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKey{Name: rigProvider}, &got); err != nil {
		t.Fatal(err)
	}
	c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	if c == nil || c.Status != metav1.ConditionFalse || !apimeta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.ConditionProviderReady) {
		t.Fatalf("after the second apply, want DialHomeUnreachable false and Ready kept: %+v", got.Status.Conditions)
	}

	stopMgr()
	if err := <-done; err != nil {
		t.Fatalf("manager: %v", err)
	}
	stopped = true
	eventually(t, 30*time.Second, "the Lease is released on stop", func() (bool, string) {
		var l coordinationv1.Lease
		if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
			return false, err.Error()
		}
		return l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "", "still held"
	})
}

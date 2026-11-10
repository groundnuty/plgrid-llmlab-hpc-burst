package providerctl

// Issue #304 end to end, in one process: the Provider's controller as it runs
// (the managed reconciler, its relay owner, hostdial.Owner, with a stub ssh
// that prints srun's step-limit refusal) and the lifecycle controller as it
// runs (lifecycle.Reconciler, the planner included), over one store. A
// deployment's Allocation is planned by the lifecycle controller and
// submitted by the Provider's; its relays end for good through the real
// owner; and the release follows through the lifecycle controller's own
// pass: its Instance ends, it is deleted, its finalizer cancels the job, and
// the planner makes a replacement. Nothing here calls the release; the test
// only runs the two controllers' passes and reads the objects.
//
// The Instance never registers (there is no agent), so it is pending when the
// relays fail: the lifecycle controller's step 5 judges an Instance of any
// phase, and internal/lifecycle's relay test covers a ready one.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

const lcProfile = "example-profile"

// lcCheckpoints is a fixed checkpoint file list with no chat template.
type lcCheckpoints struct{}

func (lcCheckpoints) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	return []deploymentid.File{
		{Path: "config.json", SHA256: strings.Repeat("1", 64)},
		{Path: "model.safetensors", SHA256: strings.Repeat("2", 64)},
	}, nil
}

func (lcCheckpoints) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

// withLifecycleStore gives the env a store both controllers can work on: the
// kinds the lifecycle controller writes, with their status subresources, the
// Provider with its PlatformProfile, and a UID and creation time for every
// object made, as the API server gives them.
func withLifecycleStore(e *env) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, appsv1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			e.t.Fatal(err)
		}
	}
	gpus, cpus := int32(4), int32(8)
	maxTime := v1alpha1.Duration("48h")
	mem := resource.MustParse("16Gi")
	prof := &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: lcProfile},
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true,
			Partitions: []v1alpha1.PartitionProfile{{Name: gpuPart, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: testArch},
				MaxTime: &maxTime, GPUsPerNode: &gpus, CPUsPerGPU: &cpus, MemPerGPU: &mem}}},
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: testProvider, Generation: 1, CreationTimestamp: metav1.NewTime(at(11, 0, 0)),
			Labels: map[string]string{v1alpha1.LabelPlatform: "example"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: lcProfile}, AllowedPartitions: []string{gpuPart}},
	}
	funcs := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if obj.GetUID() == "" {
			var b [16]byte
			_, _ = rand.Read(b[:])
			h := hex.EncodeToString(b[:])
			obj.SetUID(types.UID(h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]))
		}
		if ts := obj.GetCreationTimestamp(); ts.IsZero() {
			obj.SetCreationTimestamp(metav1.NewTime(e.clock.Now()))
		}
		return c.Create(ctx, obj, opts...)
	}}
	e.c = clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(prof, prov).
		WithStatusSubresource(&v1alpha1.ModelDeployment{}, &v1alpha1.Instance{}, &v1alpha1.Allocation{}, &v1alpha1.Provider{}, &v1alpha1.PlatformProfile{}).
		WithInterceptorFuncs(funcs).WithReturnManagedFields().Build()
}

// lifecycleOf is the lifecycle controller over the env's store, in
// host-dialed mode, writing the Allocations' credentials where the Provider's
// controller reads them and handing out the env's agent build.
func lifecycleOf(e *env) (*lifecycle.Reconciler, *lifecycle.MemoryRecorder) {
	rec := &lifecycle.MemoryRecorder{}
	return &lifecycle.Reconciler{
		Client: e.c, Reader: e.c, Clock: e.clock,
		Backend:     &lifecycle.Backend{Reader: e.c, Clock: e.clock, CredentialsDir: e.credDir},
		Checkpoints: lcCheckpoints{}, Recorder: rec,
		Config: lifecycle.Config{CredentialsDir: e.credDir, FrontConfigured: true, HostDialed: true,
			AgentDigest: func(string) (string, error) { return e.digest, nil }},
	}, rec
}

// lcDeployment is one Instance of a served model on the env's Provider.
func lcDeployment() *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "md", Namespace: testNS, Generation: 1},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: "example-model",
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: "example-org/example-model", Revision: "0123456789abcdef0123456789abcdef01234567"},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", Image: "docker.io/vllm/vllm-openai",
					ImageDigest: "sha256:" + strings.Repeat("1", 64)},
				Flags: []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "32768"}},
			}},
			Instances: 1,
			Providers: v1alpha1.ProviderSelection{Names: []string{testProvider}},
			Workload: &v1alpha1.Workload{ContextTokens: v1alpha1.ContextTokens{Typical: 4096},
				OutputTokens: v1alpha1.OutputTokens{Typical: 512}, TargetSessions: 4},
		},
	}
}

// lcPass runs one lifecycle pass over the deployment.
func lcPass(t *testing.T, e *env, r *lifecycle.Reconciler) {
	t.Helper()
	if _, err := r.Reconcile(e.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "md"}}); err != nil {
		t.Fatalf("the lifecycle pass: %v", err)
	}
}

func lcAllocations(t *testing.T, e *env) []v1alpha1.Allocation {
	t.Helper()
	var l v1alpha1.AllocationList
	if err := e.c.List(e.ctx, &l, client.InNamespace(testNS)); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

func lcInstances(t *testing.T, e *env) []v1alpha1.Instance {
	t.Helper()
	var l v1alpha1.InstanceList
	if err := e.c.List(e.ctx, &l, client.InNamespace(testNS)); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// The issue's acceptance: a final RelayFailed on a running Allocation leads to
// its release within a bounded time, through the lifecycle's release path,
// and the Instance's replacement follows the planner's usual rules. Here the
// Provider's controller is restarted after the relays end for good and before
// it observes the Allocation again, so its memory of the failure is gone: the
// mark the failure path wrote is in the store, and the lifecycle controller's
// next pass releases the Allocation.
func TestARelayEndedForGoodReleasesTheAllocationThroughTheLifecycle(t *testing.T) {
	g := newRelayGate(t)
	e := newEnv(t, stubbedRelays(g.stub), withLifecycleStore)
	stop := runRelaysStoppable(e)
	lc, lcRec := lifecycleOf(e)
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.c.Create(e.ctx, lcDeployment()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3 && len(lcAllocations(t, e)) == 0; i++ {
		lcPass(t, e, lc)
	}
	allocs := lcAllocations(t, e)
	if len(allocs) != 1 {
		t.Fatalf("the lifecycle controller planned %d Allocations, want 1", len(allocs))
	}
	old := allocs[0]
	ins := lcInstances(t, e)
	if len(ins) != 1 || ins[0].Spec.AllocationRef.Name != old.Name {
		t.Fatalf("want one Instance on %s, got %+v", old.Name, ins)
	}
	served := ins[0]
	e.run(old.Name, 4)
	a := e.get(old.Name)
	tag, job := tagOf(a), a.Status.AtProvider.JobID
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || job == "" {
		t.Fatalf("the Allocation is %s with job %q, want Running", a.Status.AtProvider.Phase, job)
	}
	waitRelays(t, "both relays' ssh running", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].PID != 0 && rs[1].PID != 0
	})
	g.open(t)
	waitRelays(t, "both channels' RelayFailed raised", func() bool { return len(e.rec.relayFailedMessages()) >= 2 })

	// A restart before any observation; the lifecycle's next pass releases
	// the Allocation from the mark in the store.
	restartController(e, stop)
	if st, _, _ := relayCondition(e.get(old.Name)); st != "True" {
		t.Fatalf("after the restart, before any observation, the condition is %q, want True", st)
	}
	lcPass(t, e, lc)
	a = e.get(old.Name)
	if a.DeletionTimestamp.IsZero() {
		t.Fatal("the lifecycle pass after the mark did not release the Allocation")
	}
	var in v1alpha1.Instance
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: served.Name}, &in); err != nil {
		t.Fatal(err)
	}
	if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != wantRelayCondition {
		t.Errorf("the Instance is %s with end reason %q; want ended with %s", in.Status.Phase, in.Status.EndReason, wantRelayCondition)
	}
	if ev := lcRec.Find("failing", wantRelayCondition); len(ev) != 1 || ev[0].Name != served.Name || !strings.Contains(ev[0].Message, "step limit") {
		t.Errorf("failing/%s events %+v: want one, for %s, naming srun's refusal", wantRelayCondition, ev, served.Name)
	}

	// The finalizer cancels the job by its tag, and the Allocation goes, with
	// its relays.
	waitRelays(t, "the released Allocation gone", func() bool {
		e.run(old.Name, 1)
		var got v1alpha1.Allocation
		err := e.c.Get(e.ctx, clientKey(old.Name), &got)
		return client.IgnoreNotFound(err) == nil && err != nil
	})
	if live := e.live(tag); len(live) != 0 {
		t.Errorf("jobs %v of the released Allocation still live", live)
	}
	if rs := relaysOf(e, tag); len(rs) != 0 {
		t.Errorf("the released Allocation keeps %d relays", len(rs))
	}

	// The planner replaces the failed Instance by its usual rules: a new
	// Instance on a new Allocation.
	var repl []v1alpha1.Allocation
	for i := 0; i < 5 && len(repl) == 0; i++ {
		e.clock.Advance(10 * time.Second)
		lcPass(t, e, lc)
		repl = lcAllocations(t, e)
	}
	if len(repl) != 1 || repl[0].Name == old.Name || repl[0].UID == old.UID {
		t.Fatalf("after the release the deployment has Allocations %+v; want one replacement", repl)
	}
	var live []string
	for _, i := range lcInstances(t, e) {
		if i.Status.Phase != v1alpha1.InstanceEnded {
			live = append(live, i.Name)
			if i.Spec.AllocationRef.Name != repl[0].Name {
				t.Errorf("live Instance %s is on %s, want the replacement %s", i.Name, i.Spec.AllocationRef.Name, repl[0].Name)
			}
		}
	}
	if len(live) != 1 || live[0] == served.Name {
		t.Errorf("live Instances %v: want one replacement of %s", live, served.Name)
	}
}

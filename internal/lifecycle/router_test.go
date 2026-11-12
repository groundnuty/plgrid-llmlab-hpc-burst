package lifecycle

// The router's opt-in on the controller's side (#429, beta; the science agent's rulings of 12 Nov
// 2026, Q1): --instance-router sets Config.InstanceRouter, each new Instance records it as its
// immutable launch.router, and the desired state carries a Router only for an Instance that has
// one. Alpha's desired state is pinned: its reference was captured once from main's own code at
// 3fc6001d, running pinDesiredJSON on pinInstance there, and frozen below, so it never comes from
// the code under test.

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// alphaDesiredJSON is main's desiredInstance for pinInstance, at 3fc6001d.
const alphaDesiredJSON = `{"name":"md-pin","uid":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","deploymentID":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","launch":{"image":"docker.io/vllm/vllm-openai","imageDigest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","command":["vllm","serve","/hpcburst/model","--uds","/hpcburst/run/worker-0.sock","--served-model-name","example-model","--max-num-seqs=256"],"env":[{"name":"EXAMPLE_VAR","value":"1"}],"keyEnv":"VLLM_API_KEY","checkpoint":{"repository":"example-org/example-model","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"gpus":1,"maxNumSeqs":256},"generation":1,"workers":[{"name":"worker-0","generation":2,"engineSocket":"/hpcburst/run/worker-0.sock"}]}`

// pinInstance is a fixed Instance whose desired state the alpha pin freezes: phase ready, so no
// checks are asked; restart generations 2 for worker-0 and 1 for the whole Instance; no key file.
func pinInstance() *v1alpha1.Instance {
	return &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "md-pin", Namespace: "ns", UID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
			Annotations: map[string]string{
				v1alpha1.RestartGenerationAnnotation(v1alpha1.WorkerName(0)): "2",
				v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance):  "1",
			},
		},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			IdentityRecord: "id-0123456789ab",
			AllocationRef:  v1alpha1.ObjectReference{Name: "alloc-pin"},
			Launch: v1alpha1.Launch{
				Image: "docker.io/vllm/vllm-openai", ImageDigest: "sha256:" + strings.Repeat("1", 64),
				Command:    []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--served-model-name", "example-model", "--max-num-seqs=256"},
				Env:        []v1alpha1.LaunchEnvVar{{Name: "EXAMPLE_VAR", Value: "1"}},
				KeyEnv:     "VLLM_API_KEY",
				Checkpoint: v1alpha1.LaunchCheckpoint{Repository: "example-org/example-model", Revision: strings.Repeat("a", 40)},
				GPUs:       1, MaxNumSeqs: 256,
			},
		},
		Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady},
	}
}

// pinDesiredJSON is the desired state the Backend renders for in, as JSON.
func pinDesiredJSON(t *testing.T, in *v1alpha1.Instance) string {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Reader: clientfake.NewClientBuilder().WithScheme(scheme).Build(), CredentialsDir: t.TempDir()}
	di, err := b.desiredInstance(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(di)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// With no router, the desired state is main's, byte for byte. The positive control: the same
// Instance with launch.router envoy differs by exactly one member, the router, at generation 0;
// and a raised router generation (the annotation the router rung raises) shows in it.
func TestAlphaDesiredStateIsUnchangedWithoutTheRouterFlag(t *testing.T) {
	if got := pinDesiredJSON(t, pinInstance()); got != alphaDesiredJSON {
		t.Fatalf("alpha's desired state changed:\n got %s\nwant %s", got, alphaDesiredJSON)
	}
	withRouter := pinInstance()
	withRouter.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	var alpha, beta map[string]any
	if err := json.Unmarshal([]byte(alphaDesiredJSON), &alpha); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(pinDesiredJSON(t, withRouter)), &beta); err != nil {
		t.Fatal(err)
	}
	router, ok := beta["router"]
	if !ok {
		t.Fatalf("an Instance with launch.router envoy has no router in its desired state: %v", beta)
	}
	if want := map[string]any{"kind": "envoy", "generation": float64(0)}; !reflect.DeepEqual(router, want) {
		t.Errorf("router = %v, want %v", router, want)
	}
	delete(beta, "router")
	// The launch the agent receives is the same: the router is the desired state's, not the engine's.
	if !reflect.DeepEqual(alpha, beta) {
		t.Errorf("beyond the router, the desired state differs:\n alpha %v\n  beta %v", alpha, beta)
	}
	withRouter.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartRouter)] = "3"
	b := &Backend{Reader: clientfake.NewClientBuilder().WithScheme(pinScheme(t)).Build(), CredentialsDir: t.TempDir()}
	di, err := b.desiredInstance(context.Background(), withRouter)
	if err != nil {
		t.Fatal(err)
	}
	if di.Router == nil || *di.Router != (agentapi.DesiredRouter{Kind: agentapi.RouterEnvoy, Generation: 3}) {
		t.Errorf("after the router rung raised it to 3, the desired router is %+v", di.Router)
	}
	if di.Workers[0].Generation != 2 || di.Generation != 1 {
		t.Errorf("a router generation moved another part's: workers %+v, instance %d", di.Workers, di.Generation)
	}
}

func pinScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// The flag is read when an Instance is created and recorded in its launch: a controller restarted
// with another flag leaves the running Instance as it was created, and only an Instance created
// after the restart takes the new value. Alpha's control: a controller without the flag creates an
// Instance with no router.
func TestTheInstanceRouterIsRecordedWhenTheInstanceIsCreated(t *testing.T) {
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.create(mdSpec("first"))
	rg.until(2*time.Minute, 10*time.Second, "an Instance of first", func() bool { return len(rg.instances("first")) == 1 })
	if got := rg.instances("first")[0].Spec.Launch.Router; got != v1alpha1.InstanceRouterEnvoy {
		t.Fatalf("an Instance created under --instance-router envoy has launch.router %q", got)
	}
	name := rg.instances("first")[0].Name

	rg.restartController() // the same store, Config rebuilt without the flag
	if rg.r.Config.InstanceRouter != "" {
		t.Fatalf("the restarted controller kept InstanceRouter %q", rg.r.Config.InstanceRouter)
	}
	rg.create(mdSpec("second"))
	rg.until(2*time.Minute, 10*time.Second, "an Instance of second", func() bool { return len(rg.instances("second")) == 1 })
	if got := rg.instance(name).Spec.Launch.Router; got != v1alpha1.InstanceRouterEnvoy {
		t.Errorf("after a restart without the flag, the running Instance's launch.router is %q, want envoy", got)
	}
	if got := rg.instances("second")[0].Spec.Launch.Router; got != "" {
		t.Errorf("an Instance created without the flag has launch.router %q, want none", got)
	}
}

// The deployment ID does not change with the router: it is placement, not identity (HBEP-9).
func TestTheInstanceRouterIsNoDeploymentIDInput(t *testing.T) {
	ids := map[string]string{}
	for _, router := range []v1alpha1.InstanceRouter{"", v1alpha1.InstanceRouterEnvoy} {
		rg := newRig(t)
		rg.r.Config.InstanceRouter = router
		rg.create(mdSpec("md"))
		rg.until(2*time.Minute, 10*time.Second, "an Instance", func() bool { return len(rg.instances("md")) == 1 })
		in := rg.instances("md")[0]
		ids[string(router)] = string(in.Spec.DeploymentID)
		if !strings.HasPrefix(in.Spec.IdentityRecord, "id-") {
			t.Errorf("identity record %q", in.Spec.IdentityRecord)
		}
	}
	if ids[""] == "" || ids[""] != ids["envoy"] {
		t.Errorf("deployment IDs without and with the router: %q and %q, want one ID", ids[""], ids["envoy"])
	}
}

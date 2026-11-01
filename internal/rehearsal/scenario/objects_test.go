package scenario_test

// The API objects of the rehearsal: one batch PlatformProfile whose partition
// is the fake cluster's, one Provider over the login node, and one served
// ModelDeployment. The engine double's modes travel as the engine's env, the
// way the real controller writes a launch (internal/lifecycle/desired.go).

import (
	"context"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	profileName  = "rehearsal"
	providerName = "rehearsal"
	partition    = "rehearsal-gpu"
	namespace    = "default"
	mdName       = "rehearsal"
	// model is the plain name clients ask the front for.
	model = "example-org/rehearsal-model"
)

func kubeClient(t *testing.T, state string) client.Client {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(state, "kubeconfig"))
	if err != nil {
		t.Fatal(err)
	}
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

func ptr[T any](v T) *T { return &v }

// timing is the rehearsal's scaled clock: a lease of minutes, not HBEP-7's
// 2 h 10 min drain window. The drain window is routing.maxRequestDuration plus
// the profile's drainMargin, when that is shorter than the profile's
// drainWindow (api/v1alpha1/writers.go, InstanceDrainWindow).
type timing struct {
	lease, maxRequest, drainMargin, stopGrace, agentTimeout string
}

func batchProfile(arch string, tm timing) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: profileName},
		Spec: v1alpha1.PlatformProfileSpec{
			ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42",
			InflightWindow: dur("5m"), StagingDir: "$SCRATCH/stage",
			DrainMargin: dur(tm.drainMargin), HandoverMargin: dur("0s"),
			Restart: &v1alpha1.RestartPolicy{StopGrace: dur(tm.stopGrace)},
			Health:  &v1alpha1.HealthThresholds{AgentTimeout: dur(tm.agentTimeout)},
			Partitions: []v1alpha1.PartitionProfile{{
				Name: partition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu", CPUArch: arch},
				MaxTime: dur("30m"), GPUsPerNode: ptr(int32(1)), CPUsPerGPU: ptr(int32(2)),
				MemPerGPU: ptr(resource.MustParse("2Gi")),
			}},
		},
	}
}

func batchProvider(creds string) *v1alpha1.Provider {
	return &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: providerName},
		Spec: v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: profileName},
			AllowedPartitions:  []string{partition},
			CredentialsRef:     &v1alpha1.CredentialsReference{File: creds},
		},
	}
}

// Engine identity of the double: any digest of the right form; the fake
// apptainer pulls nothing.
const (
	checkpointRepo = "example-org/rehearsal-model"
	checkpointRev  = "0123456789abcdef0123456789abcdef01234567"
	engineDigest   = "sha256:" + "ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"
)

func servedDeployment(tm timing, engineEnv ...v1alpha1.EnvVar) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: mdName, Namespace: namespace},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: model,
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: checkpointRepo, Revision: checkpointRev},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: engineDigest,
					Image: "docker.io/vllm/vllm-openai"},
				Flags: []v1alpha1.EngineFlag{{Name: "max-model-len", Value: "4096"}},
				Env:   engineEnv,
			}},
			Instances:  1,
			Providers:  v1alpha1.ProviderSelection{Names: []string{providerName}},
			LeaseLimit: dur(tm.lease),
			Routing:    &v1alpha1.Routing{MaxRequestDuration: dur(tm.maxRequest)},
			Workload: &v1alpha1.Workload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 512},
				// Longest defaults to 4096, which with the typical context
				// would not fit max-model-len 4096 (HBEP-7: every shape fits).
				OutputTokens:   v1alpha1.OutputTokens{Typical: 64, Longest: ptr(int32(256))},
				TargetSessions: 2,
			},
		},
	}
}

func create(t *testing.T, c client.Client, objs ...client.Object) {
	t.Helper()
	for _, o := range objs {
		if err := c.Create(context.Background(), o); err != nil {
			t.Fatalf("create %T %s: %v", o, o.GetName(), err)
		}
	}
}

// conditionTrue reports whether the object's condition has status True.
func conditionTrue(conds []metav1.Condition, typ string) bool {
	return meta.IsStatusConditionTrue(conds, typ)
}

// eventsFor lists the events about an object, newest last.
func eventsFor(t *testing.T, c client.Client, name string) []corev1.Event {
	t.Helper()
	var l corev1.EventList
	if err := c.List(context.Background(), &l); err != nil {
		t.Fatalf("list events: %v", err)
	}
	var out []corev1.Event
	for _, e := range l.Items {
		if e.InvolvedObject.Name == name {
			out = append(out, e)
		}
	}
	return out
}

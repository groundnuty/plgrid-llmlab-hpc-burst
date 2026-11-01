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
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
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
	c, err := client.New(restConfig(t, state), client.Options{Scheme: apiScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// restConfig is the API's client configuration from up's kubeconfig. Several
// waits poll at once, so the client-side rate limit is raised above
// client-go's default of 5 per second, which would slow every wait.
func restConfig(t *testing.T, state string) *rest.Config {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(state, "kubeconfig"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.QPS, cfg.Burst = 50, 100
	return cfg
}

func apiScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

func ptr[T any](v T) *T { return &v }

// timing is the rehearsal's scaled clock: a lease of minutes, not HBEP-7's
// 2 h 10 min drain window. The drain window is routing.maxRequestDuration plus
// the profile's drainMargin, when that is shorter than the profile's
// drainWindow (api/v1alpha1/writers.go, InstanceDrainWindow). The profile sets
// drainWindow explicitly, longer than that sum, so the test computes the drain
// start from the spec alone (drainStartOf) rather than with the helper the
// controller and the front share.
type timing struct {
	lease, maxRequest, drainMargin, drainWindow, stopGrace, agentTimeout string
}

func batchProfile(arch string, tm timing) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: profileName},
		Spec: v1alpha1.PlatformProfileSpec{
			ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42",
			InflightWindow: dur("5m"), StagingDir: "$SCRATCH/stage",
			DrainMargin: dur(tm.drainMargin), DrainWindow: dur(tm.drainWindow), HandoverMargin: dur("0s"),
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
			// Step (g) expects the Allocations to end at scale 0: the default,
			// Keep, would hold a warm Allocation until its hard end. 10 s is
			// the scaled idle limit.
			RetentionPolicy: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur("10s")},
			Routing:         &v1alpha1.Routing{MaxRequestDuration: dur(tm.maxRequest)},
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

// orphanEvents lists the events.k8s.io Events whose reason is TunnelOrphaned:
// frps listed a proxy online with no current Instance for TunnelOrphanGrace
// (HBEP-6, an orphan). The controller raises it regarding the Allocation that
// admitted the tunnel, or its Lease.
func orphanEvents(t *testing.T, c client.Client) []eventsv1.Event {
	t.Helper()
	var l eventsv1.EventList
	if err := c.List(context.Background(), &l); err != nil {
		t.Fatalf("list events.k8s.io events: %v", err)
	}
	var out []eventsv1.Event
	for _, e := range l.Items {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			out = append(out, e)
		}
	}
	return out
}

// reportOrphanedTunnels logs each TunnelOrphaned event so far as a FINDING
// about the agent's teardown order, and returns how many there are.
func reportOrphanedTunnels(t *testing.T, c client.Client, step string) int {
	t.Helper()
	evs := orphanEvents(t, c)
	for _, e := range evs {
		t.Logf("FINDING step (%s): %s regarding %s %s: %.300s", step, e.Reason, e.Regarding.Kind, e.Regarding.Name, e.Note)
	}
	t.Logf("step (%s): %d TunnelOrphaned events", step, len(evs))
	return len(evs)
}

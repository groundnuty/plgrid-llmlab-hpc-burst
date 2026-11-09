package stack

// The stand-in's API objects: one batch PlatformProfile whose partition is the
// fake cluster's, one Provider over the login node, and one served
// ModelDeployment. The engine double's modes travel as the engine's env, the
// way the real controller writes a launch (internal/lifecycle/desired.go).

import (
	"runtime"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The rehearsal's names.
const (
	ProfileName  = "rehearsal"
	ProviderName = "rehearsal"
	Partition    = "rehearsal-gpu"
	Namespace    = "default"
	MDName       = "rehearsal"
	// Model is the plain name clients ask the front for.
	Model = "example-org/rehearsal-model"
)

// Engine identity of the double: any digest of the right form; the fake
// apptainer pulls nothing.
const (
	CheckpointRepo = "example-org/rehearsal-model"
	CheckpointRev  = "0123456789abcdef0123456789abcdef01234567"
	EngineDigest   = "sha256:" + "ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"
)

// Dur is an API duration.
func Dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

// Ptr is a pointer to v.
func Ptr[T any](v T) *T { return &v }

// Timing is the stand-in's scaled clock: a lease of minutes, not HBEP-7's
// 2 h 10 min drain window. The drain window is routing.maxRequestDuration plus
// the profile's drainMargin, when that is shorter than the profile's
// drainWindow (api/v1alpha1/writers.go, InstanceDrainWindow).
type Timing struct {
	Lease, MaxRequest, DrainMargin, DrainWindow, HandoverMargin, StopGrace, AgentTimeout, StartTimeout string
}

// CPUArch is this host's architecture as a profile names it. A host whose
// architecture no profile names fails the test at once: a stand-in must not go
// on into its setup with an empty class.
func CPUArch(t T) string {
	t.Helper()
	switch runtime.GOARCH {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	}
	t.Fatalf("no profile CPU architecture for GOARCH %s", runtime.GOARCH)
	return ""
}

// BatchProfile is the stand-in's batch profile: one partition, the fake
// cluster's, and the timing given.
func BatchProfile(arch string, tm Timing) *v1alpha1.PlatformProfile {
	hm := tm.HandoverMargin
	if hm == "" {
		hm = "0s"
	}
	return &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: ProfileName},
		Spec: v1alpha1.PlatformProfileSpec{
			ProviderKind: v1alpha1.PlatformBatch, HoldRelease: true, JSONDataParser: "v0.0.42",
			InflightWindow: Dur("5m"), StagingDir: "$SCRATCH/stage",
			DrainMargin: Dur(tm.DrainMargin), DrainWindow: Dur(tm.DrainWindow), HandoverMargin: Dur(hm),
			Restart: &v1alpha1.RestartPolicy{StopGrace: Dur(tm.StopGrace)},
			Health:  &v1alpha1.HealthThresholds{AgentTimeout: Dur(tm.AgentTimeout), StartTimeout: Dur(tm.StartTimeout)},
			Partitions: []v1alpha1.PartitionProfile{{
				Name: Partition, AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "example-gpu", CPUArch: arch},
				MaxTime: Dur("30m"), GPUsPerNode: Ptr(int32(1)), CPUsPerGPU: Ptr(int32(2)),
				MemPerGPU: Ptr(resource.MustParse("2Gi")),
			}},
		},
	}
}

// BatchProvider is the stand-in's Provider over the login node, with the
// credentials file the login node wrote.
func BatchProvider(creds string) *v1alpha1.Provider {
	return &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: ProviderName},
		Spec: v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: ProfileName},
			AllowedPartitions:  []string{Partition},
			CredentialsRef:     &v1alpha1.CredentialsReference{File: creds},
		},
	}
}

// ServedDeployment is the stand-in's served ModelDeployment of the engine
// double, on ProviderName, with the timing given.
func ServedDeployment(tm Timing, engineEnv ...v1alpha1.EnvVar) *v1alpha1.ModelDeployment {
	return ServedDeploymentAs(Served{Name: MDName, Model: Model, Provider: ProviderName, MaxModelLen: "4096"}, tm, engineEnv...)
}

// Served names one served ModelDeployment: its name, the model clients ask for,
// its one Provider and the engine's max-model-len.
type Served struct {
	Name, Model, Provider, MaxModelLen string
}

// ServedDeploymentAs is ServedDeployment under the names given.
func ServedDeploymentAs(d Served, tm Timing, engineEnv ...v1alpha1.EnvVar) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: d.Name, Namespace: Namespace},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model: d.Model,
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{
				Checkpoint: v1alpha1.CheckpointSpec{Repository: CheckpointRepo, Revision: CheckpointRev},
				Engine: v1alpha1.EngineSpec{Name: "vllm", Version: "0.32.0", ImageDigest: EngineDigest,
					Image: "docker.io/vllm/vllm-openai"},
				Flags: []v1alpha1.EngineFlag{{Name: "max-model-len", Value: d.MaxModelLen}},
				Env:   engineEnv,
			}},
			Instances:  1,
			Providers:  v1alpha1.ProviderSelection{Names: []string{d.Provider}},
			LeaseLimit: Dur(tm.Lease),
			// The Allocations end at scale 0: the default, Keep, would hold a
			// warm Allocation until its hard end. 10 s is the scaled idle limit.
			RetentionPolicy: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: Dur("10s")},
			Routing:         &v1alpha1.Routing{MaxRequestDuration: Dur(tm.MaxRequest)},
			Workload: &v1alpha1.Workload{
				ContextTokens: v1alpha1.ContextTokens{Typical: 512},
				// Longest defaults to 4096, which with the typical context
				// would not fit max-model-len 4096 (HBEP-7: every shape fits).
				OutputTokens:   v1alpha1.OutputTokens{Typical: 64, Longest: Ptr(int32(256))},
				TargetSessions: 2,
			},
		},
	}
}

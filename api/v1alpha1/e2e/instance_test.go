package e2e

import (
	"reflect"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// An Instance's spec is immutable, so its identity reference is too: an
// Instance never changes its deployment ID or its Allocation (HBEP-3, HBEP-11).
func TestInstanceSpecIsImmutable(t *testing.T) {
	const msg = "an Instance's spec is immutable"
	ns := newNamespace(t)
	in := newInstance(ns, "immutable")
	must(t, c.Create(bg(), in), "create")

	other := strings.Repeat("a", 64)
	refused := map[string]func(*v1alpha1.Instance){
		"the deployment ID": func(i *v1alpha1.Instance) {
			i.Spec.DeploymentID = v1alpha1.FullDeploymentID(other)
			i.Spec.IdentityRecord = "id-" + other[:12] // consistent, so only immutability refuses it
		},
		"the identity record alone": func(i *v1alpha1.Instance) { i.Spec.IdentityRecord = "id-" + other[:12] },
		"the Allocation":            func(i *v1alpha1.Instance) { i.Spec.AllocationRef.Name = "helios-a-zzzzz" },
		// The launch is immutable with the rest of the spec, so a restart runs
		// the original launch (HBEP-7, What the controller reads).
		"the launch's command": func(i *v1alpha1.Instance) { i.Spec.Launch.Command = append(i.Spec.Launch.Command, "--seed=1") },
		"the launch's image digest": func(i *v1alpha1.Instance) {
			i.Spec.Launch.ImageDigest = "sha256:" + strings.Repeat("5", 64)
		},
		"the launch's environment": func(i *v1alpha1.Instance) {
			i.Spec.Launch.Env = []v1alpha1.LaunchEnvVar{{Name: "VLLM_LOGGING_LEVEL", Value: "DEBUG"}}
		},
		"the launch's GPU count": func(i *v1alpha1.Instance) { i.Spec.Launch.GPUs = 2 },
	}
	for what, edit := range refused {
		t.Run("refuse changing "+what, func(t *testing.T) {
			got := &v1alpha1.Instance{}
			must(t, c.Get(bg(), objKey(in), got), "get")
			edit(got)
			requireRefused(t, c.Update(bg(), got), msg)
		})
	}

	// The same object written back, a label, an annotation and a finalizer are all accepted.
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	must(t, c.Update(bg(), got), "write back unchanged")
	got.Labels = map[string]string{v1alpha1.LabelProvider: "helios-a", v1alpha1.LabelDeploymentID: fullID[:12]}
	got.Annotations = map[string]string{"example/note": "x"}
	got.Finalizers = []string{v1alpha1.FinalizerDrain}
	must(t, c.Update(bg(), got), "update metadata only")
	got.Finalizers = nil
	must(t, c.Update(bg(), got), "remove the finalizer")
	if !reflect.DeepEqual(got.Spec, in.Spec) {
		t.Fatalf("spec changed: %+v -> %+v", in.Spec, got.Spec)
	}
}

func TestInstanceIdentityReference(t *testing.T) {
	ns := newNamespace(t)
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.Instance)
		want   string
	}{
		{"a 12-character ID where the full ID is required", func(i *v1alpha1.Instance) { i.Spec.DeploymentID = v1alpha1.FullDeploymentID(fullID[:12]) }, "deploymentID"},
		{"a 63-character ID", func(i *v1alpha1.Instance) { i.Spec.DeploymentID = v1alpha1.FullDeploymentID(fullID[:63]) }, "deploymentID"},
		{"uppercase hex", func(i *v1alpha1.Instance) { i.Spec.DeploymentID = v1alpha1.FullDeploymentID(strings.ToUpper(fullID)) }, "deploymentID"},
		{"an identity record of another ID", func(i *v1alpha1.Instance) { i.Spec.IdentityRecord = "id-" + strings.Repeat("a", 12) },
			"identityRecord must be 'id-' followed by the first 12 characters of deploymentID"},
		{"an identity record without the prefix", func(i *v1alpha1.Instance) { i.Spec.IdentityRecord = fullID[:12] },
			"identityRecord must be 'id-' followed by the first 12 characters of deploymentID"},
		{"an identity record named with the full ID", func(i *v1alpha1.Instance) { i.Spec.IdentityRecord = "id-" + fullID },
			"identityRecord must be 'id-' followed by the first 12 characters of deploymentID"},
		{"no Allocation", func(i *v1alpha1.Instance) { i.Spec.AllocationRef.Name = "" }, "allocationRef"},
		{"no launch command", func(i *v1alpha1.Instance) { i.Spec.Launch.Command = nil }, "launch.command"},
		{"a launch image digest that is a tag", func(i *v1alpha1.Instance) { i.Spec.Launch.ImageDigest = "latest" }, "launch.imageDigest"},
		{"a launch checkpoint at a branch", func(i *v1alpha1.Instance) { i.Spec.Launch.Checkpoint.Revision = "main" }, "launch.checkpoint.revision"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			in := newInstance(ns, uniq("inst"))
			tc.mutate(in)
			requireRefused(t, c.Create(bg(), in), tc.want)
		})
	}
	t.Run("accept a consistent reference", func(t *testing.T) {
		in := newInstance(ns, uniq("inst"))
		must(t, c.Create(bg(), in), "create")
	})
}

// The launch's command passes max-num-seqs only as one argument,
// --max-num-seqs=<n>, with n equal to launch.maxNumSeqs, so the front's cap and
// the engine's limit cannot disagree. This is the rule instances:$.spec.launch#0.
func TestInstanceLaunchCommandAgreesWithMaxNumSeqs(t *testing.T) {
	const msg = "command passes max-num-seqs only as --max-num-seqs=<launch.maxNumSeqs>"
	ns := newNamespace(t)
	// withCommand returns a mutation that replaces the launch's command (the
	// first three arguments are the program, its subcommand and the model) and
	// sets launch.maxNumSeqs.
	withCommand := func(maxNumSeqs int32, args ...string) func(*v1alpha1.Instance) {
		return func(i *v1alpha1.Instance) {
			i.Spec.Launch.Command = append([]string{"vllm", "serve", "/hpcburst/model"}, args...)
			i.Spec.Launch.MaxNumSeqs = maxNumSeqs
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.Instance)
	}{
		{"a value other than launch.maxNumSeqs", withCommand(256, "--max-num-seqs=8")},
		{"launch.maxNumSeqs other than the value", withCommand(128, "--max-num-seqs=256")},
		{"the two-argument form", withCommand(256, "--max-num-seqs", "256")},
		{"the underscore spelling", withCommand(256, "--max_num_seqs=256")},
		{"the underscore spelling with another value", withCommand(256, "--max_num_seqs=8")},
		{"a value that is not a number", withCommand(256, "--max-num-seqs=abc")},
		{"an empty value", withCommand(256, "--max-num-seqs=")},
		{"the flag with no value", withCommand(256, "--max-num-seqs")},
		{"a correct argument and a wrong one", withCommand(256, "--max-num-seqs=256", "--max_num_seqs=8")},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			in := newInstance(ns, uniq("inst"))
			tc.mutate(in)
			requireRefused(t, c.Create(bg(), in), msg)
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.Instance)
	}{
		{"the argument with launch.maxNumSeqs", withCommand(256, "--max-num-seqs=256")},
		{"the argument among other flags", withCommand(64, "--dtype=bfloat16", "--max-num-seqs=64", "--seed=1")},
		{"a command with no max-num-seqs argument", withCommand(256, "--dtype=bfloat16")},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			in := newInstance(ns, uniq("inst"))
			tc.mutate(in)
			must(t, c.Create(bg(), in), "create")
		})
	}
}

func TestInstancePhasesAndStatus(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "phases")
	must(t, c.Create(bg(), in), "create")

	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Phase = "running"
	requireRefused(t, c.Status().Update(bg(), got), "Unsupported value")

	got.Status.Phase = v1alpha1.InstanceVerifying
	got.Status.Verdict = "maybe"
	requireRefused(t, c.Status().Update(bg(), got), "Unsupported value")

	// Every phase of HBEP-3 is accepted, with the Instance's metadata.
	for _, ph := range []v1alpha1.InstancePhase{v1alpha1.InstancePending, v1alpha1.InstanceStarting, v1alpha1.InstanceVerifying,
		v1alpha1.InstanceReady, v1alpha1.InstanceDraining, v1alpha1.InstanceEnded} {
		must(t, c.Get(bg(), objKey(in), got), "get")
		got.Status.Phase = ph
		got.Status.Verdict = v1alpha1.VerdictPass
		got.Status.VerificationRecord = "ver-example"
		got.Status.JobID, got.Status.Node, got.Status.URL = "424242", "node-1", "https://example.invalid/v1"
		must(t, c.Status().Update(bg(), got), "phase "+string(ph))
	}
	must(t, c.Get(bg(), objKey(in), got), "get")
	if got.Status.Phase != v1alpha1.InstanceEnded || got.Status.JobID != "424242" {
		t.Fatalf("status = %+v", got.Status)
	}

	// The main resource ignores status.
	got.Status.Phase = v1alpha1.InstancePending
	must(t, c.Update(bg(), got), "update with a changed status")
	again := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), again), "get")
	if again.Status.Phase != v1alpha1.InstanceEnded {
		t.Fatalf("the main resource wrote the status: phase = %s", again.Status.Phase)
	}
}

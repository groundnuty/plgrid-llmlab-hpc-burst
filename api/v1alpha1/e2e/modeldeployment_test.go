package e2e

import (
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

type mdCase struct {
	name   string
	mutate func(*v1alpha1.ModelDeployment)
	want   string // refusals only: the rule's message
}

func runMDCases(t *testing.T, refused, accepted []mdCase) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			md := newModelDeployment(ns, uniq("md"))
			tc.mutate(md)
			requireRefused(t, c.Create(bg(), md), tc.want)
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			md := newModelDeployment(ns, uniq("md"))
			tc.mutate(md)
			must(t, c.Create(bg(), md), "create ModelDeployment")
		})
	}
}

func setFlags(flags ...v1alpha1.EngineFlag) func(*v1alpha1.ModelDeployment) {
	return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Flags = flags }
}

// A branch name is refused; only a full commit id is admitted (HBEP-3, HBEP-9).
func TestCheckpointRevisionIsAFullCommitID(t *testing.T) {
	const msg = "revision must be a full commit id"
	rev := func(r string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Checkpoint.Revision = r }
	}
	runMDCases(t, []mdCase{
		{"a branch name", rev("main"), msg},
		{"a tag", rev("v1.0"), msg},
		{"an abbreviated id", rev(commit40[:7]), msg},
		{"39 hex characters", rev(commit40[:39]), msg},
		{"41 hex characters", rev(commit40 + "a"), msg},
		{"uppercase hex", rev(strings.ToUpper(commit40)), msg},
		{"a ref path", rev("refs/heads/main"), msg},
		{"an empty revision", rev(""), msg},
	}, []mdCase{
		{"a full commit id", rev(commit40), ""},
		{"another full commit id", rev("fedcba9876543210fedcba9876543210fedcba98"), ""},
	})
}

func TestEngineImageDigest(t *testing.T) {
	const msg = "imageDigest must be 'sha256:'"
	dig := func(d string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Engine.ImageDigest = d }
	}
	runMDCases(t, []mdCase{
		{"a tag instead of a digest", dig("latest"), msg},
		{"a short digest", dig("sha256:abcd"), msg},
		{"no prefix", dig(strings.Repeat("2", 64)), msg},
		{"uppercase hex", dig("sha256:" + strings.Repeat("A", 64)), msg},
		{"another algorithm", dig("sha512:" + strings.Repeat("2", 64)), msg},
	}, []mdCase{{"a sha256 digest", dig("sha256:" + strings.Repeat("a", 64)), ""}})
}

// Short flag aliases and secret flags are refused in specs (HBEP-3, HBEP-11),
// and flag and environment names are ASCII with one spelling (HBEP-3 owner
// decision, PR #46 comment 6521536735).
func TestEngineFlagsUseLongNamesAndCarryNoKeys(t *testing.T) {
	flag := func(name string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: "1"})
	}
	const (
		dashes  = "an engine flag name is a long name without leading dashes"
		charset = "an engine flag name matches ^[a-z0-9][a-z0-9-]*$"
		alias   = "a short flag alias such as tp, pp or dp is refused"
		keys    = "a key is never written in a spec"
		passed  = "the framework passes model, revision and chat-template itself"
	)
	refusedAliases := []string{"tp", "pp", "dp", "dcp", "pcp", "dpn", "ep", "q", "n", "r", "sc", "cc", "ac", "dc", "asc"}
	var refused []mdCase
	for _, a := range refusedAliases {
		refused = append(refused, mdCase{"the short alias " + a, flag(a), alias})
	}
	refused = append(refused, []mdCase{
		{"a dashed short alias, -tp", flag("-tp"), dashes},
		{"a dashed short alias, -O", flag("-O"), dashes},
		{"a long name with its dashes", flag("--max-model-len"), dashes},
		{"underscores, max_model_len", flag("max_model_len"), charset},
		{"underscores, max_num_seqs", flag("max_num_seqs"), charset},
		{"uppercase", flag("Max-Model-Len"), charset},
		{"a value glued to the name", flag("max-model-len=4096"), charset},
		{"a space in the name", flag("max model len"), charset},
		{"a non-ASCII letter", flag("max-modèl-len"), charset},
		{"an empty name", flag(""), "name"},
		{"the api-key flag", flag("api-key"), keys},
		{"the hf-token flag", flag("hf-token"), keys},
		{"the model flag", flag("model"), passed},
		{"the revision flag", flag("revision"), passed},
		{"the chat-template flag", flag("chat-template"), passed},
	}...)
	runMDCases(t, refused, []mdCase{
		{"long names", setFlags(
			v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "4"},
			v1alpha1.EngineFlag{Name: "pipeline-parallel-size", Value: "2"},
			v1alpha1.EngineFlag{Name: "data-parallel-size", Value: "1"},
			v1alpha1.EngineFlag{Name: "enable-auto-tool-choice", Value: "true"}), ""},
		{"a boolean set to false", flag("enable-prefix-caching"), ""},
		{"a flag whose name merely contains token or key", setFlags(
			v1alpha1.EngineFlag{Name: "tokenizer-mode", Value: "auto"},
			v1alpha1.EngineFlag{Name: "kv-cache-dtype", Value: "fp8"}), ""},
		{"a name that starts with a digit", flag("8bit-cache"), ""},
		{"a flag given twice, in order", setFlags(
			v1alpha1.EngineFlag{Name: "override-generation-config", Value: `{"a":1}`},
			v1alpha1.EngineFlag{Name: "override-generation-config", Value: `{"b":2}`}), ""},
		{"a JSON value kept as a string", flag("hf-overrides"), ""},
		{"no flags", setFlags(), ""},
	})
}

func TestEngineEnvironment(t *testing.T) {
	env := func(vars ...v1alpha1.EnvVar) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Env = vars }
	}
	one := func(name string) func(*v1alpha1.ModelDeployment) {
		return env(v1alpha1.EnvVar{Name: name, Value: "1"})
	}
	const charset = "an environment variable name matches ^[A-Z_][A-Z0-9_]*$"
	runMDCases(t, []mdCase{
		{"HF_TOKEN", one("HF_TOKEN"), "a key is never written in a spec"},
		{"a lowercase name", one("hf_home"), charset},
		{"a mixed-case name", one("Hf_Home"), charset},
		{"a name with a dash", one("VLLM-USE"), charset},
		{"a name that starts with a digit", one("1VLLM"), charset},
		{"a non-ASCII letter", one("VLLM_É"), charset},
		{"a name with a space", one("VLLM USE"), charset},
		{"the same variable twice", env(
			v1alpha1.EnvVar{Name: "VLLM_USE_DEEP_GEMM", Value: "1"}, v1alpha1.EnvVar{Name: "VLLM_USE_DEEP_GEMM", Value: "0"}), "Duplicate value"},
	}, []mdCase{
		{"an input variable", one("VLLM_USE_DEEP_GEMM"), ""},
		{"a name with a leading underscore", one("_PRIVATE"), ""},
		{"a name with digits", one("CUDA_VISIBLE_DEVICES_0"), ""},
		{"two different variables", env(
			v1alpha1.EnvVar{Name: "HF_HOME", Value: "/data/hf"}, v1alpha1.EnvVar{Name: "VLLM_LOGGING_LEVEL", Value: "INFO"}), ""},
	})
}

func TestModelDeploymentProvidersAndInstances(t *testing.T) {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{v1alpha1.LabelPlatform: "example-platform"}}
	runMDCases(t, []mdCase{
		{"no provider and no selector", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers = v1alpha1.ProviderSelection{} },
			"name at least one provider, or give a label selector"},
		{"an empty name list", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{}} },
			"name at least one provider, or give a label selector"},
		{"a provider name with a dot", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{"helios.a"} }, "names"},
		{"a provider name with uppercase letters", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{"Helios-a"} }, "names"},
		{"a negative number of instances", func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = -1 }, "instances"},
		{"no model name", func(md *v1alpha1.ModelDeployment) { md.Spec.Model = "" }, "model"},
	}, []mdCase{
		{"providers by name", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{"helios-a", "athena-a"} }, ""},
		{"providers by selector only", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers = v1alpha1.ProviderSelection{Selector: sel} }, ""},
		{"zero instances", func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 0 }, ""},
		{"an end time", func(md *v1alpha1.ModelDeployment) { md.Spec.EndTime = &metav1.Time{Time: metav1.Now().Add(3600e9)} }, ""},
		{"a model name with a slash and an at sign", func(md *v1alpha1.ModelDeployment) { md.Spec.Model = "org/m@x" }, ""},
	})
}

func TestRetentionPolicy(t *testing.T) {
	const msg = "idleLimit is required with mode ReleaseWhenIdle and refused with mode Keep"
	pol := func(p v1alpha1.RetentionPolicy) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.RetentionPolicy = &p }
	}
	until := &metav1.Time{Time: metav1.Now().Add(86400e9)}
	runMDCases(t, []mdCase{
		{"release when idle without an idle limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle}), msg},
		{"keep with an idle limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, IdleLimit: ptr.To(v1alpha1.Duration("10m"))}), msg},
		{"an unknown mode", pol(v1alpha1.RetentionPolicy{Mode: "Forever"}), "Unsupported value"},
		{"an idle limit that is not a duration", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("soon"))}), "idleLimit"},
	}, []mdCase{
		{"keep until a date", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: until}), ""},
		{"keep with no date (the deployment's end time)", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}), ""},
		{"release when idle after a limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("30m"))}), ""},
	})
}

// A field the spec omits takes its schema default: one instance, and the Keep
// mode of an empty retention policy.
func TestModelDeploymentDefaults(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "defaults")
	u := &unstructured.Unstructured{}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(md)
	must(t, err, "convert")
	u.Object = obj
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("ModelDeployment")
	unstructured.RemoveNestedField(u.Object, "spec", "instances")
	must(t, unstructured.SetNestedMap(u.Object, map[string]any{}, "spec", "retentionPolicy"), "empty retention policy")
	must(t, c.Create(bg(), u), "create without instances")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if got.Spec.Instances != 1 {
		t.Errorf("spec.instances defaulted to %d, want 1", got.Spec.Instances)
	}
	if got.Spec.RetentionPolicy == nil || got.Spec.RetentionPolicy.Mode != v1alpha1.RetentionKeep {
		t.Errorf("an empty retention policy = %+v, want mode Keep", got.Spec.RetentionPolicy)
	}
}

// An edit to a ModelDeployment's serving spec is allowed: it rolls out new
// deployment IDs (HBEP-3, open question 7; HBEP-11, Admission). This accepts
// the edit; the refused edits are the ones a rule names.
func TestServingSpecMayChange(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "edit")
	must(t, c.Create(bg(), md), "create")
	for step, edit := range []func(*v1alpha1.ModelDeployment){
		func(m *v1alpha1.ModelDeployment) {
			m.Spec.Serving.Checkpoint.Revision = "fedcba9876543210fedcba9876543210fedcba98"
		},
		func(m *v1alpha1.ModelDeployment) {
			m.Spec.Serving.Flags = append(m.Spec.Serving.Flags, v1alpha1.EngineFlag{Name: "seed", Value: "7"})
		},
		func(m *v1alpha1.ModelDeployment) { m.Spec.Serving.ChatTemplate = "{{ messages }}" },
		func(m *v1alpha1.ModelDeployment) { m.Spec.Serving.Engine.Version = "0.33.0" },
		func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 },
	} {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		before := got.Generation
		edit(got)
		must(t, c.Update(bg(), got), "edit the serving spec, step "+string(rune('1'+step)))
		if got.Generation <= before {
			t.Errorf("step %d: generation did not advance (%d -> %d)", step+1, before, got.Generation)
		}
	}
	// The same edit that the create-time rule refuses stays refused on update.
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Spec.Serving.Checkpoint.Revision = "main"
	requireRefused(t, c.Update(bg(), got), "revision must be a full commit id")
}

// The scale subresource reads and writes the number of instances.
func TestScaleSubresource(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "scale")
	must(t, c.Create(bg(), md), "create")
	scale := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 5}}
	must(t, c.SubResource("scale").Update(bg(), md, client.WithSubResourceBody(scale)), "scale to 5")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if got.Spec.Instances != 5 {
		t.Fatalf("spec.instances = %d after scaling to 5", got.Spec.Instances)
	}
	got.Status.Instances = 4
	must(t, c.Status().Update(bg(), got), "write status.instances")
	read := &autoscalingv1.Scale{}
	must(t, c.SubResource("scale").Get(bg(), got, read), "read the scale")
	if read.Spec.Replicas != 5 || read.Status.Replicas != 4 {
		t.Fatalf("scale = spec %d, status %d; want 5 and 4", read.Spec.Replicas, read.Status.Replicas)
	}
}

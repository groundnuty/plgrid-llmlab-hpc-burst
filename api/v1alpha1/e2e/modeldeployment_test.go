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

// The engine's digest is written under the canonical object's own key,
// image_digest, as gpu_model and cpu_arch are (HBEP-3: code-font names stay
// verbatim). The check reads the stored object, not the Go type.
func TestEngineDigestKeyIsTheCanonicalOne(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "digest-key")
	must(t, c.Create(bg(), md), "create")
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("ModelDeployment"))
	must(t, c.Get(bg(), objKey(md), u), "get unstructured")
	engine, _, _ := unstructured.NestedMap(u.Object, "spec", "serving", "engine")
	if engine["image_digest"] != digest {
		t.Errorf("spec.serving.engine.image_digest = %v, want %s; keys %v", engine["image_digest"], digest, engine)
	}
	if _, wrong := engine["imageDigest"]; wrong {
		t.Errorf("spec.serving.engine.imageDigest is written: %v", engine)
	}
}

func TestEngineImageDigest(t *testing.T) {
	const msg = "image_digest must be 'sha256:'"
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

// vLLM v0.32.0's short flag aliases, collected from the option strings of
// vllm/engine/arg_utils.py and vllm/entrypoints/launchers/cli_args.py, not from
// the admission rule: a rule that lost an entry must fail this list.
var vllmShortAliases = []string{
	"ac", "asc", "cc", "dc", "dcp", "dp", "dpa", "dpb", "dpe", "dph", "dpl", "dpm", "dpn", "dpp", "dpr",
	"ep", "n", "pcp", "pp", "q", "r", "sc", "tp",
}

// vLLM v0.32.0's boolean flags (argparse's BooleanOptionalAction, which also
// generates --no-<name>) among those in HBEP-3's v1 table: vllm/engine/arg_utils.py
// line 380 builds the action for every bool field. disable-log-stats is not here:
// it is a store_true flag, not a BooleanOptionalAction.
var vllmBooleanFlags = []string{
	"trust-remote-code", "enforce-eager", "enable-expert-parallel", "enable-auto-tool-choice",
	"enable-prefix-caching", "enable-chunked-prefill", "enable-log-requests",
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
	var refused []mdCase
	for _, a := range vllmShortAliases {
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
		{"a boolean set to true", setFlags(v1alpha1.EngineFlag{Name: "enable-prefix-caching", Value: "true"}), ""},
		{"a boolean set to false", setFlags(v1alpha1.EngineFlag{Name: "enable-prefix-caching", Value: "false"}), ""},
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

// vLLM's parsers never set allow_abbrev, so argparse expands a unique prefix:
// "--api-ke SECRET" is "--api-key SECRET" (review finding B3; checked against
// FlexibleArgumentParser in vllm/utils/argparse_utils.py, v0.32.0). Every prefix of
// a refused long name is refused with it, and config is refused outright
// because --config can carry any flag.
func TestEngineFlagsRefuseAbbreviationsOfRefusedNames(t *testing.T) {
	flag := func(name string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: "x"})
	}
	const (
		keys   = "a key is never written in a spec"
		passed = "the framework passes model, revision and chat-template itself"
		config = "config is refused"
	)
	var refused []mdCase
	for _, n := range []string{"api-ke", "api-k", "api-", "api", "ap", "hf-tok", "hf-t", "hf-", "hf", "h"} {
		refused = append(refused, mdCase{"the abbreviation " + n + " of a key flag", flag(n), keys})
	}
	for _, n := range []string{"mode", "mod", "mo", "revisio", "revis", "rev", "chat-templat", "chat-temp", "chat-", "chat", "ch"} {
		refused = append(refused, mdCase{"the abbreviation " + n + " of a flag the framework passes", flag(n), passed})
	}
	for _, n := range []string{"config", "confi", "conf", "con", "co"} {
		refused = append(refused, mdCase{"config or its abbreviation " + n, flag(n), config})
	}
	runMDCases(t, refused, []mdCase{
		// Neighbours that are not prefixes of a refused name stay admitted.
		{"served-model-name", flag("served-model-name"), ""},
		{"model-impl, which extends model but is not a prefix of it", flag("model-impl"), ""},
		{"tokenizer-revision", flag("tokenizer-revision"), ""},
		{"compilation-config", flag("compilation-config"), ""},
		{"hf-overrides", flag("hf-overrides"), ""},
		{"api-server-count", flag("api-server-count"), ""},
		{"chat-template-content-format", flag("chat-template-content-format"), ""},
		{"max-model-len", flag("max-model-len"), ""},
		{"ordinary flags", setFlags(
			v1alpha1.EngineFlag{Name: "dtype", Value: "bfloat16"},
			v1alpha1.EngineFlag{Name: "seed", Value: "7"}), ""},
	})
	// A refused flag after ordinary ones, on create and on update.
	ordinary := []v1alpha1.EngineFlag{{Name: "dtype", Value: "bfloat16"}, {Name: "seed", Value: "7"}}
	runMDCases(t, []mdCase{
		{"an abbreviated key flag after ordinary flags", setFlags(append(append([]v1alpha1.EngineFlag{}, ordinary...), v1alpha1.EngineFlag{Name: "api-ke", Value: "SECRET"})...), keys},
		{"config after ordinary flags", setFlags(append(append([]v1alpha1.EngineFlag{}, ordinary...), v1alpha1.EngineFlag{Name: "config", Value: "/x.yaml"})...), config},
	}, nil)
	ns := newNamespace(t)
	md := newModelDeployment(ns, "abbrev-update")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Spec.Serving.Flags = append(got.Spec.Serving.Flags, v1alpha1.EngineFlag{Name: "revisio", Value: "main"})
	requireRefused(t, c.Update(bg(), got), passed)
}

// A boolean flag is given under its positive name with "true" or "false"
// (HBEP-3); the launcher passes "false" as --no-<name>. A second spelling would
// split one setting into two deployment IDs (review findings B5).
func TestBooleanEngineFlagsTakeTrueOrFalse(t *testing.T) {
	const (
		negative = "a boolean flag is given under its positive name"
		value    = "a boolean flag takes exactly 'true' or 'false'"
	)
	flag := func(name, v string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: v})
	}
	var refused, accepted []mdCase
	for _, b := range vllmBooleanFlags {
		accepted = append(accepted,
			mdCase{b + " is true", flag(b, "true"), ""},
			mdCase{b + " is false", flag(b, "false"), ""})
		for _, v := range []string{"1", "0", "True", "FALSE", "yes", "no", "on", " true", "true ", ""} {
			refused = append(refused, mdCase{b + " set to " + `"` + v + `"`, flag(b, v), value})
		}
		for _, v := range []string{"true", "false"} {
			refused = append(refused, mdCase{"no-" + b + " set to " + v, flag("no-"+b, v), negative})
		}
	}
	refused = append(refused,
		mdCase{"the negated spelling the review found", flag("no-enable-prefix-caching", "true"), negative},
		mdCase{"a no- prefix on a flag that is not a boolean", flag("no-dtype", "x"), negative},
		mdCase{"the value 1 on the boolean from the review", flag("enable-prefix-caching", "1"), value},
	)
	accepted = append(accepted,
		// A flag that is not a boolean keeps whatever value it is given.
		mdCase{"seed with the value 1", flag("seed", "1"), ""},
		mdCase{"max-model-len with the value true, which is not a boolean flag's rule", flag("max-model-len", "true"), ""},
		// disable-log-stats is a store_true flag, outside the boolean rule.
		mdCase{"disable-log-stats with 1", flag("disable-log-stats", "1"), ""},
		// A flag that contains "no" but does not start with "no-".
		mdCase{"node-rank is not a negated boolean", flag("node-rank", "1"), ""},
		mdCase{"nnodes is not a negated boolean", flag("nnodes", "2"), ""},
	)
	runMDCases(t, refused, accepted)
}

func TestEngineEnvironment(t *testing.T) {
	env := func(vars ...v1alpha1.EnvVar) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Env = vars }
	}
	one := func(name string) func(*v1alpha1.ModelDeployment) {
		return env(v1alpha1.EnvVar{Name: name, Value: "1"})
	}
	const (
		charset = "an environment variable name matches ^[A-Z_][A-Z0-9_]*$"
		key     = "a key is never written in a spec"
	)
	ordinary := []v1alpha1.EnvVar{{Name: "HF_HOME", Value: "/data/hf"}, {Name: "VLLM_LOGGING_LEVEL", Value: "INFO"}}
	after := func(name string) func(*v1alpha1.ModelDeployment) {
		return env(append(append([]v1alpha1.EnvVar{}, ordinary...), v1alpha1.EnvVar{Name: name, Value: "example-secret"})...)
	}
	runMDCases(t, []mdCase{
		{"HF_TOKEN", one("HF_TOKEN"), key},
		// vLLM reads VLLM_API_KEY as its API key (vllm/envs.py, v0.32.0, a variable
		// it marks as a credential); huggingface_hub still reads
		// HUGGING_FACE_HUB_TOKEN for backward compatibility; vLLM's S3 loading
		// reads S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY.
		{"VLLM_API_KEY", one("VLLM_API_KEY"), key},
		{"HUGGING_FACE_HUB_TOKEN", one("HUGGING_FACE_HUB_TOKEN"), key},
		{"S3_ACCESS_KEY_ID", one("S3_ACCESS_KEY_ID"), key},
		{"S3_SECRET_ACCESS_KEY", one("S3_SECRET_ACCESS_KEY"), key},
		{"VLLM_API_KEY after ordinary variables", after("VLLM_API_KEY"), key},
		{"HF_TOKEN after ordinary variables", after("HF_TOKEN"), key},
		{"HUGGING_FACE_HUB_TOKEN after ordinary variables", after("HUGGING_FACE_HUB_TOKEN"), key},
		{"S3_ACCESS_KEY_ID after ordinary variables", after("S3_ACCESS_KEY_ID"), key},
		{"S3_SECRET_ACCESS_KEY after ordinary variables", after("S3_SECRET_ACCESS_KEY"), key},
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
		// The refusal is by exact name: names that merely resemble a credential's pass.
		{"names that resemble a credential", env(
			v1alpha1.EnvVar{Name: "HF_TOKEN_PATH", Value: "/run/hf"}, v1alpha1.EnvVar{Name: "VLLM_API_KEY_FILE", Value: "/run/k"},
			v1alpha1.EnvVar{Name: "S3_ENDPOINT_URL", Value: "http://s3.invalid"}), ""},
	})
}

// A credential variable is refused on update too, whether it replaces the list
// or joins ordinary entries, and an ordinary update is accepted.
func TestEngineEnvironmentCredentialsAreRefusedOnUpdate(t *testing.T) {
	const key = "a key is never written in a spec"
	ns := newNamespace(t)
	md := newModelDeployment(ns, "env-update")
	md.Spec.Serving.Env = []v1alpha1.EnvVar{{Name: "HF_HOME", Value: "/data/hf"}}
	must(t, c.Create(bg(), md), "create")
	get := func() *v1alpha1.ModelDeployment {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		return got
	}
	for _, name := range []string{"HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "VLLM_API_KEY", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"} {
		t.Run("refuse adding "+name, func(t *testing.T) {
			got := get()
			got.Spec.Serving.Env = append(got.Spec.Serving.Env, v1alpha1.EnvVar{Name: name, Value: "example-secret"})
			requireRefused(t, c.Update(bg(), got), key)
		})
	}
	t.Run("accept adding an ordinary variable", func(t *testing.T) {
		got := get()
		got.Spec.Serving.Env = append(got.Spec.Serving.Env, v1alpha1.EnvVar{Name: "VLLM_LOGGING_LEVEL", Value: "INFO"})
		must(t, c.Update(bg(), got), "update")
	})
}

// The credentials reference on a checkpoint names exactly one source, as it does on a
// Provider. The rule is generated once per place the type is used, and each
// place needs its own test: neutralising this copy failed nothing before.
func TestCheckpointHubTokenReferenceNamesExactlyOneSource(t *testing.T) {
	const want = "name exactly one of file and secretRef"
	ref := func(r v1alpha1.CredentialsReference) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Serving.Checkpoint.HubTokenRef = &r }
	}
	secret := &v1alpha1.SecretKeyReference{Name: "hub", Namespace: "team", Key: "token"}
	runMDCases(t, []mdCase{
		{"neither a file nor a Secret", ref(v1alpha1.CredentialsReference{}), want},
		{"both a file and a Secret", ref(v1alpha1.CredentialsReference{File: "/run/hub", SecretRef: secret}), want},
	}, []mdCase{
		{"a file", ref(v1alpha1.CredentialsReference{File: "/run/hub"}), ""},
		{"a Secret", ref(v1alpha1.CredentialsReference{SecretRef: secret}), ""},
		{"no token reference", func(*v1alpha1.ModelDeployment) {}, ""},
	})
}

// An omitted chat template and an empty one are different: the checkpoint's own
// template applies to the first and the second names an empty template
// (HBEP-3). The pointer keeps the difference through the API server.
func TestChatTemplatePresenceIsKept(t *testing.T) {
	ns := newNamespace(t)
	for _, tc := range []struct {
		name string
		set  *string
	}{
		{"omitted", nil},
		{"empty", ptr.To("")},
		{"text", ptr.To("{{ messages }}")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := newModelDeployment(ns, uniq("tpl"))
			md.Spec.Serving.ChatTemplate = tc.set
			must(t, c.Create(bg(), md), "create")
			got := &v1alpha1.ModelDeployment{}
			must(t, c.Get(bg(), objKey(md), got), "get")
			switch {
			case tc.set == nil && got.Spec.Serving.ChatTemplate != nil:
				t.Errorf("an omitted template came back as %q", *got.Spec.Serving.ChatTemplate)
			case tc.set != nil && got.Spec.Serving.ChatTemplate == nil:
				t.Errorf("the template %q came back omitted", *tc.set)
			case tc.set != nil && *got.Spec.Serving.ChatTemplate != *tc.set:
				t.Errorf("the template %q came back as %q", *tc.set, *got.Spec.Serving.ChatTemplate)
			}
		})
	}
	// An edit between the three states is allowed and rolls out new IDs.
	md := newModelDeployment(ns, uniq("tpl"))
	must(t, c.Create(bg(), md), "create")
	for _, set := range []*string{ptr.To(""), nil, ptr.To("x")} {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		got.Spec.Serving.ChatTemplate = set
		must(t, c.Update(bg(), got), "edit the template")
	}
}

// The current and the active IDs in a ModelDeployment's status hold the full ID,
// the form an Instance carries, so a controller never compares two forms.
func TestDeploymentStatusHoldsFullIDs(t *testing.T) {
	const short = "0e1dd69efe8a"
	class := v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}
	ns := newNamespace(t)
	md := newModelDeployment(ns, "status-ids")
	must(t, c.Create(bg(), md), "create")
	get := func() *v1alpha1.ModelDeployment {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		return got
	}
	for what, set := range map[string]func(*v1alpha1.ModelDeployment, v1alpha1.FullDeploymentID){
		"currentIDs": func(m *v1alpha1.ModelDeployment, id v1alpha1.FullDeploymentID) {
			m.Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{AcceleratorClass: class, ID: id}}
		},
		"activeIDs": func(m *v1alpha1.ModelDeployment, id v1alpha1.FullDeploymentID) {
			m.Status.ActiveIDs = []v1alpha1.ActiveDeploymentID{{AcceleratorClass: class, ID: id, Instances: 1}}
		},
	} {
		t.Run("refuse the 12-character form in "+what, func(t *testing.T) {
			got := get()
			set(got, short)
			requireRefused(t, c.Status().Update(bg(), got), "id")
		})
		t.Run("refuse uppercase hex in "+what, func(t *testing.T) {
			got := get()
			set(got, v1alpha1.FullDeploymentID(strings.ToUpper(fullID)))
			requireRefused(t, c.Status().Update(bg(), got), "id")
		})
		t.Run("accept the full ID in "+what, func(t *testing.T) {
			got := get()
			set(got, fullID)
			must(t, c.Status().Update(bg(), got), "status update")
		})
	}
	got := get()
	if len(got.Status.CurrentIDs) != 1 || got.Status.CurrentIDs[0].ID != fullID || len(got.Status.ActiveIDs) != 1 || got.Status.ActiveIDs[0].ID != fullID {
		t.Fatalf("status IDs = %+v / %+v", got.Status.CurrentIDs, got.Status.ActiveIDs)
	}
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
		func(m *v1alpha1.ModelDeployment) { m.Spec.Serving.ChatTemplate = ptr.To("{{ messages }}") },
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

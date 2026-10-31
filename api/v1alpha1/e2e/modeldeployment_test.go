package e2e

import (
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Flags = flags }
}

// A branch name is refused; only a full commit id is admitted (HBEP-3, HBEP-9).
func TestCheckpointRevisionIsAFullCommitID(t *testing.T) {
	const msg = "revision must be a full commit id"
	rev := func(r string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Checkpoint.Revision = r }
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

// Every field of the API is camelCase (Kubernetes API conventions), so the
// engine's digest is imageDigest, not the hash object's image_digest; the
// deployment-ID library maps one onto the other (HBEP-3). The check reads the
// stored object, not the Go type.
func TestEngineDigestKeyIsCamelCase(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "digest-key")
	must(t, c.Create(bg(), md), "create")
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("ModelDeployment"))
	must(t, c.Get(bg(), objKey(md), u), "get unstructured")
	engine, _, _ := unstructured.NestedMap(u.Object, "spec", "source", "served", "engine")
	if engine["imageDigest"] != digest {
		t.Errorf("spec.source.served.engine.imageDigest = %v, want %s; keys %v", engine["imageDigest"], digest, engine)
	}
	if _, wrong := engine["image_digest"]; wrong {
		t.Errorf("spec.source.served.engine.image_digest is written: %v", engine)
	}
}

func TestEngineImageDigest(t *testing.T) {
	const msg = "imageDigest must be 'sha256:'"
	dig := func(d string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Engine.ImageDigest = d }
	}
	runMDCases(t, []mdCase{
		{"a tag instead of a digest", dig("latest"), msg},
		{"a short digest", dig("sha256:abcd"), msg},
		{"no prefix", dig(strings.Repeat("2", 64)), msg},
		{"uppercase hex", dig("sha256:" + strings.Repeat("A", 64)), msg},
		{"another algorithm", dig("sha512:" + strings.Repeat("2", 64)), msg},
	}, []mdCase{{"a sha256 digest", dig("sha256:" + strings.Repeat("a", 64)), ""}})
}

// The engine image is a repository without a tag: the agent pulls
// image@imageDigest, so a tag beside it would mislead a reader. A colon before
// the last "/" is a registry port, not a tag. This is the rule
// modeldeployments:$.spec.source.served.engine.image#0.
func TestEngineImageIsARepositoryWithoutATag(t *testing.T) {
	const msg = "image is a repository without a tag"
	img := func(i string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Engine.Image = i }
	}
	runMDCases(t, []mdCase{
		{"a latest tag", img("docker.io/vllm/vllm-openai:latest"), msg},
		{"a version tag on a bare name", img("vllm-openai:v1"), msg},
		{"a tag after a registry port", img("registry.example:5000/vllm/vllm-openai:v1"), msg},
	}, []mdCase{
		{"a repository", img("docker.io/vllm/vllm-openai"), ""},
		{"a registry port before the last slash", img("registry.example:5000/vllm/vllm-openai"), ""},
		{"a registry port and one path segment", img("registry.example:5000/vllm"), ""},
		{"a bare name", img("vllm-openai"), ""},
	})
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

// underscored returns name with every dash written as an underscore: the other
// spelling vLLM's parser reads as the same flag (HBEP-3, how the serving spec
// gives flags).
func underscored(name string) string { return strings.ReplaceAll(name, "-", "_") }

// hbep3FlagNames reads the flag names of HBEP-3's v1 table for vLLM from the
// HBEP itself, so that the admission rule's list of known names is checked
// against the specification and not against a copy made by the same hand. The
// first column of each row holds the names in code font; environment variables
// (uppercase) are not flags.
func hbep3FlagNames(t *testing.T) []string {
	t.Helper()
	text := string(readAll(t, filepath.Join("..", "..", "..", "hbep", "0003-resource-model-and-naming", "README.md")))
	_, table, ok := strings.Cut(text, "| Flag or variable | `v1` | Reason |")
	if !ok {
		t.Fatal("HBEP-3 has no v1 table for vLLM: the header row was not found")
	}
	var names []string
	seen := map[string]bool{}
	tick := regexp.MustCompile("`([a-z0-9][a-z0-9-]*)`")
	for _, line := range strings.Split(table, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			if len(names) > 0 {
				break // the table ends at its first non-row line
			}
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) < 3 {
			continue
		}
		for _, m := range tick.FindAllStringSubmatch(cells[1], -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				names = append(names, m[1])
			}
		}
	}
	if len(names) < 40 {
		t.Fatalf("read only %d flag names from HBEP-3's table; the table's format changed: %v", len(names), names)
	}
	return names
}

// Short flag aliases and secret flags are refused in specs (HBEP-3, HBEP-11),
// and flag and environment names are ASCII. A flag name as written is lowercase
// letters, digits, dashes and underscores, and every refusal compares it after
// "_" is replaced with "-" (HBEP-3 amendment, PR #47).
func TestEngineFlagsUseLongNamesAndCarryNoKeys(t *testing.T) {
	flag := func(name string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: "1"})
	}
	const (
		dashes  = "an engine flag name is a long name without leading dashes"
		charset = "an engine flag name matches ^[a-z0-9][a-z0-9_-]*$"
		dotted  = "a dotted sub-key"
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
		{"uppercase", flag("Max-Model-Len"), charset},
		{"a value glued to the name", flag("max-model-len=4096"), charset},
		{"a space in the name", flag("max model len"), charset},
		{"a non-ASCII letter", flag("max-modèl-len"), charset},
		{"a name that starts with an underscore", flag("_max_model_len"), charset},
		{"an empty name", flag(""), "name"},
		{"a dotted sub-key", flag("hf-overrides.rope"), dotted},
		{"a dotted sub-key with underscores", flag("hf_overrides.rope"), dotted},
		{"a dot alone", flag("a.b"), dotted},
		{"the api-key flag", flag("api-key"), keys},
		{"the api_key spelling", flag("api_key"), keys},
		{"the hf-token flag", flag("hf-token"), keys},
		{"the hf_token spelling", flag("hf_token"), keys},
		{"the model flag", flag("model"), passed},
		{"the revision flag", flag("revision"), passed},
		{"the chat-template flag", flag("chat-template"), passed},
		{"the chat_template spelling", flag("chat_template"), passed},
	}...)
	runMDCases(t, refused, []mdCase{
		{"long names", setFlags(
			v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "4"},
			v1alpha1.EngineFlag{Name: "pipeline-parallel-size", Value: "2"},
			v1alpha1.EngineFlag{Name: "data-parallel-size", Value: "1"},
			v1alpha1.EngineFlag{Name: "enable-auto-tool-choice", Value: "true"}), ""},
		// An underscore is one spelling of the dash, normalised by the engine's
		// parser and by the deployment-ID library, so it is admitted.
		{"max_model_len, the underscore spelling of a known flag", flag("max_model_len"), ""},
		{"max_num_seqs", flag("max_num_seqs"), ""},
		{"tensor_parallel_size", setFlags(v1alpha1.EngineFlag{Name: "tensor_parallel_size", Value: "4"}), ""},
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
// FlexibleArgumentParser in vllm/utils/argparse_utils.py, v0.32.0). Every proper
// prefix of a known long name is refused, in either spelling (HBEP-3), and the
// names with their own refusal (keys, the flags the framework passes, config)
// say so.
func TestEngineFlagsRefuseAbbreviationsOfRefusedNames(t *testing.T) {
	flag := func(name string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: "x"})
	}
	const (
		keys   = "a key is never written in a spec"
		passed = "the framework passes model, revision and chat-template itself"
		config = "config is refused"
		lora   = "lora-modules is refused in alpha"
	)
	var refused []mdCase
	for _, n := range []string{"api-ke", "api-k", "api-", "api", "ap", "hf-tok", "hf-t", "hf-", "hf", "h",
		"api_ke", "api_k", "api_", "hf_tok", "hf_t", "hf_"} {
		refused = append(refused, mdCase{"the abbreviation " + n + " of a key flag", flag(n), keys})
	}
	for _, n := range []string{"mode", "mod", "mo", "revisio", "revis", "rev", "chat-templat", "chat-temp", "chat-", "chat", "ch",
		"chat_templat", "chat_temp", "chat_"} {
		refused = append(refused, mdCase{"the abbreviation " + n + " of a flag the framework passes", flag(n), passed})
	}
	for _, n := range []string{"config", "confi", "conf", "con", "co"} {
		refused = append(refused, mdCase{"config or its abbreviation " + n, flag(n), config})
	}
	for _, n := range []string{"lora-modules", "lora_modules", "lora-module", "lora-m", "lora_", "lora"} {
		refused = append(refused, mdCase{"lora-modules or its abbreviation " + n, flag(n), lora})
	}
	runMDCases(t, refused, []mdCase{
		// Neighbours that are not prefixes of a known name stay admitted.
		{"served-model-name", flag("served-model-name"), ""},
		{"model-impl, which extends model but is not a prefix of it", flag("model-impl"), ""},
		{"tokenizer-revision", setFlags(v1alpha1.EngineFlag{Name: "tokenizer-revision", Value: commit40}), ""},
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
	after := func(f v1alpha1.EngineFlag) func(*v1alpha1.ModelDeployment) {
		return setFlags(append(append([]v1alpha1.EngineFlag{}, ordinary...), f)...)
	}
	runMDCases(t, []mdCase{
		{"an abbreviated key flag after ordinary flags", after(v1alpha1.EngineFlag{Name: "api-ke", Value: "SECRET"}), keys},
		{"an underscored abbreviated key flag after ordinary flags", after(v1alpha1.EngineFlag{Name: "api_ke", Value: "SECRET"}), keys},
		{"config after ordinary flags", after(v1alpha1.EngineFlag{Name: "config", Value: "/x.yaml"}), config},
		{"lora-modules after ordinary flags", after(v1alpha1.EngineFlag{Name: "lora-modules", Value: "a=org/adapter"}), lora},
	}, nil)
	ns := newNamespace(t)
	md := newModelDeployment(ns, "abbrev-update")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Spec.Source.Served.Flags = append(got.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "revisio", Value: "main"})
	requireRefused(t, c.Update(bg(), got), passed)
}

// Every proper prefix of every flag in HBEP-3's v1 table is refused, in the
// dashed and the underscored spelling, unless it is itself a flag of the table
// (argparse takes an exact match first, so tokenizer stays admitted although it
// is a prefix of tokenizer-mode). The names are read from the HBEP, not from the
// rule, so a flag missing from the rule's list fails here.
func TestEngineFlagsRefuseEveryPrefixOfAKnownFlag(t *testing.T) {
	const msg = "a proper prefix of a known flag name is refused"
	known := hbep3FlagNames(t)
	isKnown := map[string]bool{}
	for _, k := range known {
		isKnown[k] = true
	}
	ns := newNamespace(t)
	try := func(name, value string) error {
		md := newModelDeployment(ns, uniq("md"))
		md.Spec.Source.Served.Flags = []v1alpha1.EngineFlag{{Name: name, Value: value}}
		return c.Create(bg(), md)
	}
	for _, k := range known {
		t.Run(k, func(t *testing.T) {
			var accepted []string
			for i := 1; i < len(k); i++ {
				p := k[:i]
				if isKnown[p] {
					continue
				}
				for _, spelling := range []string{p, underscored(p)} {
					if err := try(spelling, "x"); err == nil {
						accepted = append(accepted, spelling)
					} else if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), msg) {
						t.Errorf("prefix %q of %q: want the refusal %q, got %v", spelling, k, msg, err)
					}
				}
			}
			if len(accepted) > 0 {
				t.Errorf("prefixes of %q were accepted: %v", k, accepted)
			}
		})
	}
	// The other side: every known name is admitted with a value that suits it,
	// except the ones with a refusal of their own.
	own := map[string]bool{"api-key": true, "hf-token": true, "model": true, "revision": true, "chat-template": true, "config": true, "lora-modules": true}
	isBool := map[string]bool{}
	for _, b := range vllmBooleanFlags {
		isBool[b] = true
	}
	for _, k := range known {
		if own[k] {
			continue
		}
		value := "x"
		switch {
		case isBool[k]:
			value = "true"
		case k == "tokenizer":
			value = "example-org/example-1b" // the checkpoint's own
		case k == "tokenizer-revision":
			value = commit40
		}
		for _, spelling := range []string{k, underscored(k)} {
			if err := try(spelling, value); err != nil {
				t.Errorf("the known flag %q was refused: %v", spelling, err)
			}
		}
	}
}

// A boolean flag is given under its positive name with "true" or "false"
// (HBEP-3); the launcher passes "false" as --no-<name>. A second spelling would
// split one setting into two deployment IDs (review finding B5).
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
			mdCase{b + " is false", flag(b, "false"), ""},
			mdCase{underscored(b) + " is false", flag(underscored(b), "false"), ""})
		for _, v := range []string{"1", "0", "True", "FALSE", "yes", "no", "on", " true", "true ", ""} {
			refused = append(refused, mdCase{b + " set to " + `"` + v + `"`, flag(b, v), value})
		}
		refused = append(refused, mdCase{underscored(b) + ` set to "1"`, flag(underscored(b), "1"), value})
		for _, v := range []string{"true", "false"} {
			refused = append(refused, mdCase{"no-" + b + " set to " + v, flag("no-"+b, v), negative})
		}
		refused = append(refused, mdCase{"no_" + underscored(b) + " set to true", flag("no_"+underscored(b), "true"), negative})
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

// lora-modules and a speculative-config that names a draft model refer to other
// checkpoints without a pinned revision, the risk behind L-8.3: both are refused
// in alpha, with a pinned form left for beta (HBEP-3, references to other
// checkpoints inside input values).
func TestEngineFlagsThatNameOtherCheckpointsAreRefused(t *testing.T) {
	const draft = "a speculative-config that names a draft model is refused in alpha"
	spec := func(name, value string) func(*v1alpha1.ModelDeployment) {
		return setFlags(v1alpha1.EngineFlag{Name: name, Value: value})
	}
	runMDCases(t, []mdCase{
		{"a draft model", spec("speculative-config", `{"model":"org/draft-1b","num_speculative_tokens":5}`), draft},
		{"a draft model, spaced as JSON allows", spec("speculative-config", "{\"num_speculative_tokens\": 5,\n  \"model\" : \"org/draft-1b\"}"), draft},
		{"an EAGLE head, which is another checkpoint too", spec("speculative-config", `{"method":"eagle","model":"org/eagle-head"}`), draft},
		{"the underscore spelling of the name", spec("speculative_config", `{"model":"org/draft-1b"}`), draft},
	}, []mdCase{
		{"n-gram speculation, which names no model", spec("speculative-config", `{"method":"ngram","num_speculative_tokens":3,"prompt_lookup_max":4}`), ""},
		{"multi-token prediction, which names no model", spec("speculative-config", `{"method":"mtp","num_speculative_tokens":1}`), ""},
		{"an empty speculative-config", spec("speculative-config", `{}`), ""},
		{"a model key that is not the draft's, as a longer key name", spec("speculative-config", `{"method":"ngram","max_model_len":4096}`), ""},
		{"a flag that is not speculative-config may carry a model key", spec("hf-overrides", `{"model":"x"}`), ""},
	})
}

// A tokenizer other than the checkpoint's is another unpinned checkpoint (L-8.3):
// it needs a tokenizer-revision that is a full commit id (HBEP-3). A
// tokenizer-revision is a full commit id wherever it appears.
func TestTokenizerOtherThanTheCheckpointsIsPinned(t *testing.T) {
	const (
		needsPin = "a tokenizer other than the checkpoint's needs a tokenizer-revision flag"
		hexOnly  = "tokenizer-revision must be a full commit id"
	)
	const own = "example-org/example-1b" // newModelDeployment's checkpoint repository
	f := func(pairs ...string) func(*v1alpha1.ModelDeployment) {
		var flags []v1alpha1.EngineFlag
		for i := 0; i < len(pairs); i += 2 {
			flags = append(flags, v1alpha1.EngineFlag{Name: pairs[i], Value: pairs[i+1]})
		}
		return setFlags(flags...)
	}
	const other = "other-org/other-tokenizer"
	runMDCases(t, []mdCase{
		{"another tokenizer with no revision", f("tokenizer", other), needsPin},
		{"another tokenizer with a branch as its revision", f("tokenizer", other, "tokenizer-revision", "main"), needsPin},
		{"another tokenizer with an abbreviated revision", f("tokenizer", other, "tokenizer-revision", commit40[:12]), needsPin},
		{"another tokenizer, underscored, with no revision", f("tokenizer", other, "tokenizer_revision", "v1.0"), needsPin},
		{"another tokenizer with a revision of 41 hex characters", f("tokenizer", other, "tokenizer-revision", commit40+"a"), needsPin},
		{"a branch as the revision of the checkpoint's own tokenizer", f("tokenizer-revision", "main"), hexOnly},
		{"a branch given after a good revision, which would win", f("tokenizer", other, "tokenizer-revision", commit40, "tokenizer-revision", "main"), hexOnly},
		{"an uppercase revision", f("tokenizer", other, "tokenizer-revision", strings.ToUpper(commit40)), needsPin},
	}, []mdCase{
		{"another tokenizer, pinned", f("tokenizer", other, "tokenizer-revision", commit40), ""},
		{"another tokenizer, pinned, in the other order", f("tokenizer-revision", commit40, "tokenizer", other), ""},
		{"another tokenizer, pinned, with underscores", f("tokenizer", other, "tokenizer_revision", commit40), ""},
		{"the checkpoint's own tokenizer, named", f("tokenizer", own), ""},
		{"no tokenizer flag", f("tokenizer-mode", "auto"), ""},
		{"a pinned revision of the checkpoint's own tokenizer", f("tokenizer-revision", commit40), ""},
	})
}

func TestEngineEnvironment(t *testing.T) {
	env := func(vars ...v1alpha1.EnvVar) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Env = vars }
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
	md.Spec.Source.Served.Env = []v1alpha1.EnvVar{{Name: "HF_HOME", Value: "/data/hf"}}
	must(t, c.Create(bg(), md), "create")
	get := func() *v1alpha1.ModelDeployment {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		return got
	}
	for _, name := range []string{"HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "VLLM_API_KEY", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"} {
		t.Run("refuse adding "+name, func(t *testing.T) {
			got := get()
			got.Spec.Source.Served.Env = append(got.Spec.Source.Served.Env, v1alpha1.EnvVar{Name: name, Value: "example-secret"})
			requireRefused(t, c.Update(bg(), got), key)
		})
	}
	t.Run("accept adding an ordinary variable", func(t *testing.T) {
		got := get()
		got.Spec.Source.Served.Env = append(got.Spec.Source.Served.Env, v1alpha1.EnvVar{Name: "VLLM_LOGGING_LEVEL", Value: "INFO"})
		must(t, c.Update(bg(), got), "update")
	})
}

// The credentials reference on a checkpoint names exactly one source, as it does on a
// Provider. The rule is generated once per place the type is used, and each
// place needs its own test: neutralising this copy failed nothing before.
func TestCheckpointHubTokenReferenceNamesExactlyOneSource(t *testing.T) {
	const want = "name exactly one of file and secretRef"
	ref := func(r v1alpha1.CredentialsReference) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Checkpoint.HubTokenRef = &r }
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
			md.Spec.Source.Served.ChatTemplate = tc.set
			must(t, c.Create(bg(), md), "create")
			got := &v1alpha1.ModelDeployment{}
			must(t, c.Get(bg(), objKey(md), got), "get")
			switch {
			case tc.set == nil && got.Spec.Source.Served.ChatTemplate != nil:
				t.Errorf("an omitted template came back as %q", *got.Spec.Source.Served.ChatTemplate)
			case tc.set != nil && got.Spec.Source.Served.ChatTemplate == nil:
				t.Errorf("the template %q came back omitted", *tc.set)
			case tc.set != nil && *got.Spec.Source.Served.ChatTemplate != *tc.set:
				t.Errorf("the template %q came back as %q", *tc.set, *got.Spec.Source.Served.ChatTemplate)
			}
		})
	}
	// An edit between the three states is allowed and rolls out new IDs.
	md := newModelDeployment(ns, uniq("tpl"))
	must(t, c.Create(bg(), md), "create")
	for _, set := range []*string{ptr.To(""), nil, ptr.To("x")} {
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		got.Spec.Source.Served.ChatTemplate = set
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
			m.Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{AcceleratorClass: &class, ID: id}}
		},
		"activeIDs": func(m *v1alpha1.ModelDeployment, id v1alpha1.FullDeploymentID) {
			m.Status.ActiveIDs = []v1alpha1.ActiveDeploymentID{{AcceleratorClass: &class, ID: id, Instances: 1}}
		},
	} {
		t.Run("refuse the 12-character form in "+what, func(t *testing.T) {
			got := get()
			set(got, short)
			requireRefused(t, c.Status().Update(bg(), got), "should match '^[0-9a-f]{64}$'")
		})
		t.Run("refuse uppercase hex in "+what, func(t *testing.T) {
			got := get()
			set(got, v1alpha1.FullDeploymentID(strings.ToUpper(fullID)))
			requireRefused(t, c.Status().Update(bg(), got), "should match '^[0-9a-f]{64}$'")
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

// A managed catalog entry has no accelerator class, so its status entries name
// none: the class is optional, and none is invented for the entry.
func TestManagedDeploymentStatusNamesNoAcceleratorClass(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "managed-status")
	md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: "example-managed", CatalogName: "example-model-27b"}}
	must(t, c.Create(bg(), md), "create")
	md.Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{ID: fullID}}
	md.Status.ActiveIDs = []v1alpha1.ActiveDeploymentID{{ID: fullID, Instances: 1}}
	must(t, c.Status().Update(bg(), md), "write a status without an accelerator class")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if len(got.Status.CurrentIDs) != 1 || got.Status.CurrentIDs[0].AcceleratorClass != nil || got.Status.ActiveIDs[0].AcceleratorClass != nil {
		t.Fatalf("status IDs = %+v / %+v", got.Status.CurrentIDs, got.Status.ActiveIDs)
	}
	// A class that is given still has both of its fields.
	got.Status.CurrentIDs[0].AcceleratorClass = &v1alpha1.AcceleratorClass{GPUModel: "GH200"}
	requireRefused(t, c.Status().Update(bg(), got), "cpuArch")
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

// The provider names are an ordered list of unique names: the order is the
// order the Providers are tried in (HBEP-7, Where), so the list is atomic, not
// a set, and it is read back as written. This is the rule
// modeldeployments:$.spec.providers.names#0.
func TestModelDeploymentProviderNamesAreOrderedAndUnique(t *testing.T) {
	const msg = "providers.names lists each Provider once"
	names := func(n ...string) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = n }
	}
	runMDCases(t, []mdCase{
		{"a name listed twice in a row", names("a-prov", "a-prov"), msg},
		{"a name listed twice with another between", names("a-prov", "b-prov", "a-prov"), msg},
		{"a repeated name among three", names("a-prov", "b-prov", "b-prov"), msg},
	}, []mdCase{
		{"one name", names("a-prov"), ""},
		{"two different names", names("a-prov", "b-prov"), ""},
	})

	// The order is kept: b-prov is listed first and stays first.
	ns := newNamespace(t)
	for _, want := range [][]string{{"b-prov", "a-prov"}, {"c-prov", "a-prov", "b-prov"}} {
		md := newModelDeployment(ns, uniq("md"))
		md.Spec.Providers.Names = want
		must(t, c.Create(bg(), md), "create")
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		if !reflect.DeepEqual(got.Spec.Providers.Names, want) {
			t.Errorf("providers.names = %v, want %v in that order", got.Spec.Providers.Names, want)
		}
	}
}

func TestRetentionPolicy(t *testing.T) {
	const msg = "idleLimit is required with mode ReleaseWhenIdle and refused with mode Keep"
	const until = "until is refused with mode ReleaseWhenIdle"
	pol := func(p v1alpha1.RetentionPolicy) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.RetentionPolicy = &p }
	}
	date := &metav1.Time{Time: metav1.Now().Add(86400e9)}
	runMDCases(t, []mdCase{
		{"release when idle without an idle limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle}), msg},
		{"keep with an idle limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, IdleLimit: ptr.To(v1alpha1.Duration("10m"))}), msg},
		{"an idle limit with no mode, which is keep", pol(v1alpha1.RetentionPolicy{IdleLimit: ptr.To(v1alpha1.Duration("10m"))}), msg},
		{"release when idle with a date to keep until", pol(v1alpha1.RetentionPolicy{
			Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("10m")), Until: date}), until},
		{"an unknown mode", pol(v1alpha1.RetentionPolicy{Mode: "Forever"}), "Unsupported value"},
		{"an idle limit that is not a duration", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("soon"))}), "idleLimit"},
	}, []mdCase{
		{"keep until a date", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: date}), ""},
		{"a date with no mode, which is keep", pol(v1alpha1.RetentionPolicy{Until: date}), ""},
		{"keep with no date (the deployment's end time)", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}), ""},
		{"an empty policy", pol(v1alpha1.RetentionPolicy{}), ""},
		{"release when idle after a limit", pol(v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("30m"))}), ""},
	})
}

// A field the spec omits takes its schema default: one instance. The retention
// mode has none: the stored object keeps an unset mode, and the controller reads
// it as Keep (HBEP-7's decided default), so a later change of that default
// leaves no stale stored values.
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
	if got.Spec.RetentionPolicy == nil || got.Spec.RetentionPolicy.Mode != "" {
		t.Errorf("an empty retention policy = %+v; the schema must not write a mode", got.Spec.RetentionPolicy)
	}
	if got.Spec.RetentionPolicy.EffectiveMode() != v1alpha1.RetentionKeep {
		t.Errorf("the effective mode of an empty policy is %q, want Keep", got.Spec.RetentionPolicy.EffectiveMode())
	}
}

// A ModelDeployment serves a checkpoint on our engine or a catalog entry of a
// managed platform, never both and never neither (review finding B9). A catalog
// entry carries no checkpoint, engine or flags, and none is invented for it.
func TestModelDeploymentSourceIsServedOrManaged(t *testing.T) {
	const msg = "source names exactly one of served and managed"
	managed := &v1alpha1.ManagedCatalogEntry{Platform: "example-managed", CatalogName: "example-model-27b"}
	src := func(s v1alpha1.ModelSource) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.Source = s }
	}
	served := newModelDeployment("x", "x").Spec.Source.Served
	runMDCases(t, []mdCase{
		{"neither served nor managed", src(v1alpha1.ModelSource{}), msg},
		{"both served and managed", src(v1alpha1.ModelSource{Served: served, Managed: managed}), msg},
		{"a managed entry without a platform", src(v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{CatalogName: "m"}}), "platform"},
		{"a managed entry without a catalog name", src(v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: "p"}}), "catalogName"},
		{"a managed entry with an empty revision", src(v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{
			Platform: "p", CatalogName: "m", Revision: ptr.To("")}}), "revision"},
		{"a served source without a checkpoint revision", func(md *v1alpha1.ModelDeployment) { md.Spec.Source.Served.Checkpoint.Revision = "" }, "revision"},
	}, []mdCase{
		{"a served source", src(v1alpha1.ModelSource{Served: served}), ""},
		{"a managed entry with no checkpoint, engine or flags", src(v1alpha1.ModelSource{Managed: managed}), ""},
		{"a managed entry with a revision", src(v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{
			Platform: "example-managed", CatalogName: "example-model-27b", Revision: ptr.To("2026-10-01")}}), ""},
	})
	// A catalog entry's stored form has no served block, and a served one no managed block.
	ns := newNamespace(t)
	md := newModelDeployment(ns, "managed-stored")
	md.Spec.Source = v1alpha1.ModelSource{Managed: managed}
	must(t, c.Create(bg(), md), "create")
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("ModelDeployment"))
	must(t, c.Get(bg(), objKey(md), u), "get unstructured")
	source, _, _ := unstructured.NestedMap(u.Object, "spec", "source")
	if _, has := source["served"]; has || source["managed"] == nil {
		t.Errorf("spec.source = %v", source)
	}
	// Switching between the two is an edit of what is served: allowed, and it rolls out new IDs.
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Spec.Source = v1alpha1.ModelSource{Served: served}
	must(t, c.Update(bg(), got), "switch from a catalog entry to a served checkpoint")
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
			m.Spec.Source.Served.Checkpoint.Revision = "fedcba9876543210fedcba9876543210fedcba98"
		},
		func(m *v1alpha1.ModelDeployment) {
			m.Spec.Source.Served.Flags = append(m.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "seed", Value: "7"})
		},
		func(m *v1alpha1.ModelDeployment) { m.Spec.Source.Served.ChatTemplate = ptr.To("{{ messages }}") },
		func(m *v1alpha1.ModelDeployment) { m.Spec.Source.Served.Engine.Version = "0.33.0" },
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
	got.Spec.Source.Served.Checkpoint.Revision = "main"
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

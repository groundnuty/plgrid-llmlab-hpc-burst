package e2e

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The manifests in examples/ are what docs/quickstart.md tells a user to apply
// (issue #162). A user copies them and replaces the placeholders, so they have
// to stay valid as the contracts change. Every one goes through server-side
// admission on the real CRDs and their CEL rules, under strict field
// validation, so a renamed field is refused instead of pruned. Nothing here
// compares an example with the value it was written from: the checks are
// admission, and agreement with the shipped profiles (profiles/), with the
// other examples, and with the list of examples the issue asks for.

func examplesDir() string { return filepath.Join("..", "..", "..", "examples") }

// dryRunStrict sends obj to the API server's admission as a create, with strict
// field validation, and stores nothing.
func dryRunStrict(obj map[string]any) error {
	u := &unstructured.Unstructured{Object: runtime.DeepCopyJSON(obj)}
	return c.Create(bg(), u, client.FieldValidation("Strict"), client.DryRunAll)
}

// shippedProfile is what an example needs to know about a profile of profiles/.
type shippedProfile struct {
	file       string
	kind       string          // spec.providerKind
	partitions map[string]bool // spec.partitions[].name
	qos        map[string]bool // spec.qos[].name
}

// loadProfileManifests reads the PlatformProfile objects of profiles/, by the
// name inside the object. The draft files (*.draft.yaml) are inputs to the
// schema, not manifests, and are not of kind PlatformProfile.
func loadProfileManifests(t *testing.T) map[string]shippedProfile {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(profilesDir(), "*.yaml"))
	must(t, err, "list profiles/")
	out := map[string]shippedProfile{}
	for _, f := range files {
		for _, obj := range readManifestFile(t, f) {
			if obj["kind"] != "PlatformProfile" || obj["apiVersion"] != v1alpha1.GroupVersion.String() {
				continue
			}
			name, _, _ := unstructured.NestedString(obj, "metadata", "name")
			kind, _, _ := unstructured.NestedString(obj, "spec", "providerKind")
			p := shippedProfile{file: filepath.Base(f), kind: kind, partitions: map[string]bool{}, qos: map[string]bool{}}
			parts, _, _ := unstructured.NestedSlice(obj, "spec", "partitions")
			for _, raw := range parts {
				pn, _ := raw.(map[string]any)["name"].(string)
				p.partitions[pn] = true
			}
			qoses, _, _ := unstructured.NestedSlice(obj, "spec", "qos")
			for _, raw := range qoses {
				qn, _ := raw.(map[string]any)["name"].(string)
				p.qos[qn] = true
			}
			if prev, dup := out[name]; dup {
				t.Fatalf("profiles/%s and profiles/%s are both PlatformProfile %q", prev.file, p.file, name)
			}
			out[name] = p
		}
	}
	if len(out) == 0 {
		t.Fatalf("profiles/ holds no PlatformProfile: the profile checks would pass on nothing")
	}
	return out
}

// Every example, and every shipped profile, passes admission. A contract change
// that an example does not follow fails here.
func TestExamplesAreAdmitted(t *testing.T) {
	for _, e := range loadExamples(t) {
		t.Run(e.label(), func(t *testing.T) {
			must(t, dryRunStrict(e.obj), "admit "+e.label())
		})
	}
	// The managed profile is new with the examples; helios and athena have their own
	// tests (shipped_profiles_test.go), and are admitted here too, by dry run.
	for name, p := range loadProfileManifests(t) {
		t.Run("profile "+name, func(t *testing.T) {
			for _, obj := range readManifestFile(t, filepath.Join(profilesDir(), p.file)) {
				if obj["kind"] == "PlatformProfile" {
					// Under another name, so a profile that an earlier test created, or that
					// the host already holds, does not turn the dry run into AlreadyExists.
					must(t, dryRunStrict(edit(t, obj, uniq("ex"), "metadata", "name")), "admit profiles/"+p.file)
				}
			}
		})
	}
}

// providers indexes the example Providers by name.
func providers(exs []example) map[string]example {
	out := map[string]example{}
	for _, e := range exs {
		if e.kind() == "Provider" {
			out[e.name()] = e
		}
	}
	return out
}

// The examples agree with the profiles they name and with each other: a
// Provider names a shipped profile and, if the profile is a batch one, a
// partition that profile lists; a ModelDeployment names Providers that exist
// among the examples, a served one batch Providers, and a managed one a
// managed Provider on its platform (internal/lifecycle, catalogReady). The
// expected values are the shipped profiles' and the contracts', not the
// examples' own.
func TestExamplesAgreeWithTheirProfilesAndEachOther(t *testing.T) {
	exs := loadExamples(t)
	profs := loadProfileManifests(t)
	provs := providers(exs)
	kindOf := func(provider example) string {
		ref, _, _ := unstructured.NestedString(provider.obj, "spec", "platformProfileRef", "name")
		return profs[ref].kind
	}
	for _, e := range exs {
		switch e.kind() {
		case "Provider":
			t.Run(e.label(), func(t *testing.T) {
				ref, _, _ := unstructured.NestedString(e.obj, "spec", "platformProfileRef", "name")
				p, ok := profs[ref]
				if !ok {
					t.Fatalf("spec.platformProfileRef.name %q is not a PlatformProfile in profiles/ (%s)", ref, sortedKeys(profs))
				}
				parts, _, _ := unstructured.NestedStringSlice(e.obj, "spec", "allowedPartitions")
				switch p.kind {
				case string(v1alpha1.PlatformBatch):
					if len(parts) != 1 {
						t.Errorf("a batch Provider allows exactly one partition in alpha (HBEP-4, open question 12); allowedPartitions is %v", parts)
					}
					for _, part := range parts {
						if !p.partitions[part] {
							t.Errorf("partition %q is not in profiles/%s (%s)", part, p.file, sortedKeys(p.partitions))
						}
					}
				case string(v1alpha1.PlatformManaged):
					if len(parts) != 0 {
						t.Errorf("a managed Provider has no partitions; allowedPartitions is %v", parts)
					}
				default:
					t.Errorf("profiles/%s has providerKind %q", p.file, p.kind)
				}
				qos, _, _ := unstructured.NestedString(e.obj, "spec", "qos")
				if msg := qosProblem(qos, p); msg != "" {
					t.Error(msg)
				}
				if lbl, _, _ := unstructured.NestedString(e.obj, "metadata", "labels", v1alpha1.LabelPlatform); lbl == "" {
					t.Errorf("the Provider has no %s label: HBEP-3 makes the platform a label, and a managed ModelDeployment matches it", v1alpha1.LabelPlatform)
				}
			})
		case "ModelDeployment":
			t.Run(e.label(), func(t *testing.T) {
				names, _, _ := unstructured.NestedStringSlice(e.obj, "spec", "providers", "names")
				if len(names) == 0 {
					t.Fatalf("spec.providers.names is empty: the examples name their Providers, and this check reads names only")
				}
				if ns, _, _ := unstructured.NestedString(e.obj, "metadata", "namespace"); ns == "" {
					t.Errorf("metadata.namespace is unset: ModelDeployments are namespaced, and the quickstart applies the file as it is")
				}
				_, served, _ := unstructured.NestedMap(e.obj, "spec", "source", "served")
				platform, managed, _ := unstructured.NestedString(e.obj, "spec", "source", "managed", "platform")
				for _, n := range names {
					prov, ok := provs[n]
					if !ok {
						t.Errorf("spec.providers.names has %q, which no example Provider is called (%s)", n, sortedKeys(provs))
						continue
					}
					switch {
					case served && kindOf(prov) != string(v1alpha1.PlatformBatch):
						t.Errorf("a served deployment names Provider %q, whose profile is not a batch one", n)
					case managed:
						lbl, _, _ := unstructured.NestedString(prov.obj, "metadata", "labels", v1alpha1.LabelPlatform)
						if kindOf(prov) != string(v1alpha1.PlatformManaged) || lbl != platform {
							t.Errorf("a managed deployment on platform %q names Provider %q: its profile kind is %q and its platform label %q", platform, n, kindOf(prov), lbl)
						}
					}
				}
			})
		default:
			t.Errorf("%s: examples/ holds Providers and ModelDeployments; kind %q is neither", e.label(), e.kind())
		}
	}
}

// qosProblem says why a Provider's spec.qos cannot be used with its profile, or "" when it can or
// is unset. The Slurm provider fails reading the profile, with the field named, when the profile's
// qos list does not hold the Provider's (HBEP-4, Provider settings and QOS limits), so an example
// that names an unlisted QOS would be admitted and then never ready. A profile with no qos list at
// all lists none.
func qosProblem(qos string, p shippedProfile) string {
	if qos == "" || p.qos[qos] {
		return ""
	}
	return fmt.Sprintf("spec.qos %q is not listed in profiles/%s qos (%s): the Slurm provider fails reading the profile", qos, p.file, sortedKeys(p.qos))
}

// The Helios example asks for QOS now, as the quickstart says, and the profile it names lists it.
// The control cases show qosProblem refuses an unlisted QOS, one on a profile with no list, and
// accepts an unset one, so the agreement check above is not one that accepts everything.
func TestExampleProviderQOSIsListedInItsProfile(t *testing.T) {
	profs := loadProfileManifests(t)
	var helios example
	for _, e := range loadExamples(t) {
		if e.file == "provider-helios-gh200.yaml" {
			helios = e
		}
	}
	if helios.obj == nil {
		t.Fatalf("this check reads provider-helios-gh200.yaml")
	}
	if qos, _, _ := unstructured.NestedString(helios.obj, "spec", "qos"); qos != "now" {
		t.Errorf("the Helios example's spec.qos is %q; the quickstart says it asks for now", qos)
	}
	helProf := profs["helios"]
	if !helProf.qos["now"] {
		t.Fatalf("profiles/%s does not list the QOS now (%s)", helProf.file, sortedKeys(helProf.qos))
	}
	for _, tc := range []struct {
		name string
		qos  string
		prof shippedProfile
		bad  bool
	}{
		{"a listed QOS", "now", helProf, false},
		{"no QOS", "", helProf, false},
		{"an unlisted QOS", "nowx", helProf, true},
		{"a QOS on a profile with no list", "now", profs["llmlab"], true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := qosProblem(tc.qos, tc.prof); (got != "") != tc.bad {
				t.Errorf("qosProblem(%q, profiles/%s) = %q; want a problem: %v", tc.qos, tc.prof.file, got, tc.bad)
			}
		})
	}
}

func sortedKeys[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

// The set of examples is the one issue #162 lists. The list is the issue's; the
// examples only have to satisfy it.
func TestExamplesCoverTheIssue(t *testing.T) {
	exs := loadExamples(t)
	profs := loadProfileManifests(t)
	has := func(what string, match func(example) bool) {
		t.Helper()
		for _, e := range exs {
			if match(e) {
				return
			}
		}
		t.Errorf("examples/ has no %s", what)
	}
	providerOn := func(profile, partition string) func(example) bool {
		return func(e example) bool {
			ref, _, _ := unstructured.NestedString(e.obj, "spec", "platformProfileRef", "name")
			parts, _, _ := unstructured.NestedStringSlice(e.obj, "spec", "allowedPartitions")
			return e.kind() == "Provider" && ref == profile && (partition == "" || (len(parts) == 1 && parts[0] == partition))
		}
	}
	served := func(e example) bool {
		_, ok, _ := unstructured.NestedMap(e.obj, "spec", "source", "served")
		return e.kind() == "ModelDeployment" && ok
	}
	hasFlag := func(e example, name string) bool {
		flags, _, _ := unstructured.NestedSlice(e.obj, "spec", "source", "served", "flags")
		for _, f := range flags {
			if n, _ := f.(map[string]any)["name"].(string); strings.ReplaceAll(n, "_", "-") == name {
				return true
			}
		}
		return false
	}
	has("batch Provider for Helios plgrid-gpu-gh200", providerOn("helios", "plgrid-gpu-gh200"))
	has("batch Provider for Athena plgrid-gpu-a100", providerOn("athena", "plgrid-gpu-a100"))
	has("managed Provider on LLMLab", func(e example) bool {
		ref, _, _ := unstructured.NestedString(e.obj, "spec", "platformProfileRef", "name")
		return e.kind() == "Provider" && profs[ref].kind == string(v1alpha1.PlatformManaged)
	})
	has("served ModelDeployment", served)
	has("served ModelDeployment with a reasoning parser", func(e example) bool { return served(e) && hasFlag(e, "reasoning-parser") })
	has("managed ModelDeployment", func(e example) bool {
		_, ok, _ := unstructured.NestedMap(e.obj, "spec", "source", "managed")
		return e.kind() == "ModelDeployment" && ok
	})
}

var placeholder = regexp.MustCompile(`<[^<>]*>`)

// Each private value is a placeholder (issue #162). The placeholders are the
// four the issue names; a value in angle brackets that is not one of them is a
// typo that a user would apply as it stands, and a private field without
// brackets is a real value.
func TestExamplesHoldOnlyPlaceholders(t *testing.T) {
	allowed := map[string]bool{"<account>": true, "<grant>": true, "<login-node>": true, "<user>": true}
	var walk func(path string, v any, visit func(path, s string))
	walk = func(path string, v any, visit func(path, s string)) {
		switch x := v.(type) {
		case map[string]any:
			for k, vv := range x {
				walk(path+"."+k, vv, visit)
			}
		case []any:
			for i, vv := range x {
				walk(fmt.Sprintf("%s[%d]", path, i), vv, visit)
			}
		case string:
			visit(path, x)
		}
	}
	for _, e := range loadExamples(t) {
		t.Run(e.label(), func(t *testing.T) {
			walk("", e.obj, func(path, s string) {
				for _, tok := range placeholder.FindAllString(s, -1) {
					if !allowed[tok] {
						t.Errorf("%s holds %q, which is not one of the placeholders <account>, <grant>, <login-node>, <user>", path, tok)
					}
				}
			})
			if e.kind() != "Provider" {
				return
			}
			for _, field := range []string{"account", "grant"} {
				if v, found, _ := unstructured.NestedString(e.obj, "spec", field); found && v != "<"+field+">" {
					t.Errorf("spec.%s is %q; an example holds the placeholder <%s>", field, v, field)
				}
			}
			if f, _, _ := unstructured.NestedString(e.obj, "spec", "credentialsRef", "file"); !strings.HasPrefix(f, "/home/<user>/") {
				t.Errorf("spec.credentialsRef.file is %q; an example's path starts with /home/<user>/", f)
			}
		})
	}
}

// An example that admission would refuse is refused by this test's own path. These
// are the controls for TestExamplesAreAdmitted: the same call that admits each
// example refuses a broken copy of one, a misspelt key (pruned without strict
// validation) and a CEL rule among them, so a pass above is not a path that
// admits everything.
func TestExamplesRefuseABrokenCopy(t *testing.T) {
	byFile := map[string]example{}
	for _, e := range loadExamples(t) {
		byFile[e.file] = e
	}
	prov, md := byFile["provider-helios-gh200.yaml"], byFile["modeldeployment-qwen25-instruct.yaml"]
	if prov.obj == nil || md.obj == nil {
		t.Fatalf("this control reads provider-helios-gh200.yaml and modeldeployment-qwen25-instruct.yaml")
	}
	cases := []struct {
		name string
		base example
		path []any
		val  any
		want string
	}{
		{"a misspelt Provider key", prov, []any{"spec", "acount"}, "<account>", "acount"},
		{"a credentials reference with a file and a Secret", prov, []any{"spec", "credentialsRef", "secretRef"},
			map[string]any{"name": "n", "namespace": "ns", "key": "k"}, "name exactly one of file and secretRef"},
		{"a branch name as the checkpoint revision", md, []any{"spec", "source", "served", "checkpoint", "revision"}, "main", "revision must be a full commit id"},
		{"an engine flag with leading dashes", md, []any{"spec", "source", "served", "flags", 0, "name"}, "--max-model-len", "an engine flag name is a long name without leading dashes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			must(t, dryRunStrict(tc.base.obj), "the unbroken example")
			err := dryRunStrict(edit(t, tc.base.obj, tc.val, tc.path...))
			if err == nil {
				t.Fatalf("the broken copy was admitted")
			}
			if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) {
				t.Fatalf("want a 400 or 422 refusal, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal does not mention %q: %v", tc.want, err)
			}
		})
	}
}

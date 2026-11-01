package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The alpha PlatformProfiles that profiles/ ships (HBEP-4, open questions 3, 7
// and 8). The checks here are admission against the real CRD, and the shape of
// the untyped block's envelopes. They do not compare a typed value with the
// fact it was copied from: that would prove only the copy. The Slurm provider's
// own checks of the same files are in internal/slurm.

// profilesDir is the repository's profiles directory, relative to this package.
func profilesDir() string { return filepath.Join("..", "..", "..", "profiles") }

// shippedProfiles are the manifests of profiles/, and the keys, under
// spec.profile.facts, of the facts spike S1 added to each (issue #18). A managed
// profile (llmlab.yaml, HBEP-4 "A managed profile") has no batch fields and no
// untyped block, so it has no facts to key.
var shippedProfiles = []struct {
	file, name string
	s1Facts    [][]string
	managed    bool
}{
	{"helios.yaml", "helios", [][]string{
		{"slurm", "hold_release"}, {"containers", "apptainer_compute_nodes"}, {"containers", "user_namespaces_compute_nodes"},
		{"storage", "home_readable_from_compute_nodes"}, {"reachability", "compute_outbound_internet"},
		{"compute_node", "host_python3"}, {"compute_node", "architecture"},
	}, false},
	{"athena.yaml", "athena", [][]string{
		{"slurm", "hold_release"}, {"containers", "apptainer_compute_nodes"}, {"containers", "user_namespaces_compute_nodes"},
		{"storage", "home_readable_from_compute_nodes"}, {"reachability", "compute_node_outbound_internet"},
		{"compute_node", "host_python3"}, {"compute_node", "architecture"},
	}, false},
	{"llmlab.yaml", "llmlab", nil, true},
}

// loadShipped reads a shipped manifest as the object an operator would apply.
// A missing or unreadable file fails the test: it never skips.
func loadShipped(t *testing.T, file string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(profilesDir(), file))
	must(t, err, "read the shipped profile "+file)
	obj := map[string]any{}
	must(t, yaml.Unmarshal(raw, &obj), "parse "+file)
	if obj["kind"] != "PlatformProfile" || obj["apiVersion"] != v1alpha1.GroupVersion.String() {
		t.Fatalf("%s is a %v of %v; want a PlatformProfile of %s", file, obj["kind"], obj["apiVersion"], v1alpha1.GroupVersion)
	}
	return obj
}

// strictCreate creates obj with strict field validation: under the default, the
// API server prunes a misspelled typed key and admits the object.
func strictCreate(t *testing.T, obj map[string]any) (*unstructured.Unstructured, error) {
	t.Helper()
	u := &unstructured.Unstructured{Object: runtime.DeepCopyJSON(obj)}
	err := c.Create(bg(), u, client.FieldValidation("Strict"))
	if err == nil {
		cleanup(t, u)
	}
	return u, err
}

// edit returns a deep copy of obj in which the value at path is replaced. A path
// element is a map key or a list index. A nil value removes a map key.
func edit(t *testing.T, obj map[string]any, value any, path ...any) map[string]any {
	t.Helper()
	out := runtime.DeepCopyJSON(obj)
	var cur any = out
	for i, p := range path {
		last := i == len(path)-1
		switch k := p.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				t.Fatalf("path %v: element %d (%q) is not under a map", path, i, k)
			}
			if last {
				if value == nil {
					delete(m, k)
				} else {
					m[k] = value
				}
				return out
			}
			cur = m[k]
		case int:
			l, ok := cur.([]any)
			if !ok || k >= len(l) {
				t.Fatalf("path %v: element %d (%d) is not in a list", path, i, k)
			}
			if last {
				l[k] = value
				return out
			}
			cur = l[k]
		default:
			t.Fatalf("path %v: element %d has type %T", path, i, p)
		}
	}
	t.Fatalf("path %v is empty", path)
	return nil
}

// Every shipped manifest is admitted by the real CRD, under strict validation, and the
// untyped block comes back from the API server as it was sent.
func TestShippedProfilesAreAdmitted(t *testing.T) {
	for _, sp := range shippedProfiles {
		t.Run(sp.name, func(t *testing.T) {
			obj := loadShipped(t, sp.file)
			if got := (&unstructured.Unstructured{Object: obj}).GetName(); got != sp.name {
				t.Fatalf("metadata.name is %q; want %q", got, sp.name)
			}
			stored, err := strictCreate(t, obj)
			must(t, err, "create the shipped "+sp.file)

			// The block that nothing validates must survive the round trip: nothing
			// pruned, no null dropped, no date turned into a timestamp.
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("PlatformProfile"))
			must(t, c.Get(bg(), client.ObjectKeyFromObject(stored), got), "get")
			want, _, _ := unstructured.NestedFieldNoCopy(obj, "spec", "profile")
			have, _, _ := unstructured.NestedFieldNoCopy(got.Object, "spec", "profile")
			if a, b := canonicalJSON(t, want), canonicalJSON(t, have); a != b {
				t.Fatalf("spec.profile changed on the way through the API server:\nsent  %.300s\ngot   %.300s", a, b)
			}
		})
	}
}

func canonicalJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	must(t, err, "marshal")
	return string(b)
}

// One broken value is refused. Each case starts from a shipped manifest that the
// same test first shows is admitted, so the refusal comes from the one change.
func TestShippedProfilesRefuseABrokenValue(t *testing.T) {
	cases := []struct {
		name  string
		file  string
		value any
		path  []any
		want  []string
	}{
		{"imageFormat that Apptainer does not run", "athena.yaml", "oci", []any{"spec", "partitions", 0, "imageFormat"}, []string{"imageFormat", "Unsupported value"}},
		{"unknown provider kind", "helios.yaml", "cloud", []any{"spec", "providerKind"}, []string{"providerKind", "Unsupported value"}},
		{"providerKind removed", "helios.yaml", nil, []any{"spec", "providerKind"}, []string{"providerKind", "Required value"}},
		{"GPU model removed", "helios.yaml", nil, []any{"spec", "partitions", 1, "gpuModel"}, []string{"gpuModel", "Required value"}},
		{"maxTime that is not a duration", "athena.yaml", "2 days", []any{"spec", "partitions", 0, "maxTime"}, []string{"maxTime"}},
		{"drainWindow of zero", "helios.yaml", "0s", []any{"spec", "drainWindow"}, []string{"drainWindow must be a positive duration"}},
		{"maxIdle of zero", "athena.yaml", "0s", []any{"spec", "maxIdle"}, []string{"maxIdle must be a positive duration"}},
		{"failureThreshold of zero", "athena.yaml", int64(0), []any{"spec", "health", "failureThreshold"}, []string{"failureThreshold"}},
		{"timeZone that is not a zone name", "helios.yaml", "Warsaw time", []any{"spec", "timeZone"}, []string{"timeZone"}},
		{"stagingDir with a .. segment", "athena.yaml", "$SCRATCH/..", []any{"spec", "stagingDir"}, []string{"stagingDir must not contain a '..' segment"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := loadShipped(t, tc.file)
			// Control: the shipped manifest under another name is admitted.
			_, err := strictCreate(t, edit(t, obj, uniq("ctl"), "metadata", "name"))
			must(t, err, "create an unbroken copy of "+tc.file)
			_, err = strictCreate(t, edit(t, edit(t, obj, tc.value, tc.path...), uniq("bad"), "metadata", "name"))
			requireRefused(t, err, tc.want...)
		})
	}
}

// A misspelled typed key is refused, not pruned, under strict validation, so a
// typo in a shipped manifest cannot pass admission by being dropped.
func TestShippedProfilesRefuseAMisspelledTypedKey(t *testing.T) {
	obj := loadShipped(t, "helios.yaml")
	spec, _ := obj["spec"].(map[string]any)
	if _, ok := spec["holdRelease"]; !ok {
		t.Fatalf("helios.yaml has no spec.holdRelease; this test renames it")
	}
	// Control: the shipped manifest under another name is admitted.
	_, err := strictCreate(t, edit(t, obj, uniq("ctl"), "metadata", "name"))
	must(t, err, "create an unbroken copy of helios.yaml")
	bad := edit(t, edit(t, obj, nil, "spec", "holdRelease"), true, "spec", "holdReleese")
	_, err = strictCreate(t, edit(t, bad, uniq("bad"), "metadata", "name"))
	if err == nil {
		t.Fatalf("a manifest with the key holdReleese was admitted")
	}
	if !apierrors.IsBadRequest(err) && !apierrors.IsInvalid(err) {
		t.Fatalf("want a 400 or 422 refusal, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "holdReleese") {
		t.Fatalf("the refusal does not name the misspelled key: %v", err)
	}
	t.Logf("refused as %T: %v", err, err)
}

var (
	dateText       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	partitionEntry = regexp.MustCompile(`^spec\.profile\.facts\.partitions\[\d+\]$`)
	methods        = map[string]bool{"measured": true, "documented": true, "unknown": true}
)

// Every envelope in the untyped block has a known method, a source and a date
// written as text (HBEP-4, open question 7). An envelope is found by its
// structure, not by its provenance fields, so one that lost a field is seen: a
// fact is a map with a value, and a partition entry is an item of
// facts.partitions, whose provenance is grouped over the entry.
func TestShippedProfilesKeepTheEnvelope(t *testing.T) {
	for _, sp := range shippedProfiles {
		t.Run(sp.name, func(t *testing.T) {
			obj := loadShipped(t, sp.file)
			if sp.managed {
				// A managed profile is the other branch of the same contract: it names its kind and
				// holds neither partitions nor the untyped block, whose envelope the batch profiles
				// keep. A batch field here would be a platform LLMLab does not have.
				if kind, _, _ := unstructured.NestedString(obj, "spec", "providerKind"); kind != string(v1alpha1.PlatformManaged) {
					t.Errorf("%s: providerKind is %q; want %q", sp.file, kind, v1alpha1.PlatformManaged)
				}
				for _, key := range []string{"profile", "partitions", "qos"} {
					if _, has, _ := unstructured.NestedFieldNoCopy(obj, "spec", key); has {
						t.Errorf("%s: a managed profile holds spec.%s", sp.file, key)
					}
				}
				return
			}
			profile, found, _ := unstructured.NestedMap(obj, "spec", "profile")
			if !found {
				t.Fatalf("%s has no spec.profile", sp.file)
			}
			seen := 0
			var walk func(path string, v any)
			walk = func(path string, v any) {
				switch x := v.(type) {
				case map[string]any:
					if _, isFact := x["value"]; isFact || partitionEntry.MatchString(path) {
						seen++
						if s, _ := x["method"].(string); !methods[s] {
							t.Errorf("%s: method %v is not measured, documented or unknown", path, x["method"])
						}
						if x["source"] == nil || x["source"] == "" {
							t.Errorf("%s: no source", path)
						}
						if d, _ := x["date"].(string); !dateText.MatchString(d) {
							t.Errorf("%s: date %v is not a YYYY-MM-DD string", path, x["date"])
						}
					}
					for k, e := range x {
						walk(path+"."+k, e)
					}
				case []any:
					for i, e := range x {
						walk(fmt.Sprintf("%s[%d]", path, i), e)
					}
				}
			}
			walk("spec.profile", profile)
			if seen < 50 {
				t.Errorf("only %d envelopes found in spec.profile; the drafts' facts are missing", seen)
			}

			// The facts of spike S1 are measured and name issue #18 as their source.
			for _, key := range sp.s1Facts {
				fact, found, _ := unstructured.NestedMap(profile, append([]string{"facts"}, key...)...)
				path := "spec.profile.facts." + strings.Join(key, ".")
				if !found {
					t.Errorf("%s is missing: spike S1's fact was not added", path)
					continue
				}
				if fact["method"] != "measured" {
					t.Errorf("%s: method %v; want measured", path, fact["method"])
				}
				if s, _ := fact["source"].(string); !strings.Contains(s, "issue #18") {
					t.Errorf("%s: source does not name issue #18", path)
				}
				if fact["date"] != "2026-10-30" {
					t.Errorf("%s: date %v; want the measurement date 2026-10-30", path, fact["date"])
				}
			}
		})
	}
}

// The hard-coded list above names every PlatformProfile that profiles/ ships, so a profile added
// to the directory cannot stay out of the admission and envelope checks (PR #170 review: the list
// did not cover llmlab.yaml). The directory is read with the loader the example checks use.
func TestShippedProfilesListCoversTheDirectory(t *testing.T) {
	listed := map[string]string{}
	for _, sp := range shippedProfiles {
		listed[sp.name] = sp.file
	}
	onDisk := loadProfileManifests(t)
	for name, p := range onDisk {
		if listed[name] != p.file {
			t.Errorf("profiles/%s (PlatformProfile %q) is not in shippedProfiles", p.file, name)
		}
	}
	for name, file := range listed {
		if p, ok := onDisk[name]; !ok || p.file != file {
			t.Errorf("shippedProfiles lists %s as PlatformProfile %q, which profiles/ does not hold", file, name)
		}
	}
}

package launcher

// The CRD deletion guard's launcher side (guard.go; issue #242) without an API server: what the
// embedded manifest holds, the order of the removal, and what counts as "present". Whether the API
// server then refuses a delete is judged elsewhere, by a real one: api/v1alpha1/e2e on an API server
// that evaluates admission policies, and cmd/hpcburst on a real k3s.

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The embedded manifest is the one HBEP-11 describes: a policy that fails closed, matching deletes of
// CustomResourceDefinitions and refusing those of the framework's group, and a binding that denies.
func TestGuardObjectsAreWhatHBEP11Describes(t *testing.T) {
	policy, binding, err := guardObjects()
	if err != nil {
		t.Fatal(err)
	}
	if policy.GetName() != GuardName || binding.GetName() != GuardName {
		t.Errorf("names %q and %q, want %q for both", policy.GetName(), binding.GetName(), GuardName)
	}
	if fp, _, _ := unstructured.NestedString(policy.Object, "spec", "failurePolicy"); fp != "Fail" {
		t.Errorf("failurePolicy %q, want Fail: a guard that lets deletes through when it cannot decide guards nothing", fp)
	}
	rules, _, _ := unstructured.NestedSlice(policy.Object, "spec", "matchConstraints", "resourceRules")
	if len(rules) != 1 {
		t.Fatalf("%d resource rules, want 1", len(rules))
	}
	rule := rules[0].(map[string]any)
	for key, want := range map[string][]any{
		"apiGroups": {"apiextensions.k8s.io"}, "operations": {"DELETE"}, "resources": {"customresourcedefinitions"},
	} {
		if got, _ := rule[key].([]any); !slices.Equal(got, want) {
			t.Errorf("rule %s = %v, want %v", key, got, want)
		}
	}
	if actions, _, _ := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions"); !slices.Equal(actions, []string{"Deny"}) {
		t.Errorf("the binding's validationActions %v, want [Deny]", actions)
	}
	if name, _, _ := unstructured.NestedString(binding.Object, "spec", "policyName"); name != GuardName {
		t.Errorf("the binding names the policy %q, want %q", name, GuardName)
	}
}

// The launcher embeds the file of the repository: what it decodes is what decoding the file on disk
// gives, so there is one source and no copy to drift.
func TestGuardIsTheFileOfTheRepository(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "config", "admission", "crd-deletion-guard.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	onDisk, err := decodeManifests(f, "crd-deletion-guard.yaml")
	if err != nil {
		t.Fatal(err)
	}
	policy, binding, err := guardObjects()
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk) != 2 || !reflect.DeepEqual(onDisk[0].Object, policy.Object) || !reflect.DeepEqual(onDisk[1].Object, binding.Object) {
		t.Errorf("the embedded guard differs from config/admission/crd-deletion-guard.yaml on disk")
	}
}

func guardFake(objs ...runtime.Object) (*fake.FakeDynamicClient, *[]string) {
	scheme := runtime.NewScheme()
	kinds := map[schema.GroupVersionResource]string{guardPolicyGVR: "ValidatingAdmissionPolicyList", guardBindingGVR: "ValidatingAdmissionPolicyBindingList"}
	f := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, kinds, objs...)
	var deleted []string
	f.PrependReactor("delete", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		deleted = append(deleted, a.GetResource().Resource)
		return false, nil, nil
	})
	return f, &deleted
}

func guardObject(kind, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "admissionregistration.k8s.io/v1", "kind": kind, "metadata": map[string]any{"name": name}, "spec": spec,
	}}
}

func TestRemoveGuardDeletesTheBindingFirst(t *testing.T) {
	policy, binding, _ := guardObjects()
	f, deleted := guardFake(policy, binding)
	removed, err := removeGuard(context.Background(), f, io.Discard)
	if err != nil || !removed {
		t.Fatalf("removeGuard: %v, %v", removed, err)
	}
	if want := []string{"validatingadmissionpolicybindings", "validatingadmissionpolicies"}; !slices.Equal(*deleted, want) {
		t.Errorf("deleted %v, want %v: the binding goes first, so the policy is never without it half-removed", *deleted, want)
	}
	// Repeating it succeeds and removes nothing.
	removed, err = removeGuard(context.Background(), f, io.Discard)
	if err != nil || removed {
		t.Errorf("a second removeGuard: removed %v, %v, want a success that removed nothing", removed, err)
	}
	// Only the policy left over (the binding went by hand): it still goes.
	f, _ = guardFake(policy)
	if removed, err := removeGuard(context.Background(), f, io.Discard); err != nil || !removed {
		t.Errorf("removeGuard with only the policy: %v, %v", removed, err)
	}
}

func TestRemoveGuardReportsAnErrorThatIsNotNotFound(t *testing.T) {
	policy, binding, _ := guardObjects()
	f, _ := guardFake(policy, binding)
	boom := errors.New("example: the API server is down")
	f.PrependReactor("delete", "validatingadmissionpolicybindings", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, boom
	})
	if removed, err := removeGuard(context.Background(), f, io.Discard); !errors.Is(err, boom) || removed {
		t.Errorf("removeGuard = %v, %v, want the API error and nothing removed", removed, err)
	}
}

// What counts as present is what stops a delete: the policy and a binding that names it and denies.
func TestCheckGuardIsPresentOnlyWhatStopsADelete(t *testing.T) {
	policy, binding, _ := guardObjects()
	named := func(policyName string, actions ...any) *unstructured.Unstructured {
		return guardObject("ValidatingAdmissionPolicyBinding", GuardName, map[string]any{"policyName": policyName, "validationActions": actions})
	}
	for _, tc := range []struct {
		name    string
		objs    []runtime.Object
		present bool
		detail  string
	}{
		{"both", []runtime.Object{policy, binding}, true, ""},
		{"neither", nil, false, "ValidatingAdmissionPolicy " + GuardName + " is not there"},
		{"the policy alone, as the by-hand removal leaves it", []runtime.Object{policy}, false, "ValidatingAdmissionPolicyBinding " + GuardName + " is not there"},
		{"the binding alone", []runtime.Object{binding}, false, "ValidatingAdmissionPolicy " + GuardName + " is not there"},
		{"a binding that only warns", []runtime.Object{policy, named(GuardName, "Warn")}, false, "do not include Deny"},
		{"a binding of another policy", []runtime.Object{policy, named("other", "Deny")}, false, `names the policy "other"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := guardFake(tc.objs...)
			st, err := checkGuard(context.Background(), f)
			if err != nil {
				t.Fatal(err)
			}
			if st.Present != tc.present || !strings.Contains(st.Detail, tc.detail) {
				t.Errorf("checkGuard = %+v, want present %v with detail containing %q", st, tc.present, tc.detail)
			}
		})
	}
}

// An error from the API server is a failure to ask, never an absent guard.
func TestCheckGuardDoesNotCallAnErrorAbsent(t *testing.T) {
	f, _ := guardFake()
	boom := errors.New("example: the API server is down")
	f.PrependReactor("get", "*", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, boom })
	if st, err := checkGuard(context.Background(), f); !errors.Is(err, boom) || st != nil {
		t.Errorf("checkGuard = %+v, %v, want the error and no state", st, err)
	}
	if apierrors.IsNotFound(boom) {
		t.Fatal("the test's error must not look like NotFound")
	}
}

// RemoveCRDGuard needs a recorded host.
func TestRemoveCRDGuardRefusesWithoutAHost(t *testing.T) {
	_, err := RemoveCRDGuard(context.Background(), GuardOptions{StateDir: filepath.Join(shortDir(t), "st")})
	if !errors.Is(err, errNotRunning) {
		t.Errorf("RemoveCRDGuard with no host: %v, want %v", err, errNotRunning)
	}
}

// A binding that someone weakened to Warn (or pointed at another policy) is set back to the
// manifest's before the apply: server-side apply alone would add Deny to the Warn, and the API
// server refuses the pair.
func TestNormalizeBindingResetsWhatWasChangedByHand(t *testing.T) {
	_, want, _ := guardObjects()
	weak := guardObject("ValidatingAdmissionPolicyBinding", GuardName, map[string]any{"policyName": "other", "validationActions": []any{"Warn"}})
	f, _ := guardFake(weak)
	if err := normalizeBinding(context.Background(), f, want); err != nil {
		t.Fatal(err)
	}
	st, err := checkGuard(context.Background(), f) // the policy is absent here, so only the binding's parts count
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(st.Detail, "Deny") || strings.Contains(st.Detail, "names the policy") {
		t.Errorf("after normalizing, the binding is still wrong: %s", st.Detail)
	}
	// A binding that is already right is not written, and one that does not exist is left to the apply.
	f, _ = guardFake(want)
	var writes int
	f.PrependReactor("update", "*", func(k8stesting.Action) (bool, runtime.Object, error) { writes++; return false, nil, nil })
	if err := normalizeBinding(context.Background(), f, want); err != nil || writes != 0 {
		t.Errorf("normalizing a right binding: %v, %d writes, want none", err, writes)
	}
	f, _ = guardFake()
	if err := normalizeBinding(context.Background(), f, want); err != nil {
		t.Errorf("normalizing with no binding: %v", err)
	}
}

package launcher

// The CRD deletion guard's launcher side (guard.go; issue #242) without an API server: what the
// embedded manifest holds, the order of the removal, and what counts as "present". Whether the API
// server then refuses a delete is judged elsewhere, by a real one: api/v1alpha1/e2e on an API server
// that evaluates admission policies, and cmd/hpcburst on a real k3s.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
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
	kinds := map[schema.GroupVersionResource]string{guardPolicyGVR: "ValidatingAdmissionPolicyList", guardBindingGVR: "ValidatingAdmissionPolicyBindingList", crdGVR: "CustomResourceDefinitionList"}
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
	filtered := guardObject("ValidatingAdmissionPolicyBinding", GuardName, map[string]any{
		"policyName": GuardName, "validationActions": []any{"Deny"},
		"matchResources": map[string]any{"objectSelector": map[string]any{"matchLabels": map[string]any{"example": "never"}}},
	})
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
		{"a binding that only warns", []runtime.Object{policy, named(GuardName, "Warn")}, false, "are not just Deny"},
		{"a binding with an objectSelector the CRDs do not satisfy", []runtime.Object{policy, filtered}, false, "spec.matchResources"},
		{"a binding with a parameter reference", []runtime.Object{policy, guardObject("ValidatingAdmissionPolicyBinding", GuardName, map[string]any{"policyName": GuardName, "validationActions": []any{"Deny"}, "paramRef": map[string]any{"name": "p"}})}, false, "spec.paramRef"},
		{"a policy with a match condition that is never true", []runtime.Object{guardObject("ValidatingAdmissionPolicy", GuardName, map[string]any{"matchConditions": []any{map[string]any{"name": "never", "expression": "false"}}, "failurePolicy": "Fail", "validations": []any{}}), binding}, false, "spec.matchConditions"},
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

// FrameworkGroup is the group the manifest's rule names, which WaitGuardEnforced probes by.
func TestFrameworkGroupIsTheGroupTheGuardProtects(t *testing.T) {
	policy, _, err := guardObjects()
	if err != nil {
		t.Fatal(err)
	}
	vals, _, _ := unstructured.NestedSlice(policy.Object, "spec", "validations")
	if len(vals) != 1 || !strings.Contains(vals[0].(map[string]any)["expression"].(string), "'"+FrameworkGroup+"'") {
		t.Errorf("the policy's validations %v do not name the group %s", vals, FrameworkGroup)
	}
}

// What was added to the guard by hand does not survive an install: the spec is replaced whole (review of
// PR 250: B1). Server-side apply alone keeps a field of another manager that the manifest does not set.
func TestResetSpecsReplacesWhatWasAddedByHand(t *testing.T) {
	policy, binding, _ := guardObjects()
	filtered := guardObject("ValidatingAdmissionPolicyBinding", GuardName, map[string]any{
		"policyName": "other", "validationActions": []any{"Warn"},
		"matchResources": map[string]any{
			"objectSelector":    map[string]any{"matchLabels": map[string]any{"example": "never"}},
			"namespaceSelector": map[string]any{"matchLabels": map[string]any{"example": "never"}},
		},
		"paramRef": map[string]any{"name": "p", "parameterNotFoundAction": "Allow"},
	})
	switched := guardObject("ValidatingAdmissionPolicy", GuardName, map[string]any{
		"failurePolicy": "Ignore", "matchConditions": []any{map[string]any{"name": "never", "expression": "false"}},
		"validations": []any{map[string]any{"expression": "true"}},
	})
	f, _ := guardFake(switched, filtered)
	if err := resetSpecs(context.Background(), f, policy, binding); err != nil {
		t.Fatal(err)
	}
	gotB, _ := f.Resource(guardBindingGVR).Get(context.Background(), GuardName, metav1.GetOptions{})
	gotP, _ := f.Resource(guardPolicyGVR).Get(context.Background(), GuardName, metav1.GetOptions{})
	if !reflect.DeepEqual(gotB.Object["spec"], binding.Object["spec"]) {
		t.Errorf("the binding's spec after the reset is %v, want the manifest's %v", gotB.Object["spec"], binding.Object["spec"])
	}
	if !reflect.DeepEqual(gotP.Object["spec"], policy.Object["spec"]) {
		t.Errorf("the policy's spec after the reset is %v, want the manifest's %v", gotP.Object["spec"], policy.Object["spec"])
	}
	// One that is already right is not written, and one that does not exist is left to the apply.
	f, _ = guardFake(policy, binding)
	var writes int
	f.PrependReactor("update", "*", func(k8stesting.Action) (bool, runtime.Object, error) { writes++; return false, nil, nil })
	if err := resetSpecs(context.Background(), f, policy, binding); err != nil || writes != 0 {
		t.Errorf("resetting a right guard: %v, %d writes, want none", err, writes)
	}
	f, _ = guardFake()
	if err := resetSpecs(context.Background(), f, policy, binding); err != nil {
		t.Errorf("resetting with no guard: %v", err)
	}
}

func crdObject(name, group string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": name}, "spec": map[string]any{"group": group},
	}}
}

// up returns only once a delete of a framework CRD is refused, by a dry-run delete that is the guard's own
// refusal (review of PR 250: B2): not before, not on another CRD's refusal, and not forever.
func TestWaitGuardEnforced(t *testing.T) {
	const crd = "providers." + FrameworkGroup
	refusal := apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, crd,
		errors.New("refusing to delete the custom resource definition "+crd+": it would delete every resource of that kind"))

	t.Run("waits through the attempts the API server accepts, and probes with a dry run", func(t *testing.T) {
		f, _ := guardFake(crdObject("widgets.other.example.invalid", "other.example.invalid"), crdObject(crd, FrameworkGroup))
		var attempts int
		var dry []bool
		f.PrependReactor("delete", "customresourcedefinitions", func(a k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			dry = append(dry, len(a.(k8stesting.DeleteActionImpl).DeleteOptions.DryRun) > 0)
			if attempts < 4 {
				return true, nil, nil // accepted: the guard is not enforced yet
			}
			return true, nil, refusal
		})
		if err := waitGuardEnforced(context.Background(), f, 30*time.Second, time.Millisecond, io.Discard); err != nil {
			t.Fatal(err)
		}
		if attempts != 4 {
			t.Errorf("%d attempts, want 4: the wait must go on until the refusal", attempts)
		}
		for i, d := range dry {
			if !d {
				t.Errorf("attempt %d was not a dry run: it would have deleted the CRD", i+1)
			}
		}
	})

	t.Run("a Forbidden that is not the guard's does not count", func(t *testing.T) {
		f, _ := guardFake(crdObject(crd, FrameworkGroup))
		f.PrependReactor("delete", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "customresourcedefinitions"}, crd, errors.New("example: RBAC says no"))
		})
		err := waitGuardEnforced(context.Background(), f, 50*time.Millisecond, time.Millisecond, io.Discard)
		if err == nil || !strings.Contains(err.Error(), crd) {
			t.Errorf("waitGuardEnforced = %v, want a timeout naming %s", err, crd)
		}
	})

	t.Run("never refused: an error that names the CRD, not a hang", func(t *testing.T) {
		f, _ := guardFake(crdObject(crd, FrameworkGroup))
		start := time.Now()
		err := waitGuardEnforced(context.Background(), f, 100*time.Millisecond, time.Millisecond, io.Discard)
		if err == nil || !strings.Contains(err.Error(), crd) || !strings.Contains(err.Error(), "still did not refuse") {
			t.Errorf("waitGuardEnforced = %v, want an error that names %s and says the API server did not refuse", err, crd)
		}
		if time.Since(start) > 10*time.Second {
			t.Errorf("the wait took %s against a 100 ms bound", time.Since(start))
		}
	})

	t.Run("no framework CRD: nothing to probe, said so, no error", func(t *testing.T) {
		f, _ := guardFake(crdObject("widgets.other.example.invalid", "other.example.invalid"))
		var out strings.Builder
		if err := waitGuardEnforced(context.Background(), f, time.Second, time.Millisecond, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "nothing to probe") {
			t.Errorf("the log %q does not say there was nothing to probe", out.String())
		}
	})
}

// statusGuard asks only a host that is healthy, reports an absent guard without making the host unhealthy,
// and makes a failure to ask a reason (review of PR 250: B3).
func TestStatusGuard(t *testing.T) {
	var asked int
	var mu sync.Mutex
	status := http.StatusNotFound
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked++
		code := status
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		reason := map[int]string{404: "NotFound", 500: "InternalError"}[code]
		fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"example","reason":%q,"code":%d}`, reason, code)
	}))
	t.Cleanup(srv.Close)
	cfg := &rest.Config{Host: srv.URL}
	count := func() int { mu.Lock(); defer mu.Unlock(); return asked }
	run := func(health string, readyz *bool) (*StatusReport, []string) {
		rep := &StatusReport{Health: health, Readyz: readyz}
		var reasons []string
		statusGuard(context.Background(), cfg, rep, func(f string, a ...any) {
			rep.Health = Unhealthy
			reasons = append(reasons, fmt.Sprintf(f, a...))
		})
		return rep, reasons
	}

	before := count()
	if rep, reasons := run(Unhealthy, ptr(true)); rep.CRDGuard != nil || len(reasons) != 0 || count() != before {
		t.Errorf("an unhealthy host: guard %+v, reasons %v, %d requests; want it not asked at all", rep.CRDGuard, reasons, count()-before)
	}
	if rep, _ := run(Healthy, ptr(false)); rep.CRDGuard != nil || count() != before {
		t.Errorf("a host whose /readyz failed was asked: %+v, %d requests", rep.CRDGuard, count()-before)
	}
	if rep, _ := run(Healthy, nil); rep.CRDGuard != nil || count() != before {
		t.Errorf("a host whose /readyz was not checked was asked: %+v, %d requests", rep.CRDGuard, count()-before)
	}
	// Healthy, and the guard is not there: reported absent, and the host stays healthy.
	rep, reasons := run(Healthy, ptr(true))
	if rep.CRDGuard == nil || rep.CRDGuard.Present || rep.Health != Healthy || len(reasons) != 0 {
		t.Errorf("an absent guard: %+v, health %s, reasons %v; want absent, healthy, no reason", rep.CRDGuard, rep.Health, reasons)
	}
	// A failure to ask is not "absent": no state, and a reason that makes the host unhealthy.
	mu.Lock()
	status = http.StatusInternalServerError
	mu.Unlock()
	rep, reasons = run(Healthy, ptr(true))
	if rep.CRDGuard != nil || rep.Health != Unhealthy || len(reasons) != 1 || !strings.Contains(reasons[0], "could not be checked") {
		t.Errorf("an API error: %+v, health %s, reasons %v; want no state and one reason", rep.CRDGuard, rep.Health, reasons)
	}
}

package launcher

// The three admission policies' launcher side (admission.go; issue #589, PR 3) without an API server:
// what the embedded manifests hold, what counts as a probe that refused, the wait, and the ORDER in
// which the agents' writes open. Whether the API server then refuses what an agent writes is judged
// elsewhere, by real requests made with real agent tokens: api/v1alpha1/e2e on envtest and cmd/hpcburst
// on a real k3s.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// The embedded files are the files of the repository.
func TestAdmissionPoliciesAreTheFilesOfTheRepository(t *testing.T) {
	for _, p := range admissionPolicies {
		onDisk, err := os.Open(filepath.Join("..", "..", p.file))
		if err != nil {
			t.Fatal(err)
		}
		objs, err := decodeManifests(onDisk, p.file)
		onDisk.Close()
		if err != nil {
			t.Fatal(err)
		}
		policy, binding, err := p.objects()
		if err != nil {
			t.Fatal(err)
		}
		if len(objs) != 2 || !reflect.DeepEqual(objs[0].Object, policy.Object) || !reflect.DeepEqual(objs[1].Object, binding.Object) {
			t.Errorf("the embedded %s differs from %s on disk", p.name, p.file)
		}
	}
}

// What HBEP-10 says about each policy, written here and not read back: the names, a failurePolicy that
// denies when it cannot decide, a binding that denies, and a refusal that is a Forbidden with a message.
func TestAdmissionPoliciesAreWhatHBEP10Describes(t *testing.T) {
	if got, want := AdmissionPolicyNames(), []string{"hpcburst-capacity-guard", "hpcburst-owner-pin", "hpcburst-approver-pin"}; !slices.Equal(got, want) {
		t.Fatalf("the policies are %v, want %v", got, want)
	}
	for _, p := range admissionPolicies {
		policy, binding, err := p.objects()
		if err != nil {
			t.Fatal(err)
		}
		if fp, _, _ := unstructured.NestedString(policy.Object, "spec", "failurePolicy"); fp != "Fail" {
			t.Errorf("%s: failurePolicy %q, want Fail: a policy that lets a write through when it cannot decide bounds nothing", p.name, fp)
		}
		if actions, _, _ := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions"); !slices.Equal(actions, []string{"Deny"}) {
			t.Errorf("%s: the binding's validationActions %v, want [Deny]", p.name, actions)
		}
		if name, _, _ := unstructured.NestedString(binding.Object, "spec", "policyName"); name != p.name {
			t.Errorf("%s: the binding names the policy %q", p.name, name)
		}
		vals, _, _ := unstructured.NestedSlice(policy.Object, "spec", "validations")
		if len(vals) == 0 {
			t.Errorf("%s: no validations", p.name)
		}
		for i, v := range vals {
			m := v.(map[string]any)
			if m["reason"] != "Forbidden" || m["message"] == nil || m["message"] == "" || m["expression"] == "" {
				t.Errorf("%s: validation %d = %v, want an expression, a message and reason Forbidden", p.name, i, m)
			}
		}
	}
	// The capacity guard matches the agents' group and nothing else: every agent identity, and not the
	// person or a controller.
	policy, _, _ := admissionPolicies[0].objects()
	conds, _, _ := unstructured.NestedSlice(policy.Object, "spec", "matchConditions")
	if len(conds) != 1 || !strings.Contains(conds[0].(map[string]any)["expression"].(string), "g == 'system:serviceaccounts:hpcburst-agents'") {
		t.Errorf("the capacity guard's match conditions %v do not select the group system:serviceaccounts:hpcburst-agents", conds)
	}
	if AgentsGroup != "system:serviceaccounts:hpcburst-agents" {
		t.Errorf("AgentsGroup = %q", AgentsGroup)
	}
}

// request.subResource is absent on the main resource, and reading it errors (plan section 1, verified
// against a real API server): every reference goes through has(), here and in every later edit. The one
// form accepted is has(request.subResource) ? request.subResource : ..., where the read is under its own
// guard; after that form and every has() are taken out, no reference may remain.
func TestEverySubResourceReferenceGoesThroughHas(t *testing.T) {
	const ref = "request.subResource"
	checked := 0
	for _, p := range admissionPolicies {
		policy, _, _ := p.objects()
		var exprs []string
		for _, field := range []string{"variables", "validations", "matchConditions"} {
			items, _, _ := unstructured.NestedSlice(policy.Object, "spec", field)
			for _, it := range items {
				exprs = append(exprs, it.(map[string]any)["expression"].(string))
			}
		}
		for _, e := range exprs {
			if !strings.Contains(e, ref) {
				continue
			}
			checked++
			rest := strings.ReplaceAll(e, "has("+ref+") ? "+ref, "")
			rest = strings.ReplaceAll(rest, "has("+ref+")", "")
			if strings.Contains(rest, ref) {
				t.Errorf("%s: %q reads %s outside has()", p.name, e, ref)
			}
		}
	}
	if checked == 0 {
		t.Error("no policy reads request.subResource: the capacity guard must, to tell scale and status from the main resource")
	}
	// The check itself: an unguarded read is found.
	if rest := strings.ReplaceAll("variables.x == "+ref, "has("+ref+")", ""); !strings.Contains(rest, ref) {
		t.Error("the check does not find an unguarded read")
	}
}

func TestCheckAdmissionIsPresentOnlyWhatStopsAWrite(t *testing.T) {
	f := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		guardPolicyGVR: "ValidatingAdmissionPolicyList", guardBindingGVR: "ValidatingAdmissionPolicyBindingList",
	})
	for _, p := range admissionPolicies {
		st, err := p.check(context.Background(), f)
		if err != nil || st.Present || !strings.Contains(st.Detail, p.name) {
			t.Errorf("%s on an empty host = %+v, %v, want absent, naming it", p.name, st, err)
		}
	}
	var objs []runtime.Object
	for _, p := range admissionPolicies {
		policy, binding, _ := p.objects()
		objs = append(objs, policy.DeepCopy(), binding.DeepCopy())
	}
	f = fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		guardPolicyGVR: "ValidatingAdmissionPolicyList", guardBindingGVR: "ValidatingAdmissionPolicyBindingList",
	}, objs...)
	for _, p := range admissionPolicies {
		if st, err := p.check(context.Background(), f); err != nil || !st.Present {
			t.Errorf("%s on a host that holds its manifest = %+v, %v, want present", p.name, st, err)
		}
	}
	// Only the owner pin's binding goes: the other two are still present, and the pin is reported by name.
	if err := f.Resource(guardBindingGVR).Delete(context.Background(), OwnerPinName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range admissionPolicies {
		st, err := p.check(context.Background(), f)
		if err != nil || st.Present != (p.name != OwnerPinName) {
			t.Errorf("%s with only the owner pin's binding gone = %+v, %v", p.name, st, err)
		}
	}
}

func fakeCRDHost(names ...string) *fake.FakeDynamicClient {
	var objs []runtime.Object
	for _, n := range names {
		objs = append(objs, crdObject(n, FrameworkGroup))
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, objs...)
}

func forbiddenBy(policy string) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: FrameworkGroup, Resource: "x"}, "y",
		fmt.Errorf("ValidatingAdmissionPolicy '%s' with binding '%s' denied request: a message", policy, policy))
}

// A probe counts as refused only when the refusal names its own policy: an RBAC refusal, another
// policy's, a schema refusal and an accepted probe are all "not enforced yet" for this one.
func TestAProbeCountsOnlyWhenTheRefusalNamesItsOwnPolicy(t *testing.T) {
	rbacDenial := apierrors.NewForbidden(schema.GroupResource{Group: FrameworkGroup, Resource: "modeldeployments"}, "x", errors.New(`User "u" cannot create resource`))
	for _, tc := range []struct {
		name        string
		err         error
		wantRefused bool
		wantErr     bool
	}{
		{"its own refusal", forbiddenBy(CapacityGuardName), true, false},
		{"another policy's refusal", forbiddenBy(OwnerPinName), false, true},
		{"a bare RBAC Forbidden", rbacDenial, false, true},
		{"a schema refusal", apierrors.NewInvalid(schema.GroupKind{Group: FrameworkGroup, Kind: "ModelDeployment"}, "x", nil), false, true},
		{"an accepted probe", nil, false, false},
		{"a Forbidden whose text names the policy without the API server's wording", apierrors.NewForbidden(schema.GroupResource{}, "x", errors.New("hpcburst-capacity-guard")), false, true},
	} {
		p := admissionPolicy{guardDef: guardDef{name: CapacityGuardName}, attempt: func(context.Context, *rest.Config) error { return tc.err }}
		refused, err := p.probe(context.Background(), nil)
		if refused != tc.wantRefused || (err != nil) != tc.wantErr {
			t.Errorf("%s: refused %v, err %v, want refused %v, an error %v", tc.name, refused, err, tc.wantRefused, tc.wantErr)
		}
	}
}

// ProbeAdmission names a policy of the three, and refuses another name.
func TestProbeAdmissionRefusesAnUnknownName(t *testing.T) {
	if _, err := ProbeAdmission(context.Background(), nil, "hpcburst-protect-crds"); err == nil || !strings.Contains(err.Error(), "not one of the admission policies") {
		t.Errorf("ProbeAdmission of the CRD guard: %v, want a refusal", err)
	}
}

// The wait returns once every policy has refused; a policy that refuses only after some tries is waited
// for; one whose probe never refuses fails the wait with its name; and one whose CRD is absent is not
// probed and not an error, but reported.
func TestWaitAdmissionEnforced(t *testing.T) {
	crds := func(ps []admissionPolicy) *fake.FakeDynamicClient {
		var n []string
		for _, p := range ps {
			n = append(n, p.crd)
		}
		return fakeCRDHost(n...)
	}
	mk := func(name string, tries *int, refuseAfter int) admissionPolicy {
		return admissionPolicy{guardDef: guardDef{name: name}, crd: name + "." + FrameworkGroup,
			attempt: func(context.Context, *rest.Config) error {
				*tries++
				if *tries > refuseAfter {
					return forbiddenBy(name)
				}
				return nil
			}}
	}
	t.Run("all refuse, one after some tries", func(t *testing.T) {
		var a, b, c int
		ps := []admissionPolicy{mk("p-a", &a, 0), mk("p-b", &b, 3), mk("p-c", &c, 0)}
		un, err := waitAdmissionEnforced(context.Background(), nil, crds(ps), ps, 5*time.Second, time.Millisecond, io.Discard)
		if err != nil || len(un) != 0 || b != 4 {
			t.Errorf("= %v, %v after %d tries of the slow one, want none unprobed and no error after 4", un, err, b)
		}
	})
	t.Run("one never refuses", func(t *testing.T) {
		var a, b int
		ps := []admissionPolicy{mk("p-a", &a, 0), mk("p-b", &b, 1<<30)}
		_, err := waitAdmissionEnforced(context.Background(), nil, crds(ps), ps, 50*time.Millisecond, time.Millisecond, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "p-b") || strings.Contains(err.Error(), "p-a") {
			t.Errorf("error %v, want one that names p-b and not p-a", err)
		}
	})
	t.Run("a refusal by another policy never counts", func(t *testing.T) {
		p := admissionPolicy{guardDef: guardDef{name: "p-a"}, crd: "p-a." + FrameworkGroup,
			attempt: func(context.Context, *rest.Config) error { return forbiddenBy("p-other") }}
		_, err := waitAdmissionEnforced(context.Background(), nil, crds([]admissionPolicy{p}), []admissionPolicy{p}, 30*time.Millisecond, time.Millisecond, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "p-a") || !strings.Contains(err.Error(), "p-other") {
			t.Errorf("error %v, want one naming p-a and showing what was answered", err)
		}
	})
	t.Run("an absent CRD is reported, not probed, not an error", func(t *testing.T) {
		var a, b int
		ps := []admissionPolicy{mk("p-a", &a, 0), mk("p-b", &b, 0)}
		var log strings.Builder
		un, err := waitAdmissionEnforced(context.Background(), nil, fakeCRDHost(ps[0].crd), ps, time.Second, time.Millisecond, &log)
		if err != nil || !slices.Equal(un, []string{"p-b"}) || b != 0 || a == 0 {
			t.Errorf("= %v, %v with %d probes of the absent one and %d of the present one, want [p-b], no error, 0 and >0", un, err, b, a)
		}
		if !strings.Contains(log.String(), "nothing to probe p-b with") {
			t.Errorf("the log %q does not say there was nothing to probe p-b with", log.String())
		}
	})
}

// The window is closed, not shortened (team-lead, 15 Nov 2026): the policies, then the narrow role,
// then the wait, and only then the wide role. Recorded step by step, so the order is what is judged.
func TestInstallAgentAccessClosesTheWindow(t *testing.T) {
	var order []string
	steps := func(unprobed []string, waitErr error) agentAccessSteps {
		order = nil
		rec := func(s string) func(context.Context) error {
			return func(context.Context) error { order = append(order, s); return nil }
		}
		return agentAccessSteps{
			policies: rec("policies"), narrow: rec("narrow"), wide: rec("wide"), log: io.Discard,
			wait: func(_ context.Context, d time.Duration) ([]string, error) {
				order = append(order, fmt.Sprintf("wait %s", d))
				return unprobed, waitErr
			},
		}
	}
	hook := func(context.Context) error { order = append(order, "held"); return nil }

	writes, err := installAgentAccess(context.Background(), steps(nil, nil), AgentAccessOptions{BeforeWait: hook})
	if err != nil || !writes || !slices.Equal(order, []string{"policies", "narrow", "held", "wait 1m0s", "wide"}) {
		t.Errorf("order %v, writes %v, %v, want policies, narrow, held, wait 1m0s, wide", order, writes, err)
	}
	if _, err := installAgentAccess(context.Background(), steps(nil, nil), AgentAccessOptions{EnforcementTimeout: 3 * time.Second}); err != nil || !slices.Contains(order, "wait 3s") {
		t.Errorf("order %v, %v, want the option's timeout to bound the wait", order, err)
	}

	// A probe that never refuses: an error, and the wide role is never applied.
	writes, err = installAgentAccess(context.Background(), steps(nil, errors.New("p never refused")), AgentAccessOptions{})
	if err == nil || writes || slices.Contains(order, "wide") {
		t.Errorf("a wait that failed: order %v, writes %v, %v, want an error and no wide role", order, writes, err)
	}
	// A policy that could not be probed: no error, no wide role.
	writes, err = installAgentAccess(context.Background(), steps([]string{OwnerPinName}, nil), AgentAccessOptions{})
	if err != nil || writes || slices.Contains(order, "wide") {
		t.Errorf("an unprobed policy: order %v, writes %v, %v, want no error and no wide role", order, writes, err)
	}
	// Each earlier step's failure stops the later ones.
	for _, failAt := range []string{"policies", "narrow"} {
		st := steps(nil, nil)
		boom := errors.New("boom")
		if failAt == "policies" {
			st.policies = func(context.Context) error { order = append(order, "policies"); return boom }
		} else {
			st.narrow = func(context.Context) error { order = append(order, "narrow"); return boom }
		}
		order = nil
		if writes, err := installAgentAccess(context.Background(), st, AgentAccessOptions{}); !errors.Is(err, boom) || writes || slices.Contains(order, "wide") || slices.ContainsFunc(order, func(s string) bool { return strings.HasPrefix(s, "wait") }) {
			t.Errorf("%s failed: order %v, writes %v, %v, want the error and no wait or wide role", failAt, order, writes, err)
		}
	}
	// A hook that fails stops the step before the wait.
	order = nil
	if _, err := installAgentAccess(context.Background(), steps(nil, nil), AgentAccessOptions{BeforeWait: func(context.Context) error { return errors.New("stop") }}); err == nil || slices.Contains(order, "wide") {
		t.Errorf("a failing BeforeWait: order %v, %v, want an error and no wide role", order, err)
	}
}

// ApplyManifests waits for each CRD to be Established, not for its kind to be served, so a probe made right
// after it can fail with "no matches for kind" (the REST mapper's error) or a 404 (the dynamic client's)
// while discovery lags (team-lead, from #603). That is "not yet": retried within the bound, never counted as
// the policy being enforced, never a permanent failure. Only a refusal that names the probe's own policy is
// enforcement.
func TestAProbeOfAKindThatIsNotServedYetIsRetriedNeverCounted(t *testing.T) {
	noMatch := func() error {
		return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: FrameworkGroup, Kind: "ModelDeployment"}, SearchedVersions: []string{"v1alpha1"}}
	}
	notFound := func() error {
		return apierrors.NewNotFound(schema.GroupResource{Group: FrameworkGroup, Resource: "modeldeployments"}, "")
	}
	// One probe: not served is an error that says so, and is not a refusal.
	for name, err := range map[string]error{"no match for the kind": noMatch(), "a 404": notFound()} {
		p := admissionPolicy{guardDef: guardDef{name: CapacityGuardName}, attempt: func(context.Context, *rest.Config) error { return err }}
		refused, perr := p.probe(context.Background(), nil)
		if refused || !errors.Is(perr, errNotServedYet) {
			t.Errorf("%s: refused %v, error %v, want not refused, with the not-served error", name, refused, perr)
		}
	}

	// The wait: answers no-match and 404 a few times, then the policy's refusal. It returns only after the
	// refusal, having probed once more than it was refused.
	var tries int
	answers := []error{noMatch(), notFound(), noMatch(), nil}
	p := admissionPolicy{guardDef: guardDef{name: CapacityGuardName}, crd: "modeldeployments." + FrameworkGroup,
		attempt: func(context.Context, *rest.Config) error {
			tries++
			if tries <= len(answers) {
				return answers[tries-1]
			}
			return forbiddenBy(CapacityGuardName)
		}}
	un, err := waitAdmissionEnforced(context.Background(), nil, fakeCRDHost(p.crd), []admissionPolicy{p}, 10*time.Second, time.Millisecond, io.Discard)
	if err != nil || len(un) != 0 || tries != len(answers)+1 {
		t.Errorf("= %v, %v after %d probes, want success on the probe after the %d answers that were not a refusal (including one that was accepted)", un, err, tries, len(answers))
	}

	// No match until the bound: a failure that names the policy, not a success.
	stuck := admissionPolicy{guardDef: guardDef{name: CapacityGuardName}, crd: "modeldeployments." + FrameworkGroup,
		attempt: func(context.Context, *rest.Config) error { return noMatch() }}
	_, err = waitAdmissionEnforced(context.Background(), nil, fakeCRDHost(stuck.crd), []admissionPolicy{stuck}, 50*time.Millisecond, time.Millisecond, io.Discard)
	if err == nil || !strings.Contains(err.Error(), CapacityGuardName) || !strings.Contains(err.Error(), "does not serve") {
		t.Errorf("no match until the bound: %v, want a failure that names %s and says the kind is not served", err, CapacityGuardName)
	}
}

// A policy edited by hand to decide something else exists, and is not present: the fields the server does
// not default are compared whole. The CRD guard's manifest sets no match condition, so an extra one was
// always found; these policies set theirs, so an edit to a field they have must be found too.
func TestCheckAdmissionFindsAPolicyEditedByHand(t *testing.T) {
	for _, p := range admissionPolicies {
		for _, tc := range []struct {
			name  string
			edit  func(spec map[string]any)
			field string
		}{
			{"a failure policy that lets a write through", func(s map[string]any) { s["failurePolicy"] = "Ignore" }, "spec.failurePolicy"},
			{"a match condition that is never true", func(s map[string]any) {
				s["matchConditions"] = []any{map[string]any{"name": "never", "expression": "false"}}
			}, "spec.matchConditions"},
			{"validations that always pass", func(s map[string]any) {
				s["validations"] = []any{map[string]any{"expression": "true", "reason": "Forbidden", "message": "x"}}
			}, "spec.validations"},
			{"a variable that is something else", func(s map[string]any) {
				s["variables"] = []any{map[string]any{"name": "op", "expression": "'NEVER'"}}
			}, "spec.variables"},
		} {
			policy, binding, _ := p.objects()
			edited := policy.DeepCopy()
			spec := edited.Object["spec"].(map[string]any)
			tc.edit(spec)
			f := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				guardPolicyGVR: "ValidatingAdmissionPolicyList", guardBindingGVR: "ValidatingAdmissionPolicyBindingList",
			}, edited, binding.DeepCopy())
			st, err := p.check(context.Background(), f)
			if err != nil || st.Present || !strings.Contains(st.Detail, tc.field) {
				t.Errorf("%s with %s = %+v, %v, want absent, naming %s", p.name, tc.name, st, err, tc.field)
			}
		}
	}
}

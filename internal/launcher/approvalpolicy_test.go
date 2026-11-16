package launcher

// The global ApprovalPolicy that up writes, and the check that the controller's definitions exist (approvalpolicy.go;
// issue #589, launcher track), without an API server of their own: a fake dynamic client whose recorded
// actions show what was SENT, so "never overwritten" is judged by the absence of a write and not by the
// object's content alone. The expected user names are literals here, never read back from the code under test.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
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
	k8stesting "k8s.io/client-go/testing"
)

const globalPolicyPath = "/apis/" + FrameworkGroup + "/v1alpha1/approvalpolicies/global"

// policyHost is a fake API server that holds the named definitions and the given objects.
func policyHost(crds []string, objs ...runtime.Object) *fake.FakeDynamicClient {
	for _, n := range crds {
		objs = append(objs, crdObject(n, FrameworkGroup))
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		crdGVR: "CustomResourceDefinitionList", approvalPolicyGVR: "ApprovalPolicyList",
	}, objs...)
}

// existingPolicy is a global policy a person wrote, with approvers that are not the admin.
func existingPolicy() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": FrameworkGroup + "/v1alpha1", "kind": "ApprovalPolicy",
		"metadata": map[string]any{"name": "global"},
		"spec": map[string]any{
			"scope":     map[string]any{"global": true},
			"approvers": []any{map[string]any{"user": "example-person"}, map[string]any{"group": "example-approvers"}},
		},
	}}
}

// writes are the actions that change something, of any resource.
func writes(f *fake.FakeDynamicClient) []string {
	var out []string
	for _, a := range f.Actions() {
		switch a.GetVerb() {
		case "get", "list", "watch":
		default:
			out = append(out, a.GetVerb()+" "+a.GetResource().Resource)
		}
	}
	return out
}

// onPolicies counts the actions of one verb on approvalpolicies.
func onPolicies(f *fake.FakeDynamicClient, verb string) int {
	n := 0
	for _, a := range f.Actions() {
		if a.GetVerb() == verb && a.GetResource().Resource == "approvalpolicies" {
			n++
		}
	}
	return n
}

func whoamiIs(user string, calls *int) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		if calls != nil {
			*calls++
		}
		return user, nil
	}
}

func noMatch() error {
	return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: FrameworkGroup, Kind: "ApprovalPolicy"}, SearchedVersions: []string{"v1alpha1"}}
}

func notServed404() error {
	return apierrors.NewNotFound(schema.GroupResource{Group: FrameworkGroup, Resource: "approvalpolicies"}, "")
}

// failFirst makes the first n actions of the verb on approvalpolicies answer err, and the rest go on to the
// fake's own tracker.
func failFirst(f *fake.FakeDynamicClient, verb string, n int, err func() error) {
	var mu sync.Mutex
	seen := 0
	f.PrependReactor(verb, "approvalpolicies", func(k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		seen++
		if seen <= n {
			return true, nil, err()
		}
		return false, nil, nil
	})
}

func TestEnsureGlobalPolicyCreatesItWithTheAdminAsItsOnlyApprover(t *testing.T) {
	f := policyHost([]string{approvalPolicyCRD})
	var asked int
	var log strings.Builder
	out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", &asked), time.Second, time.Millisecond, &log)
	if err != nil || out != GlobalPolicyCreated {
		t.Fatalf("= %q, %v, want %q", out, err, GlobalPolicyCreated)
	}
	got, err := f.Resource(approvalPolicyGVR).Get(context.Background(), "global", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetAPIVersion() != FrameworkGroup+"/v1alpha1" || got.GetKind() != "ApprovalPolicy" {
		t.Errorf("the object is %s %s", got.GetAPIVersion(), got.GetKind())
	}
	// Written out in full: the global scope, and one approver, the admin.
	want := map[string]any{
		"scope":     map[string]any{"global": true},
		"approvers": []any{map[string]any{"user": "example-admin"}},
	}
	if spec, _, _ := unstructured.NestedMap(got.Object, "spec"); !reflect.DeepEqual(spec, want) {
		t.Errorf("spec = %v, want %v", spec, want)
	}
	if w := writes(f); !slices.Equal(w, []string{"create approvalpolicies"}) {
		t.Errorf("writes = %v, want exactly one create of an ApprovalPolicy", w)
	}
	if asked != 1 {
		t.Errorf("the admin was asked for %d times, want once", asked)
	}
	if !strings.Contains(log.String(), "example-admin") {
		t.Errorf("the log %q does not name the approver", log.String())
	}
}

// An existing policy is left exactly as it is, whatever it says: no create, no update, no patch, no delete
// was SENT, and the admin is not even asked.
func TestEnsureGlobalPolicyNeverWritesOverOneThatExists(t *testing.T) {
	orig := existingPolicy()
	f := policyHost([]string{approvalPolicyCRD}, orig.DeepCopy())
	var asked int
	out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", &asked), time.Second, time.Millisecond, io.Discard)
	if err != nil || out != GlobalPolicyExists {
		t.Fatalf("= %q, %v, want %q", out, err, GlobalPolicyExists)
	}
	if w := writes(f); len(w) != 0 {
		t.Errorf("the fake API server saw writes %v, want none", w)
	}
	if asked != 0 {
		t.Errorf("the admin was asked for %d times although no policy was to be written", asked)
	}
	got, err := f.Resource(approvalPolicyGVR).Get(context.Background(), "global", metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(got.Object, orig.Object) {
		t.Errorf("the policy changed: %v (err %v), was %v", got.Object, err, orig.Object)
	}
}

// A second up finds what the first wrote and writes nothing: the first creates, the second sends no write.
func TestEnsureGlobalPolicyTwiceWritesOnce(t *testing.T) {
	f := policyHost([]string{approvalPolicyCRD})
	for i, want := range []string{GlobalPolicyCreated, GlobalPolicyExists} {
		if out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), time.Second, time.Millisecond, io.Discard); err != nil || out != want {
			t.Fatalf("run %d = %q, %v, want %q", i+1, out, err, want)
		}
	}
	if w := writes(f); len(w) != 1 {
		t.Errorf("writes = %v, want the one create of the first run", w)
	}
}

// A host without the definition is not a failure: nothing is written and the outcome says why.
func TestEnsureGlobalPolicyWithoutTheDefinitionWritesNothing(t *testing.T) {
	f := policyHost([]string{"operations." + FrameworkGroup, "approvals." + FrameworkGroup}) // the other two are there
	var asked int
	var log strings.Builder
	out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", &asked), time.Second, time.Millisecond, &log)
	if err != nil || out != GlobalPolicyNoCRD {
		t.Fatalf("= %q, %v, want %q and no error", out, err, GlobalPolicyNoCRD)
	}
	if w := writes(f); len(w) != 0 {
		t.Errorf("writes = %v, want none", w)
	}
	for _, a := range f.Actions() {
		if a.GetResource().Resource == "approvalpolicies" {
			t.Errorf("%s of approvalpolicies was sent although the definition is absent", a.GetVerb())
		}
	}
	if asked != 0 {
		t.Errorf("the admin was asked for %d times", asked)
	}
	if !strings.Contains(log.String(), approvalPolicyCRD) {
		t.Errorf("the log %q does not name the definition", log.String())
	}
	for _, w := range []string{"ApprovalPolicy", approvalPolicyCRD, "--manifests config/crd"} {
		if !strings.Contains(noApprovalPolicyCRDWarning, w) {
			t.Errorf("the warning lacks %q: %s", w, noApprovalPolicyCRDWarning)
		}
	}
}

// ApplyManifests waits for the definition to be Established, not for its kind to be served, so the Get and
// the Create can meet "no matches for kind" or a 404 for a while (#603): that is retried, and ends in a
// policy; forever it is an error that names ApprovalPolicy, never a silent skip.
func TestEnsureGlobalPolicyRetriesAKindThatIsNotServedYet(t *testing.T) {
	t.Run("no matches on the get, then a 404 on the create, then served", func(t *testing.T) {
		f := policyHost([]string{approvalPolicyCRD})
		failFirst(f, "get", 3, noMatch)
		failFirst(f, "create", 2, notServed404)
		var asked int
		out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", &asked), 10*time.Second, time.Millisecond, io.Discard)
		if err != nil || out != GlobalPolicyCreated {
			t.Fatalf("= %q, %v, want %q", out, err, GlobalPolicyCreated)
		}
		if n := onPolicies(f, "create"); n != 3 {
			t.Errorf("%d creates, want 3 (two answered 404, the third accepted)", n)
		}
		if n := onPolicies(f, "get"); n < 4 {
			t.Errorf("%d gets, want at least 4 (three answered no match)", n)
		}
		if asked != 1 {
			t.Errorf("the admin was asked for %d times across the retries, want once", asked)
		}
		if _, err := f.Resource(approvalPolicyGVR).Get(context.Background(), "global", metav1.GetOptions{}); err != nil {
			t.Errorf("no policy after the retries: %v", err)
		}
	})
	for name, tc := range map[string]struct {
		verb string
		err  func() error
	}{
		"no matches for the kind on the get, for ever": {"get", noMatch},
		"a 404 on the create, for ever":                {"create", notServed404},
	} {
		t.Run(name, func(t *testing.T) {
			f := policyHost([]string{approvalPolicyCRD})
			failFirst(f, tc.verb, 1<<30, tc.err)
			start := time.Now()
			out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), 60*time.Millisecond, time.Millisecond, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "ApprovalPolicy") || out != "" {
				t.Fatalf("= %q, %v, want an error that names ApprovalPolicy", out, err)
			}
			if d := time.Since(start); d > 10*time.Second {
				t.Errorf("took %s against a 60 ms bound", d)
			}
			if tc.verb == "get" && onPolicies(f, "create") != 0 {
				t.Errorf("a create was sent while the kind was not matched")
			}
		})
	}
}

// Another up (or a person) wrote it between the get and the create: that is "exists", not a failure.
func TestEnsureGlobalPolicyAlreadyExistsOnCreateIsSuccess(t *testing.T) {
	f := policyHost([]string{approvalPolicyCRD})
	f.PrependReactor("create", "approvalpolicies", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: FrameworkGroup, Resource: "approvalpolicies"}, "global")
	})
	out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), time.Second, time.Millisecond, io.Discard)
	if err != nil || out != GlobalPolicyExists {
		t.Errorf("= %q, %v, want %q", out, err, GlobalPolicyExists)
	}
}

// Anything else is an error, and is not retried as if it were "not yet".
func TestEnsureGlobalPolicyOtherFailuresAreErrors(t *testing.T) {
	forbidden := func() error {
		return apierrors.NewForbidden(schema.GroupResource{Group: FrameworkGroup, Resource: "approvalpolicies"}, "global", errors.New("example: no"))
	}
	t.Run("a get that is refused", func(t *testing.T) {
		f := policyHost([]string{approvalPolicyCRD})
		failFirst(f, "get", 1<<30, forbidden)
		if _, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), 10*time.Second, time.Millisecond, io.Discard); err == nil || !apierrors.IsForbidden(errors.Unwrap(err)) {
			t.Errorf("error %v, want the refusal", err)
		}
		if n := onPolicies(f, "get"); n != 1 {
			t.Errorf("%d gets, want 1: a refusal is not retried", n)
		}
	})
	t.Run("a create that is refused", func(t *testing.T) {
		f := policyHost([]string{approvalPolicyCRD})
		failFirst(f, "create", 1<<30, forbidden)
		if _, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), 10*time.Second, time.Millisecond, io.Discard); err == nil || !strings.Contains(err.Error(), "create ApprovalPolicy") {
			t.Errorf("error %v, want a failed create", err)
		}
		if n := onPolicies(f, "create"); n != 1 {
			t.Errorf("%d creates, want 1: a refusal is not retried", n)
		}
	})
	t.Run("the admin cannot be identified", func(t *testing.T) {
		f := policyHost([]string{approvalPolicyCRD})
		who := func(context.Context) (string, error) { return "", errors.New("example: no review") }
		if _, err := ensureGlobalApprovalPolicy(context.Background(), f, who, time.Second, time.Millisecond, io.Discard); err == nil || !strings.Contains(err.Error(), "example: no review") {
			t.Errorf("error %v, want the review's", err)
		}
		if w := writes(f); len(w) != 0 {
			t.Errorf("writes = %v, want none without an approver to name", w)
		}
	})
	t.Run("the definition cannot be read", func(t *testing.T) {
		f := policyHost(nil)
		f.PrependReactor("get", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewInternalError(errors.New("example: down"))
		})
		if out, err := ensureGlobalApprovalPolicy(context.Background(), f, whoamiIs("example-admin", nil), time.Second, time.Millisecond, io.Discard); err == nil || out == GlobalPolicyNoCRD {
			t.Errorf("= %q, %v, want an error, not 'no CRD'", out, err)
		}
	})
}

// adminUsername is the user name of a SelfSubjectReview, as the API server answers it.
func TestAdminUsernameIsWhatTheSelfSubjectReviewSays(t *testing.T) {
	var mu sync.Mutex
	status, body := 200, `{"kind":"SelfSubjectReview","apiVersion":"authentication.k8s.io/v1","status":{"userInfo":{"username":"example-admin","groups":["example-group"]}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/apis/authentication.k8s.io/v1/selfsubjectreviews" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	cfg := &rest.Config{Host: srv.URL}
	if got, err := adminUsername(context.Background(), cfg); err != nil || got != "example-admin" {
		t.Errorf("= %q, %v, want example-admin", got, err)
	}
	mu.Lock()
	body = `{"kind":"SelfSubjectReview","apiVersion":"authentication.k8s.io/v1","status":{"userInfo":{}}}`
	mu.Unlock()
	if got, err := adminUsername(context.Background(), cfg); err == nil || got != "" {
		t.Errorf("a review with no user name = %q, %v, want an error", got, err)
	}
	mu.Lock()
	status, body = 500, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`
	mu.Unlock()
	if got, err := adminUsername(context.Background(), cfg); err == nil || got != "" {
		t.Errorf("a failed review = %q, %v, want an error", got, err)
	}
}

// up refuses to start the controller when the API server lacks the definition of Operation, Approval or
// ApprovalPolicy; a host started without a controller is not asked.
func TestCheckControllerCRDs(t *testing.T) {
	all := []string{"operations." + FrameworkGroup, "approvals." + FrameworkGroup, approvalPolicyCRD}
	t.Run("all three present", func(t *testing.T) {
		if err := checkControllerCRDs(context.Background(), policyHost(all), "/bin/controller"); err != nil {
			t.Errorf("= %v, want nil", err)
		}
	})
	for i, missing := range all {
		t.Run("missing "+missing, func(t *testing.T) {
			var have []string
			for j, n := range all {
				if j != i {
					have = append(have, n)
				}
			}
			err := checkControllerCRDs(context.Background(), policyHost(have), "/bin/controller")
			if !errors.Is(err, errMissingControllerCRDs) || !strings.Contains(err.Error(), missing) || !strings.Contains(err.Error(), "--manifests config/crd") {
				t.Fatalf("= %v, want an error that names %s and says --manifests config/crd", err, missing)
			}
			for _, n := range have {
				if strings.Contains(err.Error(), n) {
					t.Errorf("the error names %s, which is there: %v", n, err)
				}
			}
		})
	}
	t.Run("none present: all three are named", func(t *testing.T) {
		err := checkControllerCRDs(context.Background(), policyHost(nil), "/bin/controller")
		if err == nil {
			t.Fatal("= nil, want an error")
		}
		for _, n := range all {
			if !strings.Contains(err.Error(), n) {
				t.Errorf("the error does not name %s: %v", n, err)
			}
		}
	})
	t.Run("no controller: not checked at all", func(t *testing.T) {
		f := policyHost(nil) // no definitions
		if err := checkControllerCRDs(context.Background(), f, ""); err != nil {
			t.Errorf("= %v, want nil for a host with no controller", err)
		}
		if n := len(f.Actions()); n != 0 {
			t.Errorf("%d requests for a host with no controller, want none", n)
		}
	})
	t.Run("a failure to ask is not 'missing'", func(t *testing.T) {
		f := policyHost(all)
		f.PrependReactor("get", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewInternalError(errors.New("example: down"))
		})
		err := checkControllerCRDs(context.Background(), f, "/bin/controller")
		if err == nil || errors.Is(err, errMissingControllerCRDs) {
			t.Errorf("= %v, want an error that does not call the definitions missing", err)
		}
	})
}

const presentPolicyBody = `{"apiVersion":"hpcburst.groundnuty.github.io/v1alpha1","kind":"ApprovalPolicy","metadata":{"name":"global"},"spec":{"scope":{"global":true},"approvers":[{"user":"example-admin"},{"group":"example-group"},{"serviceAccount":{"namespace":"example-ns","name":"example-sa"}}]}}`

const notFoundBody = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"example","reason":"NotFound","code":404}`

func TestCheckGlobalPolicyReadsTheApprovers(t *testing.T) {
	obj := existingPolicy()
	_ = unstructured.SetNestedSlice(obj.Object, []any{
		map[string]any{"user": "example-admin"}, map[string]any{"group": "example-group"},
		map[string]any{"serviceAccount": map[string]any{"namespace": "example-ns", "name": "example-sa"}},
	}, "spec", "approvers")
	got, err := checkGlobalPolicy(context.Background(), policyHost(nil, obj))
	want := &GlobalPolicyState{Present: true, Approvers: []string{"user example-admin", "group example-group", "serviceAccount example-ns/example-sa"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("= %+v, %v, want %+v", got, err, want)
	}
	// No policy, and no definition: one state.
	for name, f := range map[string]*fake.FakeDynamicClient{"no policy": policyHost([]string{approvalPolicyCRD}), "no definition": policyHost(nil)} {
		if got, err := checkGlobalPolicy(context.Background(), f); err != nil || !reflect.DeepEqual(got, &GlobalPolicyState{}) {
			t.Errorf("%s = %+v, %v, want absent", name, got, err)
		}
	}
	f := policyHost(nil)
	f.PrependReactor("get", "approvalpolicies", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("example: down"))
	})
	if got, err := checkGlobalPolicy(context.Background(), f); err == nil || got != nil {
		t.Errorf("a failed read = %+v, %v, want an error and no state", got, err)
	}
}

// statusGlobalPolicy asks only a host that is healthy, reports an absent policy without making the host
// unhealthy, and makes a failure to ask a reason.
func TestStatusGlobalPolicy(t *testing.T) {
	var mu sync.Mutex
	asked, status, body := 0, http.StatusNotFound, notFoundBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		asked++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	cfg := &rest.Config{Host: srv.URL}
	count := func() int { mu.Lock(); defer mu.Unlock(); return asked }
	run := func(health string, readyz *bool) (*StatusReport, []string) {
		rep := &StatusReport{Health: health, Readyz: readyz}
		var reasons []string
		statusGlobalPolicy(context.Background(), cfg, rep, func(f string, a ...any) {
			rep.Health = Unhealthy
			reasons = append(reasons, fmt.Sprintf(f, a...))
		})
		return rep, reasons
	}
	before := count()
	if rep, reasons := run(Unhealthy, ptr(true)); rep.GlobalApprovalPolicy != nil || len(reasons) != 0 || count() != before {
		t.Errorf("an unhealthy host: %+v, %v, %d requests; want it not asked", rep.GlobalApprovalPolicy, reasons, count()-before)
	}
	if rep, _ := run(Healthy, ptr(false)); rep.GlobalApprovalPolicy != nil || count() != before {
		t.Errorf("a host whose /readyz failed was asked: %+v", rep.GlobalApprovalPolicy)
	}
	if rep, _ := run(Healthy, nil); rep.GlobalApprovalPolicy != nil || count() != before {
		t.Errorf("a host whose /readyz was not checked was asked: %+v", rep.GlobalApprovalPolicy)
	}
	// Absent: a state, and the host stays healthy.
	rep, reasons := run(Healthy, ptr(true))
	if rep.GlobalApprovalPolicy == nil || rep.GlobalApprovalPolicy.Present || rep.GlobalApprovalPolicy.Detail != "" || rep.Health != Healthy || len(reasons) != 0 {
		t.Errorf("an absent policy: %+v, health %s, reasons %v; want absent, healthy, no reason", rep.GlobalApprovalPolicy, rep.Health, reasons)
	}
	// Present, with its approvers.
	mu.Lock()
	status, body = http.StatusOK, presentPolicyBody
	mu.Unlock()
	rep, reasons = run(Healthy, ptr(true))
	want := []string{"user example-admin", "group example-group", "serviceAccount example-ns/example-sa"}
	if g := rep.GlobalApprovalPolicy; g == nil || !g.Present || !slices.Equal(g.Approvers, want) || rep.Health != Healthy || len(reasons) != 0 {
		t.Errorf("a present policy: %+v, health %s, reasons %v; want present with %v", g, rep.Health, reasons, want)
	}
	// A failure to ask is not "absent": the state says why, and the host is unhealthy.
	mu.Lock()
	status, body = http.StatusInternalServerError, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"example","reason":"InternalError","code":500}`
	mu.Unlock()
	rep, reasons = run(Healthy, ptr(true))
	if g := rep.GlobalApprovalPolicy; g == nil || g.Present || !strings.Contains(g.Detail, "could not be read") || rep.Health != Unhealthy || len(reasons) != 1 {
		t.Errorf("an API error: %+v, health %s, reasons %v; want a state with a reason, and an unhealthy host", g, rep.Health, reasons)
	}
}

// Status itself reports the policy, on a host it judged healthy, and not at all with --no-api.
func TestStatusReportsTheGlobalPolicy(t *testing.T) {
	ask := func(h *fakeHost, noAPI bool) *StatusReport {
		t.Helper()
		rep, err := Status(context.Background(), StatusOptions{StateDir: h.l.dir, NoGCProbe: true, NoAPI: noAPI})
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	h := newFakeHost(t) // answers 404 to the policy
	if rep := ask(h, false); rep.Health != Healthy || rep.GlobalApprovalPolicy == nil || rep.GlobalApprovalPolicy.Present {
		t.Errorf("a host with no policy: %s %v %+v, want healthy and an absent policy", rep.Health, rep.Reasons, rep.GlobalApprovalPolicy)
	}
	h.globalPolicyStatus, h.globalPolicyBody = http.StatusOK, presentPolicyBody
	rep := ask(h, false)
	if g := rep.GlobalApprovalPolicy; rep.Health != Healthy || g == nil || !g.Present || len(g.Approvers) != 3 || g.Approvers[0] != "user example-admin" {
		t.Errorf("a host with the policy: %s %v %+v", rep.Health, rep.Reasons, g)
	}
	b, err := json.Marshal(rep)
	if err != nil || !strings.Contains(string(b), `"global_approval_policy":{"present":true,"approvers":["user example-admin",`) {
		t.Errorf("the JSON is %s (%v)", b, err)
	}
	if rep := ask(h, true); rep.GlobalApprovalPolicy != nil {
		t.Errorf("--no-api reports %+v, want nothing: it did not ask", rep.GlobalApprovalPolicy)
	}
	h.globalPolicyStatus, h.globalPolicyBody = http.StatusInternalServerError, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`
	if rep := ask(h, false); rep.Health != Unhealthy || rep.GlobalApprovalPolicy == nil || rep.GlobalApprovalPolicy.Detail == "" {
		t.Errorf("a failed read: %s %v %+v, want unhealthy with a reason", rep.Health, rep.Reasons, rep.GlobalApprovalPolicy)
	}
}

func TestUpReportNamesTheGlobalPolicyOutcome(t *testing.T) {
	b, err := json.Marshal(UpReport{GlobalApprovalPolicy: GlobalPolicyCreated})
	if err != nil || !strings.Contains(string(b), `"global_approval_policy":"created"`) {
		t.Errorf("the JSON is %s (%v)", b, err)
	}
	if GlobalPolicyExists != "exists" || GlobalPolicyNoCRD != "no CRD" {
		t.Errorf("the outcomes are %q and %q, want exists and no CRD", GlobalPolicyExists, GlobalPolicyNoCRD)
	}
}

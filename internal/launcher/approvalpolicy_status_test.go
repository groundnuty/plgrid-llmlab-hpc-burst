package launcher

// What status says about the global ApprovalPolicy, and the check that the controller's definitions exist
// (approvalpolicy.go; issue #589, launcher track). The helpers are approvalpolicy_test.go's.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

const globalPolicyPath = "/apis/" + FrameworkGroup + "/v1alpha1/approvalpolicies/global"

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

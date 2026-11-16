package launcher

// The launcher's wait until a CRD's kind is served (issue #603), over the dynamic client's fake and a mapper double. A CRD is Established before
// the API server serves its kind: the discovery that maps the kind, and the endpoint that lists it, can lag. The tests order their steps by the
// wait's own reads (a counter the fake bumps when the wait asks), never by a sleep, as internal/testenv/crdinstall's do for the test rigs.

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

var widgetsGVR = schemaGVR("example.invalid", "v1", "widgets")

// failGuard bounds how long a test of the wait lets a wrong implementation hang: a failure guard only.
const failGuard = 60 * time.Second

func widgetCRDManifest() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "widgets.example.invalid"},
		"spec": map[string]any{
			"group": "example.invalid", "scope": "Namespaced",
			"names":    map[string]any{"kind": "Widget", "listKind": "WidgetList", "plural": "widgets", "singular": "widget"},
			"versions": []any{map[string]any{"name": "v1", "served": true, "storage": true}},
		},
	}}
}

func widgetObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.invalid/v1", "kind": "Widget",
		"metadata": map[string]any{"name": "w1", "namespace": "default"},
	}}
}

// testMapper is a mapper double: it finds Widget only once discovery lists it, and counts its resets, one per round of the wait. Only
// RESTMapping and Reset are called.
type testMapper struct {
	meta.RESTMapper
	listed atomic.Bool
	resets atomic.Int32
}

func (m *testMapper) Reset() { m.resets.Add(1) }

func (m *testMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk.Kind == "CustomResourceDefinition" { // the API server always serves the CRD type itself
		return &meta.RESTMapping{Resource: apiextensionsCRD, GroupVersionKind: gk.WithVersion("v1"), Scope: meta.RESTScopeRoot}, nil
	}
	if gk.Kind != "Widget" || !m.listed.Load() {
		return nil, &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}
	return &meta.RESTMapping{Resource: widgetsGVR, GroupVersionKind: gk.WithVersion("v1"), Scope: meta.RESTScopeNamespace}, nil
}

// patchRecord is an apply the fake saw.
type patchRecord struct {
	resource   string
	name       string
	servedThen bool // whether the Widget endpoint answered lists when it was applied
}

// servedRig is a fake API server for one CRD and one kind: the CRD is Established unless the test says not; the Widget endpoint answers lists
// only once served; discovery lists Widget only once the mapper's listed flag is set. It records the applies and counts the reads of the wait.
type servedRig struct {
	dyn         *dynamicfake.FakeDynamicClient
	mapper      *testMapper
	established atomic.Bool
	served      atomic.Bool
	crdReads    atomic.Int32
	lists       atomic.Int32

	mu      sync.Mutex
	patches []patchRecord
}

func newServedRig(t *testing.T) *servedRig {
	t.Helper()
	r := &servedRig{mapper: &testMapper{}}
	r.established.Store(true)
	r.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{widgetsGVR: "WidgetList"})
	r.dyn.PrependReactor("get", "customresourcedefinitions", func(clienttesting.Action) (bool, runtime.Object, error) {
		r.crdReads.Add(1)
		crd := widgetCRDManifest()
		if r.established.Load() {
			_ = unstructured.SetNestedSlice(crd.Object, []any{map[string]any{"type": "Established", "status": "True"}}, "status", "conditions")
		}
		return true, crd, nil
	})
	r.dyn.PrependReactor("list", "widgets", func(clienttesting.Action) (bool, runtime.Object, error) {
		r.lists.Add(1)
		if !r.served.Load() {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "example.invalid", Resource: "widgets"}, "")
		}
		return false, nil, nil // the tracker answers an empty list
	})
	r.dyn.PrependReactor("patch", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
		p := a.(clienttesting.PatchAction)
		r.mu.Lock()
		r.patches = append(r.patches, patchRecord{resource: a.GetResource().Resource, name: p.GetName(), servedThen: r.served.Load()})
		r.mu.Unlock()
		return true, &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": p.GetName()}}}, nil
	})
	return r
}

func (r *servedRig) patched() []patchRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]patchRecord(nil), r.patches...)
}

// serve flips the server to serving Widget: discovery lists it and its endpoint answers.
func (r *servedRig) serve() {
	r.served.Store(true)
	r.mapper.listed.Store(true)
}

type applyResult struct {
	n   int
	err error
}

// startApply runs applyManifests in a goroutine, polling every millisecond within failGuard.
func (r *servedRig) startApply(objs ...*unstructured.Unstructured) <-chan applyResult {
	done := make(chan applyResult, 1)
	go func() {
		n, err := applyManifests(context.Background(), r.dyn, r.mapper, objs, nil, failGuard, time.Millisecond)
		done <- applyResult{n, err}
	}()
	return done
}

// awaitCount blocks until get() reaches want, or fails if the apply returned first or the guard passed: an apply that returns before the wait
// has asked (the mutant "return at once") is caught here.
func awaitCount(t *testing.T, get func() int32, want int32, done <-chan applyResult, what string) {
	t.Helper()
	guard := time.After(failGuard)
	for get() < want {
		select {
		case res := <-done:
			t.Fatalf("the apply returned (%d applied, %v) before %s", res.n, res.err, what)
		case <-guard:
			t.Fatalf("the apply never reached %s", what)
		case <-time.After(time.Millisecond):
		}
	}
}

func requireStillRunning(t *testing.T, done <-chan applyResult, what string) {
	t.Helper()
	select {
	case res := <-done:
		t.Fatalf("the apply returned (%d applied, %v) %s", res.n, res.err, what)
	default:
	}
}

func requireReturns(t *testing.T, done <-chan applyResult, what string) applyResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(failGuard):
		t.Fatalf("the apply did not return %s", what)
	}
	return applyResult{}
}

// A CRD that is Established but whose kind discovery does not list yet holds the apply: with no object of the kind to apply, nothing but the
// wait can hold it (applyOne's own retry is for an object it applies), and it returns only once the kind is served. The steps are ordered by
// the wait's rounds, counted in the mapper's resets.
func TestApplyManifestsWaitsUntilDiscoveryListsTheKind(t *testing.T) {
	r := newServedRig(t)
	done := r.startApply(widgetCRDManifest())
	awaitCount(t, r.mapper.resets.Load, 3, done, "its third round")
	requireStillRunning(t, done, "while discovery did not list the kind")
	if r.lists.Load() != 0 {
		t.Errorf("the wait listed the kind %d times before discovery listed it", r.lists.Load())
	}

	r.serve()
	res := requireReturns(t, done, "once the kind was served")
	if res.err != nil || res.n != 1 {
		t.Errorf("the apply ended with %d applied and %v, want the one CRD and no error", res.n, res.err)
	}
	if r.lists.Load() == 0 {
		t.Error("the wait never listed the kind: discovery listing it is not the endpoint answering")
	}
}

// A kind that discovery lists but whose endpoint answers 404 holds the next apply back: the object of the kind is applied only after a list of
// the kind answers, the second refusal is seen first, and the apply that follows finds the endpoint serving.
func TestApplyManifestsHoldsTheNextApplyUntilAListOfTheKindAnswers(t *testing.T) {
	r := newServedRig(t)
	r.mapper.listed.Store(true) // discovery lists it; the endpoint does not answer yet
	done := r.startApply(widgetCRDManifest(), widgetObject())
	awaitCount(t, r.lists.Load, 2, done, "its second refused list of the kind")
	requireStillRunning(t, done, "while the kind's list was refused")
	for _, p := range r.patched() {
		if p.resource == "widgets" {
			t.Fatalf("the Widget was applied (%+v) while a list of the kind was refused", p)
		}
	}

	r.serve()
	res := requireReturns(t, done, "once the kind answered")
	if res.err != nil || res.n != 2 {
		t.Fatalf("the apply ended with %d applied and %v, want the CRD and the Widget and no error", res.n, res.err)
	}
	got := r.patched()
	if len(got) != 2 || got[0].resource != "customresourcedefinitions" || got[1].resource != "widgets" || !got[1].servedThen {
		t.Errorf("the applies were %+v, want the CRD, then the Widget with the endpoint serving", got)
	}
}

// A CRD that is not Established holds the wait for the kind too: the endpoint is asked only after the CRD's condition is set.
func TestApplyManifestsWaitsUntilTheCRDIsEstablished(t *testing.T) {
	r := newServedRig(t)
	r.established.Store(false)
	r.serve()
	done := r.startApply(widgetCRDManifest())
	awaitCount(t, r.crdReads.Load, 2, done, "its second read of the CRD")
	requireStillRunning(t, done, "while the CRD was not Established")
	if r.lists.Load() != 0 {
		t.Errorf("the kind was listed %d times before the CRD was Established", r.lists.Load())
	}

	r.established.Store(true)
	if res := requireReturns(t, done, "once the CRD was Established"); res.err != nil || res.n != 1 {
		t.Errorf("the apply ended with %d applied and %v, want the one CRD and no error", res.n, res.err)
	}
}

// A CRD that is already Established and served is no wait: the kind is listed once, and the object of the kind is applied after the CRD.
func TestApplyManifestsGoesStraightOnWhenTheKindIsServed(t *testing.T) {
	r := newServedRig(t)
	r.serve()
	res := requireReturns(t, r.startApply(widgetCRDManifest(), widgetObject()), "with everything served")
	if res.err != nil || res.n != 2 {
		t.Fatalf("the apply ended with %d applied and %v, want 2 and no error", res.n, res.err)
	}
	if n := r.lists.Load(); n != 1 {
		t.Errorf("the kind was listed %d times, want once: nothing was left to wait for", n)
	}
	if got := r.patched(); len(got) != 2 || got[0].resource != "customresourcedefinitions" || got[1].resource != "widgets" {
		t.Errorf("the applies were %+v, want the CRD, then the Widget", got)
	}
}

// A kind that is never served fails the wait after its bound with an error that names the CRD and the kind, and says which step was refused:
// discovery not listing it, or the endpoint not answering. The bound is short here: the test waits for the failure, not for a time.
func TestWaitServedFailsNamingTheCRDAndTheKind(t *testing.T) {
	const bound = 50 * time.Millisecond
	for _, c := range []struct {
		name       string
		discovered bool
		want       string
	}{
		{"discovery never lists the kind", false, "not in discovery"},
		{"the endpoint never answers", true, "not served"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newServedRig(t)
			r.mapper.listed.Store(c.discovered)
			err := waitServed(context.Background(), r.dyn, r.mapper, []*unstructured.Unstructured{widgetCRDManifest()}, bound, time.Millisecond)
			if err == nil {
				t.Fatal("a kind that is never served passed")
			}
			for _, want := range []string{"widgets.example.invalid", "kind Widget", c.want, "within " + bound.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the error %q does not say %q", err, want)
				}
			}
		})
	}
}

// A CRD that names no served version has no kind to wait for: the error names it, and nothing is waited for in vain.
func TestWaitServedRefusesACRDThatServesNoVersion(t *testing.T) {
	crd := widgetCRDManifest()
	_ = unstructured.SetNestedSlice(crd.Object, []any{map[string]any{"name": "v1", "served": false, "storage": true}}, "spec", "versions")
	r := newServedRig(t)
	err := waitServed(context.Background(), r.dyn, r.mapper, []*unstructured.Unstructured{crd}, failGuard, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "widgets.example.invalid") || !strings.Contains(err.Error(), "served version") {
		t.Errorf("a CRD that serves no version: %v, want an error naming it and the served version", err)
	}
	if r.mapper.resets.Load() != 0 {
		t.Error("the wait ran a round for a CRD with no kind to wait for")
	}
}

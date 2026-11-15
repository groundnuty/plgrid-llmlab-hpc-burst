package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// waitCRDsServed returns once every CRD in crds is Established and a list of its
// kind answers, or fails after limit with an error that names the first CRD or
// kind that is not served yet. Creating a CRD returns before the API server
// serves it: a rig that creates an object of one of the kinds right away can meet
// "no matches for kind" while discovery catches up, and the more CRDs a rig
// installs, the wider that window (#589, #593's CI: PlatformProfile). An
// Established condition alone is not enough either, so the wait also lists each
// kind, as api/v1alpha1/e2e's installCRDs does.
func waitCRDsServed(ctx context.Context, cl client.Client, crds []*apiextensionsv1.CustomResourceDefinition, limit, every time.Duration) error {
	last := "no CRD checked yet"
	check := func(ctx context.Context) (bool, error) {
		for _, want := range crds {
			got := &apiextensionsv1.CustomResourceDefinition{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(want), got); err != nil {
				last = fmt.Sprintf("CRD %s cannot be read: %v", want.Name, err)
				return false, nil
			}
			if !crdEstablished(got) {
				last = fmt.Sprintf("CRD %s is not Established", want.Name)
				return false, nil
			}
		}
		// Established is not served: list each kind until the API answers.
		for _, want := range crds {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group: want.Spec.Group, Version: servedVersion(want), Kind: want.Spec.Names.ListKind,
			})
			if err := cl.List(ctx, list); err != nil {
				last = fmt.Sprintf("kind %s of CRD %s is not served: %v", want.Spec.Names.Kind, want.Name, err)
				return false, nil
			}
		}
		return true, nil
	}
	if err := wait.PollUntilContextTimeout(ctx, every, limit, true, check); err != nil {
		return fmt.Errorf("the CRDs were not served within %s: %s", limit, last)
	}
	return nil
}

func crdEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}
	return false
}

// servedVersion is the version of crd that the API server serves.
func servedVersion(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, v := range crd.Spec.Versions {
		if v.Served {
			return v.Name
		}
	}
	return crd.Spec.Versions[0].Name
}

// widgetCRD is a CRD of an example kind for the tests of the wait. It is not
// Established until a test says so.
func widgetCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.example.invalid"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "example.invalid",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "Widget", ListKind: "WidgetList", Plural: "widgets", Singular: "widget"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true},
			},
		},
	}
}

func establish(crd *apiextensionsv1.CustomResourceDefinition) {
	crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{
		{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
	}
}

func crdTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// failGuard bounds how long a test of the wait lets a wrong helper hang: a
// failure guard only. The tests order their steps by channels, never by a sleep.
const failGuard = 60 * time.Second

// pollCounter closes a channel when a call is seen for the nth time.
type pollCounter struct {
	n    atomic.Int32
	at   int32
	seen chan struct{}
}

func newPollCounter(at int32) *pollCounter {
	return &pollCounter{at: at, seen: make(chan struct{})}
}

func (p *pollCounter) hit() {
	if p.n.Add(1) == p.at {
		close(p.seen)
	}
}

// startWait runs waitCRDsServed in a goroutine and returns its result channel.
func startWait(cl client.Client, crds []*apiextensionsv1.CustomResourceDefinition, limit time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() { done <- waitCRDsServed(context.Background(), cl, crds, limit, time.Millisecond) }()
	return done
}

// awaitPolls blocks until the counter has seen its call, or fails if the helper
// returned first: a helper that returns before it has looked at the CRD (the
// mutant "return at once") is caught here.
func awaitPolls(t *testing.T, p *pollCounter, done <-chan error, what string) {
	t.Helper()
	select {
	case <-p.seen:
	case err := <-done:
		t.Fatalf("the wait returned (%v) before %s", err, what)
	case <-time.After(failGuard):
		t.Fatalf("the wait never reached %s", what)
	}
}

func requireStillWaiting(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("the wait returned (%v) %s", err, what)
	default:
	}
}

func requireReturns(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(failGuard):
		t.Fatalf("the wait did not return %s", what)
	}
	return nil
}

// A CRD that is created but not Established yet holds the wait: it has polled
// the CRD at least twice, and still not returned, before the test establishes
// it; once it is Established, the wait lists the kind, and only then returns nil.
// The steps are ordered by the helper's own reads, counted in an interceptor.
func TestWaitCRDsServedWaitsUntilTheCRDIsEstablished(t *testing.T) {
	crd := widgetCRD()
	gets := newPollCounter(2)
	var listsBeforeEstablished, lists atomic.Int32
	established := make(chan struct{})
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(crd).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			err := c.Get(ctx, key, obj, opts...)
			gets.hit()
			return err
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			lists.Add(1)
			select {
			case <-established:
			default:
				listsBeforeEstablished.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
	}).Build()

	done := startWait(cl, []*apiextensionsv1.CustomResourceDefinition{crd}, failGuard)
	awaitPolls(t, gets, done, "its second read of the CRD")
	requireStillWaiting(t, done, "while the CRD was not Established")

	stored := &apiextensionsv1.CustomResourceDefinition{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(crd), stored); err != nil {
		t.Fatal(err)
	}
	establish(stored)
	close(established)
	// The fake client, like the API server, takes a CRD's status only through the
	// status subresource.
	if err := cl.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := requireReturns(t, done, "after the CRD was Established"); err != nil {
		t.Errorf("the wait failed after the CRD was Established: %v", err)
	}
	if n := listsBeforeEstablished.Load(); n != 0 {
		t.Errorf("the wait listed the kind %d times before the CRD was Established", n)
	}
	if lists.Load() == 0 {
		t.Error("the wait never listed the kind: Established alone is not served")
	}
}

// A CRD that is Established but whose kind the API server does not serve yet
// holds the wait too: a list of the kind fails with "no matches for kind", as
// the CI run did, until the test lets it answer.
func TestWaitCRDsServedWaitsUntilTheKindAnswers(t *testing.T) {
	crd := widgetCRD()
	establish(crd)
	lists := newPollCounter(2)
	var serve atomic.Bool
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(crd).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if !serve.Load() {
				lists.hit()
				return &apimeta.NoKindMatchError{
					GroupKind:        schema.GroupKind{Group: "example.invalid", Kind: "Widget"},
					SearchedVersions: []string{"v1"},
				}
			}
			return c.List(ctx, list, opts...)
		},
	}).Build()

	done := startWait(cl, []*apiextensionsv1.CustomResourceDefinition{crd}, failGuard)
	awaitPolls(t, lists, done, "its second refused list of the kind")
	requireStillWaiting(t, done, "while the kind was not served")

	serve.Store(true)
	if err := requireReturns(t, done, "once the kind answered"); err != nil {
		t.Errorf("the wait failed once the kind answered: %v", err)
	}
}

// A CRD that is already Established and served is no wait at all, and several
// CRDs are all waited for: the wait returns nil only when the last one is served.
func TestWaitCRDsServedReturnsAtOnceWhenEverythingIsServed(t *testing.T) {
	first, second := widgetCRD(), widgetCRD()
	second.Name = "gadgets.example.invalid"
	second.Spec.Names = apiextensionsv1.CustomResourceDefinitionNames{Kind: "Gadget", ListKind: "GadgetList", Plural: "gadgets", Singular: "gadget"}
	establish(first)
	establish(second)
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(first, second).Build()
	if err := waitCRDsServed(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{first, second}, failGuard, time.Millisecond); err != nil {
		t.Errorf("two served CRDs: %v", err)
	}
}

// A CRD that is never Established fails the wait after its bound, with an error
// that names the CRD; so does a kind that is never served, and a CRD that does
// not exist. The bound is short here: the test waits for the failure, not for a
// time.
func TestWaitCRDsServedFailsNamingWhatIsNotServed(t *testing.T) {
	const bound = 50 * time.Millisecond

	notEstablished := widgetCRD()
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(notEstablished).Build()
	err := waitCRDsServed(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{notEstablished}, bound, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "widgets.example.invalid") || !strings.Contains(err.Error(), "not Established") {
		t.Errorf("a CRD that is never Established: %v, want an error naming widgets.example.invalid and not Established", err)
	}

	unserved := widgetCRD()
	establish(unserved)
	cl = fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(unserved).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return &apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "example.invalid", Kind: "Widget"}, SearchedVersions: []string{"v1"}}
		},
	}).Build()
	err = waitCRDsServed(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{unserved}, bound, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "kind Widget") || !strings.Contains(err.Error(), "widgets.example.invalid") {
		t.Errorf("a kind that is never served: %v, want an error naming kind Widget and its CRD", err)
	}

	missing := widgetCRD()
	cl = fake.NewClientBuilder().WithScheme(crdTestScheme(t)).Build()
	err = waitCRDsServed(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{missing}, bound, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "widgets.example.invalid") {
		t.Errorf("a CRD that does not exist: %v, want an error naming widgets.example.invalid", err)
	}
}

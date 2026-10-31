package agentserver

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func allocationObject(ns, name, uid string, annotations map[string]string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid), Annotations: annotations}}
}

func lookupOver(t *testing.T, objs ...*v1alpha1.Allocation) AllocationLookup {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	b := clientfake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return NewClientLookup(b.Build())
}

func TestClientLookupFindsTheAllocationByExternalNameWithItsJobID(t *testing.T) {
	l := lookupOver(t,
		allocationObject("ns-1", "other", "uid-other", map[string]string{v1alpha1.AnnotationExternalName: "tag-other", v1alpha1.AnnotationJobID: "job-other"}),
		allocationObject("ns-2", "mine", "uid-mine", map[string]string{v1alpha1.AnnotationExternalName: "tag-mine", v1alpha1.AnnotationJobID: "job-42"}),
	)
	got, err := l.ByTag(context.Background(), "tag-mine")
	if err != nil {
		t.Fatal(err)
	}
	if want := (Allocation{Namespace: "ns-2", Name: "mine", UID: "uid-mine", JobID: "job-42"}); got != want {
		t.Errorf("ByTag = %+v, want %+v (a tag is found in any namespace)", got, want)
	}
}

func TestClientLookupGivesAnEmptyJobIDBeforeOneIsRecorded(t *testing.T) {
	l := lookupOver(t, allocationObject("ns-1", "fresh", "uid-fresh", map[string]string{v1alpha1.AnnotationExternalName: "tag-fresh"}))
	got, err := l.ByTag(context.Background(), "tag-fresh")
	if err != nil {
		t.Fatal(err)
	}
	if got.JobID != "" || got.UID != "uid-fresh" {
		t.Errorf("ByTag = %+v, want UID uid-fresh and no job ID", got)
	}
}

func TestClientLookupOfAnUnknownTagIsErrNotFound(t *testing.T) {
	l := lookupOver(t, allocationObject("ns-1", "a", "uid-a", map[string]string{v1alpha1.AnnotationExternalName: "tag-a"}))
	if _, err := l.ByTag(context.Background(), "tag-unknown"); !errors.Is(err, ErrNotFound) {
		t.Errorf("ByTag = %v, want ErrNotFound", err)
	}
}

// An Allocation that has no tag yet is not found by the empty tag, which an
// agent with no tag header would present.
func TestClientLookupOfTheEmptyTagIsErrNotFoundEvenWhenAnAllocationHasNoTag(t *testing.T) {
	l := lookupOver(t,
		allocationObject("ns-1", "untagged", "uid-untagged", nil),
		allocationObject("ns-1", "blank", "uid-blank", map[string]string{v1alpha1.AnnotationExternalName: ""}),
	)
	if got, err := l.ByTag(context.Background(), ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("ByTag(\"\") = %+v, %v, want ErrNotFound", got, err)
	}
}

func TestClientLookupOfATagTwoAllocationsShareIsAmbiguous(t *testing.T) {
	tag := map[string]string{v1alpha1.AnnotationExternalName: "tag-dup"}
	l := lookupOver(t,
		allocationObject("ns-1", "first", "uid-1", tag),
		allocationObject("ns-2", "second", "uid-2", tag),
	)
	got, err := l.ByTag(context.Background(), "tag-dup")
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("ByTag = %+v, %v, want ErrAmbiguous", got, err)
	}
	if got != (Allocation{}) {
		t.Errorf("ByTag returned %+v with the error, want none", got)
	}
}

func TestClientLookupPassesOnAListFailureAsAnErrorThatIsNeitherNotFoundNorAmbiguous(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("the cache is not synced")
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return boom },
	}).Build()

	_, err := NewClientLookup(c).ByTag(context.Background(), "tag-a")
	if !errors.Is(err, boom) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrAmbiguous) {
		t.Errorf("ByTag = %v, want an error that wraps the list failure only", err)
	}
}

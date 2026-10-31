package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

const (
	annoCreateSucceeded = "crossplane.io/external-create-succeeded"
	createSucceededAt   = "2026-10-30T09:00:05Z"
)

// conflictCounter wraps Update so a test can see that a conflict really
// happened: the bad path of crossplane-runtime's updater runs only then.
type conflictCounter struct{ conflicts, updates int }

func (c *conflictCounter) funcs() interceptor.Funcs {
	return interceptor.Funcs{Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		err := cl.Update(ctx, obj, opts...)
		c.updates++
		if kerrors.IsConflict(err) {
			c.conflicts++
		}
		return err
	}}
}

// storedAndStale returns a fake API server that holds the newest record and a
// stale in-memory copy of the same Allocation: an older resourceVersion, the
// older record, and the create-succeeded annotation that is waiting to be
// persisted.
func storedAndStale(t *testing.T) (client.Client, *conflictCounter, *v1alpha1.Allocation) {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	stored := allocation(map[string]string{
		"crossplane.io/external-name": wantTag,
		AnnoSubmission:                inFlight, // n=2
		AnnoJobID:                     "1001",
	})
	cnt := &conflictCounter{}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(stored).WithInterceptorFuncs(cnt.funcs()).Build()

	current := &v1alpha1.Allocation{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stored), current); err != nil {
		t.Fatal(err)
	}
	stale := allocation(map[string]string{
		"crossplane.io/external-name": wantTag,
		AnnoSubmission:                rejected1, // n=1
		annoCreateSucceeded:           createSucceededAt,
	})
	stale.ResourceVersion = "1" // not the server's
	if stale.ResourceVersion == current.ResourceVersion {
		t.Fatalf("fixture: the stale copy has the server's resourceVersion %q", current.ResourceVersion)
	}
	return c, cnt, stale
}

// storedAttempt reads the n and state of the submission record the server holds,
// parsing the JSON directly rather than through the code under test.
func storedAttempt(t *testing.T, c client.Client, key client.ObjectKey) (n int, state string, an map[string]string) {
	t.Helper()
	got := &v1alpha1.Allocation{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	var rec struct {
		N     int    `json:"n"`
		State string `json:"state"`
	}
	if err := json.Unmarshal([]byte(got.Annotations[AnnoSubmission]), &rec); err != nil {
		t.Fatalf("stored submission annotation %q: %v", got.Annotations[AnnoSubmission], err)
	}
	return rec.N, rec.State, got.Annotations
}

// A conflict on the update must not roll the submission record back. The
// control is crossplane-runtime's own updater, which re-applies every
// in-memory annotation after a conflict (api.go:283-301) and so does.
func TestCriticalAnnotationUpdaterKeepsNewerRecordOnConflict(t *testing.T) {
	cases := []struct {
		name      string
		updater   func(client.Client) managed.CriticalAnnotationUpdater
		wantN     int
		wantState string
	}{
		{"provider updater", func(c client.Client) managed.CriticalAnnotationUpdater {
			return CriticalAnnotationUpdater{Kube: c, Reader: c}
		}, 2, "InFlight"},
		{"control: crossplane-runtime updater", func(c client.Client) managed.CriticalAnnotationUpdater {
			return managed.NewRetryingCriticalAnnotationUpdater(c)
		}, 1, "Rejected"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, cnt, stale := storedAndStale(t)
			if err := tc.updater(c).UpdateCriticalAnnotations(context.Background(), stale); err != nil {
				t.Fatal(err)
			}
			if cnt.conflicts < 1 {
				t.Fatalf("no conflict occurred (%d updates): the test exercises nothing", cnt.updates)
			}
			n, state, an := storedAttempt(t, c, client.ObjectKeyFromObject(stale))
			if n != tc.wantN || state != tc.wantState {
				t.Errorf("stored submission record n=%d state=%s, want n=%d state=%s", n, state, tc.wantN, tc.wantState)
			}
			if an[annoCreateSucceeded] != createSucceededAt {
				t.Errorf("stored %s = %q, want %q", annoCreateSucceeded, an[annoCreateSucceeded], createSucceededAt)
			}
			if an[AnnoJobID] != "1001" {
				t.Errorf("stored job-id = %q, want 1001 untouched", an[AnnoJobID])
			}
		})
	}
}

// After the conflict the reconciler carries on with the in-memory object, so
// it must show the server's record: a later write built from a stale record
// would roll the server back.
func TestCriticalAnnotationUpdaterRefreshesTheInMemoryObject(t *testing.T) {
	c, cnt, stale := storedAndStale(t)
	if err := (CriticalAnnotationUpdater{Kube: c, Reader: c}).UpdateCriticalAnnotations(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if cnt.conflicts < 1 {
		t.Fatal("no conflict occurred: the test exercises nothing")
	}
	var rec struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal([]byte(stale.Annotations[AnnoSubmission]), &rec); err != nil || rec.N != 2 {
		t.Errorf("in-memory submission record %q (err %v), want n=2", stale.Annotations[AnnoSubmission], err)
	}
	if stale.Annotations[AnnoJobID] != "1001" {
		t.Errorf("in-memory job-id %q, want the server's 1001", stale.Annotations[AnnoJobID])
	}
}

// Without a conflict the update is a plain write that persists the annotation.
func TestCriticalAnnotationUpdaterPersistsWithoutConflict(t *testing.T) {
	c, cnt, _ := storedAndStale(t)
	o := &v1alpha1.Allocation{}
	key := client.ObjectKey{Namespace: "ns", Name: "alloc"}
	if err := c.Get(context.Background(), key, o); err != nil {
		t.Fatal(err)
	}
	o.SetAnnotations(mergeAnno(o.GetAnnotations(), "crossplane.io/external-create-pending", "2026-10-30T09:00:04Z"))
	if err := (CriticalAnnotationUpdater{Kube: c, Reader: c}).UpdateCriticalAnnotations(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if cnt.conflicts != 0 {
		t.Errorf("%d conflicts on a current copy", cnt.conflicts)
	}
	_, _, an := storedAttempt(t, c, key)
	if an["crossplane.io/external-create-pending"] != "2026-10-30T09:00:04Z" {
		t.Errorf("create-pending not persisted: %v", an)
	}
}

func mergeAnno(an map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for kk, vv := range an {
		out[kk] = vv
	}
	return out
}

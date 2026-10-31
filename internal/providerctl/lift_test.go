package providerctl

// The hold lift's guard and its marker (review of PR 76: Astra B8, Opus N8).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// HBEP-7's closed list names reason JobHeldUser. provider.HoldOf calls a
// held job, or a reason starting JobHeld, a user hold, so the hold kind alone
// is broader than the list.
func TestLiftableNeedsReasonJobHeldUser(t *testing.T) {
	start := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	base := provider.Job{ID: "7", Hold: provider.HoldUser, Reason: "JobHeldUser"}
	for _, tc := range []struct {
		name string
		mod  func(*provider.Job, *record)
		want bool
	}{
		{"held by its user", func(*provider.Job, *record) {}, true},
		{"another JobHeld reason", func(j *provider.Job, _ *record) { j.Reason = "JobHeldUserPending" }, false},
		{"held, other reason", func(j *provider.Job, _ *record) { j.Reason = "BeginTime" }, false},
		{"admin hold", func(j *provider.Job, _ *record) { j.Hold, j.Reason = provider.HoldAdmin, "JobHeldAdmin" }, false},
		{"restarted", func(j *provider.Job, _ *record) { j.RestartCount = 1 }, false},
		{"started", func(j *provider.Job, _ *record) { j.StartTime = &start }, false},
		{"marker names it", func(_ *provider.Job, r *record) { r.lifted = "7" }, false},
	} {
		j, r := base, record{}
		tc.mod(&j, &r)
		if got := liftable(&r, j); got != tc.want {
			t.Errorf("%s: liftable %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Opus N8: the marker write after a lift meets a conflict (another writer
// changed the Allocation since it was read). It re-reads and writes again, so
// the marker names the job; without the retry it would be missing for good,
// since a released job is never lifted again.
func TestHoldLiftedMarkerSurvivesAConflict(t *testing.T) {
	e := newEnv(t)
	armed := &atomic.Bool{}
	armed.Store(true)
	e.c = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if obj.GetAnnotations()[v1alpha1.AnnotationHoldLifted] != "" && armed.CompareAndSwap(true, false) {
				return kerrors.NewConflict(schema.GroupResource{Group: v1alpha1.Group, Resource: "allocations"}, obj.GetName(), nil)
			}
			return w.Update(ctx, obj, opts...)
		},
	})
	e.build()
	e.newAlloc("a")
	e.run("a", 1)
	if armed.Load() {
		t.Fatal("precondition: no marker write met the conflict")
	}
	if n := e.lifts(); n != 1 {
		t.Fatalf("precondition: %d lifts, want 1", n)
	}
	if got := e.get("a").Annotations[v1alpha1.AnnotationHoldLifted]; got != ptFirstJob {
		t.Errorf("hold-lifted %q after a conflict on its write, want %s", got, ptFirstJob)
	}
}

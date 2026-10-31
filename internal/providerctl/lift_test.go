package providerctl

// The hold lift's guard and its marker (review of PR 76: Astra B8, Opus N8).

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
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

// releaseCalls counts the "scontrol release <id>" lines the fake login node
// received, from its call log. A release that the fake refused is still a
// call; one that failed before it reached the node (lineFaults) is not.
func releaseCalls(sl *fakeslurm.Platform, id string) int {
	n := 0
	for _, c := range sl.Calls() {
		if len(c.Words) == 3 && c.Words[0] == "scontrol" && c.Words[1] == "release" && c.Words[2] == id {
			n++
		}
	}
	return n
}

// Issue 93 (follow-up to Astra B8): liftRecorded re-reads the job under the
// per-job lock and releases it only when that re-read is liftable, not the
// view its caller passed in. The caller's view here is liftable; between that
// view and the lift the job changes under it, and the evidence is the number
// of "scontrol release" calls the fake login node logged: 0. Two changes, each
// caught by a different guard: a requeue puts the job on a user hold again with
// restart count 1 (reason and hold kind still read as liftable; only the
// restart count tells), and an admin hold (the hold kind tells). The control
// case changes nothing and must make exactly 1 call, so a 0 above is not an
// artefact of the setup (a marker already set, a job not held, a release that
// never reaches the node).
func TestLiftRereadsTheJobBeforeReleasing(t *testing.T) {
	for _, tc := range []struct {
		name         string
		change       func(*fakeslurm.Platform, int) error // nil: the job stays as the caller saw it
		wantReleases int
		wantRestarts int32
		wantHold     provider.HoldKind
	}{
		{"unchanged job is released once", nil, 1, 0, provider.HoldUser},
		{"requeued and held again", (*fakeslurm.Platform).RequeueHold, 0, 1, provider.HoldUser},
		{"admin hold", (*fakeslurm.Platform).HoldAdmin, 0, 0, provider.HoldAdmin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			f := &lineFaults{}
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f))
			// The first lift fails before it reaches the node: the job stays
			// held and unmarked, as it would after a crash between the two.
			f.fail("scontrol release", 1, slurm.ErrTransport)
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 1)
			a := e.get("alloc-a")
			id := a.Annotations[v1alpha1.AnnotationJobID]
			n, err := strconv.Atoi(id)
			if err != nil {
				t.Fatalf("precondition: job ID %q: %v", id, err)
			}
			if j := slurmJob(t, sl, id); !j.Held || a.Annotations[v1alpha1.AnnotationHoldLifted] != "" || f.failed() != 1 || releaseCalls(sl, id) != 0 {
				t.Fatalf("precondition: job held %v, marker %q, %d releases failed, %d reached the node; want held, no marker, 1, 0",
					j.Held, a.Annotations[v1alpha1.AnnotationHoldLifted], f.failed(), releaseCalls(sl, id))
			}
			ext, err := (&connector{s: e.ctl.shared}).Connect(e.ctx, a)
			if err != nil {
				t.Fatal(err)
			}
			x := ext.(*external)
			rec, _ := readRecord(a)
			stale, found, err := x.p.Observe(e.ctx, id)
			if err != nil || !found || !liftable(&rec, stale) {
				t.Fatalf("precondition: the caller's view %+v (found %v, err %v) is not liftable", stale, found, err)
			}
			if tc.change != nil {
				if err := tc.change(sl, n); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(15 * time.Second)
			fresh, found, err := x.p.Observe(e.ctx, id)
			if err != nil || !found || fresh.RestartCount != tc.wantRestarts || fresh.Hold != tc.wantHold || liftable(&rec, fresh) != (tc.wantReleases == 1) {
				t.Fatalf("precondition: the re-read %+v (found %v, err %v) has restart count %d, hold %v, liftable %v; want %d, %v, %v",
					fresh, found, err, fresh.RestartCount, fresh.Hold, liftable(&rec, fresh), tc.wantRestarts, tc.wantHold, tc.wantReleases == 1)
			}
			lifted, err := x.liftRecorded(e.ctx, a, &rec, stale)
			if err != nil {
				t.Fatalf("liftRecorded: %v", err)
			}
			if got := releaseCalls(sl, id); got != tc.wantReleases {
				t.Errorf("%d \"scontrol release\" calls reached the login node, want %d", got, tc.wantReleases)
			}
			if lifted != (tc.wantReleases == 1) {
				t.Errorf("liftRecorded reported lifted %v, want %v", lifted, tc.wantReleases == 1)
			}
			wantMarker := ""
			if tc.wantReleases == 1 {
				wantMarker = id
			}
			if got := e.get("alloc-a").Annotations[v1alpha1.AnnotationHoldLifted]; got != wantMarker {
				t.Errorf("hold-lifted marker %q, want %q", got, wantMarker)
			}
		})
	}
}

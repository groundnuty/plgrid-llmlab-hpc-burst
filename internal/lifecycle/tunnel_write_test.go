package lifecycle

// The TunnelConnected writer and the reconciler write the same Instance's status (a
// science-agent ruling of 1 Nov 2026, for #90). Both are a read-modify-write
// Status().Update that carries the resourceVersion of the copy it read, so whichever writes
// second gets a Conflict and neither loses the other's conditions. A JSON merge patch would
// break that on a CRD, where it replaces the whole conditions list with the writer's possibly
// stale copy.
//
// The test drives the two real write paths against each other: WriteTunnelConnected (what
// cmd/hpcburst-controller's writer calls) and pass.setStatus (every status write of the
// reconciler goes through it). A client interceptor holds the first status write of one side
// at the call, with its copy already read, until the test has made the other side's write, so
// the interference is forced, in both directions, and not left to scheduling. The
// controller-runtime fake client enforces the resourceVersion on a status Update as the API
// server does, and answers a merge patch with no check, as the API server does.

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// barrier holds the first status write that goes through its client, whichever verb it uses,
// at the call until the test releases it, and records how each status write ended.
type barrier struct {
	gated   bool
	arrived chan struct{} // closed when the held write has reached the client
	release chan struct{}
	once    sync.Once

	mu      sync.Mutex
	results []string // one entry per status write: the verb and how it ended
}

func newBarrier(gated bool) *barrier {
	return &barrier{gated: gated, arrived: make(chan struct{}), release: make(chan struct{})}
}

func (b *barrier) hold() {
	if !b.gated {
		return
	}
	first := false
	b.once.Do(func() { first = true })
	if !first {
		return
	}
	close(b.arrived)
	select {
	case <-b.release:
	case <-time.After(30 * time.Second): // the test failed and left: do not leak the goroutine
	}
}

func (b *barrier) record(verb string, err error) {
	out := "ok"
	switch {
	case apierrors.IsConflict(err):
		out = "conflict"
	case err != nil:
		out = "error: " + err.Error()
	}
	b.mu.Lock()
	b.results = append(b.results, verb+" "+out)
	b.mu.Unlock()
}

func (b *barrier) outcomes() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.results)
}

func (b *barrier) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			b.hold()
			err := c.SubResource(sub).Update(ctx, obj, opts...)
			b.record("update", err)
			return err
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			b.hold()
			err := c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			b.record("patch", err)
			return err
		},
	}
}

// waitArrived fails the test when the held write never reaches its client: the writer under test
// did not write through a status update or patch at all.
func (b *barrier) waitArrived(t *testing.T, who string) {
	t.Helper()
	select {
	case <-b.arrived:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s never reached its status write", who)
	}
}

// interleaveStore is one stored Instance and a client for each side, over one fake API.
type interleaveStore struct {
	t    *testing.T
	base client.WithWatch
	key  client.ObjectKey
}

func newInterleaveStore(t *testing.T) *interleaveStore {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	seed := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "in-interleave", UID: types.UID("00000000-0000-4000-8000-0000000000aa"), Generation: 2}}
	seed.Status.Phase = v1alpha1.InstanceVerifying
	seed.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionVerified, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPassed, Message: "seeded", LastTransitionTime: metav1.Now()}}
	base := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).WithStatusSubresource(&v1alpha1.Instance{}).Build()
	return &interleaveStore{t: t, base: base, key: client.ObjectKeyFromObject(seed)}
}

func (s *interleaveStore) get() *v1alpha1.Instance {
	s.t.Helper()
	in := &v1alpha1.Instance{}
	if err := s.base.Get(context.Background(), s.key, in); err != nil {
		s.t.Fatal(err)
	}
	return in
}

// side is a client whose status writes go through b.
func (s *interleaveStore) side(b *barrier) client.Client {
	return interceptor.NewClient(s.base, b.funcs())
}

var (
	tunnelOnline  = metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOnline, Message: "frps lists the Instance's proxy as online"}
	tunnelOffline = metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProxyOffline, Message: "frps does not list a proxy for the Instance"}
)

// reconcilerWrite is what a pass does to the Instance's status through the real setStatus: it
// sets Healthy and the phase, and a message that tells which write it is.
func reconcilerWrite(ctx context.Context, c client.Client, in *v1alpha1.Instance, message string) error {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p := &pass{r: &Reconciler{Client: c}, ctx: ctx, now: now}
	return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Phase = v1alpha1.InstanceReady
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionTrue, v1alpha1.ReasonHealthy, message, in.Generation, metav1.NewTime(now))
	})
}

// checkBoth asserts the stored Instance holds the reconciler's write and the tunnel's.
func (s *interleaveStore) checkBoth(want metav1.Condition, healthyMessage string) {
	s.t.Helper()
	got := s.get()
	if tc := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionTunnelConnected); tc == nil || tc.Status != want.Status || tc.Reason != want.Reason {
		s.t.Errorf("stored TunnelConnected = %+v, want the writer's %s/%s: its write was lost", tc, want.Status, want.Reason)
	}
	if h := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionHealthy); h == nil || h.Message != healthyMessage {
		s.t.Errorf("stored Healthy = %+v, want the reconciler's message %q: its write was lost", h, healthyMessage)
	}
	if got.Status.Phase != v1alpha1.InstanceReady {
		s.t.Errorf("stored phase %q, want the reconciler's ready: its write was lost", got.Status.Phase)
	}
	if v := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionVerified); v == nil || v.Message != "seeded" {
		s.t.Errorf("stored Verified = %+v, want the seeded one kept", v)
	}
}

// TestTunnelConnectedAndReconcilerWritesInterfereBothWays covers the shared status: whichever of
// the TunnelConnected writer and the reconciler writes second gets a Conflict, and nothing of
// the first's write is lost.
//
//   - Writer first: the writer has read the Instance, the reconciler writes (through the real
//     setStatus), then the writer writes its stale copy. It must get a Conflict, read the
//     Instance again and write again: both conditions stored.
//   - Reconciler first: the reconciler has read the Instance, the writer writes, then the
//     reconciler writes its stale copy. setStatus must fail with a Conflict (the pass fails and
//     the key is requeued, HBEP-7 open question 11), and the stored status keeps the writer's
//     condition; the requeue's pass then writes, over a fresh read, and both are stored.
//
// The interference is forced by a barrier at the first status write, never left to timing, and
// the order of outcomes is asserted, so a conflict count that two sequential writers could also
// produce proves nothing here. A write that carries no resourceVersion, or goes as a merge
// patch, fails both halves: the first by losing the reconciler's write, the second by losing
// the writer's.
func TestTunnelConnectedAndReconcilerWritesInterfereBothWays(t *testing.T) {
	ctx := context.Background()

	t.Run("the writer reads first, the reconciler writes, the writer conflicts and retries", func(t *testing.T) {
		st := newInterleaveStore(t)
		tunnelB, lifeB := newBarrier(true), newBarrier(false)
		tunnelC, lifeC := st.side(tunnelB), st.side(lifeB)

		stale := st.get() // the writer's copy, read before the reconciler's write
		done := make(chan error, 1)
		go func() { done <- WriteTunnelConnected(ctx, tunnelC, st.base, stale, tunnelOnline) }()
		tunnelB.waitArrived(t, "the TunnelConnected writer")

		// The writer holds its stale copy at the call. The reconciler writes now, through
		// the real setStatus, over a fresh read.
		if err := reconcilerWrite(ctx, lifeC, st.get(), "written between the writer's read and its write"); err != nil {
			t.Fatalf("the reconciler's write: %v", err)
		}
		close(tunnelB.release)
		if err := <-done; err != nil {
			t.Fatalf("WriteTunnelConnected: %v", err)
		}

		if got, want := tunnelB.outcomes(), []string{"update conflict", "update ok"}; !slices.Equal(got, want) {
			t.Fatalf("the writer's status writes ended %v, want %v: a Conflict on the stale copy, then the write over a fresh read", got, want)
		}
		st.checkBoth(tunnelOnline, "written between the writer's read and its write")
	})

	t.Run("the reconciler reads first, the writer writes, the reconciler conflicts and is requeued", func(t *testing.T) {
		st := newInterleaveStore(t)
		tunnelB, lifeB := newBarrier(false), newBarrier(true)
		tunnelC, lifeC := st.side(tunnelB), st.side(lifeB)

		stale := st.get() // the reconciler's copy, read before the writer's write
		done := make(chan error, 1)
		go func() { done <- reconcilerWrite(ctx, lifeC, stale, "the reconciler's write") }()
		lifeB.waitArrived(t, "the reconciler")

		if err := WriteTunnelConnected(ctx, tunnelC, st.base, st.get(), tunnelOffline); err != nil {
			t.Fatalf("WriteTunnelConnected: %v", err)
		}
		close(lifeB.release)
		err := <-done
		if !apierrors.IsConflict(err) {
			t.Fatalf("setStatus on a copy the writer has written since: %v, want a Conflict that fails the pass", err)
		}
		if got, want := lifeB.outcomes(), []string{"update conflict"}; !slices.Equal(got, want) {
			t.Fatalf("the reconciler's status writes ended %v, want %v", got, want)
		}
		// The stale write changed nothing: the writer's condition is there, the reconciler's is not.
		mid := st.get()
		if tc := apimeta.FindStatusCondition(mid.Status.Conditions, v1alpha1.ConditionTunnelConnected); tc == nil || tc.Reason != tunnelOffline.Reason {
			t.Fatalf("after the reconciler's conflict TunnelConnected = %+v, want the writer's %s", tc, tunnelOffline.Reason)
		}
		if h := apimeta.FindStatusCondition(mid.Status.Conditions, v1alpha1.ConditionHealthy); h != nil || mid.Status.Phase != v1alpha1.InstanceVerifying {
			t.Fatalf("after the reconciler's conflict Healthy = %+v, phase %q: the stale write must have changed nothing", h, mid.Status.Phase)
		}
		// The requeued pass reads again and writes.
		if err := reconcilerWrite(ctx, lifeC, st.get(), "the reconciler's write"); err != nil {
			t.Fatalf("the requeued pass's write: %v", err)
		}
		st.checkBoth(tunnelOffline, "the reconciler's write")
	})
}

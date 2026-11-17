package lifecycle

// The removal of finished Operations (#589, PR 5, AC 9): a finished Operation goes 7 days after status.finishedAt, by an
// injected clock and never a sleep; one that has not finished stays; the delete carries a UID precondition. The times are
// written out by hand.

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

var ttlFinished = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func finishedOp(name string, phase v1alpha1.OperationPhase, at *time.Time) *v1alpha1.Operation {
	op := opNamed(name, scaleOp(1))
	op.Namespace = opNS
	op.Status.Phase = phase
	if at != nil {
		op.Status.FinishedAt = opTime(*at)
	}
	return op
}

var errPrecondition = errors.New("Precondition failed: UID in precondition does not match UID in record")

type ttlRig struct {
	r     *operationTTLReconciler
	c     client.Client
	clock *pfake.Clock
	// deletes records the preconditions of every delete.
	deletes []*types.UID
}

func newTTLRig(t *testing.T, objs ...client.Object) *ttlRig {
	t.Helper()
	scheme, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	g := &ttlRig{clock: pfake.NewClock(ttlFinished)}
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		var o client.DeleteOptions
		o.ApplyOptions(opts)
		var uid *types.UID
		if o.Preconditions != nil {
			uid = o.Preconditions.UID
		}
		g.deletes = append(g.deletes, uid)
		// The API server refuses a delete whose UID precondition is not the stored object's UID with a Conflict; the fake
		// client does not check it, so the rig does.
		if uid != nil {
			var stored v1alpha1.Operation
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &stored); err == nil && stored.UID != *uid {
				return apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("operations").GroupResource(), obj.GetName(), errPrecondition)
			}
		}
		return c.Delete(ctx, obj, opts...)
	}}
	g.c = clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Operation{}).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	g.r = &operationTTLReconciler{Client: g.c, Reader: g.c, Clock: g.clock}
	return g
}

func (g *ttlRig) reconcile(t *testing.T, name string) ctrl.Result {
	t.Helper()
	res, err := g.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: opNS, Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (g *ttlRig) exists(name string) bool {
	var op v1alpha1.Operation
	err := g.c.Get(context.Background(), types.NamespacedName{Namespace: opNS, Name: name}, &op)
	return err == nil
}

// A Done, a Stale and a Refused Operation each go 7 days after they finished: not a minute before, at the minute. The
// requeue is for exactly the time left.
func TestAFinishedOperationGoesSevenDaysAfterItFinished(t *testing.T) {
	for _, phase := range []v1alpha1.OperationPhase{v1alpha1.OperationPhaseDone, v1alpha1.OperationPhaseStale, v1alpha1.OperationPhaseRefused} {
		t.Run(string(phase), func(t *testing.T) {
			g := newTTLRig(t, finishedOp("op", phase, &ttlFinished))
			// Written out by hand: finished 10 Nov 12:00, so it goes at 17 Nov 12:00.
			g.clock.Set(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
			if res := g.reconcile(t, "op"); res.RequeueAfter != 7*24*time.Hour || !g.exists("op") {
				t.Fatalf("at the finish: requeue %s, exists %v, want 168h and kept", res.RequeueAfter, g.exists("op"))
			}
			g.clock.Set(time.Date(2026, 11, 17, 11, 59, 0, 0, time.UTC))
			if res := g.reconcile(t, "op"); res.RequeueAfter != time.Minute || !g.exists("op") {
				t.Fatalf("a minute before: requeue %s, exists %v, want 1m and kept", res.RequeueAfter, g.exists("op"))
			}
			g.clock.Set(time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC))
			if res := g.reconcile(t, "op"); res.RequeueAfter != 0 || g.exists("op") {
				t.Fatalf("at 7 days: requeue %s, exists %v, want it gone", res.RequeueAfter, g.exists("op"))
			}
			if len(g.deletes) != 1 || g.deletes[0] == nil || *g.deletes[0] != "uid-op" {
				t.Errorf("the delete carried the preconditions %v, want the Operation's UID uid-op", g.deletes)
			}
		})
	}
}

// An Operation that has not finished is never removed, however old; one that is gone needs no pass.
func TestAnUnfinishedOperationIsNeverRemoved(t *testing.T) {
	for _, phase := range []v1alpha1.OperationPhase{"", v1alpha1.OperationPhasePlanned, v1alpha1.OperationPhaseAwaitingApproval, v1alpha1.OperationPhaseExecuting, v1alpha1.OperationPhasePartlyFailed} {
		g := newTTLRig(t, finishedOp("op", phase, nil))
		g.clock.Set(ttlFinished.Add(365 * 24 * time.Hour))
		if res := g.reconcile(t, "op"); res.RequeueAfter != 0 || !g.exists("op") || len(g.deletes) != 0 {
			t.Errorf("phase %q: requeue %s, exists %v, deletes %d, want it kept and no delete", phase, res.RequeueAfter, g.exists("op"), len(g.deletes))
		}
	}
	g := newTTLRig(t)
	if res := g.reconcile(t, "gone"); res.RequeueAfter != 0 {
		t.Errorf("an Operation that is gone: requeue %s", res.RequeueAfter)
	}
}

// An Operation made again under the same name, in the time between the read and the delete, is not the one removed: the
// delete carries the UID that was read, and the API server refuses another's with a Conflict, which is no error.
func TestTheDelCarriesTheUIDSoARecreatedOperationIsNotRemoved(t *testing.T) {
	old := finishedOp("op", v1alpha1.OperationPhaseDone, &ttlFinished)
	g := newTTLRig(t, old)
	g.clock.Set(ttlFinished.Add(8 * 24 * time.Hour))
	// The fake client enforces a UID precondition against the stored object. The reconciler reads the old one; the stored
	// object is replaced by one of the same name and another UID before its delete.
	read := g.r.Reader
	g.r.Reader = readThenReplace{Reader: read, replace: func() {
		var cur v1alpha1.Operation
		if err := g.c.Get(context.Background(), types.NamespacedName{Namespace: opNS, Name: "op"}, &cur); err != nil {
			t.Fatal(err)
		}
		if err := g.c.Delete(context.Background(), &cur); err != nil {
			t.Fatal(err)
		}
		fresh := finishedOp("op", "", nil)
		fresh.UID = "uid-new-op"
		fresh.ResourceVersion = ""
		if err := g.c.Create(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
	}}
	if res := g.reconcile(t, "op"); res.RequeueAfter != 0 {
		t.Fatalf("requeue %s", res.RequeueAfter)
	}
	var cur v1alpha1.Operation
	if err := g.c.Get(context.Background(), types.NamespacedName{Namespace: opNS, Name: "op"}, &cur); err != nil || cur.UID != "uid-new-op" {
		t.Errorf("the Operation made again is %v (%v): it must stay", cur.UID, err)
	}
}

// readThenReplace reads, then runs replace once: a write that lands between the reconciler's read and its delete.
type readThenReplace struct {
	client.Reader
	replace func()
}

func (r readThenReplace) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if r.replace != nil {
		r.replace()
	}
	return err
}

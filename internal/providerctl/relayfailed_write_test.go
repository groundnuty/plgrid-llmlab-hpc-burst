package providerctl

// Issue #304: the failure path's own write of RelayEndedForGood
// (persistRelayFailed), from the delta review of #319. It writes only for the
// relay's Allocation and job, only while the Allocation runs (an existing
// mark is kept, never cleared), only while this copy leads, and it keeps a
// concurrent writer's status across a conflict.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// runningAlloc makes an Allocation whose status is Running with job, as the
// Provider's controller records it.
func runningAlloc(t *testing.T, e *env, name, job string, mods ...func(*v1alpha1.Allocation)) *v1alpha1.Allocation {
	t.Helper()
	a := e.newAlloc(name)
	a.Status.AtProvider.Phase, a.Status.AtProvider.JobID = v1alpha1.AllocationRunning, job
	for _, m := range mods {
		m(a)
	}
	if err := e.c.Status().Update(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	return e.get(name)
}

// The checks of each attempt, with the patches counted at the store: the
// write refuses another UID, another job, and an Allocation that is ended,
// completing or being deleted, with no patch; it keeps a write that another
// writer commits between its read and its patch, by reading again after the
// conflict; it refuses a job that changed between attempts; it keeps a mark
// already there, on a running Allocation and on one that ended since; and it
// writes nothing once the lead is over.
func TestTheFailurePathsMarkChecksTheAllocationItsJobAndItsPhase(t *testing.T) {
	var patches atomic.Int32
	var beforePatch atomic.Pointer[func(ctx context.Context, c client.Client, obj client.Object)]
	e := newEnv(t, hostDialed, withInterceptors(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches.Add(1)
			if f := beforePatch.Swap(nil); f != nil {
				(*f)(ctx, c, obj)
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}))
	s := e.ctl.shared
	lead := context.Background()
	const msg = "the control relay ended for good: srun refused the step: step limit reached for this job"
	mark := func(name, uid, job, m string) error {
		return s.persistRelayFailed(lead, clientKey(name), relayFailure{uid: uid, jobID: job, msg: m, at: e.clock.Now()})
	}
	// refused checks that a mark was refused, with no patch and no condition.
	refused := func(t *testing.T, name string, err error) {
		t.Helper()
		if !errors.Is(err, errMarkRefused) {
			t.Errorf("the mark on %s: %v, want refused", name, err)
		}
		if st, _, m := relayCondition(e.get(name)); st != "" {
			t.Errorf("%s is marked %s, %q", name, st, m)
		}
	}
	noPatch := func(t *testing.T, before int32, what string) {
		t.Helper()
		if n := patches.Load() - before; n != 0 {
			t.Errorf("%d status patches %s, want 0", n, what)
		}
	}

	a := runningAlloc(t, e, "alloc-a", "4242")
	t.Run("another Allocation", func(t *testing.T) {
		before := patches.Load()
		refused(t, "alloc-a", mark("alloc-a", "uid-another", "4242", msg))
		noPatch(t, before, "for another UID")
	})
	t.Run("another job", func(t *testing.T) {
		before := patches.Load()
		refused(t, "alloc-a", mark("alloc-a", string(a.UID), "4243", msg))
		noPatch(t, before, "for another job")
	})
	for _, tc := range []struct {
		name string
		mod  func(*v1alpha1.Allocation)
		del  bool
	}{
		{"ended", func(a *v1alpha1.Allocation) { a.Status.AtProvider.Phase = v1alpha1.AllocationEnded }, false},
		{"completing", func(a *v1alpha1.Allocation) { a.Status.AtProvider.JobCompleting = true }, false},
		{"being deleted", func(*v1alpha1.Allocation) {}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "alloc-" + map[string]string{"ended": "ended", "completing": "compl", "being deleted": "del"}[tc.name]
			b := runningAlloc(t, e, name, "4242", tc.mod)
			if tc.del {
				b.Finalizers = append(b.Finalizers, "example.org/hold")
				if err := e.c.Update(e.ctx, b); err != nil {
					t.Fatal(err)
				}
				if err := e.c.Delete(e.ctx, b); err != nil {
					t.Fatal(err)
				}
				if e.get(name).DeletionTimestamp.IsZero() {
					t.Fatal("the Allocation is not being deleted")
				}
			}
			before := patches.Load()
			refused(t, name, mark(name, string(b.UID), "4242", msg))
			noPatch(t, before, "for an Allocation that is "+tc.name)
		})
	}
	t.Run("a concurrent write is kept across the retry", func(t *testing.T) {
		c := runningAlloc(t, e, "alloc-c", "4242")
		other := func(ctx context.Context, cl client.Client, obj client.Object) {
			fresh := &v1alpha1.Allocation{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
				t.Error(err)
				return
			}
			fresh.SetConditions(xpv2.Condition{Type: "AdminHold", Status: corev1.ConditionFalse, Reason: "NotHeldByAdmin", LastTransitionTime: *mt(e.clock.Now())})
			if err := cl.Status().Update(ctx, fresh); err != nil {
				t.Error(err)
			}
		}
		beforePatch.Store(&other)
		before := patches.Load()
		if err := mark("alloc-c", string(c.UID), "4242", msg); err != nil {
			t.Fatalf("the mark after another writer's commit: %v", err)
		}
		got := e.get("alloc-c")
		if st, reason, m := relayCondition(got); st != corev1.ConditionTrue || reason != wantRelayReason || m != msg {
			t.Errorf("the condition is %q, %q, %q; want True, %s, %q", st, reason, m, wantRelayReason, msg)
		}
		if h := got.GetCondition("AdminHold"); h.Status != corev1.ConditionFalse || h.Reason != "NotHeldByAdmin" {
			t.Errorf("the other writer's condition is %+v after the retry; want it kept", h)
		}
		if n := patches.Load() - before; n != 2 {
			t.Errorf("%d status patches, want 2: the one the commit made stale, and the retry", n)
		}
	})
	t.Run("a job that changed between attempts is refused", func(t *testing.T) {
		d := runningAlloc(t, e, "alloc-d", "4242")
		newJob := func(ctx context.Context, cl client.Client, obj client.Object) {
			fresh := &v1alpha1.Allocation{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
				t.Error(err)
				return
			}
			fresh.Status.AtProvider.JobID = "4243"
			if err := cl.Status().Update(ctx, fresh); err != nil {
				t.Error(err)
			}
		}
		beforePatch.Store(&newJob)
		before := patches.Load()
		refused(t, "alloc-d", mark("alloc-d", string(d.UID), "4242", msg))
		if n := patches.Load() - before; n != 1 {
			t.Errorf("%d status patches, want 1: the stale one, and no retry for job 4243", n)
		}
	})
	t.Run("a mark already there is kept", func(t *testing.T) {
		c := e.get("alloc-c")
		before := patches.Load()
		if err := mark("alloc-c", string(c.UID), "4242", "the serving relay ended for good: another cause"); err != nil {
			t.Fatal(err)
		}
		c.Status.AtProvider.Phase = v1alpha1.AllocationEnded
		if err := e.c.Status().Update(e.ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := mark("alloc-c", string(c.UID), "4242", "the serving relay ended for good: another cause"); err != nil {
			t.Errorf("a mark on an Allocation that ended since it was marked: %v, want it kept with no error", err)
		}
		if st, _, m := relayCondition(e.get("alloc-c")); st != corev1.ConditionTrue || m != msg {
			t.Errorf("the mark is %q, %q; want it kept, with %q", st, m, msg)
		}
		noPatch(t, before, "for a mark already there")
	})
	t.Run("a lead that is over", func(t *testing.T) {
		f := runningAlloc(t, e, "alloc-f", "4242")
		over, cancel := context.WithCancel(context.Background())
		cancel()
		before := patches.Load()
		err := s.persistRelayFailed(over, clientKey("alloc-f"), relayFailure{uid: string(f.UID), jobID: "4242", msg: msg, at: e.clock.Now()})
		if err == nil {
			t.Error("a mark with the lead over: no error")
		}
		if st, _, m := relayCondition(e.get("alloc-f")); st != "" {
			t.Errorf("alloc-f is marked %s, %q, with the lead over", st, m)
		}
		noPatch(t, before, "with the lead over")
	})
}

// A copy that loses the lead while its failure path reads the Allocation
// marks nothing (the delta review of #319, blocking 1): the relays end for
// good at the gate, the failure path's read of the Allocation starts the
// Lease's release hook (StopRelays, as the relay fence calls it) and goes on
// only once this copy's lead is over, as a slow read would, and the patch
// that would follow is refused. Once StopRelays has returned, which waits for
// the relays' loops and so for their callbacks, the Allocation is unmarked
// and no RelayFailed was raised.
func TestACopyThatLosesTheLeadWhileItReadsTheAllocationMarksNothing(t *testing.T) {
	var armed atomic.Bool
	var reads atomic.Int32
	var once sync.Once
	stopped := make(chan struct{})
	g := newRelayGate(t)
	var e *env
	e = newEnv(t, stubbedRelays(g.stub), withInterceptors(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() {
				reads.Add(1)
				lead := e.ctl.shared.relays.leadContext()
				once.Do(func() { go func() { e.ctl.StopRelays(); close(stopped) }() })
				select {
				case <-lead.Done():
				case <-time.After(30 * time.Second):
					t.Error("the lead did not end within 30 s of the release hook")
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}))
	runRelaysStoppable(e)
	startAllocation(t, e)
	armed.Store(true)
	g.open(t)
	select {
	case <-stopped:
	case <-time.After(60 * time.Second):
		t.Fatal("StopRelays did not return within 60 s")
	}
	armed.Store(false)
	if reads.Load() == 0 {
		t.Fatal("the failure path read no Allocation: the relays did not end for good while armed")
	}
	if st, _, m := relayCondition(e.get("alloc-a")); st != "" {
		t.Errorf("a copy that lost the lead marked the Allocation %s: %q", st, m)
	}
	if got := e.rec.relayFailedMessages(); len(got) != 0 {
		t.Errorf("a copy that lost the lead raised RelayFailed: %q", got)
	}
}

package lifecycle

// Scenario tests for HBEP-7's controller restarts during a handover (Test plan,
// README lines 446 and 453). They share handover_test.go's timeline: the primary
// is ready at 08:00:50 and has its drain start at 09:50:20; its first
// replacement R1 is planned at 09:19:50 (waits [0 s], start-to-ready 30 s,
// margin 30 min), starts at 09:20:00 and is ready at 09:20:30 or a little
// later; R1's replacement R2 is planned at 10:39:30.

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

var errHOKilled = errors.New("the controller was killed")

// hoKiller models a controller process that dies right after one of its writes:
// the write is stored, the caller sees an error instead of the answer (so
// nothing after the write runs), and every later write of the lifecycle
// controller fails until revive. Writes of other field managers (the Provider's
// controller) are never touched.
type hoKiller struct {
	mu    sync.Mutex
	after func(op string, obj client.Object) bool // the write to die after; nil: none
	dead  bool
}

func (k *hoKiller) guard(op string, obj client.Object, mine bool, do func() error) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.dead && mine {
		return errHOKilled
	}
	if err := do(); err != nil {
		return err
	}
	if mine && k.after != nil && k.after(op, obj) {
		k.dead = true
		return errHOKilled
	}
	return nil
}

func (k *hoKiller) funcs() interceptor.Funcs {
	lifecycle := func(m string) bool { return m == v1alpha1.FieldManagerLifecycle }
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			var o client.CreateOptions
			o.ApplyOptions(opts)
			return k.guard("create", obj, lifecycle(o.FieldManager), func() error { return c.Create(ctx, obj, opts...) })
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			var o client.UpdateOptions
			o.ApplyOptions(opts)
			return k.guard("update", obj, lifecycle(o.FieldManager), func() error { return c.Update(ctx, obj, opts...) })
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return k.guard("delete", obj, true, func() error { return c.Delete(ctx, obj, opts...) })
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			var o client.SubResourceUpdateOptions
			o.ApplyOptions(opts)
			return k.guard("status", obj, lifecycle(o.FieldManager), func() error { return c.SubResource(sub).Update(ctx, obj, opts...) })
		},
	}
}

func (k *hoKiller) arm(after func(string, client.Object) bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.after = after
}

func (k *hoKiller) isDead() bool { k.mu.Lock(); defer k.mu.Unlock(); return k.dead }

// revive starts a new controller on the same store, with nothing armed.
func (k *hoKiller) revive(rg *rig) {
	k.mu.Lock()
	k.dead, k.after = false, nil
	k.mu.Unlock()
	rg.restartController()
}

// step is rg.Step with a reconcile that may die; it reports whether it did.
func (k *hoKiller) step(rg *rig) bool {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}})
	if err != nil && !errors.Is(err, errHOKilled) {
		rg.t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
	}
	return k.isDead()
}

// stepTo is hoStepTo for a controller that may die; it stops at the death.
func (k *hoKiller) stepTo(rg *rig, target time.Time, coarse time.Duration) bool {
	rg.t.Helper()
	for rg.clock.Now().Before(target) {
		rg.clock.Advance(min(coarse, target.Sub(rg.clock.Now())))
		if k.step(rg) {
			return true
		}
	}
	return false
}

// hoVerificationRecords lists the verification records of an Instance.
func hoVerificationRecords(rg *rig, uid types.UID) []appsv1.ControllerRevision {
	rg.t.Helper()
	var l appsv1.ControllerRevisionList
	if err := rg.c.List(rg.ctx, &l, client.InNamespace(rigNS),
		client.MatchingLabels{v1alpha1.LabelRecord: v1alpha1.RecordVerification, v1alpha1.LabelInstanceUID: string(uid)}); err != nil {
		rg.t.Fatal(err)
	}
	return l.Items
}

// hoCheckHandover is the invariant after every kill: one job per Allocation
// needed (allocs), no orphan Allocation (each is named by exactly one Instance),
// and at most one live replacement per primary.
func hoCheckHandover(t *testing.T, rg *rig, allocs int) {
	t.Helper()
	if got := rg.submissions(); got != allocs {
		t.Fatalf("%d sbatch calls, want %d (one per Allocation needed)", got, allocs)
	}
	as := rg.allocations(hoMD)
	if len(as) != allocs {
		t.Fatalf("%d Allocations, want %d", len(as), allocs)
	}
	named, live := map[string]int{}, map[string]int{}
	for _, in := range rg.instances(hoMD) {
		named[in.Spec.AllocationRef.Name]++
		if in.Status.Phase != v1alpha1.InstanceEnded {
			if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" {
				if live[pred]++; live[pred] > 1 {
					t.Fatalf("primary %s has two live replacements", pred)
				}
			}
		}
	}
	for _, a := range as {
		if named[a.Name] != 1 {
			t.Fatalf("Allocation %s is named by %d Instances, want exactly 1", a.Name, named[a.Name])
		}
	}
}

// TestHandoverRestartPlansTheSameTime covers: the controller restarts between
// the primary's start and the planned time, and plans the same time from the
// waits and the start-to-ready time in status. 14 waits (12 of 5 min, 10, 40)
// plus the primary's 0 s are 15, fewer than 20, so the largest, 40 min, is used:
// 09:50:20 - (40 min + 30 s + 30 min) = 08:39:50. The run without a restart and
// the runs restarted at 08:00:30 (the primary still starting), 08:05:00, 08:30:00
// and 08:39:40 (10 s before) all create the replacement at 08:39:50.
func TestHandoverRestartPlansTheSameTime(t *testing.T) {
	waits := append(slices.Repeat(hoMinutes(5), 12), hoMinutes(10, 40)...)
	want := hoT(8, 39, 50)
	run := func(t *testing.T, restarts ...time.Time) time.Time {
		rg := newRig(t, withMaxTime(4*time.Hour))
		hoSetWaits(rg, waits...)
		rg.create(mdSpec(hoMD))
		for _, at := range restarts {
			// The first minute takes 10 s steps: the primary's start-to-ready is
			// measured in steps.
			if at.After(hoT(8, 1, 0)) {
				hoStepTo(rg, hoT(8, 1, 0), 10*time.Second, nil)
			}
			coarse := 10 * time.Second
			if at.After(hoT(8, 1, 0)) {
				coarse = time.Minute
			}
			hoStepTo(rg, at, coarse, nil)
			rg.restartController()
		}
		hoStepTo(rg, hoT(8, 1, 0), 10*time.Second, nil)
		p := rg.instances(hoMD)[0]
		hoStepTo(rg, want.Add(-10*time.Second), time.Minute, func() {
			if n := len(rg.instances(hoMD)); n != 1 {
				t.Fatalf("at %s: %d Instances, a replacement before 08:39:50", rg.clock.Now().Format(time.TimeOnly), n)
			}
		})
		hoStepTo(rg, want, 10*time.Second, nil)
		r := hoRepl(rg, p.Name)
		if len(r) != 1 {
			t.Fatalf("restarts %v: %d replacements at %s, want 1", restarts, len(r), want.Format(time.TimeOnly))
		}
		hoStepTo(rg, hoT(9, 0, 0), time.Minute, func() { hoAtMostOneReplacement(rg) })
		if s := rg.submissions(); s != 2 {
			t.Fatalf("restarts %v: %d sbatch calls, want 2", restarts, s)
		}
		return r[0].CreationTimestamp.Time
	}
	base := run(t)
	if !base.Equal(want) {
		t.Fatalf("without a restart the replacement is created at %s, want %s", base.Format(time.TimeOnly), want.Format(time.TimeOnly))
	}
	for _, restarts := range [][]time.Time{
		{hoT(8, 0, 30)}, {hoT(8, 5, 0)}, {hoT(8, 30, 0)}, {hoT(8, 39, 40)}, {hoT(8, 5, 0), hoT(8, 30, 0), hoT(8, 39, 40)},
	} {
		if got := run(t, restarts...); !got.Equal(base) {
			t.Fatalf("restarts %v: the replacement is created at %s, without a restart at %s", restarts, got.Format(time.TimeOnly), base.Format(time.TimeOnly))
		}
	}
}

// hoKill is one kill of the controller during a handover: it dies right after
// the write that after matches, at the tick at, and a new controller carries on
// to end.
type hoKill struct {
	name  string
	at    time.Time
	after func(p *v1alpha1.Instance) func(op string, obj client.Object) bool
	// crashed checks the store as the dead controller left it.
	crashed func(t *testing.T, rg *rig, p *v1alpha1.Instance)
	// resume, when set, runs after the restart and before the next full step.
	resume func(t *testing.T, rg *rig)
	end    time.Time
	allocs int // Allocations needed at the end
	// final checks the store at the end.
	final func(t *testing.T, rg *rig, p *v1alpha1.Instance)
}

// hoCreated matches the creation of a kind of object.
func hoCreated[T client.Object](nth int, match func(T) bool) func(string, client.Object) bool {
	n := 0
	return func(op string, obj client.Object) bool {
		o, ok := obj.(T)
		if !ok || op != "create" || !match(o) {
			return false
		}
		n++
		return n == nth
	}
}

// TestHandoverControllerKilledAtEachStep covers: the controller is killed at
// each step of a handover, and after the restart there are no duplicate
// submissions and no orphan Allocations. Kill points, each in its own run:
//   - after the replacement Instance is created and before its Allocation (the
//     Instance holds the name its Allocation will take);
//   - after the Allocation and before its credential file;
//   - after R1's verification record is written and before its status names it;
//   - after the primary's draining status is written, and after its ended status
//     and before its drain finalizer goes.
func TestHandoverControllerKilledAtEachStep(t *testing.T) {
	tail := func(t *testing.T, rg *rig, p *v1alpha1.Instance) { // the primary drained, R1 serves
		if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonDrained {
			t.Fatalf("the primary is %s/%s, want ended/Drained", got.Status.Phase, got.Status.EndReason)
		}
	}
	cases := []hoKill{{
		name: "after the replacement Instance, before its Allocation", at: hoT(9, 19, 50), end: hoT(9, 22, 0), allocs: 2,
		after: func(*v1alpha1.Instance) func(string, client.Object) bool {
			return hoCreated(1, func(in *v1alpha1.Instance) bool { return in.Annotations[v1alpha1.AnnotationPredecessor] != "" })
		},
		crashed: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			r := hoRepl(rg, p.Name)
			if len(r) != 1 || r[0].Status.Phase != "" {
				t.Fatalf("replacements %+v, want one with no status yet", r)
			}
			if _, ok := rg.allocation(r[0].Spec.AllocationRef.Name); ok || rg.submissions() != 1 {
				t.Fatalf("the replacement's Allocation exists (%v) or %d sbatch calls, want none and 1", ok, rg.submissions())
			}
		},
		final: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			r := hoRepl(rg, p.Name)
			if len(r) != 1 || r[0].Status.Phase != v1alpha1.InstanceReady || !r[0].CreationTimestamp.Time.Equal(hoT(9, 19, 50)) {
				t.Fatalf("replacements %+v, want one created at 09:19:50 and ready", r)
			}
		},
	}, {
		name: "after the Allocation, before its credential file", at: hoT(9, 19, 50), end: hoT(9, 22, 0), allocs: 2,
		after: func(*v1alpha1.Instance) func(string, client.Object) bool {
			// The killer is armed after the primary's start, so the
			// replacement's Allocation is the first created after arming.
			return hoCreated(1, func(*v1alpha1.Allocation) bool { return true })
		},
		crashed: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			a, _ := rg.allocation(hoRepl(rg, p.Name)[0].Spec.AllocationRef.Name)
			if a == nil {
				t.Fatal("the replacement's Allocation does not exist")
			}
			if _, err := os.Stat(agentapi.AllocationCredentialPath(rg.creds, string(a.UID))); err == nil || rg.submissions() != 1 {
				t.Fatalf("credential file stat error %v, %d sbatch calls: want no file and 1 call", err, rg.submissions())
			}
		},
		// The real Provider submits only once the file exists; the fake does not
		// wait, so the new controller writes the file before the next Provider
		// pass, as it would have to before any submission.
		resume: func(t *testing.T, rg *rig) {
			rg.reconcileAll()
			a, _ := rg.allocation(hoRepl(rg, rg.instances(hoMD)[0].Name)[0].Spec.AllocationRef.Name)
			if _, err := os.Stat(agentapi.AllocationCredentialPath(rg.creds, string(a.UID))); err != nil || rg.submissions() != 1 {
				t.Fatalf("credential file stat error %v, %d sbatch calls: want the file and still 1 call", err, rg.submissions())
			}
		},
		final: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			if r := hoRepl(rg, p.Name); len(r) != 1 || r[0].Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("replacements %+v, want one ready", r)
			}
		},
	}, {
		// The killer's coarse pre-steps land on the minute grid (08:02:00 ...
		// 09:20:00, then 09:20:10), so R1, planned for 09:19:50, is created at
		// 09:20:00; its job starts at the next provider pass, 09:20:10, and, as
		// the primary's, its verification record is written 10 s later.
		name: "after a verification record, before the status names it", at: hoT(9, 20, 20), end: hoT(9, 22, 0), allocs: 2,
		after: func(*v1alpha1.Instance) func(string, client.Object) bool {
			// Armed after the primary's start: R1's record is the first after.
			return hoCreated(1, func(r *appsv1.ControllerRevision) bool {
				return r.Labels[v1alpha1.LabelRecord] == v1alpha1.RecordVerification
			})
		},
		crashed: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			r := hoRepl(rg, p.Name)[0]
			if recs := hoVerificationRecords(rg, r.UID); len(recs) != 1 || recs[0].Name != "ver-"+string(r.UID)+"-1" || r.Status.VerificationRecord != "" {
				t.Fatalf("records %d, status names %q: want the one record and no name yet", len(recs), r.Status.VerificationRecord)
			}
		},
		final: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			r := hoRepl(rg, p.Name)[0]
			recs := hoVerificationRecords(rg, r.UID)
			if len(recs) != 1 || r.Status.VerificationRecord != recs[0].Name || r.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("records %d, status names %q, phase %s: want the one record named and the Instance ready", len(recs), r.Status.VerificationRecord, r.Status.Phase)
			}
		},
	}, {
		name: "after the primary is draining", at: hoT(9, 50, 20), end: hoT(9, 52, 0), allocs: 2,
		after: func(p *v1alpha1.Instance) func(string, client.Object) bool {
			return func(op string, obj client.Object) bool {
				in, ok := obj.(*v1alpha1.Instance)
				return ok && op == "status" && in.Name == p.Name && in.Status.Phase == v1alpha1.InstanceDraining
			}
		},
		crashed: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			// A draining Instance is not ready: R1 alone is.
			if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining || rg.readyCount(hoMD) != 1 {
				t.Fatalf("the primary is %s with %d ready, want draining and 1 (R1)", ph, rg.readyCount(hoMD))
			}
		},
		final: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			tail(t, rg, p)
			if n := len(rg.rec.Find("replaced", "HandedOver")); n != 1 || len(rg.rec.Find("degraded", "CapacityDropped")) != 0 {
				t.Fatalf("%d HandedOver events (want 1) and some CapacityDropped", n)
			}
			// R1's own replacement is planned as any other; its time is
			// TestHandoverBothServeThenPrimaryDrainsAndSecondReplacementIsPlanned's
			// subject, and this run's coarse steps shift it, so it is not
			// asserted here.
		},
	}, {
		name: "after the primary is ended, before its finalizer goes", at: hoT(9, 50, 20), end: hoT(9, 52, 0), allocs: 2,
		after: func(p *v1alpha1.Instance) func(string, client.Object) bool {
			return func(op string, obj client.Object) bool {
				in, ok := obj.(*v1alpha1.Instance)
				return ok && op == "status" && in.Name == p.Name && in.Status.Phase == v1alpha1.InstanceEnded
			}
		},
		crashed: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceEnded || len(got.Finalizers) != 1 {
				t.Fatalf("the primary is %s with finalizers %v, want ended and its drain finalizer", got.Status.Phase, got.Finalizers)
			}
		},
		final: func(t *testing.T, rg *rig, p *v1alpha1.Instance) {
			tail(t, rg, p)
			if f := rg.instance(p.Name).Finalizers; len(f) != 0 {
				t.Fatalf("the ended primary still has finalizers %v", f)
			}
		},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := &hoKiller{}
			rg := newRig(t, withMaxTime(4*time.Hour), withInterceptors(k.funcs()))
			p := hoStart(rg, nil)
			k.arm(tc.after(p))
			if k.stepTo(rg, tc.at.Add(-10*time.Second), time.Minute) || !k.stepTo(rg, tc.at.Add(time.Minute), 10*time.Second) {
				t.Fatalf("the controller died too early or not at all (now %s)", rg.clock.Now().Format(time.TimeOnly))
			}
			if now := rg.clock.Now(); !now.Equal(tc.at) {
				t.Fatalf("the controller died at %s, want %s", now.Format(time.TimeOnly), tc.at.Format(time.TimeOnly))
			}
			tc.crashed(t, rg, p)
			k.revive(rg)
			if tc.resume != nil {
				tc.resume(t, rg)
			}
			hoStepTo(rg, tc.end, 10*time.Second, func() { hoAtMostOneReplacement(rg) })
			hoCheckHandover(t, rg, tc.allocs)
			tc.final(t, rg, p)
		})
	}
}

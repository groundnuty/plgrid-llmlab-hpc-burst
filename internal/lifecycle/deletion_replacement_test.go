package lifecycle

// A deployment that is being deleted is never given a replacement (HBEP-7, Release; issue #503). The deletion of a ModelDeployment is
// the shutdown case: its Instance drains and ends, its empty Allocation goes at once, and no pass makes capacity for it. Row A's first
// hosted stand-in on the new flow showed a second Allocation of a deleted deployment with no job, and nothing then said where it came
// from, so this holds every ordering of the deletion that a pass can meet against the question directly: over a quarter of an hour of
// passes after the delete, the run has made one Instance, one Allocation and one submission, and the Allocation has gone, its job
// cancelled. A test that did not make the Allocation go would prove nothing about the window, so each case needs that too.
//
// The orderings (what a pass sees of the deployment, and of its Instance, while the Instance ends):
//   - gone: the deployment is deleted and its Instance with it, as garbage collection would;
//   - gone, Instance not yet deleted: the deployment is gone and the Instance is still ready (the collector is late);
//   - held: a finalizer holds the deployment, with its deletion timestamp set.
//
// The tests after the first are the orderings it cannot reach, where the deletion lands inside a pass. A pass reads the deployment at
// its start and lists the Instances after that (reconcile.go, Reconcile and observe: two reads), so a deletion that lands between them,
// with the garbage collector marking the Instance, hands the pass a live deployment and no live Instance: row A's hosted stand-in met it
// (#503), as a second tag that never got a job. The fix reads the deployment again, from the API, immediately before each create
// (provision.go, confirmDeployment). The tests: the forced interleaving, the residual that the read cannot close (a delete between it and
// the create) with its end state, and the control that a live deployment still gets its replacement.

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestADeploymentBeingDeletedGetsNoReplacementAtAnyOrdering(t *testing.T) {
	until := metav1.NewTime(rigStart.Add(time.Hour))
	policies := []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
	}{
		{"the default policy", nil},
		{"keep until a date", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: &until}},
		{"release when idle, 20 minutes", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(20 * time.Minute)}},
	}
	for _, pc := range policies {
		for _, ordering := range []string{"gone", "gone, Instance not yet deleted", "held"} {
			t.Run(pc.name+"/"+ordering, func(t *testing.T) {
				rg := newRig(t)
				md := mdSpec("md")
				md.Spec.RetentionPolicy = pc.policy
				rg.create(md)
				rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
				a := rg.allocations("md")[0]
				switch ordering {
				case "held":
					rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Finalizers = append(m.Finalizers, "test.example/hold") })
				}
				if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
					t.Fatal(err)
				}
				if ordering != "gone, Instance not yet deleted" {
					for _, in := range rg.instances("md") {
						if err := rg.c.Delete(rg.ctx, &in); err != nil {
							t.Fatal(err)
						}
					}
				}
				seenInstances, seenAllocations := map[types.UID]bool{}, map[string]bool{}
				for rg.clock.Now().Before(rigStart.Add(40*time.Minute)) && len(seenAllocations) < 2 {
					for _, in := range rg.instances("md") {
						seenInstances[in.UID] = true
					}
					for _, al := range rg.allocations("md") {
						seenAllocations[al.Name] = true
					}
					rg.clock.Advance(10 * time.Second)
					rg.Step()
					if allocReleased(rg, a.Name) && rg.clock.Now().After(rigStart.Add(25*time.Minute)) {
						break
					}
				}
				if len(seenInstances) != 1 || len(seenAllocations) != 1 || rg.submissions() != 1 {
					t.Errorf("after the delete: %d Instances, %d Allocations and %d submissions were made in all, want 1, 1 and 1: a replacement for a deployment being deleted",
						len(seenInstances), len(seenAllocations), rg.submissions())
				}
				if !allocReleased(rg, a.Name) {
					t.Fatalf("the Allocation of the deleted deployment was not released within the window: the case would prove nothing about replacement")
				}
				if n := allocCount(rg.batch, "scancel", allocJobID(&a)); n < 1 {
					t.Errorf("scancel calls for the released job = %d, want at least 1", n)
				}
			})
		}
	}
}

// TestADeletionBetweenAPassesTwoReadsMakesNoReplacement makes that ordering. The deployment is read live, then, as the pass lists the
// Instances, the deployment is deleted and its Instance marked deleting (what the garbage collector does a moment after a delete). The
// pass holds a live deployment that wants one Instance and an Instance that no longer counts, so, unless it looks again, it makes capacity
// for a deployment that is gone: a second Instance and a second Allocation. After it, passes run as usual for half an hour, and what the
// run has made is counted. Three ways the second read can find the deployment:
//   - gone: the deletion took it;
//   - held: a finalizer holds it, with its deletion timestamp set;
//   - recreated: a deployment of the same name, another UID, stands where it was. The old deployment is gone, so the pass makes nothing for
//     it (Instances it made would be owned by a UID that no longer exists); the new one is the next pass's, and gets its own Instance.
func TestADeletionBetweenAPassesTwoReadsMakesNoReplacement(t *testing.T) {
	for _, ordering := range []string{"gone", "held", "recreated"} {
		t.Run(ordering, func(t *testing.T) {
			var armed bool
			var onLists func(ctx context.Context, c client.WithWatch)
			rg := newRig(t, withInterceptors(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*v1alpha1.InstanceList); ok && armed {
					armed = false
					onLists(ctx, c)
				}
				return c.List(ctx, list, opts...)
			}}))
			rg.create(mdSpec("md"))
			rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
			if ordering == "held" {
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Finalizers = append(m.Finalizers, "test.example/hold") })
			}
			a := rg.allocations("md")[0]
			live := rg.md("md")
			onLists = func(ctx context.Context, c client.WithWatch) {
				if err := c.Delete(ctx, live); err != nil {
					t.Errorf("delete the deployment in the middle of the pass: %v", err)
				}
				if ordering == "recreated" {
					again := mdSpec("md")
					again.UID = newUID()
					if err := c.Create(ctx, again); err != nil {
						t.Errorf("recreate the deployment in the middle of the pass: %v", err)
					}
				}
				var il v1alpha1.InstanceList
				if err := c.List(ctx, &il); err != nil {
					t.Errorf("list the Instances in the middle of the pass: %v", err)
				}
				for i := range il.Items {
					if err := c.Delete(ctx, &il.Items[i]); err != nil {
						t.Errorf("delete an Instance in the middle of the pass: %v", err)
					}
				}
			}
			// One pass, in the order that matters: the sync steps first, then the reconcile that reads the deployment before it lists.
			if err := rg.prov.Sync(rg.ctx); err != nil {
				t.Fatal(err)
			}
			rg.syncAgents()
			armed = true
			rg.clock.Advance(10 * time.Second)
			_, _ = rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: live.Namespace, Name: live.Name}})
			if armed {
				t.Fatal("the pass never listed the Instances: the case would say nothing")
			}
			if ordering == "held" {
				// The deployment stays, held by its finalizer, and the pass wrote its status: a pass that stopped creating because the
				// deployment is going must not report that no Provider can take it. Read before any later pass rewrites the conditions.
				for _, c := range rg.md("md").Status.Conditions {
					if c.Reason == v1alpha1.ReasonProviderUnavailable {
						t.Errorf("the deployment being deleted reports %s: %s", c.Reason, c.Message)
					}
				}
			}
			ownedByOld, seenAllocations := map[types.UID]bool{}, map[string]bool{}
			for rg.clock.Now().Before(rigStart.Add(30 * time.Minute)) {
				for _, in := range rg.instances("md") {
					for _, o := range in.OwnerReferences {
						if o.UID == live.UID {
							ownedByOld[in.UID] = true
						}
					}
				}
				for _, al := range rg.allocations("md") {
					seenAllocations[al.Name] = true
				}
				rg.clock.Advance(10 * time.Second)
				rg.Step()
			}
			if len(ownedByOld) != 1 {
				t.Errorf("%d Instances were made for the deployment deleted in the middle of the pass, want its one: a replacement for a deployment that was just deleted", len(ownedByOld))
			}
			if ordering != "recreated" {
				if len(seenAllocations) != 1 || rg.submissions() != 1 {
					t.Errorf("%d Allocations and %d submissions were made in all, want 1 and 1: a replacement for a deployment that was just deleted", len(seenAllocations), rg.submissions())
				}
				if !allocReleased(rg, a.Name) {
					t.Errorf("the Allocation of the deleted deployment was not released")
				}
			}
		})
	}
}

// replacementRig is a rig with a hook on each kind of call the tests below force: onAllocationCreate runs just before an Allocation is
// created, and failDeploymentRead makes the n-th read of the deployment in the pass fail (1 is the pass's own, first read).
type replacementRig struct {
	*rig
	armedAllocation    bool
	onAllocationCreate func(ctx context.Context, c client.WithWatch)
	armedInstance      bool
	onInstanceCreate   func(ctx context.Context, c client.WithWatch)
	failRead           int
	reads              int
}

func newReplacementRig(t *testing.T) *replacementRig {
	rr := &replacementRig{}
	rr.rig = newRig(t, withInterceptors(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*v1alpha1.Instance); ok && rr.armedInstance {
				rr.armedInstance = false
				rr.onInstanceCreate(ctx, c)
			}
			if _, ok := obj.(*v1alpha1.Allocation); ok && rr.armedAllocation {
				rr.armedAllocation = false
				rr.onAllocationCreate(ctx, c)
			}
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.ModelDeployment); ok && rr.failRead > 0 {
				rr.reads++
				if rr.reads == rr.failRead {
					rr.failRead = 0
					return context.DeadlineExceeded
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}))
	return rr
}

// loseTheInstance deletes the deployment's Instance and nothing else, as a failure of it would: the deployment is live and wants one
// Instance, so the next pass makes capacity.
func (rr *replacementRig) loseTheInstance() {
	rr.t.Helper()
	for _, in := range rr.instances("md") {
		in.Finalizers = nil
		if err := rr.c.Update(rr.ctx, &in); err != nil {
			rr.t.Fatal(err)
		}
		if err := rr.c.Delete(rr.ctx, &in); err != nil {
			rr.t.Fatal(err)
		}
	}
}

func (rr *replacementRig) reconcileOnce(md *v1alpha1.ModelDeployment) error {
	rr.t.Helper()
	if err := rr.prov.Sync(rr.ctx); err != nil {
		rr.t.Fatal(err)
	}
	rr.syncAgents()
	rr.clock.Advance(10 * time.Second)
	_, err := rr.r.Reconcile(rr.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: md.Namespace, Name: md.Name}})
	return err
}

// TestADeleteBetweenTheLastReadAndTheCreateLeavesNothingRunning is the residual the read before each create cannot close: the deployment
// is live at that read and deleted just before the Allocation is created, so one is made for a deployment that is already gone. It is
// made on a deployment's first provisioning, so that no warm Allocation can take the Instance instead. What bounds it is checked at its
// end state: after the passes that follow, no Allocation of the deployment is left, and any job the Provider submitted for one has been
// cancelled, so no job runs on. (The Instance made just before it is owned by the deleted deployment; the rig has no garbage collector,
// so the shutdown rules end it, as they would end one the collector had marked.)
func TestADeleteBetweenTheLastReadAndTheCreateLeavesNothingRunning(t *testing.T) {
	rr := newReplacementRig(t)
	md := mdSpec("md")
	rr.create(md)
	live := rr.md("md")
	rr.onAllocationCreate = func(ctx context.Context, c client.WithWatch) {
		if err := c.Delete(ctx, live); err != nil {
			t.Errorf("delete the deployment between the read and the create: %v", err)
		}
	}
	rr.armedAllocation = true
	_ = rr.reconcileOnce(live)
	if rr.armedAllocation {
		t.Fatal("the pass never created an Allocation: the case would say nothing")
	}
	jobs := map[string]string{}
	for rr.clock.Now().Before(rigStart.Add(30 * time.Minute)) {
		for _, al := range rr.allAllocations() {
			jobs[al.Name] = allocJobID(&al)
		}
		rr.clock.Advance(10 * time.Second)
		rr.Step()
	}
	if len(jobs) != 1 {
		t.Errorf("%d Allocations were seen, want the one made in the window: the residual was not made", len(jobs))
	}
	if left := rr.allAllocations(); len(left) != 0 {
		t.Errorf("%d Allocations of the deleted deployment are left, want none", len(left))
	}
	for name, id := range jobs {
		if id != "" && allocCount(rr.batch, "scancel", id) < 1 {
			t.Errorf("the job of Allocation %s (submitted) was never cancelled: it would run on", name)
		}
	}
}

// TestADeleteAfterTheInstanceIsCreatedMakesNoAllocation: the deployment is deleted after the Instance's create and before its Allocation's,
// so the guard before the Allocation is what stops it. The Instance is the deleted deployment's to end (owned by it); no Allocation is
// made, nothing is submitted, and nothing is left.
func TestADeleteAfterTheInstanceIsCreatedMakesNoAllocation(t *testing.T) {
	rr := newReplacementRig(t)
	rr.create(mdSpec("md"))
	live := rr.md("md")
	rr.onInstanceCreate = func(ctx context.Context, c client.WithWatch) {
		if err := c.Delete(ctx, live); err != nil {
			t.Errorf("delete the deployment between the two creates: %v", err)
		}
	}
	rr.armedInstance = true
	_ = rr.reconcileOnce(live)
	if rr.armedInstance {
		t.Fatal("the pass never created an Instance: the case would say nothing")
	}
	if n := len(rr.instances("md")); n != 1 {
		t.Fatalf("%d Instances were made in the window, want the one before the delete: the case would say nothing", n)
	}
	for rr.clock.Now().Before(rigStart.Add(30 * time.Minute)) {
		if n := len(rr.allAllocations()); n != 0 {
			t.Fatalf("%d Allocations were made for a deployment deleted before them", n)
		}
		rr.clock.Advance(10 * time.Second)
		rr.Step()
	}
	if rr.submissions() != 0 {
		t.Errorf("%d submissions were made for a deployment deleted before its Allocation", rr.submissions())
	}
}

// TestALiveDeploymentStillGetsItsReplacement is the control: with the deployment live, losing its Instance still makes a replacement
// Instance, and the deployment is back to one ready Instance. The read before each create blocks only a deployment that is gone.
func TestALiveDeploymentStillGetsItsReplacement(t *testing.T) {
	rr := newReplacementRig(t)
	rr.create(mdSpec("md"))
	rr.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rr.readyCount("md") == 1 })
	first := rr.instances("md")[0].UID
	rr.loseTheInstance()
	rr.until(15*time.Minute, 10*time.Second, "a replacement Instance ready", func() bool {
		for _, in := range rr.liveInstances("md") {
			if in.UID != first && in.Status.Phase == v1alpha1.InstanceReady {
				return true
			}
		}
		return false
	})
}

// TestAFailedReadBeforeACreateFailsThePassAndCreatesNothing: when the read of the deployment right before a create fails, the pass
// returns the error and creates nothing, as when its first read fails; the manager retries it with backoff.
func TestAFailedReadBeforeACreateFailsThePassAndCreatesNothing(t *testing.T) {
	rr := newReplacementRig(t)
	rr.create(mdSpec("md"))
	rr.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rr.readyCount("md") == 1 })
	live := rr.md("md")
	rr.loseTheInstance()
	before := rr.submissions()
	rr.failRead, rr.reads = 2, 0
	err := rr.reconcileOnce(live)
	if rr.reads < 2 {
		t.Fatalf("the pass read the deployment %d times, want its first read and the one before a create: the case would say nothing", rr.reads)
	}
	if err == nil {
		t.Fatal("the pass returned no error although its read before a create failed")
	}
	if n := len(rr.instances("md")); n != 0 {
		t.Errorf("the pass whose read before a create failed made %d Instances", n)
	}
	if rr.submissions() != before {
		t.Errorf("the pass whose read before a create failed made %d submissions", rr.submissions()-before)
	}
}

// TestAnAllocationTheLadderMadeIsReleasedAtOnceOnDelete: the restart ladder's last rung fails the Instance with RestartLoop, releases its
// Allocation and makes a new one (health.go, restart; HBEP-7, Health and restarts). That new Allocation, pending in the queue or already
// running, is released at once when the deployment is deleted, like any other under HBEP-7's "as shutdown" and the release at once (#503;
// the science agent's guidance on the ladder). The climb is the one of TestHealthRepeatedFailuresClimbLadder: two worker restarts, two
// Instance restarts, then the loop. After the delete no further Allocation or submission appears, and none is left.
func TestAnAllocationTheLadderMadeIsReleasedAtOnceOnDelete(t *testing.T) {
	for _, state := range []string{"pending", "running"} {
		t.Run(state, func(t *testing.T) {
			rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
			// A policy that holds an empty Allocation for 20 minutes, which the ladder's new Allocation copies from the deployment: the
			// release at once is what must override it.
			rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) {
				m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(20 * time.Minute)}
			})
			if state == "pending" {
				rg.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
			}
			ag := rg.agentOf(alloc).agent
			for k := 0; k < 5; k++ {
				starts := len(ag.Starts())
				ag.CrashWorker(healthWorker, 1)
				healthUntilEvent(rg, in.Name, k, 3*time.Minute, startTick)
				if k < 4 {
					for len(ag.Starts()) == starts {
						startStep(rg, "md", startTick)
					}
				}
			}
			if got := rg.instance(in.Name); got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonRestartLoop {
				t.Fatalf("Instance %s (%s), want ended/RestartLoop: the climb did not reach its last rung", got.Status.Phase, got.Status.EndReason)
			}
			var made string
			startUntil(rg, "md", 3*time.Minute, "the failed Allocation released and the ladder's new one made", func() bool {
				if _, alive := rg.allocation(alloc); alive {
					return false
				}
				for _, a := range rg.allocations("md") {
					made = a.Name
				}
				return made != "" && rg.submissions() == 2
			})
			if a, ok := rg.allocation(made); !ok || (state == "pending") != (a.Status.AtProvider.StartTime == nil) {
				t.Fatalf("the ladder's Allocation is %v (found %t), want it %s: the case would say nothing", a.Status.AtProvider.Phase, ok, state)
			}
			if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
				t.Fatal(err)
			}
			for _, i := range rg.instances("md") {
				if err := rg.c.Delete(rg.ctx, &i); err != nil {
					t.Fatal(err)
				}
			}
			rg.until(2*time.Minute, 10*time.Second, "the ladder's Allocation released at once", func() bool { return allocReleased(rg, made) })
			rg.run(20*time.Minute, time.Minute)
			if left := rg.allAllocations(); len(left) != 0 {
				t.Errorf("%d Allocations are left after the delete, want none", len(left))
			}
			if rg.submissions() != 2 {
				t.Errorf("%d submissions in all, want the two (the first job and the ladder's): the delete made no third", rg.submissions())
			}
		})
	}
}

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
// The last test below is the ordering the others cannot reach: one pass reads the deployment, and only then lists the Instances (reconcile.go,
// Reconcile and observe: two reads), and a deletion that lands between them, with the garbage collector marking the Instance, hands the
// pass a live deployment and no live Instance. Row A's hosted stand-in met it (#503): its second tag, an Allocation that never got a job.

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

// TestAFailedSecondReadOfTheDeploymentFailsThePass: when the read of the deployment after the lists fails, the pass returns the error and
// does nothing from the view it could not confirm, as it does when its first read fails. It is retried by the manager with backoff.
func TestAFailedSecondReadOfTheDeploymentFailsThePass(t *testing.T) {
	var reads int
	var failSecond bool
	rg := newRig(t, withInterceptors(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && failSecond {
			reads++
			if reads == 2 {
				failSecond = false
				return context.DeadlineExceeded
			}
		}
		return c.Get(ctx, key, obj, opts...)
	}}))
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	live := rg.md("md")
	before := rg.submissions()
	failSecond = true
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: live.Namespace, Name: live.Name}})
	if reads < 2 {
		t.Fatalf("the pass read the deployment %d times, want its two reads: the case would say nothing", reads)
	}
	if err == nil {
		t.Fatal("the pass returned no error although its second read of the deployment failed")
	}
	if rg.submissions() != before {
		t.Errorf("the pass whose second read failed made %d submissions", rg.submissions()-before)
	}
}

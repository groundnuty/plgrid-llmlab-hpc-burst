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
// A pass that saw the deployment live while its Instance ended would make a replacement, as it should for a live deployment, whatever
// (B) says: (B) turns on the deletion, which such a pass does not see. The controller reads the deployment through the API reader
// (internal/lifecycle, manager.go), never a cache, so a pass cannot see that, and no test here pretends it can.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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

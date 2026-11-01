package lifecycle

// What no-overlap mode leaves alone: capacity that already exists, and
// placements already made.
//   - A retained warm Allocation takes the replacement without a new
//     submission, so a Provider whose submit slots are all taken is still
//     "beside" for that replacement (HBEP-7, Provisioning, open question 14).
//   - A queued successor stays on its Provider when the mode lifts.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestNoOverlapWarmAllocationTakesTheReplacement: with maxSubmitPerUser 2, a
// running primary and a retained warm Allocation take both slots. The warm
// Allocation is the second Instance of a deployment scaled up to two at 09:00
// and back to one while it was still starting, so its job started at 09:00:20
// and its hard end, 13:00:20, is an hour later than the primary's, 12:00:20.
// The primary's replacement, planned at 09:19:30, goes onto the warm
// Allocation: no third job is submitted, nothing is marked as a no-overlap
// successor, and the handover is the ordinary one (the replacement serves at
// the drain start, 09:50:20; NoOverlapSlot never). Without the check for warm
// capacity, the full count put the deployment in the mode: no replacement
// before the drain start, and NoOverlapSlot from it.
func TestNoOverlapWarmAllocationTakesTheReplacement(t *testing.T) {
	qos := []v1alpha1.QOSProfile{{Name: "example-s2", MaxSubmitPerUser: i32(2)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s2"), withMaxTime(4*time.Hour), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 2 }))...)
	p := *hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 0, 0), 5*time.Minute, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	var warm string
	rg.until(5*time.Minute, 10*time.Second, "the second Allocation started", func() bool {
		for _, a := range rg.allocations(hoMD) {
			if a.Name != p.Spec.AllocationRef.Name && a.Status.AtProvider.StartTime != nil {
				warm = a.Name
				return true
			}
		}
		return false
	})
	if rg.readyCount(hoMD) != 1 {
		t.Fatalf("%d Instances ready when the second job started, want only the primary", rg.readyCount(hoMD))
	}
	// Scaled down while the second Instance is still starting: it, not the
	// primary, is the surplus one, and its Allocation is kept.
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.until(5*time.Minute, 10*time.Second, "one live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 1 })
	if live := rg.liveInstances(hoMD); live[0].Name != p.Name || allocReleased(rg, warm) {
		t.Fatalf("live %+v and warm Allocation %s released = %v, want the primary alone live and the Allocation kept", live, warm, allocReleased(rg, warm))
	}
	if n := len(rg.allAllocations()); n != 2 || rg.submissions() != 2 {
		t.Fatalf("%d Allocations and %d submissions, want the primary's and the warm one's only", n, rg.submissions())
	}

	noGap := func() {
		if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonNoOverlapSlot {
			t.Fatalf("at %s Degraded = %s/%s %q: the warm Allocation takes the replacement, so there is no gap", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason, c.Message)
		}
	}
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, noGap)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, noGap)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Spec.AllocationRef.Name != warm {
		t.Fatalf("replacements %+v, want one, on the warm Allocation %s", reps, warm)
	}
	if reps[0].Annotations[annotationNoOverlapSuccessor] != "" {
		t.Fatal("the replacement on the warm Allocation is marked as a no-overlap successor")
	}
	if rg.submissions() != 2 {
		t.Fatalf("%d submissions, want no job for the replacement", rg.submissions())
	}
	repl := reps[0].Name
	hoStepUntil(rg, hoT(9, 49, 0), 10*time.Second, func() bool { return rg.instance(repl).Status.Phase == v1alpha1.InstanceReady }, noGap)
	hoStepTo(rg, hoT(9, 51, 0), 10*time.Second, noGap)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining && ph != v1alpha1.InstanceEnded {
		t.Fatalf("at 09:51:00 the primary is %s, want drained", ph)
	}
	if got := rg.instance(repl); got.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("at 09:51:00 the replacement is %s, want ready", got.Status.Phase)
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("at 09:51:00 Degraded = %s/%s %q after an ordinary handover, want False", c.Status, c.Reason, c.Message)
	}
}

// TestNoOverlapSuccessorStaysWhenTheModeLifts: on the queued path the
// successor is created at 09:19:30 and pends on the GPU cap. At 09:30 a second
// Provider with room joins the deployment, so the mode lifts. The pending
// successor is not moved (nooverlap.go): at the drain start the primary is not
// marked and the successor not stamped, the successor is late
// (ReplacementLate at 09:49:30), and the gap that follows is CapacityDropped,
// not NoOverlapSlot. Once the primary's job ends the successor starts where it
// was and the deployment recovers.
func TestNoOverlapSuccessorStaysWhenTheModeLifts(t *testing.T) {
	rg := noOverlapQueuedRig(t)
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("replacements %+v, want one queued successor, marked", reps)
	}
	succ := reps[0].Name
	hoStepTo(rg, hoT(9, 30, 0), 5*time.Minute, nil)
	rg.addProvider("example-second", "gpu-long", 4*time.Hour, nil, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{rigProvider, "example-second"} })

	hoStepTo(rg, hoT(9, 49, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 49, 30), 10*time.Second, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonReplacementLate {
		t.Fatalf("at 09:49:30 Degraded = %s/%s %q, want ReplacementLate: the mode lifted, so the pending successor is late", c.Status, c.Reason, c.Message)
	}
	gap := false
	hoStepTo(rg, hoT(9, 52, 0), 10*time.Second, func() {
		if rg.clock.Now().Before(hoT(9, 50, 20)) {
			return
		}
		gap = true
		if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonNoOverlapSlot {
			t.Fatalf("at %s Degraded = %s/%s %q: the mode lifted before the drain start, so the gap is no planned one", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason, c.Message)
		}
	})
	if !gap {
		t.Fatal("no step after the drain start was checked")
	}
	if got := rg.instancesByName()[p.Name]; got == nil || got.Annotations[annotationNoOverlap] != "" {
		t.Fatalf("the primary %+v, want it present and not marked as drained in the mode", got)
	}
	// The successor is where it was created, and nothing was made elsewhere.
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return rg.instance(succ).Status.Phase == v1alpha1.InstanceReady }, nil)
	for _, in := range rg.instances(hoMD) {
		if in.Labels[v1alpha1.LabelProvider] != rigProvider {
			t.Fatalf("Instance %s is on Provider %s, want every Instance on %s", in.Name, in.Labels[v1alpha1.LabelProvider], rigProvider)
		}
	}
	if n := len(rg.allAllocations()); n > 2 {
		t.Fatalf("%d Allocations, want the primary's and the successor's only", n)
	}
	if got := rg.instance(succ); got.Annotations[annotationNoOverlapGap] != "" {
		t.Fatalf("the successor carries gap %q, want none: the mode had lifted at the drain start", got.Annotations[annotationNoOverlapGap])
	}
}

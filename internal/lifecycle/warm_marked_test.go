package lifecycle

// Issue #332: warm placement passes over an Allocation marked RelayEndedForGood
// (issue #304). SubmittedAsOther (HBEP-4, Read-back) is the other mark under
// which step 5 (allocationFailed) ends an Instance placed on an Allocation, but
// it is not skipped, because nothing releases an empty one (see relayEnded).
//
// The pass runs step 7 (placement) before step 9 (retention, which releases
// an empty Allocation marked RelayEndedForGood), so without the skip one pass
// PLACES an Instance on the marked, empty Allocation, and the next pass fails
// it with the mark's reason, which counts toward RestartLoop (failedEnd). Every
// test here therefore runs the real pass (rg.Step, the rig's provider, agents
// and reconcile): a call of warmAllocation on a hand-built list proves
// nothing, because it never runs the pass whose step order is the defect.
//
// The skip test first has a control that runs the same steps with no mark, and
// the Instance goes onto the empty Allocation: a marked case that is passed
// over only means something when the unmarked one is picked.
//
// The outcome test (a Provider at its submit limit) holds for every mark,
// whichever way the Allocation is dealt with, and fails for a skip that is not
// matched by a release.

import (
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	submittedAsOtherCond   = "SubmittedAsOther"
	submittedAsOtherReason = "ReadBackMismatch"
	submittedAsOtherMsg    = "job 1 was established with other settings than requested: time limit 1h0m0s, requested 2h0m0s"
)

// readBackMark sets SubmittedAsOther True on the Allocation, with reason
// ReadBackMismatch and a message in the shape the Provider's controller writes
// (internal/providerctl/observe.go, readBack).
func readBackMark(t *testing.T, rg *rig, name string) {
	t.Helper()
	a, ok := rg.allocation(name)
	if !ok {
		t.Fatalf("Allocation %s is gone", name)
	}
	a.SetConditions(xpv2.Condition{Type: submittedAsOtherCond, Status: corev1.ConditionTrue, Reason: submittedAsOtherReason,
		Message: submittedAsOtherMsg, LastTransitionTime: metav1.NewTime(rg.clock.Now())})
	if err := rg.c.Status().Update(rg.ctx, a); err != nil {
		t.Fatal(err)
	}
}

// warmMarkCase is one mark: how it is written and the reason a failed Instance
// would carry. skipped is true for a mark that warm placement passes over.
type warmMarkCase struct {
	cond    string
	mark    func(*testing.T, *rig, string)
	opts    []rigOption
	skipped bool
}

var warmMarkCases = []warmMarkCase{
	{cond: relayCond, mark: relayMark, opts: []rigOption{withHostDialed()}, skipped: true},
	{cond: submittedAsOtherCond, mark: readBackMark},
}

// warmEmptyThenScaleUp makes one Allocation started, empty and kept (a served
// Instance that was scaled to zero and ended), then optionally marks it, then
// scales the deployment to one Instance and runs ONE pass. It returns the
// Allocation as it was before the pass and the clock's time at the scale-up,
// which every Instance of the pass is created after (the served one before).
func warmEmptyThenScaleUp(t *testing.T, rg *rig, mark func(*testing.T, *rig, string)) (v1alpha1.Allocation, time.Time) {
	t.Helper()
	a, _ := retentionEmpty(t, rg, nil)
	rg.run(time.Minute, 10*time.Second)
	if allocReleased(rg, a.Name) || len(rg.agents) != 1 || len(rg.backend.registrations(a.UID)) == 0 {
		t.Fatalf("the empty Allocation must be kept with its agent registered: released %v, agents %d", allocReleased(rg, a.Name), len(rg.agents))
	}
	if mark != nil {
		mark(t, rg, a.Name)
	}
	scaledAt := rg.clock.Now()
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.clock.Advance(time.Second)
	rg.Step()
	return a, scaledAt
}

// onAllocation counts the deployment's Instances, ended ones included, whose
// Allocation is the named one and that were created at or after t (the
// scale-up, so the Instance that served before it is not counted).
func onAllocation(rg *rig, name string, t time.Time) int {
	n := 0
	for _, in := range rg.instances("md") {
		if in.Spec.AllocationRef.Name == name && !in.CreationTimestamp.Time.Before(t) {
			n++
		}
	}
	return n
}

// The control: with no mark the empty Allocation is warm capacity, so the
// scaled-up Instance goes onto it in the first pass and no job is submitted.
func TestWarmPlacementTakesAnUnmarkedEmptyAllocation(t *testing.T) {
	rg := newRig(t, withHostDialed())
	a, scaledAt := warmEmptyThenScaleUp(t, rg, nil)
	if got := onAllocation(rg, a.Name, scaledAt); got != 1 || rg.submissions() != 1 {
		t.Fatalf("%d Instances placed on the unmarked empty Allocation and %d sbatch calls; want 1 and 1 (the first job only)", got, rg.submissions())
	}
}

// A Running, empty Allocation of the deployment that carries the mark is passed
// over by warm placement in the very pass that sees the mark and the scale-up
// together: the Instance goes to a new Allocation, no Instance is ever placed
// on the marked one, none fails with the mark's reason, and none ends in a
// failure that counts toward RestartLoop.
func TestWarmPlacementSkipsAMarkedEmptyAllocation(t *testing.T) {
	for _, tc := range warmMarkCases {
		if !tc.skipped {
			continue
		}
		t.Run(tc.cond, func(t *testing.T) {
			rg := newRig(t, tc.opts...)
			a, scaledAt := warmEmptyThenScaleUp(t, rg, tc.mark)
			if stored, ok := rg.allocation(a.Name); !ok || stored.GetCondition(xpv2.ConditionType(tc.cond)).Status != corev1.ConditionTrue {
				t.Fatalf("the Allocation does not carry %s True after the pass: the mark was lost, so the case proves nothing", tc.cond)
			}
			if got := onAllocation(rg, a.Name, scaledAt); got != 0 {
				t.Fatalf("%d Instances were placed on the Allocation marked %s in the first pass; want 0", got, tc.cond)
			}
			rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
			// Long enough for a placed Instance to meet step 5 and fail.
			rg.run(5*time.Minute, 10*time.Second)
			live := rg.liveInstances("md")
			if len(live) != 1 || live[0].Status.Phase != v1alpha1.InstanceReady || live[0].Spec.AllocationRef.Name == a.Name {
				t.Fatalf("live Instances %d: %+v; want one ready Instance on a new Allocation, not %s", len(live), live, a.Name)
			}
			if ev := rg.rec.Find("failing", tc.cond); len(ev) != 0 {
				t.Errorf("failing/%s events %+v; want none", tc.cond, ev)
			}
			for _, in := range rg.instances("md") {
				if failedEnd(&in) {
					t.Errorf("Instance %s ended with %q, a failure that counts toward RestartLoop", in.Name, in.Status.EndReason)
				}
			}
			if got := onAllocation(rg, a.Name, scaledAt); got != 0 {
				t.Errorf("%d Instances were placed on the marked Allocation by the end; want 0", got)
			}
			if got := rg.submissions(); got != 2 {
				t.Errorf("sbatch calls = %d, want 2 (the first job and one for the new Instance)", got)
			}
		})
	}
}

// A marked, empty Allocation never leaves a deployment without capacity. The
// Provider's QOS allows one submission per user, so the Allocation holds the
// only slot (slots.go counts every Allocation that has not ended); the
// deployment is scaled from zero to one Instance, and within ten minutes one
// Instance serves, on another Allocation, with the marked one released. This
// holds for every mark, and whichever way the Allocation is dealt with: a
// release (RelayEndedForGood, step 9) or a placement that fails and releases it
// (SubmittedAsOther, step 5). It fails for a skip that nothing releases: the
// deployment then waits on a slot that is held until the Allocation's retention
// date. Only the outcome is asserted, so it holds if SubmittedAsOther gets the
// release too (issue #332).
func TestAMarkedEmptyAllocationDoesNotStarveAProviderAtItsSubmitLimit(t *testing.T) {
	for _, tc := range warmMarkCases {
		t.Run(tc.cond, func(t *testing.T) {
			qos := []v1alpha1.QOSProfile{{Name: "example-s1", MaxSubmitPerUser: i32(1)}}
			opts := append(qosOpts(qos, "", "example-s1"), tc.opts...)
			opts = append(opts, withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 1 }))
			rg := newRig(t, opts...)
			a, _ := warmEmptyThenScaleUp(t, rg, tc.mark)
			if rg.submissions() != 1 {
				t.Fatalf("sbatch calls = %d after the first pass; want 1 (the first job)", rg.submissions())
			}
			rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
			rg.run(5*time.Minute, 10*time.Second)
			live := rg.liveInstances("md")
			if len(live) != 1 || live[0].Status.Phase != v1alpha1.InstanceReady || live[0].Spec.AllocationRef.Name == a.Name {
				t.Fatalf("live Instances %d: %+v; want one ready Instance on an Allocation other than %s", len(live), live, a.Name)
			}
			if !allocReleased(rg, a.Name) {
				t.Errorf("the Allocation marked %s was not released", tc.cond)
			}
		})
	}
}

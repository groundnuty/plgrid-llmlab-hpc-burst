package e2e

// The Provider's start estimates against a real API server (#661; HBEP-4, Estimate; HBEP-10, The release cost):
// rows P1 and P2 of the plan. A Provider's manager runs as the controller binary runs it (NewProviderManager and
// Setup), over the rig of the Allocation event rows (a real slurm.Batch over the fake Slurm), with the real
// clock: the estimate is asked only for a plan not older than 60 s, so the plan's time and the controller's clock
// must be the same one. The estimate itself is the fake Slurm's sbatch --test-only; the apply is real, so the API
// server checks every entry against the CRD and the managed fields show who wrote it. The lifecycle controller's
// side (S4-1, S4-2, S4-3, S4-5) is in internal/lifecycle, and S4-4 in api/v1alpha1/e2e.

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// The probe's start, worked out by hand: epoch 1796017690 is 2026-11-30T05:48:10Z (#661, 16 Nov 2026).
const (
	probeEpoch = "1796017690"
	estPlanA   = "0123456789abcdef"
	estPlanB   = "fedcba9876543210"
)

var probeTime = time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC)

// startEstimateManager starts the rig's Provider manager on the real clock and returns its stop. The rig's own
// manager start (startManager) moves a fake clock on, which would age a plan past the estimate's 60 s in a second.
func startEstimateManager(t *testing.T, r *evRig) (stop func()) {
	t.Helper()
	// The profile decides how the platform's estimate is read: epoch seconds, as Athena's does (#661's probe).
	pp := &v1alpha1.PlatformProfile{}
	if err := c.Get(r.ctx, client.ObjectKey{Name: r.prov + "-profile"}, pp); err != nil {
		t.Fatal(err)
	}
	pp.Spec.StartEstimateTime = v1alpha1.StartEstimateEpochSeconds
	if err := c.Update(r.ctx, pp); err != nil {
		t.Fatal(err)
	}
	mgr, err := providerctl.NewProviderManager(cfg, scheme(), r.prov, providerctl.ManagerOptions{
		LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond, GracefulShutdownTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := r.options()
	opts.Clock = slurm.RealClock
	opts.NewPlatform = func(v slurm.Validated) (providerctl.Platform, io.Closer, error) {
		return slurm.NewBatch(r.faults, v.Profile, v.Settings, slurm.Options{Clock: slurm.RealClock, PollInterval: 300 * time.Millisecond, BusyInterval: 100 * time.Millisecond}), io.NopCloser(nil), nil
	}
	opts.PollInterval, opts.BusyInterval = 300*time.Millisecond, 100*time.Millisecond
	if err := providerctl.Setup(mgr, opts); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(r.ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the manager stopped with %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the manager did not stop within 30 s")
		}
	}
}

// plannedOperation creates a Release Operation and writes a plan for it, as the lifecycle controller would: the
// phase, the plan's ID, the time it was read (now, on the real clock) and one costed target per Allocation, each on
// this rig's Provider.
func plannedOperation(t *testing.T, r *evRig, name, planID string, allocs ...*v1alpha1.Allocation) *v1alpha1.Operation {
	t.Helper()
	names := make([]string, len(allocs))
	for i, a := range allocs {
		names[i] = a.Name
	}
	op := &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.ns},
		Spec: v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "example-md"},
			Release: &v1alpha1.ReleaseParameters{Allocations: names}, Owner: "system:serviceaccount:example-agents:example-agent"},
	}
	if err := c.Create(r.ctx, op); err != nil {
		t.Fatal(err)
	}
	var targets []v1alpha1.PlanTarget
	for _, a := range allocs {
		targets = append(targets, v1alpha1.PlanTarget{Kind: v1alpha1.TargetKindAllocation, Name: a.Name, UID: a.UID, Generation: 1,
			Provider: r.prov, Effect: v1alpha1.PlanEffectReleaseStarted, NeedsApproval: true, Cost: &v1alpha1.ReleaseCost{}})
	}
	op.Status = v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: &v1alpha1.OperationPlan{
		ID: planID, ObservedAt: metav1.Now(),
		ModelDeployment: v1alpha1.PlannedDeployment{Name: "example-md", UID: "00000000-0000-0000-0000-0000000000b1", Generation: 1}, Targets: targets,
	}}
	if err := c.Status().Update(r.ctx, op); err != nil {
		t.Fatal(err)
	}
	return op
}

func getOperation(t *testing.T, r *evRig, name string) *v1alpha1.Operation {
	t.Helper()
	op := &v1alpha1.Operation{}
	if err := c.Get(r.ctx, client.ObjectKey{Namespace: r.ns, Name: name}, op); err != nil {
		t.Fatal(err)
	}
	return op
}

// ownsEstimates is the field managers whose managed fields hold status.estimates.
func ownsEstimates(op *v1alpha1.Operation) []string {
	var out []string
	for _, mf := range op.ManagedFields {
		if mf.FieldsV1 != nil && strings.Contains(string(mf.FieldsV1.Raw), "f:estimates") {
			out = append(out, mf.Manager+"/"+mf.Subresource)
		}
	}
	slices.Sort(out)
	return out
}

// P1: an Operation with two targets of one shape on the Provider gets exactly one Estimate call and one entry that
// covers both targets, written by server-side apply under the Provider's estimate manager.
// P2: a re-plan with a new ID asks again and the entry replaces the old plan's; the same ID again asks nothing.
func TestTheProviderEstimatesOnePerPlanAndShapeAndWritesThemByApply(t *testing.T) {
	// No durable log: the estimate rows read no event, and a rig with the log starts the controller binary as a child process.
	r := newEvRig(t, rigOpts{noLog: true})
	r.sl.SetTestOnlyRaw(probeEpoch)
	a1, a2 := r.newAlloc("est-alloc-1", "30m"), r.newAlloc("est-alloc-2", "30m")
	stop := startEstimateManager(t, r)
	defer stop()

	plannedOperation(t, r, "est-op-1", estPlanA, a1, a2)
	eventually(t, "the Provider's estimate for the plan is written", 60*time.Second, func() bool {
		return len(getOperation(t, r, "est-op-1").Status.Estimates) == 1
	})
	got := getOperation(t, r, "est-op-1")
	est := got.Status.Estimates[0]
	if est.Provider != r.prov || est.PlanID != estPlanA || est.Shape == "" {
		t.Errorf("entry %s/%s/%s, want %s/%s/<a shape>", est.Provider, est.PlanID, est.Shape, r.prov, estPlanA)
	}
	if want := []string{string(a1.UID), string(a2.UID)}; !slices.Equal(sortedCopy(est.Targets), sortedCopy(want)) {
		t.Errorf("targets %v, want both Allocations' UIDs %v", est.Targets, want)
	}
	if est.Start.Time == nil || !est.Start.Time.Equal(&metav1.Time{Time: probeTime}) || est.Start.Unknown != "" || !est.Start.Advisory {
		t.Errorf("start %+v, want %v, advisory", est.Start, probeTime)
	}
	if est.Start.ObservedAt == nil || time.Since(est.Start.ObservedAt.Time) > time.Minute || time.Until(est.Start.ObservedAt.Time) > time.Minute {
		t.Errorf("observedAt %v, want the read time just now", est.Start.ObservedAt)
	}
	if n := r.sl.CountTestOnly(); n != 1 {
		t.Errorf("%d sbatch --test-only calls for two targets of one shape, want exactly 1", n)
	}
	// Who wrote it: this Provider's estimate manager, on the status subresource, and not the shared Provider manager.
	if want := v1alpha1.FieldManagerProviderEstimate(r.prov) + "/status"; !slices.Equal(ownsEstimates(got), []string{want}) {
		t.Errorf("the managers that own status.estimates are %v, want only %s", ownsEstimates(got), want)
	}
	if got.Status.Plan.ID != estPlanA || got.Status.Phase != v1alpha1.OperationPhasePlanned {
		t.Errorf("the plan ID %s or the phase %s changed under the estimate", got.Status.Plan.ID, got.Status.Phase)
	}

	// P2, a re-plan with a new ID: a second call and a new entry that replaces the old plan's.
	replan := getOperation(t, r, "est-op-1")
	replan.Status.Plan.ID, replan.Status.Plan.ObservedAt = estPlanB, metav1.Now()
	if err := c.Status().Update(r.ctx, replan); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the new plan's estimate is written", 60*time.Second, func() bool {
		list := getOperation(t, r, "est-op-1").Status.Estimates
		return len(list) == 1 && list[0].PlanID == estPlanB
	})
	if n := r.sl.CountTestOnly(); n != 2 {
		t.Errorf("%d calls after a re-plan with a new ID, want 2", n)
	}

	// The same ID again, with the phase moved on (an event the controller sees): no call. A second Operation made
	// afterwards, whose estimate arrives, is the barrier: by then the pass for the first has been through.
	again := getOperation(t, r, "est-op-1")
	again.Status.Phase = v1alpha1.OperationPhaseAwaitingApproval
	if err := c.Status().Update(r.ctx, again); err != nil {
		t.Fatal(err)
	}
	plannedOperation(t, r, "est-op-2", estPlanA, a1)
	eventually(t, "the second Operation's estimate is written", 60*time.Second, func() bool {
		return len(getOperation(t, r, "est-op-2").Status.Estimates) == 1
	})
	if n := r.sl.CountTestOnly(); n != 3 {
		t.Errorf("%d calls in all, want 3: one per plan and shape, and none for the same plan ID again", n)
	}
	if list := getOperation(t, r, "est-op-1").Status.Estimates; len(list) != 1 || list[0].PlanID != estPlanB {
		t.Errorf("the first Operation's entries %+v, want the new plan's one", list)
	}
}

func sortedCopy(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

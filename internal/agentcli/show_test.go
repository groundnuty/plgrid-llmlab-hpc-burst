package agentcli

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The goldens are written by hand from docs/agent-cli.md: they are not the printer's output.
func TestOperationJSONGoldens(t *testing.T) {
	endOp := opObject("end-m", userA, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "m"}}, v1alpha1.OperationStatus{
		Phase: v1alpha1.OperationPhasePlanned,
		Plan: &v1alpha1.OperationPlan{ID: "fedcba9876543210", ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m", Generation: 4},
			Targets: []v1alpha1.PlanTarget{{Kind: v1alpha1.TargetKindModelDeployment, Name: "m", UID: "uid-m", Effect: v1alpha1.PlanEffectSetEndTime,
				Cost: &v1alpha1.ReleaseCost{Note: "no HPC capacity held"}}}},
		Conditions: []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonNotConfirmed,
			Message: "planned: waiting for the owner to confirm plan fedcba9876543210", LastTransitionTime: metav1.NewTime(t0.Add(time.Second))}},
	})
	reportSpec := v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "m"},
		Release: &v1alpha1.ReleaseParameters{Allocations: []string{"alloc-a", "alloc-b"}}, Confirm: &v1alpha1.OperationConfirm{PlanID: "0011223344556677"}}
	reportOp := opObject("rel-m", userA, reportSpec, v1alpha1.OperationStatus{
		Phase: v1alpha1.OperationPhasePartlyFailed,
		Plan: &v1alpha1.OperationPlan{ID: "0011223344556677", ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m", Generation: 2},
			Targets: []v1alpha1.PlanTarget{
				{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-a", UID: "uid-alloc-a", Provider: "helios", Effect: v1alpha1.PlanEffectReleaseKept},
				{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-b", UID: "uid-alloc-b", Provider: "helios", Effect: v1alpha1.PlanEffectReleaseKept}}},
		Conditions: []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: "PartlyFailed",
			Message: "1 of 2 targets failed", LastTransitionTime: metav1.NewTime(t0.Add(time.Hour))}},
		Targets: []v1alpha1.TargetOutcome{
			{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-a", UID: "uid-alloc-a", State: v1alpha1.TargetStateDone, LastTransitionTime: metav1.NewTime(t0.Add(30 * time.Minute))},
			{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-b", UID: "uid-alloc-b", State: v1alpha1.TargetStateFailed, Reason: "CancelRefused",
				Message: "the provider refused the cancel", LastTransitionTime: metav1.NewTime(t0.Add(time.Hour))},
		}})
	reportOp.Namespace = "default"

	for _, r := range []struct {
		golden string
		op     *v1alpha1.Operation
		now    time.Duration
		ns     string
	}{
		{"operation-scale.json", scaleOperation(), 2 * time.Minute, testNS},
		{"operation-end.json", endOp, 2 * time.Minute, testNS},
		{"operation-report.json", reportOp, 3 * time.Hour, "default"},
	} {
		t.Run(r.golden, func(t *testing.T) {
			w := newWorld(t, []client.Object{r.op})
			w.now = t0.Add(r.now)
			if code := w.run("", "operation", r.op.Name, "-n", r.ns, "--json"); code != exitOK {
				t.Fatalf("exit %d, stderr %q", code, w.errs.String())
			}
			sameJSON(t, w.out.Bytes(), readGolden(t, r.golden))
		})
	}
}

func TestOperationText(t *testing.T) {
	w := newWorld(t, []client.Object{scaleOperation()})
	w.now = t0.Add(2 * time.Minute)
	if code := w.run("", "operation", "scale-m"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	out := w.out.String()
	contains(t, "stdout", out,
		"operation scale-m (namespace team): Scale ModelDeployment m to 0 Instances",
		"owner: "+userB, "purpose: free the slot for the nightly run", "phase: AwaitingApproval", "confirmed plan: "+planAB,
		"condition Executable=False AwaitingApproval: Allocation alloc-p: needs an approval under provider.sano, global",
		"Allocation alloc-p (provider sano): CancelPending, needs approval (cancel a pending Allocation)",
		"policy provider.sano: approvers serviceaccount hpcburst-agents/agent-a",
		"policy global: approvers user admin, group operators",
		"queue age: 48h0m0s (observed 2026-11-20T10:00:00Z)",
		// An unknown figure says why, with its observation time, and an estimate says advisory.
		"start estimate (advisory): unknown: the platform gives no start estimate for the job (observed 2026-11-20T10:00:00Z)",
		"provider queue waits: median 3h0m0s, p90 70h0m0s, max 72h0m0s, of 5 (observed 2026-11-20T09:59:30Z)",
		"start to ready: unknown: not measured yet; the controller plans with 30 min (observed 2026-11-20T10:00:00Z)",
		"estimate: unknown: no estimate within 60 s",
		"Instance inst-r (provider sano): DrainLastReady, needs approval",
		"lease left: 9h30m0s (observed 2026-11-20T10:00:00Z)", "GPU-hours left: 38.0 (observed 2026-11-20T10:00:00Z)",
		"last ready Instance: the model serves nothing on its own capacity",
		"Allocation alloc-r (provider sano): Keep, needs no approval")
	lacks(t, "stdout", out, "the estimate is in status.estimates", "confirm with:")
}

// The estimates the Provider's controller applied (status.estimates[], #661) show on the plan's targets in --json: a known one with
// its time and age, and a failed one as a final unknown with its reason, whatever the age. A target no entry covers reads as it
// did before. The expected objects are written by hand from docs/agent-cli.md (the same shapes as testdata/estimate-states.json),
// never taken from the printer; the clock is read once.
func TestOperationJSONShowsTheEstimatesTheProviderApplied(t *testing.T) {
	op := scaleOperation() // plan planAB, observed at t0; alloc-p, inst-r and alloc-r on sano
	start, read, failedRead := mt(time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)), mt(t0.Add(10*time.Second)), mt(t0.Add(5*time.Second))
	op.Status.Estimates = []v1alpha1.OperationEstimate{
		{Provider: "sano", PlanID: planAB, Shape: "aaaaaaaaaaaaaaaa", Targets: []string{"uid-alloc-p"},
			Start: v1alpha1.TimeFigure{Time: start, ObservedAt: read, Advisory: true}},
		{Provider: "sano", PlanID: planAB, Shape: "bbbbbbbbbbbbbbbb", Targets: []string{"uid-inst-r"},
			Start: v1alpha1.TimeFigure{Unknown: "the estimate command gave no start", ObservedAt: failedRead, Advisory: true}},
		// An older plan's entry for the same target, read LATER with another time: it is not the current plan's, so it is not shown
		// (the latest read would win among the current plan's entries, never over another plan's).
		{Provider: "sano", PlanID: "0000000000000000", Shape: "cccccccccccccccc", Targets: []string{"uid-alloc-p"},
			Start: v1alpha1.TimeFigure{Time: mt(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)), ObservedAt: mt(t0.Add(30 * time.Second)), Advisory: true}},
	}
	w := newWorld(t, []client.Object{op})
	w.now = t0.Add(70 * time.Second)
	if code := w.run("", "operation", "scale-m", "--json"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	var got struct {
		Operation struct {
			Plan struct {
				Targets []struct {
					Name     string          `json:"name"`
					Estimate json.RawMessage `json:"estimate"`
				} `json:"targets"`
			} `json:"plan"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(w.out.Bytes(), &got); err != nil {
		t.Fatalf("the output is not JSON: %v\n%s", err, w.out.String())
	}
	want := map[string]string{
		"alloc-p": `{"state": "known", "time": "2026-11-23T08:00:00Z", "observedAt": "2026-11-20T10:00:10Z", "age": "1m0s", "advisory": true}`,
		"inst-r":  `{"state": "unknown", "unknown": "the estimate command gave no start", "observedAt": "2026-11-20T10:00:05Z", "advisory": true}`,
		"alloc-r": ``, // no cost: no estimate, whatever an entry says
	}
	if len(got.Operation.Plan.Targets) != len(want) {
		t.Fatalf("%d targets in the plan's JSON, want %d", len(got.Operation.Plan.Targets), len(want))
	}
	for _, tg := range got.Operation.Plan.Targets {
		w, ok := want[tg.Name]
		switch {
		case !ok:
			t.Errorf("unexpected target %s", tg.Name)
		case w == "" && len(tg.Estimate) != 0:
			t.Errorf("%s shows an estimate %s, want none", tg.Name, tg.Estimate)
		case w != "":
			sameJSON(t, tg.Estimate, []byte(w))
		}
	}
	// The control: the same Operation without the entries shows no known estimate, so the rows above come from the entries.
	bare := scaleOperation()
	wb := newWorld(t, []client.Object{bare})
	wb.now = t0.Add(70 * time.Second)
	if code := wb.run("", "operation", "scale-m", "--json"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, wb.errs.String())
	}
	if strings.Contains(wb.out.String(), `"state":"known"`) || strings.Contains(wb.out.String(), `"state": "known"`) {
		t.Errorf("an Operation with no status.estimates shows a known estimate:\n%s", wb.out.String())
	}
}

// A figure is never shown as a zero when its value is not known.
func TestUnknownFiguresAreNeverZero(t *testing.T) {
	plan := &v1alpha1.OperationPlan{ID: planAB, ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m"},
		Targets: []v1alpha1.PlanTarget{{Kind: v1alpha1.TargetKindAllocation, Name: "a", UID: "uid-a", Provider: "sano", Effect: v1alpha1.PlanEffectReleaseKept, Cost: &v1alpha1.ReleaseCost{
			LeaseLeft:    &v1alpha1.DurationFigure{Unknown: "the job's hard end is not observed yet", ObservedAt: mt(t0)},
			GPUHoursLeft: &v1alpha1.GPUHoursFigure{Unknown: "the job's hard end is not observed yet", ObservedAt: mt(t0)},
			QueueAge:     &v1alpha1.DurationFigure{ObservedAt: mt(t0)}, // neither a value nor a reason
			QueueWaits:   &v1alpha1.QueueWaitsFigure{Unknown: "the Provider has recorded no queue waits yet"},
			StartToReady: &v1alpha1.DurationFigure{Duration: dur(0), ObservedAt: mt(t0)}, // a zero that is a value: 0s is shown as it is
		}}}}
	op := opObject("rel", userA, v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "m"}}, v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: plan})
	w := newWorld(t, []client.Object{op})
	w.now = t0.Add(10 * time.Second)
	if code := w.run("", "operation", "rel"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	contains(t, "stdout", w.out.String(),
		"lease left: unknown: the job's hard end is not observed yet (observed 2026-11-20T10:00:00Z)",
		"GPU-hours left: unknown: the job's hard end is not observed yet",
		"queue age: unknown: the plan records neither a value nor a reason",
		"provider queue waits: unknown: the Provider has recorded no queue waits yet",
		"start to ready: 0s",
		"estimate: pending")
	// "0s" appears once, for the figure whose value is zero; no unknown figure turned into one.
	if n := strings.Count(w.out.String(), "0s"); n != 1 {
		t.Errorf("%d occurrences of \"0s\" in\n%s", n, w.out.String())
	}
}

func TestOperationNotFound(t *testing.T) {
	w := newWorld(t, nil)
	if code := w.run("", "operation", "absent"); code != exitFail {
		t.Errorf("exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "Operation absent not found in namespace team")
}

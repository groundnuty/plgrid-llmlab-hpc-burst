package agentcli

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func mt(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

func dur(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

// agentAPolicy and globalPolicy are the policies of the fixture: A approves under the Provider's, the
// person (and a group of operators) under the global one.
func policies() []v1alpha1.AppliedPolicy {
	return []v1alpha1.AppliedPolicy{
		{Name: "provider.sano", Approvers: []v1alpha1.Approver{{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}}},
		{Name: "global", Approvers: []v1alpha1.Approver{{User: "admin"}, {Group: "operators"}}},
	}
}

// scalePlan is the plan of testdata/operation-scale.json: a scale to zero of a deployment with a pending
// Allocation and one ready Instance, which cancels the first and drains the second, both under two
// policies.
func scalePlan() *v1alpha1.OperationPlan {
	waits := &v1alpha1.QueueWaitsFigure{Median: dur(3 * time.Hour), P90: dur(70 * time.Hour), Max: dur(72 * time.Hour), Count: 5, ObservedAt: mt(t0.Add(-30 * time.Second))}
	return &v1alpha1.OperationPlan{
		ID: planAB, ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m", Generation: 4},
		Targets: []v1alpha1.PlanTarget{
			{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-p", UID: "uid-alloc-p", Provider: "sano", Effect: v1alpha1.PlanEffectCancelPending,
				NeedsApproval: true, ApprovalRule: "cancel a pending Allocation", Policies: policies(),
				Cost: &v1alpha1.ReleaseCost{
					QueueAge:      &v1alpha1.DurationFigure{Duration: dur(48 * time.Hour), ObservedAt: mt(t0)},
					StartEstimate: &v1alpha1.TimeFigure{Unknown: "the platform gives no start estimate for the job", ObservedAt: mt(t0), Advisory: true},
					QueueWaits:    waits,
					StartToReady:  &v1alpha1.DurationFigure{Unknown: "not measured yet; the controller plans with 30 min", ObservedAt: mt(t0)},
					// The plan's placeholder: it is never shown, the target's estimate replaces it.
					EstimateNow: &v1alpha1.TimeFigure{Unknown: "the estimate operation is beta step 4", ObservedAt: mt(t0), Advisory: true},
				}},
			{Kind: v1alpha1.TargetKindInstance, Name: "inst-r", UID: "uid-inst-r", Provider: "sano", Effect: v1alpha1.PlanEffectDrainLastReady,
				NeedsApproval: true, ApprovalRule: "drain the last ready Instance", Policies: policies(),
				Cost: &v1alpha1.ReleaseCost{
					LeaseLeft:    &v1alpha1.DurationFigure{Duration: dur(9*time.Hour + 30*time.Minute), ObservedAt: mt(t0)},
					GPUHoursLeft: &v1alpha1.GPUHoursFigure{GPUHours: "38.0", ObservedAt: mt(t0)},
					LastReady:    true,
					QueueWaits:   waits,
				}},
			{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-r", UID: "uid-alloc-r", Provider: "sano", Effect: v1alpha1.PlanEffectKeep},
		},
	}
}

// scaleOperation is the stored Operation of testdata/operation-scale.json, owned by agent B, waiting.
func scaleOperation() *v1alpha1.Operation {
	spec := scaleSpec("m", 0)
	spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planAB}
	op := opObject("scale-m", userB, spec, v1alpha1.OperationStatus{
		Phase: v1alpha1.OperationPhaseAwaitingApproval, Plan: scalePlan(),
		Conditions: []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonAwaitingApproval,
			Message: "Allocation alloc-p: needs an approval under provider.sano, global", LastTransitionTime: metav1.NewTime(t0.Add(5 * time.Minute))}},
	})
	op.Annotations = map[string]string{v1alpha1.AnnotationPurpose: "free the slot for the nightly run"}
	return op
}

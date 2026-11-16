package lifecycle

// What needs approval (HBEP-10, What needs approval; #589 plan, section 9). Each effect of a plan has a
// rule, one row of HBEP-10's table, and the rule that made a target need approval, or not, is written in
// the target's approvalRule.

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The rules, by the table's rows. EndStarting is the erratum's (HBEP-10, Implementation history, 16 Nov
// 2026): an Instance still starting on a started Allocation, ended at once, has the drain's rule.
const (
	ruleCancelPending  = "Cancel a pending Allocation, other than a hedge loser"
	ruleDrainLastReady = "Drain a deployment's last ready Instance before its end time"
	ruleReleaseStarted = "Release a started Allocation before its hard end, outside the spec"
	ruleReleaseKept    = "Release an Allocation that its retention policy keeps"
	ruleDrainReleased  = "Drain an Instance whose Allocation its retention then releases before its hard end: under ReleaseWhenIdle, a keep date before the hard end, or the profile's maxIdle"
	ruleDrainKept      = "Drain an Instance while another stays ready, its Allocation kept until its hard end"
	ruleEndReleased    = "End an Instance still starting, as a drain: its Allocation's retention then releases it before its hard end (ReleaseWhenIdle, a keep date before the hard end, or the profile's maxIdle)"
	ruleEndKept        = "End an Instance still starting, as a drain: its Allocation is kept until its hard end"
	ruleKeep           = "Keep an Allocation under its retention policy"
	ruleSetEndTime     = "Set the end time to now: the consequences are separate targets"
	ruleDeleteMD       = "Delete the ModelDeployment: the consequences are separate targets"
	ruleOverAllocation = "over-allocation's own trim under the agreed spec"
)

// releasesBeforeHardEnd is the structural test of the drain's row (HBEP-10, What needs approval): whether
// the Allocation's retention would release it before its hard end once its Instances are gone. It never
// reads the clock, so the answer, and with it the plan's ID, does not change as time passes. It asks more
// often than the clock-based reading would, never less (the science agent, 15 Nov 2026, Q11):
//   - the deployment is deleted, or the Allocation is unknown;
//   - the policy is ReleaseWhenIdle, or the profile sets a maxIdle;
//   - the hard end is not observed yet;
//   - the keep date, the policy's until, else the deployment's end time, is before the hard end. An End
//     that is not a delete sets the end time to now, which is before the hard end whenever the policy
//     names no later date (endsNow).
func releasesBeforeHardEnd(a *v1alpha1.Allocation, v fateView, endsNow bool) bool {
	if a == nil || v.deleted {
		return true
	}
	pol := a.Spec.RetentionPolicy
	if pol.EffectiveMode() == v1alpha1.RetentionReleaseWhenIdle || v.maxIdle != nil {
		return true
	}
	he := a.Status.AtProvider.HardEnd
	if he == nil {
		return true
	}
	if pol != nil && pol.Until != nil {
		return pol.Until.Time.Before(he.Time)
	}
	if endsNow {
		return true
	}
	return v.endTime != nil && v.endTime.Before(he.Time)
}

// instanceNeeds is whether an effect on an Instance needs approval, and the rule that says so.
func instanceNeeds(effect v1alpha1.PlanEffect, a *v1alpha1.Allocation, v fateView, endsNow bool) (bool, string) {
	switch effect {
	case v1alpha1.PlanEffectCancelPending:
		return true, ruleCancelPending
	case v1alpha1.PlanEffectDrainLastReady:
		return true, ruleDrainLastReady
	case v1alpha1.PlanEffectEndStarting:
		if releasesBeforeHardEnd(a, v, endsNow) {
			return true, ruleEndReleased
		}
		return false, ruleEndKept
	}
	// DrainInstance, another Instance staying ready.
	if releasesBeforeHardEnd(a, v, endsNow) {
		return true, ruleDrainReleased
	}
	return false, ruleDrainKept
}

// allocationNeeds is whether an effect on an Allocation needs approval, and the rule that says so.
func allocationNeeds(effect v1alpha1.PlanEffect) (bool, string) {
	switch effect {
	case v1alpha1.PlanEffectCancelPending:
		return true, ruleCancelPending
	case v1alpha1.PlanEffectReleaseStarted:
		return true, ruleReleaseStarted
	case v1alpha1.PlanEffectReleaseKept:
		return true, ruleReleaseKept
	}
	return false, ruleKeep
}

// allocationFate is what an Allocation that a plan empties becomes, by retention's own rules
// (retentionFate) at the plan's time: released at once (cancelled when its job has not started, released
// when it has), or kept, with the date retention next looks at it. The Allocation is emptied at at.
func allocationFate(a *v1alpha1.Allocation, v fateView, at time.Time) (v1alpha1.PlanEffect, time.Time) {
	release, until, _, needIdle := retentionFate(a, v, at, nil)
	if needIdle {
		release, until, _, _ = retentionFate(a, v, at, &at)
	}
	switch {
	case release && a.Status.AtProvider.StartTime == nil:
		return v1alpha1.PlanEffectCancelPending, time.Time{}
	case release:
		return v1alpha1.PlanEffectReleaseStarted, time.Time{}
	}
	return v1alpha1.PlanEffectKeep, until
}

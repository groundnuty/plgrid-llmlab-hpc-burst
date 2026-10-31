package lifecycle

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// retention applies each empty Allocation's retention policy (HBEP-7,
// Reconcile loop, step 9; Retention). An Allocation is empty when it hosts no
// live Instance:
//   - one that has not started is cancelled: nothing waits for it;
//   - one that ended, or failed, is released;
//   - a running one is kept as warm capacity until its policy's date (the
//     deployment's end time by default, or at once once the deployment is
//     gone), or released after its idle limit under release-when-idle; the
//     profile's maxIdle releases it even under keep. None outlives its hard
//     end.
func (p *pass) retention() error {
	present := map[string]bool{}
	for name := range p.allocs {
		present[name] = true
	}
	p.r.Backend.prune(p.key.Namespace, p.key.Name, present)
	for _, a := range p.allocs {
		if !a.DeletionTimestamp.IsZero() {
			continue
		}
		if len(p.liveOn(a.Name)) > 0 {
			p.mem.busy(a.UID)
			continue
		}
		release, why := p.releaseEmpty(a)
		if !release {
			continue
		}
		if err := p.deleteAllocation(a, why); err != nil {
			return err
		}
	}
	return nil
}

// releaseEmpty decides whether an empty Allocation goes now, and why.
func (p *pass) releaseEmpty(a *v1alpha1.Allocation) (bool, string) {
	obs := a.Status.AtProvider
	switch obs.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true, fmt.Sprintf("its job is %s", obs.Phase)
	}
	if obs.StartTime == nil {
		// An Instance is created before its Allocation, so an unstarted
		// Allocation with no live Instance waits for nothing.
		return true, "a pending Allocation with no Instance is cancelled"
	}
	if obs.HardEnd != nil && !p.now.Before(obs.HardEnd.Time) {
		return true, "its hard end passed"
	}
	if obs.HardEnd != nil {
		p.at(obs.HardEnd.Time)
	}
	idle := p.mem.idleSince(a.UID, p.now)
	pol := a.Spec.RetentionPolicy
	var maxIdle *time.Duration
	if pv := p.pvOf(a.Labels); pv != nil && pv.profile != nil {
		maxIdle = pv.settings.maxIdle
	}
	if maxIdle != nil {
		if t := idle.Add(*maxIdle); !p.now.Before(t) {
			return true, fmt.Sprintf("idle for the profile's maxIdle %s", *maxIdle)
		} else {
			p.at(t)
		}
	}
	switch pol.EffectiveMode() {
	case v1alpha1.RetentionReleaseWhenIdle:
		var limit time.Duration
		if pol.IdleLimit != nil {
			d, err := pol.IdleLimit.AsDuration()
			if err == nil {
				limit = d
			}
		}
		if t := idle.Add(limit); !p.now.Before(t) {
			return true, fmt.Sprintf("released when idle, after %s", limit)
		} else {
			p.at(t)
		}
		return false, ""
	default:
		until := p.keepUntil(pol)
		if until == nil {
			return false, ""
		}
		if !p.now.Before(*until) {
			return true, fmt.Sprintf("kept until %s", until.UTC().Format(time.RFC3339))
		}
		p.at(*until)
		return false, ""
	}
}

// keepUntil is the date a kept Allocation is kept until: the policy's date,
// else the deployment's end time; once the deployment is gone, its end has
// come. Nil means until the hard end.
func (p *pass) keepUntil(pol *v1alpha1.RetentionPolicy) *time.Time {
	if pol != nil && pol.Until != nil {
		t := pol.Until.Time
		return &t
	}
	if p.md == nil || !p.md.DeletionTimestamp.IsZero() {
		t := p.now
		return &t
	}
	if p.md.Spec.EndTime != nil {
		t := p.md.Spec.EndTime.Time
		return &t
	}
	return nil
}

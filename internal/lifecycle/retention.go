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
//   - one that ended, or failed, is released, as is a running one one of
//     whose host-dialed relays ended for good (RelayEndedForGood);
//   - a running one of a deployment that is gone or being deleted is released
//     at once, whatever its policy (HBEP-7, Release; issue #503): the policy
//     is for a deployment that still exists;
//   - a running one of a deployment that exists is kept as warm capacity until
//     its policy's date (the deployment's end time by default), or released
//     after its idle limit under release-when-idle; the profile's maxIdle
//     releases it even under keep. None outlives its hard end.
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
			if _, idle, _ := v1alpha1.IdleSince(a); idle {
				if err := p.writeIdleSince(a, nil); err != nil {
					return err
				}
			}
			continue
		}
		release, why, err := p.releaseEmpty(a)
		if err != nil {
			return err
		}
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
func (p *pass) releaseEmpty(a *v1alpha1.Allocation) (bool, string, error) {
	obs := a.Status.AtProvider
	switch obs.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true, fmt.Sprintf("its job is %s", obs.Phase), nil
	}
	if c := a.GetCondition(v1alpha1.ConditionRelayEndedForGood); c.Status == "True" {
		// Kept capacity that nothing can reach: no Instance placed on it
		// could serve, and its job would hold its GPU until the policy's
		// date (issue #304).
		return true, "a relay ended for good: " + c.Message, nil
	}
	if obs.StartTime == nil {
		// An Instance is created before its Allocation, so an unstarted
		// Allocation with no live Instance waits for nothing.
		return true, "a pending Allocation with no Instance is cancelled", nil
	}
	if p.md == nil || !p.md.DeletionTimestamp.IsZero() {
		// A user who deletes a deployment releases its GPUs, whatever its policy (HBEP-7, Release; the science agent's ruling on
		// #503, 13 Nov 2026): release-when-idle's limit, like keep's date, is for a deployment that still exists. Before this, only
		// keep was cut by the deletion (keepUntil), and a release-when-idle job ran on for its whole idle limit, 30 minutes in the
		// example, holding the owner's shared slot.
		return true, "its deployment was deleted", nil
	}
	if obs.HardEnd != nil && !p.now.Before(obs.HardEnd.Time) {
		return true, "its hard end passed", nil
	}
	if obs.HardEnd != nil {
		p.at(obs.HardEnd.Time)
	}
	idle, err := p.idleAnchor(a)
	if err != nil {
		return false, "", err
	}
	pol := a.Spec.RetentionPolicy
	var maxIdle *time.Duration
	if pv := p.pvOf(a.Labels); pv != nil && pv.profile != nil {
		maxIdle = pv.settings.maxIdle
	}
	if maxIdle != nil {
		if t := idle.Add(*maxIdle); !p.now.Before(t) {
			return true, fmt.Sprintf("idle for the profile's maxIdle %s", *maxIdle), nil
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
			return true, fmt.Sprintf("released when idle, after %s", limit), nil
		} else {
			p.at(t)
		}
		return false, "", nil
	default:
		until := p.keepUntil(pol)
		if until == nil {
			return false, "", nil
		}
		if !p.now.Before(*until) {
			return true, fmt.Sprintf("kept until %s", until.UTC().Format(time.RFC3339)), nil
		}
		p.at(*until)
		return false, "", nil
	}
}

// idleAnchor returns when the Allocation became idle, from its idle-since
// annotation, which only the lifecycle controller writes (v1alpha1.IdleSince):
// the latest EndTime of its Instances, or its job's start when no Instance was
// ever placed on it. A missing annotation is written now, so the anchor
// survives a controller restart and the deletion of the Instances.
func (p *pass) idleAnchor(a *v1alpha1.Allocation) (time.Time, error) {
	t, ok, err := v1alpha1.IdleSince(a)
	if err == nil && ok {
		return t, nil
	}
	anchor := time.Time{}
	hosted := false
	for _, in := range p.insts {
		if in.Spec.AllocationRef.Name != a.Name {
			continue
		}
		hosted = true
		if in.Status.EndTime != nil && in.Status.EndTime.After(anchor) {
			anchor = in.Status.EndTime.Time
		}
	}
	switch {
	case !hosted && a.Status.AtProvider.StartTime != nil:
		anchor = a.Status.AtProvider.StartTime.Time
	case anchor.IsZero():
		anchor = p.now // the Instances are gone without an end time: count from now, never earlier
	}
	if err := p.writeIdleSince(a, &anchor); err != nil {
		return time.Time{}, err
	}
	return anchor.UTC().Truncate(time.Second), nil
}

// writeIdleSince sets (t non-nil) or removes the Allocation's idle-since
// annotation, with a resourceVersion precondition.
func (p *pass) writeIdleSince(a *v1alpha1.Allocation, t *time.Time) error {
	next := a.DeepCopy()
	if t != nil {
		v1alpha1.SetIdleSince(next, *t)
	} else {
		v1alpha1.ClearIdleSince(next)
	}
	if err := p.r.Client.Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("update Allocation %s idle-since: %w", a.Name, err)
	}
	*a = *next
	return nil
}

// keepUntil is the date a kept Allocation of a deployment that exists is kept
// until: the policy's date, else the deployment's end time. Nil means until the
// hard end. A deployment that is gone or being deleted never gets here: its empty
// Allocation is released at once (releaseEmpty).
func (p *pass) keepUntil(pol *v1alpha1.RetentionPolicy) *time.Time {
	if pol != nil && pol.Until != nil {
		t := pol.Until.Time
		return &t
	}
	if p.md != nil && p.md.Spec.EndTime != nil {
		t := p.md.Spec.EndTime.Time
		return &t
	}
	return nil
}

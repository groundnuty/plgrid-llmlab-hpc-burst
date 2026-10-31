package lifecycle

import (
	"fmt"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
	for _, uid := range p.mem.goneAllocs(present) {
		// The Allocation is gone: its credential has nothing left to
		// authenticate (docs/development.md, Runtime conventions).
		if err := os.Remove(agentapi.AllocationCredentialPath(p.r.Config.CredentialsDir, string(uid))); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove a released Allocation's credential: %w", err)
		}
	}
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
	if obs.StartTime == nil {
		// An Instance is created before its Allocation, so an unstarted
		// Allocation with no live Instance waits for nothing.
		return true, "a pending Allocation with no Instance is cancelled", nil
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

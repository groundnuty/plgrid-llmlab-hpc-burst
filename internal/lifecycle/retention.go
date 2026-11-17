package lifecycle

import (
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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

// releaseEmpty decides whether an empty Allocation goes now, and why. The decision is retentionFate's, a
// pure function; what stays here are the two side effects the old body made in the middle of it: the
// idle anchor's annotation write (idleAnchor), made only once the earlier rules left the decision open, as
// before, and the requeue (p.at).
func (p *pass) releaseEmpty(a *v1alpha1.Allocation) (bool, string, error) {
	view := p.retentionView(a)
	release, at, why, needIdle := retentionFate(a, view, p.now, nil)
	if needIdle {
		idle, err := p.idleAnchor(a)
		if err != nil {
			return false, "", err
		}
		release, at, why, _ = retentionFate(a, view, p.now, &idle)
	}
	if !at.IsZero() {
		p.at(at)
	}
	return release, why, nil
}

// fateView is what retention reads besides the Allocation: whether its deployment is gone or being
// deleted, the deployment's end time, and the profile's maxIdle for the Allocation's Provider. A plan's
// view differs from the pass's own where the Operation changes the deployment (End sets its end time to
// now, or deletes it).
type fateView struct {
	// deleted: the deployment is gone, or is being deleted.
	deleted bool
	// heldByKeptWarm: the deployment is being deleted and holds FinalizerKeptWarm, so an Operation's End with delete is
	// deleting it, not a person (keptWarmSkipsDeletion).
	heldByKeptWarm bool
	// endTime is the deployment's end time, nil when it has none.
	endTime *time.Time
	// maxIdle is the profile's idle limit that releases even a kept Allocation; nil when the Provider's
	// profile sets none (or is not read).
	maxIdle *time.Duration
}

// retentionView is the pass's view for an Allocation.
func (p *pass) retentionView(a *v1alpha1.Allocation) fateView {
	v := fateView{deleted: p.md == nil || !p.md.DeletionTimestamp.IsZero()}
	v.heldByKeptWarm = p.md != nil && !p.md.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(p.md, v1alpha1.FinalizerKeptWarm)
	if p.md != nil && p.md.Spec.EndTime != nil {
		t := p.md.Spec.EndTime.Time
		v.endTime = &t
	}
	if pv := p.pvOf(a.Labels); pv != nil && pv.profile != nil {
		v.maxIdle = pv.settings.maxIdle
	}
	return v
}

// keptWarm reports whether the Allocation carries the kept-warm mark: its job started after an Operation committed to cancel it
// as queued, and no rule but the profile's maxIdle, its hard end, HBEP-7's unconditional releases and a person's direct delete of
// its deployment releases it (HBEP-10, The report, as amended by #698).
func keptWarm(a *v1alpha1.Allocation) bool { return a.Annotations[v1alpha1.AnnotationKeptWarm] != "" }

// keptWarmSkipsDeletion is the ONE function that tells an Operation's delete of a deployment from a person's (HBEP-10, The
// report, as amended by #698): a deployment that is being deleted AND holds FinalizerKeptWarm is an Operation's End with
// delete, so its marked Allocations skip retention's deletion rule; a deployment that is gone, or is being deleted WITHOUT the
// finalizer, was deleted by a person, a person's act outside the guard by design, and the deletion rule releases its marked
// Allocations with retention's cause.
func keptWarmSkipsDeletion(a *v1alpha1.Allocation, v fateView) bool {
	return keptWarm(a) && v.heldByKeptWarm
}

// retentionFate is the pure decision of releaseEmpty: whether the empty Allocation a goes at now, why, and
// at, the earliest later time at which the answer can change (the zero time when none can). at is set
// also when release is true: the old body asked for a pass at the hard end before it ruled on idleness,
// so a release by maxIdle still carried that request. emptiedAt is the idle anchor, when the Allocation
// was emptied. Only the idleness rules read it, so a nil emptiedAt on a decision that needs it returns
// needIdle with nothing decided: the caller computes the anchor (a write) and calls again. The rules, in
// order, are HBEP-7's, Retention (see retention).
func retentionFate(a *v1alpha1.Allocation, v fateView, now time.Time, emptiedAt *time.Time) (release bool, at time.Time, why string, needIdle bool) {
	obs := a.Status.AtProvider
	switch obs.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true, time.Time{}, fmt.Sprintf("its job is %s", obs.Phase), false
	}
	if c := a.GetCondition(v1alpha1.ConditionRelayEndedForGood); c.Status == "True" {
		// Kept capacity that nothing can reach: no Instance placed on it
		// could serve, and its job would hold its GPU until the policy's
		// date (issue #304).
		// The condition's message, the Provider controller's, stays out of the release cause.
		return true, time.Time{}, "a relay ended for good", false
	}
	if obs.StartTime == nil {
		// An Instance is created before its Allocation, so an unstarted
		// Allocation with no live Instance waits for nothing.
		return true, time.Time{}, "a pending Allocation with no Instance is cancelled", false
	}
	if v.deleted && !keptWarmSkipsDeletion(a, v) {
		// A user who deletes a deployment releases its GPUs, whatever its policy (HBEP-7, Release; the science agent's ruling on
		// #503, 13 Nov 2026): release-when-idle's limit, like keep's date, is for a deployment that still exists. Before this, only
		// keep was cut by the deletion (keepUntil), and a release-when-idle job ran on for its whole idle limit, 30 minutes in the
		// example, holding the owner's shared slot.
		return true, time.Time{}, "its deployment was deleted", false
	}
	if obs.HardEnd != nil && !now.Before(obs.HardEnd.Time) {
		return true, time.Time{}, "its hard end passed", false
	}
	if obs.HardEnd != nil {
		at = obs.HardEnd.Time
	}
	if emptiedAt == nil {
		return false, time.Time{}, "", true
	}
	idle := *emptiedAt
	later := func(t time.Time) {
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	pol := a.Spec.RetentionPolicy
	if v.maxIdle != nil {
		if t := idle.Add(*v.maxIdle); !now.Before(t) {
			return true, at, fmt.Sprintf("idle for the profile's maxIdle %s", *v.maxIdle), false
		} else {
			later(t)
		}
	}
	if keptWarm(a) {
		// Kept warm: the profile's maxIdle, its hard end and the unconditional releases above are all that release it, and
		// every other rule, the idle limit, a keep date and the deployment's end time, skips it (HBEP-10, The report, as
		// amended by #698).
		return false, at, "", false
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
		if t := idle.Add(limit); !now.Before(t) {
			return true, at, fmt.Sprintf("released when idle, after %s", limit), false
		} else {
			later(t)
		}
		return false, at, "", false
	default:
		until := keepUntilOf(pol, v.endTime)
		if until == nil {
			return false, at, "", false
		}
		if !now.Before(*until) {
			return true, at, fmt.Sprintf("kept until %s", until.UTC().Format(time.RFC3339)), false
		}
		later(*until)
		return false, at, "", false
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

// keepUntilOf is the date a kept Allocation of a deployment that exists is kept until: the policy's
// date, else the deployment's end time. Nil means until the hard end. A deployment that is gone or being
// deleted never gets here: its empty Allocation is released at once (retentionFate).
func keepUntilOf(pol *v1alpha1.RetentionPolicy, endTime *time.Time) *time.Time {
	if pol != nil && pol.Until != nil {
		t := pol.Until.Time
		return &t
	}
	return endTime
}

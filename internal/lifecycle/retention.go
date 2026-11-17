package lifecycle

import (
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
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
		// The mark is cleared by two conditions, evaluated every pass from the cluster's state (markCleared), whatever the order of
		// the writes that led here (HBEP-10, The report, Kept warm).
		if keptWarm(a) && p.markCleared(a) {
			if err := p.clearKeptWarm(a); err != nil {
				return err
			}
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

// clearKeptWarm removes the kept-warm mark, an update with the resourceVersion read.
func (p *pass) clearKeptWarm(a *v1alpha1.Allocation) error {
	next := a.DeepCopy()
	delete(next.Annotations, v1alpha1.AnnotationKeptWarm)
	if err := p.r.Client.Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("remove the kept-warm mark of Allocation %s: %w", a.Name, err)
	}
	*a = *next
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
	// approvedRelease: a committed, unfinished Operation whose spec write has landed has the Allocation as a target its plan releases
	// (ReleaseStarted, or a kept Allocation's release), so the mark does not block that approved release, by whichever path its
	// action uses (its act, the shutdown or retention). The mark is not cleared by it: the Allocation is released with its mark, and if
	// the Operation finishes without releasing it the mark still protects it (HBEP-10, The report, Kept warm, as #707 at 7bb52fc1 says).
	approvedRelease bool
	// markPending: an Operation committed to cancel the Allocation as queued and its mark has not landed yet, so retention treats it
	// as marked: nothing but the profile's maxIdle, its hard end and the failure releases releases a started one.
	markPending bool
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
	v.approvedRelease = p.approvedRelease(a)
	v.markPending = p.markPending(a)
	if p.md != nil && p.md.Spec.EndTime != nil {
		t := p.md.Spec.EndTime.Time
		v.endTime = &t
	}
	if pv := p.pvOf(a.Labels); pv != nil && pv.profile != nil {
		v.maxIdle = pv.settings.maxIdle
	}
	return v
}

// keptWarm reports whether the Allocation carries the kept-warm mark: an Operation committed to cancel it as queued and marked it
// before it acted (markBeforeActing). While its job is pending it follows the usual rules, since its queued cancel was approved;
// once the job has started (keptWarmStarted), no rule but the profile's maxIdle, its hard end, HBEP-7's failure releases, a person's
// direct delete of its deployment while no finalizer holds, and an approved Release releases it (HBEP-10, The report, Kept warm).
func keptWarm(a *v1alpha1.Allocation) bool { return a.Annotations[v1alpha1.AnnotationKeptWarm] != "" }

// keptWarmStarted reports whether the Allocation carries the mark and its job has started: the one state retention's policy rules, the
// shutdown and, while the finalizer holds, the deployment-deletion rule skip.
func keptWarmStarted(a *v1alpha1.Allocation) bool {
	return keptWarm(a) && jobStarted(a)
}

// markCleared reports whether a marked Allocation's mark goes now. Two conditions, each read from the cluster's state, and neither reads
// the Operation that marked it:
//   - an Instance is placed on it while an Operation that targets it is committed and unfinished (a replacement, an added Instance, a
//     repurpose): a live Instance that is not on its way out and that Operation did not commit to;
//   - it hosts a live Instance that the spec keeps and that is not on its way out, with its deployment not in shutdown (HBEP-7,
//     Reconcile loop, step 4) and no committed, unfinished Operation targeting it, which is the serving pair of a Scale above zero once
//     the Operation has finished. Under an End or a Scale to zero the spec ends that Instance, so the mark stays.
//
// An Operation whose approved plan releases a marked Allocation does not clear its mark: the mark does not block that release
// (approvedRelease), and the Allocation goes with it. Once the mark is gone, the Allocation follows its retention policy as any other
// does (HBEP-10, The report, Kept warm).
//
// Two stated limits, which no design here closes: a mark is cleared when an Instance is placed on its Allocation, so a later shutdown
// of that Instance follows retention as for any serving Allocation, and an agent that extends a deployment's end time, which needs no
// approval, can have the controller reuse a kept-warm Allocation and release it at the new end time; and where the profile sets no
// maxIdle a marked, started Allocation idles until its hard end, which the report names.
func (p *pass) markCleared(a *v1alpha1.Allocation) bool {
	live := p.liveOn(a.Name)
	if len(live) == 0 || p.md == nil || p.shutdown {
		return false // nothing hosted, or the deployment is shutting down: the spec keeps nothing
	}
	// The mark is the Allocation's own, so no condition reads the Operation that marked it: removing an Operation, finished or not, by
	// its 7 days or by a person, never removes a mark. Only an Operation that is committed and not finished and targets the Allocation
	// is read, and it is the one in force (HBEP-10, The report, Kept warm).
	targeting := p.unfinishedOperationTargeting(a)
	for _, in := range live {
		if in.Status.Phase == v1alpha1.InstanceDraining || !in.DeletionTimestamp.IsZero() {
			continue // on its way out
		}
		if _, began := drainBegan(in); began {
			continue
		}
		if targeting != nil {
			if !committedInstanceOf(targeting, in) {
				return true // placed while the Operation is unfinished: a replacement, an added Instance, a repurpose
			}
			continue
		}
		if p.keptBySpec(in) {
			return true // it serves on, the spec keeps it, and no Operation ends it
		}
	}
	return false
}

// keptBySpec reports whether the spec keeps the Instance: it is one of the primaries the desired count chooses, or a member or
// replacement of one's hedge. A surplus Instance is one the controller's own scale-down will take.
func (p *pass) keptBySpec(in *v1alpha1.Instance) bool {
	for _, c := range p.primaries() {
		if c.Name == in.Name || hedgeKey(c) == hedgeKey(in) {
			return true
		}
	}
	return false
}

// committedInstanceOf reports whether the Operation committed to the Instance as a target.
func committedInstanceOf(op *v1alpha1.Operation, in *v1alpha1.Instance) bool {
	for _, t := range op.Status.Committed {
		if t.Kind == v1alpha1.TargetKindInstance && t.UID == in.UID {
			return true
		}
	}
	return false
}

// unfinishedOperationTargeting returns the Operation that is committed and not finished and has the Allocation as a committed
// target, or nil.
func (p *pass) unfinishedOperationTargeting(a *v1alpha1.Allocation) *v1alpha1.Operation {
	for _, op := range p.ops {
		if !isExecuting(op) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindAllocation && t.UID == a.UID {
				return op
			}
		}
	}
	return nil
}

// keptWarmSkipsDeletion is the ONE function that says when the deployment-deletion rule skips a marked Allocation: while its
// deployment is being deleted AND holds FinalizerKeptWarm. A delete made while the finalizer holds, a person's included, waits for
// the marked, started Allocations; a deployment that is gone, or is being deleted WITHOUT the finalizer, was deleted when nothing
// held it, and the deletion rule releases its marked Allocations with retention's cause (HBEP-10, The report, Kept warm).
func keptWarmSkipsDeletion(a *v1alpha1.Allocation, v fateView) bool {
	return (keptWarm(a) || v.markPending) && v.heldByKeptWarm && !v.approvedRelease
}

// approvedRelease reports whether a committed, unfinished Operation whose spec write has landed has the Allocation as a target its
// plan releases as started or kept (priced, as ReleaseStarted): the mark does not block that release.
func (p *pass) approvedRelease(a *v1alpha1.Allocation) bool {
	for _, op := range p.ops {
		if !isExecuting(op) || !apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindAllocation && t.UID == a.UID && releasingEffect(t.Effect) && t.Effect != v1alpha1.PlanEffectCancelPending {
				return true
			}
		}
	}
	return false
}

// markPending reports whether an Operation that is committed and not finished committed to cancel the Allocation as queued and its
// mark has not landed: the write failed and is retried each pass, and until it lands retention treats the Allocation as marked.
func (p *pass) markPending(a *v1alpha1.Allocation) bool {
	if keptWarm(a) {
		return false
	}
	for _, op := range p.ops {
		if !isExecuting(op) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindAllocation && t.UID == a.UID && t.Effect == v1alpha1.PlanEffectCancelPending {
				return true
			}
		}
	}
	return false
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
	if !jobStarted(a) {
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
	if (keptWarm(a) || v.markPending) && !v.approvedRelease {
		// Kept warm and started (a marked, pending one was released above, since its queued cancel was approved): the profile's
		// maxIdle, its hard end and the unconditional releases above are all that release it, and every other rule, the idle limit,
		// a keep date and the deployment's end time, skips it (HBEP-10, The report, Kept warm: protect first).
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

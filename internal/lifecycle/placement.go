package lifecycle

import (
	"errors"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The placement order (HBEP-7, Provisioning, Where; #604 PR 3). provisionWith walks the steps placementOrder gives
// it, in order, and the first step that places an Instance ends the walk. A step is a Provider and a kind: a warm step
// places on one of the deployment's warm Allocations there (warmFor), a submit step makes a new Allocation there. A warm
// step that finds an Allocation not registered again after a restart holds the submit steps after it, not the warm
// steps (amendment #719).

type stepKind int

const (
	// stepWarm places the Instance on a warm Allocation of the deployment on the Provider: no slot is reserved and no
	// caps are checked, since the Allocation is already listed and counted.
	stepWarm stepKind = iota
	// stepSubmit reserves a slot on the Provider and makes a new Allocation and its Instance.
	stepSubmit
)

// step is one try of a placement: the Provider and what is tried there.
type step struct {
	pv   *providerView
	kind stepKind
}

// stepResult is what one step did.
type stepResult int

const (
	// stepPassed: nothing was placed here, so the next step is tried.
	stepPassed stepResult = iota
	// stepPlaced: an Instance was created.
	stepPlaced
	// stepStopped: the placement ends with nothing placed: the deployment is gone, or an error came.
	stepStopped
	// stepDeferred: nothing was placed here because a warm Allocation of the deployment on this Provider has not
	// registered again after the controller's restart. The walk goes on to the later warm steps, and holds the
	// submissions after this step until the deferral ends.
	stepDeferred
)

// placeTarget is what a Provider that passed the per-Provider checks of a placement gives its steps: the class's
// current ID the new Instance carries, and the launch's sizing there.
type placeTarget struct {
	cid    *classID
	params v1alpha1.BatchParameters
}

// placementOrder returns the steps of a placement, in order (HBEP-7, Provisioning, Where):
//   - a replacement: its predecessor's Provider, warm then submit; then a warm Allocation of the predecessor's class
//     on each other allowed Provider, in order; then a warm Allocation of another class on each; then a submit step
//     on each, in order;
//   - a missing primary: a warm step on each allowed Provider, in order, of any class; then a submit step on each.
//
// A hedge's member (o.hedgeGroup) takes the rows of its need, and o.skipProviders names the Providers of the hedge's
// live members, which it passes over. o.only is a filter over every step: no step on another Provider, warm or
// submit (the queued no-overlap successor, whose Provider is its predecessor's, and the over-allocation extra,
// which is made beside the Allocation that was just submitted; the plan's ruling of k616, 16 Nov 2026). o.skip
// names the predecessor's Provider when its caps keep the replacement from running beside the predecessor, so the
// rows are the replacement's without its own two steps. The predecessor's Provider has its two steps first and is
// left out of the sweeps, which would only try it again. A predecessor whose Provider is unknown has no own steps and
// no class, so every Provider is of another class.
func (p *pass) placementOrder(pred *v1alpha1.Instance, o placeOpts) []step {
	allowed := func(pv *providerView) bool {
		return pv.name != o.skip && !o.skipProviders[pv.name] && (o.only == "" || pv.name == o.only)
	}
	var own *providerView
	if pred != nil {
		own = p.pvOf(pred.Labels)
	}
	var steps []step
	if own != nil && allowed(own) {
		steps = append(steps, step{own, stepWarm}, step{own, stepSubmit})
	}
	var rest []*providerView
	for _, pv := range p.providers {
		if allowed(pv) && (own == nil || pv.name != own.name) {
			rest = append(rest, pv)
		}
	}
	if pred == nil {
		for _, pv := range rest {
			steps = append(steps, step{pv, stepWarm})
		}
	} else {
		// The predecessor's class first: a warm Allocation of it takes the replacement before one of another class.
		for _, sameClass := range []bool{true, false} {
			for _, pv := range rest {
				if (own != nil && pv.class == own.class) == sameClass {
					steps = append(steps, step{pv, stepWarm})
				}
			}
		}
	}
	for _, pv := range rest {
		steps = append(steps, step{pv, stepSubmit})
	}
	return steps
}

// targetOf runs the per-Provider checks of a placement: the Provider is usable and not too short, its class has a
// current ID that is not blocked, its class is not paused, and it can host the sizing. A Provider that fails is passed
// over; the records of that (a Degraded reason, the sizing refusal) are made only when record is set, which the submit
// step sets, so a Provider is recorded once per placement and a warm placement elsewhere adds none.
func (p *pass) targetOf(pv *providerView, o placeOpts, record bool) *placeTarget {
	if !pv.usable() || pv.tooShort != "" {
		return nil
	}
	cid := p.ids[pv.class]
	if cid == nil || cid.id == "" || cid.blocked != "" {
		return nil
	}
	if paused, until := p.loopPaused(pv.class, pv.settings); paused {
		if record && !o.quiet {
			p.degrade(v1alpha1.ReasonRestartLoop, "provisioning in class %s/%s pauses until %s after repeated failures", pv.class.GPUModel, pv.class.CPUArch, until.UTC().Format(time.RFC3339))
		}
		p.at(until)
		return nil
	}
	params, err := partitionSizingFor(pv, p.plan.gpus, p.plan.workers, p.md.Spec.LeaseLimit)
	if err != nil {
		if o.quiet || !record {
			return nil
		}
		var u *unsupported
		if asUnsupported(err, &u) {
			if p.sizingRefused == nil {
				p.sizingRefused = map[string]string{}
			}
			p.sizingRefused[pv.name] = u.msg
		} else {
			p.degrade(v1alpha1.ReasonProviderUnavailable, "Provider %s: %v", pv.name, err)
		}
		return nil
	}
	return &placeTarget{cid: cid, params: params}
}

// deferralEnd is when a placement that waits for an unregistered warm Allocation next looks again, and false when it
// does not wait. until is the end of agentGrace after the controller's start (warmFor), zero when no warm Allocation
// waits. A replacement never waits past its start deadline, its ReplacementLate deadline: the wait ends there, and
// at that deadline the deferral lapses and the placement goes on to a submission, since the replacement is late and
// its predecessor's drain start is near. The deadline is o.deadline when the caller read one (a hedge member's, whose
// predecessor may be gone), else the predecessor's (replacementDeadline). A missing primary has no start deadline,
// so agentGrace alone bounds it (the science agent's ruling on #604 PR 3's plan, Q3, 16 Nov 2026; HBEP-7 amendment
// #719). decideOverlap asks it too (reusableWarm), so an unregistered spare is room beside only until the deadline.
func (p *pass) deferralEnd(pred *v1alpha1.Instance, o placeOpts, until time.Time) (time.Time, bool) {
	if until.IsZero() {
		return time.Time{}, false
	}
	deadline, ok := o.deadline, !o.deadline.IsZero()
	if !ok && pred != nil {
		deadline, ok = p.replacementDeadline(pred)
	}
	if ok {
		if !p.now.Before(deadline) {
			return time.Time{}, false
		}
		if until.After(deadline) {
			until = deadline
		}
	}
	return until, true
}

// replacementDeadline is the start deadline of pred's replacement: the predecessor's drain start minus its class's
// start-to-ready time (lateAt), the time after which a replacement that has not started is late (HBEP-7, Handover,
// lateness). False when the predecessor's Provider, class or lease is unknown.
func (p *pass) replacementDeadline(pred *v1alpha1.Instance) (time.Time, bool) {
	pv := p.pvOf(pred.Labels)
	if pv == nil || !pv.hasClass {
		return time.Time{}, false
	}
	ds, ok := p.drainStart(pred, pv)
	if !ok {
		return time.Time{}, false
	}
	return lateAt(ds, startToReady(p.md, pv.class)), true
}

// waitsBeforeOwnSubmit answers one question of the placement order: a replacement with a slot on its predecessor's
// Provider, while a warm Allocation on ANOTHER Provider has not registered again after the controller's restart.
// False: it submits on the predecessor's Provider at once. HBEP-7 gives that Provider first ("when it has a slot"),
// and amendment #719 holds only the new submissions that come AFTER the warm Allocation's step in the order, so a
// replacement's own-Provider submission, which comes first, does not wait. True is the literal reading the amendment
// dropped: wait for such an Allocation before any submission. It is kept as a predicate, false (k616, 17 Nov 2026), so
// a different ruling is one edit.
func (p *pass) waitsBeforeOwnSubmit() bool {
	if p.r.waitsBeforeOwnSubmit != nil {
		return p.r.waitsBeforeOwnSubmit()
	}
	return false
}

// waitsForWarmBefore reports whether the placement waits, before the replacement's submit step on its predecessor's
// Provider, for an unregistered warm Allocation on a Provider of the steps that follow (waitsBeforeOwnSubmit), and
// if so until when.
func (p *pass) waitsForWarmBefore(pred *v1alpha1.Instance, st step, later []step, o placeOpts) (time.Time, bool) {
	if pred == nil || st.kind != stepSubmit || !p.waitsBeforeOwnSubmit() {
		return time.Time{}, false
	}
	if own := p.pvOf(pred.Labels); own == nil || own.name != st.pv.name {
		return time.Time{}, false
	}
	for _, l := range later {
		if l.kind != stepWarm {
			continue
		}
		t := p.targetOf(l.pv, o, false)
		if t == nil {
			continue
		}
		if warm, deferUntil := p.warmFor(l.pv, t.params, pred); warm == nil {
			if until, ok := p.deferralEnd(pred, o, deferUntil); ok {
				return until, true
			}
		}
	}
	return time.Time{}, false
}

// warmStep is a warm step: it places the Instance on the Provider's warm Allocation that warmFor picks, through
// placeOnWarm, which takes the Provider's lock. An Allocation whose agent has not registered again yet, during the grace
// after the controller's restart, is waited for (stepDeferred, with the wait's end; deferralEnd) while none is placed here.
func (p *pass) warmStep(pv *providerView, t *placeTarget, pred *v1alpha1.Instance, o placeOpts) (stepResult, time.Time, error) {
	warm, deferUntil := p.warmFor(pv, t.params, pred)
	if warm == nil {
		if until, ok := p.deferralEnd(pred, o, deferUntil); ok {
			return stepDeferred, until, nil
		}
		return stepPassed, time.Time{}, nil
	}
	// An Instance placed on a warm Allocation creates no Allocation, so nothing is marked: the mark
	// is written once, in an Allocation's create request, and an existing one is never marked later
	// (HBEP-7, Handover, No-overlap mode: "fixed at creation"). A reusable warm Allocation also
	// means the primary has room beside it (decideOverlap), so this is not a queued-path successor.
	// placeOnWarm takes the Provider's lock around the create, so a pass that reads the reserved slots and the extra's
	// headroom never sees the Instance half-counted (#708); it also clears the Allocation's idle anchor.
	if err := p.placeOnWarm(pv, t.cid, pred, warm, o); err != nil {
		if errors.Is(err, errDeploymentGone) {
			return stepStopped, time.Time{}, nil
		}
		return stepStopped, time.Time{}, err
	}
	return stepPlaced, time.Time{}, nil
}

// submitOn is a submit step: it reserves a slot on the Provider and makes the Instance and its Allocation. A
// Provider with no slot is passed over.
func (p *pass) submitOn(pv *providerView, t *placeTarget, pred *v1alpha1.Instance, o placeOpts) (stepResult, error) {
	name, err := randomName(pv.name)
	if err != nil {
		return stepStopped, err
	}
	// The slot is reserved before the Instance, the durable intent, so
	// no two workers both pass the count (slots.go).
	//
	// The queued-successor mark is judged from this same read: whether the primary's Allocation
	// still holds its slot is what the reservation's own list says (reserveSlotFor). A replacement
	// takes its predecessor's reserved slot (slots_reserved.go; HBEP-7, Per-user limits).
	var capsOf *v1alpha1.BatchParameters
	if o.caps {
		capsOf = &t.params
	}
	// A missing primary refused on a Provider simply goes on to the next step: nothing is cancelled to make room for it,
	// since an extra is admitted only with headroom for the primaries that need a slot (o.ask.headroom; #708).
	ok, holds, err := p.reserveSlotWith(pv, name, p.primaryAllocKey(o.primaryName(pred)), p.takersOf(pred), capsOf, o.quiet, o.ask)
	if err != nil {
		return stepStopped, err
	}
	if !ok {
		return stepPassed, nil
	}
	if err := p.createInstance(pv, t.cid, pred, name, o); err != nil {
		p.releaseSlot(pv, name)
		if errors.Is(err, errDeploymentGone) {
			return stepStopped, nil
		}
		return stepStopped, err
	}
	if err := p.createAllocation(pv, name, t.params, markValue(holds, o.successorOf)); err != nil {
		if errors.Is(err, errDeploymentGone) {
			return stepStopped, nil // the Instance made just before it is the deleted deployment's to end
		}
		return stepPlaced, err
	}
	// A new Allocation, a submission: capacity() reads where the missing primaries made one (the extra goes
	// to those Providers only; a placement on a warm Allocation is none).
	p.submitted = append(p.submitted, pv.name)
	return stepPlaced, nil
}

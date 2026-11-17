package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The placement order (HBEP-7, Provisioning, Where; #604 PR 3). provisionWith walks the steps placementOrder gives
// it, in order, and the first step that places an Instance, or that defers the placement, ends the walk. A step is a
// Provider and a kind: a warm step places on one of the deployment's warm Allocations there (warmFor), a submit step
// makes a new Allocation there.

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
	// stepStopped: the placement ends with nothing placed: it is deferred, the deployment is gone, or an error came.
	stepStopped
)

// placeTarget is what a Provider that passed the per-Provider checks of a placement gives its steps: the class's
// current ID the new Instance carries, and the launch's sizing there.
type placeTarget struct {
	cid    *classID
	params v1alpha1.BatchParameters
}

// placementOrder returns the steps of a placement, in order.
func (p *pass) placementOrder(pred *v1alpha1.Instance, o placeOpts) []step {
	allowed := func(pv *providerView) bool {
		return pv.name != o.skip && !o.skipProviders[pv.name] && (o.only == "" || pv.name == o.only)
	}
	order := p.providers
	if pred != nil {
		if pv := p.pvOf(pred.Labels); pv != nil {
			order = append([]*providerView{pv}, order...)
		}
	}
	var steps []step
	seen := map[string]bool{}
	for _, pv := range order {
		if seen[pv.name] || !allowed(pv) {
			continue
		}
		seen[pv.name] = true
		steps = append(steps, step{pv, stepWarm}, step{pv, stepSubmit})
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

// deferralEnd is when a placement that waits for an unregistered warm Allocation (until: the end of agentGrace after
// the controller's start) next looks again, and false when it does not wait.
func (p *pass) deferralEnd(pred *v1alpha1.Instance, until time.Time) (time.Time, bool) {
	return until, !until.IsZero()
}

// placeOnWarm is a warm step: it places the Instance on the Provider's warm Allocation that warmFor picks. An
// Allocation whose agent has not registered again yet, during the grace after the controller's restart, defers the
// whole placement (deferralEnd).
func (p *pass) placeOnWarm(pv *providerView, t *placeTarget, pred *v1alpha1.Instance, o placeOpts) (stepResult, error) {
	warm, deferUntil := p.warmFor(pv, t.params, pred)
	if warm == nil {
		if until, ok := p.deferralEnd(pred, deferUntil); ok {
			p.at(until)
			p.deferred = true
			return stepStopped, nil
		}
		return stepPassed, nil
	}
	// An Instance placed on a warm Allocation creates no Allocation, so nothing is marked: the mark
	// is written once, in an Allocation's create request, and an existing one is never marked later
	// (HBEP-7, Handover, No-overlap mode: "fixed at creation"). A reusable warm Allocation also
	// means the primary has room beside it (decideOverlap), so this is not a queued-path successor.
	if err := p.createInstance(pv, t.cid, pred, warm.Name, o); err != nil {
		if errors.Is(err, errDeploymentGone) {
			return stepStopped, nil
		}
		return stepStopped, err
	}
	// Placed: the Allocation is no longer idle (v1alpha1.ClearIdleSince).
	return stepPlaced, p.writeIdleSince(warm, nil)
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
	// A missing primary refused for the submit limit alone makes a pending extra yield its slot (yield.go). The
	// trigger is this step's refusal: a warm step reserves nothing, so it never refuses.
	var onLimit func(context.Context, slotReads) error
	if pred == nil && !o.extra && o.hedgeGroup == "" {
		onLimit = p.yieldOnLimit(pv)
	}
	ok, holds, err := p.reserveSlotWith(pv, name, p.primaryAllocKey(o.primaryName(pred)), p.takersOf(pred), capsOf, o.quiet, o.alsoLeave, onLimit)
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

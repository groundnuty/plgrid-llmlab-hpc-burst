package lifecycle

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// No-overlap mode (HBEP-7, Handover, No-overlap mode; science-agent decision,
// 1 Nov 2026, within the owner's "gap on Helios" of the same day). A
// deployment runs in this mode while no allowed Provider can take a primary's
// replacement beside it: on the primary's own Provider the count leaves no
// free submit slot, or the two Instances would exceed the QOS's per-user caps
// on GPUs, CPUs or memory; and no other allowed Provider has a slot.
// Make-before-break is then impossible, so the drain rule's deadline starts
// the drain: the primary serves until its drain start and drains as any
// Instance does, so no request in flight is cut.
//   - The gap is planned, so ReplacementLate is not set.
//   - Queued path: a submit slot is free and only a per-user cap blocks, as on
//     Helios. The replacement is created at its planned time on the
//     primary's Provider, accepted, and pends until the predecessor's job
//     ends.
//   - One-slot path: no slot is free. The successor is not submitted early;
//     step 7 creates it once the predecessor's job has left the slot (phase
//     Ended, or deletion complete), which the slot reservation decides.
//   - One rule for NoOverlapSlot: it shows from the primary's drain start,
//     in the mode, until a successor is ready, and at no other time. The
//     deployment has Degraded NoOverlapSlot, naming the Provider and the
//     limit, ranking before SubmitLimitReached and CapacityDropped. A
//     primary whose Allocation fails before its drain start has no planned
//     gap: that is a failure (CapacityDropped and its usual reasons), even
//     when a queued successor already exists. A successor that fails before
//     it is ready keeps the gap open, and its retry carries it on.
//   - A queued successor stays on the Provider it was created on. If the
//     mode lifts before the primary's drain start (another Provider gains a
//     slot), the pending successor is not moved: cancelling a queued job to
//     resubmit it elsewhere would lose its place for nothing, so the
//     replacement is late there (ReplacementLate, the ordinary rule) and
//     the primary drains at its drain start with it still pending, which
//     the deployment shows as CapacityDropped, not as a planned gap.
//
// The mode is decided live while the primary's Allocation runs, and recorded
// on the Instances so that it survives a controller restart, the
// predecessor's removal and a successor's failure. All are lifecycle-owned
// annotations, proposed for api/v1alpha1/constants.go, like
// annotationDrainStarted:
//   - annotationNoOverlap on the predecessor, when it drains in the mode;
//   - annotationNoOverlapSuccessor on a successor from its creation, with
//     the Provider and the limit;
//   - annotationNoOverlapGap on a successor once the gap has begun: at the
//     predecessor's drain start for a queued successor that exists by then,
//     at creation for one created after it (the one-slot path, a retry). Its
//     value names the Instance that drained.
//
// A gap is carried by one Instance at a time, the newest of its chain (the
// predecessor, then its successor, then that successor's retry, and so on):
// see gapCarriers.
const (
	// annotationNoOverlap is written on a primary when it drains in the mode
	// with no replacement ready. Its value names the Provider and the limit.
	annotationNoOverlap = v1alpha1.Group + "/no-overlap"
	// annotationNoOverlapSuccessor is written on a no-overlap successor
	// when it is created, with the same value.
	annotationNoOverlapSuccessor = v1alpha1.Group + "/no-overlap-successor"
	// annotationNoOverlapGap is written on a no-overlap successor once the
	// gap has begun, with the name of the primary that drained.
	annotationNoOverlapGap = v1alpha1.Group + "/no-overlap-gap"
)

// overlapCheck is the no-overlap decision for one primary.
type overlapCheck struct {
	// blocked: no allowed Provider can take the primary's replacement
	// beside it, for its per-user limits.
	blocked bool
	// beside: the primary's own Provider can take it beside the primary.
	beside bool
	// queued: blocked, and the primary's Provider has a free submit slot,
	// so only a per-user cap blocks (the queued path).
	queued bool
	// why names the Provider and the limit.
	why string
}

// overlap decides whether the deployment is in no-overlap mode for the primary
// in, whose Allocation runs (HBEP-7, Handover, No-overlap mode). It is
// computed once per pass and Instance. A Provider that cannot take the
// replacement for another reason (not Ready, too short, a size it cannot host)
// is not a limit, so it leaves the normal handover rules in force.
func (p *pass) overlap(in *v1alpha1.Instance) (overlapCheck, error) {
	if c, ok := p.overlaps[in.UID]; ok {
		return c, nil
	}
	c, err := p.decideOverlap(in)
	if err != nil {
		return overlapCheck{}, err
	}
	if p.overlaps == nil {
		p.overlaps = map[types.UID]overlapCheck{}
	}
	p.overlaps[in.UID] = c
	return c, nil
}

func (p *pass) decideOverlap(in *v1alpha1.Instance) (overlapCheck, error) {
	pv := p.pvOf(in.Labels)
	a := p.allocs[in.Spec.AllocationRef.Name]
	if pv == nil || !pv.usable() || pv.tooShort != "" || a == nil || a.Spec.ForProvider.Batch == nil || p.specErr != "" || p.md == nil {
		return overlapCheck{beside: true}, nil
	}
	params, err := partitionSizing(pv, p.plan.gpus, p.md.Spec.LeaseLimit)
	if err != nil {
		return overlapCheck{beside: true}, nil
	}
	// The existing replacement, if any, already holds its own slot.
	var skip []string
	if repl := p.replacementOf(in); repl != nil {
		skip = append(skip, in.Namespace+"/"+repl.Spec.AllocationRef.Name)
	}
	slotFree, slotWhy, err := p.slotFor(pv, skip)
	if err != nil {
		return overlapCheck{}, err
	}
	capWhy := capsExceeded(pv, in.Name, *a.Spec.ForProvider.Batch, params)
	if slotFree && capWhy == "" {
		return overlapCheck{beside: true}, nil
	}
	for _, q := range p.providers {
		if q == pv || !q.usable() || q.tooShort != "" {
			continue
		}
		if cid := p.ids[q.class]; cid == nil || cid.id == "" || cid.blocked != "" {
			continue
		}
		if _, err := partitionSizing(q, p.plan.gpus, p.md.Spec.LeaseLimit); err != nil {
			continue
		}
		free, _, err := p.slotFor(q, skip)
		if err != nil {
			return overlapCheck{}, err
		}
		if free {
			return overlapCheck{}, nil // another Provider has room: not no-overlap mode
		}
	}
	why := slotWhy
	if slotFree {
		why = capWhy
	}
	return overlapCheck{blocked: true, queued: slotFree, why: why}, nil
}

// slotFor reports whether the Provider's count leaves a free submit slot,
// not counting the Allocations in skip, and if not, why, naming the Provider
// and its limit.
func (p *pass) slotFor(pv *providerView, skip []string) (bool, string, error) {
	lim := pv.limits.MaxSubmitPerUser
	if lim == nil {
		return true, "", nil
	}
	n, err := p.r.slots().count(p.ctx, p.r.Reader, pv.name, p.now, skip...)
	if err != nil {
		return false, "", err
	}
	if n+1 <= int(*lim) {
		return true, "", nil
	}
	return false, fmt.Sprintf("Provider %s has no free submit slot for a replacement: %d Allocations not ended, at its %s", pv.name, n, submitLimitText(pv)), nil
}

// capsExceeded returns why the predecessor's Allocation (pred) and its
// replacement (repl) together exceed the Provider's per-user caps on GPUs,
// CPUs or memory, or "" when they fit (HBEP-7, Provisioning, Per-user limits).
func capsExceeded(pv *providerView, inst string, pred, repl v1alpha1.BatchParameters) string {
	l := pv.limits
	var over []string
	if q := l.MaxGPUsPerUser; q != nil && pred.GPUs+repl.GPUs > *q {
		over = append(over, fmt.Sprintf("%d GPUs, above the %s %d of QOS %s", pred.GPUs+repl.GPUs, v1alpha1.LimitMaxGPUsPerUser, *q, l.From[v1alpha1.LimitMaxGPUsPerUser]))
	}
	if q := l.MaxCPUsPerUser; q != nil && pred.CPUsPerTask+repl.CPUsPerTask > *q {
		over = append(over, fmt.Sprintf("%d CPUs, above the %s %d of QOS %s", pred.CPUsPerTask+repl.CPUsPerTask, v1alpha1.LimitMaxCPUsPerUser, *q, l.From[v1alpha1.LimitMaxCPUsPerUser]))
	}
	if q := l.MaxMemPerUser; q != nil {
		sum := pred.Memory.DeepCopy()
		sum.Add(repl.Memory)
		if sum.Cmp(*q) > 0 {
			over = append(over, fmt.Sprintf("%s of memory, above the %s %s of QOS %s", sum.String(), v1alpha1.LimitMaxMemPerUser, q.String(), l.From[v1alpha1.LimitMaxMemPerUser]))
		}
	}
	if len(over) == 0 {
		return ""
	}
	return fmt.Sprintf("Instance %s and its replacement together need %s on Provider %s", inst, strings.Join(over, " and "), pv.name)
}

// placeReplacement creates the primary's replacement at its planned time
// (HBEP-7, Handover). With room beside the primary on its own Provider, it
// goes there first, as ever. When only another Provider has room, it goes
// there. In no-overlap mode it goes to the primary's Provider on the queued
// path, marked as a no-overlap successor, and is not created on the one-slot
// path, where step 7 creates it once the slot is free.
func (p *pass) placeReplacement(in *v1alpha1.Instance, ov overlapCheck) (bool, error) {
	switch {
	case !ov.blocked && ov.beside:
		return p.provision(in)
	case !ov.blocked:
		return p.provisionWith(in, placeOpts{skip: in.Labels[v1alpha1.LabelProvider]})
	case ov.queued:
		return p.provisionWith(in, placeOpts{only: in.Labels[v1alpha1.LabelProvider], successorOf: ov.why})
	}
	return false, nil
}

// gapCarrier is the Instance that holds a gap open.
type gapCarrier struct {
	in *v1alpha1.Instance
	// drained names the primary that drained in the mode; why names the
	// Provider and the limit.
	drained, why string
}

// gapCarriers returns, for each gap that has begun and has not ended, the
// Instance that carries it: the newest of the chain of a primary that drained
// in the mode and the successors made for it.
//   - A primary carries it when it drained in the mode (annotationNoOverlap,
//     and its drain has begun) and is out of service.
//   - A successor carries it when the gap had begun by its creation or at
//     its predecessor's drain start (annotationNoOverlapGap), and it has not
//     been ready. One that never became ready (it failed) still carries it
//     until its retry exists, and the retry takes it on.
//
// A successor that has been ready ends the gap. An Instance that a stamped
// successor names as its predecessor no longer carries it; the successor does.
// A queued successor not yet stamped (its predecessor has not drained)
// carries nothing: a predecessor that fails first is a failure, not a gap.
func (p *pass) gapCarriers() []gapCarrier {
	passed := map[string]bool{}
	for _, in := range p.insts {
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && !p.stale[in.UID] && in.Annotations[annotationNoOverlapGap] != "" {
			passed[pred] = true
		}
	}
	var out []gapCarrier
	for _, in := range p.insts {
		if p.stale[in.UID] || passed[in.Name] {
			continue
		}
		if why := in.Annotations[annotationNoOverlap]; why != "" {
			if _, began := drainBegan(in); began && !countable(in) {
				out = append(out, gapCarrier{in: in, drained: in.Name, why: why})
			}
			continue
		}
		why, drained := in.Annotations[annotationNoOverlapSuccessor], in.Annotations[annotationNoOverlapGap]
		if why != "" && drained != "" && in.Status.ReadyTime == nil {
			out = append(out, gapCarrier{in: in, drained: drained, why: why})
		}
	}
	return out
}

// noOverlapCarriers returns the gap carriers that are out of service: each is
// the predecessor of a missing primary, which step 7 creates as its successor,
// marked, on its Provider first (HBEP-7, Handover, No-overlap mode, the
// one-slot path), whether the carrier drained or is a successor that failed.
func (p *pass) noOverlapCarriers() []gapCarrier {
	var out []gapCarrier
	for _, g := range p.gapCarriers() {
		if !countable(g.in) {
			out = append(out, g)
		}
	}
	return out
}

// holdsGap reports whether the ended Instance in is kept until a successor
// exists: it carries a gap that no successor names yet, and the deployment is
// short of primaries, so step 7 will create one. Were it deleted first, a
// controller restarting between the deletion and the successor's creation
// would find no record of the gap.
func (p *pass) holdsGap(in *v1alpha1.Instance) bool {
	if p.shutdown || p.md == nil || p.stale[in.UID] || len(p.primaries()) >= int(p.md.Spec.Instances) {
		return false
	}
	for _, g := range p.gapCarriers() {
		if g.in == in {
			return true
		}
	}
	return false
}

// noOverlapGap sets Degraded NoOverlapSlot while a no-overlap gap lasts: from
// a primary's drain start, when it drained in the mode, until a successor is
// ready, and only while fewer Instances are ready than desired. It reads the
// recorded annotations, so it holds across a controller restart, the
// predecessor's removal and a successor's failure (gapCarriers).
func (p *pass) noOverlapGap(ready, desired int32) {
	if p.shutdown || ready >= desired {
		return
	}
	for _, g := range p.gapCarriers() {
		p.degrade(v1alpha1.ReasonNoOverlapSlot, "Instance %s drained with no replacement beside it: %s", g.drained, g.why)
	}
}

// drainStartPassed reports whether the primary's drain start has come, as the
// drain rule dates it (not an earlier drain, such as a scale-down's).
func (p *pass) drainStartPassed(in *v1alpha1.Instance) bool {
	ds, ok := p.drainStart(in, p.pvOf(in.Labels))
	return ok && !p.now.Before(ds)
}

// markNoOverlap records, once a primary has drained at its drain start with no
// replacement ready, that the deployment is in no-overlap mode for it
// (annotationNoOverlap on the primary, and annotationNoOverlapSuccessor and
// annotationNoOverlapGap on its replacement if it has one), so NoOverlapSlot
// holds from now until a successor is ready, across a controller restart. It
// is called while the primary drains, after the drain began: a failed check
// or write never holds the drain back, and the next pass tries again until the
// primary ends. Outside the mode, or for a stale deployment's Instance or a
// drain that is not the drain rule's (a scale-down's), it records nothing.
func (p *pass) markNoOverlap(in *v1alpha1.Instance) error {
	if p.stale[in.UID] || p.md == nil || p.shutdown || !p.drainStartPassed(in) {
		return nil
	}
	repl := p.replacementOf(in)
	if repl != nil && repl.Status.Phase == v1alpha1.InstanceReady {
		return nil // handed over
	}
	why := in.Annotations[annotationNoOverlap]
	if why == "" {
		ov, err := p.overlap(in)
		if err != nil || !ov.blocked {
			return err
		}
		why = ov.why
		if err := p.setMeta(in, func(n *v1alpha1.Instance) {
			if n.Annotations == nil {
				n.Annotations = map[string]string{}
			}
			n.Annotations[annotationNoOverlap] = why
		}); err != nil {
			return err
		}
	}
	// The replacement that is not ready now is the primary's successor; the
	// stamp is what keeps the gap open once the primary is gone.
	if repl == nil || repl.Annotations[annotationNoOverlapGap] != "" {
		return nil
	}
	return p.setMeta(repl, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		if n.Annotations[annotationNoOverlapSuccessor] == "" {
			n.Annotations[annotationNoOverlapSuccessor] = why
		}
		n.Annotations[annotationNoOverlapGap] = in.Name
	})
}

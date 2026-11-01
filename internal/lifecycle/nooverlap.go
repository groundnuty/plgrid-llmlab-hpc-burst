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
//   - From the primary's drain start until its successor is ready, the
//     deployment has Degraded NoOverlapSlot, naming the Provider and the
//     limit. It ranks before SubmitLimitReached and CapacityDropped.
//
// The mode is decided live while the primary's Allocation runs, and recorded
// on the Instances so that it survives a controller restart and the
// predecessor's removal: annotationNoOverlap on the predecessor at its drain
// start, annotationNoOverlapSuccessor on its successor from its creation.
// Both are lifecycle-owned annotations, proposed for
// api/v1alpha1/constants.go, like annotationDrainStarted.
const (
	// annotationNoOverlap is written on a primary at its drain start when
	// the deployment is in no-overlap mode for it and no replacement is
	// ready. Its value names the Provider and the limit.
	annotationNoOverlap = v1alpha1.Group + "/no-overlap"
	// annotationNoOverlapSuccessor is written on a no-overlap successor
	// when it is created, with the same value.
	annotationNoOverlapSuccessor = v1alpha1.Group + "/no-overlap-successor"
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

// noOverlapPredecessors returns the primaries that drained in no-overlap mode
// and have no live successor yet: step 7 creates a missing primary as the
// successor of one of them, marked, on its Provider first (HBEP-7, Handover,
// No-overlap mode, the one-slot path).
func (p *pass) noOverlapPredecessors() []*v1alpha1.Instance {
	var out []*v1alpha1.Instance
	for _, in := range p.insts {
		if p.stale[in.UID] || in.Annotations[annotationNoOverlap] == "" || countable(in) {
			continue
		}
		if p.replacementOf(in) == nil && !p.successorReady(in) {
			out = append(out, in)
		}
	}
	return out
}

// successorReady reports whether an Instance naming in as its predecessor has
// been ready.
func (p *pass) successorReady(in *v1alpha1.Instance) bool {
	for _, o := range p.insts {
		if o.Annotations[v1alpha1.AnnotationPredecessor] == in.Name && o.Status.ReadyTime != nil {
			return true
		}
	}
	return false
}

// noOverlapGap sets Degraded NoOverlapSlot while a no-overlap gap lasts: from
// a primary's drain start, when it drained in no-overlap mode, until its
// successor is ready, and only while fewer Instances are ready than desired.
// It reads the recorded annotations, so it holds across a controller restart,
// and on the successor's own mark once the predecessor is gone.
func (p *pass) noOverlapGap(ready, desired int32) {
	if p.shutdown || ready >= desired {
		return
	}
	for _, in := range p.insts {
		if p.stale[in.UID] {
			continue
		}
		if why := in.Annotations[annotationNoOverlap]; why != "" && !countable(in) && !p.successorReady(in) {
			if _, began := drainBegan(in); began {
				p.degrade(v1alpha1.ReasonNoOverlapSlot, "Instance %s drained with no replacement beside it: %s", in.Name, why)
			}
			continue
		}
		why := in.Annotations[annotationNoOverlapSuccessor]
		if why == "" || ended(in) || in.Status.ReadyTime != nil {
			continue
		}
		// Once the predecessor is gone, or when it was not marked (it was
		// gone before its drain start was recorded), the successor's mark
		// carries the gap; a marked predecessor carries it itself.
		pred := in.Annotations[v1alpha1.AnnotationPredecessor]
		if pi := p.instanceNamed(pred); pi == nil || (!countable(pi) && pi.Annotations[annotationNoOverlap] == "") {
			p.degrade(v1alpha1.ReasonNoOverlapSlot, "Instance %s drained with no replacement beside it: %s", pred, why)
		}
	}
}

// instanceNamed returns the deployment's Instance of that name, or nil.
func (p *pass) instanceNamed(name string) *v1alpha1.Instance {
	for _, in := range p.insts {
		if in.Name == name {
			return in
		}
	}
	return nil
}

// markNoOverlap records, at a primary's drain start with no replacement ready,
// that the deployment is in no-overlap mode for it (annotationNoOverlap), so
// NoOverlapSlot holds from now until its successor is ready, across a
// controller restart. Outside the mode, or for a stale deployment's Instance,
// it records nothing.
func (p *pass) markNoOverlap(in *v1alpha1.Instance) error {
	if p.stale[in.UID] || p.md == nil || in.Annotations[annotationNoOverlap] != "" {
		return nil
	}
	ov, err := p.overlap(in)
	if err != nil || !ov.blocked {
		return err
	}
	return p.setMeta(in, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		n.Annotations[annotationNoOverlap] = ov.why
	})
}

package lifecycle

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func asUnsupported(err error, u **unsupported) bool { return errors.As(err, u) }

// primaries returns the Instances that count towards capacity: not draining,
// ended or deleting, and not the replacement of a predecessor that still
// counts (HBEP-7, Reconcile loop, step 7).
func (p *pass) primaries() []*v1alpha1.Instance {
	counted := map[string]bool{}
	for _, in := range p.insts {
		if countable(in) && !p.stale[in.UID] {
			counted[in.Name] = true
		}
	}
	var out []*v1alpha1.Instance
	for _, in := range p.insts {
		if !countable(in) || p.stale[in.UID] {
			continue
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && counted[pred] {
			continue
		}
		out = append(out, in)
	}
	return out
}

func countable(in *v1alpha1.Instance) bool {
	return in.DeletionTimestamp.IsZero() && in.Status.Phase != v1alpha1.InstanceDraining && in.Status.Phase != v1alpha1.InstanceEnded
}

// capacity creates one Allocation and its Instance per missing primary, and
// scales a surplus down (HBEP-7, Reconcile loop, step 7).
func (p *pass) capacity() error {
	if err := p.ensureCredentials(); err != nil {
		return err
	}
	prim := p.primaries()
	desired := int(p.md.Spec.Instances)
	for i := len(prim); i < desired; i++ {
		ok, err := p.provision(nil)
		if err != nil {
			return err
		}
		if !ok {
			if !p.deferred {
				p.noProvider()
			}
			break
		}
	}
	if len(prim) > desired {
		return p.scaleDown(prim, len(prim)-desired)
	}
	return nil
}

// noProvider explains why an Instance that is needed could not be placed.
// When every usable Provider that is not too short refused the spec's size,
// the spec is unsupported (HBEP-7, Provisioning). When a Provider would have
// taken it but had no free submit slot, the deployment has SubmitLimitReached,
// naming each such Provider and its limit (Per-user limits). A deployment whose
// every usable Provider is too short has PartitionTooShort alone.
func (p *pass) noProvider() {
	if !p.canProvision() {
		return
	}
	for _, c := range p.ids {
		if c.transient {
			return // a transient Hub failure sets no reason: it is retried (HBEP-3)
		}
	}
	short := 0
	for _, pv := range p.providers {
		if pv.usable() && pv.tooShort != "" {
			short++
		}
	}
	if len(p.sizingRefused) > 0 && len(p.sizingRefused) == countUsable(p.providers)-short {
		msg := p.firstByProvider(p.sizingRefused)
		p.specErr = msg
		p.degrade(v1alpha1.ReasonSpecUnsupported, "%s", msg)
		return
	}
	if len(p.noSlot) > 0 {
		p.degrade(v1alpha1.ReasonSubmitLimitReached, "no allowed Provider has a free submit slot for a missing primary: %s", p.allByProvider(p.noSlot))
		return
	}
	for _, d := range p.degraded {
		switch d.reason {
		case v1alpha1.ReasonProviderUnavailable, v1alpha1.ReasonVerificationFailed, v1alpha1.ReasonRestartLoop, v1alpha1.ReasonIdentityUnresolved,
			v1alpha1.ReasonPartitionTooShort, v1alpha1.ReasonSubmitLimitReached:
			return // already explained
		}
	}
	p.degrade(v1alpha1.ReasonProviderUnavailable, "no allowed batch Provider can take a new Allocation")
}

// firstByProvider returns the entry of the first Provider, in the allowed
// order, that has one.
func (p *pass) firstByProvider(m map[string]string) string {
	for _, pv := range p.providers {
		if msg, ok := m[pv.name]; ok {
			return msg
		}
	}
	return ""
}

// allByProvider joins the entries in the allowed Providers' order.
func (p *pass) allByProvider(m map[string]string) string {
	var parts []string
	for _, pv := range p.providers {
		if msg, ok := m[pv.name]; ok {
			parts = append(parts, msg)
		}
	}
	return strings.Join(parts, "; ")
}

func countUsable(pvs []*providerView) int {
	n := 0
	for _, pv := range pvs {
		if pv.usable() {
			n++
		}
	}
	return n
}

// scaleDown removes surplus primaries: pending Allocations first, newest first;
// then Instances still starting, newest first; then ready primaries with the
// nearest hard end, which drain. A surplus primary's replacement goes with it,
// never promoted (HBEP-7, Reconcile loop, step 7; Release: scale-down).
func (p *pass) scaleDown(prim []*v1alpha1.Instance, surplus int) error {
	rank := func(in *v1alpha1.Instance) int {
		a := p.allocs[in.Spec.AllocationRef.Name]
		switch {
		case a == nil || a.Status.AtProvider.StartTime == nil:
			return 0
		case in.Status.Phase != v1alpha1.InstanceReady:
			return 1
		}
		return 2
	}
	victims := append([]*v1alpha1.Instance(nil), prim...)
	sort.SliceStable(victims, func(i, j int) bool {
		a, b := victims[i], victims[j]
		if ra, rb := rank(a), rank(b); ra != rb {
			return ra < rb
		}
		if rank(a) == 2 {
			return leaseBefore(a, b)
		}
		return b.CreationTimestamp.Before(&a.CreationTimestamp) // newest first
	})
	for _, in := range victims[:surplus] {
		// A surplus primary's replacement goes with it, never promoted
		// (step 7); a serving one drains like any surplus Instance (Release:
		// scale-down).
		if repl := p.replacementOf(in); repl != nil {
			if err := p.removeSurplus(repl, "its predecessor was scaled down"); err != nil {
				return err
			}
		}
		if err := p.removeSurplus(in, "scaled down"); err != nil {
			return err
		}
	}
	return nil
}

// removeSurplus takes one Instance out at scale-down (HBEP-7, Release): a
// ready one drains and ends, and its Allocation then follows its retention
// policy; one not yet serving ends at once, and its Allocation is cancelled
// only when its job has not started.
func (p *pass) removeSurplus(in *v1alpha1.Instance, why string) error {
	if in.Status.Phase == v1alpha1.InstanceReady {
		return p.startDrain(in)
	}
	a := p.allocs[in.Spec.AllocationRef.Name]
	notStarted := a == nil || a.Status.AtProvider.StartTime == nil
	return p.end(in, a, v1alpha1.ReasonDrained, why, notStarted)
}

func leaseBefore(a, b *v1alpha1.Instance) bool {
	switch {
	case a.Status.LeaseEnd == nil:
		return false
	case b.Status.LeaseEnd == nil:
		return true
	}
	return a.Status.LeaseEnd.Before(b.Status.LeaseEnd)
}

// replacements gives every primary whose Allocation runs, that has no live
// replacement, and whose replacement's planned time has come, one replacement;
// it sets ReplacementLate when a replacement has not started by its
// predecessor's drain start minus the class's start-to-ready time (HBEP-7,
// Handover).
func (p *pass) replacements() error {
	for _, in := range p.primaries() {
		a := p.allocs[in.Spec.AllocationRef.Name]
		pv := p.pvOf(in.Labels)
		if a == nil || a.Status.AtProvider.StartTime == nil || pv == nil || pv.profile == nil || !pv.hasClass {
			continue
		}
		ds, ok := p.drainStart(in, pv)
		if !ok {
			continue
		}
		if p.md.Spec.EndTime != nil && !p.md.Spec.EndTime.After(ds) {
			continue // the deployment ends before this lease drains: no replacement
		}
		s2r := startToReady(p.md, pv.class)
		repl := p.replacementOf(in)
		if repl == nil {
			var waits []v1alpha1.QueueWait
			if pv.prov != nil {
				waits = pv.prov.Status.QueueWaits
			}
			t := replacementTime(ds, a.Status.AtProvider.StartTime.Time, waits, s2r, pv.settings.handoverMargin)
			if p.now.Before(t) {
				p.at(t)
			} else {
				placed, err := p.provision(in)
				if err != nil {
					return err
				}
				if placed {
					repl = p.replacementOf(in)
				}
			}
		}
		late := lateAt(ds, s2r)
		if p.now.Before(late) {
			p.at(late)
			continue
		}
		started := false
		if repl != nil {
			if ra := p.allocs[repl.Spec.AllocationRef.Name]; ra != nil && ra.Status.AtProvider.StartTime != nil {
				started = true
			}
		}
		if started {
			continue
		}
		p.degrade(v1alpha1.ReasonReplacementLate, "the replacement of Instance %s has not started, and its drain start is at %s", in.Name, ds.UTC().Format(time.RFC3339))
		if p.mem.once("late/" + string(in.UID)) {
			p.r.Recorder.Event(p.md, nil, eventDegraded, v1alpha1.ReasonReplacementLate,
				fmt.Sprintf("the replacement of Instance %s has not started by %s, its drain start minus the class's start-to-ready time", in.Name, late.UTC().Format(time.RFC3339)))
		}
	}
	return nil
}

package lifecycle

import (
	"errors"
	"fmt"
	"sort"
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
		if countable(in) {
			counted[in.Name] = true
		}
	}
	var out []*v1alpha1.Instance
	for _, in := range p.insts {
		if !countable(in) {
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
			p.noProvider()
			break
		}
	}
	if len(prim) > desired {
		return p.scaleDown(prim, len(prim)-desired)
	}
	return nil
}

// noProvider explains why an Instance that is needed could not be placed.
func (p *pass) noProvider() {
	if !p.canProvision() {
		return
	}
	if len(p.sizingRefused) > 0 && len(p.sizingRefused) == countUsable(p.providers) {
		p.specErr = p.sizingRefused[0]
		p.degrade(v1alpha1.ReasonSpecUnsupported, "%s", p.sizingRefused[0])
		return
	}
	for _, d := range p.degraded {
		switch d.reason {
		case v1alpha1.ReasonProviderUnavailable, v1alpha1.ReasonVerificationFailed, v1alpha1.ReasonRestartLoop, reasonIdentityUnresolved:
			return // already explained
		}
	}
	p.degrade(v1alpha1.ReasonProviderUnavailable, "no allowed batch Provider can take a new Allocation")
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
		if repl := p.replacementOf(in); repl != nil {
			if err := p.end(repl, p.allocs[repl.Spec.AllocationRef.Name], endScaledDown, "its predecessor was scaled down", true); err != nil {
				return err
			}
		}
		a := p.allocs[in.Spec.AllocationRef.Name]
		if in.Status.Phase == v1alpha1.InstanceReady {
			if err := p.startDrain(in, "scale-down"); err != nil {
				return err
			}
			continue
		}
		pending := a == nil || a.Status.AtProvider.StartTime == nil
		if err := p.end(in, a, endScaledDown, "scaled down", pending); err != nil {
			return err
		}
	}
	return nil
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
			p.r.Recorder.Event(p.md, true, eventDegraded, v1alpha1.ReasonReplacementLate,
				fmt.Sprintf("the replacement of Instance %s has not started by %s, its drain start minus the class's start-to-ready time", in.Name, late.UTC().Format(time.RFC3339)))
		}
	}
	return nil
}

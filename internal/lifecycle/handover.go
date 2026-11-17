package lifecycle

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func asUnsupported(err error, u **unsupported) bool { return errors.As(err, u) }

// primaries returns the deployment's chosen primaries, one per hedge: the spec.instances most advanced of the
// Instances that count, in HBEP-7's order (selectPrimaries; Reconcile loop, step 7; Over-allocation and
// qualification (beta), How it counts). An Instance counts when it is not draining, ended or deleting and is not the
// replacement of a predecessor that still counts. The members of one hedge count once, through its representative
// (hedge.go), so a two-member hedge for one missing primary is one primary, and step 7 cancels no member of it. The
// rest are the deployment's surplus (scaleDownTargets): they are no primary, hold no reserved slot and get no
// replacement. With no surplus these are the Instances that count, as in alpha.
func (p *pass) primaries() []*v1alpha1.Instance {
	chosen, _ := selectPrimaries(p.selection(nil), p.desiredCount())
	return chosen
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
	// While a Scale Operation is Executing, the deployment keeps at most the Operation's count, whatever the spec says
	// before its write has landed or after a person raised it: the committed targets are the Operation's to take, and
	// step 7 takes nothing of its own (below; HBEP-10, Approval and execution).
	scaling := p.executingScale()
	if scaling != nil {
		desired = min(desired, int(scaling.Spec.Scale.Instances))
	}
	// A missing primary whose predecessor drained in no-overlap mode, or whose
	// predecessor was such a primary's successor that failed, is that
	// predecessor's successor, marked, on its Provider first (HBEP-7,
	// Handover, No-overlap mode, the one-slot path and the retries).
	carriers := p.noOverlapCarriers()
	var placed []string // the Providers where the loop made a new Allocation, in order
	for i := len(prim); i < desired; i++ {
		var ok bool
		var err error
		before := len(p.submitted)
		if len(carriers) > 0 {
			g := carriers[0]
			carriers = carriers[1:]
			ok, err = p.provisionWith(g.in, placeOpts{successorOf: g.why, gapOf: g.drained})
		} else {
			ok, err = p.provision(nil)
		}
		// Each Provider where this loop made a new Allocation (a submission, never a placement on a warm one) gets
		// its extra below, a no-overlap successor's included: HBEP-7 refuses the extra in no-overlap mode by the
		// slot and caps checks failing on the same counts, not by a skip.
		for _, name := range p.submitted[before:] {
			placed = appendOnce(placed, name)
		}
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
	if p.holdsSelection() {
		// An Executing Operation's committed targets are acted on by the Operations step, before this one. The controller's
		// own selection is never called here, beside any Operation: it could pick another victim than the one that was
		// approved, or one the Operation did not commit to (HBEP-7, step 7).
		// No extra is made beside an Executing Operation either: the Operation holds the deployment's count, and an
		// extra would add an Allocation the plan did not count (k616's merge of #664 into #708).
		return nil
	}
	// Each Provider where the loop made a new Allocation gets its extra, after every missing primary it could place
	// (extra.go). An extra no check admits is silent, so it never reaches noProvider() above.
	if err := p.createExtras(placed); err != nil {
		return err
	}
	// The chosen primaries are read again: the loop above made Instances, which the selection counts. The surplus,
	// ready or not, is taken every pass, not only when the count is lowered (scaleDownTargets).
	return p.scaleDown(p.primaries(), 0)
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

// scaleDown takes what step 7 takes: the surplus the selection leaves over, ready Instances draining, the nearest
// hard end first, and non-ready ones beyond k per Provider, pending ones first, newest first, then started ones. A
// surplus primary's replacement goes with it, never promoted, and a hedge goes whole, each member by its own state
// (HBEP-7, Reconcile loop, step 7; Release: scale-down; scaleDownTargets).
func (p *pass) scaleDown(prim []*v1alpha1.Instance, surplus int) error {
	for _, t := range p.scaleDownTargets(prim, surplus) {
		if err := p.applyScaleTarget(t); err != nil {
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

// overlapOrDefer is p.overlap for a primary's replacement rules. When the check
// fails (it lists Allocations across namespaces), the rules for that primary
// wait for the next pass, which the returned error brings about with backoff:
// the error is kept in p.overlapErrs and returned at the end of the pass, so
// it neither skips retention and the status write nor, by guessing, sets or
// withholds ReplacementLate, an event that is recorded once.
func (p *pass) overlapOrDefer(in *v1alpha1.Instance) (overlapCheck, bool) {
	ov, err := p.overlap(in)
	if err != nil {
		p.overlapErrs = append(p.overlapErrs, fmt.Errorf("no-overlap check of Instance %s: %w", in.Name, err))
		return overlapCheck{}, false
	}
	return ov, true
}

// servesPast reports whether the deployment still serves after t: it has no end time, or its end time is after
// t. replacements() replaces a primary only when its deployment serves past the primary's drain start, and the
// reserved replacement slot is held for exactly such a primary (slots_reserved.go, reservedOn), through this one
// test, so the two never disagree (the science agent's ruling on #598's plan, comment 6897017776: "a primary
// holds a slot whenever the controller would create its replacement").
func servesPast(md *v1alpha1.ModelDeployment, t time.Time) bool {
	return md.Spec.EndTime == nil || md.Spec.EndTime.After(t)
}

// replacements gives every primary whose Allocation runs, that has no live
// replacement, and whose replacement's planned time has come, one replacement;
// it sets ReplacementLate when a replacement has not started by its
// predecessor's drain start minus the class's start-to-ready time (HBEP-7,
// Handover).
func (p *pass) replacements() error {
	hedges := map[string][]*v1alpha1.Instance{}
	for _, g := range p.primaryHedges() {
		hedges[g.key] = g.members
	}
	for _, in := range p.primaries() {
		if p.committedInstance(in) {
			continue // an Executing Operation takes it: a drain that failed to start is retried, not replaced
		}
		// A hedge of more than one live member with no winner yet is one need still being met: its replacement
		// waits for the winner, so none is planned for a member that then loses (#611's review).
		if m := hedges[hedgeKey(in)]; len(m) > 1 && hedgeWinner(m) == nil {
			continue
		}
		a := p.allocs[in.Spec.AllocationRef.Name]
		pv := p.pvOf(in.Labels)
		if a == nil || a.Status.AtProvider.StartTime == nil || pv == nil || pv.profile == nil || !pv.hasClass {
			continue
		}
		ds, ok := p.drainStart(in, pv)
		if !ok {
			continue
		}
		if !servesPast(p.md, ds) {
			continue // the deployment ends before this lease drains: no replacement (the reserved slot's test too)
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
				ov, ok := p.overlapOrDefer(in)
				if !ok {
					continue
				}
				placed, err := p.placeReplacement(in, ov)
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
		ov, ok := p.overlapOrDefer(in)
		if !ok {
			continue
		}
		if ov.blocked {
			continue // no-overlap mode: the gap is planned, so no ReplacementLate
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

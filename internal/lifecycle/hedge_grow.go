package lifecycle

import (
	"errors"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// When a hedge grows (beta; HBEP-7, Hedging (beta), When a hedge grows; #604 PR 2).
//
// A hedge with no winner gets its next member once every member has reached its hedge time: its expected start
// (its job's submission time, or now while it has none, plus its Provider's planning wait) when that is no later
// than the hedge's start deadline, or now when it is later (the observed waits exceed the time left). The start
// deadline of a replacement hedge is its predecessor's ReplacementLate deadline; of a missing primary's, its first
// member's creation plus handoverMargin. With no wait observed on a member's Provider, its expected start is the
// deadline minus handoverMargin for a replacement and the deadline for a missing primary, so a missing primary
// with no wait observed never hedges in its first pass. No member is added while any member's job has started,
// beyond hedgeWidth live members, or while an executing Operation holds the hedge (hedgeFrozen). At most one
// member is added per hedge per pass, so a pass reserves on one Provider at a time; the next pass adds the next.
// Every input is in status, so a restarted controller adds the same members at the same times, and each pending
// member's hedge time is a requeue deadline (step 11).

// hedgeNeed is what a hedge is for: its first member, and the predecessor it replaces (nil for a missing
// primary). predGone is set for a replacement hedge whose predecessor is gone: its deadline has passed.
type hedgeNeed struct {
	first    *v1alpha1.Instance
	pred     *v1alpha1.Instance
	predGone bool
}

// needOf reads a hedge's need from its members: the member named by its key (the first member), else the
// oldest, and the Instance that member names as its predecessor.
func (p *pass) needOf(g hedgeGroup) hedgeNeed {
	var first *v1alpha1.Instance
	for _, m := range g.members {
		if m.Name == g.key {
			first = m
		}
	}
	if first == nil {
		for _, m := range g.members {
			if first == nil || olderThan(m, first) {
				first = m
			}
		}
	}
	n := hedgeNeed{first: first}
	if name := first.Annotations[v1alpha1.AnnotationPredecessor]; name != "" {
		n.pred = p.instanceNamed(name)
		n.predGone = n.pred == nil
	}
	return n
}

// startDeadline returns a hedge's start deadline and the handoverMargin it is planned with, and false when it
// can't be read (a predecessor's Provider or lease unknown): the hedge then waits.
func (p *pass) startDeadline(n hedgeNeed) (deadline time.Time, margin time.Duration, replacement, ok bool) {
	switch {
	case n.predGone:
		pv := p.pvOf(n.first.Labels)
		if pv == nil {
			return time.Time{}, 0, true, false
		}
		return p.now, pv.settings.handoverMargin, true, true
	case n.pred != nil:
		pv := p.pvOf(n.pred.Labels)
		if pv == nil || !pv.hasClass {
			return time.Time{}, 0, true, false
		}
		ds, ok := p.drainStart(n.pred, pv)
		if !ok {
			return time.Time{}, 0, true, false
		}
		return lateAt(ds, startToReady(p.md, pv.class)), pv.settings.handoverMargin, true, true
	}
	pv := p.pvOf(n.first.Labels)
	if pv == nil {
		return time.Time{}, 0, false, false
	}
	return n.first.CreationTimestamp.Time.Add(pv.settings.handoverMargin), pv.settings.handoverMargin, false, true
}

// hedgeTime is a member's hedge time (HBEP-7, Hedging (beta), A member's hedge time), from its Allocation's
// submission time and its Provider's queue waits (hedgeTimeOf).
func (p *pass) hedgeTime(m *v1alpha1.Instance, deadline time.Time, margin time.Duration, replacement bool) time.Time {
	var waits []v1alpha1.QueueWait
	if pv := p.pvOf(m.Labels); pv != nil && pv.prov != nil {
		waits = pv.prov.Status.QueueWaits
	}
	var submit *time.Time
	if a := p.allocOf(m); a != nil && a.Status.AtProvider.SubmitTime != nil {
		t := a.Status.AtProvider.SubmitTime.Time
		submit = &t
	}
	return hedgeTimeOf(p.now, submit, waits, deadline, margin, replacement)
}

// hedgeTimeOf is the hedge-time rule on its inputs: the expected start is the submission time, or now while there is
// none, plus the planning wait (p90Wait); with no wait observed it is the deadline minus margin for a replacement
// and the deadline for a missing primary. The hedge time is now when the expected start is after the deadline, and
// the expected start otherwise: a member still pending then has outlived the prediction.
func hedgeTimeOf(now time.Time, submit *time.Time, waits []v1alpha1.QueueWait, deadline time.Time, margin time.Duration, replacement bool) time.Time {
	var expected time.Time
	if wait, ok := p90Wait(waits); ok {
		base := now
		if submit != nil {
			base = *submit
		}
		expected = base.Add(wait)
	} else if replacement {
		expected = deadline.Add(-margin)
	} else {
		expected = deadline
	}
	if expected.After(deadline) {
		return now // the observed waits exceed the time left: predicted late
	}
	return expected
}

// hedgeFrozen reports whether an executing Operation holds the hedge, so no member is added to it (HBEP-7, Hedging
// (beta), Operations: "While an Operation is Executing, no member is added to a hedge among its committed
// targets"). It is false until #589's execution reads the committed targets; Reconciler.hedgeFrozen, when set,
// answers instead (the hook #589 PR 5 fills, and tests set).
func (p *pass) hedgeFrozen(g hedgeGroup) bool {
	if p.r.hedgeFrozen != nil {
		return p.r.hedgeFrozen(g)
	}
	return false
}

// growHedges adds, to each hedge with no winner whose members have all reached their hedge times, its next member
// (HBEP-7, Reconcile loop, step 8; Hedging (beta), When a hedge grows; Which Provider is next). It runs after step
// 7's new primaries and step 8's replacements, so a first member made this pass, which has no submission time
// yet, is read with now as its submission, and a missing primary whose Provider's waits exceed the margin gets its
// second member in the same pass.
func (p *pass) growHedges() error {
	if p.md == nil || p.shutdown || p.md.Spec.Source.Served == nil {
		return nil
	}
	width := int(p.md.Spec.HedgeWidthCount())
	var errs []error
	for _, g := range groupHedges(p.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !p.stale[in.UID] }) {
		if len(g.members) >= width || hedgeWinner(g.members) != nil || p.hedgeFrozen(g) {
			continue
		}
		started := false
		for _, m := range g.members {
			if jobStarted(p.allocOf(m)) || !m.DeletionTimestamp.IsZero() {
				started = true
			}
		}
		if started {
			continue // a member whose job started is expected to serve within the class's start-to-ready time
		}
		need := p.needOf(g)
		deadline, margin, replacement, ok := p.startDeadline(need)
		if !ok {
			continue
		}
		due, latest := true, time.Time{}
		for _, m := range g.members {
			ht := p.hedgeTime(m, deadline, margin, replacement)
			if ht.After(p.now) {
				due = false
			}
			if ht.After(latest) {
				latest = ht
			}
		}
		if !due {
			p.at(latest)
			continue
		}
		skip := map[string]bool{}
		for _, m := range g.members {
			skip[m.Labels[v1alpha1.LabelProvider]] = true
		}
		if _, err := p.provisionWith(need.pred, placeOpts{hedgeGroup: need.first.Name, skipProviders: skip, quiet: true, caps: true}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

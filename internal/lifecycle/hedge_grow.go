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
// than the hedge's start deadline, but never before hedgeMinWait after its submission (or after now, while it has
// none), or now when the expected start is later than the deadline (the observed waits exceed the time left). The start
// deadline of a replacement hedge is its predecessor's ReplacementLate deadline; of a missing primary's, its first
// member's creation plus handoverMargin. With no wait observed on a member's Provider, its expected start is the
// deadline minus handoverMargin for a replacement and the deadline for a missing primary, so a missing primary
// with no wait observed never hedges in its first pass. No member is added while any member's job has started,
// beyond hedgeWidth live members, or while an executing Operation holds the hedge (hedgeFrozen). At most one
// member is added per hedge per pass, so a pass reserves on one Provider at a time; the next pass adds the next.
// Every input is in status, so a restarted controller adds the same members at the same times, and each pending
// member's hedge time is a requeue deadline (step 11).

// hedgeNeed is what a hedge is for: its key (the first member's name, which every later member names, the first
// member gone or not), its first live member, and the predecessor it replaces (nil for a missing primary). predGone
// is set for a replacement hedge whose predecessor is gone: its deadline has passed.
type hedgeNeed struct {
	key      string
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
	n := hedgeNeed{key: g.key, first: first}
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

// hedgeMinWait is the least a member waits after its submission (after now, while it has none) before it is judged
// late, when it is not predicted late (HBEP-7, Hedging (beta), A member's hedge time; the science agent's ruling on
// #627's main merge, 17 Nov 2026). Without it a Provider whose observed waits are all near zero has a planning wait
// near zero, so a member still pending one pass after its submission has outlived its prediction and gains a sibling
// at once, and on two fast Providers both start: a started loser, which A rescue, not insurance rules out. Ten
// minutes is under the default handoverMargin, so a missing primary still gets its second member within its margin.
// A fixed value in beta; it becomes a profile setting only if beta's waits show a platform needs another.
const hedgeMinWait = 10 * time.Minute

// hedgeTimeOf is the hedge-time rule on its inputs: the expected start is the submission time, or now while there is
// none, plus the planning wait (p90Wait); with no wait observed it is the deadline minus margin for a replacement
// and the deadline for a missing primary. The hedge time is now when the expected start is after the deadline: the
// member is predicted late, and the minimum wait does not apply to it. Otherwise it is the expected start, but never
// before the submission time, or now while there is none, plus hedgeMinWait: a member still pending then has
// outlived the prediction, and one that is merely starting normally isn't late.
func hedgeTimeOf(now time.Time, submit *time.Time, waits []v1alpha1.QueueWait, deadline time.Time, margin time.Duration, replacement bool) time.Time {
	base := now
	if submit != nil {
		base = *submit
	}
	var expected time.Time
	if wait, ok := p90Wait(waits); ok {
		expected = base.Add(wait)
	} else if replacement {
		expected = deadline.Add(-margin)
	} else {
		expected = deadline
	}
	if expected.After(deadline) {
		return now // the observed waits exceed the time left: predicted late
	}
	if floor := base.Add(hedgeMinWait); expected.Before(floor) {
		return floor
	}
	return expected
}

// hedgeFrozen reports whether an executing Operation holds the hedge, so no member is added to it (HBEP-7, Hedging
// (beta), Operations: "While an Operation is Executing, no member is added to a hedge among its committed
// targets"). The pass answers from its own Operations (frozenByOperation: p.ops is read by the Operations step before
// step 7, and a failed read fails the pass); Reconciler.hedgeFrozen, when set, answers instead, the seam tests use.
func (p *pass) hedgeFrozen(g hedgeGroup) bool {
	if p.r.hedgeFrozen != nil {
		return p.r.hedgeFrozen(g)
	}
	return frozenByOperation(g.members, p.ops)
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
	// Only a need grows: a chosen primary's hedge, and a chosen primary's replacement hedge (selectPrimaries, through
	// primaries). A surplus Instance's hedge, an extra's or a primary's own made surplus, keeps its members and gets
	// no new one: the need it was for is met (#616; #635's Fable review, agreed with k616).
	needs := map[string]bool{}
	for _, c := range p.primaries() {
		needs[hedgeKey(c)] = true
		for _, r := range p.replacementMembers(c) {
			needs[hedgeKey(r)] = true
		}
	}
	var errs []error
	for _, g := range groupHedges(p.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !p.stale[in.UID] }) {
		if !needs[g.key] {
			continue
		}
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
		// The new member names the hedge's key, never a surviving member's name: once the first member has left, a
		// survivor's name would start a second hedge for the same need (#627's review).
		if _, err := p.provisionWith(need.pred, placeOpts{hedgeGroup: need.key, skipProviders: skip, quiet: true, caps: true}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

package lifecycle

import (
	"sort"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Start-up outcomes (beta; HBEP-7, Hedging (beta), The width, The start-up failure rate; #604 PR 4).
//
// Each Allocation of the deployment whose job started adds one start-up outcome to its accelerator class: a success
// when its first Instance becomes ready, or passes its checks and is ranked out before ready (Over-allocation and
// qualification (beta), How k adjusts); a failure when that Instance ends before ready because its start,
// verification, qualification or warm-up failed, its registration or start timed out, or its job was requeued,
// failed or lost. A failed verification counts when it is a rehash, staging or node fault; a settings contradiction
// or a shape misfit adds none, since it faults the spec and stops the class (recordStartOutcomes reads the
// verification record for it). Nor does an Instance ended before ready by a hedge loss, a scale-down, a shutdown or a
// deletion. A class keeps its latest MaxStartOutcomes, each keyed by its Allocation's UID, so a restarted controller
// counts none twice; its rate is failures of count, shown from one outcome.

// endOutcome is what an Instance's end before ready adds: a failure, a success, or nothing. Every end reason the
// package writes has an entry; TestEveryEndReasonHasAnOutcome holds the table to the emit sites.
var endOutcome = map[string]string{
	// Its start, registration, verification, qualification or warm-up failed, or timed out.
	v1alpha1.ReasonRegistrationTimeout: v1alpha1.StartOutcomeFailed,
	v1alpha1.ReasonStartTimeout:        v1alpha1.StartOutcomeFailed,
	v1alpha1.ReasonTunnelNotConnected:  v1alpha1.StartOutcomeFailed, // its start timed out with its tunnel down
	v1alpha1.ReasonVerificationFailed:  v1alpha1.StartOutcomeFailed, // a rehash, staging or node fault; a spec fault adds none
	v1alpha1.ReasonQualificationFailed: v1alpha1.StartOutcomeFailed, // qualification or warm-up
	v1alpha1.ReasonRestartLoop:         v1alpha1.StartOutcomeFailed, // its restarts before ready kept failing
	// Its job was requeued, failed or lost (the science agent's ruling on #604's plan, Q6: an Allocation ended early
	// or lost before ready is a failure, unless the end was our own cancel).
	v1alpha1.ReasonRequeued:                     v1alpha1.StartOutcomeFailed,
	v1alpha1.ReasonAllocationFailed:             v1alpha1.StartOutcomeFailed,
	v1alpha1.ReasonAllocationLost:               v1alpha1.StartOutcomeFailed,
	v1alpha1.ReasonAllocationEndedEarly:         v1alpha1.StartOutcomeFailed,
	string(v1alpha1.ConditionRelayEndedForGood): v1alpha1.StartOutcomeFailed, // the job runs, but nothing can reach it: lost
	// Passed its checks, and ranked out before ready.
	v1alpha1.ReasonRankedOut: v1alpha1.StartOutcomeReady,
	// Ended by the controller's own decision, or for no fault of a start: no outcome.
	v1alpha1.ReasonDrained:                     "", // a scale-down, a shutdown, a deletion, a drain
	v1alpha1.ReasonHedgeLost:                   "",
	v1alpha1.ReasonSpecUnsupported:             "", // its Allocation can never be placed: the spec's, not a start's
	v1alpha1.ReasonAllocationRefused:           "", // refused at submission: its job never started
	string(v1alpha1.ConditionSubmittedAsOther): "", // submitted under another identity: the configuration's, not a start's
}

// startOutcomeOf returns the start-up outcome an Instance adds, and false when it adds none: its Allocation's job has
// not started, it is not its Allocation's first Instance (the caller checks firstOnAllocation), it is still starting,
// or it ended before ready with a reason that adds none. A ready Instance is a success at its ready time; an Instance
// ranked out before ready a success at its end; one that ended before ready for a start-up fault a failure at its
// end, with its end reason. A VerificationFailed end is a failure here; recordStartOutcomes drops one whose
// verification record names a spec fault. Each outcome carries the throughput gate's verdict when its Instance has
// one (gateOutcomeOf). It reads only the two objects.
func startOutcomeOf(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) (v1alpha1.StartOutcome, bool) {
	if alloc == nil || alloc.UID == "" || alloc.Status.AtProvider.StartTime == nil {
		return v1alpha1.StartOutcome{}, false
	}
	o := v1alpha1.StartOutcome{AllocationUID: alloc.UID}
	o.Gate, o.ReferenceDate = gateOutcomeOf(in)
	if in.Status.ReadyTime != nil {
		o.Result, o.Time = v1alpha1.StartOutcomeReady, *in.Status.ReadyTime
		return o, true
	}
	if !ended(in) || in.Status.EndTime == nil {
		return v1alpha1.StartOutcome{}, false
	}
	switch endOutcome[in.Status.EndReason] {
	case v1alpha1.StartOutcomeReady:
		o.Result, o.Time = v1alpha1.StartOutcomeReady, *in.Status.EndTime
	case v1alpha1.StartOutcomeFailed:
		o.Result, o.Reason, o.Time = v1alpha1.StartOutcomeFailed, in.Status.EndReason, *in.Status.EndTime
	default:
		return v1alpha1.StartOutcome{}, false
	}
	return o, true
}

// gateOutcomeOf is the throughput gate's verdict an outcome keeps (HBEP-7, How k adjusts, Recorded): the Instance's
// final verdict, and the matched entry's date with a Passed, Missed or Suspended one. Empty for an Instance the gate
// has not judged finally: one that never reached it, or one whose first miss awaits its retry.
func gateOutcomeOf(in *v1alpha1.Instance) (verdict, date string) {
	g := in.Status.Gate
	if g == nil || g.Verdict == "" {
		return "", ""
	}
	if g.Verdict == v1alpha1.GateRecordOnly {
		return g.Verdict, ""
	}
	return g.Verdict, g.ReferenceDate
}

// classCounts returns a class's counts from its outcomes, the one place they come from: the failures, those of them
// in verification or qualification (#616: the CEL rule on qualificationFailures counts a Failed outcome whose reason
// is VerificationFailed or QualificationFailed), and the outcomes kept. The API server refuses a status whose counts
// disagree with its outcomes, the whole ModelDeployment status write with it, so no count is ever set apart.
func classCounts(outcomes []v1alpha1.StartOutcome) (failures, qualificationFailures, count int32) {
	for _, o := range outcomes {
		if o.Result != v1alpha1.StartOutcomeFailed {
			continue
		}
		failures++
		if o.Reason == v1alpha1.ReasonVerificationFailed || o.Reason == v1alpha1.ReasonQualificationFailed {
			qualificationFailures++
		}
	}
	return failures, qualificationFailures, int32(len(outcomes))
}

// specFault reports whether the Instance's verification failed on a spec fault, a settings contradiction or a shape
// misfit, from its latest verification record. One whose record can't be read now is not known yet: ok is false, and
// its outcome waits for a later pass.
func (p *pass) specFault(in *v1alpha1.Instance) (fault, ok bool) {
	if in.Status.VerificationRecord == "" {
		return false, true
	}
	d, found, err := p.r.readVerification(p.ctx, in.Namespace, in.Status.VerificationRecord)
	if err != nil {
		return false, false
	}
	return found && (d.Cause == causeContradiction || d.Cause == causeShape), true
}

// recordStartOutcomes adds to the status each class's new start-up outcomes, from the pass's Instances and
// Allocations: one per Allocation whose UID the class does not keep yet, read from its first Instance. A class keeps
// its latest MaxStartOutcomes by the time each was decided, oldest first, so an outcome found again after it was
// trimmed is trimmed again, and its counts are set from them (classCounts). An outcome whose
// Allocation is gone before a pass recorded it is not recorded; a failure releases its Allocation, but the deletion
// takes at least the pass that ends the Instance, which records it.
func (p *pass) recordStartOutcomes(s *v1alpha1.ModelDeploymentStatus) {
	at := map[v1alpha1.AcceleratorClass]int{}
	kept := map[string]bool{}
	for i, c := range s.StartOutcomes {
		at[c.AcceleratorClass] = i
		for _, o := range c.Outcomes {
			kept[string(o.AllocationUID)] = true
		}
	}
	type added struct {
		class v1alpha1.AcceleratorClass
		o     v1alpha1.StartOutcome
	}
	var news []added
	for _, in := range p.insts {
		if p.stale[in.UID] {
			continue
		}
		a := p.allocOf(in)
		pv := p.pvOf(in.Labels)
		if a == nil || kept[string(a.UID)] || pv == nil || !pv.hasClass || !p.firstOnAllocation(in) {
			continue
		}
		o, ok := startOutcomeOf(in, a)
		if !ok {
			continue
		}
		if o.Result == v1alpha1.StartOutcomeFailed && o.Reason == v1alpha1.ReasonVerificationFailed {
			if fault, known := p.specFault(in); !known || fault {
				continue
			}
		}
		kept[string(a.UID)] = true
		news = append(news, added{class: pv.class, o: o})
	}
	sort.SliceStable(news, func(i, j int) bool {
		if !news[i].o.Time.Equal(&news[j].o.Time) {
			return news[i].o.Time.Before(&news[j].o.Time)
		}
		return news[i].o.AllocationUID < news[j].o.AllocationUID
	})
	for _, n := range news {
		i, ok := at[n.class]
		if !ok {
			i = len(s.StartOutcomes)
			at[n.class] = i
			s.StartOutcomes = append(s.StartOutcomes, v1alpha1.ClassStartOutcomes{AcceleratorClass: n.class})
		}
		s.StartOutcomes[i].Outcomes = append(s.StartOutcomes[i].Outcomes, n.o)
	}
	for i := range s.StartOutcomes {
		c := &s.StartOutcomes[i]
		// Kept and new outcomes in one order, by the time each was decided (a tie on the Allocation's UID), before the
		// oldest go. An outcome trimmed earlier whose Allocation still lives (a serving Instance, a warm spare) is found
		// again each pass; ordered by its time it sorts to the head and is trimmed again, rather than coming back at
		// the tail and pushing a newer one out (PR 650's review: the window would drift to Ready entries).
		sort.SliceStable(c.Outcomes, func(a, b int) bool {
			if !c.Outcomes[a].Time.Equal(&c.Outcomes[b].Time) {
				return c.Outcomes[a].Time.Before(&c.Outcomes[b].Time)
			}
			return c.Outcomes[a].AllocationUID < c.Outcomes[b].AllocationUID
		})
		if over := len(c.Outcomes) - v1alpha1.MaxStartOutcomes; over > 0 {
			c.Outcomes = append([]v1alpha1.StartOutcome(nil), c.Outcomes[over:]...)
		}
		c.Failures, c.QualificationFailures, c.Count = classCounts(c.Outcomes)
	}
}

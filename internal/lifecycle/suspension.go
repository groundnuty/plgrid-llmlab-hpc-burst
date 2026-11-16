package lifecycle

import (
	"fmt"
	"slices"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A suspect reference (HBEP-7, Over-allocation and qualification (beta), The throughput gate: a suspect reference,
// At most two releases for one reference; #616, PR 2).
//
// When a class's two most recent Passed or Missed outcomes against one entry and its date, from two Allocations, are
// both Missed, the entry is suspended for the deployment: its key and date are appended to
// status.suspendedReferences in the pass that records the second miss, and one degraded ReferenceSuspect event
// regards the ModelDeployment. The gate then records only against an entry listed with the same date (the verdict
// Suspended). The listing is held in status, not derived from the outcomes, so it outlives the class's 20 kept
// outcomes and a restarted controller reads it. Each pass removes a listing whose entry is gone from the profiles or
// has another date.
//
// A suspension is also written before a third failure is made: the gate counts, on top of the misses in the recorded
// outcomes, the Instances that already failed against the entry and whose outcome is not recorded yet, which is every
// one this pass has failed (step 10 records them) and any an earlier pass failed but could not record. If this failure
// would be the third, the entry is suspended in this pass and the Instance records Suspended, so three Instances
// reaching their second miss in one pass release two Allocations, never three.
//
// An outcome carries the matched entry's date but not its key. The run of misses is therefore counted by the class
// and the date, and the key of the entry that is listed comes from the Instance of the newest miss (missedKey). Two
// entries with one date that differ in their key, one class's deployment having been edited from one to the other, so
// count as one run; the listing names only the newest miss's entry, and the gate stays on for the other.

// suspectRun is how many of the class's most recent Passed or Missed outcomes against the entry dated date are
// Missed in an unbroken run, newest first. An outcome that is not Passed or Missed (Suspended, RecordOnly, none), or
// is against another entry, is passed over: it neither breaks the run nor joins it.
func suspectRun(outcomes []v1alpha1.StartOutcome, date string) int {
	run := 0
	for i := len(outcomes) - 1; i >= 0; i-- {
		o := outcomes[i]
		if o.ReferenceDate != date || (o.Gate != v1alpha1.GatePassed && o.Gate != v1alpha1.GateMissed) {
			continue
		}
		if o.Gate != v1alpha1.GateMissed {
			break
		}
		run++
	}
	return run
}

// classOutcomes are the class's recorded outcomes, oldest first.
func classOutcomes(s *v1alpha1.ModelDeploymentStatus, class v1alpha1.AcceleratorClass) []v1alpha1.StartOutcome {
	for _, c := range s.StartOutcomes {
		if c.AcceleratorClass == class {
			return c.Outcomes
		}
	}
	return nil
}

// listedSuspended reports whether the listing holds the entry with this key and date.
func listedSuspended(list []v1alpha1.SuspendedReference, key v1alpha1.ReferenceKey, date string) bool {
	return slices.ContainsFunc(list, func(r v1alpha1.SuspendedReference) bool {
		return r.Date == date && sameKey(r.ReferenceKey, key)
	})
}

// classOf is the accelerator class of a Provider, the zero class for one whose class is not known.
func classOf(pv *providerView) v1alpha1.AcceleratorClass {
	if pv != nil && pv.hasClass {
		return pv.class
	}
	return v1alpha1.AcceleratorClass{}
}

// gateRef is what a suspension is counted against: an accelerator class's entry, by its date.
type gateRef struct {
	class v1alpha1.AcceleratorClass
	date  string
}

// suspectCount is how many misses the class has against the entry dated ref.date in an unbroken run, as the gate
// sees them now: the run in the recorded outcomes (suspectRun), and the Instances that failed against the entry and
// have no recorded outcome yet. Those are read from the Instances themselves, whose verdict Missed is written in the
// write that fails them, so the count holds for the failures of this pass, which step 10 has not recorded, and for
// any an earlier pass made and could not record, and after a controller restart. An Instance whose Allocation is
// gone has no outcome to add, and none is counted.
func (p *pass) suspectCount(ref gateRef) int {
	recorded := classOutcomes(&p.md.Status, ref.class)
	n := suspectRun(recorded, ref.date)
	kept := map[types.UID]bool{}
	for _, o := range recorded {
		kept[o.AllocationUID] = true
	}
	for _, in := range p.insts {
		g := in.Status.Gate
		if p.stale[in.UID] || g == nil || g.Verdict != v1alpha1.GateMissed || g.ReferenceDate != ref.date || classOf(p.pvOf(in.Labels)) != ref.class {
			continue
		}
		if a := p.allocOf(in); a != nil && !kept[a.UID] && p.firstOnAllocation(in) {
			n++
		}
	}
	return n
}

// referenceSuspended reports whether the entry with this key and date is suspended for the deployment, as the gate
// reads it: it is listed, or the pass is about to list it, because the misses against it make two (suspectCount). The
// second case is remembered for step 10, which lists it even when the outcomes of the failures are not recorded in it.
func (p *pass) referenceSuspended(pv *providerView, entry v1alpha1.ReferenceKey, date string) bool {
	if p.md == nil {
		return false
	}
	if listedSuspended(p.md.Status.SuspendedReferences, entry, date) {
		return true
	}
	ref := gateRef{class: classOf(pv), date: date}
	if p.suspectCount(ref) < 2 {
		return false
	}
	if p.suspectNow == nil {
		p.suspectNow = map[gateRef]v1alpha1.ReferenceKey{}
	}
	p.suspectNow[ref] = entry
	return true
}

// maxSuspendedReferences is the listing's bound (the CRD's maxItems).
const maxSuspendedReferences = 32

// updateSuspensions is step 10's part of suspension, after the pass's outcomes are recorded in s: it removes the
// listings whose entry is gone or has another date, and lists the entries the outcomes (and the gate of this pass)
// show suspect. It returns the listings it added, for their events. A class whose newest two Passed or Missed
// outcomes against a date are Missed is listed when the profile holds the entry with that date for the newest
// miss's Instance; a listing the profile no longer backs is not made, so a suspension that was lifted by a changed
// date does not come back from outcomes that still carry the old one.
func (p *pass) updateSuspensions(s *v1alpha1.ModelDeploymentStatus) []v1alpha1.SuspendedReference {
	if p.md == nil {
		return nil
	}
	list := slices.Clone(s.SuspendedReferences)
	if p.profilesRead() {
		list = slices.DeleteFunc(list, func(r v1alpha1.SuspendedReference) bool { return !p.profilesHold(r.ReferenceKey, r.Date) })
	}
	var added []v1alpha1.SuspendedReference
	now := metav1.NewTime(p.now)
	add := func(key v1alpha1.ReferenceKey, date string) {
		if listedSuspended(list, key, date) {
			return
		}
		r := v1alpha1.SuspendedReference{ReferenceKey: key, Date: date, SuspendedTime: now}
		list = append(list, r)
		added = append(added, r)
	}
	for _, ref := range p.sortedSuspectNow() {
		add(p.suspectNow[ref], ref.date)
	}
	for _, c := range s.StartOutcomes {
		for _, date := range missedDates(c.Outcomes) {
			if suspectRun(c.Outcomes, date) < 2 {
				continue
			}
			if key, ok := p.newestMissedKey(c, date); ok && p.profilesHold(key, date) {
				add(key, date)
			}
		}
	}
	if over := len(list) - maxSuspendedReferences; over > 0 {
		sort.SliceStable(list, func(i, j int) bool { return list[i].SuspendedTime.Before(&list[j].SuspendedTime) })
		list = list[over:]
	}
	s.SuspendedReferences = list
	return added
}

// sortedSuspectNow is the entries this pass found suspect while it judged, in a fixed order.
func (p *pass) sortedSuspectNow() []gateRef {
	refs := make([]gateRef, 0, len(p.suspectNow))
	for r := range p.suspectNow {
		refs = append(refs, r)
	}
	sort.Slice(refs, func(i, j int) bool {
		if a, b := refs[i].class, refs[j].class; a != b {
			return a.GPUModel+"/"+a.CPUArch < b.GPUModel+"/"+b.CPUArch
		}
		return refs[i].date < refs[j].date
	})
	return refs
}

// missedDates are the dates of the Missed outcomes, once each, in a fixed order.
func missedDates(outcomes []v1alpha1.StartOutcome) []string {
	var dates []string
	for _, o := range outcomes {
		if o.Gate == v1alpha1.GateMissed && o.ReferenceDate != "" && !slices.Contains(dates, o.ReferenceDate) {
			dates = append(dates, o.ReferenceDate)
		}
	}
	sort.Strings(dates)
	return dates
}

// newestMissedKey is the key of the entry the class's newest Missed outcome against date was judged on, read from
// that outcome's Instance when the pass still holds it. ok is false when it does not, or its key can't be read: the
// suspension is then made by a later pass, or by this pass's own gate (suspectNow).
func (p *pass) newestMissedKey(c v1alpha1.ClassStartOutcomes, date string) (v1alpha1.ReferenceKey, bool) {
	for i := len(c.Outcomes) - 1; i >= 0; i-- {
		o := c.Outcomes[i]
		if o.Gate != v1alpha1.GateMissed || o.ReferenceDate != date {
			continue
		}
		for _, in := range p.insts {
			if a := p.allocOf(in); a != nil && a.UID == o.AllocationUID && p.firstOnAllocation(in) {
				key, err := p.missedKey(in)
				return key, err == nil
			}
		}
		return v1alpha1.ReferenceKey{}, false
	}
	return v1alpha1.ReferenceKey{}, false
}

// missedKey is the key an Instance with a Missed verdict was judged on: its ID and flags, the sessions of its
// recorded miss, and the typical context and output of its verification record.
func (p *pass) missedKey(in *v1alpha1.Instance) (v1alpha1.ReferenceKey, error) {
	g := in.Status.Gate
	if g == nil || len(g.Misses) == 0 {
		return v1alpha1.ReferenceKey{}, fmt.Errorf("Instance %s has no recorded miss", in.Name)
	}
	rec := in.Status.VerificationRecord
	if w := workerNames(in)[0]; several(in) {
		rec = workerEntry(in, w).Record
	}
	shapes, err := p.recordedShapes(in, gateWorker{name: workerNames(in)[0], record: rec})
	if err != nil {
		return v1alpha1.ReferenceKey{}, err
	}
	return instanceKey(in, ranShape{sessions: g.Misses[0].Throughput.TargetSessions, context: shapes.ContextTypical, output: shapes.OutputTypical}), nil
}

// profilesRead reports whether every Provider's profile of the deployment was read: a listing is lifted only on
// what was seen, never on a profile that could not be.
func (p *pass) profilesRead() bool {
	for _, pv := range p.providers {
		if pv.profile == nil {
			return false
		}
	}
	return len(p.providers) > 0
}

// profilesHold reports whether a Provider's profile of the deployment holds the entry with this key and date.
func (p *pass) profilesHold(key v1alpha1.ReferenceKey, date string) bool {
	for _, pv := range p.providers {
		if pv.profile == nil {
			continue
		}
		for _, e := range pv.profile.Spec.ExpectedPerformance {
			if e.Date == date && sameKey(e.ReferenceKey, key) {
				return true
			}
		}
	}
	return false
}

// announceSuspended raises the one degraded ReferenceSuspect event of each listing step 10 has written, regarding the
// ModelDeployment and naming the entry. It runs once the status write has succeeded, so a failed write raises
// nothing and the next pass, which lists the entry again, raises it once. The listing is the level truth, and no
// condition carries the event (HBEP-7, `ReferenceSuspect` is emitted once).
func (p *pass) announceSuspended(added []v1alpha1.SuspendedReference) {
	for _, r := range added {
		p.r.Recorder.Event(p.md, nil, eventDegraded, v1alpha1.ReasonReferenceSuspect,
			fmt.Sprintf("the expected-performance entry dated %s for %s is suspended: two Allocations in a row missed it, so the throughput gate records only against it until the entry's date changes", r.Date, describeKey(r.ReferenceKey)))
	}
}

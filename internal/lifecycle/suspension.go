package lifecycle

import (
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A suspect reference (HBEP-7, Over-allocation and qualification (beta), The throughput gate: a suspect reference,
// At most two releases for one reference; amendment #655, the science agent's ruling on #652; #616, PR 2).
//
// When the two most recent Passed or Missed verdicts against one entry, from two Allocations, are both Missed, the
// entry is suspended for the deployment: it is listed in status.suspendedReferences and one degraded ReferenceSuspect
// event regards the ModelDeployment. The gate then records only against an entry listed with the same profile, key
// and date (the verdict Suspended), so a wrong reference releases at most two Allocations.
//
// An entry is one profile's: it is keyed by the PlatformProfile it comes from, its key and its date (entryRef). Two
// profiles' entries for one class are two entries, so a right entry's passes never break a wrong entry's run, and
// Providers sharing one profile share its entries, so together they release at most two. Two entries of one profile
// with one date and different keys (two shapes) are two entries as well.
//
// Every gated Instance's verdict counts, not only an Allocation's first: an Instance placed on a warm Allocation is
// judged by the gate like any. The run is held in status.referenceMisses, one item per entry whose latest verdict is
// Missed and that is not suspended yet. A Passed verdict against the entry removes it; a Missed from another
// Allocation suspends the entry and removes it. recordVerdict writes that change to the ModelDeployment's status in
// the pass that judges, BEFORE the Instance's own verdict is written: an Instance's Missed or Passed verdict never
// exists without its count, whatever the pass returns on after, and a write that fails leaves the Instance unjudged,
// so the next pass judges it again (the count is idempotent by Instance). The record is kept in status, so it outlasts
// restart.window, which prunes the ended Instances, and a controller restart. Three Instances on their retry in one
// pass are judged one after the other, each reading the record the last one wrote, so the third reads the suspension
// the second wrote, and two Allocations are released, never three.
//
// The two lists' caps err toward recording only (the science agent's ruling on #652): a Missed verdict that the full
// record (32) can't hold suspends its entry at once rather than evict another entry's miss, and while the listing is
// full (32) the gate records only for the deployment (gateJudge), with one event when it fills, and never drops a
// listing to make room. Only a profile edit lifts a listing (liftSuspensions).

// entryRef is one expected-performance entry as the run counts it: its profile, its key and its date.
type entryRef struct {
	profile string
	key     v1alpha1.ReferenceKey
	date    string
}

// isMiss reports whether the record is this entry's.
func (e entryRef) isMiss(m v1alpha1.ReferenceMiss) bool {
	return m.PlatformProfile == e.profile && m.Date == e.date && sameKey(m.ReferenceKey, e.key)
}

// isListed reports whether the listing is this entry's: the same profile, key and date. A listing that names no
// profile matches the entry in any profile.
func (e entryRef) isListed(r v1alpha1.SuspendedReference) bool {
	return (r.PlatformProfile == "" || r.PlatformProfile == e.profile) && r.Date == e.date && sameKey(r.ReferenceKey, e.key)
}

// referenceSuspended reports whether the entry is listed as suspended for the deployment, as the gate reads it.
func (p *pass) referenceSuspended(e entryRef) bool {
	return p.md != nil && slices.ContainsFunc(p.md.Status.SuspendedReferences, e.isListed)
}

// maxReferenceMisses and maxSuspendedReferences are the two lists' bounds (the CRD's maxItems). Neither list ever
// drops an item to make room (countVerdict).
const (
	maxReferenceMisses     = 32
	maxSuspendedReferences = 32
)

// recordVerdict counts a gate verdict toward the entry's suspension, before the Instance's own write (see above). It
// writes nothing for work that counts nothing (a first miss, Suspended, RecordOnly), or when the record already says
// it. It returns an error when the write fails; the caller then returns it, leaving the Instance unjudged.
func (p *pass) recordVerdict(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, w gateWork) error {
	if w.ref == nil || p.md == nil || alloc == nil {
		return nil
	}
	before := len(p.md.Status.SuspendedReferences)
	misses, listed, added, changed := countVerdict(p.md.Status.ReferenceMisses, p.md.Status.SuspendedReferences, *w.ref, w.counts, in, alloc, metav1.NewTime(p.now))
	if !changed {
		return nil
	}
	next := p.md.DeepCopy()
	next.Status.ReferenceMisses, next.Status.SuspendedReferences = misses, listed
	if err := p.updateMDStatus(next); err != nil {
		return fmt.Errorf("record the throughput gate's %s verdict against the entry dated %s: %w", w.counts, w.ref.date, err)
	}
	p.announceSuspended(added)
	if before < maxSuspendedReferences && len(listed) >= maxSuspendedReferences {
		p.announceListFull()
	}
	return nil
}

// listFull reports whether the deployment's listing of suspended entries is at its cap: the gate then records only
// for the deployment (gateJudge), since no further entry could be suspended.
func (p *pass) listFull() bool {
	return p.md != nil && len(p.md.Status.SuspendedReferences) >= maxSuspendedReferences
}

// countVerdict is recordVerdict's change, pure: the record and the listing after one verdict against the entry, the
// listing it added (a suspension), and whether anything changed. A Passed verdict removes the entry's miss. A Missed
// verdict is recorded once per Instance; one from another Allocation than the recorded miss suspends the entry, and
// one from the same Allocation takes the recorded one's place. A first miss that the full record can't hold suspends
// the entry at once. Neither list drops an item: with the listing full, nothing is listed and the record is left as
// it was (gateJudge records only then, so no Missed verdict reaches here).
func countVerdict(misses []v1alpha1.ReferenceMiss, listed []v1alpha1.SuspendedReference, e entryRef, verdict string,
	in *v1alpha1.Instance, alloc *v1alpha1.Allocation, now metav1.Time) (_ []v1alpha1.ReferenceMiss, _, added []v1alpha1.SuspendedReference, changed bool) {
	i := slices.IndexFunc(misses, e.isMiss)
	switch verdict {
	case v1alpha1.GatePassed:
		if i < 0 {
			return misses, listed, nil, false
		}
		return slices.Delete(slices.Clone(misses), i, i+1), listed, nil, true
	case v1alpha1.GateMissed:
	default:
		return misses, listed, nil, false
	}
	if i >= 0 && misses[i].InstanceUID == in.UID {
		return misses, listed, nil, false
	}
	suspend := func() ([]v1alpha1.ReferenceMiss, []v1alpha1.SuspendedReference, []v1alpha1.SuspendedReference, bool) {
		if len(listed) >= maxSuspendedReferences {
			return misses, listed, nil, false
		}
		r := v1alpha1.SuspendedReference{PlatformProfile: e.profile, ReferenceKey: e.key, Date: e.date, SuspendedTime: now}
		out := misses
		if i >= 0 {
			out = slices.Delete(slices.Clone(misses), i, i+1)
		}
		return out, append(slices.Clone(listed), r), []v1alpha1.SuspendedReference{r}, true
	}
	if i >= 0 && misses[i].AllocationUID != alloc.UID {
		return suspend()
	}
	m := v1alpha1.ReferenceMiss{PlatformProfile: e.profile, ReferenceKey: e.key, Date: e.date, InstanceUID: in.UID, AllocationUID: alloc.UID, Time: now}
	if i >= 0 {
		out := slices.Clone(misses)
		out[i] = m
		return out, listed, nil, true
	}
	if len(misses) >= maxReferenceMisses {
		return suspend()
	}
	return append(slices.Clone(misses), m), listed, nil, true
}

// liftSuspensions is step 10's part of suspension: it removes each listing, and each recorded miss, whose profile no
// longer holds the entry with that key and date (the entry was edited or removed). Only a profile the pass read is
// judged: a listing of a profile it could not read, or that none of the deployment's Providers uses now, is kept. A
// listing that names no profile is removed only once every profile was read and none holds it.
func (p *pass) liftSuspensions(s *v1alpha1.ModelDeploymentStatus) {
	if p.md == nil {
		return
	}
	read := p.readProfiles()
	gone := func(profile string, key v1alpha1.ReferenceKey, date string) bool {
		if profile == "" {
			if !p.profilesRead() {
				return false
			}
			for _, prof := range read {
				if holds(prof, key, date) {
					return false
				}
			}
			return true
		}
		prof, ok := read[profile]
		return ok && !holds(prof, key, date)
	}
	s.SuspendedReferences = slices.DeleteFunc(slices.Clone(s.SuspendedReferences), func(r v1alpha1.SuspendedReference) bool {
		return gone(r.PlatformProfile, r.ReferenceKey, r.Date)
	})
	s.ReferenceMisses = slices.DeleteFunc(slices.Clone(s.ReferenceMisses), func(m v1alpha1.ReferenceMiss) bool {
		return gone(m.PlatformProfile, m.ReferenceKey, m.Date)
	})
}

// readProfiles are the profiles of the deployment's Providers that the pass read, by name.
func (p *pass) readProfiles() map[string]*v1alpha1.PlatformProfile {
	out := map[string]*v1alpha1.PlatformProfile{}
	for _, pv := range p.providers {
		if pv.profile != nil {
			out[pv.profile.Name] = pv.profile
		}
	}
	return out
}

// profilesRead reports whether every Provider's profile of the deployment was read.
func (p *pass) profilesRead() bool {
	for _, pv := range p.providers {
		if pv.profile == nil {
			return false
		}
	}
	return len(p.providers) > 0
}

// holds reports whether the profile holds an entry with this key and date.
func holds(prof *v1alpha1.PlatformProfile, key v1alpha1.ReferenceKey, date string) bool {
	return slices.ContainsFunc(prof.Spec.ExpectedPerformance, func(e v1alpha1.ExpectedPerformance) bool {
		return e.Date == date && sameKey(e.ReferenceKey, key)
	})
}

// announceListFull raises one degraded ReferenceSuspect event regarding the ModelDeployment when a write fills its
// listing of suspended entries: from then on the gate records only for the deployment, until a profile edit lifts a
// listing.
func (p *pass) announceListFull() {
	p.r.Recorder.Event(p.md, nil, eventDegraded, v1alpha1.ReasonReferenceSuspect,
		fmt.Sprintf("the list of suspended expected-performance entries is full (%d): the throughput gate records only for this deployment until a listing is lifted by a profile edit", maxSuspendedReferences))
}

// announceSuspended raises the one degraded ReferenceSuspect event of each listing a write has added, regarding the
// ModelDeployment and naming the entry. It runs once the status write has succeeded, so a failed write raises
// nothing. The listing is the level truth, and no condition carries the event (HBEP-7, `ReferenceSuspect` is emitted
// once).
func (p *pass) announceSuspended(added []v1alpha1.SuspendedReference) {
	for _, r := range added {
		p.r.Recorder.Event(p.md, nil, eventDegraded, v1alpha1.ReasonReferenceSuspect,
			fmt.Sprintf("the expected-performance entry dated %s for %s in PlatformProfile %s is suspended: two Allocations in a row missed it, so the throughput gate records only against it until the entry's date changes", r.Date, describeKey(r.ReferenceKey), r.PlatformProfile))
	}
}

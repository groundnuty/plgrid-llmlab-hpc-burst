package lifecycle

// The suspension's pure parts (HBEP-7, The throughput gate: a suspect reference; #616, PR 2): the run of misses, the
// listing's lookup, its lifting and its bound. Every vector is a literal worked out by hand.

import (
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func gateOutcome(gate, date string) v1alpha1.StartOutcome {
	return v1alpha1.StartOutcome{Result: v1alpha1.StartOutcomeReady, Gate: gate, ReferenceDate: date}
}

// A run is the Missed outcomes at the end of the class's Passed and Missed ones against the date, newest first: a
// Passed one ends it, and what is not a Passed or Missed verdict against the date (Suspended, RecordOnly, none, another
// date's) is passed over.
func TestSuspectRun(t *testing.T) {
	m, p := func() v1alpha1.StartOutcome { return gateOutcome("Missed", "2026-10-23") }, func() v1alpha1.StartOutcome { return gateOutcome("Passed", "2026-10-23") }
	for _, tc := range []struct {
		name string
		in   []v1alpha1.StartOutcome
		want int
	}{
		{"none", nil, 0},
		{"one miss", []v1alpha1.StartOutcome{m()}, 1},
		{"two misses", []v1alpha1.StartOutcome{m(), m()}, 2},
		{"a miss, a pass, a miss", []v1alpha1.StartOutcome{m(), p(), m()}, 1},
		{"a pass, two misses", []v1alpha1.StartOutcome{p(), m(), m()}, 2},
		{"two misses, a pass", []v1alpha1.StartOutcome{m(), m(), p()}, 0},
		{"a RecordOnly between two misses is passed over", []v1alpha1.StartOutcome{m(), gateOutcome("RecordOnly", ""), m()}, 2},
		{"a Suspended one between two misses is passed over", []v1alpha1.StartOutcome{m(), gateOutcome("Suspended", "2026-10-23"), m()}, 2},
		{"an outcome with no verdict between two misses is passed over", []v1alpha1.StartOutcome{m(), {Result: v1alpha1.StartOutcomeFailed, Reason: v1alpha1.ReasonRegistrationTimeout}, m()}, 2},
		{"a miss of another date is another entry's", []v1alpha1.StartOutcome{gateOutcome("Missed", "2026-10-24"), m()}, 1},
		{"a pass of another date does not end this date's run", []v1alpha1.StartOutcome{m(), gateOutcome("Passed", "2026-10-24"), m()}, 2},
	} {
		if got := suspectRun(tc.in, "2026-10-23"); got != tc.want {
			t.Errorf("%s: a run of %d, want %d", tc.name, got, tc.want)
		}
	}
}

// The listing is looked up by the entry's key, compared as the matcher does (0.9 is 0.90), and its date.
func TestListedSuspendedByKeyAndDate(t *testing.T) {
	key := func(util string) v1alpha1.ReferenceKey {
		k := gateKey()
		k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", util)
		return k
	}
	list := []v1alpha1.SuspendedReference{{ReferenceKey: key("0.9"), Date: "2026-10-23"}}
	for _, tc := range []struct {
		name string
		key  v1alpha1.ReferenceKey
		date string
		want bool
	}{
		{"the same", key("0.9"), "2026-10-23", true},
		{"0.90 is 0.9", key("0.90"), "2026-10-23", true},
		{"another date", key("0.9"), "2026-10-24", false},
		{"another value", key("0.85"), "2026-10-23", false},
		{"another shape", func() v1alpha1.ReferenceKey { k := key("0.9"); k.Sessions = 16; return k }(), "2026-10-23", false},
	} {
		if got := listedSuspended(list, tc.key, tc.date); got != tc.want {
			t.Errorf("%s: listed %v, want %v", tc.name, got, tc.want)
		}
	}
}

// suspendPass is a pass over a deployment whose Providers' profiles hold entries, for updateSuspensions.
func suspendPass(listed []v1alpha1.SuspendedReference, profiles ...*v1alpha1.PlatformProfile) *pass {
	md := &v1alpha1.ModelDeployment{}
	md.Status.SuspendedReferences = listed
	p := &pass{now: time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC), md: md}
	for _, prof := range profiles {
		p.providers = append(p.providers, &providerView{profile: prof})
	}
	return p
}

// A listing is lifted when the profiles the pass read no longer hold its entry with its date: the entry is gone, or has
// another date. It is kept while one holds it, and kept when a Provider's profile could not be read, or the deployment
// has no Provider: a listing is lifted only on what was seen.
func TestSuspensionsAreLiftedOnlyWhatTheProfilesNoLongerHold(t *testing.T) {
	key := gateKey()
	listed := []v1alpha1.SuspendedReference{{ReferenceKey: key, Date: "2026-10-23", SuspendedTime: metav1.NewTime(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))}}
	entry := func(date string) *v1alpha1.PlatformProfile {
		e := gateFixtureEntry(key)
		e.Date = date
		return gateProfile(e)
	}
	for _, tc := range []struct {
		name     string
		profiles []*v1alpha1.PlatformProfile
		kept     bool
	}{
		{"the entry with its date is there", []*v1alpha1.PlatformProfile{entry("2026-10-23")}, true},
		{"the entry has another date", []*v1alpha1.PlatformProfile{entry("2026-10-24")}, false},
		{"the entry is gone", []*v1alpha1.PlatformProfile{gateProfile()}, false},
		{"another Provider's profile holds it", []*v1alpha1.PlatformProfile{gateProfile(), entry("2026-10-23")}, true},
		{"a profile that could not be read", []*v1alpha1.PlatformProfile{nil, gateProfile()}, true},
		{"no Provider", nil, true},
	} {
		p := suspendPass(listed, tc.profiles...)
		s := p.md.Status.DeepCopy()
		p.updateSuspensions(s)
		if got := len(s.SuspendedReferences) == 1; got != tc.kept {
			t.Errorf("%s: kept %v, want %v", tc.name, got, tc.kept)
		}
	}
}

// The listing is bounded by the CRD's 32: when the 33rd entry is listed, the oldest listing goes, so the status write
// is never refused for its length.
func TestSuspensionsAreBoundedBy32DroppingTheOldest(t *testing.T) {
	var entries []v1alpha1.ExpectedPerformance
	var listed []v1alpha1.SuspendedReference
	for i := range 33 {
		k := gateKey()
		k.Sessions = int32(i + 1)
		e := gateFixtureEntry(k)
		entries = append(entries, e)
		if i < 32 {
			listed = append(listed, v1alpha1.SuspendedReference{ReferenceKey: k, Date: "2026-10-23", SuspendedTime: metav1.NewTime(time.Date(2026, 11, 1, 0, i, 0, 0, time.UTC))})
		}
	}
	p := suspendPass(listed, gateProfile(entries...))
	p.suspectNow = map[gateRef]v1alpha1.ReferenceKey{{date: "2026-10-23"}: entries[32].ReferenceKey}
	s := p.md.Status.DeepCopy()
	added := p.updateSuspensions(s)
	if len(s.SuspendedReferences) != 32 || len(added) != 1 || added[0].Sessions != 33 {
		t.Fatalf("%d listings and %d added, want 32 and the 33rd added", len(s.SuspendedReferences), len(added))
	}
	for _, r := range s.SuspendedReferences {
		if r.Sessions == 1 {
			t.Errorf("the oldest listing (sessions 1) is still listed: %s", strconv.Itoa(int(r.Sessions)))
		}
	}
}

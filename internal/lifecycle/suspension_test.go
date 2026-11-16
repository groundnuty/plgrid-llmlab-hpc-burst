package lifecycle

// The suspension's pure parts (HBEP-7, The throughput gate: a suspect reference; amendment #655; #616, PR 2): an
// entry's identity (its profile, its key and its date), one verdict's count, the lifting and the bounds. Every
// vector is a literal worked out by hand.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	profA = "example-profile-a"
	profB = "example-profile-b"
)

// ref is the fixture entry of profile prof dated 2026-10-23.
func ref(prof string) entryRef { return entryRef{profile: prof, key: gateKey(), date: "2026-10-23"} }

// An entry is its profile, its key (compared as the matcher does: 0.9 is 0.90) and its date. A listing that names no
// profile matches the entry in any profile.
func TestAnEntryIsItsProfileItsKeyAndItsDate(t *testing.T) {
	key := func(util string) v1alpha1.ReferenceKey {
		k := gateKey()
		k.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", util)
		return k
	}
	e := entryRef{profile: profA, key: key("0.9"), date: "2026-10-23"}
	for _, tc := range []struct {
		name   string
		listed v1alpha1.SuspendedReference
		want   bool
	}{
		{"the same", v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: key("0.9"), Date: "2026-10-23"}, true},
		{"0.90 is 0.9", v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: key("0.90"), Date: "2026-10-23"}, true},
		{"another profile", v1alpha1.SuspendedReference{PlatformProfile: profB, ReferenceKey: key("0.9"), Date: "2026-10-23"}, false},
		{"no profile named", v1alpha1.SuspendedReference{ReferenceKey: key("0.9"), Date: "2026-10-23"}, true},
		{"another date", v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: key("0.9"), Date: "2026-10-24"}, false},
		{"another value", v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: key("0.85"), Date: "2026-10-23"}, false},
		{"another shape", v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: func() v1alpha1.ReferenceKey { k := key("0.9"); k.Sessions = 16; return k }(), Date: "2026-10-23"}, false},
	} {
		if got := e.isListed(tc.listed); got != tc.want {
			t.Errorf("%s: listed %v, want %v", tc.name, got, tc.want)
		}
	}
	miss := v1alpha1.ReferenceMiss{PlatformProfile: profA, ReferenceKey: key("0.90"), Date: "2026-10-23"}
	if !e.isMiss(miss) {
		t.Errorf("a recorded miss of the same profile, key and date is not the entry's")
	}
	miss.PlatformProfile = ""
	if e.isMiss(miss) {
		t.Errorf("a recorded miss with no profile is the entry's: a miss always names its profile")
	}
}

// inst and alloc are an Instance and an Allocation with the given UIDs.
func inst(uid string) *v1alpha1.Instance {
	return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}}
}
func alloc(uid string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}}
}

var countNow = metav1.NewTime(time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC))

// recorded is the fixture entry's recorded miss of profile prof, by Instance and Allocation.
func recorded(prof, in, al string) v1alpha1.ReferenceMiss {
	return v1alpha1.ReferenceMiss{PlatformProfile: prof, ReferenceKey: gateKey(), Date: "2026-10-23", InstanceUID: types.UID(in), AllocationUID: types.UID(al), Time: countNow}
}

// One verdict's count. A first Missed is recorded; a second from another Allocation suspends the entry and removes
// the miss; one from the same Allocation takes the recorded one's place and suspends nothing; the same Instance again
// changes nothing. A Passed removes the entry's miss, and nothing else's. Another profile's miss is another entry's.
// Suspended and RecordOnly count nothing.
func TestOneVerdictsCount(t *testing.T) {
	for _, tc := range []struct {
		name         string
		misses       []v1alpha1.ReferenceMiss
		e            entryRef
		verdict      string
		in, al       string
		wantMisses   []v1alpha1.ReferenceMiss
		wantListed   int
		wantAdded    bool
		wantChanged  bool
		wantProfiled string
	}{
		{"a first miss is recorded", nil, ref(profA), "Missed", "i1", "a1", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, 0, false, true, ""},
		{"a second miss from another Allocation suspends", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "Missed", "i2", "a2", nil, 1, true, true, profA},
		{"a second miss from the same Allocation takes the place", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "Missed", "i2", "a1", []v1alpha1.ReferenceMiss{recorded(profA, "i2", "a1")}, 0, false, true, ""},
		{"the same Instance again changes nothing", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "Missed", "i1", "a1", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, 0, false, false, ""},
		{"another profile's miss is another entry's", []v1alpha1.ReferenceMiss{recorded(profB, "i1", "a1")}, ref(profA), "Missed", "i2", "a2", []v1alpha1.ReferenceMiss{recorded(profB, "i1", "a1"), recorded(profA, "i2", "a2")}, 0, false, true, ""},
		{"a pass removes the entry's miss", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "Passed", "i2", "a2", []v1alpha1.ReferenceMiss{}, 0, false, true, ""},
		{"another profile's pass leaves the miss", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profB), "Passed", "i2", "a2", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, 0, false, false, ""},
		{"a pass with no miss changes nothing", nil, ref(profA), "Passed", "i2", "a2", nil, 0, false, false, ""},
		{"Suspended counts nothing", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "Suspended", "i2", "a2", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, 0, false, false, ""},
		{"RecordOnly counts nothing", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, ref(profA), "RecordOnly", "i2", "a2", []v1alpha1.ReferenceMiss{recorded(profA, "i1", "a1")}, 0, false, false, ""},
	} {
		misses, listed, added, changed := countVerdict(tc.misses, nil, tc.e, tc.verdict, inst(tc.in), alloc(tc.al), countNow)
		if changed != tc.wantChanged || len(listed) != tc.wantListed || (len(added) == 1) != tc.wantAdded {
			t.Errorf("%s: changed %v, %d listed, %d added; want %v, %d, added %v", tc.name, changed, len(listed), len(added), tc.wantChanged, tc.wantListed, tc.wantAdded)
		}
		if len(misses) != len(tc.wantMisses) {
			t.Errorf("%s: misses %+v, want %+v", tc.name, misses, tc.wantMisses)
			continue
		}
		for i := range misses {
			if w := tc.wantMisses[i]; misses[i].PlatformProfile != w.PlatformProfile || misses[i].InstanceUID != w.InstanceUID || misses[i].AllocationUID != w.AllocationUID || misses[i].Date != w.Date || !sameKey(misses[i].ReferenceKey, w.ReferenceKey) {
				t.Errorf("%s: miss %d is %+v, want %+v", tc.name, i, misses[i], w)
			}
		}
		if tc.wantAdded && (added[0].PlatformProfile != tc.wantProfiled || added[0].Date != "2026-10-23" || !sameKey(added[0].ReferenceKey, gateKey()) || !added[0].SuspendedTime.Equal(&countNow)) {
			t.Errorf("%s: the listing %+v, want the entry of %s dated 2026-10-23, suspended now", tc.name, added[0], tc.wantProfiled)
		}
	}
}

// The record and the listing are bounded by the CRD's 32: the 33rd drops the oldest, so the status write is never
// refused for its length.
func TestTheRecordAndTheListingAreBoundedBy32DroppingTheOldest(t *testing.T) {
	var misses []v1alpha1.ReferenceMiss
	var listed []v1alpha1.SuspendedReference
	for i := range 32 {
		k := gateKey()
		k.Sessions = int32(i + 1)
		at := metav1.NewTime(time.Date(2026, 11, 1, 0, i, 0, 0, time.UTC))
		misses = append(misses, v1alpha1.ReferenceMiss{PlatformProfile: profA, ReferenceKey: k, Date: "2026-10-23", InstanceUID: "i", AllocationUID: "a", Time: at})
		listed = append(listed, v1alpha1.SuspendedReference{PlatformProfile: profA, ReferenceKey: k, Date: "2026-10-23", SuspendedTime: at})
	}
	k := gateKey()
	k.Sessions = 33
	out, _, _, _ := countVerdict(misses, nil, entryRef{profile: profA, key: k, date: "2026-10-23"}, "Missed", inst("i33"), alloc("a33"), countNow)
	if len(out) != 32 || out[0].Sessions != 2 || out[31].Sessions != 33 {
		t.Fatalf("%d misses, the first with %d sessions and the last with %d; want 32, from 2 to 33", len(out), out[0].Sessions, out[len(out)-1].Sessions)
	}
	k.Sessions = 1
	_, got, added, _ := countVerdict([]v1alpha1.ReferenceMiss{{PlatformProfile: profB, ReferenceKey: k, Date: "2026-10-23", InstanceUID: "i1", AllocationUID: "a1", Time: countNow}},
		listed, entryRef{profile: profB, key: k, date: "2026-10-23"}, "Missed", inst("i2"), alloc("a2"), countNow)
	if len(got) != 32 || len(added) != 1 || got[31].PlatformProfile != profB || got[0].Sessions != 2 {
		t.Fatalf("%d listings and %d added, want 32 with profile B's listed last and the oldest (sessions 1) gone", len(got), len(added))
	}
}

// liftPass is a pass over a deployment whose Providers' profiles hold entries, for liftSuspensions.
func liftPass(s v1alpha1.ModelDeploymentStatus, profiles ...*v1alpha1.PlatformProfile) *pass {
	md := &v1alpha1.ModelDeployment{Status: s}
	p := &pass{now: countNow.Time, md: md}
	for _, prof := range profiles {
		p.providers = append(p.providers, &providerView{profile: prof})
	}
	return p
}

// named is a profile named prof holding the fixture entry with the given dates.
func named(prof string, dates ...string) *v1alpha1.PlatformProfile {
	var es []v1alpha1.ExpectedPerformance
	for _, d := range dates {
		e := gateFixtureEntry(gateKey())
		e.Date = d
		es = append(es, e)
	}
	pr := gateProfile(es...)
	pr.Name = prof
	return pr
}

// A listing, and a recorded miss, are lifted when their own profile, read by the pass, no longer holds the entry with
// that key and date: it is gone, or has another date. Another profile holding it keeps nothing. A profile the pass did
// not read keeps them. A listing that names no profile is lifted only when every profile was read and none holds it.
func TestSuspensionsAreLiftedOnlyWhatTheirProfileNoLongerHolds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profile  string
		profiles []*v1alpha1.PlatformProfile
		kept     bool
	}{
		{"its profile holds the entry with its date", profA, []*v1alpha1.PlatformProfile{named(profA, "2026-10-23")}, true},
		{"its profile's entry has another date", profA, []*v1alpha1.PlatformProfile{named(profA, "2026-10-24")}, false},
		{"its profile's entry is gone", profA, []*v1alpha1.PlatformProfile{named(profA)}, false},
		{"only another profile holds it", profA, []*v1alpha1.PlatformProfile{named(profA), named(profB, "2026-10-23")}, false},
		{"its profile was not read", profA, []*v1alpha1.PlatformProfile{named(profB)}, true},
		{"no profile named, one holds it", "", []*v1alpha1.PlatformProfile{named(profA), named(profB, "2026-10-23")}, true},
		{"no profile named, none holds it", "", []*v1alpha1.PlatformProfile{named(profA), named(profB)}, false},
		{"no profile named, one could not be read", "", []*v1alpha1.PlatformProfile{nil, named(profB)}, true},
	} {
		s := v1alpha1.ModelDeploymentStatus{
			SuspendedReferences: []v1alpha1.SuspendedReference{{PlatformProfile: tc.profile, ReferenceKey: gateKey(), Date: "2026-10-23", SuspendedTime: countNow}},
		}
		if tc.profile != "" {
			s.ReferenceMisses = []v1alpha1.ReferenceMiss{recorded(tc.profile, "i1", "a1")}
		}
		p := liftPass(s, tc.profiles...)
		out := p.md.Status.DeepCopy()
		p.liftSuspensions(out)
		if got := len(out.SuspendedReferences) == 1; got != tc.kept {
			t.Errorf("%s: listing kept %v, want %v", tc.name, got, tc.kept)
		}
		if tc.profile != "" {
			if got := len(out.ReferenceMisses) == 1; got != tc.kept {
				t.Errorf("%s: recorded miss kept %v, want %v", tc.name, got, tc.kept)
			}
		}
	}
}

package e2e

// Expected performance's API (beta; HBEP-4, Platform profile schema, Expected performance; HBEP-4's Test plan row
// "Expected performance (beta)"; #616 PR A): a PlatformProfile's spec.expectedPerformance, the reference
// measurements the throughput gate reads, and a ModelDeployment's status.suspendedReferences and status.referenceMisses
// (amendment 655), which hold the same key and name the profile it comes from.
// Every value is written out here and invented: none comes from a spike or a live run.

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The messages of the rules, as the API server returns them.
const (
	msgMaxNumSeqs = "capacityFlags always holds max-num-seqs"
	msgFlagName   = "a capacity flag's name is 1 to 64 characters"
	msgDate       = "date must be a real calendar date, YYYY-MM-DD"
	msgRefDate    = "referenceDate must be a real calendar date, YYYY-MM-DD"
	msgUnique     = "expectedPerformance holds one entry per deploymentID, capacityFlags and shape"
)

// flags builds a capacityFlags map from name, value pairs.
func flags(kv ...string) map[string]v1alpha1.FlagValue {
	m := map[string]v1alpha1.FlagValue{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = v1alpha1.FlagValue(kv[i+1])
	}
	return m
}

// repeat is a string of n copies of the letter x.
func repeat(n int) string { return strings.Repeat("x", n) }

// entry is a valid expected-performance entry: one worker of 8 sessions at 1,024 tokens of context and 128 of output,
// max-num-seqs 16, measured on 10 November 2026.
func entry() v1alpha1.ExpectedPerformance {
	return v1alpha1.ExpectedPerformance{
		ReferenceKey: v1alpha1.ReferenceKey{
			DeploymentID: "0123456789ab", CapacityFlags: flags("max-num-seqs", "16"),
			Sessions: 8, ContextTokens: 1024, OutputTokens: 128,
		},
		TimePerOutputToken: "20ms", TimeToFirstToken: "400ms",
		Method: "one run of one worker on one node of the example class",
		Source: "an invented reference run record",
		Date:   "2026-11-10",
	}
}

// withKey returns the entry with its key changed by set.
func withKey(set func(*v1alpha1.ReferenceKey)) v1alpha1.ExpectedPerformance {
	e := entry()
	set(&e.ReferenceKey)
	return e
}

// profileWith creates a profile that holds the entries, and deletes it when the test ends if it was created.
func profileWith(t *testing.T, entries ...v1alpha1.ExpectedPerformance) (*v1alpha1.PlatformProfile, error) {
	t.Helper()
	p := newProfile(uniq("ep"))
	p.Spec.ExpectedPerformance = entries
	err := c.Create(bg(), p)
	if err == nil {
		cleanup(t, p)
	}
	return p, err
}

// profileWithout creates a profile with one valid entry that lacks the JSON field named, as a client that leaves the
// field out would write it, and returns the error of the create.
func profileWithout(t *testing.T, field string) error {
	t.Helper()
	p := newProfile(uniq("ep"))
	p.Spec.ExpectedPerformance = []v1alpha1.ExpectedPerformance{entry()}
	p.APIVersion, p.Kind = v1alpha1.GroupVersion.String(), "PlatformProfile"
	raw, err := json.Marshal(p)
	must(t, err, "marshal")
	obj := map[string]any{}
	must(t, json.Unmarshal(raw, &obj), "unmarshal")
	first := obj["spec"].(map[string]any)["expectedPerformance"].([]any)[0].(map[string]any)
	if _, ok := first[field]; !ok {
		t.Fatalf("the entry has no %q to leave out: %v", field, first)
	}
	delete(first, field)
	u := &unstructured.Unstructured{Object: obj}
	err = c.Create(bg(), u)
	if err == nil {
		cleanup(t, u)
	}
	return err
}

// TestExpectedPerformanceRoundTripsAndDefaultsItsTolerance: an entry is kept as written, and a tolerance left out reads
// back 100, the API server's default (HBEP-4, Expected performance; HBEP-7, The throughput gate, The limits).
func TestExpectedPerformanceRoundTripsAndDefaultsItsTolerance(t *testing.T) {
	two := withKey(func(k *v1alpha1.ReferenceKey) {
		k.DeploymentID, k.CapacityFlags = "fedcba987654", flags("max-num-seqs", "32", "gpu-memory-utilization", "0.9")
		k.Sessions, k.ContextTokens, k.OutputTokens = 16, 2048, 256
	})
	two.TolerancePercent = ptr.To(int32(50))
	two.TimePerOutputToken, two.TimeToFirstToken = "31.5ms", "1.2s"
	p, err := profileWith(t, entry(), two)
	must(t, err, "create with two entries")
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	if len(got.Spec.ExpectedPerformance) != 2 {
		t.Fatalf("%d entries read back, want 2", len(got.Spec.ExpectedPerformance))
	}
	one, second := got.Spec.ExpectedPerformance[0], got.Spec.ExpectedPerformance[1]
	if one.DeploymentID != "0123456789ab" || len(one.CapacityFlags) != 1 || one.CapacityFlags["max-num-seqs"] != "16" ||
		one.Sessions != 8 || one.ContextTokens != 1024 || one.OutputTokens != 128 ||
		one.TimePerOutputToken != "20ms" || one.TimeToFirstToken != "400ms" || one.Date != "2026-11-10" ||
		one.Method == "" || one.Source == "" {
		t.Errorf("the first entry reads back as %+v", one)
	}
	if one.TolerancePercent == nil || *one.TolerancePercent != 100 {
		t.Errorf("tolerancePercent left out reads back as %v, want the default 100", one.TolerancePercent)
	}
	if second.DeploymentID != "fedcba987654" || len(second.CapacityFlags) != 2 || second.CapacityFlags["gpu-memory-utilization"] != "0.9" ||
		second.Sessions != 16 || second.ContextTokens != 2048 || second.OutputTokens != 256 ||
		second.TimePerOutputToken != "31.5ms" || second.TimeToFirstToken != "1.2s" {
		t.Errorf("the second entry reads back as %+v", second)
	}
	if second.TolerancePercent == nil || *second.TolerancePercent != 50 {
		t.Errorf("tolerancePercent 50 reads back as %v", second.TolerancePercent)
	}
	// An absent list is allowed: no shipped profile holds an entry yet.
	_, err = profileWith(t)
	must(t, err, "create with no entry")
}

// TestExpectedPerformanceBounds is HBEP-4's CRD row: 32 entries are accepted and 33 refused, 16 capacity flags and 17,
// names and values of 64 characters and 65, and each of the entry's own bounds on both sides. Every case changes one thing
// in a valid entry.
func TestExpectedPerformanceBounds(t *testing.T) {
	// n entries that differ in their sessions, so they hold n keys.
	manyEntries := func(n int) []v1alpha1.ExpectedPerformance {
		var es []v1alpha1.ExpectedPerformance
		for i := 1; i <= n; i++ {
			es = append(es, withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = int32(i) }))
		}
		return es
	}
	// n capacity flags: max-num-seqs and n-1 others.
	nFlags := func(n int) map[string]v1alpha1.FlagValue {
		m := flags("max-num-seqs", "16")
		for i := 1; i < n; i++ {
			m[fmt.Sprintf("flag-%02d", i)] = "1"
		}
		return m
	}
	tolerance := func(n int32) v1alpha1.ExpectedPerformance {
		e := entry()
		e.TolerancePercent = ptr.To(n)
		return e
	}
	cases := []struct {
		name    string
		entries []v1alpha1.ExpectedPerformance
		refuse  string // "" accepts; otherwise the text the refusal holds
	}{
		{"32 entries", manyEntries(32), ""},
		{"33 entries", manyEntries(33), "must have at most 32 items"},
		{"16 capacity flags", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = nFlags(16) })}, ""},
		{"17 capacity flags", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = nFlags(17) })}, "capacityFlags"},
		{"a 64-character flag name", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", repeat(64), "1")
		})}, ""},
		{"a 65-character flag name", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", repeat(65), "1")
		})}, msgFlagName},
		{"a 64-character flag value", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", repeat(64))
		})}, ""},
		{"a 65-character flag value", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", repeat(65))
		})}, "capacityFlags"},
		{"no capacity flag", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = map[string]v1alpha1.FlagValue{} })}, "capacityFlags"},
		{"tolerancePercent 5", []v1alpha1.ExpectedPerformance{tolerance(5)}, "tolerancePercent"},
		{"tolerancePercent 10", []v1alpha1.ExpectedPerformance{tolerance(10)}, ""},
		{"tolerancePercent 400", []v1alpha1.ExpectedPerformance{tolerance(400)}, ""},
		{"tolerancePercent 500", []v1alpha1.ExpectedPerformance{tolerance(500)}, "tolerancePercent"},
		{"outputTokens 1", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 1 })}, "outputTokens"},
		{"outputTokens 2", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 2 })}, ""},
		{"sessions 0", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = 0 })}, "sessions"},
		{"sessions 1024", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = 1024 })}, ""},
		{"sessions 1025", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = 1025 })}, "sessions"},
		{"contextTokens 0", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.ContextTokens = 0 })}, "contextTokens"},
		{"contextTokens 1", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.ContextTokens = 1 })}, ""},
		{"an 11-character deploymentID", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789a" })}, "deploymentID"},
		{"a 13-character deploymentID", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789abc" })}, "deploymentID"},
		{"a capital letter in the deploymentID", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789AB" })}, "deploymentID"},
		{"a deploymentID that is no hex", []v1alpha1.ExpectedPerformance{withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789ag" })}, "deploymentID"},
		{"an empty method", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Method = ""; return e }()}, "method"},
		{"an empty source", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Source = ""; return e }()}, "source"},
		{"an empty date", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Date = ""; return e }()}, "date"},
		{"a 1,024-character method", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Method = repeat(1024); return e }()}, ""},
		{"a 1,025-character method", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Method = repeat(1025); return e }()}, "method"},
		{"a 1,025-character source", []v1alpha1.ExpectedPerformance{func() v1alpha1.ExpectedPerformance { e := entry(); e.Source = repeat(1025); return e }()}, "source"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := profileWith(t, tc.entries...)
			if tc.refuse == "" {
				must(t, err, "create with "+tc.name)
				return
			}
			requireRefused(t, err, tc.refuse)
		})
	}
	// A client that leaves the field out of the entry altogether is refused too.
	for _, field := range []string{"method", "source", "date", "timePerOutputToken", "timeToFirstToken", "deploymentID", "capacityFlags", "sessions", "contextTokens", "outputTokens"} {
		t.Run("no "+field, func(t *testing.T) {
			requireRefused(t, profileWithout(t, field), field)
		})
	}
}

// TestCapacityFlagsAlwaysHoldMaxNumSeqs and TestCapacityFlagNamesAreOneTo64Characters are the two rules of the key that
// an expected-performance entry, a suspended reference and a reference miss share, each tried at all three places.
func TestCapacityFlagsAlwaysHoldMaxNumSeqs(t *testing.T) {
	for _, site := range keySites() {
		t.Run(site.name, func(t *testing.T) {
			requireRefused(t, site.try(t, withKey(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = flags("gpu-memory-utilization", "0.9") }).ReferenceKey), msgMaxNumSeqs)
			requireRefused(t, site.try(t, withKey(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = flags("max-num-seq", "16", "gpu-memory-utilization", "0.9")
			}).ReferenceKey), msgMaxNumSeqs)
			must(t, site.try(t, withKey(func(k *v1alpha1.ReferenceKey) {
				k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
			}).ReferenceKey), "max-num-seqs with another flag")
		})
	}
}

func TestCapacityFlagNamesAreOneTo64Characters(t *testing.T) {
	for _, site := range keySites() {
		t.Run(site.name, func(t *testing.T) {
			named := func(name string) v1alpha1.ReferenceKey {
				return withKey(func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = flags("max-num-seqs", "16", name, "1") }).ReferenceKey
			}
			requireRefused(t, site.try(t, named("")), msgFlagName)
			requireRefused(t, site.try(t, named(repeat(65))), msgFlagName)
			must(t, site.try(t, named("x")), "a 1-character flag name")
			must(t, site.try(t, named(repeat(64))), "a 64-character flag name")
		})
	}
}

// keySite is a place that holds a ReferenceKey: try writes one there, with the rest of the object valid, and returns the
// error of the write.
type keySite struct {
	name string
	try  func(t *testing.T, k v1alpha1.ReferenceKey) error
}

func keySites() []keySite {
	return []keySite{
		{"a PlatformProfile's expectedPerformance entry", func(t *testing.T, k v1alpha1.ReferenceKey) error {
			e := entry()
			e.ReferenceKey = k
			_, err := profileWith(t, e)
			return err
		}},
		{"a ModelDeployment's suspended reference", func(t *testing.T, k v1alpha1.ReferenceKey) error {
			return writeSuspended(t, suspended(k, "2026-11-10"))
		}},
		{"a ModelDeployment's reference miss", func(t *testing.T, k v1alpha1.ReferenceKey) error {
			return writeReferenceMisses(t, referenceMiss(k, "2026-11-10"))
		}},
	}
}

// TestExpectedPerformanceKeyIsUnique: no two entries of a profile share a deploymentID, capacity flags (as a map, by
// equality) and shape; two that differ in any one of them are two keys (rule platformprofiles:$.spec#2).
func TestExpectedPerformanceKeyIsUnique(t *testing.T) {
	other := func(set func(*v1alpha1.ReferenceKey)) v1alpha1.ExpectedPerformance { return withKey(set) }
	t.Run("two entries with one key", func(t *testing.T) {
		second := entry()
		second.TimePerOutputToken, second.Date = "25ms", "2026-11-12"
		_, err := profileWith(t, entry(), second)
		requireRefused(t, err, msgUnique)
	})
	t.Run("two entries with one key, among others", func(t *testing.T) {
		unrelated := other(func(k *v1alpha1.ReferenceKey) { k.Sessions = 3 })
		last := other(func(k *v1alpha1.ReferenceKey) { k.Sessions = 5 })
		_, err := profileWith(t, entry(), unrelated, last, other(func(k *v1alpha1.ReferenceKey) { k.Sessions = 5 }))
		requireRefused(t, err, msgUnique)
	})
	t.Run("one key written twice with its flags in another order", func(t *testing.T) {
		a := other(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
		})
		b := other(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("gpu-memory-utilization", "0.9", "max-num-seqs", "16")
		})
		_, err := profileWith(t, a, b)
		requireRefused(t, err, msgUnique)
	})
	differing := []struct {
		name string
		set  func(*v1alpha1.ReferenceKey)
	}{
		{"the value of max-num-seqs", func(k *v1alpha1.ReferenceKey) { k.CapacityFlags = flags("max-num-seqs", "32") }},
		{"an extra capacity flag", func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
		}},
		{"the deploymentID", func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789ac" }},
		{"the sessions", func(k *v1alpha1.ReferenceKey) { k.Sessions = 9 }},
		{"the contextTokens", func(k *v1alpha1.ReferenceKey) { k.ContextTokens = 2048 }},
		{"the outputTokens", func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 256 }},
	}
	for _, tc := range differing {
		t.Run("two entries that differ only in "+tc.name, func(t *testing.T) {
			_, err := profileWith(t, entry(), other(tc.set))
			must(t, err, "create two entries that differ only in "+tc.name)
		})
	}
	t.Run("two entries whose flags differ only in a value", func(t *testing.T) {
		a := other(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
		})
		b := other(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.85")
		})
		_, err := profileWith(t, a, b)
		must(t, err, "create two entries whose flags differ only in a value")
	})
	t.Run("32 entries of 32 keys", func(t *testing.T) {
		var es []v1alpha1.ExpectedPerformance
		for i := 1; i <= 32; i++ {
			es = append(es, other(func(k *v1alpha1.ReferenceKey) { k.Sessions = int32(i) }))
		}
		_, err := profileWith(t, es...)
		must(t, err, "create 32 distinct keys")
	})
	t.Run("32 entries whose last repeats the first", func(t *testing.T) {
		var es []v1alpha1.ExpectedPerformance
		for i := 1; i <= 31; i++ {
			es = append(es, other(func(k *v1alpha1.ReferenceKey) { k.Sessions = int32(i) }))
		}
		es = append(es, es[0])
		_, err := profileWith(t, es...)
		requireRefused(t, err, msgUnique)
	})
}

// dateSite is a place that holds a calendar date: try writes one there, with the rest of the object valid, and returns the
// error of the write. Every date the API holds (an entry's date, a suspended reference's date, a reference miss's date, a
// gate verdict's referenceDate on an Instance and on a start-up outcome) matches YYYY-MM-DD and must also be a real
// calendar date, which a pattern cannot say (rules ...[].date#0, ...suspendedReferences[].date#0,
// ...referenceMisses[].date#0 and the two ...referenceDate#0).
type dateSite struct {
	name     string
	field    string // the field's JSON name, which the refusal names
	msg      string // its rule's message
	optional bool   // the field is omitted when empty, so "" is no value to refuse
	try      func(t *testing.T, date string) error
}

func dateSites() []dateSite {
	return []dateSite{
		{"an entry's date", "date", msgDate, false, func(t *testing.T, date string) error {
			e := entry()
			e.Date = date
			_, err := profileWith(t, e)
			return err
		}},
		{"a suspended reference's date", "date", msgDate, false, func(t *testing.T, date string) error {
			return writeSuspended(t, suspended(entry().ReferenceKey, date))
		}},
		{"a reference miss's date", "date", msgDate, false, func(t *testing.T, date string) error {
			return writeReferenceMisses(t, referenceMiss(entry().ReferenceKey, date))
		}},
		{"an Instance gate's referenceDate", "referenceDate", msgRefDate, true, func(t *testing.T, date string) error {
			return writeInstanceGate(t, &v1alpha1.InstanceGate{Verdict: v1alpha1.GatePassed, ReferenceDate: date})
		}},
		{"a start-up outcome's referenceDate", "referenceDate", msgRefDate, true, func(t *testing.T, date string) error {
			o := outcome("uid-1", "Ready", "")
			o.Gate, o.ReferenceDate = v1alpha1.GatePassed, date
			_, err := withStartOutcomes(t, newNamespace(t), uniq("refdate"), classOutcomes(0, 1, o))
			return err
		}},
	}
}

func TestDatesAreRealCalendarDates(t *testing.T) {
	for _, site := range dateSites() {
		t.Run(site.name, func(t *testing.T) {
			// The pattern passes each of these; only the calendar refuses them.
			for _, date := range []string{"2026-02-30", "2026-02-29", "2026-04-31", "2026-13-01", "2026-00-10", "2026-01-00", "2026-01-32"} {
				t.Run("refuse "+date, func(t *testing.T) {
					requireRefused(t, site.try(t, date), site.msg)
				})
			}
			for _, date := range []string{"2026-02-28", "2024-02-29", "2026-12-31", "2026-01-01"} {
				t.Run("accept "+date, func(t *testing.T) {
					must(t, site.try(t, date), "write "+date)
				})
			}
			// The pattern and the length refuse these before the calendar is asked.
			shapes := []string{"2026-2-28", "26-02-28", "2026/02/28", "2026-02-28T00:00:00Z", "20260228"}
			if !site.optional {
				shapes = append(shapes, "")
			}
			for _, date := range shapes {
				t.Run("refuse the shape "+date, func(t *testing.T) {
					requireRefused(t, site.try(t, date), site.field)
				})
			}
		})
	}
}

// suspended is a suspended reference with a key and a date, suspended at a fixed time.
func suspended(k v1alpha1.ReferenceKey, date string) v1alpha1.SuspendedReference {
	return v1alpha1.SuspendedReference{ReferenceKey: k, Date: date,
		SuspendedTime: metav1.NewTime(time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC))}
}

// writeSuspended writes the suspended references to a fresh deployment's status and returns the error of that write.
func writeSuspended(t *testing.T, refs ...v1alpha1.SuspendedReference) error {
	t.Helper()
	md := newModelDeployment(newNamespace(t), uniq("suspended"))
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Status.SuspendedReferences = refs
	return c.Status().Update(bg(), got)
}

// referenceMiss is a reference miss of one profile's entry, with a key and a date, recorded at a fixed time from an
// Instance and an Allocation named by invented UIDs.
func referenceMiss(k v1alpha1.ReferenceKey, date string) v1alpha1.ReferenceMiss {
	return v1alpha1.ReferenceMiss{PlatformProfile: "example-profile", ReferenceKey: k, Date: date,
		InstanceUID: "uid-1", AllocationUID: "uid-2",
		Time: metav1.NewTime(time.Date(2026, 11, 15, 9, 30, 0, 0, time.UTC))}
}

// writeReferenceMisses writes the reference misses to a fresh deployment's status and returns the error of that write.
func writeReferenceMisses(t *testing.T, misses ...v1alpha1.ReferenceMiss) error {
	t.Helper()
	md := newModelDeployment(newNamespace(t), uniq("refmiss"))
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Status.ReferenceMisses = misses
	return c.Status().Update(bg(), got)
}

// writeReferenceMissWithout writes one reference miss to a fresh deployment's status with the JSON field named left
// out of it, as a client that omits the field would write it, and returns the error of that write. The typed client
// writes "" for a string left empty, which is a value, so it cannot show a field's absence.
func writeReferenceMissWithout(t *testing.T, miss v1alpha1.ReferenceMiss, field string) error {
	t.Helper()
	md := newModelDeployment(newNamespace(t), uniq("without"))
	must(t, c.Create(bg(), md), "create")
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("ModelDeployment"))
	must(t, c.Get(bg(), objKey(md), u), "get")
	raw, err := json.Marshal(miss)
	must(t, err, "marshal the miss")
	m := map[string]any{}
	must(t, json.Unmarshal(raw, &m), "unmarshal the miss")
	if _, ok := m[field]; !ok {
		t.Fatalf("the miss has no %q to leave out: %v", field, m)
	}
	delete(m, field)
	must(t, unstructured.SetNestedSlice(u.Object, []any{m}, "status", "referenceMisses"), "set status.referenceMisses")
	return c.Status().Update(bg(), u)
}

// TestSuspendedReferencesRoundTripAndAreBounded: a suspended reference is kept as written, 32 are accepted and 33
// refused, and it needs its date and its time.
func TestSuspendedReferencesRoundTripAndAreBounded(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "suspended")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Status.SuspendedReferences = []v1alpha1.SuspendedReference{
		suspended(withKey(func(k *v1alpha1.ReferenceKey) {
			k.CapacityFlags = flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9")
		}).ReferenceKey, "2026-11-10")}
	must(t, c.Status().Update(bg(), got), "write one suspended reference")
	back := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), back), "get")
	if len(back.Status.SuspendedReferences) != 1 {
		t.Fatalf("%d suspended references read back, want 1", len(back.Status.SuspendedReferences))
	}
	r := back.Status.SuspendedReferences[0]
	if r.DeploymentID != "0123456789ab" || r.CapacityFlags["gpu-memory-utilization"] != "0.9" || r.Sessions != 8 || r.ContextTokens != 1024 ||
		r.OutputTokens != 128 || r.Date != "2026-11-10" || !r.SuspendedTime.Equal(&got.Status.SuspendedReferences[0].SuspendedTime) {
		t.Errorf("read back %+v", r)
	}

	many := func(n int) []v1alpha1.SuspendedReference {
		var refs []v1alpha1.SuspendedReference
		for i := 1; i <= n; i++ {
			refs = append(refs, suspended(withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = int32(i) }).ReferenceKey, "2026-11-10"))
		}
		return refs
	}
	must(t, writeSuspended(t, many(32)...), "write 32 suspended references")
	requireRefused(t, writeSuspended(t, many(33)...), "must have at most 32 items")
	// The shared key's own bounds hold here too.
	requireRefused(t, writeSuspended(t, suspended(withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789a" }).ReferenceKey, "2026-11-10")), "deploymentID")
	requireRefused(t, writeSuspended(t, suspended(withKey(func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 1 }).ReferenceKey, "2026-11-10")), "outputTokens")
	requireRefused(t, writeSuspended(t, suspended(entry().ReferenceKey, "")), "date")
	noTime := suspended(entry().ReferenceKey, "2026-11-10")
	noTime.SuspendedTime = metav1.Time{}
	requireRefused(t, writeSuspended(t, noTime), "suspendedTime")
}

// TestSuspendedReferenceNamesItsProfile: a suspended reference's platformProfile is kept when set, is left out of the
// object when unset (a listing written before amendment 655 names no profile), and is at most 253 characters.
func TestSuspendedReferenceNamesItsProfile(t *testing.T) {
	named := func(profile string) v1alpha1.SuspendedReference {
		r := suspended(entry().ReferenceKey, "2026-11-10")
		r.PlatformProfile = profile
		return r
	}
	// stored writes one suspended reference and returns it as the API server holds it, with only the fields it has.
	stored := func(t *testing.T, r v1alpha1.SuspendedReference) map[string]any {
		t.Helper()
		md := newModelDeployment(newNamespace(t), uniq("suspended"))
		must(t, c.Create(bg(), md), "create")
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		got.Status.SuspendedReferences = []v1alpha1.SuspendedReference{r}
		must(t, c.Status().Update(bg(), got), "write one suspended reference")
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("ModelDeployment"))
		must(t, c.Get(bg(), objKey(md), u), "get")
		items, _, err := unstructured.NestedSlice(u.Object, "status", "suspendedReferences")
		must(t, err, "read status.suspendedReferences")
		if len(items) != 1 {
			t.Fatalf("%d suspended references read back, want 1", len(items))
		}
		return items[0].(map[string]any)
	}
	t.Run("set", func(t *testing.T) {
		if got := stored(t, named("example-profile"))["platformProfile"]; got != "example-profile" {
			t.Errorf("platformProfile reads back %v, want example-profile", got)
		}
	})
	t.Run("unset", func(t *testing.T) {
		if got, present := stored(t, named(""))["platformProfile"]; present {
			t.Errorf("platformProfile reads back %q, want it absent", got)
		}
	})
	t.Run("253 characters", func(t *testing.T) {
		must(t, writeSuspended(t, named(repeat(253))), "write a 253-character platformProfile")
	})
	t.Run("254 characters", func(t *testing.T) {
		requireRefused(t, writeSuspended(t, named(repeat(254))), "platformProfile")
	})
}

// TestReferenceMissesRoundTripAndAreBounded (amendment 655): a reference miss is kept as written, 32 are accepted and 33
// refused, and each of its fields is needed and bounded: the profile's name is 1 to 253 characters, each UID 1 to 64.
func TestReferenceMissesRoundTripAndAreBounded(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "refmiss")
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Status.ReferenceMisses = []v1alpha1.ReferenceMiss{{
		PlatformProfile: "example-profile",
		ReferenceKey: v1alpha1.ReferenceKey{
			DeploymentID: "0123456789ab", CapacityFlags: flags("max-num-seqs", "16", "gpu-memory-utilization", "0.9"),
			Sessions: 8, ContextTokens: 1024, OutputTokens: 128,
		},
		Date: "2026-11-10", InstanceUID: "uid-1", AllocationUID: "uid-2",
		Time: metav1.NewTime(time.Date(2026, 11, 15, 9, 30, 0, 0, time.UTC)),
	}}
	must(t, c.Status().Update(bg(), got), "write one reference miss")
	back := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), back), "get")
	if len(back.Status.ReferenceMisses) != 1 {
		t.Fatalf("%d reference misses read back, want 1", len(back.Status.ReferenceMisses))
	}
	r := back.Status.ReferenceMisses[0]
	for _, f := range []struct {
		name      string
		got, want any
	}{
		{"platformProfile", r.PlatformProfile, "example-profile"},
		{"deploymentID", r.DeploymentID, "0123456789ab"},
		{"capacityFlags", fmt.Sprint(r.CapacityFlags), "map[gpu-memory-utilization:0.9 max-num-seqs:16]"},
		{"sessions", r.Sessions, int32(8)},
		{"contextTokens", r.ContextTokens, int32(1024)},
		{"outputTokens", r.OutputTokens, int32(128)},
		{"date", r.Date, "2026-11-10"},
		{"instanceUID", string(r.InstanceUID), "uid-1"},
		{"allocationUID", string(r.AllocationUID), "uid-2"},
		{"time", r.Time.UTC().Format(time.RFC3339), "2026-11-15T09:30:00Z"},
	} {
		if f.got != f.want {
			t.Errorf("%s reads back %v, want %v", f.name, f.got, f.want)
		}
	}

	many := func(n int) []v1alpha1.ReferenceMiss {
		var misses []v1alpha1.ReferenceMiss
		for i := 1; i <= n; i++ {
			misses = append(misses, referenceMiss(withKey(func(k *v1alpha1.ReferenceKey) { k.Sessions = int32(i) }).ReferenceKey, "2026-11-10"))
		}
		return misses
	}
	must(t, writeReferenceMisses(t, many(32)...), "write 32 reference misses")
	requireRefused(t, writeReferenceMisses(t, many(33)...), "must have at most 32 items")

	// with changes a valid miss: one field is set to a value the rule behind it refuses.
	with := func(set func(*v1alpha1.ReferenceMiss)) v1alpha1.ReferenceMiss {
		m := referenceMiss(entry().ReferenceKey, "2026-11-10")
		set(&m)
		return m
	}
	// The shared key's own bounds hold here too.
	requireRefused(t, writeReferenceMisses(t, referenceMiss(withKey(func(k *v1alpha1.ReferenceKey) { k.DeploymentID = "0123456789a" }).ReferenceKey, "2026-11-10")), "deploymentID")
	requireRefused(t, writeReferenceMisses(t, referenceMiss(withKey(func(k *v1alpha1.ReferenceKey) { k.OutputTokens = 1 }).ReferenceKey, "2026-11-10")), "outputTokens")

	t.Run("the profile's name is 1 to 253 characters", func(t *testing.T) {
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.PlatformProfile = "" })), "platformProfile")
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.PlatformProfile = repeat(254) })), "platformProfile")
		must(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.PlatformProfile = "x" })), "write a 1-character platformProfile")
		must(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.PlatformProfile = repeat(253) })), "write a 253-character platformProfile")
	})
	t.Run("each UID is 1 to 64 characters", func(t *testing.T) {
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.InstanceUID = "" })), "instanceUID")
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.AllocationUID = "" })), "allocationUID")
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.InstanceUID = types.UID(repeat(65)) })), "instanceUID")
		requireRefused(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.AllocationUID = types.UID(repeat(65)) })), "allocationUID")
		must(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) {
			m.InstanceUID, m.AllocationUID = types.UID(repeat(64)), types.UID(repeat(64))
		})), "write two 64-character UIDs")
		must(t, writeReferenceMisses(t, with(func(m *v1alpha1.ReferenceMiss) { m.InstanceUID, m.AllocationUID = "u", "v" })), "write two 1-character UIDs")
	})
	t.Run("a field left out is refused", func(t *testing.T) {
		for _, field := range []string{"platformProfile", "date", "instanceUID", "allocationUID", "time"} {
			t.Run(field, func(t *testing.T) {
				requireRefused(t, writeReferenceMissWithout(t, referenceMiss(entry().ReferenceKey, "2026-11-10"), field), field)
			})
		}
	})
}

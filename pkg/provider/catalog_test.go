package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// MergeCatalog's tests. Managed IDs are expected as the SHA-256 of HBEP-3's
// canonical form written out by hand, never from deploymentid.

var t0 = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func ce(name string) CatalogEntry { return CatalogEntry{Name: name} }

func ceRev(name, rev string) CatalogEntry { return CatalogEntry{Name: name, Revision: &rev} }

// handID is the managed ID of HBEP-3's canonical form, written by hand.
func handID(platform, name, revision string) v1alpha1.FullDeploymentID {
	s := `{"catalog_name":"` + name + `","kind":"managed","opaque":"true","platform":"` + platform + `"`
	if revision != "" {
		s += `,"revision":"` + revision + `"`
	}
	s += `,"scheme":"v1"}`
	h := sha256.Sum256([]byte(s))
	return v1alpha1.FullDeploymentID(hex.EncodeToString(h[:]))
}

func mtp(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

func names(cat []v1alpha1.CatalogEntryStatus) []string {
	out := make([]string, len(cat))
	for i, e := range cat {
		out[i] = e.Name
	}
	return out
}

func find(t *testing.T, cat []v1alpha1.CatalogEntryStatus, name string) v1alpha1.CatalogEntryStatus {
	t.Helper()
	for _, e := range cat {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("no entry %q in %v", name, names(cat))
	return v1alpha1.CatalogEntryStatus{}
}

func mustMerge(t *testing.T, old []v1alpha1.CatalogEntryStatus, listed []CatalogEntry, platform string, now time.Time) CatalogMerge {
	t.Helper()
	m, err := MergeCatalog(old, listed, platform, now)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMergeCatalogAddRemoveReturn(t *testing.T) {
	m := mustMerge(t, nil, []CatalogEntry{ce("example-b"), ceRev("example-a", "r1")}, "llmlab", t0)
	if got := strings.Join(names(m.Catalog), ","); got != "example-a,example-b" {
		t.Fatalf("catalog = %s, want example-a,example-b sorted by name", got)
	}
	a := find(t, m.Catalog, "example-a")
	if !a.Listed || a.Revision == nil || *a.Revision != "r1" || a.ManagedID != handID("llmlab", "example-a", "r1") || !a.LastListedTime.Time.Equal(t0) {
		t.Fatalf("example-a = %+v", a)
	}
	if b := find(t, m.Catalog, "example-b"); b.Revision != nil || b.ManagedID != handID("llmlab", "example-b", "") {
		t.Fatalf("example-b = %+v, want no revision and the ID without one", b)
	}

	// example-a leaves: kept, unlisted, as it was.
	m2 := mustMerge(t, m.Catalog, []CatalogEntry{ce("example-b")}, "llmlab", t0.Add(time.Minute))
	a2 := find(t, m2.Catalog, "example-a")
	if a2.Listed || !a2.LastListedTime.Time.Equal(t0) || *a2.Revision != "r1" || a2.ManagedID != a.ManagedID {
		t.Fatalf("example-a after leaving = %+v, want unlisted, last listed at t0, r1", a2)
	}
	// It returns with another revision.
	m3 := mustMerge(t, m2.Catalog, []CatalogEntry{ceRev("example-a", "r2")}, "llmlab", t0.Add(2*time.Minute))
	a3 := find(t, m3.Catalog, "example-a")
	if !a3.Listed || *a3.Revision != "r2" || a3.ManagedID != handID("llmlab", "example-a", "r2") || !a3.LastListedTime.Time.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("example-a back = %+v", a3)
	}
	if b := find(t, m3.Catalog, "example-b"); b.Listed {
		t.Fatal("example-b still listed after it left")
	}
	if len(m.Skipped)+len(m2.Skipped)+len(m3.Skipped)+len(m3.Pruned)+m3.Truncated != 0 {
		t.Fatal("a plain listing skipped, pruned or truncated something")
	}
}

func TestMergeCatalogNeverChangesOld(t *testing.T) {
	old := mustMerge(t, nil, []CatalogEntry{ceRev("example-a", "r1"), ce("example-b")}, "llmlab", t0).Catalog
	before := make([]v1alpha1.CatalogEntryStatus, len(old))
	for i := range old {
		before[i] = *old[i].DeepCopy()
	}
	mustMerge(t, old, []CatalogEntry{ce("example-c")}, "other-platform", t0.Add(time.Minute))
	if !apiequality.Semantic.DeepEqual(before, old) {
		t.Fatalf("MergeCatalog changed old:\n before %+v\n after  %+v", before, old)
	}
}

func TestMergeCatalogSkipsWhatTheStatusCannotHold(t *testing.T) {
	listed := []CatalogEntry{
		ce("example-ok"),
		ce(""),
		ce(strings.Repeat("n", 254)),
		ce(strings.Repeat("ü", 253)), // 253 characters, 506 bytes: holdable
		ceRev("example-longrev", strings.Repeat("r", 257)),
		ce("example-\xff"),
	}
	m := mustMerge(t, nil, listed, "llmlab", t0)
	if got := len(m.Catalog); got != 2 {
		t.Fatalf("catalog = %v, want example-ok and the 253-character name", names(m.Catalog))
	}
	find(t, m.Catalog, "example-ok")
	find(t, m.Catalog, strings.Repeat("ü", 253))
	// The words say what is wrong with the measured value; the category is
	// the stable kind of failure that an event is keyed on.
	want := map[string]struct{ why, category string }{
		"":                       {"empty", "EmptyName"},
		strings.Repeat("n", 254): {"254 characters", "NameTooLong"},
		"example-longrev":        {"257 characters", "RevisionTooLong"},
		"example-\xff":           {"UTF-8", "InvalidName"},
	}
	if len(m.Skipped) != len(want) {
		t.Fatalf("skipped = %+v, want %d entries", m.Skipped, len(want))
	}
	for _, s := range m.Skipped {
		if w, ok := want[s.Name]; !ok || !strings.Contains(s.Why, w.why) || string(s.Category) != w.category {
			t.Fatalf("skipped %q as %q because %q, want category %q and a reason holding %q", s.Name, s.Category, s.Why, w.category, w.why)
		}
	}
	// A longer revision is the same category with other words.
	for _, n := range []int{257, 300} {
		m := mustMerge(t, nil, []CatalogEntry{ceRev("example-longrev", strings.Repeat("r", n))}, "llmlab", t0)
		if len(m.Skipped) != 1 || m.Skipped[0].Category != "RevisionTooLong" || !strings.Contains(m.Skipped[0].Why, fmt.Sprintf("%d characters", n)) {
			t.Fatalf("a revision of %d characters: skipped = %+v, want RevisionTooLong naming %d", n, m.Skipped, n)
		}
	}
}

func TestMergeCatalogDuplicates(t *testing.T) {
	// The same entry twice is one entry.
	m := mustMerge(t, nil, []CatalogEntry{ceRev("example-a", "r1"), ceRev("example-a", "r1")}, "llmlab", t0)
	if len(m.Catalog) != 1 || len(m.Skipped) != 0 {
		t.Fatalf("an exact duplicate gave %v, skipped %+v, want one entry", names(m.Catalog), m.Skipped)
	}
	// Two revisions of one name are ambiguous, in either order and however
	// often: the name is skipped once, so the ID never depends on the order.
	for _, order := range [][]CatalogEntry{
		{ceRev("example-a", "r1"), ceRev("example-a", "r2"), ceRev("example-a", "r1")},
		{ceRev("example-a", "r2"), ceRev("example-a", "r1")},
		{ce("example-a"), ceRev("example-a", "r1")},
	} {
		m := mustMerge(t, nil, append(order, ce("example-b")), "llmlab", t0)
		if got := strings.Join(names(m.Catalog), ","); got != "example-b" {
			t.Fatalf("catalog = %s, want example-b alone", got)
		}
		if len(m.Skipped) != 1 || m.Skipped[0].Name != "example-a" || m.Skipped[0].Category != "AmbiguousRevision" || !strings.Contains(m.Skipped[0].Why, "twice") {
			t.Fatalf("skipped = %+v, want example-a once", m.Skipped)
		}
	}
	// An ambiguous name that was listed before stays, unlisted.
	old := mustMerge(t, nil, []CatalogEntry{ceRev("example-a", "r1")}, "llmlab", t0).Catalog
	m = mustMerge(t, old, []CatalogEntry{ceRev("example-a", "r1"), ceRev("example-a", "r2")}, "llmlab", t0.Add(time.Minute))
	if a := find(t, m.Catalog, "example-a"); a.Listed || *a.Revision != "r1" {
		t.Fatalf("example-a = %+v, want it kept unlisted with r1", a)
	}
}

func TestMergeCatalogRecomputesUnlistedIDsOverTheCurrentPlatform(t *testing.T) {
	old := mustMerge(t, nil, []CatalogEntry{ceRev("example-a", "r1"), ce("example-b")}, "llmlab", t0).Catalog
	m := mustMerge(t, old, []CatalogEntry{ce("example-b")}, "example-other", t0.Add(time.Minute))
	if a := find(t, m.Catalog, "example-a"); a.Listed || a.ManagedID != handID("example-other", "example-a", "r1") {
		t.Fatalf("unlisted example-a ID = %s, want it over example-other", a.ManagedID)
	}
	if b := find(t, m.Catalog, "example-b"); b.ManagedID != handID("example-other", "example-b", "") {
		t.Fatalf("listed example-b ID = %s, want it over example-other", b.ManagedID)
	}
}

// numbered returns n entries example-0000, example-0001, ... from first.
func numbered(first, n int) []CatalogEntry {
	out := make([]CatalogEntry, n)
	for i := range out {
		out[i] = ce(fmt.Sprintf("example-%04d", first+i))
	}
	return out
}

func TestMergeCatalogPrunesUnlistedOldestFirst(t *testing.T) {
	// 1024 entries, example-0000 listed longest ago, one more second each.
	var old []v1alpha1.CatalogEntryStatus
	for i := range MaxCatalogEntries {
		old = append(old, v1alpha1.CatalogEntryStatus{
			Name: fmt.Sprintf("example-%04d", i), ManagedID: handID("llmlab", fmt.Sprintf("example-%04d", i), ""),
			Listed: true, LastListedTime: mtp(t0.Add(time.Duration(i) * time.Second)),
		})
	}
	// example-0010 to 1023 stay listed and five new names arrive: 1019
	// listed, room for five of the ten that left, so the five listed longest
	// ago go.
	listed := append(numbered(10, MaxCatalogEntries-10), ce("new-0"), ce("new-1"), ce("new-2"), ce("new-3"), ce("new-4"))
	m := mustMerge(t, old, listed, "llmlab", t0.Add(time.Hour))
	if len(m.Catalog) != MaxCatalogEntries {
		t.Fatalf("catalog holds %d, want %d", len(m.Catalog), MaxCatalogEntries)
	}
	if got := strings.Join(m.Pruned, ","); got != "example-0000,example-0001,example-0002,example-0003,example-0004" {
		t.Fatalf("pruned %s, want the five listed longest ago", got)
	}
	for i := 5; i < 10; i++ {
		if e := find(t, m.Catalog, fmt.Sprintf("example-%04d", i)); e.Listed {
			t.Fatalf("%s listed", e.Name)
		}
	}
	find(t, m.Catalog, "new-4")
	if m.Truncated != 0 || len(m.Skipped) != 0 {
		t.Fatalf("truncated %d, skipped %+v, want none", m.Truncated, m.Skipped)
	}
}

func TestMergeCatalogNoLastListedTimeIsOldest(t *testing.T) {
	old := []v1alpha1.CatalogEntryStatus{
		{Name: "example-z", ManagedID: handID("llmlab", "example-z", ""), LastListedTime: nil},
		{Name: "example-y", ManagedID: handID("llmlab", "example-y", ""), LastListedTime: mtp(t0)},
	}
	listed := numbered(0, MaxCatalogEntries-1) // room for one
	m := mustMerge(t, old, listed, "llmlab", t0.Add(time.Hour))
	if got := strings.Join(m.Pruned, ","); got != "example-z" {
		t.Fatalf("pruned %s, want example-z, which has no lastListedTime", got)
	}
	find(t, m.Catalog, "example-y")
}

func TestMergeCatalogNeverPrunesAListedEntryForRoom(t *testing.T) {
	old := mustMerge(t, nil, []CatalogEntry{ce("old-a"), ce("old-b"), ce("old-c")}, "llmlab", t0).Catalog
	m := mustMerge(t, old, numbered(0, MaxCatalogEntries), "llmlab", t0.Add(time.Minute))
	if len(m.Catalog) != MaxCatalogEntries || len(m.Pruned) != 3 || m.Truncated != 0 {
		t.Fatalf("catalog %d, pruned %v, truncated %d, want 1024 listed, the 3 old pruned", len(m.Catalog), m.Pruned, m.Truncated)
	}
	for _, e := range m.Catalog {
		if !e.Listed {
			t.Fatalf("%s is unlisted, want every kept entry listed", e.Name)
		}
	}
}

func TestMergeCatalogTruncatesAListingAboveTheLimit(t *testing.T) {
	old := mustMerge(t, nil, []CatalogEntry{ce("old-a")}, "llmlab", t0).Catalog
	// 1030 names in reverse order: the first 1024 by name are kept.
	listed := numbered(0, MaxCatalogEntries+6)
	for i, k := 0, len(listed)-1; i < k; i, k = i+1, k-1 {
		listed[i], listed[k] = listed[k], listed[i]
	}
	m := mustMerge(t, old, listed, "llmlab", t0.Add(time.Minute))
	if len(m.Catalog) != MaxCatalogEntries || m.Truncated != 6 {
		t.Fatalf("catalog %d, truncated %d, want 1024 and 6", len(m.Catalog), m.Truncated)
	}
	if first, last := m.Catalog[0].Name, m.Catalog[len(m.Catalog)-1].Name; first != "example-0000" || last != "example-1023" {
		t.Fatalf("kept %s to %s, want example-0000 to example-1023", first, last)
	}
	if strings.Join(m.Pruned, ",") != "old-a" {
		t.Fatalf("pruned %v, want old-a: no room for an unlisted entry", m.Pruned)
	}
}

func TestMergeCatalogRefusesAnEmptyPlatform(t *testing.T) {
	if _, err := MergeCatalog(nil, []CatalogEntry{ce("example-a")}, "", t0); err == nil {
		t.Fatal("MergeCatalog with no platform = nil error")
	}
}

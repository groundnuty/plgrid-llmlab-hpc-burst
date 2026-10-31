package provider

import (
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// Limits of a managed Provider's status.catalog (api/v1alpha1,
// ProviderStatus.Catalog and CatalogEntryStatus). Lengths count characters,
// as the API server's maxLength does.
const (
	MaxCatalogEntries     = 1024
	MaxCatalogNameLen     = 253
	MaxCatalogRevisionLen = 256
)

// SkippedEntry is a listed entry the status cannot hold.
type SkippedEntry struct {
	// Name is the entry's name as listed.
	Name string
	// Why says what is wrong with it, without its value.
	Why string
}

// CatalogMerge is the status.catalog after one successful catalog read, and
// what the read could not keep.
type CatalogMerge struct {
	// Catalog is the new status.catalog, sorted by name, at most
	// MaxCatalogEntries long.
	Catalog []v1alpha1.CatalogEntryStatus
	// Skipped are the listed entries the status cannot hold: an empty or
	// invalid name, a name above MaxCatalogNameLen, a revision above
	// MaxCatalogRevisionLen, or a name listed twice with different revisions.
	// The Provider's controller raises an event naming each.
	Skipped []SkippedEntry
	// Pruned are the unlisted entries dropped to make room, oldest
	// lastListedTime first.
	Pruned []string
	// Truncated is how many holdable listed entries did not fit: the listing
	// alone held more than MaxCatalogEntries, and the first ones in name order
	// were kept. The controller raises CatalogTruncated while it is above 0:
	// when it first is, then at most once an hour (RepeatGate.CatalogEvents).
	Truncated int
}

// MergeCatalog merges one successful read, which listed listed, into old,
// the stored status.catalog, for a Provider on platform (HBEP-4, Managed
// provider in alpha; the bounds are @hpc-burst-science-agent's ruling of 31
// Oct 2026). It never changes old.
//
//   - Every listed entry the status can hold is listed true, with
//     lastListedTime now and its managed ID: deploymentid.ComputeManaged over
//     the platform, the name and the revision (absent when empty), in the
//     full 64-character form.
//   - An entry the status cannot hold is skipped (Skipped); the rest of the
//     read still counts.
//   - An old entry the read did not list stays, with listed false and its
//     revision and lastListedTime as they were; its managed ID is computed
//     again over the current platform, so the catalog never mixes platforms.
//   - The catalog holds at most MaxCatalogEntries. Room is made by pruning
//     unlisted entries, oldest lastListedTime first (an entry with none is the
//     oldest; ties go by name); a listed entry is never pruned for room. When
//     the listed entries alone are more than that, the first
//     MaxCatalogEntries in name order are kept (Truncated).
//
// The only error is a platform that deploymentid refuses.
func MergeCatalog(old []v1alpha1.CatalogEntryStatus, listed []CatalogEntry, platform string, now time.Time) (CatalogMerge, error) {
	if _, err := deploymentid.ComputeManaged(deploymentid.CatalogEntry{Platform: platform, CatalogName: "x"}); err != nil {
		return CatalogMerge{}, fmt.Errorf("platform: %w", err)
	}
	var out CatalogMerge
	listedNow := map[string]v1alpha1.CatalogEntryStatus{}
	ambiguous := map[string]bool{}
	for _, l := range listed {
		if ambiguous[l.Name] {
			continue
		}
		e, why := holdable(l, platform, now)
		if why != "" {
			out.Skipped = append(out.Skipped, SkippedEntry{Name: l.Name, Why: why})
			continue
		}
		if prev, seen := listedNow[l.Name]; seen {
			if !sameRevision(prev.Revision, e.Revision) {
				delete(listedNow, l.Name)
				ambiguous[l.Name] = true
				out.Skipped = append(out.Skipped, SkippedEntry{Name: l.Name, Why: "listed twice with different revisions"})
			}
			continue
		}
		listedNow[l.Name] = e
	}

	keep := make([]v1alpha1.CatalogEntryStatus, 0, len(listedNow))
	for _, e := range listedNow {
		keep = append(keep, e)
	}
	sortByName(keep)
	if len(keep) > MaxCatalogEntries {
		out.Truncated = len(keep) - MaxCatalogEntries
		keep = keep[:MaxCatalogEntries]
	}

	var unlisted []v1alpha1.CatalogEntryStatus
	for _, e := range old {
		if _, ok := listedNow[e.Name]; ok {
			continue
		}
		u := *e.DeepCopy()
		u.Listed = false
		in := deploymentid.CatalogEntry{Platform: platform, CatalogName: u.Name}
		if u.Revision != nil {
			in.Revision = *u.Revision
		}
		if id, err := deploymentid.ComputeManaged(in); err == nil {
			u.ManagedID = v1alpha1.FullDeploymentID(id.SHA256)
		}
		unlisted = append(unlisted, u)
	}
	if room := MaxCatalogEntries - len(keep); len(unlisted) > room {
		sort.SliceStable(unlisted, func(i, k int) bool { return olderListed(unlisted[i], unlisted[k]) })
		for _, p := range unlisted[:len(unlisted)-room] {
			out.Pruned = append(out.Pruned, p.Name)
		}
		unlisted = unlisted[len(unlisted)-room:]
	}
	out.Catalog = append(keep, unlisted...)
	sortByName(out.Catalog)
	return out, nil
}

// holdable returns the status of one listed entry, or why the status cannot
// hold it.
func holdable(l CatalogEntry, platform string, now time.Time) (v1alpha1.CatalogEntryStatus, string) {
	switch n := utf8.RuneCountInString(l.Name); {
	case l.Name == "":
		return v1alpha1.CatalogEntryStatus{}, "an empty name"
	case !utf8.ValidString(l.Name):
		return v1alpha1.CatalogEntryStatus{}, "a name that is not valid UTF-8"
	case n > MaxCatalogNameLen:
		return v1alpha1.CatalogEntryStatus{}, fmt.Sprintf("a name of %d characters, above %d", n, MaxCatalogNameLen)
	}
	in := deploymentid.CatalogEntry{Platform: platform, CatalogName: l.Name}
	var revision *string
	if l.Revision != nil && *l.Revision != "" {
		if n := utf8.RuneCountInString(*l.Revision); n > MaxCatalogRevisionLen {
			return v1alpha1.CatalogEntryStatus{}, fmt.Sprintf("a revision of %d characters, above %d", n, MaxCatalogRevisionLen)
		}
		r := *l.Revision
		revision, in.Revision = &r, r
	}
	id, err := deploymentid.ComputeManaged(in)
	if err != nil {
		return v1alpha1.CatalogEntryStatus{}, "a name or revision the deployment ID refuses"
	}
	t := metav1.NewTime(now)
	return v1alpha1.CatalogEntryStatus{
		Name: l.Name, Revision: revision, ManagedID: v1alpha1.FullDeploymentID(id.SHA256),
		Listed: true, LastListedTime: &t,
	}, ""
}

func sameRevision(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func sortByName(s []v1alpha1.CatalogEntryStatus) {
	sort.Slice(s, func(i, k int) bool { return s[i].Name < s[k].Name })
}

// olderListed orders unlisted entries for pruning: no lastListedTime first,
// then the oldest, then by name.
func olderListed(a, b v1alpha1.CatalogEntryStatus) bool {
	switch {
	case a.LastListedTime == nil && b.LastListedTime == nil:
		return a.Name < b.Name
	case a.LastListedTime == nil:
		return true
	case b.LastListedTime == nil:
		return false
	case !a.LastListedTime.Equal(b.LastListedTime):
		return a.LastListedTime.Before(b.LastListedTime)
	}
	return a.Name < b.Name
}

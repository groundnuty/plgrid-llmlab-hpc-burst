package managed

import (
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// mergeCatalog returns the catalog status after a successful read that
// listed listed: every old entry, marked unlisted with its revision, managed
// ID and lastListedTime as they were, then the listed ones on top, sorted by
// name (HBEP-4: "An entry that leaves the listing stays, marked unlisted").
// It never changes old. A listing the status cannot hold is an error.
func mergeCatalog(old []v1alpha1.CatalogEntryStatus, listed []provider.CatalogEntry, platform string, now time.Time) ([]v1alpha1.CatalogEntryStatus, error) {
	byName := make(map[string]v1alpha1.CatalogEntryStatus, len(old)+len(listed))
	for _, e := range old {
		c := *e.DeepCopy()
		c.Listed = false
		byName[e.Name] = c
	}
	for _, l := range listed {
		entry, err := catalogEntry(l, platform, now)
		if err != nil {
			return nil, err
		}
		byName[l.Name] = entry
	}
	if len(byName) > maxCatalogEntries {
		return nil, fmt.Errorf("the catalog would hold %d entries, above the %d status.catalog allows", len(byName), maxCatalogEntries)
	}
	out := make([]v1alpha1.CatalogEntryStatus, 0, len(byName))
	for _, e := range byName {
		out = append(out, e)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out, nil
}

// catalogEntry is the status of one listed entry: its managed ID is
// deploymentid.ComputeManaged over the platform, the name and the revision
// (HBEP-3, Deployment ID), in its full 64-character form. An empty revision
// is no revision. Lengths count characters, as the API server's maxLength
// does.
func catalogEntry(l provider.CatalogEntry, platform string, now time.Time) (v1alpha1.CatalogEntryStatus, error) {
	if n := utf8.RuneCountInString(l.Name); n > maxCatalogNameLen {
		return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry name has %d characters, above the %d status.catalog allows", n, maxCatalogNameLen)
	}
	in := deploymentid.CatalogEntry{Platform: platform, CatalogName: l.Name}
	var revision *string
	if l.Revision != nil && *l.Revision != "" {
		if n := utf8.RuneCountInString(*l.Revision); n > maxCatalogRevision {
			return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry %q: revision has %d characters, above the %d status.catalog allows", l.Name, n, maxCatalogRevision)
		}
		r := *l.Revision
		revision, in.Revision = &r, r
	}
	id, err := deploymentid.ComputeManaged(in)
	if err != nil {
		return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry %q: %w", l.Name, err)
	}
	t := metav1.NewTime(now)
	return v1alpha1.CatalogEntryStatus{
		Name: l.Name, Revision: revision, ManagedID: v1alpha1.FullDeploymentID(id.SHA256),
		Listed: true, LastListedTime: &t,
	}, nil
}

package fake

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ManagedController is a stand-in for one managed Provider's controller, for
// the lifecycle controller's tests of managed-source ModelDeployments (HBEP-4,
// Managed provider in alpha). It reads the catalog of a Managed through the
// provider.Managed interface and writes only the Provider's status, under
// v1alpha1.FieldManagerProvider. The Provider's name is Provider, and its label
// v1alpha1.LabelPlatform names the platform the managed IDs are computed over.
//
// Sync is one catalog read. It has no rate limit: the real controller reads at
// most once a minute, and a test decides how often to call Sync.
//
//   - A successful read puts one entry in status.catalog for every name ever
//     listed, sorted by name. An entry in the latest listing has listed true
//     and lastListedTime now; one that left the listing is kept with listed
//     false, its revision, managed ID and lastListedTime as they were; one that
//     comes back is listed again. The managed ID is deploymentid.ComputeManaged
//     over the platform, the name and the revision the entry has now, as the
//     full 64-character form. A revision that is absent or empty is absent.
//     lastCatalogRead and lastReconcileTime are now, the Ready condition is
//     True with reason CatalogRead, and CredentialsRejected is False.
//   - A key the gateway rejects (an error wrapping provider.ErrKeyRejected)
//     sets CredentialsRejected True and leaves the catalog, lastCatalogRead and
//     Ready as they were. lastReconcileTime is now.
//   - Any other error is unknown: Sync writes nothing at all, and returns nil,
//     as the batch Controller does for a failed read.
//   - The key's expiry (Managed.SetKeyExpiry), when set, makes the condition
//     CredentialsExpiring True with reason ExpiresWithinADay from a day before
//     the expiry, and Expired from the expiry on; before that it is False with
//     reason NotExpiring. It is evaluated on the passes that write. The real
//     managed controller (#69) reads the key's expiry from the credentials file
//     with provider.ReadManagedCredentials, where this stand-in takes it from
//     Managed.SetKeyExpiry.
//   - A Provider with no platform label is an error, before any read. A listing
//     with an entry the status cannot hold (no name, a name above 253
//     characters, a revision above 256, or more than 1024 entries ever) is an
//     error that writes nothing.
//   - Conditions the controller does not write, such as DialHomeUnreachable,
//     are kept as found.
type ManagedController struct {
	Client   client.Client
	Provider string
	Managed  *Managed
	Clock    *Clock
}

// Limits of status.catalog (api/v1alpha1, CatalogEntryStatus).
const (
	maxCatalogEntries  = 1024
	maxCatalogNameLen  = 253
	maxCatalogRevision = 256
	expiryWarning      = 24 * time.Hour
)

// Sync reads the catalog once and writes the Provider's status.
func (m *ManagedController) Sync(ctx context.Context) error {
	var prov v1alpha1.Provider
	if err := m.Client.Get(ctx, client.ObjectKey{Name: m.Provider}, &prov); err != nil {
		return fmt.Errorf("get Provider %s: %w", m.Provider, err)
	}
	platform := prov.Labels[v1alpha1.LabelPlatform]
	if platform == "" {
		return fmt.Errorf("no %s label on Provider %s: the managed IDs need the platform", v1alpha1.LabelPlatform, m.Provider)
	}
	now := m.Clock.Now().UTC().Truncate(time.Second)
	old := prov.DeepCopy()
	entries, err := m.Managed.ListCatalog(ctx)
	switch {
	case err == nil:
		catalog, merr := mergeCatalog(prov.Status.Catalog, entries, platform, now)
		if merr != nil {
			return merr
		}
		prov.Status.Catalog, prov.Status.LastCatalogRead = catalog, mt(now)
		setCondition(&prov, v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead", "", now)
		setCondition(&prov, v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, "KeyAccepted", "", now)
	case errors.Is(err, provider.ErrKeyRejected):
		setCondition(&prov, v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected", err.Error(), now)
	default:
		return nil // an unknown error changes nothing
	}
	m.setExpiring(&prov, now)
	prov.Status.ObservedGeneration, prov.Status.LastReconcileTime = prov.Generation, mt(now)
	return writeProviderStatus(ctx, m.Client, old, &prov)
}

// setExpiring sets CredentialsExpiring from the key's expiry; with none known
// it leaves the condition alone.
func (m *ManagedController) setExpiring(prov *v1alpha1.Provider, now time.Time) {
	exp := m.Managed.KeyExpiry()
	if exp.IsZero() {
		return
	}
	msg := "the key expires at " + exp.Format(time.RFC3339)
	switch {
	case !now.Before(exp):
		setCondition(prov, v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "Expired", "the key expired at "+exp.Format(time.RFC3339), now)
	case !now.Before(exp.Add(-expiryWarning)):
		setCondition(prov, v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "ExpiresWithinADay", msg, now)
	default:
		setCondition(prov, v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring", msg, now)
	}
}

func setCondition(prov *v1alpha1.Provider, typ string, status metav1.ConditionStatus, reason, message string, now time.Time) {
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message,
		ObservedGeneration: prov.Generation, LastTransitionTime: *mt(now),
	})
}

// mergeCatalog returns the catalog status after a successful read that listed
// listed: the old entries, all marked unlisted, then the listed ones on top of
// them, sorted by name. It never changes old.
func mergeCatalog(old []v1alpha1.CatalogEntryStatus, listed []provider.CatalogEntry, platform string, now time.Time) ([]v1alpha1.CatalogEntryStatus, error) {
	byName := make(map[string]v1alpha1.CatalogEntryStatus, len(old)+len(listed))
	for _, e := range old {
		e.Listed = false
		byName[e.Name] = *e.DeepCopy()
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

// catalogEntry builds the status of one listed entry.
func catalogEntry(l provider.CatalogEntry, platform string, now time.Time) (v1alpha1.CatalogEntryStatus, error) {
	if len(l.Name) > maxCatalogNameLen {
		return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry name has %d characters, above the %d status.catalog allows", len(l.Name), maxCatalogNameLen)
	}
	in := deploymentid.CatalogEntry{Platform: platform, CatalogName: l.Name}
	var revision *string
	if l.Revision != nil && *l.Revision != "" {
		if len(*l.Revision) > maxCatalogRevision {
			return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry %q: revision has %d characters, above the %d status.catalog allows", l.Name, len(*l.Revision), maxCatalogRevision)
		}
		r := *l.Revision
		revision, in.Revision = &r, r
	}
	id, err := deploymentid.ComputeManaged(in)
	if err != nil {
		return v1alpha1.CatalogEntryStatus{}, fmt.Errorf("catalog entry %q: %w", l.Name, err)
	}
	return v1alpha1.CatalogEntryStatus{
		Name: l.Name, Revision: revision, ManagedID: v1alpha1.FullDeploymentID(id.SHA256),
		Listed: true, LastListedTime: mt(now),
	}, nil
}

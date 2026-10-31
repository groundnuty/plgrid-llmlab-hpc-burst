package fake

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
// most once a minute, and a test decides how often to call Sync. The rules for
// the catalog, readiness, expiry and events are @hpc-burst-science-agent's
// ruling of 31 Oct 2026; they are the shared code of package provider
// (MergeCatalog, SetManagedReadiness, ExpiringCondition), which the real
// managed controller (#69) applies too, so the two cannot drift.
//
//   - A successful read puts the catalog that provider.MergeCatalog gives in
//     status.catalog: one entry for every name listed, and the entries that
//     left the listing kept with listed false, their revision, managed ID and
//     lastListedTime as they were, so long as the 1024 entries of the status
//     leave room. When they do not, the unlisted entries with the oldest
//     lastListedTime are dropped first, and a listed entry is never dropped for
//     room; if the listing alone is longer than 1024, the first 1024 names are
//     kept. A listed entry the status cannot hold (an empty name, a name above
//     253 characters, a revision above 256, a name listed twice with different
//     revisions) is skipped, and the rest of the listing is recorded: the read
//     still counts as successful. lastCatalogRead and lastReconcileTime are
//     now, lastSuccessfulStatusRead is now, Ready is True with reason
//     CatalogRead, and CredentialsRejected is False.
//   - A key the gateway rejects (an error wrapping provider.ErrKeyRejected)
//     sets CredentialsRejected True, and leaves the catalog and lastCatalogRead
//     as they were. It is a failed read for readiness, as is an unknown error.
//   - Any other error is unknown. Sync writes lastReconcileTime and
//     observedGeneration, applies the readiness rule and evaluates
//     CredentialsExpiring, and leaves the catalog, lastCatalogRead and
//     CredentialsRejected alone. It returns nil, as the batch Controller does
//     for a failed read.
//   - Readiness is the batch staleness rule (provider.SetManagedReadiness,
//     docs/development.md, Provider readiness): a failed read leaves
//     lastSuccessfulStatusRead alone, and Ready turns False with reason
//     ReadsFailing only when there is no successful read yet or the last is
//     more than v1alpha1.ProviderReadyStaleness (5 minutes) old; until then
//     Ready keeps its value.
//   - CredentialsExpiring is provider.ExpiringCondition of the key's expiry on
//     every pass that writes, whether the read worked or not: with no expiry
//     (a zero Managed.KeyExpiry) it is False with reason NotExpiring and the
//     message "the credentials file names no expiry"; from a day before the
//     expiry it is True with ExpiresWithinADay, and from the expiry on Expired.
//     The real controller reads the expiry from the credentials file with
//     provider.ReadManagedCredentials, where this stand-in takes it from
//     Managed.SetKeyExpiry. A rotation to a key without one clears the state at
//     the next pass.
//   - Events go to Recorder when it is set (nil means none), after the status
//     is written by a successful read: one Warning CatalogEntrySkipped for each
//     skipped entry, naming it, and when the listing alone was longer than 1024
//     a Warning CatalogTruncated on every such read. Neither is a condition.
//   - A Provider with no platform label is an error, before any read.
//   - Conditions the controller does not write, such as DialHomeUnreachable,
//     are kept as found.
type ManagedController struct {
	Client   client.Client
	Provider string
	Managed  *Managed
	Clock    *Clock
	// Recorder receives the events of a successful read; nil records none.
	Recorder events.EventRecorder
}

// eventAction is the action of the events a catalog read raises.
const eventAction = "ReadCatalog"

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
	var merge provider.CatalogMerge
	switch {
	case err == nil:
		var merr error
		if merge, merr = provider.MergeCatalog(prov.Status.Catalog, entries, platform, now); merr != nil {
			return merr
		}
		prov.Status.Catalog, prov.Status.LastCatalogRead = merge.Catalog, mt(now)
		provider.SetManagedReadiness(&prov.Status, prov.Generation, true, now)
		setCondition(&prov, v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted, "", now)
	case errors.Is(err, provider.ErrKeyRejected):
		setCondition(&prov, v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected, err.Error(), now)
		provider.SetManagedReadiness(&prov.Status, prov.Generation, false, now)
	default: // an unknown error: the catalog, lastCatalogRead and CredentialsRejected stay
		provider.SetManagedReadiness(&prov.Status, prov.Generation, false, now)
	}
	m.setExpiring(&prov, now)
	prov.Status.ObservedGeneration, prov.Status.LastReconcileTime = prov.Generation, mt(now)
	if err := writeProviderStatus(ctx, m.Client, old, &prov); err != nil {
		return err
	}
	m.raise(&prov, merge)
	return nil
}

// setExpiring sets CredentialsExpiring from the key's expiry; a zero expiry
// means none.
func (m *ManagedController) setExpiring(prov *v1alpha1.Provider, now time.Time) {
	exp := m.Managed.KeyExpiry()
	status, reason, msg := provider.ExpiringCondition(&exp, now)
	setCondition(prov, v1alpha1.ConditionCredentialsExpiring, status, reason, msg, now)
}

// raise sends the events of one successful read: a Warning for each skipped
// entry, and one for a truncated listing.
func (m *ManagedController) raise(prov *v1alpha1.Provider, merge provider.CatalogMerge) {
	if m.Recorder == nil {
		return
	}
	for _, s := range merge.Skipped {
		m.Recorder.Eventf(prov, nil, corev1.EventTypeWarning, provider.EventCatalogEntrySkipped, eventAction, "%s", provider.SkippedNote(s))
	}
	if merge.Truncated > 0 {
		m.Recorder.Eventf(prov, nil, corev1.EventTypeWarning, provider.EventCatalogTruncated, eventAction, "%s", provider.TruncatedNote(merge.Truncated))
	}
}

func setCondition(prov *v1alpha1.Provider, typ string, status metav1.ConditionStatus, reason, message string, now time.Time) {
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message,
		ObservedGeneration: prov.Generation, LastTransitionTime: *mt(now),
	})
}

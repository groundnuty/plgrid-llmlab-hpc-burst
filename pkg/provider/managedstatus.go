package provider

import (
	"fmt"
	"time"
	"unicode/utf8"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The rules a managed Provider's controller and its stand-in
// (pkg/provider/fake.ManagedController) both apply to the Provider's status,
// in one place so they cannot drift (HBEP-4, Managed provider in alpha;
// @hpc-burst-science-agent's ruling of 31 Oct 2026).

// Event reasons of a managed Provider's catalog read, both of type degraded
// (EventActionDegraded). Neither is a condition.
const (
	// EventCatalogEntrySkipped: a listed entry the status cannot hold was
	// skipped (CatalogMerge.Skipped); the rest of the read was recorded.
	EventCatalogEntrySkipped = "CatalogEntrySkipped"
	// EventCatalogTruncated: the listing alone held more entries than
	// status.catalog can (CatalogMerge.Truncated). Raised when the truncation
	// starts, then at most once an hour while it lasts (RepeatGate); Ready
	// stays True.
	EventCatalogTruncated = "CatalogTruncated"
)

// SkippedNote is the note of an EventCatalogEntrySkipped event, before
// NewManagedEvent cuts it to MaxEventNote. A name above 64 characters is
// shortened.
func SkippedNote(s SkippedEntry) string {
	name := s.Name
	if utf8.RuneCountInString(name) > 64 {
		name = string([]rune(name)[:64]) + "..."
	}
	return fmt.Sprintf("catalog entry %q skipped: %s", name, s.Why)
}

// TruncatedNote is the note of an EventCatalogTruncated event.
func TruncatedNote(truncated int) string {
	return fmt.Sprintf("the gateway listed %d entries more than status.catalog holds (%d); the first %d by name are kept",
		truncated, MaxCatalogEntries, MaxCatalogEntries)
}

// NoExpiryMessage is CredentialsExpiring's message when the credentials file
// names no expiry.
const NoExpiryMessage = "the credentials file names no expiry"

// ExpiringCondition is a managed Provider's CredentialsExpiring condition at
// now: with no expiry known, False with ReasonNotExpiring and
// NoExpiryMessage; from CredentialsExpiringWindow before the expiry, True
// with ReasonExpiresWithinADay; from the expiry on, True with ReasonExpired;
// before that, False with ReasonNotExpiring. The file is re-read at every
// pass, so a rotation to a file without an expiry clears an Expired state.
func ExpiringCondition(expiry *time.Time, now time.Time) (metav1.ConditionStatus, string, string) {
	if expiry == nil || expiry.IsZero() {
		return metav1.ConditionFalse, v1alpha1.ReasonNotExpiring, NoExpiryMessage
	}
	at := expiry.UTC().Format(time.RFC3339)
	switch {
	case !now.Before(*expiry):
		return metav1.ConditionTrue, v1alpha1.ReasonExpired, "the key expired at " + at
	case !now.Before(expiry.Add(-CredentialsExpiringWindow)):
		return metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay, "the key expires at " + at
	}
	return metav1.ConditionFalse, v1alpha1.ReasonNotExpiring, "the key expires at " + at
}

// SetManagedReadiness applies the batch staleness rule (HBEP-4, Operational
// readiness 1; docs/development.md, Provider readiness) to a managed
// Provider after a pass. readOK says whether the pass read the catalog
// successfully: then lastSuccessfulStatusRead is now and Ready is True with
// ReasonCatalogRead. Otherwise (a rejected key, an unknown error, a refused
// credentials file) lastSuccessfulStatusRead is left alone, and Ready turns
// False with ReasonReadsFailing only when there was no successful read or the
// last is older than v1alpha1.ProviderReadyStaleness; until then it keeps its
// value. The message names the time of the last success, not its age, so a
// failing Provider is not rewritten for it. now is whole seconds.
func SetManagedReadiness(st *v1alpha1.ProviderStatus, generation int64, readOK bool, now time.Time) {
	cond := metav1.Condition{Type: v1alpha1.ConditionProviderReady, ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(now)}
	if readOK {
		t := metav1.NewTime(now)
		st.LastSuccessfulStatusRead = &t
		cond.Status, cond.Reason = metav1.ConditionTrue, v1alpha1.ReasonCatalogRead
		apimeta.SetStatusCondition(&st.Conditions, cond)
		return
	}
	last := st.LastSuccessfulStatusRead
	switch {
	case last == nil:
		cond.Message = "reads are failing: no successful read yet"
	case now.Sub(last.Time) > v1alpha1.ProviderReadyStaleness:
		cond.Message = "reads are failing: the last successful read was at " + last.UTC().Format(time.RFC3339)
	default:
		return
	}
	cond.Status, cond.Reason = metav1.ConditionFalse, v1alpha1.ReasonReadsFailing
	apimeta.SetStatusCondition(&st.Conditions, cond)
}

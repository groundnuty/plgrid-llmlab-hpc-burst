package fake

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// More tests of the managed fake's catalog loop: a rejected key, an unknown
// error, readiness, key expiry and the conditions the fake does not own (the
// limits of status.catalog are in managed_controller_catalog_test.go). They use
// the helpers of managed_controller_test.go. Every expected time, reason and
// message is written out by hand, never taken from provider.SetManagedReadiness
// or provider.ExpiringCondition, which the fake calls.

const (
	// The Ready message of a Provider whose last successful read was at 12:00:00
	// on 2026-10-30 (at(12, 0, 0)).
	readsFailingSince1200 = "reads are failing: the last successful read was at 2026-10-30T12:00:00Z"
	noExpiryMessage       = "the credentials file names no expiry"
)

// wantAny returns the condition of type typ, which must exist.
func (e *menv) wantAny(typ string) metav1.Condition {
	e.t.Helper()
	c := apimeta.FindStatusCondition(e.provider().Status.Conditions, typ)
	if c == nil {
		e.t.Fatalf("at %s condition %s is missing", e.clock.Now().Format("15:04:05"), typ)
	}
	return *c
}

// wantMessage checks the message of a condition.
func (e *menv) wantMessage(typ, msg string) {
	e.t.Helper()
	c := e.wantAny(typ)
	if c.Message != msg {
		e.t.Fatalf("at %s condition %s message = %q, want %q", e.clock.Now().Format("15:04:05"), typ, c.Message, msg)
	}
}

// wantReady checks Ready, its reason and message, and that
// v1alpha1.ProviderReady, the reader's judgement, agrees with ready at now.
func (e *menv) wantReady(ready bool, reason, msg string) {
	e.t.Helper()
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	e.wantCondition(v1alpha1.ConditionProviderReady, status, reason)
	e.wantMessage(v1alpha1.ConditionProviderReady, msg)
	if got := v1alpha1.ProviderReady(e.provider(), e.clock.Now()); got != ready {
		e.t.Fatalf("at %s v1alpha1.ProviderReady = %v, want %v", e.clock.Now().Format("15:04:05"), got, ready)
	}
}

func TestManagedRejectedKeySetsTheConditionAndLeavesTheCatalog(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))

	e.gw.RejectKey(true)
	e.gw.SetCatalog(entry("example-a"), entry("example-d")) // hidden behind the rejection
	e.syncAt(at(12, 5, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected)
	wantMT(t, "condition time", &c.LastTransitionTime, at(12, 5, 0))
	if !strings.Contains(c.Message, provider.ErrKeyRejected.Error()) {
		t.Fatalf("message = %q, want it to carry the gateway's refusal", c.Message)
	}
	if got := e.names(); len(got) != 1 || got[0] != "example-a" {
		t.Fatalf("catalog = %v, want it untouched by a rejected read", got)
	}
	wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
	prov := e.provider()
	wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 0, 0)) // not advanced
	wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 5, 0))
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 0, 0))
	// 5 minutes after the last success is not yet stale: Ready keeps its value.
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")

	// A minute later the last success is 6 minutes old: Ready turns False, once.
	// The CredentialsRejected condition does not churn while the key stays
	// rejected.
	e.syncAt(at(12, 6, 0))
	c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected)
	wantMT(t, "condition time", &c.LastTransitionTime, at(12, 5, 0))
	e.wantReady(false, v1alpha1.ReasonReadsFailing, readsFailingSince1200)
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 0, 0))
	r := e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, v1alpha1.ReasonReadsFailing)
	wantMT(t, "Ready time", &r.LastTransitionTime, at(12, 6, 0))
	e.syncAt(at(12, 6, 30))
	r = e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, v1alpha1.ReasonReadsFailing)
	wantMT(t, "Ready time", &r.LastTransitionTime, at(12, 6, 0)) // not rewritten while it fails

	// An accepted read clears the rejection and turns Ready True again.
	e.gw.RejectKey(false)
	e.syncAt(at(12, 7, 0))
	c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted)
	wantMT(t, "condition time", &c.LastTransitionTime, at(12, 7, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 7, 0))
	if got := e.names(); len(got) != 2 || got[1] != "example-d" {
		t.Fatalf("catalog = %v, want example-d once a read succeeds", got)
	}
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 7, 0))
}

func TestManagedRejectedKeyFromTheStartWritesNoCatalog(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.gw.RejectKey(true)
	e.syncAt(at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected)
	prov := e.provider()
	if len(prov.Status.Catalog) != 0 || prov.Status.LastCatalogRead != nil {
		t.Fatalf("status = catalog %v, lastCatalogRead %v, want none: nothing was ever read", prov.Status.Catalog, prov.Status.LastCatalogRead)
	}
	// With no successful read ever, the first failed pass turns Ready False.
	wantNilMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead)
	e.wantReady(false, v1alpha1.ReasonReadsFailing, "reads are failing: no successful read yet")
}

func TestManagedUnknownErrorWritesTimesReadinessAndExpiryOnly(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	// 24.5 hours after 12:00 on 10-30, so 23.5 hours after 13:00.
	e.gw.SetKeyExpiry(time.Date(2026, 10, 31, 12, 30, 0, 0, time.UTC))
	e.syncAt(at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, v1alpha1.ReasonNotExpiring)
	before := e.provider()

	boom := errors.New("example transport failure")
	e.gw.FailReads(boom)
	e.gw.RejectKey(true)                // the unknown error wins: no rejection is seen
	e.gw.SetCatalog(entry("example-z")) // hidden behind the failure

	// 3 minutes after the last success: the times move, Ready keeps its value.
	e.syncAt(at(12, 3, 0))
	mid := e.provider()
	wantMT(t, "lastReconcileTime", mid.Status.LastReconcileTime, at(12, 3, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")

	// An hour after: Ready is False, the expiry is within a day, and what the
	// read owns is as it was.
	e.syncAt(at(13, 0, 0))
	after := e.provider()
	wantMT(t, "lastReconcileTime", after.Status.LastReconcileTime, at(13, 0, 0))
	if after.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration = %d, want 1", after.Status.ObservedGeneration)
	}
	if !apiequality.Semantic.DeepEqual(before.Status.Catalog, after.Status.Catalog) {
		t.Fatalf("an unknown error changed the catalog:\n before %+v\n after  %+v", before.Status.Catalog, after.Status.Catalog)
	}
	if got := e.names(); len(got) != 1 || got[0] != "example-a" {
		t.Fatalf("catalog = %v, want [example-a]", got)
	}
	wantMT(t, "lastCatalogRead", after.Status.LastCatalogRead, at(12, 0, 0))
	wantMT(t, "lastSuccessfulStatusRead", after.Status.LastSuccessfulStatusRead, at(12, 0, 0))
	e.wantReady(false, v1alpha1.ReasonReadsFailing, readsFailingSince1200)
	c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted)
	wantMT(t, "CredentialsRejected time", &c.LastTransitionTime, at(12, 0, 0)) // untouched
	c = e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay)
	wantMT(t, "CredentialsExpiring time", &c.LastTransitionTime, at(13, 0, 0))
	e.wantMessage(v1alpha1.ConditionCredentialsExpiring, "the key expires at 2026-10-31T12:30:00Z")
	if n := e.gw.Count("list-catalog"); n != 3 {
		t.Fatalf("list-catalog logged %d times, want 3: the failing reads were made", n)
	}

	// The failure ends, the key is still rejected: that is no success, so Ready
	// stays False, and the rejection shows.
	e.gw.FailReads(nil)
	e.syncAt(at(13, 1, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected)
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay)
	e.wantReady(false, v1alpha1.ReasonReadsFailing, readsFailingSince1200)
	e.gw.RejectKey(false)
	e.syncAt(at(13, 2, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
	if got := e.names(); len(got) != 2 || got[1] != "example-z" {
		t.Fatalf("catalog = %v, want example-z once a read succeeds", got)
	}
}

func TestManagedReadinessFollowsAGatewayThatStartsFailing(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")

	e.gw.FailReads(errors.New("example gateway down"))
	for _, st := range []struct {
		when  time.Time
		ready bool
		msg   string
	}{
		{at(12, 1, 0), true, ""},
		{at(12, 2, 0), true, ""},
		{at(12, 3, 0), true, ""},
		{at(12, 4, 0), true, ""},
		{at(12, 5, 0), true, ""}, // exactly 5 minutes: not yet stale
		{at(12, 6, 0), false, readsFailingSince1200},
		{at(12, 7, 0), false, readsFailingSince1200},
	} {
		e.syncAt(st.when)
		reason := v1alpha1.ReasonCatalogRead
		if !st.ready {
			reason = v1alpha1.ReasonReadsFailing
		}
		e.wantReady(st.ready, reason, st.msg)
		wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 0, 0))
		wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 0, 0))
		wantMT(t, "lastReconcileTime", e.provider().Status.LastReconcileTime, st.when)
	}

	e.gw.FailReads(nil)
	e.syncAt(at(12, 8, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 8, 0))
}

func TestManagedKeyExpiry(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))
	// No expiry known: not expiring, and it says why.
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, v1alpha1.ReasonNotExpiring)
	e.wantMessage(v1alpha1.ConditionCredentialsExpiring, noExpiryMessage)

	e.gw.SetKeyExpiry(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))
	for _, st := range []struct {
		when   time.Time
		status metav1.ConditionStatus
		reason string
		msg    string
	}{
		{time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), metav1.ConditionFalse, v1alpha1.ReasonNotExpiring, "the key expires at 2026-11-01T12:00:00Z"},
		{time.Date(2026, 10, 31, 11, 59, 59, 0, time.UTC), metav1.ConditionFalse, v1alpha1.ReasonNotExpiring, "the key expires at 2026-11-01T12:00:00Z"},
		{time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay, "the key expires at 2026-11-01T12:00:00Z"}, // a day before
		{time.Date(2026, 11, 1, 11, 59, 59, 0, time.UTC), metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay, "the key expires at 2026-11-01T12:00:00Z"},
		{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), metav1.ConditionTrue, v1alpha1.ReasonExpired, "the key expired at 2026-11-01T12:00:00Z"},
	} {
		e.syncAt(st.when)
		e.wantCondition(v1alpha1.ConditionCredentialsExpiring, st.status, st.reason)
		e.wantMessage(v1alpha1.ConditionCredentialsExpiring, st.msg)
	}

	// A rotation to a key with no expiry clears the Expired state at the next pass.
	e.gw.SetKeyExpiry(time.Time{})
	e.syncAt(time.Date(2026, 11, 1, 12, 1, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, v1alpha1.ReasonNotExpiring)
	e.wantMessage(v1alpha1.ConditionCredentialsExpiring, noExpiryMessage)

	// A rejected key is still checked for its expiry.
	e.gw.SetKeyExpiry(time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC))
	e.gw.RejectKey(true)
	e.syncAt(time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, v1alpha1.ReasonExpiresWithinADay)

	// So is a failed read of any kind.
	e.gw.RejectKey(false)
	e.gw.FailReads(errors.New("example transport failure"))
	e.gw.SetKeyExpiry(time.Date(2026, 11, 1, 13, 1, 0, 0, time.UTC))
	e.syncAt(time.Date(2026, 11, 1, 13, 2, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, v1alpha1.ReasonExpired)
	e.gw.SetKeyExpiry(time.Time{})
	e.syncAt(time.Date(2026, 11, 1, 13, 3, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, v1alpha1.ReasonNotExpiring)
	e.wantMessage(v1alpha1.ConditionCredentialsExpiring, noExpiryMessage)
}

func TestManagedKeepsAConditionItDoesNotOwn(t *testing.T) {
	e := newManagedEnv(t, withManagedFields)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))
	set := metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example: the agent did not register",
		LastTransitionTime: *mt(at(12, 0, 30)), ObservedGeneration: 1,
	}
	applyDialHome(t, e.c, managedProvider, set)
	want := findDialHome(t, e.provider())

	for i, st := range []struct {
		when      time.Time
		reject    bool
		failReads error
	}{
		{at(12, 1, 0), false, nil},
		{at(12, 2, 0), true, nil},                    // a rejected key writes the status
		{at(12, 3, 0), false, errors.New("example")}, // an unknown error writes the times, readiness and expiry
		{at(12, 4, 0), false, nil},
	} {
		e.gw.RejectKey(st.reject)
		e.gw.FailReads(st.failReads)
		e.syncAt(st.when)
		if got := findDialHome(t, e.provider()); !apiequality.Semantic.DeepEqual(got, want) {
			t.Fatalf("step %d: DialHomeUnreachable = %+v, want it unchanged: %+v", i, got, want)
		}
	}

	// Every manager on the stored Provider is one of the two writers.
	var sawProvider bool
	for _, mf := range e.provider().ManagedFields {
		switch mf.Manager {
		case v1alpha1.FieldManagerProvider:
			sawProvider = true
		case v1alpha1.FieldManagerLifecycle:
		default:
			t.Fatalf("a write by field manager %q on the Provider", mf.Manager)
		}
	}
	if !sawProvider {
		t.Fatalf("managed fields = %+v, want an entry for %s", e.provider().ManagedFields, v1alpha1.FieldManagerProvider)
	}
}

func TestManagedCatalogIsSortedByNameWhateverTheListingOrder(t *testing.T) {
	e := newManagedEnv(t)
	var desc, shuffled []provider.CatalogEntry
	for i := 11; i >= 0; i-- { // the gateway lists in descending order
		desc = append(desc, entry(fmt.Sprintf("example-%02d", i)))
	}
	e.gw.SetCatalog(desc...)
	e.syncAt(at(12, 0, 0))
	for _, i := range []int{20, 3, 15, 7, 24, 1, 18} { // then new names in no order
		shuffled = append(shuffled, entry(fmt.Sprintf("example-%02d", i)))
	}
	e.gw.SetCatalog(shuffled...)
	e.syncAt(at(12, 1, 0))

	got := e.names()
	if len(got) != 12+4 { // 03, 07 and 01 were listed before; 20, 15, 24 and 18 are new
		t.Fatalf("%d entries, want 16: %v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("catalog is not sorted by name: %v", got)
		}
	}
}

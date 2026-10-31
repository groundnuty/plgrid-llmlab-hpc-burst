package fake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// More tests of the managed fake's catalog loop: a rejected key, an unknown
// error, key expiry, the limits of status.catalog and the conditions the fake
// does not own. They use the helpers of managed_controller_test.go.

func TestManagedRejectedKeySetsTheConditionAndLeavesTheCatalog(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))

	e.gw.RejectKey(true)
	e.gw.SetCatalog(entry("example-a"), entry("example-d")) // hidden behind the rejection
	e.syncAt(at(12, 5, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
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
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead") // as it was

	// The condition does not churn while the key stays rejected.
	e.syncAt(at(12, 6, 0))
	c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	wantMT(t, "condition time", &c.LastTransitionTime, at(12, 5, 0))

	e.gw.RejectKey(false)
	e.syncAt(at(12, 7, 0))
	c = e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, "KeyAccepted")
	wantMT(t, "condition time", &c.LastTransitionTime, at(12, 7, 0))
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
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	prov := e.provider()
	if len(prov.Status.Catalog) != 0 || prov.Status.LastCatalogRead != nil {
		t.Fatalf("status = catalog %v, lastCatalogRead %v, want none: nothing was ever read", prov.Status.Catalog, prov.Status.LastCatalogRead)
	}
	e.wantNoCondition(v1alpha1.ConditionProviderReady)
}

func TestManagedUnknownErrorChangesNothing(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.gw.SetKeyExpiry(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)) // two days out
	e.syncAt(at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring")
	before := e.provider()

	boom := errors.New("example transport failure")
	e.gw.FailReads(boom)
	e.gw.RejectKey(true)                                     // the unknown error wins: no rejection is seen
	e.gw.SetCatalog(entry("example-z"))                      // hidden behind the failure
	e.syncAt(time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC)) // within a day of the expiry
	after := e.provider()
	if after.ResourceVersion != before.ResourceVersion || !apiequality.Semantic.DeepEqual(before.Status, after.Status) {
		t.Fatalf("an unknown error changed the status:\n before %+v\n after  %+v", before.Status, after.Status)
	}
	if n := e.gw.Count("list-catalog"); n != 2 {
		t.Fatalf("list-catalog logged %d times, want 2: the failing read was made", n)
	}

	e.gw.FailReads(nil) // the next read that works sees the key rejected and the time
	e.syncAt(time.Date(2026, 10, 31, 13, 1, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, "KeyRejected")
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "ExpiresWithinADay")
}

func TestManagedKeyExpiry(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"))
	e.syncAt(at(12, 0, 0))
	e.wantNoCondition(v1alpha1.ConditionCredentialsExpiring) // no expiry known

	e.gw.SetKeyExpiry(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC))
	for _, st := range []struct {
		when   time.Time
		status metav1.ConditionStatus
		reason string
	}{
		{time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), metav1.ConditionFalse, "NotExpiring"},
		{time.Date(2026, 10, 31, 11, 59, 59, 0, time.UTC), metav1.ConditionFalse, "NotExpiring"},
		{time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), metav1.ConditionTrue, "ExpiresWithinADay"}, // a day before
		{time.Date(2026, 11, 1, 11, 59, 59, 0, time.UTC), metav1.ConditionTrue, "ExpiresWithinADay"},
		{time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), metav1.ConditionTrue, "Expired"},
	} {
		e.syncAt(st.when)
		e.wantCondition(v1alpha1.ConditionCredentialsExpiring, st.status, st.reason)
	}

	// A rejected key is still checked for its expiry.
	e.gw.SetKeyExpiry(time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC))
	e.gw.RejectKey(true)
	e.syncAt(time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "ExpiresWithinADay")
}

func TestManagedLimitsOfTheCatalogStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog []provider.CatalogEntry
	}{
		{"an entry with no name", []provider.CatalogEntry{entry("")}},
		{"a name above 253 characters", []provider.CatalogEntry{entry(strings.Repeat("n", 254))}},
		{"a revision above 256 characters", []provider.CatalogEntry{{Name: "example-a", Revision: rev(strings.Repeat("r", 257))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newManagedEnv(t)
			e.gw.SetCatalog(entry("example-ok"))
			e.syncAt(at(12, 0, 0))
			before := e.provider()
			e.gw.SetCatalog(append([]provider.CatalogEntry{entry("example-fine")}, tc.catalog...)...)
			if err := e.ctl.Sync(context.Background()); err == nil {
				t.Fatal("Sync = nil, want an error")
			}
			if after := e.provider(); after.ResourceVersion != before.ResourceVersion {
				t.Fatal("a listing the status cannot hold was partly written")
			}
		})
	}

	t.Run("1024 entries fit and the 1025th does not", func(t *testing.T) {
		e := newManagedEnv(t)
		var many []provider.CatalogEntry
		for i := range 1024 {
			many = append(many, entry(fmt.Sprintf("example-%04d", i)))
		}
		e.gw.SetCatalog(many...)
		e.syncAt(at(12, 0, 0))
		if n := len(e.provider().Status.Catalog); n != 1024 {
			t.Fatalf("%d entries, want 1024", n)
		}
		before := e.provider()
		e.gw.SetCatalog(entry("example-extra")) // 1025 names ever listed
		if err := e.ctl.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), "1024") {
			t.Fatalf("Sync = %v, want an error naming the limit of 1024", err)
		}
		if e.provider().ResourceVersion != before.ResourceVersion {
			t.Fatal("the over-long catalog was written")
		}
	})
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
		{at(12, 3, 0), false, errors.New("example")}, // an unknown error writes nothing
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

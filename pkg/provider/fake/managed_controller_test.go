package fake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The tests of the managed fake's catalog loop (HBEP-4, Managed provider in
// alpha; the test plan's "listing changes and a rejected key"). Times are
// written out by hand, and every managed ID expectation is computed by calling
// deploymentid.ComputeManaged, or the hash itself, on hand-written inputs,
// apart from the fake's own code path.

const (
	managedProvider = "example-managed"
	testPlatform    = "example-platform"
)

type menv struct {
	t     *testing.T
	clock *Clock
	gw    *Managed
	c     client.Client
	ctl   *ManagedController
}

func newManagedEnv(t *testing.T, opts ...func(*clientfake.ClientBuilder) *clientfake.ClientBuilder) *menv {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clock := NewClock(at(12, 0, 0))
	gw := NewManaged(clock)
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{
		Name: managedProvider, Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: testPlatform},
	}}
	builder := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(prov).WithStatusSubresource(&v1alpha1.Provider{})
	for _, opt := range opts {
		builder = opt(builder)
	}
	c := builder.Build()
	return &menv{t: t, clock: clock, gw: gw, c: c,
		ctl: &ManagedController{Client: c, Provider: managedProvider, Managed: gw, Clock: clock}}
}

func (e *menv) provider() *v1alpha1.Provider {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: managedProvider}, &p); err != nil {
		e.t.Fatal(err)
	}
	return &p
}

func (e *menv) syncAt(t time.Time) {
	e.t.Helper()
	e.clock.Set(t)
	if err := e.ctl.Sync(context.Background()); err != nil {
		e.t.Fatalf("Sync at %s: %v", t.Format("15:04:05"), err)
	}
}

func (e *menv) sync() { e.t.Helper(); e.syncAt(e.clock.Now()) }

func (e *menv) entry(name string) v1alpha1.CatalogEntryStatus {
	e.t.Helper()
	for _, ce := range e.provider().Status.Catalog {
		if ce.Name == name {
			return ce
		}
	}
	e.t.Fatalf("status.catalog has no entry %q: %+v", name, e.provider().Status.Catalog)
	return v1alpha1.CatalogEntryStatus{}
}

func (e *menv) names() []string {
	var out []string
	for _, ce := range e.provider().Status.Catalog {
		out = append(out, ce.Name)
	}
	return out
}

func (e *menv) wantCondition(typ string, status metav1.ConditionStatus, reason string) metav1.Condition {
	e.t.Helper()
	c := apimeta.FindStatusCondition(e.provider().Status.Conditions, typ)
	if c == nil || c.Status != status || c.Reason != reason {
		e.t.Fatalf("at %s condition %s = %+v, want %s %s", e.clock.Now().Format("15:04:05"), typ, c, status, reason)
	}
	return *c
}

func (e *menv) wantNoCondition(typ string) {
	e.t.Helper()
	if c := apimeta.FindStatusCondition(e.provider().Status.Conditions, typ); c != nil {
		e.t.Fatalf("at %s condition %s = %+v, want none", e.clock.Now().Format("15:04:05"), typ, c)
	}
}

// wantManagedID is ComputeManaged of hand-written inputs, as the full 64
// characters the status field holds.
func wantManagedID(t *testing.T, platform, name, revision string) v1alpha1.FullDeploymentID {
	t.Helper()
	id, err := deploymentid.ComputeManaged(deploymentid.CatalogEntry{Platform: platform, CatalogName: name, Revision: revision})
	if err != nil {
		t.Fatal(err)
	}
	return v1alpha1.FullDeploymentID(id.SHA256)
}

// wantEntry checks one catalog entry of the status.
func wantEntry(t *testing.T, got v1alpha1.CatalogEntryStatus, listed bool, revision string, lastListed time.Time) {
	t.Helper()
	switch {
	case got.Listed != listed:
		t.Fatalf("entry %s: listed = %v, want %v", got.Name, got.Listed, listed)
	case revision == "" && got.Revision != nil, revision != "" && (got.Revision == nil || *got.Revision != revision):
		t.Fatalf("entry %s: revision = %v, want %q", got.Name, got.Revision, revision)
	case got.ManagedID != wantManagedID(t, testPlatform, got.Name, revision):
		t.Fatalf("entry %s: managedID = %s, want ComputeManaged of (%s, %s, %q)", got.Name, got.ManagedID, testPlatform, got.Name, revision)
	}
	wantMT(t, "entry "+got.Name+" lastListedTime", got.LastListedTime, lastListed)
}

func TestManagedIDIsComputeManagedOfHandWrittenInputs(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(provider.CatalogEntry{Name: "example-27b", Revision: rev("2026-09")}, entry("example-9b"))
	e.sync()

	// The canonical form of HBEP-3, written out by hand (keys sorted, no
	// spaces), hashed with the standard library.
	withRev := sha256.Sum256([]byte(`{"catalog_name":"example-27b","kind":"managed","opaque":"true","platform":"example-platform","revision":"2026-09","scheme":"v1"}`))
	noRev := sha256.Sum256([]byte(`{"catalog_name":"example-9b","kind":"managed","opaque":"true","platform":"example-platform","scheme":"v1"}`))
	a, b := e.entry("example-27b"), e.entry("example-9b")
	if string(a.ManagedID) != hex.EncodeToString(withRev[:]) {
		t.Fatalf("example-27b managedID = %s, want sha256 of its canonical form %x", a.ManagedID, withRev)
	}
	if string(b.ManagedID) != hex.EncodeToString(noRev[:]) {
		t.Fatalf("example-9b managedID = %s, want sha256 of its canonical form %x: no revision, no revision member", b.ManagedID, noRev)
	}
	// And the contract's own function, on the same hand-written inputs.
	if want := wantManagedID(t, "example-platform", "example-27b", "2026-09"); a.ManagedID != want {
		t.Fatalf("example-27b managedID = %s, want %s", a.ManagedID, want)
	}
	if want := wantManagedID(t, "example-platform", "example-9b", ""); b.ManagedID != want {
		t.Fatalf("example-9b managedID = %s, want %s", b.ManagedID, want)
	}
	if len(a.ManagedID) != 64 || a.ManagedID == b.ManagedID || a.ManagedID.Display() != string(a.ManagedID)[:12] {
		t.Fatalf("managed IDs %s and %s: want two different 64-character IDs", a.ManagedID, b.ManagedID)
	}

	// An empty revision is no revision.
	e.gw.SetCatalog(provider.CatalogEntry{Name: "example-9b", Revision: rev("")})
	e.sync()
	got := e.entry("example-9b")
	if got.Revision != nil || got.ManagedID != b.ManagedID {
		t.Fatalf("an empty revision gave revision %v, managedID %s, want none and %s", got.Revision, got.ManagedID, b.ManagedID)
	}
}

func TestManagedIDFollowsThePlatformLabel(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-9b"))
	e.sync()
	first := e.entry("example-9b").ManagedID

	prov := e.provider()
	prov.Labels[v1alpha1.LabelPlatform] = "example-other-platform"
	if err := e.c.Update(context.Background(), prov); err != nil {
		t.Fatal(err)
	}
	e.sync()
	got := e.entry("example-9b").ManagedID
	if got == first || got != wantManagedID(t, "example-other-platform", "example-9b", "") {
		t.Fatalf("managedID = %s after the platform changed, want ComputeManaged over example-other-platform and not %s", got, first)
	}
}

func TestManagedProviderWithoutAPlatformLabelIsAnError(t *testing.T) {
	e := newManagedEnv(t)
	prov := e.provider()
	delete(prov.Labels, v1alpha1.LabelPlatform)
	if err := e.c.Update(context.Background(), prov); err != nil {
		t.Fatal(err)
	}
	before := e.provider()
	e.gw.SetCatalog(entry("example-9b"))
	err := e.ctl.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), v1alpha1.LabelPlatform) {
		t.Fatalf("Sync = %v, want an error naming the %s label", err, v1alpha1.LabelPlatform)
	}
	if n := e.gw.Count("list-catalog"); n != 0 {
		t.Fatalf("list-catalog logged %d times, want 0: no read without a platform", n)
	}
	if after := e.provider(); after.ResourceVersion != before.ResourceVersion {
		t.Fatal("the Provider's status was written without a platform")
	}
}

func TestManagedCatalogFollowsTheListing(t *testing.T) {
	e := newManagedEnv(t)
	e.gw.SetCatalog(entry("example-a"), entry("example-b"))
	e.syncAt(at(12, 0, 0))
	prov := e.provider()
	if got := e.names(); len(got) != 2 || got[0] != "example-a" || got[1] != "example-b" {
		t.Fatalf("catalog = %v, want [example-a example-b]", got)
	}
	wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
	wantEntry(t, e.entry("example-b"), true, "", at(12, 0, 0))
	wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 0, 0))
	wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 0, 0))
	if prov.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration = %d, want 1", prov.Status.ObservedGeneration)
	}
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, v1alpha1.ReasonCatalogRead)
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted)
	aID := e.entry("example-a").ManagedID

	// example-a leaves the listing, and example-c appears: a is kept, unlisted.
	e.gw.SetCatalog(entry("example-b"), entry("example-c"))
	e.syncAt(at(12, 1, 0))
	if got := e.names(); len(got) != 3 || got[0] != "example-a" || got[1] != "example-b" || got[2] != "example-c" {
		t.Fatalf("catalog = %v, want [example-a example-b example-c]: an entry that left is kept", got)
	}
	wantEntry(t, e.entry("example-a"), false, "", at(12, 0, 0)) // last listed at 12:00:00
	wantEntry(t, e.entry("example-b"), true, "", at(12, 1, 0))
	wantEntry(t, e.entry("example-c"), true, "", at(12, 1, 0))
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 1, 0))

	// example-a comes back, with a revision this time: a new ID, listed again.
	e.gw.SetCatalog(provider.CatalogEntry{Name: "example-a", Revision: rev("r2")}, entry("example-b"), entry("example-c"))
	e.syncAt(at(12, 2, 0))
	wantEntry(t, e.entry("example-a"), true, "r2", at(12, 2, 0))
	if e.entry("example-a").ManagedID == aID {
		t.Fatal("example-a kept its managed ID although its revision changed")
	}

	// An empty listing unlists everything and removes nothing.
	e.gw.SetCatalog()
	e.syncAt(at(12, 3, 0))
	if got := e.names(); len(got) != 3 {
		t.Fatalf("catalog = %v, want the three entries kept", got)
	}
	wantEntry(t, e.entry("example-a"), false, "r2", at(12, 2, 0))
	wantEntry(t, e.entry("example-c"), false, "", at(12, 2, 0)) // listed at 12:02:00 too
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 3, 0))
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, v1alpha1.ReasonCatalogRead)
	if n := e.gw.Count("list-catalog"); n != 4 {
		t.Fatalf("list-catalog logged %d times, want 4: one per Sync", n)
	}
}

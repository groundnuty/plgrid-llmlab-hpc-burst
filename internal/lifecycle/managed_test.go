package lifecycle

// Managed sources (HBEP-7, Reconcile loop, step 1; Test plan: "Managed source").
// The managed Provider's status is written by the provider fake's
// ManagedController from a scripted gateway, so the catalog, its listed flag
// and the CredentialsRejected condition are what a real Provider writes, not
// what the test assumes. A managed-source deployment gets no Allocation and no
// Instance; "nothing provisioned" is checked as no Allocation, no Instance and
// no sbatch call, since a count of Allocations alone would not show a submission.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	managedProvider = "llmlab-gateway"
	managedPlatform = "llmlab"
	managedEntry    = "example-entry"
)

// managedRig is the rig with a managed Provider beside its batch Provider.
type managedRig struct {
	*rig
	gw  *pfake.Managed
	ctl *pfake.ManagedController
}

// newManagedRig adds a managed PlatformProfile and a Provider of that name,
// labelled with the platform, whose gateway lists managedEntry.
func newManagedRig(t *testing.T, name string) *managedRig {
	rg := newRig(t)
	rg.create(&v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: managedPlatform}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}})
	rg.create(&v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: newUID(), Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: managedPlatform}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: managedPlatform}},
	})
	gw := pfake.NewManaged(rg.clock)
	gw.SetCatalog(provider.CatalogEntry{Name: managedEntry})
	return &managedRig{rig: rg, gw: gw, ctl: &pfake.ManagedController{Client: rg.c, Provider: name, Managed: gw, Clock: rg.clock}}
}

// settle lets the Provider read the catalog, then runs one round.
func (m *managedRig) settle() {
	m.t.Helper()
	if err := m.ctl.Sync(m.ctx); err != nil {
		m.t.Fatalf("catalog read: %v", err)
	}
	m.clock.Advance(10 * time.Second)
	m.Step()
}

// managedMD returns a managed-source deployment of the entry on a platform. It
// asks for 3 instances, which a managed source ignores.
func managedMD(platform string) *v1alpha1.ModelDeployment {
	return &v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "md", Namespace: rigNS, Generation: 1},
		Spec: v1alpha1.ModelDeploymentSpec{
			Model:     rigModel,
			Source:    v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: platform, CatalogName: managedEntry}},
			Instances: 3,
			Providers: v1alpha1.ProviderSelection{Names: []string{managedProvider}},
		},
	}
}

// managedExpect fails unless a condition of the deployment has the status and
// the reason; "" accepts any reason (HBEP-7 names none for a true Ready).
func managedExpect(t *testing.T, rg *rig, typ string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	if c := rg.condition("md", typ); c.Status != status || (reason != "" && c.Reason != reason) {
		t.Errorf("condition %s = %s/%s (%q), want %s/%s", typ, c.Status, c.Reason, c.Message, status, reason)
	}
}

// managedNothingProvisioned fails unless the deployment has no Allocation, no
// Instance, no submission and zero desired and ready Instances.
func managedNothingProvisioned(t *testing.T, rg *rig) {
	t.Helper()
	st := rg.md("md").Status
	if n, i, s := len(rg.allocations("md")), len(rg.instances("md")), rg.submissions(); n != 0 || i != 0 || s != 0 {
		t.Errorf("%d Allocations, %d Instances, %d sbatch calls; a managed source provisions nothing", n, i, s)
	}
	if st.DesiredInstances != 0 || st.ReadyInstances != 0 || st.Instances != 0 {
		t.Errorf("status counts desired %d, ready %d, live %d, want 0 for a managed source", st.DesiredInstances, st.ReadyInstances, st.Instances)
	}
}

// TestManagedSourceListedEntryIsReadyWithoutInstances covers a managed-source
// deployment whose entry the Provider's catalog lists: no Allocation and no
// Instance whatever spec.instances says, desiredInstances 0, Progressing False
// with ManagedSource, Ready True.
func TestManagedSourceListedEntryIsReadyWithoutInstances(t *testing.T) {
	m := newManagedRig(t, managedProvider)
	m.create(managedMD(managedPlatform))
	m.settle()
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionTrue, "")
	managedExpect(t, m.rig, v1alpha1.ConditionProgressing, metav1.ConditionFalse, v1alpha1.ReasonManagedSource)
	if c := m.condition("md", v1alpha1.ConditionDegraded); c.Status == metav1.ConditionTrue {
		t.Errorf("Degraded = %+v, want it not true", c)
	}
	managedNothingProvisioned(t, m.rig)
}

// TestManagedSourceReadyFollowsCatalogListing covers "Ready is true while the
// catalog entry is listed, and false with CatalogEntryUnlisted once it leaves the
// listing": the Provider keeps the entry with listed false, and the deployment
// is Ready again when the entry comes back.
func TestManagedSourceReadyFollowsCatalogListing(t *testing.T) {
	m := newManagedRig(t, managedProvider)
	m.create(managedMD(managedPlatform))
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionTrue, "")
	m.gw.SetCatalog()
	m.settle()
	var p v1alpha1.Provider
	if err := m.c.Get(m.ctx, client.ObjectKey{Name: managedProvider}, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Status.Catalog) != 1 || p.Status.Catalog[0].Name != managedEntry || p.Status.Catalog[0].Listed {
		t.Fatalf("Provider catalog %+v, want the entry kept with listed false", p.Status.Catalog)
	}
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonCatalogEntryUnlisted)
	managedNothingProvisioned(t, m.rig)
	m.gw.SetCatalog(provider.CatalogEntry{Name: managedEntry})
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionTrue, "")
}

// TestManagedSourceCredentialsRejected covers "false with CredentialsRejected":
// the gateway refuses the key, so the Provider keeps its catalog (the entry still
// listed) and sets CredentialsRejected; the deployment is not Ready until the key
// is accepted again.
func TestManagedSourceCredentialsRejected(t *testing.T) {
	m := newManagedRig(t, managedProvider)
	m.create(managedMD(managedPlatform))
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionTrue, "")
	m.gw.RejectKey(true)
	m.settle()
	var p v1alpha1.Provider
	if err := m.c.Get(m.ctx, client.ObjectKey{Name: managedProvider}, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Status.Catalog) != 1 || !p.Status.Catalog[0].Listed {
		t.Fatalf("Provider catalog %+v, want the entry still listed: Ready must follow CredentialsRejected, not the listing", p.Status.Catalog)
	}
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonCredentialsRejected)
	managedNothingProvisioned(t, m.rig)
	m.gw.RejectKey(false)
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionTrue, "")
}

// TestManagedSourceProviderNotOnPlatform covers "ProviderNotOnPlatform when no
// allowed managed Provider carries the entry's platform label": the allowed
// Provider lists the entry but is on another platform.
func TestManagedSourceProviderNotOnPlatform(t *testing.T) {
	m := newManagedRig(t, managedProvider)
	m.create(managedMD("another-platform"))
	m.settle()
	m.settle()
	managedExpect(t, m.rig, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonProviderNotOnPlatform)
	managedExpect(t, m.rig, v1alpha1.ConditionProgressing, metav1.ConditionFalse, v1alpha1.ReasonManagedSource)
	managedNothingProvisioned(t, m.rig)
}

// TestManagedProviderIgnoredByServedDeployment covers "A served ModelDeployment
// whose selector matches a managed Provider's labels provisions on its batch
// Providers only, and sets no condition for the managed one". The managed
// Provider sorts first and is never synced, so it has no Ready condition: a
// controller that looked at it would name it in ProviderUnavailable.
func TestManagedProviderIgnoredByServedDeployment(t *testing.T) {
	m := newManagedRig(t, "a-llmlab")
	md := mdSpec("md")
	md.Spec.Providers = v1alpha1.ProviderSelection{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
		{Key: v1alpha1.LabelPlatform, Operator: metav1.LabelSelectorOpIn, Values: []string{"helios", managedPlatform}},
	}}}
	m.create(md)
	m.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return m.readyCount("md") == 1 })
	m.run(5*time.Minute, 10*time.Second)
	als := m.allocations("md")
	if len(als) != 1 || als[0].Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("Allocations %+v, want one, on %s", als, rigProvider)
	}
	for _, c := range m.md("md").Status.Conditions {
		if strings.Contains(c.Message, "a-llmlab") || (c.Type == v1alpha1.ConditionDegraded && c.Status == metav1.ConditionTrue) {
			t.Errorf("condition %s = %s/%s %q: nothing may be said of the managed Provider", c.Type, c.Status, c.Reason, c.Message)
		}
	}
}

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The managed Provider's controller in its own manager, as the per-Provider
// manager supervisor runs it (docs/development.md, Process layout): leader
// election on the Provider's Lease, the real API server's validation of every
// status write, and the field managers of the two writers.

// Issue #69's loopback port block for listeners other than the test host's.
const portLo, portHi = 29340, 29359

// vectorHash is the hash of one of HBEP-3's golden vectors, read from the
// committed sha256.txt that sha256sum wrote, not computed here.
func vectorHash(t *testing.T, vector string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "deploymentid", "testdata", "vectors", vector, "sha256.txt"))
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b))
	if len(f) != 2 || f[1] != "canonical.json" || len(f[0]) != 64 {
		t.Fatalf("%s/sha256.txt = %q: want a sha256sum line for canonical.json", vector, b)
	}
	return f[0]
}

type managedEnv struct {
	t      *testing.T
	name   string
	gw     *llmlabtest.Server
	clock  *testingclock.FakeClock
	cancel context.CancelFunc
	done   chan error
}

// startManaged creates a managed Provider on platform llmlab whose
// credentials name gw with key, and starts its manager with the controller:
// on clock, or, with clock nil, through managed.Setup, the supervisor's hook.
func startManaged(t *testing.T, name, key string, gw *llmlabtest.Server, clock *testingclock.FakeClock) *managedEnv {
	t.Helper()
	ctx := context.Background()
	creds, err := provider.ManagedCredentials{Key: key, BaseURL: gw.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, creds, 0o600); err != nil {
		t.Fatal(err)
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}},
		Spec: v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-llmlab"},
			CredentialsRef:     &v1alpha1.CredentialsReference{File: path},
		},
	}
	if err := c.Create(ctx, prov); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), prov) })

	mgr, err := manager.New(cfg, manager.Options{
		Scheme:                        scheme(),
		Metrics:                       metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:        "0",
		LeaderElection:                true,
		LeaderElectionID:              v1alpha1.ProviderLeaseName(name),
		LeaderElectionNamespace:       v1alpha1.ControllerNamespace,
		LeaderElectionResourceLock:    "leases",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if clock == nil {
		// The supervisor's hook, with the real clock.
		if err := managed.Setup(mgr, name); err != nil {
			t.Fatal(err)
		}
	} else {
		ctl, err := managed.New(managed.Options{
			Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Provider: name, Clock: clock,
			Recorder: mgr.GetEventRecorder(v1alpha1.FieldManagerProvider),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ctl.SetupWithManager(mgr); err != nil {
			t.Fatal(err)
		}
	}
	mctx, cancel := context.WithCancel(context.Background())
	e := &managedEnv{t: t, name: name, gw: gw, clock: clock, cancel: cancel, done: make(chan error, 1)}
	go func() { e.done <- mgr.Start(mctx) }()
	t.Cleanup(e.stop)
	return e
}

func (e *managedEnv) stop() {
	e.cancel()
	select {
	case err := <-e.done:
		if err != nil {
			e.t.Errorf("manager of %s: %v", e.name, err)
		}
	case <-time.After(30 * time.Second):
		e.t.Errorf("manager of %s did not stop within 30 s", e.name)
	}
	e.done <- nil // stop is idempotent
}

func (e *managedEnv) provider() *v1alpha1.Provider {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := c.Get(context.Background(), client.ObjectKey{Name: e.name}, &p); err != nil {
		e.t.Fatal(err)
	}
	return &p
}

// waitCatalog polls until the stored catalog satisfies ok, for at most 60 s
// of real time.
func (e *managedEnv) waitCatalog(what string, ok func(byName map[string]v1alpha1.CatalogEntryStatus) bool) *v1alpha1.Provider {
	e.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		p := e.provider()
		byName := map[string]v1alpha1.CatalogEntryStatus{}
		for _, ce := range p.Status.Catalog {
			byName[ce.Name] = ce
		}
		if ok(byName) {
			return p
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%s: catalog never got there within 60 s: %+v", what, p.Status.Catalog)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// nextMinute waits until the controller waits on the clock, then moves it a
// minute on.
func (e *managedEnv) nextMinute() {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !e.clock.HasWaiters() {
		if time.Now().After(deadline) {
			e.t.Fatal("the controller never waited on the clock")
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.clock.Step(managed.CatalogInterval)
}

func TestCatalogLoopInItsManager(t *testing.T) {
	key := "example-key-e2e-0001"
	gw := llmlabtest.Start(t, llmlabtest.Options{PortLo: portLo, PortHi: portHi, Keys: []string{key}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"}, llmlabtest.Model{ID: "example-9b"})
	clock := testingclock.NewFakeClock(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
	e := startManaged(t, "example-llmlab", key, gw, clock)

	// The first pass: both entries, and example-27b has HBEP-3's catalog-entry
	// golden vector's ID (platform llmlab, revision 2026-09).
	p := e.waitCatalog("first read", func(m map[string]v1alpha1.CatalogEntryStatus) bool {
		return m["example-27b"].Listed && m["example-9b"].Listed
	})
	if got, want := string(p.Status.Catalog[0].ManagedID), vectorHash(t, "02-managed-with-revision"); p.Status.Catalog[0].Name != "example-27b" || got != want {
		t.Fatalf("example-27b managedID = %s, want golden vector 02's %s", got, want)
	}
	if p.Status.LastCatalogRead == nil || !p.Status.LastCatalogRead.Time.Equal(clock.Now()) {
		t.Fatalf("lastCatalogRead = %v, want %v", p.Status.LastCatalogRead, clock.Now())
	}
	if r := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionProviderReady); r == nil || r.Status != metav1.ConditionTrue || r.Reason != v1alpha1.ReasonCatalogRead {
		t.Fatalf("Ready = %+v, want True CatalogRead", r)
	}

	// The lifecycle controller applies its condition between two passes.
	u, err := v1alpha1.DialHomeUnreachableApply(e.name, metav1.Condition{
		Status: metav1.ConditionTrue, Reason: "NoRegistration", Message: "example", LastTransitionTime: metav1.NewTime(clock.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Apply(context.Background(), client.ApplyConfigurationFromUnstructured(u),
		client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}

	// A removal and an add, read a minute later.
	gw.RemoveModel("example-9b")
	gw.AddModel(llmlabtest.Model{ID: "example-1b"})
	e.nextMinute()
	p = e.waitCatalog("second read", func(m map[string]v1alpha1.CatalogEntryStatus) bool {
		return len(m) == 3 && !m["example-9b"].Listed && m["example-1b"].Listed && m["example-27b"].Listed
	})
	if d := apimeta.FindStatusCondition(p.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable); d == nil || d.Reason != "NoRegistration" {
		t.Fatalf("DialHomeUnreachable = %+v, want it kept", d)
	}

	// The return of the removed entry, a minute later again.
	gw.AddModel(llmlabtest.Model{ID: "example-9b"})
	e.nextMinute()
	e.waitCatalog("third read", func(m map[string]v1alpha1.CatalogEntryStatus) bool {
		return len(m) == 3 && m["example-9b"].Listed && m["example-9b"].LastListedTime.Time.Equal(clock.Now())
	})
	if n := gw.Count("GET", "/v1/models"); n != 3 {
		t.Fatalf("the gateway saw %d catalog reads, want 3: one per minute", n)
	}

	// The status has two writers only, each under its own field manager.
	sawProvider := false
	for _, mf := range e.provider().ManagedFields {
		if mf.Subresource != "status" {
			continue
		}
		switch mf.Manager {
		case v1alpha1.FieldManagerProvider:
			sawProvider = true
		case v1alpha1.FieldManagerLifecycle:
		default:
			t.Fatalf("status written by field manager %q", mf.Manager)
		}
	}
	if !sawProvider {
		t.Fatalf("no status entry for %s in %+v", v1alpha1.FieldManagerProvider, e.provider().ManagedFields)
	}

	// The controller ran as the holder of the Provider's Lease.
	var lease coordinationv1.Lease
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(e.name)}, &lease); err != nil {
		t.Fatalf("the Provider's Lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		t.Fatalf("Lease %s has no holder", lease.Name)
	}
}

// Two managed Providers, each in its own manager with its own Lease, in one
// process, as the controller binary runs them.
func TestTwoManagedProvidersInOneProcess(t *testing.T) {
	gw := llmlabtest.Start(t, llmlabtest.Options{PortLo: portLo, PortHi: portHi, Keys: []string{"example-key-a", "example-key-b"}})
	gw.SetModels(llmlabtest.Model{ID: "example-ü-\U0001F600-27b"})
	gw.RejectKey("example-key-b", 403)
	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	a := startManaged(t, "example-llmlab-a", "example-key-a", gw, testingclock.NewFakeClock(start))
	b := startManaged(t, "example-llmlab-b", "example-key-b", gw, testingclock.NewFakeClock(start))

	p := a.waitCatalog("provider a", func(m map[string]v1alpha1.CatalogEntryStatus) bool { return len(m) == 1 })
	if got, want := string(p.Status.Catalog[0].ManagedID), vectorHash(t, "03-managed-no-revision-unicode"); got != want {
		t.Fatalf("managedID = %s, want golden vector 03's %s", got, want)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		r := apimeta.FindStatusCondition(b.provider().Status.Conditions, v1alpha1.ConditionCredentialsRejected)
		if r != nil && r.Status == metav1.ConditionTrue && r.Reason == v1alpha1.ReasonKeyRejected && strings.Contains(r.Message, "403") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider b: CredentialsRejected = %+v, want True KeyRejected naming 403", r)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(b.provider().Status.Catalog) != 0 {
		t.Fatalf("provider b has a catalog although its key is rejected: %+v", b.provider().Status.Catalog)
	}
	for _, name := range []string{a.name, b.name} {
		var lease coordinationv1.Lease
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(name)}, &lease); err != nil {
			t.Fatalf("Lease of %s: %v", name, err)
		}
	}
}

// The type of internal/providerctl's Supervisor.Setup: the
// assignment fails to compile if managed.Setup drifts from it.
var _ func(manager.Manager, string) error = managed.Setup

// managed.Setup has the signature of the per-Provider manager supervisor's
// hook (internal/providerctl, Supervisor.Setup, #59) and starts the loop on
// the real clock: the first pass reads at once.
func TestSetupIsTheSupervisorsHook(t *testing.T) {
	key := "example-key-e2e-hook"
	gw := llmlabtest.Start(t, llmlabtest.Options{PortLo: portLo, PortHi: portHi, Keys: []string{key}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"})
	e := startManaged(t, "example-llmlab-hook", key, gw, nil)
	p := e.waitCatalog("first read through Setup", func(m map[string]v1alpha1.CatalogEntryStatus) bool { return m["example-27b"].Listed })
	if got, want := string(p.Status.Catalog[0].ManagedID), vectorHash(t, "02-managed-with-revision"); got != want {
		t.Fatalf("managedID = %s, want golden vector 02's %s", got, want)
	}
	if n := gw.Count("GET", "/v1/models"); n != 1 {
		t.Fatalf("the gateway saw %d catalog reads, want 1", n)
	}
}

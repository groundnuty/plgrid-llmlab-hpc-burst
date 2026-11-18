package providerctl

// The supervisor's pass over a fake client, with NewManager stubs that record
// the Providers they are called for. The e2e tests (e2e/supervisor_test.go)
// cover real managers, Leases and restarts.

import (
	"context"
	"errors"
	"runtime/debug"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// supWorld is a fake client with PlatformProfiles and Providers.
func supWorld(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func supProfile(name string, kind v1alpha1.PlatformKind) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: kind}}
}

func supProvider(name, profile string) *v1alpha1.Provider {
	p := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}}
	p.Spec.PlatformProfileRef.Name = profile
	return p
}

// builds records the Providers NewManager was called for, and fails every
// build, so no manager starts and the run stays never-started.
type builds struct {
	mu    sync.Mutex
	names []string
}

func (b *builds) newManager(name string) (manager.Manager, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.names = append(b.names, name)
	return nil, errors.New("example: the manager cannot be built")
}

func (b *builds) got() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]string(nil), b.names...)
	sort.Strings(out)
	return out
}

func newTestSupervisor(c client.Client, kind v1alpha1.PlatformKind, b *builds) *Supervisor {
	s := &Supervisor{Kind: kind, Client: c, NewManager: b.newManager,
		Setup:      func(manager.Manager, string) error { return nil },
		MinBackoff: time.Hour, MaxBackoff: time.Hour, Interval: time.Hour, Log: logging.NewNopLogger(),
	}
	s.runs = map[string]*run{}
	return s
}

// R3 / Astra B1: a Provider whose manager was never built (NewManager failed,
// so its run has no cancel and no done) is removed; the pass must stop it
// without a nil dereference, which would panic the supervisor every other
// Provider shares. The other Provider's run is kept.
func TestSupervisorStopsANeverStartedRunWithoutPanic(t *testing.T) {
	c := supWorld(t, supProfile("pp", v1alpha1.PlatformBatch), supProvider("p1", "pp"), supProvider("p2", "pp"))
	b := &builds{}
	s := newTestSupervisor(c, "", b)
	ctx := context.Background()
	if err := s.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.got(); len(got) != 2 {
		t.Fatalf("precondition: NewManager called for %v, want p1 and p2", got)
	}
	if err := c.Delete(ctx, supProvider("p1", "pp")); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("pass panicked after the never-started Provider p1 was removed: %v\n%s", p, debug.Stack())
			}
		}()
		if err := s.pass(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	st := s.Statuses()
	if len(st) != 1 || st[0].Provider != "p2" {
		t.Errorf("statuses %+v, want only p2 kept", st)
	}
}

// #256: Dropped is called once for a Provider the supervisor no longer runs,
// after its run is stopped, and never for a Provider whose manager is started
// again after a failure: the managed supervisor deletes the Provider's
// raised-events series with it, which must not happen while the Provider is
// still one to run.
func TestSupervisorCallsDroppedOnceForAProviderItNoLongerRuns(t *testing.T) {
	c := supWorld(t, supProfile("pp", v1alpha1.PlatformBatch), supProvider("p1", "pp"), supProvider("p2", "pp"))
	b := &builds{}
	s := newTestSupervisor(c, "", b)
	s.MinBackoff, s.MaxBackoff = time.Nanosecond, time.Nanosecond // every failed build is started again at the next pass
	var dropped []string
	s.Dropped = func(name string) { dropped = append(dropped, name) }
	ctx := context.Background()
	for range 3 {
		if err := s.pass(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.got(); len(got) != 6 {
		t.Fatalf("precondition: NewManager called %d times (%v), want 6: each Provider started again after each failure", len(got), got)
	}
	if len(dropped) != 0 {
		t.Fatalf("Dropped called for %v while both Providers are still to run", dropped)
	}
	if err := c.Delete(ctx, supProvider("p1", "pp")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.pass(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(dropped) != 1 || dropped[0] != "p1" {
		t.Errorf("Dropped called for %v, want once for p1", dropped)
	}
}

// The parent's addition for #69: Kind picks the providerKind the supervisor
// runs; zero means batch, so existing callers are unchanged. A managed-kind
// supervisor starts a manager for the managed Provider only, a batch one (and
// the default) for the batch Provider only.
func TestSupervisorKindSelectsProviders(t *testing.T) {
	objs := []client.Object{
		supProfile("pp-batch", v1alpha1.PlatformBatch), supProfile("pp-managed", v1alpha1.PlatformManaged),
		supProvider("prov-batch", "pp-batch"), supProvider("prov-managed", "pp-managed"),
	}
	for _, tc := range []struct {
		name string
		kind v1alpha1.PlatformKind
		want string
	}{
		{"default", "", "prov-batch"},
		{"batch", v1alpha1.PlatformBatch, "prov-batch"},
		{"managed", v1alpha1.PlatformManaged, "prov-managed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &builds{}
			s := newTestSupervisor(supWorld(t, objs...), tc.kind, b)
			if err := s.pass(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := b.got(); len(got) != 1 || got[0] != tc.want {
				t.Errorf("NewManager called for %v, want only %s", got, tc.want)
			}
		})
	}
}

// barrier makes a group of fake managers whose Start, once its context ends,
// returns only after every manager of the group has seen its own context end.
// A supervisor that stopped them one after another would wait on the first
// for ever (review of PR 132: N3).
type barrier struct {
	n       int
	started chan struct{}
	all     chan struct{} // closed once all n contexts have ended
	release chan struct{} // closed by the test's cleanup, so a failed run leaves no goroutine
	mu      sync.Mutex
	ended   int
}

// barrierManager is a manager.Manager of which the supervisor calls only Start.
type barrierManager struct {
	manager.Manager
	g *barrier
}

func (m barrierManager) Start(ctx context.Context) error {
	m.g.started <- struct{}{}
	<-ctx.Done()
	m.g.mu.Lock()
	m.g.ended++
	if m.g.ended == m.g.n {
		close(m.g.all)
	}
	m.g.mu.Unlock()
	select {
	case <-m.g.all:
		return nil
	case <-m.g.release:
		return errors.New("example: not every manager was stopped")
	}
}

// The supervisor stops its managers at once: each waits until all have been
// told to stop, so only a stop that cancels them all before it waits for any
// returns. The supervisor's stop then lasts one manager's grace, not one per
// Provider. Managers whose contexts did not derive from Start's would be
// stopped one after another by stopAll, and fail this.
func TestSupervisorStopsEveryManagerAtOnce(t *testing.T) {
	const n = 3
	c := supWorld(t, supProfile("pp", v1alpha1.PlatformBatch), supProvider("p1", "pp"), supProvider("p2", "pp"), supProvider("p3", "pp"))
	g := &barrier{n: n, started: make(chan struct{}, n), all: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(g.release) })
	s := &Supervisor{Client: c,
		NewManager: func(string) (manager.Manager, error) { return barrierManager{g: g}, nil },
		Setup:      func(manager.Manager, string) error { return nil },
		Interval:   time.Hour, MinBackoff: time.Hour, MaxBackoff: time.Hour, Log: logging.NewNopLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	for i := range n {
		select {
		case <-g.started:
		case <-time.After(30 * time.Second):
			t.Fatalf("%d of %d managers started within 30 s", i, n)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v after a clean stop", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the supervisor did not stop within 30 s: it waits for one manager before it stops the next")
	}
}

// orderedMgr is a manager that records when it starts and stops.
type orderedMgr struct {
	manager.Manager
	log  func(string)
	name string
}

func (m orderedMgr) Start(ctx context.Context) error {
	m.log("start " + m.name)
	<-ctx.Done()
	m.log("stop " + m.name)
	return nil
}

// The order the durable log's event counter relies on (PR E2, #625): one supervisor never has two managers of
// one Provider name. A manager that is no longer wanted is stopped, and waited for, and its Dropped hook (the
// counter's Forget) runs before a manager for a Provider of the same name is set up again, so the registration
// of the one is gone when the next one's begins. A change to pass that started a new manager before the old one
// had stopped, or set it up before Dropped, would let Forget retire the route and the series under the live one.
func TestSupervisorNeverRunsTwoManagersOfOneNameAtOnce(t *testing.T) {
	c := supWorld(t, supProfile("pp", v1alpha1.PlatformBatch), supProvider("p1", "pp"))
	var mu sync.Mutex
	var log []string
	add := func(s string) { mu.Lock(); log = append(log, s); mu.Unlock() }
	got := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), log...) }
	s := newTestSupervisor(c, "", &builds{})
	s.NewManager = func(name string) (manager.Manager, error) { return orderedMgr{log: add, name: name}, nil }
	s.Setup = func(_ manager.Manager, name string) error { add("setup " + name); return nil }
	s.Dropped = func(name string) { add("dropped " + name) }
	ctx := context.Background()
	waitFor := func(n int) {
		t.Helper()
		for end := time.Now().Add(10 * time.Second); len(got()) < n; time.Sleep(time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("log %v, want %d entries", got(), n)
			}
		}
	}
	if err := s.pass(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(2) // setup, start
	if err := c.Delete(ctx, supProvider("p1", "pp")); err != nil {
		t.Fatal(err)
	}
	if err := s.pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, supProvider("p1", "pp")); err != nil {
		t.Fatal(err)
	}
	if err := s.pass(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(6)
	want := []string{"setup p1", "start p1", "stop p1", "dropped p1", "setup p1", "start p1"}
	if g := got(); len(g) != len(want) || g[0] != want[0] || g[1] != want[1] || g[2] != want[2] || g[3] != want[3] || g[4] != want[4] || g[5] != want[5] {
		t.Errorf("the order was %v, want %v: the old manager stops, then Dropped, then the new one is set up", g, want)
	}
	s.stopAll()
}

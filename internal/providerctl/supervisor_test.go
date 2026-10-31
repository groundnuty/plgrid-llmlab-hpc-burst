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

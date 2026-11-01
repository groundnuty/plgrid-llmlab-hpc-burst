package e2e

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
)

// tracker counts, per Provider, how often its manager's leader-only runnable
// started and stopped.
type tracker struct {
	mu            sync.Mutex
	starts, stops map[string]int
	running       map[string]bool
	startedAt     map[string][]time.Time
}

func newTracker() *tracker {
	return &tracker{starts: map[string]int{}, stops: map[string]int{}, running: map[string]bool{}, startedAt: map[string][]time.Time{}}
}

// startTimes is when each start of the Provider's runnable began.
func (t *tracker) startTimes(p string) []time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Time(nil), t.startedAt[p]...)
}

func (t *tracker) get(p string) (starts, stops int, running bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.starts[p], t.stops[p], t.running[p]
}

// probe is a leader-only runnable: it runs while its manager leads, and fails
// at once when fail is set.
type probe struct {
	t    *tracker
	name string
	fail bool
}

func (p probe) NeedLeaderElection() bool { return true }

func (p probe) Start(ctx context.Context) error {
	p.t.mu.Lock()
	p.t.starts[p.name]++
	p.t.running[p.name] = true
	p.t.startedAt[p.name] = append(p.t.startedAt[p.name], time.Now())
	p.t.mu.Unlock()
	defer func() {
		p.t.mu.Lock()
		p.t.stops[p.name]++
		p.t.running[p.name] = false
		p.t.mu.Unlock()
	}()
	if p.fail {
		return errors.New("scripted failure of this Provider's manager")
	}
	<-ctx.Done()
	return nil
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting until %s", timeout, what)
}

func createProfile(t *testing.T, name string, kind v1alpha1.PlatformKind) {
	t.Helper()
	pp := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: kind}}
	if err := c.Create(context.Background(), pp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), pp) })
}

func createProvider(t *testing.T, name, profile string) *v1alpha1.Provider {
	t.Helper()
	p := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: profile},
	}}
	if err := c.Create(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), p) })
	return p
}

func lease(t *testing.T, provider string) (*coordinationv1.Lease, bool) {
	t.Helper()
	l := &coordinationv1.Lease{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(provider)}, l)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return l, true
}

// The supervisor starts one manager per batch Provider, each leading through
// its own Lease provider-<name> in hpcburst-system; a Provider whose manager
// keeps failing is restarted with backoff and stops no other; a deleted
// Provider's manager stops and releases its Lease; a managed Provider gets no
// manager (docs/development.md, Process layout; HBEP-11, Controllers).
func TestSupervisor(t *testing.T) {
	createProfile(t, "sup-batch", v1alpha1.PlatformBatch)
	createProfile(t, "sup-managed", v1alpha1.PlatformManaged)
	good := createProvider(t, "sup-good", "sup-batch")
	createProvider(t, "sup-broken", "sup-batch")
	createProvider(t, "sup-catalog", "sup-managed")

	tr := newTracker()
	sup := &providerctl.Supervisor{
		Client: c,
		NewManager: func(p string) (manager.Manager, error) {
			return providerctl.NewProviderManager(cfg, scheme(), p, providerctl.ManagerOptions{
				LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond,
			})
		},
		Setup: func(mgr manager.Manager, p string) error {
			return mgr.Add(probe{t: tr, name: p, fail: p == "sup-broken"})
		},
		Interval: 200 * time.Millisecond, MinBackoff: 300 * time.Millisecond, MaxBackoff: 2 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Start(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the supervisor did not stop")
		}
	}()

	eventually(t, "sup-good's manager leads and runs", 30*time.Second, func() bool {
		_, _, running := tr.get("sup-good")
		return running
	})
	eventually(t, "sup-broken's manager was started at least three times", 30*time.Second, func() bool {
		s, _, _ := tr.get("sup-broken")
		return s >= 3
	})
	starts, stops, running := tr.get("sup-good")
	if starts != 1 || stops != 0 || !running {
		t.Errorf("sup-good: %d starts, %d stops, running %v while sup-broken failed; want 1, 0, true", starts, stops, running)
	}
	if s, _, _ := tr.get("sup-catalog"); s != 0 {
		t.Errorf("the managed Provider's manager started %d times; want none", s)
	}
	if _, ok := lease(t, "sup-catalog"); ok {
		t.Error("the managed Provider has a Lease")
	}
	l, ok := lease(t, "sup-good")
	if !ok || l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
		t.Fatalf("sup-good's Lease %s in %s is missing or held by nobody", v1alpha1.ProviderLeaseName("sup-good"), v1alpha1.ControllerNamespace)
	}
	// Backoff: the restarts of sup-broken are spaced, not a hot loop. Start
	// n+1 comes no sooner than failure n plus the n-th backoff (300 ms, then
	// doubled up to 2 s), and failure n comes after start n, so each gap
	// between recorded starts is at least that backoff; a loaded machine only
	// widens it. (The test used to count the restarts in one second of sleep.)
	times := tr.startTimes("sup-broken")
	for n := 1; n < len(times); n++ {
		want := min(sup.MinBackoff<<(n-1), sup.MaxBackoff)
		if gap := times[n].Sub(times[n-1]); gap < want {
			t.Errorf("sup-broken's start %d came %v after start %d; the backoff after %d failures is %v", n+1, gap, n, n, want)
		}
	}

	if err := c.Delete(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	eventually(t, "sup-good's manager stopped", 30*time.Second, func() bool {
		_, stops, running := tr.get("sup-good")
		return stops == 1 && !running
	})
	eventually(t, "sup-good's Lease is released", 30*time.Second, func() bool {
		l, ok := lease(t, "sup-good")
		return ok && (l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "")
	})
}

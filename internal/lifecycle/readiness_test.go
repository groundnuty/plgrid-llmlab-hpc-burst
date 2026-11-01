package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// The two readiness checks' names are read by `hpcburst status` (internal/launcher). Both sides
// state them; this is the one place that sees both.
func TestReadyzCheckNamesAreTheLaunchers(t *testing.T) {
	if ReadyzCaches != launcher.ReadyzCaches || ReadyzLeading != launcher.ReadyzLeading {
		t.Fatalf("the controller registers %q and %q, the launcher asks for %q and %q",
			ReadyzCaches, ReadyzLeading, launcher.ReadyzCaches, launcher.ReadyzLeading)
	}
}

// stubCache stands in for the manager's cache in the tests of readiness's own logic (the checks
// on a real API server are in TestManagerOnControlPlane). Only the two methods readiness calls
// are defined; any other call panics on the nil embedded Cache, which would fail the test.
type stubCache struct {
	cache.Cache
	mu        sync.Mutex
	asked     []string
	informer  func(ctx context.Context, n int) error // the nth GetInformer call (1-based)
	syncReply func(ctx context.Context) bool
	calls     atomic.Int64
	inSync    atomic.Bool // WaitForCacheSync has been called
}

func (c *stubCache) GetInformer(ctx context.Context, obj client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	c.mu.Lock()
	c.asked = append(c.asked, fmt.Sprintf("%T", obj))
	c.mu.Unlock()
	n := int(c.calls.Add(1))
	if c.informer != nil {
		return nil, c.informer(ctx, n)
	}
	return nil, nil
}

func (c *stubCache) WaitForCacheSync(ctx context.Context) bool {
	c.inSync.Store(true)
	if c.syncReply != nil {
		return c.syncReply(ctx)
	}
	return true
}

func (c *stubCache) kinds() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.asked)
}

// untilTrue waits for cond with a generous deadline: the tests below wait on state, never a window.
func untilTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Minute); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// startReadiness runs r.Start and returns a stop that cancels it and waits once, bounded.
func startReadiness(t *testing.T, r *readiness) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Start returned %v on a stop; a stop is not a failure of the manager", err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("Start did not return within 10 s of its cancel; goroutines:\n%s", goroutines())
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// TestReadinessAsksForEveryWatchedKind: "caches" starts the informers of the kinds the controller
// watches before the controller is elected, ModelDeployment among them. The expected list is the
// controller's, written out here from the Watches of NewManager's builder, not read from the code
// under test.
func TestReadinessAsksForEveryWatchedKind(t *testing.T) {
	want := []string{
		"*v1alpha1.ModelDeployment", "*v1alpha1.Allocation", "*v1alpha1.Instance", "*v1alpha1.Provider",
		"*v1alpha1.PlatformProfile", "*v1.ControllerRevision",
	}
	kinds := []client.Object{}
	for _, w := range lifecycleWatches(nil) {
		kinds = append(kinds, w.obj)
	}
	if len(kinds)+1 != len(want) {
		t.Fatalf("%d watched kinds beside ModelDeployment, want %d", len(kinds), len(want)-1)
	}
	c := &stubCache{}
	r := &readiness{cache: c, elected: make(chan struct{}), leaderElection: true, kinds: append([]client.Object{&v1alpha1.ModelDeployment{}}, kinds...)}
	startReadiness(t, r)
	untilTrue(t, "the caches to count as synced", r.synced.Load)
	if got := c.kinds(); !slices.Equal(got, want) {
		t.Fatalf("informers asked for: %v\nwant: %v", got, want)
	}
}

// TestReadinessChecks: the two checks on their own, with the Elected channel open and closed.
// Without leader election "leading" passes with the channel open (R3 of #147).
func TestReadinessChecks(t *testing.T) {
	elected := make(chan struct{})
	le := &readiness{elected: elected, leaderElection: true}
	if le.checkCaches() == nil || le.checkLeading() == nil || le.serving() {
		t.Fatal("before the caches synced and the Lease is held, both checks fail and the controller does not serve")
	}
	le.synced.Store(true)
	if le.checkCaches() != nil || le.checkLeading() == nil || le.serving() {
		t.Fatal("synced but not elected: caches passes, leading fails, and the controller does not serve")
	}
	close(elected)
	if le.checkCaches() != nil || le.checkLeading() != nil || !le.serving() {
		t.Fatal("synced and elected: both pass and the controller serves")
	}
	le.synced.Store(false)
	if le.checkCaches() == nil || le.checkLeading() != nil || le.serving() {
		t.Fatal("elected but not synced: caches fails, leading passes, and the controller does not serve")
	}

	off := &readiness{elected: make(chan struct{}), leaderElection: false} // Elected never closes here
	if off.checkLeading() != nil {
		t.Error("without leader election, leading always passes")
	}
	if off.checkCaches() == nil || off.serving() {
		t.Error("without leader election the caches still count, and the controller does not serve before it is elected")
	}
}

// TestReadinessSyncedThenStopped: "caches" turns true once the informers have synced and false
// when the manager stops.
func TestReadinessSyncedThenStopped(t *testing.T) {
	r := &readiness{cache: &stubCache{}, elected: make(chan struct{}), kinds: []client.Object{&v1alpha1.ModelDeployment{}}}
	stop := startReadiness(t, r)
	untilTrue(t, "synced", r.synced.Load)
	stop()
	if r.synced.Load() || r.checkCaches() == nil {
		t.Fatal("the caches still count as synced after the stop")
	}
}

// TestReadinessKeepsTryingAnInformerTheAPICannotServe: a kind whose informer cannot start yet (a
// CRD that is not installed) leaves "caches" failing and is tried again; the check passes once it
// starts. Controller-runtime's own Kind source retries the same way.
func TestReadinessKeepsTryingAnInformerTheAPICannotServe(t *testing.T) {
	c := &stubCache{informer: func(_ context.Context, n int) error {
		if n <= 3 {
			return errors.New("no matches for kind")
		}
		return nil
	}}
	r := &readiness{cache: c, elected: make(chan struct{}), kinds: []client.Object{&v1alpha1.ModelDeployment{}}, retry: time.Millisecond}
	startReadiness(t, r)
	untilTrue(t, "the fourth try to be made", func() bool { return c.calls.Load() >= 4 })
	untilTrue(t, "synced after the informer started", r.synced.Load)
}

// TestReadinessStopsWhereverItWaits: a cancel returns Start with nil, whether it is blocked in an
// informer that never syncs, in the wait between two tries, or in the final cache sync. 200 rounds
// each, as for every runnable that a manager stops (IMPL-COMMON, Controller work queues): a stop
// that hangs holds the supervisor's restart.
func TestReadinessStopsWhereverItWaits(t *testing.T) {
	const rounds = 200
	blockForever := func(ctx context.Context, _ int) error { <-ctx.Done(); return ctx.Err() }
	failing := func(context.Context, int) error { return errors.New("no matches for kind") }
	for _, c := range []struct {
		name  string
		cache func() *stubCache
		retry time.Duration
		waits func(*stubCache) bool // true once Start is waiting where the case says
	}{
		{"an informer that never syncs", func() *stubCache { return &stubCache{informer: blockForever} }, 0, func(c *stubCache) bool { return c.calls.Load() == 1 }},
		{"the wait between tries", func() *stubCache { return &stubCache{informer: failing} }, time.Hour, func(c *stubCache) bool { return c.calls.Load() == 1 }},
		{"the final cache sync", func() *stubCache {
			return &stubCache{syncReply: func(ctx context.Context) bool { <-ctx.Done(); return false }}
		}, 0, func(c *stubCache) bool { return c.inSync.Load() }},
	} {
		t.Run(c.name, func(t *testing.T) {
			for round := 0; round < rounds && !t.Failed(); round++ {
				sc := c.cache()
				r := &readiness{cache: sc, elected: make(chan struct{}), kinds: []client.Object{&v1alpha1.ModelDeployment{}}, retry: c.retry}
				stop := startReadiness(t, r)
				untilTrue(t, "Start to reach the wait", func() bool { return c.waits(sc) })
				stop()
				if r.synced.Load() {
					t.Fatalf("round %d: synced after a stop in %s", round, c.name)
				}
			}
		})
	}
}

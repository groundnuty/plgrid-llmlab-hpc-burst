package providerctl

import (
	"context"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// TestAllocationControllerStopsWithWorkInFlight: a cancelled Allocation
// controller stops while it has work in flight, so a Provider's manager stops
// when told (hpcburst down, the supervisor's restart). The controller is
// built with Setup's own options (controllerOptions) and the default four
// workers, and has two keys that are added again while their reconciles run
// (an Allocation's status write does that). Each round cancels only once both
// keys' reconciles are inside the reconciler, held there, so the other two
// workers wait in the queue; the held reconciles then finish after the
// cancel. In that state controller-runtime v0.23.1's priority queue, its
// default, can deadlock: the second worker to finish waits in Done forever
// (issue #136; see controllerOptions). Every one of stopRounds starts must
// stop within stopLimit, far more than a stop takes; one that does not fails
// the test with every goroutine's stack.
func TestAllocationControllerStopsWithWorkInFlight(t *testing.T) {
	const stopRounds = 200
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		stopWithWorkInFlight(t, round, controllerOptions(DefaultMaxConcurrentReconciles))
	}
}

// stopWithWorkInFlight runs one round of
// TestAllocationControllerStopsWithWorkInFlight. Its deferred stop runs on
// every path: it cancels, releases the held reconciles and waits once,
// bounded, for the controller.
func stopWithWorkInFlight(t *testing.T, round int, o controller.Options) {
	t.Helper()
	const (
		stopLimit  = 10 * time.Second
		enterLimit = time.Minute
		// inFlight is how many reconciles are held inside at the cancel: one
		// per key, the most the queue hands out at once.
		inFlight = 2
	)
	keys := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "alloc-a"}},
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "alloc-b"}},
	}
	var inside atomic.Int64
	release := make(chan struct{})
	o.Reconciler = reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		inside.Add(1)
		<-release
		inside.Add(-1)
		return reconcile.Result{}, nil
	})
	c, err := controller.NewUnmanaged("allocation-stop", o)
	if err != nil {
		t.Fatal(err)
	}
	// The keys are added again every 100 microseconds until the cancel, so
	// each is ready again while its reconcile is held.
	busy := source.Func(func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		go func() {
			for ctx.Err() == nil {
				for _, k := range keys {
					q.Add(k)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
		return nil
	})
	if err := c.Watch(busy); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- c.Start(ctx) }()
	defer func() {
		cancel()
		close(release)
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("round %d: the controller returned %v", round, err)
			}
		case <-time.After(stopLimit):
			var b strings.Builder
			_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
			t.Errorf("round %d: the controller did not stop within %s of its cancel; goroutines:\n%s", round, stopLimit, b.String())
		}
	}()
	deadline := time.Now().Add(enterLimit)
	for inside.Load() < inFlight {
		if time.Now().After(deadline) {
			t.Fatalf("round %d: %d reconciles inside within %s, want %d", round, inside.Load(), enterLimit, inFlight)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

package lifecycle

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

// goroutines is the stack of every goroutine, the form of a test binary's
// time-out dump (debug 2).
func goroutines() string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return b.String()
}

// TestControllerStopsWithWorkInFlight: a cancelled lifecycle controller stops
// while it has work in flight. The controller is built with the lifecycle's
// own queue options (controllerOptions) and, like TestManagerOnControlPlane's
// rig, has two keys for its four workers: two workers wait in the queue while
// the other two reconcile, and each key is added again while its reconcile
// runs (a status write's watch event does that). In that state
// controller-runtime v0.23.1's priority queue, its default, can deadlock on
// the cancel: it hands a key to a worker that has already left, blocks there
// holding its locks, and a worker that then finishes its reconcile waits in
// Done forever (issue #136; see controllerOptions). Every one of stopRounds
// starts must stop within stopLimit, far more than a stop takes; one that
// does not fails the test with every goroutine's stack.
func TestControllerStopsWithWorkInFlight(t *testing.T) {
	const (
		stopRounds = 200
		stopLimit  = 10 * time.Second
		// reconciles is how much work each round does before its cancel.
		reconciles = 20
	)
	keys := []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "md"}},
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "md-collide"}},
	}
	for round := 0; round < stopRounds; round++ {
		var done atomic.Int64
		o := controllerOptions(DefaultWorkers)
		skip := true
		o.SkipNameValidation = &skip // one name for every round's controller
		// A reconcile takes about a millisecond, as one with API calls does,
		// so the cancel finds workers inside it.
		o.Reconciler = reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			time.Sleep(time.Millisecond)
			done.Add(1)
			return reconcile.Result{}, nil
		})
		c, err := controller.NewUnmanaged("lifecycle-stop", o)
		if err != nil {
			t.Fatal(err)
		}
		// The keys are added again every 100 microseconds until the cancel.
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
		deadline := time.Now().Add(time.Minute)
		for done.Load() < reconciles {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("round %d: %d reconciles within a minute, want %d", round, done.Load(), reconciles)
			}
			time.Sleep(time.Millisecond)
		}
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Fatalf("round %d: the controller returned %v", round, err)
			}
		case <-time.After(stopLimit):
			t.Fatalf("round %d: the controller did not stop within %s of its cancel; goroutines:\n%s", round, stopLimit, goroutines())
		}
	}
}

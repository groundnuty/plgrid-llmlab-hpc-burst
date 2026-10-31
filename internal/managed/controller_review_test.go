package managed

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Tests from the review of PR #85: each is a reviewer's counterexample,
// inverted, and fails without its fix.

// secondController is a new Controller on the env's Provider, as a restarted
// process or a new leader builds it.
func (e *env) secondController(opts ...func(*Options)) *Controller {
	e.t.Helper()
	o := Options{Client: e.c, Provider: testProvider, Clock: e.clock}
	for _, f := range opts {
		f(&o)
	}
	c, err := New(o)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// failStatusUpdate makes status update number n (from 1) fail with a
// non-conflict error.
func failStatusUpdate(n int32) func(*clientfake.ClientBuilder) *clientfake.ClientBuilder {
	var updates atomic.Int32
	return func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
		return b.WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if updates.Add(1) == n {
					return errors.New("example: the API server refused the write")
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		})
	}
}

// Astra B3, Opus B1: no restart lets a read follow another within a minute,
// whatever became of the first: an unknown answer, a refused result write, or
// a process that died between the GET and its write. The read is reserved in
// the status before it is made.
func TestNoRestartReadsTwiceWithinAMinute(t *testing.T) {
	// restart checks that a second Controller makes no read within the minute
	// of the read at 12:00:00, and reads once the minute is out.
	restart := func(t *testing.T, e *env, wantGets int) {
		t.Helper()
		if n := e.gets(); n != wantGets {
			t.Fatalf("before the restart: %d reads, want %d", n, wantGets)
		}
		second := e.secondController()
		for _, when := range []time.Time{at(12, 0, 5), at(12, 0, 59), at(12, 1, 0)} {
			e.clock.Set(when)
			if _, err := second.Sync(context.Background()); err != nil {
				t.Fatalf("second Controller at %s: %v", when.Format("15:04:05"), err)
			}
			if n := e.gets(); n != wantGets {
				t.Fatalf("second Controller at %s made a read (%d, want %d): within the minute of the last", when.Format("15:04:05"), n, wantGets)
			}
		}
		e.clock.Set(at(12, 1, 1)) // the stored reservation, 12:00:00, plus a second and a minute
		if _, err := second.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := e.gets(); n != wantGets+1 {
			t.Fatalf("second Controller at 12:01:01: %d reads, want %d", n, wantGets+1)
		}
	}

	t.Run("after an unknown answer", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")))
		e.syncAt(at(11, 58, 0))
		undo := failReads(e.gw)
		e.syncAt(at(12, 0, 0)) // a 504
		undo()
		restart(t, e, 2)
	})

	t.Run("after the result's write was refused", func(t *testing.T) {
		// Update 1 is 11:58's reservation, 2 its result, 3 12:00's
		// reservation, 4 12:00's result.
		e := newEnv(t, withModels(model("example-a")), withBuilder(failStatusUpdate(4)))
		e.syncAt(at(11, 58, 0))
		if _, err := e.trySyncAt(at(12, 0, 0)); err == nil {
			t.Fatal("Sync = nil, want the refused write's error")
		}
		restart(t, e, 2)
	})

	t.Run("after a process died between the GET and its write", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")))
		e.syncAt(at(11, 58, 0))
		// The process dies once the gateway has answered: the goroutine
		// running the pass ends there, and nothing after the GET runs.
		dying := e.secondController(func(o *Options) { o.Transport = dieAfter{base: newTransport()} })
		e.clock.Set(at(12, 0, 0))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = dying.Sync(context.Background()) }()
		wg.Wait()
		if !e.provider().Status.LastCatalogRead.Time.Equal(at(11, 58, 0)) {
			t.Fatal("the dying pass wrote its result")
		}
		restart(t, e, 2)
	})

	t.Run("a reservation that cannot be stored makes no read", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")), withBuilder(failStatusUpdate(1)))
		if _, err := e.trySyncAt(at(12, 0, 0)); err == nil || !strings.Contains(err.Error(), "reserve") {
			t.Fatalf("Sync = %v, want the reservation's error", err)
		}
		if n := e.gets(); n != 0 {
			t.Fatalf("%d reads, want 0: no read without a stored reservation", n)
		}
	})
}

// dieAfter makes the request, then ends the calling goroutine, as a process
// that dies with the answer in hand.
type dieAfter struct{ base http.RoundTripper }

func (d dieAfter) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := d.base.RoundTrip(r)
	if err == nil {
		_ = resp.Body.Close()
	}
	runtime.Goexit()
	return nil, errors.New("unreachable")
}

// Opus nit 10: a stored time in the future, as after a clock stepped back,
// is looked at again within a minute, not slept out.
func TestWaitIsAtMostAMinute(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.storeStatus(func(s *v1alpha1.ProviderStatus) { s.LastReconcileTime = mtOrNil(ptr(at(13, 0, 0))) })
	if wait := e.syncAt(at(12, 0, 0)); wait != time.Minute {
		t.Fatalf("Sync waited %v, want 1m: capped", wait)
	}
	if n := e.gets(); n != 0 {
		t.Fatalf("%d reads, want 0", n)
	}
}

// Opus nit 9: a Provider whose PlatformProfile is batch, or missing, is
// refused before anything is read, sent or written.
func TestProfileMustBeManaged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*env)
		want  string
	}{
		{"a batch profile", func(e *env) { e.setProfileKind(v1alpha1.PlatformBatch) }, `providerKind "batch"`},
		{"no profile", func(e *env) {
			var pp v1alpha1.PlatformProfile
			if err := e.c.Get(context.Background(), client.ObjectKey{Name: "example-profile"}, &pp); err != nil {
				e.t.Fatal(err)
			}
			if err := e.c.Delete(context.Background(), &pp); err != nil {
				e.t.Fatal(err)
			}
		}, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newCountingTransport()
			e := newEnv(t, withModels(model("example-a")), withTransport(rt))
			tc.setup(e)
			before := e.provider()
			if _, err := e.trySyncAt(at(12, 0, 0)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Sync = %v, want an error holding %q", err, tc.want)
			}
			if rt.count() != 0 || e.provider().ResourceVersion != before.ResourceVersion {
				t.Fatalf("%d requests sent, written %v: want none", rt.count(), e.provider().ResourceVersion != before.ResourceVersion)
			}
		})
	}
}

// The science agent's ruling of 31 Oct 2026: a managed Provider follows the
// batch staleness rule. With at most one read a minute, a gateway that starts
// failing gives six failed reads before Ready turns False, at 12:06, more than
// five minutes after the last success.
func TestReadyFollowsTheStalenessRule(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.syncAt(at(12, 0, 0))
	ready := func(when time.Time) bool { return v1alpha1.ProviderReady(e.provider(), when) }
	if !ready(at(12, 0, 0)) {
		t.Fatal("not ready after a successful read")
	}
	undo := failReads(e.gw)
	for m := 1; m <= 6; m++ {
		when := at(12, m, 0)
		e.syncAt(when)
		wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 0, 0))
		if m <= 5 {
			e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
			if !ready(when) {
				t.Fatalf("ProviderReady false at 12:0%d, %d min after the last success", m, m)
			}
			continue
		}
		r := e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionFalse, "ReadsFailing")
		if r.Message != "reads are failing: the last successful read was at 2026-10-31T12:00:00Z" {
			t.Fatalf("Ready's message = %q", r.Message)
		}
		if ready(when) {
			t.Fatal("ProviderReady true 6 min after the last success")
		}
	}
	undo()
	if n := e.gets(); n != 7 {
		t.Fatalf("%d reads, want 7: one success and six failures", n)
	}
	e.syncAt(at(12, 7, 0))
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
	if !ready(at(12, 7, 0)) {
		t.Fatal("not ready again after a successful read")
	}
}

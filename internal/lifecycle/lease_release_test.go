package lifecycle

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock/leasefault"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// TestLifecycleManagerReleasesItsLeaseOnEveryStop (issues #131 and #160;
// IMPL-COMMON, Controller work queues): stopRounds lifecycle managers, built
// as the controller builds them (NewManager with leader election), each take
// the lifecycle Lease, are stopped, and must return within stopLimit with the
// Lease free when Start returns, read with no wait. In every odd round the
// elector's release read fails (leasefault.ReleaseRead: the one read of the
// Lease whose context does not carry Start's values), so client-go's release
// returns before its write and the Lease is free only through the release
// after Start (leaderlock.ReleaseAfterStart); each such round checks that the
// fault fired, on that read alone. In every even round the elector releases
// the Lease itself. Each round takes the Lease the previous one released at
// once; after a lost release it would wait until the Lease expired by its own
// clock, controller-runtime's 15 s.
//
// Each round stops a manager that has fully started: it leads, holds the Lease,
// every watched informer has synced and the controller has started its workers.
// A 200 ms poll used to leave it there by chance in all but a round or so of 200,
// and outlasted that state by up to about 100 ms a round (#282); the round now
// waits for the state itself.
func TestLifecycleManagerReleasesItsLeaseOnEveryStop(t *testing.T) {
	const (
		stopRounds = 200
		stopLimit  = 40 * time.Second // the shutdown grace, 30 s, and the release
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cfg, cl := leaseControlPlane(t, ctx)
	creds, builds := t.TempDir(), t.TempDir()
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		stopRound(t, ctx, cl, round, round%2 == 1, Options{
			CredentialsDir: creds, AgentBuildsDir: builds, LeaderElection: true,
			Checkpoints: &stubSource{}, Registry: prometheus.NewRegistry(),
		}, cfg, stopLimit)
	}
}

// leaseControlPlane starts a control plane with the test CRDs and the
// controller namespace on the lifecycle package's envtest block, stopped when
// the test ends, and returns its config and a client.
func leaseControlPlane(t *testing.T, ctx context.Context) (*rest.Config, client.Client) {
	t.Helper()
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	}
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	})
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}); err != nil {
		t.Fatal(err)
	}
	return cp.Config, cl
}

// stopRound runs one round of TestLifecycleManagerReleasesItsLeaseOnEveryStop
// against the control plane at cfg, with the elector's release read failing
// when faulted. Its deferred stop runs on every path and waits once, bounded,
// for the manager.
func stopRound(t *testing.T, ctx context.Context, cl client.Client, round int, faulted bool, o Options, cfg *rest.Config, stopLimit time.Duration) {
	t.Helper()
	f := leasefault.New(v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	if faulted {
		f.Set(leasefault.ReleaseRead)
	}
	o.Config = f.Config(cfg)
	workers := &workersSink{}
	o.logger = logr.New(workers)
	mgr, _, err := NewManager(o)
	if err != nil {
		t.Fatalf("round %d: %v", round, err)
	}
	if !leaderlock.ReleasesAfterStart(mgr) {
		t.Fatalf("round %d: NewManager returned a %T, not one that releases its Lease after Start", round, mgr)
	}
	mctx, stop := context.WithCancel(leasefault.Mark(ctx))
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	stopped := false
	defer func() {
		if !stopped {
			stop()
			select {
			case <-done:
			case <-time.After(stopLimit):
			}
		}
	}()
	// One minute for all of it: to lead, to hold the Lease and to start fully.
	deadline := time.Now().Add(time.Minute)
	select {
	case <-mgr.Elected():
	case <-time.After(time.Until(deadline)):
		t.Fatalf("round %d: the manager did not lead within a minute", round)
	}
	eventually(t, time.Until(deadline), fmt.Sprintf("round %d: the manager holds the lifecycle Lease", round), func() (bool, string) {
		holder, err := leaseHolderOf(ctx, cl)
		if err != nil {
			return false, err.Error()
		}
		return holder != "", "no holder"
	})
	awaitFullStart(t, ctx, mgr, workers, round, deadline)
	// Read again, apart from the wait: a wait that passes too early, or none, must fail here and not go unseen.
	if ok, msg := fullyStarted(ctx, mgr, workers); !ok {
		t.Fatalf("round %d: the manager would stop before it has fully started: %s", round, msg)
	}
	stop()
	stopped = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("round %d: the manager returned %v", round, err)
		}
	case <-time.After(stopLimit):
		t.Fatalf("round %d: the manager did not return within %s of its cancel; goroutines:\n%s", round, stopLimit, goroutines())
	}
	all, unmarked := f.Failed()
	switch {
	case faulted && (all != 1 || unmarked != 1):
		t.Fatalf("round %d: %d requests for the Lease failed, %d of them unmarked reads; want exactly one, the elector's release read (requests %+v)", round, all, unmarked, f.Requests())
	case !faulted && all != 0:
		t.Fatalf("round %d: %d requests for the Lease failed with no fault set", round, all)
	}
	holder, err := leaseHolderOf(ctx, cl)
	if err != nil {
		t.Fatalf("round %d: reading the lifecycle Lease: %v", round, err)
	}
	if holder != "" {
		t.Fatalf("round %d (release read failing: %v): the stopped manager's Lease is still held by %q when Start has returned", round, faulted, holder)
	}
	if n := f.MarkedAfterReleaseRead(); faulted && n < 2 {
		t.Fatalf("round %d: %d requests for the Lease came after the elector's release read; want the release after Start's read and write", round, n)
	}
}

// workersStartedLine is what the controller logs as it starts its workers
// (controller-runtime v0.23.1, pkg/internal/controller/controller.go:306), the
// only signal of that. If a bump rewords it, the first round fails at its
// one-minute deadline, naming this line (docs/development.md, Pinned versions).
const workersStartedLine = "Starting workers"

// workersSink is a manager's log in one round, kept only for workersStartedLine.
type workersSink struct{ started atomic.Bool }

func (s *workersSink) Init(logr.RuntimeInfo) {}
func (s *workersSink) Enabled(int) bool      { return true }
func (s *workersSink) Info(_ int, msg string, _ ...any) {
	if msg == workersStartedLine {
		s.started.Store(true)
	}
}
func (s *workersSink) Error(error, string, ...any)    {}
func (s *workersSink) WithValues(...any) logr.LogSink { return s }
func (s *workersSink) WithName(string) logr.LogSink   { return s }

// fullyStarted says whether every watched informer has synced, read as readiness.Start reads them,
// and the controller has started its workers. The cache's own WaitForCacheSync will not do: it
// waits only for the informers that exist (readiness.Start), and the controller sees the sync up to
// about 100 ms after they do, at the next poll of its own.
func fullyStarted(ctx context.Context, mgr manager.Manager, workers *workersSink) (bool, string) {
	// No kind is named "", so this reads all of them.
	if ok, msg := othersSynced(ctx, mgr, ""); !ok {
		return false, msg
	}
	if !workers.started.Load() {
		return false, fmt.Sprintf("the controller has not logged %q, which says it has started its workers", workersStartedLine)
	}
	return true, ""
}

// awaitFullStart waits for fullyStarted until deadline.
func awaitFullStart(t *testing.T, ctx context.Context, mgr manager.Manager, workers *workersSink, round int, deadline time.Time) {
	t.Helper()
	for {
		ok, msg := fullyStarted(ctx, mgr, workers)
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("round %d: the manager did not fully start within a minute: %s", round, msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// leaseHolderOf reads the lifecycle Lease's holderIdentity, "" when it names
// none.
func leaseHolderOf(ctx context.Context, cl client.Client) (string, error) {
	var l coordinationv1.Lease
	if err := cl.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
		return "", err
	}
	if l.Spec.HolderIdentity == nil {
		return "", nil
	}
	return *l.Spec.HolderIdentity, nil
}

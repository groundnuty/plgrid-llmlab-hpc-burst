package lifecycle

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

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
func TestLifecycleManagerReleasesItsLeaseOnEveryStop(t *testing.T) {
	const (
		stopRounds = 200
		stopLimit  = 40 * time.Second // the shutdown grace, 30 s, and the release
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cfg, cl := leaseControlPlane(t, ctx)
	creds, builds := t.TempDir(), t.TempDir()
	// The removal of finished Operations (opttl.go) is wired into the manager with the audit policy (#660, PR B). Round 0 shows
	// that a running manager removes the Operation that finished eight days ago, and keeps the one that finished an hour ago
	// and one made while it runs (removalIsWired).
	old, young := seedFinishedOperations(t, ctx, cl)
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		var during func()
		if round == 0 {
			during = func() { removalIsWired(t, ctx, cl, old, young) }
		}
		stopRound(t, ctx, cl, round, round%2 == 1, Options{
			CredentialsDir: creds, AgentBuildsDir: builds, LeaderElection: true,
			Checkpoints: &stubSource{}, Registry: prometheus.NewRegistry(),
		}, cfg, stopLimit, during)
	}
}

// seedFinishedOperations creates two Done Operations, one finished eight days ago and one an hour ago, each naming a
// deployment of its own (so that the lifecycle controller's queue gets one key for each), and returns their keys.
func seedFinishedOperations(t *testing.T, ctx context.Context, cl client.Client) (old, young client.ObjectKey) {
	t.Helper()
	mk := func(name string, ago time.Duration) client.ObjectKey {
		op := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha1.ControllerNamespace}, Spec: v1alpha1.OperationSpec{
			Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: name + "-md"}, Owner: "example-owner"}}
		if err := cl.Create(ctx, op); err != nil {
			t.Fatalf("create Operation %s: %v", name, err)
		}
		finished := metav1.NewTime(time.Now().Add(-ago))
		op.Status.Phase, op.Status.FinishedAt = v1alpha1.OperationPhaseDone, &finished
		if err := cl.Status().Update(ctx, op); err != nil {
			t.Fatalf("finish Operation %s: %v", name, err)
		}
		return client.ObjectKeyFromObject(op)
	}
	return mk("finished-8d-ago", 8*24*time.Hour), mk("finished-1h-ago", time.Hour)
}

// removalIsWired shows that a running manager removes a finished Operation 7 days after it finished, and no sooner. The Operation
// that finished eight days ago goes. That the others stay needs a state that shows the removal controller has had the chance
// to remove them: its work queue (client-go's workqueue metrics, by the controller's name) has had a key added for each of the
// two seeded Operations, and then one more for an Operation made now, so each was handed to it. Then the Operation that
// finished an hour ago and the one made now are read, and both are still there.
func removalIsWired(t *testing.T, ctx context.Context, cl client.Client, old, young client.ObjectKey) {
	t.Helper()
	eventually(t, time.Minute, "the removal takes the Operation that finished eight days ago", func() (bool, string) {
		var op v1alpha1.Operation
		err := cl.Get(ctx, old, &op)
		return apierrors.IsNotFound(err), fmt.Sprintf("get: %v", err)
	})
	sentinel := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "sentinel", Namespace: v1alpha1.ControllerNamespace}, Spec: v1alpha1.OperationSpec{
		Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "sentinel-md"}, Owner: "example-owner"}}
	if err := cl.Create(ctx, sentinel); err != nil {
		t.Fatalf("create the sentinel Operation: %v", err)
	}
	eventually(t, time.Minute, "the removal controller has been handed the two seeded Operations and the sentinel", func() (bool, string) {
		n, found := queueAdds(t, operationTTLController)
		return found && n >= 3, fmt.Sprintf("%v keys added (a queue: %v)", n, found)
	})
	for _, k := range []client.ObjectKey{young, client.ObjectKeyFromObject(sentinel)} {
		var op v1alpha1.Operation
		if err := cl.Get(ctx, k, &op); err != nil {
			t.Errorf("the Operation %s: %v, want it kept: it had not finished 7 days ago", k.Name, err)
		}
	}
}

// queueAdds returns the number of keys the named controller's work queue has had added, and whether the queue has a series
// in controller-runtime's metrics at all.
func queueAdds(t *testing.T, controller string) (float64, bool) {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather controller-runtime's metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "workqueue_adds_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "name" && l.GetValue() == controller {
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
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
func stopRound(t *testing.T, ctx context.Context, cl client.Client, round int, faulted bool, o Options, cfg *rest.Config, stopLimit time.Duration, during func()) {
	t.Helper()
	f := leasefault.New(v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	if faulted {
		f.Set(leasefault.ReleaseRead)
	}
	o.Config = f.Config(cfg)
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
	elected := mgr.Elected()
	eventually(t, time.Minute, fmt.Sprintf("round %d: the manager leads and holds the lifecycle Lease", round), func() (bool, string) {
		select {
		case <-elected:
		default:
			return false, "not elected"
		}
		holder, err := leaseHolderOf(ctx, cl)
		if err != nil {
			return false, err.Error()
		}
		return holder != "", "no holder"
	})
	if during != nil {
		during()
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

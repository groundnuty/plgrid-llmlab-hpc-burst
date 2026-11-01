package e2e

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
)

// lateRenew is a Lease lock that makes one renew land between the release's
// read and its write (issue #131). client-go's LeaderElector.release (v0.35.3,
// tools/leaderelection/leaderelection.go:310-345) reads the Lease, then
// writes the release with the resourceVersion of that read
// (resourcelock/leaselock.go:73-97). A renew that the elector sent before
// its context was cancelled, and that the API server commits only after the
// release's read, is a write in between. lateRenew plays it: when the release
// record arrives at Update (no holder, a 1 s duration, as release writes it),
// it first renews the Lease as its holder through another client, then
// passes the release on, once. It records that the renew was committed, and
// what the release's own write returned then.
type lateRenew struct {
	resourcelock.Interface
	coord coordinationv1client.CoordinationV1Interface
	ns    string
	name  string

	mu         sync.Mutex
	tried      bool  // a release reached Update
	renewed    bool  // the injected renew was committed
	releaseErr error // what the release's write returned right after it
}

func (l *lateRenew) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if ler.HolderIdentity != "" || ler.LeaseDurationSeconds != 1 {
		return l.Interface.Update(ctx, ler)
	}
	l.mu.Lock()
	first := !l.tried
	l.tried = true
	l.mu.Unlock()
	if !first {
		return l.Interface.Update(ctx, ler)
	}
	lease, err := l.coord.Leases(l.ns).Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("the late renew's read: %w", err)
	}
	now := metav1.NewMicroTime(time.Now())
	lease.Spec.RenewTime = &now
	if _, err := l.coord.Leases(l.ns).Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("the late renew: %w", err)
	}
	err = l.Interface.Update(ctx, ler)
	l.mu.Lock()
	l.renewed, l.releaseErr = true, err
	l.mu.Unlock()
	return err
}

// played fails the test unless the late renew was committed between a
// release's read and its write, and that write then conflicted: the
// interleaving the test is about took place.
func (l *lateRenew) played(t *testing.T) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case !l.tried:
		t.Fatal("no release reached Update, so no renew was played between its read and its write")
	case !l.renewed:
		t.Fatal("the late renew was not committed")
	case !apierrors.IsConflict(l.releaseErr):
		t.Fatalf("after the late renew the release's write returned %v, want a conflict", l.releaseErr)
	}
}

// leaseHolder is the Lease's holderIdentity, "" when it names none. A Lease
// that cannot be read fails the test: it is never taken for a free one.
func leaseHolder(t *testing.T, name string) string {
	t.Helper()
	holder, err := readHolder(name)
	if err != nil {
		t.Fatalf("reading the Lease %s: %v", name, err)
	}
	return holder
}

// readHolder reads the Lease's holderIdentity, "" when it names none. A Lease
// that does not exist yet is an error like any other failed read.
func readHolder(name string) (string, error) {
	l := &coordinationv1.Lease{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: name}, l); err != nil {
		return "", err
	}
	if l.Spec.HolderIdentity == nil {
		return "", nil
	}
	return *l.Spec.HolderIdentity, nil
}

// waitHolder waits, with a generous deadline, until the Lease can be read and
// its holder satisfies want. A failed read is not a holder: the wait goes on,
// and a time-out names the last error.
func waitHolder(t *testing.T, name, what string, want func(holder string) bool) {
	t.Helper()
	var holder string
	var err error
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if holder, err = readHolder(name); err == nil && want(holder) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 30s waiting until %s: the Lease's holder is %q, the last read's error %v", what, holder, err)
		}
	}
}

// runReleaseRace starts a manager that leads through lock, waits until the
// Lease names it as holder, stops the manager, and returns the Lease's holder
// once the manager has returned; late must show the interleaving was played.
func runReleaseRace(t *testing.T, name string, lock resourcelock.Interface, late *lateRenew) string {
	t.Helper()
	lease, renew, retry := 4*time.Second, 3*time.Second, 500*time.Millisecond
	mgr, err := manager.New(cfg, manager.Options{
		Scheme:                              scheme(),
		Metrics:                             metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:              "0",
		PprofBindAddress:                    "0",
		LeaderElection:                      true,
		LeaderElectionID:                    name,
		LeaderElectionNamespace:             v1alpha1.ControllerNamespace,
		LeaderElectionReleaseOnCancel:       true,
		LeaderElectionResourceLockInterface: lock,
		LeaseDuration:                       &lease,
		RenewDeadline:                       &renew,
		RetryPeriod:                         &retry,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			<-done
		}
	}()
	elected := mgr.Elected()
	waitHolder(t, name, "the manager leads and holds the Lease", func(holder string) bool {
		select {
		case <-elected:
			return holder == lock.Identity()
		default:
			return false
		}
	})
	cancel()
	select {
	case err := <-done:
		stopped = true
		if err != nil {
			t.Fatalf("the manager returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the manager did not stop within 30 s")
	}
	late.played(t)
	return leaseHolder(t, name)
}

// lateRenewLock is a Lease lock for name with the late renew of lateRenew
// between the release's read and its write. Its identity is new on every
// call, and the Lease is deleted when the test ends, so a run of -count
// never meets a Lease an earlier run left behind.
func lateRenewLock(t *testing.T, name string) *lateRenew {
	t.Helper()
	identity := fmt.Sprintf("%s-holder-%d", name, time.Now().UnixNano())
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: name}})
	})
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	coord, err := coordinationv1client.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := resourcelock.New(resourcelock.LeasesResourceLock, v1alpha1.ControllerNamespace, name, core, coord,
		resourcelock.ResourceLockConfig{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	return &lateRenew{Interface: inner, coord: coord, ns: v1alpha1.ControllerNamespace, name: name}
}

// TestReleaseRetriedAfterALateRenew: with the same late renew, leaderlock's
// lock, which the Provider managers and the lifecycle manager lead through,
// reads the Lease again and repeats the release, so the stopped manager's
// Lease is free at once (issue #131).
func TestReleaseRetriedAfterALateRenew(t *testing.T) {
	const name = "release-race-retried"
	late := lateRenewLock(t, name)
	lock := leaderlock.WithRetriedRelease(late, logr.Discard())
	if holder := runReleaseRace(t, name, lock, late); holder != "" {
		t.Fatalf("after the late renew the Lease is still held by %q; the retried release should have freed it", holder)
	}
}

// TestReleaseLostToALateRenew confirms issue #131's cause with a forced
// interleaving, not a wait for the flake: a renew that lands between the
// release's read and its write makes the release's write conflict, and
// client-go does not retry it, so the Lease stays held by the stopped
// manager until it expires. It is the control for
// TestReleaseRetriedAfterALateRenew: it shows the late renew does make
// client-go's single release fail. It pins client-go v0.35.3's single-shot
// release: if it fails after a client-go upgrade, upstream may now retry the
// release itself, and leaderlock's retry should be revisited, not this
// control deleted.
func TestReleaseLostToALateRenew(t *testing.T) {
	const name = "release-race-plain"
	lock := lateRenewLock(t, name)
	if holder := runReleaseRace(t, name, lock, lock); holder != lock.Identity() {
		t.Fatalf("after the late renew the Lease's holder is %q; client-go's release was expected to be lost, leaving %q", holder, lock.Identity())
	}
}

// TestProviderManagerReleasesItsLeaseOnEveryStop (issue #131; IMPL-COMMON,
// Controller work queues): stopRounds Provider managers, built as the
// controller builds them (providerctl.NewProviderManager), each take the
// Lease, are stopped, and must return within stopLimit with the Lease free.
// client-go releases the Lease before the elector returns, and the manager's
// Start returns only after its stop procedure has waited for the elector
// (controller-runtime v0.23.1, pkg/manager/internal.go:381-383, 468-472,
// 545-554), so the Lease is read once Start has returned, with no wait: a
// lost release shows as a holder there. Each round takes the Lease the
// previous one released at once; after a lost release it would wait until
// the Lease expired by its own clock, 4 s after its first read of it.
func TestProviderManagerReleasesItsLeaseOnEveryStop(t *testing.T) {
	const (
		stopRounds = 200
		stopLimit  = 10 * time.Second
		provider   = "stop-rounds"
	)
	name := v1alpha1.ProviderLeaseName(provider)
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		mgr, err := providerctl.NewProviderManager(cfg, scheme(), provider, providerctl.ManagerOptions{
			LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- mgr.Start(ctx) }()
		elected := mgr.Elected()
		waitHolder(t, name, fmt.Sprintf("round %d: the manager leads and holds the Lease", round), func(holder string) bool {
			select {
			case <-elected:
				return holder != ""
			default:
				return false
			}
		})
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: the manager returned %v", round, err)
			}
		case <-time.After(stopLimit):
			t.Fatalf("round %d: the manager did not return within %s of its cancel; goroutines:\n%s", round, stopLimit, allStacks())
		}
		if holder := leaseHolder(t, name); holder != "" {
			t.Fatalf("round %d: the stopped manager's Lease is still held by %q", round, holder)
		}
	}
}

// allStacks is every goroutine's stack, the form of a test binary's time-out
// dump (debug 2).
func allStacks() string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return b.String()
}

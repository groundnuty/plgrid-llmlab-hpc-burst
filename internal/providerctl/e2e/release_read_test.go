package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock/leasefault"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
)

// readTimings are the Provider managers' leader-election timings in these
// tests: short, so that a lost leadership takes seconds.
var readTimings = providerctl.ManagerOptions{
	LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 500 * time.Millisecond,
}

// releaseReadFaults returns leasefault's Faults for the Lease name, failing
// nothing yet, and deletes the Lease when the test ends, so a run of -count
// never meets a Lease an earlier run left behind.
func releaseReadFaults(t *testing.T, name string) *leasefault.Faults {
	t.Helper()
	t.Cleanup(func() {
		_ = c.Delete(context.Background(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: name}})
	})
	return leasefault.New(v1alpha1.ControllerNamespace, name)
}

// leading is a manager started on a marked context (leasefault.Mark) that
// leads and whose Lease names holder.
type leading struct {
	holder string
	cancel context.CancelFunc
	done   chan error
}

// startLeading starts mgr on a marked context and waits, with a generous
// deadline, until it leads and the Lease name names a holder; the test's end
// stops it if the test has not.
func startLeading(t *testing.T, name string, mgr manager.Manager) *leading {
	t.Helper()
	ctx, cancel := context.WithCancel(leasefault.Mark(context.Background()))
	l := &leading{cancel: cancel, done: make(chan error, 1)}
	go func() { l.done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-l.done:
		case <-time.After(30 * time.Second):
		}
	})
	elected := mgr.Elected()
	waitHolder(t, name, "the manager leads and holds the Lease", func(holder string) bool {
		select {
		case <-elected:
			l.holder = holder
			return holder != ""
		default:
			return false
		}
	})
	return l
}

// result waits, with a generous deadline, for Start to return, and returns
// its result. The done channel carries one value, so result is called once.
func (l *leading) result(t *testing.T) error {
	t.Helper()
	select {
	case err := <-l.done:
		l.done <- err // for the cleanup's receive
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("the manager did not return within 30 s; goroutines:\n%s", allStacks())
		return nil
	}
}

// failedReleaseRead fails the test unless f failed exactly one request, a
// read that was not marked: client-go's release read, and no read of the
// acquire or the renews.
func failedReleaseRead(t *testing.T, f *leasefault.Faults) {
	t.Helper()
	if all, unmarked := f.Failed(); all != 1 || unmarked != 1 {
		t.Fatalf("%d requests for the Lease failed, %d of them unmarked reads; want exactly one, the elector's release read (requests %+v)", all, unmarked, f.Requests())
	}
}

// TestAFailedReleaseReadLeavesTheLeaseHeld confirms issue #160 with a forced
// interleaving: the elector's release read fails, so client-go v0.35.3's
// release returns before its write (leaderelection.go:315-322), and the
// Lease stays held by the stopped manager although its Start returned nil.
// leaderlock's lock, which retries a failed release write (issue #131), does
// not help: the write is never tried. It is the control for the tests below
// that release the Lease after Start. It pins client-go's single-shot
// release: if it fails after a client-go upgrade, upstream may now retry the
// read itself, and ReleaseAfterStart should be revisited, not this control
// deleted.
func TestAFailedReleaseReadLeavesTheLeaseHeld(t *testing.T) {
	const name = "release-read-plain"
	f := releaseReadFaults(t, name)
	f.Set(leasefault.ReleaseRead)
	fcfg := f.Config(cfg)
	lock, err := leaderlock.New(fcfg, v1alpha1.ControllerNamespace, name, readTimings.RenewDeadline, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := manager.New(fcfg, manager.Options{
		Scheme:                              scheme(),
		Metrics:                             metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress:              "0",
		PprofBindAddress:                    "0",
		LeaderElection:                      true,
		LeaderElectionID:                    name,
		LeaderElectionNamespace:             v1alpha1.ControllerNamespace,
		LeaderElectionReleaseOnCancel:       true,
		LeaderElectionResourceLockInterface: lock,
		LeaseDuration:                       &readTimings.LeaseDuration,
		RenewDeadline:                       &readTimings.RenewDeadline,
		RetryPeriod:                         &readTimings.RetryPeriod,
	})
	if err != nil {
		t.Fatal(err)
	}
	l := startLeading(t, name, mgr)
	if l.holder != lock.Identity() {
		t.Fatalf("the Lease names %q, not the manager's lock %q", l.holder, lock.Identity())
	}
	l.cancel()
	if err := l.result(t); err != nil {
		t.Fatalf("the manager returned %v", err)
	}
	failedReleaseRead(t, f)
	if holder := leaseHolder(t, name); holder != lock.Identity() {
		t.Fatalf("after the failed release read the Lease's holder is %q; client-go's release was expected to be lost, leaving %q", holder, lock.Identity())
	}
}

// TestProviderManagerReleasesAfterAFailedReleaseRead: a Provider manager,
// built as the controller builds it, whose elector's release read fails
// still leaves its Lease free when Start returns: the release after Start
// (leaderlock.ReleaseAfterStart) reads the Lease again, finds it naming this
// holder and writes the release (issue #160). The Lease is read once Start
// has returned, with no wait.
func TestProviderManagerReleasesAfterAFailedReleaseRead(t *testing.T) {
	const provider = "release-read"
	name := v1alpha1.ProviderLeaseName(provider)
	f := releaseReadFaults(t, name)
	f.Set(leasefault.ReleaseRead)
	mgr, err := providerctl.NewProviderManager(f.Config(cfg), scheme(), provider, readTimings)
	if err != nil {
		t.Fatal(err)
	}
	l := startLeading(t, name, mgr)
	l.cancel()
	if err := l.result(t); err != nil {
		t.Fatalf("the manager returned %v", err)
	}
	failedReleaseRead(t, f)
	if holder := leaseHolder(t, name); holder != "" {
		t.Fatalf("after the failed release read the Lease is still held by %q when Start has returned; the release after Start should have freed it", holder)
	}
	if n := f.MarkedAfterReleaseRead(); n < 2 {
		t.Fatalf("%d requests for the Lease came after the elector's release read; want the release after Start's read and write", n)
	}
}

// keepsTheLease fails the test unless an involuntary stop left the Lease to
// expire: Start returned an error that contains want, the elector's release
// read was made, nothing was asked of the Lease after it, so no release after
// Start was tried, and the Lease still names the stopped manager.
func keepsTheLease(t *testing.T, name string, l *leading, f *leasefault.Faults, want string) {
	t.Helper()
	err := l.result(t)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("the manager returned %v; want an error with %q", err, want)
	}
	if n := f.MarkedAfterReleaseRead(); n != 0 {
		t.Fatalf("%d requests for the Lease came after the elector's release read (-1: no release read); want the read and nothing after it (requests %+v)", n, f.Requests())
	}
	if holder := leaseHolder(t, name); holder != l.holder {
		t.Fatalf("after the involuntary stop the Lease's holder is %q; want the stopped manager, %q, until it expires", holder, l.holder)
	}
}

// TestProviderManagerKeepsTheLeaseAfterALostLeadership: a Provider manager
// whose renews fail loses its leadership, and Start returns "leader election
// lost" (controller-runtime v0.23.1, internal.go:627-637). Every request for
// the Lease fails from then on, the elector's release read among them, so
// the Lease keeps naming the manager; no release follows Start.
func TestProviderManagerKeepsTheLeaseAfterALostLeadership(t *testing.T) {
	const provider = "release-read-lost"
	name := v1alpha1.ProviderLeaseName(provider)
	f := releaseReadFaults(t, name)
	mgr, err := providerctl.NewProviderManager(f.Config(cfg), scheme(), provider, readTimings)
	if err != nil {
		t.Fatal(err)
	}
	l := startLeading(t, name, mgr)
	f.Set(leasefault.All)
	keepsTheLease(t, name, l, f, "leader election lost")
}

// TestProviderManagerKeepsTheLeaseAfterAFailedRunnable: a Provider manager
// one of whose runnables fails stops with that error, its context not
// cancelled. Its elector's release read fails, and no release follows Start.
func TestProviderManagerKeepsTheLeaseAfterAFailedRunnable(t *testing.T) {
	const provider = "release-read-crash"
	name := v1alpha1.ProviderLeaseName(provider)
	f := releaseReadFaults(t, name)
	f.Set(leasefault.ReleaseRead)
	mgr, err := providerctl.NewProviderManager(f.Config(cfg), scheme(), provider, readTimings)
	if err != nil {
		t.Fatal(err)
	}
	fail := make(chan struct{})
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		select {
		case <-fail:
			return errors.New("the runnable failed")
		case <-ctx.Done():
			return nil
		}
	})); err != nil {
		t.Fatal(err)
	}
	l := startLeading(t, name, mgr)
	close(fail)
	keepsTheLease(t, name, l, f, "the runnable failed")
}

// TestProviderManagerKeepsTheLeaseAfterAStopPastItsGrace: a Provider manager
// whose runnable outlasts its shutdown grace returns an error after a
// requested stop (internal.go:597-604), its context cancelled. The runnable
// may still be running, so no release follows Start; the elector's own
// release read fails.
func TestProviderManagerKeepsTheLeaseAfterAStopPastItsGrace(t *testing.T) {
	const provider = "release-read-grace"
	name := v1alpha1.ProviderLeaseName(provider)
	f := releaseReadFaults(t, name)
	f.Set(leasefault.ReleaseRead)
	o := readTimings
	o.GracefulShutdownTimeout = 500 * time.Millisecond
	mgr, err := providerctl.NewProviderManager(f.Config(cfg), scheme(), provider, o)
	if err != nil {
		t.Fatal(err)
	}
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	if err := mgr.Add(manager.RunnableFunc(func(context.Context) error {
		<-hold // outlasts the grace; the cleanup lets it go
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	l := startLeading(t, name, mgr)
	l.cancel()
	keepsTheLease(t, name, l, f, "failed waiting for all runnables to end within grace period")
}

// Package leaderlock builds the controllers' leader-election Lease locks
// (HBEP-11, Controllers): the lifecycle controller's Lease and each
// Provider manager's provider-<name> Lease. Its lock retries a release whose
// write failed, as when a late renew made it conflict, so the stopped
// manager's Lease is given up after all instead of staying held until it
// expires (issue #131).
//
// client-go's LeaderElector.release (k8s.io/client-go v0.35.3,
// tools/leaderelection/leaderelection.go:310-345) reads the Lease, then
// writes the release with that read's resourceVersion
// (resourcelock/leaselock.go:73-97), once. A renew that the elector sent
// just before its context was cancelled can be committed by the API server
// after that read: the release's write then conflicts, release logs "Failed
// to release lease" and returns, and the Lease keeps naming the stopped
// manager as holder. The next manager to start, with a new identity, takes it
// only once it holds the Lease expired by its own clock: leaseDurationSeconds
// after it first read the record unchanged (isLeaseValid and
// setObservedRecord, leaderelection.go:522-536), not after the record's
// renewTime. It tries every retry period with a jitter factor of 1.2
// (acquire, leaderelection.go:252-277; JitterFactor, :72). With
// controller-runtime's defaults (v0.23.1 pkg/manager/internal.go:54-57: a
// 15 s lease and a 2 s retry period, so tries 2 to 4.4 s apart) it leads up
// to about 19 s after its first read. WithRetriedRelease reads the Lease again
// and repeats the release while the Lease still names this holder, within
// ReleaseBudget.
//
// A release whose read fails is lost in the elector: release returns before
// its write when Lock.Get fails (leaderelection.go:315-322), and the lock
// cannot tell the release's read from a renew's or an acquire's, so it does
// not retry reads (issue #160). ReleaseAfterStart covers it: once a manager's
// Start has returned from a clean, requested stop, the elector has stopped,
// and the Lease is read again and released if it still names this holder.
package leaderlock

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ReleaseBudget bounds each of a stop's two release phases: the retries of the
// elector's release after its first attempt failed (WithRetriedRelease), and
// the release after Start (ReleaseAfterStart), its reads and writes together.
// A release takes a read and a write, milliseconds; the budget only matters
// while the API server is slow. The stop graces count both phases: a Provider
// manager's release comes after its runnables have stopped, within the 10 s
// that the controller's providerGrace leaves of the lifecycle manager's
// shutdown grace (cmd/hpcburst-controller), and the lifecycle Lease's release
// comes after that grace, within launcher.ControllerStopBudget, which the
// supervisor's kill grace for the controller covers
// (launcher.ControllerTermGrace). When both run out, the Lease stays held
// until it expires, as before.
const ReleaseBudget = time.Second

// releaseRetryEvery is the pause before each retry of a release.
const releaseRetryEvery = 100 * time.Millisecond

// New builds the Lease lock name in namespace as controller-runtime's
// leaderelection.NewResourceLock builds the manager's default one (v0.23.1,
// pkg/leaderelection/leader_election.go:66-136): a hostname_UUID identity, a
// "leader-election" user agent and a client time-out of half the renew
// deadline, at least 1 s. The lock retries a failed release
// (WithRetriedRelease); log takes its retries and its give-up.
//
// It records no events, for the lifecycle Lease and the Provider Leases
// alike: the elector's legacy leader-election events (became leader, stopped
// leading) carry no action, so under HBEP-8 nothing may act on them, and
// leadership shows in the Lease itself (`@hpc-burst-science-agent`, 1 Nov
// 2026: the ruling for #111, extended to the Provider Leases for #131).
func New(cfg *rest.Config, namespace, name string, renewDeadline time.Duration, log logr.Logger) (resourcelock.Interface, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	cfg = rest.AddUserAgent(rest.CopyConfig(cfg), "leader-election")
	cfg.Timeout = max(renewDeadline/2, time.Second)
	core, err := corev1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	coord, err := coordinationv1client.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	inner, err := resourcelock.New(resourcelock.LeasesResourceLock, namespace, name, core, coord,
		resourcelock.ResourceLockConfig{Identity: host + "_" + string(uuid.NewUUID())})
	if err != nil {
		return nil, err
	}
	return WithRetriedRelease(inner, log), nil
}

// WithRetriedRelease wraps inner so that a release reads the Lease before it
// writes, and writes only while the Lease names this lock's identity as
// holder: client-go's release writes on the strength of its cached view of
// leadership (leaderelection.go:325-327), which a holder that took the Lease
// meanwhile would not be in. A release whose read or write fails is tried
// again with a fresh read, every releaseRetryEvery, until it succeeds, the
// Lease names another holder or none (an earlier write was committed after
// all, or the Lease expired and was taken), the release's own context ends,
// or ReleaseBudget, counted from the first failure, runs out. Every other
// write passes through unchanged.
//
// log is named only when it logs, so a caller may pass controller-runtime's
// root logger before its sink is set (ctrl.Log): naming it then would make
// controller-runtime warn that no logger was set.
func WithRetriedRelease(inner resourcelock.Interface, log logr.Logger) resourcelock.Interface {
	return &retriedRelease{Interface: inner, log: log}
}

// RetriesRelease reports whether l is a lock from New or WithRetriedRelease,
// for the tests of the code that builds the managers' locks.
func RetriesRelease(l resourcelock.Interface) bool {
	_, ok := l.(*retriedRelease)
	return ok
}

type retriedRelease struct {
	resourcelock.Interface
	log logr.Logger
}

// isRelease reports whether a record is client-go's release: no holder and a
// lease duration of 1 s (leaderelection.go:328-333).
func isRelease(ler resourcelock.LeaderElectionRecord) bool {
	return ler.HolderIdentity == "" && ler.LeaseDurationSeconds == 1
}

// Update implements resourcelock.Interface.
func (l *retriedRelease) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if !isRelease(ler) {
		return l.Interface.Update(ctx, ler)
	}
	done, err := l.releaseOnce(ctx, ler, 1)
	if done {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, ReleaseBudget)
	defer cancel()
	for attempts := 2; ; attempts++ {
		select {
		case <-ctx.Done():
			l.log.WithName("leader-election").Error(err, "the leader Lease's release failed; it stays held until it expires", "lock", l.Describe(), "attempts", attempts-1)
			return err
		case <-time.After(releaseRetryEvery):
		}
		if done, err = l.releaseOnce(ctx, ler, attempts); done {
			if err == nil {
				l.log.WithName("leader-election").Info("released the leader Lease on a retry", "lock", l.Describe(), "attempts", attempts)
			}
			return err
		}
	}
}

// releaseOnce reads the Lease and writes the release if the Lease still names
// this holder. done is false when the read or the write failed, with that
// error; a Lease that names another holder or none needs no write, and is
// done.
func (l *retriedRelease) releaseOnce(ctx context.Context, ler resourcelock.LeaderElectionRecord, attempt int) (done bool, err error) {
	rec, _, err := l.Interface.Get(ctx)
	if err != nil {
		return false, err
	}
	if rec.HolderIdentity != l.Identity() {
		l.log.WithName("leader-election").Info("the leader Lease does not name this holder; nothing to release", "lock", l.Describe(), "holder", rec.HolderIdentity, "attempt", attempt)
		return true, nil
	}
	if err := l.Interface.Update(ctx, ler); err != nil {
		return false, err
	}
	return true, nil
}

// ReleaseAfterStart wraps mgr, which leads through lock, so that a clean,
// requested stop gives the Lease up even when the elector's own release was
// lost (issue #160): when Start has returned nil after its context ended,
// the elector has stopped (controller-runtime v0.23.1,
// pkg/manager/internal.go:381-392, 545-554), and Release runs within
// ReleaseBudget. Start's result is returned unchanged.
//
// An involuntary stop releases nothing: leadership lost, a runnable that
// failed, or a stop that ran past its grace, all of which make Start return
// an error (internal.go:382-392, 486-492, 597-604). On a lost leadership
// controller-runtime skips the graceful shutdown (OnStoppedLeading,
// internal.go:627-637), so the stopped manager's work may still run, and its
// Lease must expire before another leader starts (cmd/hpcburst-controller,
// providerGrace).
func ReleaseAfterStart(mgr manager.Manager, lock resourcelock.Interface, log logr.Logger) manager.Manager {
	return &releasingManager{Manager: mgr, lock: lock, log: log}
}

// ReleasesAfterStart reports whether mgr is from ReleaseAfterStart, for the
// tests of the code that builds the managers.
func ReleasesAfterStart(mgr manager.Manager) bool {
	_, ok := mgr.(*releasingManager)
	return ok
}

type releasingManager struct {
	manager.Manager
	lock resourcelock.Interface
	log  logr.Logger
}

// Start implements manager.Manager.
func (m *releasingManager) Start(ctx context.Context) error {
	err := m.Manager.Start(ctx)
	if err != nil || ctx.Err() == nil {
		return err
	}
	rctx, cancel := context.WithTimeout(context.Background(), ReleaseBudget)
	defer cancel()
	if rerr := Release(rctx, m.lock); rerr != nil {
		m.log.WithName("leader-election").Error(rerr, "the leader Lease could not be released after the manager stopped; it stays held until it expires", "lock", m.lock.Describe())
	}
	return nil
}

// Release gives the Lease up if it names lock's holder. It reads the Lease,
// trying the read again every releaseRetryEvery until it succeeds or ctx
// ends; a Lease that names another holder or none, or does not exist, needs
// no write. Otherwise it writes client-go's release record (no holder, a 1 s
// duration, the transitions kept; leaderelection.go:328-333) through lock
// once, so a lock from WithRetriedRelease retries the write itself, within
// ctx.
func Release(ctx context.Context, lock resourcelock.Interface) error {
	var rec *resourcelock.LeaderElectionRecord
	for {
		var err error
		rec, _, err = lock.Get(ctx)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("read the Lease to release it: %w", err)
		case <-time.After(releaseRetryEvery):
		}
	}
	if rec.HolderIdentity != lock.Identity() {
		return nil
	}
	now := metav1.NewTime(time.Now())
	return lock.Update(ctx, resourcelock.LeaderElectionRecord{
		LeaderTransitions: rec.LeaderTransitions, LeaseDurationSeconds: 1, RenewTime: now, AcquireTime: now,
	})
}

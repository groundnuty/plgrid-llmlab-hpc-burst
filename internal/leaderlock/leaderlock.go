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
// Not covered: a release whose read fails. release returns before its write
// when Lock.Get fails (leaderelection.go:315-322), and the lock cannot tell
// the release's read from a renew's or an acquire's, so it does not retry
// reads. The Lease then stays held until it expires, as before. Covering it
// needs a release after the manager's Start has returned, by the code that
// runs the manager.
package leaderlock

import (
	"context"
	"os"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/uuid"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// ReleaseBudget bounds the retries of one release, after its first attempt
// failed. A release takes a read and a write, milliseconds; the budget only
// matters while the API server is slow. The stop graces count it: a Provider
// manager's release comes after its runnables have stopped, within the 10 s
// that the controller's providerGrace leaves of the lifecycle manager's
// shutdown grace (cmd/hpcburst-controller), and the lifecycle Lease's release
// comes after that grace, within launcher.ControllerStopBudget, which the
// supervisor's kill grace for the controller covers
// (launcher.ControllerTermGrace). When it runs out, the Lease stays held
// until it expires, as before.
const ReleaseBudget = 2 * time.Second

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

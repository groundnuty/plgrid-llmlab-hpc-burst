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
// manager as holder. The next manager to start, with a new identity, waits
// until the Lease expires: renewTime plus leaseDurationSeconds (15 s with
// controller-runtime's defaults, v0.23.1 pkg/manager/internal.go:54-57,
// applied in pkg/manager/manager.go:546-557). WithRetriedRelease reads the
// Lease again and repeats the release while the Lease still names this
// holder, within ReleaseBudget.
//
// Not covered: a release whose read fails. release returns before its write
// when Lock.Get fails (leaderelection.go:315-322), and the lock cannot tell
// the release's read from a renew's or an acquire's, so it does not retry
// reads. The Lease then stays held until it expires, as before: the next
// start waits at most the lease duration plus a retry period, about 17 s
// with the defaults. Covering it needs a release after the manager's Start
// has returned, by the code that runs the manager.
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

// ReleaseBudget bounds the retries of one release, after its first write
// failed. A release takes a read and a write, milliseconds; the budget only
// matters while the API server is slow. It is far below the stop graces it
// runs inside: a Provider manager's release comes after its runnables have
// stopped, within the 10 s that the controller's providerGrace leaves for the
// lifecycle manager's own stop and release (cmd/hpcburst-controller,
// providerGrace), and hpcburst down sends SIGKILL 40 s after SIGTERM
// (launcher.DefaultGrace).
// When it runs out, the Lease stays held until it expires, as before.
const ReleaseBudget = 2 * time.Second

// releaseRetryEvery is the pause before each retry of a release.
const releaseRetryEvery = 100 * time.Millisecond

// New builds the Lease lock name in namespace as controller-runtime's
// leaderelection.NewResourceLock builds the manager's default one (v0.23.1,
// pkg/leaderelection/leader_election.go:66-136): a hostname_UUID identity, a
// "leader-election" user agent and a client time-out of half the renew
// deadline, at least 1 s. It records no events: the elector's legacy
// leader-election events (became leader, stopped leading) carry no action,
// so under HBEP-8 no tool may act on them, and the lifecycle Lease's lock has
// raised none since #111 (@hpc-burst-science-agent, 1 Nov 2026). The lock
// retries a failed release (WithRetriedRelease); log takes its retries and
// its give-up.
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

// WithRetriedRelease wraps inner so that a release whose write fails is
// tried again: it reads the Lease again, and while the Lease still names this
// lock's identity as holder it repeats the release with the fresh
// resourceVersion, every releaseRetryEvery, until it succeeds, the Lease names
// another holder or none (an earlier write was committed after all, or the
// Lease expired and was taken), the release's own context ends, or
// ReleaseBudget runs out. Every other write passes through unchanged.
//
// log is named only when the release is retried, so a caller may pass
// controller-runtime's root logger before its sink is set (ctrl.Log): naming
// it then would make controller-runtime warn that no logger was set.
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
	err := l.Interface.Update(ctx, ler)
	if err == nil || !isRelease(ler) {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, ReleaseBudget)
	defer cancel()
	attempts := 1
	for {
		select {
		case <-ctx.Done():
			l.log.WithName("leader-election").Error(err, "the leader Lease's release failed; it stays held until it expires", "lock", l.Describe(), "attempts", attempts)
			return err
		case <-time.After(releaseRetryEvery):
		}
		rec, _, gerr := l.Interface.Get(ctx)
		if gerr != nil {
			err = gerr
			continue
		}
		if rec.HolderIdentity != l.Identity() {
			l.log.WithName("leader-election").Info("the leader Lease no longer names this holder; nothing to release", "lock", l.Describe(), "holder", rec.HolderIdentity, "attempts", attempts)
			return nil
		}
		attempts++
		if err = l.Interface.Update(ctx, ler); err == nil {
			l.log.WithName("leader-election").Info("released the leader Lease on a retry", "lock", l.Describe(), "attempts", attempts)
			return nil
		}
	}
}

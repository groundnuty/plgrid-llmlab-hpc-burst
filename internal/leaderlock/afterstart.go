package leaderlock

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// AfterStartBudget bounds the release after Start (ReleaseAfterStart): its
// reads, its write and the write's retries together. It is the second of a
// stop's two release phases, after the elector's own release and its retries
// (ReleaseBudget), and the stop graces count it as they count ReleaseBudget.
// On a stop whose elector released the Lease it costs one read.
const AfterStartBudget = time.Second

// ReleaseAfterStart wraps mgr, which leads through lock, so that a clean,
// requested stop gives the Lease up even when the elector's own release was
// lost because its read failed (issue #160). The wrapper's Start runs mgr's;
// when that returns nil after its context ended, the elector has stopped
// (controller-runtime v0.23.1, pkg/manager/internal.go:466-472, 545-554: Start
// waits for the elector before it returns), so nothing else uses lock, and
// the Lease is read again and released if it still names lock's holder,
// within AfterStartBudget. Start's result is returned unchanged, and a
// release that fails is logged to log: the Lease then stays held until it
// expires, as before.
//
// The holder check is the only gate besides Start's result: a manager that
// never led finds another holder or none, at the cost of one read, and one
// whose acquire was written but not yet acted on (the elector had not yet
// started the leader runnables) gives the Lease back too.
//
// An involuntary stop releases nothing: a lost leadership, a runnable that
// failed, or a stop that ran past its grace each make Start return an error
// (internal.go:487-493, 597-604). client-go's elector still tries its own
// release on those paths (leaderelection.go:303-306). One corner remains: a
// leadership lost at the moment the stop is requested can make Start return
// nil, since the lost leadership sets the grace to 0 (OnStoppedLeading,
// internal.go:627-637) and a stop with no grace returns nil. The release
// after Start then repeats only what client-go's elector itself tries on a
// lost leadership, after the runnables were told to stop rather than before.
//
// log is named only when it logs, as in WithRetriedRelease.
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

// Start implements manager.Manager. The release runs before Start returns,
// so a caller that waits for Start, as the Provider Supervisor waits for each
// Provider manager before its own Start returns, sees the Lease free first:
// the Provider Leases are released before the lifecycle manager's stop goes
// on to the lifecycle Lease. Its context keeps ctx's values without its
// cancellation (context.WithoutCancel).
func (m *releasingManager) Start(ctx context.Context) error {
	err := m.Manager.Start(ctx)
	if err != nil || ctx.Err() == nil {
		return err
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), AfterStartBudget)
	defer cancel()
	released, rerr := releaseHeld(rctx, m.lock)
	switch {
	case rerr != nil:
		m.log.WithName("leader-election").Error(rerr, "could not release the leader Lease after the manager stopped; if this manager still held it, it stays held until it expires", "lock", m.lock.Describe())
	case released:
		m.log.WithName("leader-election").Info("released the leader Lease after the manager stopped; the elector's own release was lost", "lock", m.lock.Describe())
	}
	return nil
}

// releaseHeld gives the Lease up if it names lock's holder. It reads the
// Lease, trying the read again every releaseRetryEvery until it succeeds or
// ctx ends; a Lease that names another holder or none, or does not exist,
// needs no write. Otherwise it writes client-go's release record (no holder,
// a 1 s duration, the read's transitions kept; leaderelection.go:328-333)
// through lock, which writes it with the read's resourceVersion
// (resourcelock/leaselock.go:73-97), so a Lease taken or renewed since the
// read makes the write conflict instead of being overwritten. The write is
// made once; a lock from WithRetriedRelease reads again and retries it
// itself, within ctx. released reports that the Lease named this holder and
// the write returned no error.
func releaseHeld(ctx context.Context, lock resourcelock.Interface) (released bool, err error) {
	var rec *resourcelock.LeaderElectionRecord
	for {
		rec, _, err = lock.Get(ctx)
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("read the Lease to release it: %w", err)
		case <-time.After(releaseRetryEvery):
		}
	}
	if rec.HolderIdentity != lock.Identity() {
		return false, nil
	}
	now := metav1.NewTime(time.Now())
	if err := lock.Update(ctx, resourcelock.LeaderElectionRecord{
		LeaderTransitions: rec.LeaderTransitions, LeaseDurationSeconds: 1, RenewTime: now, AcquireTime: now,
	}); err != nil {
		return false, fmt.Errorf("write the Lease's release: %w", err)
	}
	return true, nil
}

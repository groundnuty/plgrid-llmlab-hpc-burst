package leaderlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
// lost because its read failed (issue #160). The wrapper's Start runs mgr's,
// and releases the Lease after it only on positive evidence that the stop
// was clean and complete:
//
//   - Start returned nil after its context ended. The elector has then
//     stopped (controller-runtime v0.23.1, pkg/manager/internal.go:466-472,
//     545-554: Start waits for it), so nothing else uses lock.
//   - The leadership was not lost: the elector's last renew write before it
//     stopped succeeded (Renewals). client-go gives the leadership up only
//     after a renew poll whose every try failed, and the first try of each
//     poll is a write while the lease is still valid (leaderelection.go:
//     284-301, 433-446), so a lost leadership always ends in a failed write.
//   - Every runnable returned. With a positive grace, as every manager here
//     has, a requested stop returns nil only when the stop procedure ended
//     the shutdown context after every runnable group had stopped; a grace
//     that runs out returns an error (internal.go:557-610). The one other
//     nil, a grace reset to 0 by a lost leadership (OnStoppedLeading,
//     internal.go:627-637) and then run out while a runnable still runs, is
//     excluded by the evidence above (review of PR #215: B1).
//
// The Lease is then read again and released if it still names lock's
// holder, within AfterStartBudget. Start's result is returned unchanged, and
// a release that fails is logged to log: the Lease then stays held until it
// expires, as before. Without that evidence nothing is released after Start:
// a lost leadership, a runnable that failed, a stop past its grace, or a
// manager that never led. client-go's elector still tries its own release on
// those paths (leaderelection.go:303-306).
//
// log is named only when it logs, as in WithRetriedRelease.
func ReleaseAfterStart(mgr manager.Manager, lock *Renewals, log logr.Logger) manager.Manager {
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
	lock *Renewals
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
	if err != nil || ctx.Err() == nil || !m.lock.ledToTheEnd() {
		return err
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), AfterStartBudget)
	defer cancel()
	released, rerr := releaseHeld(rctx, m.lock)
	switch {
	case rerr != nil:
		m.log.WithName("leader-election").Error(rerr, "could not release the leader Lease after the manager stopped; it stays held until it expires", "lock", m.lock.Describe())
	case released:
		m.log.WithName("leader-election").Info("released the leader Lease after the manager stopped; the elector's own release was lost", "lock", m.lock.Describe())
	}
	return nil
}

// TrackRenewals wraps inner, the lock a manager leads through, so that
// ReleaseAfterStart can tell a clean stop from a lost leadership. It records
// whether the last write that named this holder, an acquire or a renew,
// succeeded; client-go's release record (no holder) is not counted. A write
// cut short because its context was cancelled is not counted either: only
// the requested stop cancels the elector's context while it writes
// (internal.go:545-554), whereas a renew poll that runs out ends its tries
// with DeadlineExceeded (wait.PollUntilContextTimeout). Every other call
// passes through unchanged.
func TrackRenewals(inner resourcelock.Interface) *Renewals {
	return &Renewals{Interface: inner}
}

// Renewals is a lock from TrackRenewals.
type Renewals struct {
	resourcelock.Interface

	mu sync.Mutex
	// ok is whether the last counted write succeeded; false before any.
	ok bool
}

// Create implements resourcelock.Interface.
func (r *Renewals) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Create(ctx, ler)
	r.wrote(ctx, ler, err)
	return err
}

// Update implements resourcelock.Interface.
func (r *Renewals) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Update(ctx, ler)
	r.wrote(ctx, ler, err)
	return err
}

func (r *Renewals) wrote(ctx context.Context, ler resourcelock.LeaderElectionRecord, err error) {
	if ler.HolderIdentity == "" || ler.HolderIdentity != r.Identity() {
		return
	}
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ok = err == nil
}

// ledToTheEnd reports whether the last counted write succeeded: the manager
// led, and had not lost the leadership when its elector stopped.
func (r *Renewals) ledToTheEnd() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ok
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

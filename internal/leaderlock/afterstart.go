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
// It runs only when the elector's own release did not land, and only within
// what is left of the stop's shared ReleaseBudget (Renewals): it is skipped,
// and the skip logged with the time left, once that is spent. The Lease is
// then read again and released if it still names lock's holder, within the
// time left. Start's result is returned unchanged, and
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
	left, needed := m.lock.budgetLeft(time.Now())
	if !needed {
		return nil // the elector's release landed
	}
	if left <= 0 {
		m.log.WithName("leader-election").Info("skipped the release after the manager stopped: the shared release budget is spent; the Lease stays held until it expires", "lock", m.lock.Describe(), "left", left)
		return nil
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), left)
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
// ReleaseAfterStart can tell a clean stop from a lost leadership, and knows
// what is left of the stop's shared ReleaseBudget. It records whether the
// last write that named this holder, an acquire or a renew, succeeded;
// client-go's release record (no holder) is not counted there. It also
// records when the elector's release first reached Update, and whether that
// Update returned nil: the Lease released, or found naming another holder or
// none (WithRetriedRelease). A write
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
	// releaseAt is when the elector's release first reached Update; zero
	// before. released is whether that Update returned nil.
	releaseAt time.Time
	released  bool
	// renewedAt is the renewTime of the last acquire or renew write that
	// named this holder and succeeded, as the elector set it before sending
	// the write; zero before any, and again from the start of a release
	// (RenewedWithin).
	renewedAt time.Time
}

// Create implements resourcelock.Interface.
func (r *Renewals) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Create(ctx, ler)
	r.wrote(ctx, ler, err)
	return err
}

// Update implements resourcelock.Interface.
func (r *Renewals) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if isRelease(ler) {
		return r.release(ctx, ler)
	}
	err := r.Interface.Update(ctx, ler)
	r.wrote(ctx, ler, err)
	return err
}

// release passes a release record on, and records the first one's start and
// outcome: the elector's release, which comes before any release after Start.
func (r *Renewals) release(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	r.mu.Lock()
	r.renewedAt = time.Time{} // giving the Lease up: it leads no more
	first := r.releaseAt.IsZero()
	if first {
		r.releaseAt = time.Now()
	}
	r.mu.Unlock()
	err := r.Interface.Update(ctx, ler)
	if first {
		r.mu.Lock()
		r.released = err == nil
		r.mu.Unlock()
	}
	return err
}

// budgetLeft reports whether a release after Start is needed, and what is
// left of the stop's shared ReleaseBudget at now. It is not needed when the
// elector's release returned nil. The budget is counted from the elector's
// release write, which includes that write's first attempt, so the release
// after Start never gets more than the retries had left; an elector whose
// release never reached Update (its read failed) used none of it.
func (r *Renewals) budgetLeft(now time.Time) (left time.Duration, needed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.released:
		return 0, false
	case r.releaseAt.IsZero():
		return ReleaseBudget, true
	default:
		return ReleaseBudget - now.Sub(r.releaseAt), true
	}
}

func (r *Renewals) wrote(ctx context.Context, ler resourcelock.LeaderElectionRecord, err error) {
	if ler.HolderIdentity == "" || ler.HolderIdentity != r.Identity() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.renewedAt = ler.RenewTime.Time
	}
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	r.ok = err == nil
}

// RenewedWithin reports whether this holder leads at now by its own writes:
// its last acquire or renew write that named it succeeded, that write's
// renewTime is less than d before now, and no release has started since. It
// reads no Lease. It is the lifecycle controller's "leading" readiness check,
// which gates what the controller serves (HBEP-8, Unknown samples: a copy
// exports its state-derived series only while it leads; PR #254 review, B2).
//
// The elector's Elected channel, once closed, stays closed, and client-go
// calls OnStoppedLeading only after its release read
// (k8s.io/client-go v0.35.3, tools/leaderelection/leaderelection.go:
// renew, then release, then Run's deferred OnStoppedLeading), which can
// outlast the Lease's expiry; another copy can lead meanwhile. A renewTime is
// set before its write is sent, and another copy that read the record can
// take the Lease only once it has seen it unchanged for leaseDurationSeconds
// by its own clock (isLeaseValid against the time it first read it, after the
// write's commit): not before renewTime + leaseDuration. So with d no longer
// than the lease duration this holder stops counting itself as leader no
// later than any other copy can lead. The lifecycle manager passes its renew
// deadline (10 s against a 15 s lease), so the turn comes at least 5 s
// before another copy can lead, and before its own elector gives up, whose
// failing renew poll starts a retry period after the last success.
func (r *Renewals) RenewedWithin(now time.Time, d time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.renewedAt.IsZero() && now.Before(r.renewedAt.Add(d))
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

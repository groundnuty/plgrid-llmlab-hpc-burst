package leaderlock

import (
	"context"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// GapLock is the leader election's lock with the start's observation of
// HBEP-8, Gaps after a restart: a controller reads its Lease's previous
// renewTime before it takes the Lease. Both controllers use it: the lifecycle
// controller on its lifecycle Lease, and each Provider manager on its
// provider-<name> Lease. Every read the elector makes before this process's
// first successful write is such a read, and the last of them is the Lease as
// it stood just before the take: the previous holder's final heartbeat, or its
// release on a clean stop. A read that shows this process as the holder before
// a write has succeeded comes from a write that committed but reported an
// error; it is this run's own heartbeat and does not count.
//
// Build it with NewGapLock, or set Interface in a literal. Its methods are
// safe to call from the elector's goroutine while GapStart is read from
// another.
type GapLock struct {
	resourcelock.Interface

	// OnObserve, when set, runs after each read that counts as the start's
	// observation: a test waits on it to know the elector has read the Lease.
	// Set it before the elector starts.
	OnObserve func()

	mu       sync.Mutex
	took     bool
	observed bool
	// previous is the renewTime read before the take; zero when there was no
	// Lease or it had no renewTime (a first start).
	previous time.Time
}

// NewGapLock wraps inner, the Lease's lock, in a GapLock.
func NewGapLock(inner resourcelock.Interface) *GapLock {
	return &GapLock{Interface: inner}
}

// Get implements resourcelock.Interface.
func (l *GapLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	rec, raw, err := l.Interface.Get(ctx)
	if l.observe(rec, err) && l.OnObserve != nil {
		l.OnObserve()
	}
	return rec, raw, err
}

// observe keeps a read made before the take as the start's observation, and
// reports whether it did.
func (l *GapLock) observe(rec *resourcelock.LeaderElectionRecord, err error) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.took {
		return false
	}
	switch {
	case apierrors.IsNotFound(err):
		l.observed, l.previous = true, time.Time{}
	case err == nil && rec != nil && rec.HolderIdentity != l.Identity():
		l.observed, l.previous = true, rec.RenewTime.Time
	default:
		return false
	}
	return true
}

// Create implements resourcelock.Interface.
func (l *GapLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, ler)
	l.wrote(err)
	return err
}

// Update implements resourcelock.Interface.
func (l *GapLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, ler)
	l.wrote(err)
	return err
}

func (l *GapLock) wrote(err error) {
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.took = true
}

// GapStart returns the previous renewTime read before the take, and whether
// there is a gap to report: the Lease is held, it was read before the take,
// and that read found a renewTime. A first start, with no previous Lease,
// reports none.
func (l *GapLock) GapStart() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.previous, l.took && l.observed && !l.previous.IsZero()
}

// GapTime formats a renewTime as the API stores it, to the microsecond
// (metav1.MicroTime), so the gap's start names the exact heartbeat.
func GapTime(t time.Time) string { return t.UTC().Format(metav1.RFC3339Micro) }

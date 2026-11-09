package providerctl

// The relays' fence on the Provider's own leader Lease (HBEP-6, Fencing; the
// delta review of PR 301, D1 and D2).

import (
	"context"
	"errors"
	"sync"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

// RelayFence wraps the provider-<name> Lease's lock, which the Provider
// manager's elector writes through.
//
// The epoch: E is the leaseTransitions of this lock's own acquisition, the
// first write that succeeds naming this lock's Identity as holder. client-go
// writes the count up by one whenever the holder changes and keeps it on a
// renewal, so it is this copy's lead's number; a copy that is paused after it
// was elected keeps its own, lower, number and never borrows its successor's
// (D1). Before such a write there is no epoch.
//
// The release: before a release record (no holder) reaches the Lease, the
// fence stops this manager's relays by PID and waits for them (D2). The
// elector writes the release on both of its ways out: on a requested stop,
// after controller-runtime has stopped the leader-only runnables, and after a
// renewal that failed, inside client-go's renew() before OnStoppedLeading
// (leaderelection.go:279-307 at v0.35.3, with ReleaseOnCancel, which the
// Provider manager sets). So no copy's Lease is free while its relays run.
type RelayFence struct {
	resourcelock.Interface

	mu            sync.Mutex
	held          bool
	epoch         int64
	beforeRelease func()
}

// NewRelayFence wraps lock.
func NewRelayFence(lock resourcelock.Interface) *RelayFence {
	return &RelayFence{Interface: lock}
}

// errNotLeading: the fence has seen no write of its own acquisition yet.
var errNotLeading = errors.New("this copy has not acquired the Provider's Lease")

// Epoch is the leaseTransitions of this lock's own acquisition.
func (f *RelayFence) Epoch(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.held {
		return 0, errNotLeading
	}
	return f.epoch, nil
}

// BeforeRelease sets what runs before a release record is written: stopping
// the relays and waiting for them.
func (f *RelayFence) BeforeRelease(stop func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beforeRelease = stop
}

// Create implements resourcelock.Interface.
func (f *RelayFence) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := f.Interface.Create(ctx, ler)
	f.wrote(ler, err)
	return err
}

// Update implements resourcelock.Interface.
func (f *RelayFence) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if leaderlock.IsRelease(ler) {
		f.mu.Lock()
		stop := f.beforeRelease
		f.mu.Unlock()
		if stop != nil {
			stop()
		}
		return f.Interface.Update(ctx, ler)
	}
	err := f.Interface.Update(ctx, ler)
	f.wrote(ler, err)
	return err
}

// wrote records the epoch at the first successful write naming this lock as
// holder.
func (f *RelayFence) wrote(ler resourcelock.LeaderElectionRecord, err error) {
	if err != nil || ler.HolderIdentity == "" || ler.HolderIdentity != f.Identity() {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.held {
		f.held, f.epoch = true, int64(ler.LeaderTransitions)
	}
}

// fencedManager is the Provider manager with its relays' fence.
type fencedManager struct {
	manager.Manager
	fence *RelayFence
}

func (m fencedManager) RelayFence() *RelayFence { return m.fence }

// relayFenced is a manager that carries a RelayFence (NewProviderManager's).
type relayFenced interface{ RelayFence() *RelayFence }

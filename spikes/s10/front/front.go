// Package front holds the rules HBEP-5's front adds on top of KubeAI's
// copied balancer (third_party/forked/kubeai). The copy is unchanged; these
// rules live here, in the front's own code.
//
//   - Selection and reservation are one critical section. Upstream checks
//     the load and increments the counter under a read lock, so two
//     selectors can both pass the check on the same loads and both
//     increment (fork test TestConcurrentSelectorsBothPassOnThePlainGroup).
//     Releases take the same lock, so the counts a selector reads are the
//     counts at its reservation.
//   - Each reservation is released exactly once. Upstream's release function
//     decrements every time it is called (fork test
//     TestUpstreamReleaseDecrementsOnEveryCall); here a second call is a
//     no-op.
//   - A draining Instance's open reservations are released when the drain
//     starts, so the bound's mean covers members only. Upstream keeps them
//     in the group total until they finish, by design (KubeAI PR #747); this
//     is a deliberate divergence. Their completion later calls the same
//     once-guarded release, which then does nothing.
package front

import (
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

// Balancer serializes KubeAI's CHWBL group and tracks open reservations.
type Balancer struct {
	mu   sync.Mutex
	g    *loadbalancer.Group
	open map[string]map[*reservation]struct{}

	// onContended, when set, runs in a caller that finds the lock held,
	// before it waits. A test seam: it tells a test that the caller is
	// blocked on the lock rather than inside a selection.
	onContended func()
}

type reservation struct {
	addr     string
	release  func() // upstream's
	released bool
}

// New returns a Balancer over a fresh group with `replication` points per
// endpoint on the ring.
func New(replication int) *Balancer {
	return &Balancer{g: loadbalancer.NewGroup(replication), open: map[string]map[*reservation]struct{}{}}
}

func (b *Balancer) lock() {
	if b.mu.TryLock() {
		return
	}
	if h := b.onContended; h != nil {
		h()
	}
	b.mu.Lock()
}

// PickPrefixHash selects an endpoint with KubeAI's CHWBL for key and bound
// (125 = 1.25 x the mean) and reserves it, in one critical section. The
// release function it returns is safe to call more than once; only the
// first call counts.
func (b *Balancer) PickPrefixHash(key string, meanLoadPercentage int) (string, func(), error) {
	b.lock()
	defer b.mu.Unlock()
	addr, done, err := b.g.PickPrefixHash(key, meanLoadPercentage)
	if err != nil {
		return "", func() {}, err
	}
	r := &reservation{addr: addr, release: done}
	if b.open[addr] == nil {
		b.open[addr] = map[*reservation]struct{}{}
	}
	b.open[addr][r] = struct{}{}
	return addr, func() { b.releaseOnce(r) }, nil
}

func (b *Balancer) releaseOnce(r *reservation) {
	b.lock()
	defer b.mu.Unlock()
	b.releaseLocked(r)
}

func (b *Balancer) releaseLocked(r *reservation) {
	if r.released {
		return
	}
	r.released = true
	r.release()
	delete(b.open[r.addr], r)
}

// SetEndpoints reconciles the ring to exactly these endpoints.
func (b *Balancer) SetEndpoints(names []string) {
	b.lock()
	defer b.mu.Unlock()
	b.g.SetEndpoints(names)
}

// Remove stops new selections of an endpoint and leaves its open
// reservations counted until they are released: KubeAI's own behaviour,
// kept as the control.
func (b *Balancer) Remove(name string) {
	b.lock()
	defer b.mu.Unlock()
	b.g.RemoveEndpoint(name)
}

// Drain stops new selections of an endpoint and releases its open
// reservations at once. It returns how many it released.
func (b *Balancer) Drain(name string) int {
	b.lock()
	defer b.mu.Unlock()
	b.g.RemoveEndpoint(name)
	n := 0
	for r := range b.open[name] {
		b.releaseLocked(r)
		n++
	}
	return n
}

// TotalInFlight is KubeAI's group-wide in-flight counter.
func (b *Balancer) TotalInFlight() int64 {
	b.lock()
	defer b.mu.Unlock()
	return b.g.TotalInFlight()
}

// InFlight is KubeAI's in-flight counter for a current member.
func (b *Balancer) InFlight(name string) (int64, bool) {
	b.lock()
	defer b.mu.Unlock()
	return b.g.InFlight(name)
}

// Members is the number of current members.
func (b *Balancer) Members() int {
	b.lock()
	defer b.mu.Unlock()
	return b.g.Members()
}

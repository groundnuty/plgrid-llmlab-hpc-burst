package hostdial

import (
	"context"
	"fmt"
	"sync"
)

// DefaultRelaysPerLogin is alpha's cap on relays per login node: 4, that is 2
// Allocations, the handover pair (HBEP-6, Dial-home modes, host-dialed mode).
// The probe of 9 Nov 2026 held 4 concurrent connections, and made 10 at one a
// second, with no refusal. A per-platform budget stays HBEP-4's open question
// 17.
const DefaultRelaysPerLogin = 4

// Slots caps the relays on each login node across the whole process: every
// Provider's controller that reaches the same login node takes its slots from
// the same Slots, so two Providers on one cluster cannot hold 8. An
// Allocation takes the slots of both its relays together, before either
// starts, and holds them while it is wanted, through its relays' restarts and
// backoffs: "at most 4 relays per login node in alpha, that is, 2
// Allocations" (HBEP-6). So a relay that drops never gives its slot to
// another Allocation, which would leave both with half a pair. A third
// Allocation on one login node waits for a pair of slots.
type Slots struct {
	max int

	mu   sync.Mutex
	used map[string]int
	wake chan struct{} // closed and replaced when a slot frees
}

// NewSlots returns slots with max per login node; max below 1 means
// DefaultRelaysPerLogin.
func NewSlots(max int) *Slots {
	if max < 1 {
		max = DefaultRelaysPerLogin
	}
	return &Slots{max: max, used: map[string]int{}, wake: make(chan struct{})}
}

// SharedSlots is the process's slots, which the controller binary gives every
// Provider's relay owner.
var SharedSlots = NewSlots(DefaultRelaysPerLogin)

// Acquire takes n slots on login together, waiting while fewer are free,
// until ctx ends. The returned release gives them all back; it may be called
// once.
func (s *Slots) Acquire(ctx context.Context, login string, n int) (release func(), err error) {
	if n < 1 || n > s.max {
		return nil, fmt.Errorf("hostdial: %d slots asked of a login node capped at %d", n, s.max)
	}
	for {
		s.mu.Lock()
		if s.used[login]+n <= s.max {
			s.used[login] += n
			s.mu.Unlock()
			var once sync.Once
			return func() { once.Do(func() { s.release(login, n) }) }, nil
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Slots) release(login string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used[login] -= n; s.used[login] <= 0 {
		delete(s.used, login)
	}
	close(s.wake)
	s.wake = make(chan struct{})
}

// InUse is how many slots are taken on login.
func (s *Slots) InUse(login string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[login]
}

// Hosts is the process's register of which host-dialed Provider runs relays
// on each login host (HBEP-6, Capacity: "Only one Provider per login host (as
// ssh -G resolves its HostName) runs in host-dialed mode"). The first Provider
// to claim a host keeps it until it releases it; a second is refused.
type Hosts struct {
	mu sync.Mutex
	by map[string]string // login host → Provider
}

// NewHosts returns an empty register.
func NewHosts() *Hosts { return &Hosts{by: map[string]string{}} }

// SharedHosts is the process's register, which the controller binary gives
// every Provider.
var SharedHosts = NewHosts()

// Claim claims login for provider, and gives up any other host provider held
// (its credentials moved). It returns the holder and false when another
// Provider holds login.
func (h *Hosts) Claim(login, provider string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p, ok := h.by[login]; ok && p != provider {
		return p, false
	}
	for l, p := range h.by {
		if p == provider && l != login {
			delete(h.by, l)
		}
	}
	h.by[login] = provider
	return provider, true
}

// Release gives up every host provider holds.
func (h *Hosts) Release(provider string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for l, p := range h.by {
		if p == provider {
			delete(h.by, l)
		}
	}
}

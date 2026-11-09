package hostdial

import (
	"context"
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
// the same Slots, so two Providers on one cluster cannot hold 8. A relay holds
// its slot from just before its ssh starts until its process is gone. A fifth
// relay on one login node waits for a slot.
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

// Acquire takes a slot on login, waiting while all are taken, until ctx ends.
// The returned release gives it back; it may be called once.
func (s *Slots) Acquire(ctx context.Context, login string) (release func(), err error) {
	for {
		s.mu.Lock()
		if s.used[login] < s.max {
			s.used[login]++
			s.mu.Unlock()
			var once sync.Once
			return func() { once.Do(func() { s.release(login) }) }, nil
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

func (s *Slots) release(login string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used[login]--; s.used[login] <= 0 {
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

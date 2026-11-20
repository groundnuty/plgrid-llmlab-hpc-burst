package hostdial

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
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
//
// The 4 is the default. A Provider whose profile sets transport.loginHostBudget
// gives its login node another limit, 2 relays for each Allocation of its cap
// (SetLimit; HBEP-4, open question 17), which waiters read again when it
// changes. A limit set below the slots taken keeps the Allocations that hold
// them and holds the next back until they drain.
type Slots struct {
	max int

	mu    sync.Mutex
	used  map[string]int
	limit map[string]int // a login node's own limit, in place of max
	wake  chan struct{}  // closed and replaced when a slot frees or a limit changes
}

// NewSlots returns slots with max per login node; max below 1 means
// DefaultRelaysPerLogin.
func NewSlots(max int) *Slots {
	if max < 1 {
		max = DefaultRelaysPerLogin
	}
	return &Slots{max: max, used: map[string]int{}, limit: map[string]int{}, wake: make(chan struct{})}
}

// SharedSlots is the process's slots, which the controller binary gives every
// Provider's relay owner.
var SharedSlots = NewSlots(DefaultRelaysPerLogin)

// SetLimit gives login a limit of n relays in place of the default, and wakes
// the Allocations waiting for a pair: a limit of 0 admits none. A negative n
// changes nothing.
func (s *Slots) SetLimit(login string, n int) {
	if n < 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.limit[login]; ok && cur == n {
		return
	}
	s.limit[login] = n
	s.wakeLocked()
}

// ClearLimit gives login the default limit back, and wakes the Allocations
// waiting for a pair.
func (s *Slots) ClearLimit(login string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.limit[login]; !ok {
		return
	}
	delete(s.limit, login)
	s.wakeLocked()
}

// Limit is the most relays login may hold now.
func (s *Slots) Limit(login string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limitLocked(login)
}

func (s *Slots) limitLocked(login string) int {
	if n, ok := s.limit[login]; ok {
		return n
	}
	return s.max
}

func (s *Slots) wakeLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

// Acquire takes n slots on login together, waiting while fewer are free,
// until ctx ends. The returned release gives them all back; it may be called
// once. Under the default limit it refuses at once an n that could never be
// admitted; under a limit that SetLimit set it waits, whatever n, since the
// limit may rise.
func (s *Slots) Acquire(ctx context.Context, login string, n int) (release func(), err error) {
	s.mu.Lock()
	_, own := s.limit[login]
	s.mu.Unlock()
	if n < 1 || (n > s.max && !own) {
		return nil, fmt.Errorf("hostdial: %d slots asked of a login node capped at %d", n, s.max)
	}
	for {
		s.mu.Lock()
		if s.used[login]+n <= s.limitLocked(login) {
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
	s.wakeLocked()
}

// InUse is how many slots are taken on login.
func (s *Slots) InUse(login string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[login]
}

// Hosts is the process's register of which host-dialed Provider runs relays
// on each login host (HBEP-6, Capacity: "Only one Provider per login host (as
// ssh -G resolves its HostName) runs in host-dialed mode"). The winner is
// fixed, not first come: the oldest Provider by creationTimestamp, then by
// name, among those that use the host, so a restart cannot hand the host to
// another Provider (the delta review of PR 301, D4). A Provider enrolls, with
// its age, when its manager starts; until an older enrolled Provider has said
// which login host it uses, a younger one is not admitted to any, so it cannot
// take a host in the moment before the older one asks for it.
type Hosts struct {
	mu sync.Mutex
	by map[string]*hostEntry // Provider → its entry
}

type hostEntry struct {
	created  time.Time
	login    string // lower case; "" when it uses none
	reported bool
}

// NewHosts returns an empty register.
func NewHosts() *Hosts { return &Hosts{by: map[string]*hostEntry{}} }

// SharedHosts is the process's register, which the controller binary gives
// every Provider.
var SharedHosts = NewHosts()

// Enroll records that provider, created at created, runs in this process and
// has not said which login host it uses yet.
func (h *Hosts) Enroll(provider string, created time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.by[provider]; ok {
		e.created = created
		return
	}
	h.by[provider] = &hostEntry{created: created}
}

// Enrolled reports whether provider is enrolled.
func (h *Hosts) Enrolled(provider string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.by[provider]
	return ok
}

// Claim records that provider uses login (any case; "" for none) and reports
// whether it is the host's Provider. When it is not, holder is the Provider
// that is, or the older one that has not said which host it uses yet
// (pending). An unenrolled provider is never admitted.
func (h *Hosts) Claim(login, provider string) (holder string, pending, ok bool) {
	login = strings.ToLower(login)
	h.mu.Lock()
	defer h.mu.Unlock()
	me, enrolled := h.by[provider]
	if !enrolled {
		return "", false, false
	}
	me.login, me.reported = login, true
	if login == "" {
		return "", false, false
	}
	for name, e := range h.by {
		if name == provider || (e.reported && e.login != login) {
			continue
		}
		if e.created.Before(me.created) || (e.created.Equal(me.created) && name < provider) {
			if holder == "" || olderThan(e, name, h.by[holder], holder) {
				holder, pending = name, !e.reported
			}
		}
	}
	return holder, pending, holder == ""
}

func olderThan(a *hostEntry, an string, b *hostEntry, bn string) bool {
	return a.created.Before(b.created) || (a.created.Equal(b.created) && an < bn)
}

// OnHostOf is how many of the register's Providers use the login host that
// provider uses, provider itself included: the stack's Providers on that
// host, each holding its own master (HBEP-4, open question 17, M in the
// host-dialed cap). A Provider counts once it has said which host it uses,
// whether or not its claim was granted, since a refused Provider's master is
// open all the same. ok is false while provider is not enrolled or has not
// said which host it uses (or uses none), so M is not known yet.
func (h *Hosts) OnHostOf(provider string) (n int, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	me, enrolled := h.by[provider]
	if !enrolled || !me.reported || me.login == "" {
		return 0, false
	}
	for _, e := range h.by {
		if e.reported && e.login == me.login {
			n++
		}
	}
	return n, true
}

// Release forgets provider: its manager has stopped.
func (h *Hosts) Release(provider string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.by, provider)
}

package balancer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

// State is what the front computes for one ModelDeployment's group, at every
// join, leave and change of eligibility (HBEP-5, Load bound and cap).
type State struct {
	// Members are the deployment's routable Instances, by endpoint name. A
	// member that leaves has its open reservations released at once.
	Members []string
	// Eligible says which members may take a new request now. A member that
	// is routable but not eligible (paused, AgentUnreachable, back in
	// verifying, without its transport) keeps its place and its count.
	Eligible map[string]bool
	// Percent is KubeAI's MeanLoadPercentage in force: 125 is c = 1.25.
	Percent int
	// Caps is each member's cap in force. Absent or 0: no cap.
	Caps map[string]int
	// Attrs are each member's attributes, which a request's Match reads, so
	// a waiting request can be placed on a member that joined after it.
	Attrs map[string]Attrs
}

// Attrs are what a request's level filter reads of a member.
type Attrs struct {
	Provider  string
	DisplayID string // the deployment ID's display form
}

// Match is a request's filter: whether it may use a member. Nil matches all.
type Match func(member string, a Attrs) bool

// Errors of a pick. Neither reserves anything.
var (
	// ErrNoEligible: no member is eligible and matches the request.
	ErrNoEligible = errors.New("balancer: no eligible member matches")
	// ErrAllAtCap: every eligible matching member holds its cap.
	ErrAllAtCap = errors.New("balancer: every eligible member is at its cap")
)

// noBound is a bound so large that KubeAI's pick is the key's home.
const noBound = 1 << 40

// Group is one ModelDeployment's balancer: KubeAI's CHWBL group, copied
// unchanged under third_party/forked/kubeai, wrapped so that (HBEP-5, Load
// bound and cap):
//
//   - selection and reservation happen under one mutex, because upstream
//     checks the load and reserves under a read lock, so two selectors could
//     both pass on the same loads (L-4.3; spike S10);
//   - each reservation is released exactly once, because upstream's release
//     decrements on every call;
//   - a member that leaves has its open reservations released at once, and
//     their completions change nothing (S10's drain runs);
//   - a member that is only ineligible stays in the group with its count:
//     eligibility is a filter on the walk, as a pinned name is;
//   - the bound and the cap come from State, which the front recomputes from
//     its own count of eligible Instances;
//   - a full choice spills to the next eligible member below its cap on the
//     ring, and when every one is full the request waits in a FIFO here.
type Group struct {
	mu      sync.Mutex
	kg      *loadbalancer.Group
	state   State
	members map[string]bool
	ring    *Ring             // replica for the current members; nil when stale
	pins    map[string]string // member -> a key whose home is that member
	open    map[string]map[*Reservation]struct{}
	queue   []*waiter

	// OnDrift, when set, is called under the group's lock after a placement
	// whose counts disagree: KubeAI's total, the sum of its members' counts,
	// and the wrapper's open reservations (HBEP-5, Operational readiness 3).
	// It must not call back into the group.
	OnDrift func(total, members, open int64)

	// onContended, set by tests only, runs in a caller that finds the lock
	// held, before it waits.
	onContended func()
}

// Reservation is one request counted on one member.
type Reservation struct {
	Member  string
	Spilled bool // the cap moved it off KubeAI's choice

	g        *Group
	release  func() // upstream's
	released bool
}

// Release ends the reservation. Only the first call counts; a reservation a
// leave already released changes nothing.
func (r *Reservation) Release() {
	if r == nil || r.g == nil {
		return
	}
	g := r.g
	g.lock()
	defer g.mu.Unlock()
	if g.releaseLocked(r) {
		g.placeWaitersLocked()
	}
}

// New returns an empty group.
func New() *Group {
	return &Group{
		kg:      loadbalancer.NewGroup(Replication),
		members: map[string]bool{},
		open:    map[string]map[*Reservation]struct{}{},
	}
}

func (g *Group) lock() {
	if g.mu.TryLock() {
		return
	}
	if h := g.onContended; h != nil {
		h()
	}
	g.mu.Lock()
}

// Update applies a new state: members that joined enter the group with a
// count of zero, members that left are removed and their open reservations
// released, and the bound, caps and eligibility take effect. Waiting requests
// are then placed where they now fit. It returns how many reservations the
// leaves released.
func (g *Group) Update(s State) int {
	g.lock()
	defer g.mu.Unlock()
	next := make(map[string]bool, len(s.Members))
	for _, m := range s.Members {
		next[m] = true
	}
	released := 0
	if !sameSet(g.members, next) {
		g.kg.SetEndpoints(keys(next))
		for m := range g.members {
			if !next[m] {
				for r := range g.open[m] {
					g.releaseLocked(r)
					released++
				}
				delete(g.open, m)
			}
		}
		g.members, g.ring, g.pins = next, nil, nil
	}
	attrs := make(map[string]Attrs, len(s.Attrs))
	for k, v := range s.Attrs {
		attrs[k] = v
	}
	g.state = State{Members: keys(next), Eligible: copyBools(s.Eligible), Percent: s.Percent, Caps: copyInts(s.Caps), Attrs: attrs}
	g.placeWaitersLocked()
	return released
}

// TryPick chooses and reserves a member for key among the eligible members
// that match (nil matches all), without waiting.
func (g *Group) TryPick(key string, match Match) (*Reservation, error) {
	g.lock()
	defer g.mu.Unlock()
	return g.pickLocked(key, match)
}

func (g *Group) pickLocked(key string, match Match) (*Reservation, error) {
	ok := func(m string) bool {
		return g.members[m] && g.state.Eligible[m] && (match == nil || match(m, g.state.Attrs[m]))
	}
	if !g.anyLocked(ok) {
		return nil, ErrNoEligible
	}
	choice, done, err := g.kg.PickPrefixHash(key, g.state.Percent)
	if err != nil {
		return nil, ErrNoEligible
	}
	reserved := true
	if !ok(choice) {
		// KubeAI chose a member the request may not use: undo it and take
		// KubeAI's rule on the walk of the eligible ones, with the T and n
		// of the whole group.
		done()
		reserved = false
		choice = g.filteredChoiceLocked(key, ok)
	}
	spilled := false
	load := g.inFlightLocked(choice)
	if reserved {
		load-- // the count includes the request just reserved
	}
	if c := g.state.Caps[choice]; c > 0 && load >= int64(c) {
		if reserved {
			done()
			reserved = false
		}
		if choice = g.spillTargetLocked(key, choice, ok); choice == "" {
			return nil, ErrAllAtCap
		}
		spilled = true
	}
	if !reserved {
		if done, err = g.reserveLocked(choice); err != nil {
			return nil, err
		}
	}
	return g.registerLocked(choice, done, spilled), nil
}

func (g *Group) anyLocked(ok func(string) bool) bool {
	for m := range g.members {
		if ok(m) {
			return true
		}
	}
	return false
}

// filteredChoiceLocked is KubeAI's CHWBL rule (chwblGetAddr) applied to the
// members ok admits: the first on the walk from key whose load before the
// request is at most c * (T + 1) / n, with T and n those of the whole group;
// if none is, the first ok member on the walk, as upstream falls back to its
// first candidate. HBEP-5 does not say what a filtered walk does when no
// eligible member is within the bound; this mirrors upstream.
func (g *Group) filteredChoiceLocked(key string, ok func(string) bool) string {
	total, n := g.kg.TotalInFlight(), g.kg.Members()
	first := ""
	for _, m := range g.ringLocked().Walk(key) {
		if !ok(m) {
			continue
		}
		if first == "" {
			first = m
		}
		if LoadOK(g.inFlightLocked(m), total, n, g.state.Percent) {
			return m
		}
	}
	return first
}

// spillTargetLocked is the next member on the ring after choice, wrapping,
// that ok admits and that holds less than its cap; "" when there is none.
func (g *Group) spillTargetLocked(key, choice string, ok func(string) bool) string {
	walk := g.ringLocked().Walk(key)
	start := -1
	for i, m := range walk {
		if m == choice {
			start = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	for k := 1; k < len(walk); k++ {
		m := walk[(start+k)%len(walk)]
		if !ok(m) {
			continue
		}
		if c := g.state.Caps[m]; c <= 0 || g.inFlightLocked(m) < int64(c) {
			return m
		}
	}
	return ""
}

// LoadOK is KubeAI's bound check (chwblLoadOK), with the bound as a percent:
// a member takes a request while its load before it is at most
// percent/100 * (total + 1) / n. The arithmetic is upstream's, in the same
// order, so the replica and the fork agree at the boundary.
func LoadOK(load, total int64, n, percent int) bool {
	if total == 0 {
		return true
	}
	avg := float64(total+1) / float64(n)
	return float64(load) <= avg*(float64(percent)/100)
}

func (g *Group) inFlightLocked(m string) int64 {
	v, _ := g.kg.InFlight(m)
	return v
}

func (g *Group) ringLocked() *Ring {
	if g.ring == nil {
		g.ring = NewRing(g.state.Members)
	}
	return g.ring
}

// reserveLocked reserves one request on target through upstream's picker, with
// a key whose home is target and no bound, so the counts stay KubeAI's own.
func (g *Group) reserveLocked(target string) (func(), error) {
	pin, err := g.pinLocked(target)
	if err != nil {
		return nil, err
	}
	a, done, err := g.kg.PickPrefixHash(pin, noBound)
	if err != nil {
		return nil, err
	}
	if a != target {
		done()
		return nil, fmt.Errorf("balancer: the pin key for %s landed on %s: the ring replica diverges from the fork", target, a)
	}
	return done, nil
}

func (g *Group) pinLocked(target string) (string, error) {
	if k, ok := g.pins[target]; ok {
		return k, nil
	}
	r := g.ringLocked()
	for i := 0; i < 1_000_000; i++ {
		k := fmt.Sprintf("pin/%s/%d", target, i)
		if r.Home(k) == target {
			if g.pins == nil {
				g.pins = map[string]string{}
			}
			g.pins[target] = k
			return k, nil
		}
	}
	return "", fmt.Errorf("balancer: no pin key found for %s", target)
}

func (g *Group) registerLocked(m string, done func(), spilled bool) *Reservation {
	r := &Reservation{Member: m, Spilled: spilled, g: g, release: done}
	if g.open[m] == nil {
		g.open[m] = map[*Reservation]struct{}{}
	}
	g.open[m][r] = struct{}{}
	if g.OnDrift != nil {
		if total, members, open := g.countsLocked(); total != members || total != open {
			g.OnDrift(total, members, open)
		}
	}
	return r
}

// releaseLocked releases r once; it reports whether this call released it.
func (g *Group) releaseLocked(r *Reservation) bool {
	if r.released {
		return false
	}
	r.released = true
	r.release()
	delete(g.open[r.Member], r)
	return true
}

func (g *Group) countsLocked() (total, members, open int64) {
	total = g.kg.TotalInFlight()
	for m := range g.members {
		members += g.inFlightLocked(m)
	}
	for _, rs := range g.open {
		open += int64(len(rs))
	}
	return total, members, open
}

// Counts returns KubeAI's total in flight, the sum of its members' counts and
// the wrapper's open reservations. They are equal unless something drifted.
func (g *Group) Counts() (total, members, open int64) {
	g.lock()
	defer g.mu.Unlock()
	return g.countsLocked()
}

// InFlight is KubeAI's count for a current member.
func (g *Group) InFlight(m string) (int64, bool) {
	g.lock()
	defer g.mu.Unlock()
	return g.kg.InFlight(m)
}

// Open is the number of the wrapper's open reservations on m.
func (g *Group) Open(m string) int {
	g.lock()
	defer g.mu.Unlock()
	return len(g.open[m])
}

// Members is the number of current members (the n of the bound).
func (g *Group) Members() int {
	g.lock()
	defer g.mu.Unlock()
	return len(g.members)
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func copyBools(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyInts(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

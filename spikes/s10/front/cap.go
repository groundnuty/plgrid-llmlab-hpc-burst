package front

import (
	"errors"
	"fmt"
)

// noBound is KubeAI's bound made effectively infinite (the same value as
// the simulator's AffinityPercent): the pick is the key's home.
const noBound = 1 << 40

// ErrAllAtCap is returned by PickPrefixHashCapped when every member already
// holds cap requests. The caller holds the request at the front and asks
// again when a request completes.
var ErrAllAtCap = errors.New("front: every member is at the hard cap")

// PickPrefixHashCapped is HBEP-5's bounded-load pick plus a hard
// per-Instance cap (spike S10b, option 3). KubeAI's CHWBL chooses at
// meanLoadPercentage. If the choice already holds cap requests, the request
// goes to the next member on the ring after the choice (wrapping) that holds
// fewer, and spilled is true. If no member does, it returns ErrAllAtCap and
// reserves nothing.
//
// Everything runs in one critical section and every reservation, spills
// included, is counted in KubeAI's own counters, so the bound's mean (T and
// n) stays complete. The fork is unchanged: the spill target is reserved
// through upstream's picker with a pin key whose ring home is the target and
// an infinite bound. Upstream's own pick for the choice is released first, so
// the net effect on the counters is one request on the target.
func (b *Balancer) PickPrefixHashCapped(key string, meanLoadPercentage, cap int) (addr string, release func(), spilled bool, err error) {
	if cap < 1 {
		return "", func() {}, false, fmt.Errorf("front: cap %d must be at least 1", cap)
	}
	b.lock()
	defer b.mu.Unlock()
	choice, done, err := b.g.PickPrefixHash(key, meanLoadPercentage)
	if err != nil {
		return "", func() {}, false, err
	}
	load, _ := b.g.InFlight(choice) // includes the request just reserved
	if int(load)-1 < cap {
		return choice, b.registerLocked(choice, done), false, nil
	}
	done() // the choice is full: undo upstream's reservation
	walk := b.ringLocked().walk(key)
	start := -1
	for i, n := range walk {
		if n == choice {
			start = i
		}
	}
	if start < 0 {
		return "", func() {}, false, fmt.Errorf("front: choice %s is not on the ring replica; the replica diverges from the fork", choice)
	}
	for k := 1; k < len(walk); k++ {
		cand := walk[(start+k)%len(walk)]
		if l, _ := b.g.InFlight(cand); int(l) < cap {
			a, d, err := b.reserveLocked(cand)
			if err != nil {
				return "", func() {}, false, err
			}
			return a, b.registerLocked(a, d), true, nil
		}
	}
	return "", func() {}, false, ErrAllAtCap
}

// reserveLocked reserves one request on target through upstream's picker.
func (b *Balancer) reserveLocked(target string) (string, func(), error) {
	pin, err := b.pinLocked(target)
	if err != nil {
		return "", func() {}, err
	}
	a, d, err := b.g.PickPrefixHash(pin, noBound)
	if err != nil {
		return "", func() {}, err
	}
	if a != target {
		d()
		return "", func() {}, fmt.Errorf("front: pin key for %s landed on %s; the replica diverges from the fork", target, a)
	}
	return a, d, nil
}

// pinLocked returns a key whose ring home is target, found on the replica
// and cached until the membership changes.
func (b *Balancer) pinLocked(target string) (string, error) {
	if k, ok := b.pins[target]; ok {
		return k, nil
	}
	r := b.ringLocked()
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("pin/%s/%d", target, i)
		if r.home(k) == target {
			if b.pins == nil {
				b.pins = map[string]string{}
			}
			b.pins[target] = k
			return k, nil
		}
	}
	return "", fmt.Errorf("front: no pin key found for %s", target)
}

// AdaptiveBound is spike S10b's option 4: the tight bound while every
// desired Instance is ready, the loose one while fewer are. Percents are
// KubeAI's MeanLoadPercentage (125 = 1.25 x the mean).
type AdaptiveBound struct{ Tight, Loose int }

// Percent returns the bound for the deployment's ready and desired Instance
// counts. More ready than desired (a replacement surge) counts as complete.
func (a AdaptiveBound) Percent(ready, desired int) int {
	if ready >= desired {
		return a.Tight
	}
	return a.Loose
}

// AdaptiveCapBound is spike S10c's option 5: the tight bound plus a hard cap
// while every desired Instance is ready, the loose bound with no cap while
// fewer are. It is option 4's readiness rule applied to the cap as well as to
// the bound, so that the cap, which spills a full choice onto the next
// Instance and holds requests at the front, works only where it can help: at
// full membership. Below it, the spill target would be an Instance that
// already carries the lost member's share.
type AdaptiveCapBound struct{ Tight, Loose, Cap int }

// Percent is the bound for the deployment's ready and desired counts: the
// same rule as AdaptiveBound.
func (a AdaptiveCapBound) Percent(ready, desired int) int {
	return AdaptiveBound{Tight: a.Tight, Loose: a.Loose}.Percent(ready, desired)
}

// CapAt is the per-Instance cap in force: a.Cap while ready >= desired, and 0
// (no cap) while fewer are ready.
func (a AdaptiveCapBound) CapAt(ready, desired int) int {
	if ready >= desired {
		return a.Cap
	}
	return 0
}

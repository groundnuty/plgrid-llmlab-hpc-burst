package relaykeeper

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// DefaultClaimLapse is how long a claim on a login host outlives its holder's
// relays and sets (the science agent's ruling on #343's Q2, 10 Nov 2026).
const DefaultClaimLapse = 5 * time.Minute

// The login-host claims (HBEP-6, The relay keeper, Capacity and the login
// host; #343's Q2). The keeper is the one owner of the one-host-dialed-
// Provider-per-login-host rule, across leadership generations, and its
// claims are in the state file, so they survive a keeper restart:
//   - A Provider claims a host when a set of its with Allocations names that
//     host and nobody holds it.
//   - A set from another Provider for a held host is refused, naming the
//     holder, while the holder holds relays there, and until the claim lapses.
//   - A claim lapses when its holder has held no relay there and sent no
//     unexpired set for ClaimLapse. A restarted keeper counts from its start,
//     so the Provider that held the host before the restart gets it back
//     first.
//   - The holder releases its claim with a complete set that has no
//     Allocation, or with a set that names another host.
//   - For a host nobody holds, the oldest Provider wins, then by name, among
//     the Providers whose latest unexpired sets with Allocations name it,
//     refused ones included, so a Provider refused while another held the
//     host keeps its place.
//   - A set with no Allocation claims nothing. It is refused only for a host
//     another Provider holds, so that Provider's Ready condition shows the
//     host in use before it gets an Allocation.

// claim decides the claim for s, whose login host is host, on next. It
// returns the holder that refuses s, "" when s may go on, and whether next's
// claims changed. The caller holds the lock.
func (k *Keeper) claim(next *State, s wire.DesiredSet, host string, now time.Time) (holder string, changed bool) {
	p := s.Provider
	wants := len(s.Allocations) > 0
	for h, c := range next.Claims {
		if c.Provider == p && h != host {
			delete(next.Claims, h)
			changed = true
		}
	}
	if c, held := next.Claims[host]; held && c.Provider != p {
		if k.holds(c.Provider, host) || !k.lapsed(c, now) {
			return c.Provider, changed
		}
		delete(next.Claims, host)
		changed = true
	} else if held {
		if s.Complete && !wants {
			delete(next.Claims, host)
			changed = true
		}
		return "", changed
	}
	if !wants {
		return "", changed
	}
	if w := k.oldestWanting(s, host, now); w != p {
		return w, changed
	}
	if next.Claims == nil {
		next.Claims = map[string]Claim{}
	}
	next.Claims[host] = Claim{Provider: p, ProviderUID: s.ProviderUID, Created: s.ProviderCreated.UTC(), Since: now.UTC()}
	return "", true
}

// holds reports whether the keeper keeps any of provider's relays on host.
func (k *Keeper) holds(provider, host string) bool {
	hs := k.sets[provider]
	return len(k.wanted[provider]) > 0 && hs != nil && hs.host == host
}

// lapsed reports whether c has outlived its holder's relays and sets by the
// lapse: the holder's latest set expired (or the keeper started, when it has
// sent none since) at least ClaimLapse ago.
func (k *Keeper) lapsed(c Claim, now time.Time) bool {
	last := k.started
	if w, ok := k.wishes[c.Provider]; ok && w.expires.After(last) {
		last = w.expires
	}
	return now.Sub(last) >= k.o.ClaimLapse
}

// oldestWanting is the oldest Provider, then by name, among s's and those
// whose latest unexpired sets with Allocations name host, refused ones
// included: a Provider refused while another held the host keeps its place.
func (k *Keeper) oldestWanting(s wire.DesiredSet, host string, now time.Time) string {
	best, bestCreated := s.Provider, s.ProviderCreated
	for q, w := range k.wishes {
		if q == s.Provider || w.host != host || w.entries == 0 || !now.Before(w.expires) {
			continue
		}
		if w.created.Before(bestCreated) || (w.created.Equal(bestCreated) && q < best) {
			best, bestCreated = q, w.created
		}
	}
	return best
}

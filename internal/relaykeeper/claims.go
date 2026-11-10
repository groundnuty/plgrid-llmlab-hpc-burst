package relaykeeper

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// DefaultClaimLapse is how long a claim on a login host outlives its holder's
// relays and sets (the science agent's ruling on #343's Q2, 10 Nov 2026).
const DefaultClaimLapse = 5 * time.Minute

// The login-host claims (HBEP-6, The relay keeper, Capacity and the login
// host; #343's Q2, as #347 states it). The keeper is the one owner of the
// one-host-dialed-Provider-per-login-host rule, across leadership
// generations, and its claims are in the state file, so they survive a
// keeper restart. A Provider is its name and its UID: a claim made under
// another UID of the same name belongs to another Provider.
//   - A Provider claims an unclaimed host when a set of its with Allocations
//     names that host and it wins the host's rule: the oldest Provider, then
//     by name, among the Providers whose latest unexpired sets name the host,
//     refused sets and sets with no Allocation included.
//   - A set from another Provider for a claimed host is refused, naming the
//     holder, until the claim lapses. It lapses when its holder sends a
//     complete set with no Allocation on that host, when its holder names
//     another host, or when its holder has held no relay there and sent no
//     unexpired set for ClaimLapse, timed on the keeper's monotonic clock
//     from the later of the keeper's start and the holder's last unexpired
//     set. So the Provider that held a host before a keeper restart keeps it.
//   - A set with no Allocation claims nothing.

// claim decides the claim for s, whose login host is host, on next. It
// returns the holder that refuses s, "" when s may go on, and whether next's
// claims changed. The caller holds the lock, and has recorded s's wish.
func (k *Keeper) claim(next *State, s wire.DesiredSet, host string, now time.Time) (holder string, changed bool) {
	p := s.Provider
	mine := func(c Claim) bool { return c.Provider == p && c.ProviderUID == s.ProviderUID }
	wants := len(s.Allocations) > 0
	for h, c := range next.Claims {
		if mine(c) && h != host {
			delete(next.Claims, h)
			changed = true
		}
	}
	if c, held := next.Claims[host]; held && !mine(c) {
		if k.holds(c, host) || !k.lapsed(c, now) {
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
	if w := k.oldestWanting(s, host, now); w != p {
		return w, changed
	}
	if !wants {
		return "", changed
	}
	if next.Claims == nil {
		next.Claims = map[string]Claim{}
	}
	next.Claims[host] = Claim{Provider: p, ProviderUID: s.ProviderUID, Created: s.ProviderCreated.UTC(), Since: now.UTC()}
	return "", true
}

// holds reports whether the keeper keeps any relay of c's Provider, under its
// UID, on host.
func (k *Keeper) holds(c Claim, host string) bool {
	hs := k.sets[c.Provider]
	return len(k.wanted[c.Provider]) > 0 && hs != nil && hs.host == host && hs.set.ProviderUID == c.ProviderUID
}

// lapsed reports whether c has outlived its holder's relays and sets by the
// lapse: the holder's latest set, under its UID, expired (or the keeper
// started, when it has sent none since) at least ClaimLapse ago.
func (k *Keeper) lapsed(c Claim, now time.Time) bool {
	last := k.started
	if w, ok := k.wishes[c.Provider]; ok && w.uid == c.ProviderUID && w.expires.After(last) {
		last = w.expires
	}
	return now.Sub(last) >= k.o.ClaimLapse
}

// oldestWanting is the oldest Provider, then by name, among s's and those
// whose latest unexpired sets name host, refused ones and ones with no
// Allocation included: a Provider refused while another held the host keeps
// its place.
func (k *Keeper) oldestWanting(s wire.DesiredSet, host string, now time.Time) string {
	best, bestCreated := s.Provider, s.ProviderCreated
	for q, w := range k.wishes {
		if q == s.Provider || w.host != host || !now.Before(w.expires) {
			continue
		}
		if w.created.Before(bestCreated) || (w.created.Equal(bestCreated) && q < best) {
			best, bestCreated = q, w.created
		}
	}
	return best
}

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
// another UID of the same name belongs to another Provider, and every part
// of a claim's lapse is that Provider's own.
//   - A Provider claims an unclaimed host when a set of its with Allocations
//     names that host and it wins the host's rule: the oldest Provider, then
//     by name, among the Providers whose latest unexpired sets name the host,
//     refused sets and sets with no Allocation included.
//   - A set from another Provider for a claimed host is refused, naming the
//     holder, while a relay of the holder runs there (a process, not a Want:
//     a relay that waits or retries holds nothing), and until the claim
//     lapses.
//   - A claim lapses, once no relay of its holder runs there, when its
//     holder's latest accepted set is complete with no Allocation, or names
//     another host, or when its holder has sent no unexpired set for
//     ClaimLapse, timed on the keeper's monotonic clock from the later of the
//     keeper's start and the holder's last unexpired set. So the Provider
//     that held a host before a keeper restart keeps it, and a holder's set
//     that is refused for another host releases nothing: its relays here run
//     on (the review of PR 354).
//   - A claim is released when another Provider's set finds it lapsed, never
//     while its holder's relays run there.
//   - A set with no Allocation claims nothing.

// claim decides the claim for s, whose login host is host, on next. It
// returns the holder that refuses s, "" when s may go on, and whether next's
// claims changed. The caller holds the lock, and has recorded s's wish.
func (k *Keeper) claim(next *State, s wire.DesiredSet, host string, now time.Time) (holder string, changed bool) {
	p := s.Provider
	me := provKey{p, s.ProviderUID}
	if c, held := next.Claims[host]; held && (provKey{c.Provider, c.ProviderUID}) != me {
		if k.holdsRunning(c, host) || !k.lapsed(c, host, now) {
			return c.Provider, false
		}
		delete(next.Claims, host)
		changed = true
	} else if held {
		return "", false
	}
	if w := k.oldestWanting(me, s.ProviderCreated, host, now); w != p {
		return w, changed
	}
	if len(s.Allocations) == 0 {
		return "", changed
	}
	if next.Claims == nil {
		next.Claims = map[string]Claim{}
	}
	next.Claims[host] = Claim{Provider: p, ProviderUID: s.ProviderUID, Created: s.ProviderCreated.UTC(), Since: now.UTC()}
	return "", true
}

// holdsRunning reports whether a relay of c's Provider runs on host: the
// Runner shows it up, a process of it alive (one still stopping included),
// on that login host, started under c's UID. A relay is counted for the
// Provider UID it started under, never for the latest Want of its tag, so a
// re-created Provider's Want cannot hide its predecessor's running process
// (the delta review of PR 354). One that names no UID is counted for any.
func (k *Keeper) holdsRunning(c Claim, host string) bool {
	for _, r := range k.runner.Relays(c.Provider) {
		if r.State == wire.StateUp && r.LoginHost == host && (r.ProviderUID == "" || r.ProviderUID == c.ProviderUID) {
			return true
		}
	}
	return false
}

// lapsed reports whether c has lapsed by one of its rules, given that no
// relay of its holder runs on host: the holder's latest accepted set, under
// its UID, is complete with no Allocation or names another host; or the
// holder's latest set, under its UID, expired (or the keeper started, when
// it has sent none since) at least ClaimLapse ago.
func (k *Keeper) lapsed(c Claim, host string, now time.Time) bool {
	if hs := k.sets[c.Provider]; hs != nil && hs.set.ProviderUID == c.ProviderUID {
		if hs.host != host || (hs.set.Complete && len(hs.set.Allocations) == 0) {
			return true
		}
	}
	last := k.started
	if w, ok := k.wishes[provKey{c.Provider, c.ProviderUID}]; ok && w.expires.After(last) {
		last = w.expires
	}
	return now.Sub(last) >= k.o.ClaimLapse
}

// oldestWanting is the oldest Provider, then by name, among me and those
// whose latest unexpired sets name host, refused ones and ones with no
// Allocation included: a Provider refused while another held the host keeps
// its place.
func (k *Keeper) oldestWanting(me provKey, created time.Time, host string, now time.Time) string {
	best, bestCreated := me.name, created
	for q, w := range k.wishes {
		if q == me || w.host != host || !now.Before(w.expires) {
			continue
		}
		if w.created.Before(bestCreated) || (w.created.Equal(bestCreated) && q.name < best) {
			best, bestCreated = q.name, w.created
		}
	}
	return best
}

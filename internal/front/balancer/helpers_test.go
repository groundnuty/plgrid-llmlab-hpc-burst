package balancer

// Helpers shared by the balancer tests.
//
// Every expected value in these tests is worked out by hand from KubeAI's
// rule, or comes from the plain fork group (third_party/forked/kubeai), never
// from the wrapper under test: a candidate on the ring walk is accepted when
// its load before the request is at most c * (T + 1) / n, T the group's total
// in flight and n its number of members; if none is, the first candidate on
// the walk is taken.
//
// No test in this package may call t.Parallel: every pick reads the fork's
// global instrument, which the concurrency tests swap through
// loadbalancer.SetSelectionHook.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

var (
	three = []string{"a", "b", "c"}
	four  = []string{"a", "b", "c", "d"}
)

// The two bounds of HBEP-5: c = 1.25 at full membership, 2.0 below it.
const (
	tight = 125
	loose = 200
)

// stateOf is a State in which every member is eligible, with the same cap on
// each member (0: no cap). Eligible must always be filled: a member missing
// from it is not eligible.
func stateOf(members []string, percent, capEach int) State {
	s := State{Members: members, Eligible: map[string]bool{}, Percent: percent}
	for _, m := range members {
		s.Eligible[m] = true
	}
	if capEach > 0 {
		s.Caps = map[string]int{}
		for _, m := range members {
			s.Caps[m] = capEach
		}
	}
	return s
}

// withProviders gives each named member the Provider attribute a filter reads.
func withProviders(s State, providers map[string]string) State {
	s.Attrs = map[string]Attrs{}
	for m, p := range providers {
		s.Attrs[m] = Attrs{Provider: p}
	}
	return s
}

// providerIs is a filter that admits the members of one provider.
func providerIs(provider string) Match {
	return func(_ string, a Attrs) bool { return a.Provider == provider }
}

func newGroupOf(members []string, percent, capEach int) *Group {
	g := New()
	g.Update(stateOf(members, percent, capEach))
	return g
}

// forkOf is the plain copied group, the oracle for what KubeAI picks.
func forkOf(members []string) *loadbalancer.Group {
	f := loadbalancer.NewGroup(256)
	f.SetEndpoints(members)
	return f
}

// forkHome is the fork's pick at an unbounded load, released at once.
func forkHome(t *testing.T, f *loadbalancer.Group, key string) string {
	t.Helper()
	addr, done, err := f.PickPrefixHash(key, noBound)
	if err != nil {
		t.Fatal(err)
	}
	done()
	return addr
}

// forkWalk is the order in which KubeAI's ring visits the members from key,
// taken from the fork alone: the home among all, then the home among the
// rest, and so on. It does not use Ring.
func forkWalk(t *testing.T, members []string, key string) []string {
	t.Helper()
	rest := members
	var out []string
	for len(rest) > 0 {
		h := forkHome(t, forkOf(rest), key)
		out = append(out, h)
		rest = without(rest, h)
	}
	return out
}

func without(members []string, drop string) []string {
	var out []string
	for _, m := range members {
		if m != drop {
			out = append(out, m)
		}
	}
	return out
}

// acquirer reserves one request for key and returns its member and release.
type acquirer func(key string) (string, func(), error)

func groupAcquirer(g *Group) acquirer {
	return func(key string) (string, func(), error) {
		r, err := g.TryPick(key, nil)
		if err != nil {
			return "", nil, err
		}
		return r.Member, r.Release, nil
	}
}

func forkAcquirer(f *loadbalancer.Group, percent int) acquirer {
	return func(key string) (string, func(), error) { return f.PickPrefixHash(key, percent) }
}

// loadUp holds open requests until each member carries want[member], by pure
// affinity on generated keys: the caller must have opened the bound wide
// (noBound) and set no caps. It returns the holds, member by member.
func loadUp(t *testing.T, acquire acquirer, want map[string]int) map[string][]func() {
	t.Helper()
	need := map[string]int{}
	left := 0
	for m, n := range want {
		need[m] = n
		left += n
	}
	held := map[string][]func(){}
	for i := 0; left > 0; i++ {
		if i > 100000 {
			t.Fatalf("could not place the loads %v", want)
		}
		addr, done, err := acquire(fmt.Sprintf("load-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if need[addr] > 0 {
			need[addr]--
			left--
			held[addr] = append(held[addr], done)
		} else {
			done()
		}
	}
	return held
}

func mustPick(t *testing.T, g *Group, key string) *Reservation {
	t.Helper()
	r, err := g.TryPick(key, nil)
	if err != nil {
		t.Fatalf("TryPick(%q): %v", key, err)
	}
	return r
}

// wantCounts checks KubeAI's total, the members' sum and the open
// reservations against a number worked out by hand.
func wantCounts(t *testing.T, g *Group, want int64, when string) {
	t.Helper()
	total, members, open := g.Counts()
	if total != want || members != want || open != want {
		t.Fatalf("%s: total %d, members' sum %d, open %d; want all %d", when, total, members, open, want)
	}
}

func wantInFlight(t *testing.T, g *Group, member string, want int64, when string) {
	t.Helper()
	if got, ok := g.InFlight(member); !ok || got != want {
		t.Fatalf("%s: %s in flight = %d (member %v), want %d", when, member, got, ok, want)
	}
	if got := g.Open(member); int64(got) != want {
		t.Fatalf("%s: %s has %d open reservations, want %d", when, member, got, want)
	}
}

// waitForWaiting blocks until n requests are in the FIFO. The deadline only
// turns a hang into a failure; nothing depends on how long the wait takes.
func waitForWaiting(t *testing.T, g *Group, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for g.Waiting() != n {
		if time.Now().After(deadline) {
			t.Fatalf("Waiting() never reached %d (now %d)", n, g.Waiting())
		}
		time.Sleep(time.Millisecond)
	}
}

// result is what one PickOrWait call returned.
type result struct {
	id  int
	r   *Reservation
	err error
}

// startWaiter runs PickOrWait on a goroutine and reports on out.
func startWaiter(ctx context.Context, g *Group, id int, key string, match Match, out chan<- result) {
	go func() {
		r, err := g.PickOrWait(ctx, key, match)
		out <- result{id: id, r: r, err: err}
	}()
}

// recv waits for one result; the timeout only turns a hang into a failure.
func recv(t *testing.T, out <-chan result) result {
	t.Helper()
	select {
	case res := <-out:
		return res
	case <-time.After(10 * time.Second):
		t.Fatal("no result from PickOrWait")
		return result{}
	}
}

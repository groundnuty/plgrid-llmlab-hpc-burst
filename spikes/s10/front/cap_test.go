package front

// Spike S10b: the ring replica, the hard per-Instance cap and the adaptive
// bound. The fork is the oracle wherever a test needs one: the replica is
// checked against the fork's own picks, and a spill's target against the
// fork's pick on a group with the full Instances removed.

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

func plainGroup(members []string) *loadbalancer.Group {
	g := loadbalancer.NewGroup(256)
	g.SetEndpoints(members)
	return g
}

func balancerOf(members []string) *Balancer {
	b := New(256)
	b.SetEndpoints(members)
	return b
}

// The replica's home must equal the fork's pick at an infinite bound, for
// every key, with four members and again with three.
func TestRingMatchesUpstream(t *testing.T) {
	for _, members := range [][]string{names, names[:3], names[1:]} {
		g := plainGroup(members)
		r := newRing(members, 256)
		seen := map[string]int{}
		for i := 0; i < 5000; i++ {
			key := fmt.Sprintf("key-%d", i)
			want := home(t, g, key)
			if got := r.home(key); got != want {
				t.Fatalf("members %v key %q: replica home %s, fork home %s", members, key, got, want)
			}
			seen[want]++
		}
		if len(seen) != len(members) {
			t.Errorf("members %v: only %v are home to any of 5000 keys", members, seen)
		}
	}
}

// Under a finite bound the fork picks the first member on its ring walk
// that passes the load check, so the replica's walk order must reproduce its
// picks from uneven loads. The test counts the picks that are not the home,
// so a walk that only got the home right would not pass.
func TestRingWalkMatchesUpstreamUnderLoad(t *testing.T) {
	loads := []map[string]int{
		{"a": 30, "b": 18, "c": 18, "d": 18},
		{"a": 9, "b": 0, "c": 30, "d": 2},
		{"a": 0, "b": 0, "c": 40, "d": 0},
	}
	for _, load := range loads {
		for _, pct := range []int{125, 200} {
			g := plainGroup(names)
			loadUp(t, g, maps(load))
			r := newRing(names, 256)
			spills := 0
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("walk-%d", i)
				w := r.walk(key)
				addr, done, err := g.PickPrefixHash(key, pct)
				if err != nil {
					t.Fatal(err)
				}
				done()
				want := w[0]
				total, n := g.TotalInFlight(), g.Members()
				for _, name := range w {
					if l, _ := g.InFlight(name); float64(l) <= float64(total+1)/float64(n)*float64(pct)/100 {
						want = name
						break
					}
				}
				if addr != want {
					t.Fatalf("loads %v c=%d key %q: fork picked %s, the replica's walk %v gives %s", load, pct, key, addr, w, want)
				}
				if addr != w[0] {
					spills++
				}
			}
			if pct == 125 && spills == 0 {
				t.Errorf("loads %v c=%d: no pick left the home, the walk beyond the home was never tested", load, pct)
			}
		}
	}
}

func maps(m map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// total in flight must equal the members' sum, always: a spill that bypassed
// KubeAI's counters would make the bound's mean wrong (the L-4.3 failure).
func checkCounts(t *testing.T, b *Balancer, want map[string]int) {
	t.Helper()
	var sum int64
	for _, n := range names {
		got, ok := b.InFlight(n)
		if !ok {
			continue
		}
		sum += got
		if int(got) != want[n] {
			t.Errorf("%s in flight %d, want %d", n, got, want[n])
		}
	}
	if b.TotalInFlight() != sum {
		t.Errorf("total in flight %d, members hold %d", b.TotalInFlight(), sum)
	}
}

// A request whose choice holds cap requests goes to the next member on the
// ring after it; the oracle is the fork's pick on a group without the full
// members. The home is filled to the cap of 3, then its successor, then the
// successor's, until every member is full, which must queue.
func TestCapSpillsToTheNextOnTheRing(t *testing.T) {
	const cap = 3
	b := balancerOf(names)
	key := "k"
	want := map[string]int{}
	var full []string
	for step := 0; step < len(names); step++ {
		// the fork's pick for key among the members not yet full
		var rest []string
		for _, n := range names {
			if !slices.Contains(full, n) {
				rest = append(rest, n)
			}
		}
		target := home(t, plainGroup(rest), key)
		for i := 0; i < cap; i++ {
			addr, _, spilled, err := b.PickPrefixHashCapped(key, noBound, cap)
			if err != nil {
				t.Fatalf("step %d request %d: %v", step, i, err)
			}
			if addr != target {
				t.Fatalf("step %d request %d: went to %s, the next on the ring is %s", step, i, addr, target)
			}
			if wantSpill := step > 0; spilled != wantSpill {
				t.Fatalf("step %d: spilled %v, want %v", step, spilled, wantSpill)
			}
			want[addr]++
			checkCounts(t, b, want)
		}
		full = append(full, target)
	}
	// every member now holds the cap: the request waits and reserves nothing
	if _, _, _, err := b.PickPrefixHashCapped(key, noBound, cap); !errors.Is(err, ErrAllAtCap) {
		t.Fatalf("all members full: got %v, want ErrAllAtCap", err)
	}
	checkCounts(t, b, want)
}

// Below the cap, the capped pick is the plain pick, on the same state.
func TestCapBelowTheCapIsThePlainPick(t *testing.T) {
	capped, plain := balancerOf(names), balancerOf(names)
	for i := 0; i < 400; i++ {
		key := fmt.Sprintf("conv-%d", i%37)
		a, _, spilled, err := capped.PickPrefixHashCapped(key, 200, 1000)
		if err != nil {
			t.Fatal(err)
		}
		p, _, err := plain.PickPrefixHash(key, 200)
		if err != nil {
			t.Fatal(err)
		}
		if a != p || spilled {
			t.Fatalf("request %d: capped %s (spilled %v), plain %s", i, a, spilled, p)
		}
	}
}

// The cap binds in this sequence, and it is not vacuous: the plain pick on
// the same keys puts more than the cap on one member, the capped pick never
// does, and it spills. 60 requests on 6 conversations, cap 10, c = 2.0.
func TestCapBindsWherePlainExceedsIt(t *testing.T) {
	const cap = 10
	capped, plain := balancerOf(names), balancerOf(names)
	spills := 0
	for i := 0; i < 36; i++ {
		key := fmt.Sprintf("conv-%d", i%6)
		_, _, spilled, err := capped.PickPrefixHashCapped(key, 200, cap)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if spilled {
			spills++
		}
		if _, _, err := plain.PickPrefixHash(key, 200); err != nil {
			t.Fatal(err)
		}
	}
	peak := func(b *Balancer) (m int64) {
		for _, n := range names {
			l, _ := b.InFlight(n)
			m = max(m, l)
		}
		return m
	}
	if peak(plain) <= cap {
		t.Fatalf("plain c=2.0 peaks at %d, not above the cap %d: this sequence does not test the cap", peak(plain), cap)
	}
	if peak(capped) > cap {
		t.Errorf("capped peaks at %d, above the cap %d", peak(capped), cap)
	}
	if spills == 0 {
		t.Error("the cap never spilled a request")
	}
	t.Logf("plain peak %d, capped peak %d, %d spills", peak(plain), peak(capped), spills)
}

// A draining member's spilled-in reservations are released with the rest, and
// their later completions do nothing.
func TestDrainReleasesSpilledReservations(t *testing.T) {
	const cap = 2
	b := balancerOf(names)
	var rels []func()
	hosts := map[string]int{}
	for i := 0; i < 8; i++ { // fills the home, then spills to the rest
		addr, rel, _, err := b.PickPrefixHashCapped("k", noBound, cap)
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, rel)
		hosts[addr]++
	}
	if len(hosts) != 4 {
		t.Fatalf("8 requests at cap 2 landed on %v, want all four members full", hosts)
	}
	var spillTarget string
	for n, c := range hosts {
		if n != home(t, plainGroup(names), "k") && c == cap {
			spillTarget = n
			break
		}
	}
	if got := b.Drain(spillTarget); got != cap {
		t.Fatalf("drain released %d open reservations, want %d", got, cap)
	}
	if got, want := b.TotalInFlight(), int64(8-cap); got != want {
		t.Fatalf("after the drain the total is %d, want %d", got, want)
	}
	for _, rel := range rels { // completions of every request, drained ones included
		rel()
		rel()
	}
	if b.TotalInFlight() != 0 {
		t.Fatalf("total %d after every request completed, want 0", b.TotalInFlight())
	}
}

func TestCapRejectsANonPositiveCap(t *testing.T) {
	b := balancerOf(names)
	if _, _, _, err := b.PickPrefixHashCapped("k", 200, 0); err == nil {
		t.Fatal("cap 0 was accepted")
	}
	if b.TotalInFlight() != 0 {
		t.Fatalf("a rejected pick reserved %d", b.TotalInFlight())
	}
}

func TestAdaptiveBoundSwitchesWithReadiness(t *testing.T) {
	a := AdaptiveBound{Tight: 125, Loose: 200}
	for _, c := range []struct{ ready, desired, want int }{
		{4, 4, 125}, // every desired Instance is ready
		{3, 4, 200}, // one short
		{1, 4, 200},
		{0, 4, 200},
		{5, 4, 125}, // a replacement surge counts as complete
	} {
		if got := a.Percent(c.ready, c.desired); got != c.want {
			t.Errorf("ready %d of %d: %d, want %d", c.ready, c.desired, got, c.want)
		}
	}
}

// Spike S10c, option 5: the bound and the cap follow readiness together.
// Expected values are written down from the rule (tight bound and cap while
// ready >= desired, loose bound and no cap below), not read from the code.
func TestAdaptiveCapBoundFollowsReadiness(t *testing.T) {
	a := AdaptiveCapBound{Tight: 125, Loose: 200, Cap: 48}
	for _, c := range []struct{ ready, desired, wantPct, wantCap int }{
		{4, 4, 125, 48}, // every desired Instance is ready: tight bound, cap
		{3, 4, 200, 0},  // one short: loose bound, no cap
		{1, 4, 200, 0},
		{0, 4, 200, 0},
		{5, 4, 125, 48}, // a replacement surge counts as complete
	} {
		if got := a.Percent(c.ready, c.desired); got != c.wantPct {
			t.Errorf("ready %d of %d: bound %d, want %d", c.ready, c.desired, got, c.wantPct)
		}
		if got := a.CapAt(c.ready, c.desired); got != c.wantCap {
			t.Errorf("ready %d of %d: cap %d, want %d", c.ready, c.desired, got, c.wantCap)
		}
	}
	// the bound's half is option 4's rule
	for ready := 0; ready <= 5; ready++ {
		if a.Percent(ready, 4) != (AdaptiveBound{Tight: 125, Loose: 200}).Percent(ready, 4) {
			t.Errorf("ready %d: the bound differs from AdaptiveBound's", ready)
		}
	}
}

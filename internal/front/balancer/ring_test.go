package balancer

// The ring replica (Ring, LoadOK) against the fork's own picks, as S10b's
// tests did, and TryPick against the plain fork group under the same loads.
// The fork is the oracle throughout: no expected pick here is read from the
// wrapper.

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// The replica's home must equal the fork's pick at an unbounded load, for
// every key, with four members, with three, and with another three.
func TestRingMatchesUpstream(t *testing.T) {
	for _, members := range [][]string{four, four[:3], four[1:]} {
		f := forkOf(members)
		r := NewRing(members)
		seen := map[string]int{}
		for i := 0; i < 5000; i++ {
			key := fmt.Sprintf("key-%d", i)
			want := forkHome(t, f, key)
			if got := r.Home(key); got != want {
				t.Fatalf("members %v key %q: replica home %s, fork home %s", members, key, got, want)
			}
			w := r.Walk(key)
			distinct := map[string]bool{}
			for _, m := range w {
				distinct[m] = true
			}
			if len(w) != len(members) || len(distinct) != len(members) || w[0] != want {
				t.Fatalf("members %v key %q: walk %v, want every member once, starting at %s", members, key, w, want)
			}
			seen[want]++
		}
		if len(seen) != len(members) {
			t.Errorf("members %v: only %v are home to any of 5000 keys", members, seen)
		}
	}
}

// The whole walk, not only its head: the fork's order of visit from a key is
// its home, then the home of the group without it, and so on.
func TestRingWalkOrderMatchesTheForkRemovingHomes(t *testing.T) {
	for _, members := range [][]string{four, three} {
		r := NewRing(members)
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("order-%d", i)
			if got, want := r.Walk(key), forkWalk(t, members, key); !slices.Equal(got, want) {
				t.Fatalf("members %v key %q: replica walk %v, fork order %v", members, key, got, want)
			}
		}
	}
}

// Under a finite bound the fork picks the first member on its ring walk that
// passes the load check, so the replica's walk order must reproduce its picks
// from uneven loads. The test counts the picks that are not the home, so a
// walk that only got the home right would not pass.
func TestRingWalkMatchesUpstreamUnderLoad(t *testing.T) {
	loads := []map[string]int{
		{"a": 30, "b": 18, "c": 18, "d": 18},
		{"a": 9, "b": 0, "c": 30, "d": 2},
		{"a": 0, "b": 0, "c": 40, "d": 0},
	}
	r := NewRing(four)
	for _, load := range loads {
		for _, pct := range []int{tight, loose} {
			f := forkOf(four)
			loadUp(t, forkAcquirer(f, noBound), load)
			spills := 0
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("walk-%d", i)
				w := r.Walk(key)
				addr, done, err := f.PickPrefixHash(key, pct)
				if err != nil {
					t.Fatal(err)
				}
				done()
				// KubeAI's rule, written out: the first on the walk whose load
				// before the request is at most c * (T + 1) / n, else the first.
				want := w[0]
				total, n := f.TotalInFlight(), f.Members()
				for _, name := range w {
					if l, _ := f.InFlight(name); float64(l) <= float64(total+1)/float64(n)*float64(pct)/100 {
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
			if pct == tight && spills == 0 {
				t.Errorf("loads %v c=%d: no pick left the home, the walk beyond the home was never tested", load, pct)
			}
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := NewRing(nil)
	if r.Home("k") != "" || r.Walk("k") != nil {
		t.Fatalf("empty ring: home %q, walk %v", r.Home("k"), r.Walk("k"))
	}
	if got := NewRing([]string{"a", "a", "b"}).Walk("k"); len(got) != 2 {
		t.Fatalf("a repeated name must count once: walk %v", got)
	}
}

// LoadOK against the fork's pick at KubeAI's boundary. Four members, c = 1.25.
//
//	loads 5 (home), 4, 3, 3: T = 15, bound 1.25 * 16 / 4 = 5.0, and 5 <= 5, so
//	  the fork keeps the home (fork test TestKubeAIAdmitsOneOverStrictCapacityAtEquality).
//	loads 6 (home), 4, 3, 3: T = 16, bound 1.25 * 17 / 4 = 5.3125, 6 > 5.3125,
//	  so the fork leaves the home.
//	T = 0: any load passes.
func TestLoadOKAgreesWithTheForkAtTheBoundary(t *testing.T) {
	for _, c := range []struct {
		homeLoad int
		total    int64
		wantOK   bool
	}{
		{5, 15, true},
		{6, 16, false},
	} {
		f := forkOf(four)
		home := forkHome(t, f, "k")
		want := map[string]int{home: c.homeLoad}
		others := []int{4, 3, 3}
		for _, m := range without(four, home) {
			want[m], others = others[0], others[1:]
		}
		loadUp(t, forkAcquirer(f, noBound), want)
		if f.TotalInFlight() != c.total {
			t.Fatalf("home load %d: total %d, want %d", c.homeLoad, f.TotalInFlight(), c.total)
		}
		addr, done, err := f.PickPrefixHash("k", tight)
		if err != nil {
			t.Fatal(err)
		}
		done()
		if kept := addr == home; kept != c.wantOK {
			t.Fatalf("home load %d: the fork kept the home = %v, want %v (by hand)", c.homeLoad, kept, c.wantOK)
		}
		if got := LoadOK(int64(c.homeLoad), c.total, 4, tight); got != c.wantOK {
			t.Errorf("LoadOK(%d, %d, 4, %d) = %v, want %v", c.homeLoad, c.total, tight, got, c.wantOK)
		}
	}
	if !LoadOK(9, 0, 4, tight) {
		t.Error("LoadOK with nothing in flight must pass any load")
	}
}

// With every member eligible and no cap, TryPick picks what the plain fork
// group picks at the same percent under the same loads. Two groups are driven
// in step with seeded random keys (skewed, so some members overload) and
// random completions; each pick, and each member's count, must agree.
func TestTryPickIsThePlainPickWithoutCapOrFilter(t *testing.T) {
	rng := rand.New(rand.NewSource(20261031))
	ref := forkOf(four) // unloaded, for the homes
	type pair struct {
		r    *Reservation
		done func()
	}
	for _, pct := range []int{tight, loose} {
		g := newGroupOf(four, pct, 0)
		f := forkOf(four)
		var opens []pair
		offHome := 0
		for step := 0; step < 3000; step++ {
			key := fmt.Sprintf("conv-%d", rng.Intn(1+rng.Intn(6)))
			r, err := g.TryPick(key, nil)
			if err != nil {
				t.Fatalf("c=%d step %d key %q: %v", pct, step, key, err)
			}
			addr, done, err := f.PickPrefixHash(key, pct)
			if err != nil {
				t.Fatal(err)
			}
			if r.Member != addr || r.Spilled {
				t.Fatalf("c=%d step %d key %q: TryPick chose %s (spilled %v), the plain group %s", pct, step, key, r.Member, r.Spilled, addr)
			}
			if addr != forkHome(t, ref, key) {
				offHome++
			}
			opens = append(opens, pair{r, done})
			if rng.Intn(2) == 0 {
				j := rng.Intn(len(opens))
				opens[j].r.Release()
				opens[j].done()
				opens = append(opens[:j], opens[j+1:]...)
			}
			if total, _, _ := g.Counts(); total != f.TotalInFlight() {
				t.Fatalf("c=%d step %d: total %d, the plain group %d", pct, step, total, f.TotalInFlight())
			}
			for _, m := range four {
				a, _ := g.InFlight(m)
				b, _ := f.InFlight(m)
				if a != b {
					t.Fatalf("c=%d step %d: %s holds %d, the plain group %d", pct, step, m, a, b)
				}
			}
		}
		if pct == tight && offHome == 0 {
			t.Error("no pick left its home at c = 1.25: the walk beyond the home was never tested")
		}
		t.Logf("c=%d: %d of 3000 picks left their home", pct, offHome)
	}
}

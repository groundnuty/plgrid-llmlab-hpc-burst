// New file, not from KubeAI (plgrid-llmlab-hpc-burst spike S10).
//
// Properties of the copied group that a front built on it must work around.
// Expected values are worked out by hand from KubeAI's rule: a candidate is
// accepted when its load before the request is at most c * (T + 1) / n, with
// T the group's total in flight and n the number of endpoints.

package loadbalancer

import (
	"sync"
	"testing"
	"time"
)

// homeOf returns the endpoint a key maps to on an unloaded ring.
func homeOf(t *testing.T, G *Group, key string) string {
	t.Helper()
	addr, done, err := G.PickPrefixHash(key, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	done()
	return addr
}

// setLoads adds in-flight requests to endpoints, as open requests would.
func setLoads(G *Group, loads map[string]int64) {
	G.g.mtx.RLock()
	defer G.g.mtx.RUnlock()
	for name, n := range loads {
		G.g.addInFlight(G.g.endpoints[name].inFlight, n)
	}
}

// The strict capacity of Mirrokni, Thorup and Zadimoghaddam is
// ceil(c * (T + 1) / n) after the request. KubeAI's "<=" before the request
// admits one more when c * (T + 1) / n is a whole number. Four endpoints,
// loads 5 (home), 4, 3, 3: T = 15, c * (T + 1) / n = 1.25 * 16 / 4 = 5, the
// home's 5 <= 5 passes, and it ends at 6 against a capacity of 5.
func TestKubeAIAdmitsOneOverStrictCapacityAtEquality(t *testing.T) {
	G := newGroup(t, "a", "b", "c", "d")
	home := homeOf(t, G, "k")
	loads := map[string]int64{}
	others := []int64{4, 3, 3}
	for _, n := range []string{"a", "b", "c", "d"} {
		if n == home {
			loads[n] = 5
		} else {
			loads[n], others = others[0], others[1:]
		}
	}
	setLoads(G, loads)
	got, _, err := G.PickPrefixHash("k", 125)
	if err != nil {
		t.Fatal(err)
	}
	if got != home {
		t.Fatalf("picked %s, want the home %s: its load 5 is within KubeAI's threshold 5", got, home)
	}
	if n, _ := G.InFlight(home); n != 6 {
		t.Fatalf("home in-flight %d, want 6 (one over the strict capacity 5)", n)
	}
}

// Upstream's release function decrements every time it is called. A front
// that releases a request at drain start and again when it completes
// counts it twice, and the group total goes below the true load.
func TestUpstreamReleaseDecrementsOnEveryCall(t *testing.T) {
	G := newGroup(t, "a", "b")
	addr, done, err := G.PickPrefixHash("k", 125)
	if err != nil {
		t.Fatal(err)
	}
	done()
	done()
	if G.TotalInFlight() != -1 {
		t.Fatalf("total after two releases of one request = %d, want -1", G.TotalInFlight())
	}
	if n, _ := G.InFlight(addr); n != -1 {
		t.Fatalf("%s in-flight after two releases = %d, want -1", addr, n)
	}
}

// getBestAddr runs CHWBL's load check and the increment under the group's
// read lock, so two selectors can both check before either increments.
// Loads 25 (home), 18, 18, 18: T = 79, and the bound for a new request is
// 1.25 * 80 / 4 = 25, so the home's 25 passes for both. The first selector
// is held between its check and its increment (SetSelectionHook) until the
// second has passed its own check; nothing depends on timing. Both land on
// the home, which ends at 27. The second reservation's load before it is
// 26 with T = 80 before it, against 1.25 * 81 / 4 = 25.3125: above the bound.
// This is the plain group; spikes/s10/front serializes it and holds.
func TestConcurrentSelectorsBothPassOnThePlainGroup(t *testing.T) {
	G := newGroup(t, "a", "b", "c", "d")
	home := homeOf(t, G, "k")
	loads := map[string]int64{}
	for _, n := range []string{"a", "b", "c", "d"} {
		loads[n] = 18
	}
	loads[home] = 25
	setLoads(G, loads)

	arrived := make(chan string, 2)
	proceed := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(proceed) }) })
	var mu sync.Mutex
	seen := 0
	restore := SetSelectionHook(func(ep string) {
		mu.Lock()
		seen++
		first := seen == 1
		mu.Unlock()
		arrived <- ep
		if first {
			<-proceed
		}
	})
	defer restore()

	picks := make(chan string, 2)
	pick := func() {
		addr, _, err := G.PickPrefixHash("k", 125)
		if err != nil {
			t.Error(err)
		}
		picks <- addr
	}
	await := func(what string) string {
		select {
		case ep := <-arrived:
			return ep
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never reached the point between check and increment", what)
			return ""
		}
	}
	go pick()
	if ep := await("the first selector"); ep != home {
		t.Fatalf("first selector chose %s at the check, want the home %s", ep, home)
	}
	go pick()
	if ep := await("the second selector, while the first held the read lock,"); ep != home {
		t.Fatalf("second selector chose %s at the check, want the home %s", ep, home)
	}
	once.Do(func() { close(proceed) })
	a, b := <-picks, <-picks
	if a != home || b != home {
		t.Fatalf("picks %s and %s, want both on the home %s", a, b, home)
	}
	n, _ := G.InFlight(home)
	total := G.TotalInFlight()
	if n != 27 || total != 81 {
		t.Fatalf("home in-flight %d, total %d; want 27 and 81", n, total)
	}
	pre, before := float64(n-1), float64(total-1)
	if bound := 1.25 * (before + 1) / 4; pre <= bound {
		t.Fatalf("second reservation: load before %v within the bound %v; the interleaving did not break it", pre, bound)
	}
}

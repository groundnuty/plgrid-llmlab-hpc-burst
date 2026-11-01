package balancer

// Concurrency: spike S10's hook-forced interleaving, ported to the product
// wrapper (HBEP-5, Test plan: Concurrency). Nothing here depends on timing:
// the first selector is parked inside the fork's selection hook, between its
// load check and its reservation, and is released only after the test has
// seen the second selector either reach its own check (overlapped) or find
// the lock held (blocked). A lucky schedule cannot pass or fail it.
//
// Hand-worked, four members, c = 1.25, loads 25 (the home of "k"), 18, 18, 18:
//
//	T = 79 before the first request: bound 1.25 * 80 / 4 = 25; the home holds
//	25 <= 25, so the first selector chooses the home.
//	If the second selector checks before the first has reserved (plain
//	group): it sees the same T = 79 and the same 25 <= 25, and chooses the
//	home too. The home ends at 27, total 81: the second request was placed on
//	a member at 26 against the bound 1.25 * 81 / 4 = 25.3125.
//	If it checks after (serialised): T = 80, bound 25.3125, the home holds 26
//	> 25.3125, so it goes on along the ring to the next member, which holds
//	18 <= 25.3125. The home ends at 26, total 81.
//
// These tests set a global (loadbalancer.SetSelectionHook): they must never
// call t.Parallel.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

// outcome of two selections of key "k".
type outcome struct {
	home       string
	picks      [2]string
	overlapped bool
	homeLoad   int64
	total      int64
}

// heldBound checks the outcome against the bound, by hand (see above).
func (o outcome) heldBound() (bool, string) {
	if o.picks[0] == o.home && o.picks[1] == o.home {
		return false, fmt.Sprintf("both requests went to the home %s, the later at load 26 against the bound 25.3125", o.home)
	}
	if o.overlapped {
		return false, "the second selector passed its load check while the first was between its check and its reservation"
	}
	if o.homeLoad != 26 || o.total != 81 {
		return false, fmt.Sprintf("home load %d and total %d, want 26 and 81", o.homeLoad, o.total)
	}
	return true, ""
}

// scenario is one implementation under test.
type scenario struct {
	// acquire reserves without a bound, to load the group up.
	acquire acquirer

	// afterLoad sets c = 1.25; nil for the plain group.
	afterLoad func()

	// pick is one selection of "k" at c = 1.25.
	pick func() (string, error)

	inflight func(string) int64
	total    func() int64

	// setContended installs the wrapper's lock-wait seam; nil for the plain
	// group, which has no lock to wait on.
	setContended func(func())
}

func twoSelectors(t *testing.T, sc scenario) outcome {
	t.Helper()
	o := outcome{home: forkHome(t, forkOf(four), "k")}
	want := map[string]int{}
	for _, m := range four {
		want[m] = 18
	}
	want[o.home] = 25
	loadUp(t, sc.acquire, want)
	if sc.afterLoad != nil {
		sc.afterLoad()
	}
	for _, m := range four {
		if got := sc.inflight(m); got != int64(want[m]) {
			t.Fatalf("%s holds %d before the scenario, want %d", m, got, want[m])
		}
	}
	if got := sc.total(); got != 79 {
		t.Fatalf("total %d before the scenario, want 79", got)
	}

	atCheck := make(chan string, 2)
	contended := make(chan struct{}, 2)
	proceed := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(proceed) }) }) // a failure must not leave a selector parked
	var mu sync.Mutex
	seen := 0
	restore := loadbalancer.SetSelectionHook(func(ep string) {
		mu.Lock()
		seen++
		first := seen == 1
		mu.Unlock()
		atCheck <- ep
		if first {
			<-proceed
		}
	})
	defer restore()
	if sc.setContended != nil {
		// The seam must never block: it runs on every contended lock.
		sc.setContended(func() {
			select {
			case contended <- struct{}{}:
			default:
			}
		})
	}

	picks := make(chan [2]string, 2)
	pick := func(i int) {
		addr, err := sc.pick()
		if err != nil {
			t.Errorf("selector %d: %v", i, err)
		}
		picks <- [2]string{fmt.Sprint(i), addr}
	}
	timeout := time.After(10 * time.Second)
	go pick(0)
	select {
	case <-atCheck:
	case <-timeout:
		t.Fatal("the first selector never reached its check")
	}
	go pick(1)
	select {
	case <-atCheck:
		o.overlapped = true
	case <-contended:
	case <-timeout:
		t.Fatal("the second selector neither reached its check nor waited for the lock")
	}
	once.Do(func() { close(proceed) })
	for range 2 {
		r := <-picks
		if r[0] == "0" {
			o.picks[0] = r[1]
		} else {
			o.picks[1] = r[1]
		}
	}
	if sc.setContended != nil {
		sc.setContended(nil) // both selectors have reported back: nothing reads the seam now
	}
	o.homeLoad, o.total = sc.inflight(o.home), sc.total()
	return o
}

func TestSerializedSelectorsHoldTheBound(t *testing.T) {
	g := newGroupOf(four, noBound, 0)
	o := twoSelectors(t, scenario{
		acquire:   groupAcquirer(g),
		afterLoad: func() { g.Update(stateOf(four, tight, 0)) },
		pick: func() (string, error) {
			r, err := g.TryPick("k", nil)
			if err != nil {
				return "", err
			}
			return r.Member, nil
		},
		inflight:     func(m string) int64 { v, _ := g.InFlight(m); return v },
		total:        func() int64 { total, _, _ := g.Counts(); return total },
		setContended: func(h func()) { g.onContended = h },
	})
	t.Logf("home %s, picks %v, home load %d, total %d", o.home, o.picks, o.homeLoad, o.total)
	if ok, why := o.heldBound(); !ok {
		t.Fatal(why)
	}
	// The first selector took the home; the second went to the next member on
	// the ring, which the fork names as its pick among the members but the home.
	second := forkHome(t, forkOf(without(four, o.home)), "k")
	if o.picks[0] != o.home || o.picks[1] != second {
		t.Fatalf("picks %v: want the first on the home %s and the second on %s", o.picks, o.home, second)
	}
	// 79 + 2 requests, and KubeAI's total, the members' sum and the open
	// reservations agree on it.
	wantCounts(t, g, 81, "after both selections")
	wantInFlight(t, g, o.home, 26, "the home")
	wantInFlight(t, g, second, 19, "the second selector's member")
}

// The same scenario on the plain copied group: the control that shows the
// scenario can fail.
func TestPlainGroupBreaksTheBoundInTheSameScenario(t *testing.T) {
	f := forkOf(four)
	o := twoSelectors(t, scenario{
		acquire: forkAcquirer(f, noBound),
		pick: func() (string, error) {
			addr, _, err := f.PickPrefixHash("k", tight)
			return addr, err
		},
		inflight: func(m string) int64 { v, _ := f.InFlight(m); return v },
		total:    f.TotalInFlight,
	})
	t.Logf("home %s, picks %v, home load %d, total %d", o.home, o.picks, o.homeLoad, o.total)
	if ok, _ := o.heldBound(); ok {
		t.Fatal("the plain group held the bound; the scenario proves nothing")
	}
	// By hand: both saw T = 79 and the home at 25 <= 25, so both took it.
	if !o.overlapped || o.picks[0] != o.home || o.picks[1] != o.home || o.homeLoad != 27 || o.total != 81 {
		t.Fatalf("plain group: overlapped %v, picks %v, home load %d, total %d; want both selectors on the home %s, load 27, total 81",
			o.overlapped, o.picks, o.homeLoad, o.total, o.home)
	}
}

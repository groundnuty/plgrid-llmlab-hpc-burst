package front

// Expected values are worked out by hand from KubeAI's rule, which HBEP-5's
// owner adopted as the bound: a candidate is accepted when its load before
// the request is at most c * (T + 1) / n, T the total in flight and n the
// number of members.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

var names = []string{"a", "b", "c", "d"}

// picker is what both the plain group and the Balancer offer.
type picker interface {
	PickPrefixHash(key string, meanLoadPercentage int) (string, func(), error)
}

// loadUp holds open requests until each endpoint carries want[endpoint],
// using pure affinity on generated keys. It returns the home of "k".
func loadUp(t *testing.T, p picker, want map[string]int) {
	t.Helper()
	for i := 0; ; i++ {
		left := 0
		for _, n := range want {
			left += n
		}
		if left == 0 {
			return
		}
		if i > 100000 {
			t.Fatalf("could not place the loads %v", want)
		}
		addr, done, err := p.PickPrefixHash(fmt.Sprintf("load-%d", i), 1<<40)
		if err != nil {
			t.Fatal(err)
		}
		if want[addr] > 0 {
			want[addr]--
		} else {
			done()
		}
	}
}

func home(t *testing.T, p picker, key string) string {
	t.Helper()
	addr, done, err := p.PickPrefixHash(key, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	done()
	return addr
}

// outcome of two selections of key "k" at c = 1.25 from loads 25 (home),
// 18, 18, 18, where the first selector is held between its load check and
// its reservation until the second has either passed its own check
// (overlapped) or been seen waiting for the lock (blocked).
type outcome struct {
	home          string
	picks         [2]string
	overlapped    bool
	homeLoad, tot int64
}

// heldBound checks each reservation against the bound, by hand: T = 79
// before the first and 80 before the second. Both on the home means the
// later one found it at 26 against 1.25 * 81 / 4 = 25.3125. One on the home
// means it found 25 against at most 1.25 * 80 / 4 = 25 (or 25.3125), and an
// 18 elsewhere passes either way.
func (o outcome) heldBound() (bool, string) {
	if o.picks[0] == o.home && o.picks[1] == o.home {
		return false, fmt.Sprintf("both requests went to the home %s, the later at load 26 against the bound 25.3125", o.home)
	}
	if o.overlapped {
		return false, "the second selector passed its load check while the first was between its check and its reservation"
	}
	if o.homeLoad != 26 || o.tot != 81 {
		return false, fmt.Sprintf("home load %d and total %d, want 26 and 81", o.homeLoad, o.tot)
	}
	return true, ""
}

// twoSelectors runs the scenario. setContended installs the Balancer's
// lock-wait hook; it is nil for the plain group, which has no lock to wait
// on. The first selector parks in the selection hook; the test releases it
// only after it has seen the second selector either reach its own check
// or wait for the lock. Nothing depends on timing; the timeout only turns a
// hang into a failure.
func twoSelectors(t *testing.T, p picker, inflight func(string) int64, total func() int64, setContended func(func())) outcome {
	t.Helper()
	o := outcome{home: home(t, p, "k")}
	want := map[string]int{}
	for _, n := range names {
		want[n] = 18
	}
	want[o.home] = 25
	loadUp(t, p, want)

	atCheck := make(chan string, 2)
	contended := make(chan struct{}, 2)
	proceed := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(proceed) }) })
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
	if setContended != nil {
		setContended(func() { contended <- struct{}{} })
		defer setContended(nil)
	}

	picks := make(chan [2]string, 2)
	pick := func(i int) {
		addr, _, err := p.PickPrefixHash("k", 125)
		if err != nil {
			t.Error(err)
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
	o.homeLoad, o.tot = inflight(o.home), total()
	return o
}

func TestSerializedSelectorsHoldTheBound(t *testing.T) {
	b := New(256)
	b.SetEndpoints(names)
	o := twoSelectors(t, b,
		func(n string) int64 { v, _ := b.InFlight(n); return v },
		b.TotalInFlight,
		func(h func()) { b.onContended = h })
	t.Logf("home %s, picks %v, home load %d, total %d", o.home, o.picks, o.homeLoad, o.tot)
	if ok, why := o.heldBound(); !ok {
		t.Fatal(why)
	}
	if o.picks[0] != o.home || o.picks[1] == o.home {
		t.Fatalf("picks %v: want the first on the home %s and the second spilled", o.picks, o.home)
	}
}

// The same scenario on the plain copied group: the control that shows the
// scenario can fail.
func TestPlainGroupBreaksTheBoundInTheSameScenario(t *testing.T) {
	g := loadbalancer.NewGroup(256)
	g.SetEndpoints(names)
	o := twoSelectors(t, g,
		func(n string) int64 { v, _ := g.InFlight(n); return v },
		g.TotalInFlight, nil)
	t.Logf("home %s, picks %v, home load %d, total %d", o.home, o.picks, o.homeLoad, o.tot)
	if ok, _ := o.heldBound(); ok {
		t.Fatal("the plain group held the bound; the scenario proves nothing")
	}
}

// A second release of one request changes nothing (upstream's would
// decrement again: fork test TestUpstreamReleaseDecrementsOnEveryCall).
func TestReleaseTwiceIsANoOp(t *testing.T) {
	b := New(256)
	b.SetEndpoints(names)
	addr, release, err := b.PickPrefixHash("k", 125)
	if err != nil {
		t.Fatal(err)
	}
	_, other, _ := b.PickPrefixHash("k2", 125)
	release()
	release()
	if got := b.TotalInFlight(); got != 1 {
		t.Fatalf("total after releasing one of two requests twice = %d, want 1", got)
	}
	if n, _ := b.InFlight(addr); n < 0 {
		t.Fatalf("%s in-flight %d after a double release", addr, n)
	}
	other()
	if got := b.TotalInFlight(); got != 0 {
		t.Fatalf("total after both released = %d, want 0", got)
	}
}

// Drain releases the draining endpoint's open requests at once; their
// completion later is a no-op. Remove keeps them counted (KubeAI's own
// behaviour). Three requests are held on "a", one on "b".
func TestDrainReleasesOpenRequestsOnce(t *testing.T) {
	for _, drain := range []bool{false, true} {
		b := New(256)
		b.SetEndpoints(names)
		var onA, onB []func()
		for i := 0; len(onA) < 3 || len(onB) < 1; i++ {
			addr, rel, err := b.PickPrefixHash(fmt.Sprintf("key-%d", i), 1<<40)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case addr == "a" && len(onA) < 3:
				onA = append(onA, rel)
			case addr == "b" && len(onB) < 1:
				onB = append(onB, rel)
			default:
				rel()
			}
		}
		want := int64(4)
		if drain {
			if n := b.Drain("a"); n != 3 {
				t.Fatalf("drain released %d, want 3", n)
			}
			want = 1
		} else {
			b.Remove("a")
		}
		if got := b.TotalInFlight(); got != want {
			t.Fatalf("drain=%v: total after removing a = %d, want %d", drain, got, want)
		}
		for _, rel := range onA {
			rel() // the drained requests complete
		}
		if got := b.TotalInFlight(); got != 1 {
			t.Fatalf("drain=%v: total after a's requests completed = %d, want 1 (b's)", drain, got)
		}
		onB[0]()
		if got := b.TotalInFlight(); got != 0 {
			t.Fatalf("drain=%v: total at the end = %d, want 0", drain, got)
		}
	}
}

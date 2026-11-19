package balancer

// Two selectors at the last tokens of one member cannot both pass (HBEP-5, Workloads, The count in tokens; Load bound and
// cap: selection and reservation happen under one mutex). The interleaving is forced by a seam, not left to a schedule:
// the first selector is parked after its member has passed the conditions and before its charge is counted
// (Group.beforeRegister), and is released only once the test has seen the second selector either arrive at that same
// point (overlapped: the check and its effect were not one step) or find the lock held (blocked). After
// concurrency_test.go's pattern, with the seam in the wrapper instead of the fork, because the fork's hook fires before the
// wrapper's check and could not park a selector between the wrapper's check and its count.
//
// By hand: cap 4, B 100, nothing in flight, two Workload requests of 60 tokens each.
//
//	the first alone:     4*60 + 0 = 240 <= 400, so it passes.
//	the second after it: 4*(60+60) + 0 = 480 > 400, so it is refused.
//	both at once on the same view (the race): both pass, N = 120 against B = 100.
//
// These tests call no t.Parallel.

import (
	"errors"
	"testing"
	"time"
)

func TestTwoSelectorsAtTheLastTokensOfAMemberCannotBothPass(t *testing.T) {
	g := New()
	g.Update(budgeted([]string{"m"}, "m"))

	atRegister := make(chan struct{}, 2)
	gate := make(chan struct{})
	contended := make(chan struct{}, 2)
	g.beforeRegister = func() {
		atRegister <- struct{}{}
		<-gate
	}
	g.onContended = func() {
		select {
		case contended <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { g.beforeRegister, g.onContended = nil, nil })

	type outcome struct {
		id  int
		r   *Reservation
		err error
	}
	results := make(chan outcome, 2)
	pick := func(id int) {
		r, err := g.TryPickFor("k", nil, workload(60))
		results <- outcome{id, r, err}
	}
	timeout := time.After(10 * time.Second)

	go pick(0)
	select {
	case <-atRegister:
	case <-timeout:
		t.Fatal("the first selector never reached its registration")
	}
	go pick(1)
	overlapped := false
	select {
	case <-atRegister:
		overlapped = true
	case <-contended:
	case <-timeout:
		t.Fatal("the second selector neither reached its registration nor waited for the lock")
	}

	// Release the parked selectors one at a time, each only after the one before it has finished, so that even a broken
	// group never runs two registrations at once. A selector that is refused never parks, so it only reports.
	got := map[int]outcome{}
	for len(got) < 2 {
		select {
		case gate <- struct{}{}:
			select {
			case o := <-results:
				got[o.id] = o
			case <-timeout:
				t.Fatal("a released selector never finished")
			}
		case o := <-results:
			got[o.id] = o
		case <-timeout:
			t.Fatal("the selectors never finished")
		}
	}

	if overlapped {
		t.Error("the second selector passed its check while the first was between its check and its count")
	}
	admitted, refused := 0, 0
	for _, o := range got {
		switch {
		case o.err == nil:
			admitted++
		case errors.Is(o.err, ErrAllAtCap):
			refused++
		default:
			t.Errorf("selector %d: unexpected error %v", o.id, o.err)
		}
	}
	if admitted != 1 || refused != 1 {
		t.Fatalf("%d admitted and %d refused, want exactly one of each: two requests of 60 tokens do not fit a budget of 100", admitted, refused)
	}
	if got[0].err != nil {
		t.Errorf("the first selector, parked after its check, was refused: %v", got[0].err)
	}
	wantCharged(t, g, "m", 60, 0, "after both selections")
	wantInFlight(t, g, "m", 1, "after both selections")
	wantCounts(t, g, 1, "after both selections")
}

package hostdial

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A Provider whose profile sets transport.loginHostBudget answers a limit at every attempt to take a pair (HBEP-4, open
// question 17): with a limit of 10 (a cap of 5), five Allocations each hold their pair and the sixth waits. The default
// stays 4 where no limit is given or the limit reports none (an unset budget).
func TestAcquireWithAdmitsFiveAllocationsUnderALimitOf10(t *testing.T) {
	s := NewSlots(DefaultRelaysPerLogin)
	ctx := context.Background()
	limit := func() (int, bool) { return 10, true }
	for i := 1; i <= 5; i++ {
		short, cancel := context.WithTimeout(ctx, 2*time.Second)
		if _, err := s.AcquireWith(short, "login-a", 2, limit); err != nil {
			cancel()
			t.Fatalf("Allocation %d of a limit of 10 did not get its pair: %v", i, err)
		}
		cancel()
	}
	if n := s.InUse("login-a"); n != 10 {
		t.Fatalf("InUse = %d, want 10: five pairs", n)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.AcquireWith(short, "login-a", 2, limit); err == nil {
		t.Fatal("a sixth pair under a limit of 10 was given")
	}
	// A limit that reports none (an unset budget): the default of 4.
	login := "login-c"
	for range 2 {
		if _, err := s.AcquireWith(ctx, login, 2, func() (int, bool) { return 0, false }); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.AcquireWith(short, login, 2, func() (int, bool) { return 0, false }); err == nil {
		t.Fatal("a third pair on a node whose limit reports none was given: the default is 4")
	}
}

// The limit is read at EVERY attempt, under the slot lock, and again while a call waits: a raised limit reaches a waiter
// with nothing told to the Slots (no update path), and a lowered one never revokes a pair held, it only refuses the next
// new pair. A limit read once, at the call, would leave the waiter waiting for good.
func TestAcquireWithReadsTheLimitAtEachAttempt(t *testing.T) {
	s := NewSlots(DefaultRelaysPerLogin)
	ctx := context.Background()
	var lim atomic.Int64
	lim.Store(4)
	limit := func() (int, bool) { return int(lim.Load()), true }
	for range 2 {
		if _, err := s.AcquireWith(ctx, "login-a", 2, limit); err != nil {
			t.Fatal(err)
		}
	}
	got := make(chan func(), 1)
	go func() {
		if r, err := s.AcquireWith(ctx, "login-a", 2, limit); err == nil {
			got <- r
		}
	}()
	select {
	case <-got:
		t.Fatal("a third pair was given under a limit of 4")
	case <-time.After(100 * time.Millisecond):
	}
	lim.Store(6) // the budget is raised: the waiter reads it at its next attempt
	var third func()
	select {
	case third = <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting Allocation did not read the raised limit")
	}
	// Lowered below what is held: nothing is taken back, and a fourth NEW pair is refused.
	lim.Store(2)
	if n := s.InUse("login-a"); n != 6 {
		t.Fatalf("InUse = %d after the limit was lowered, want the 6 held", n)
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := s.AcquireWith(short, "login-a", 2, limit); err == nil {
		t.Fatal("a new pair was given above the lowered limit")
	}
	// A freed pair under the lowered limit is not given to a new one either, until the held fall under it.
	third()
	if n := s.InUse("login-a"); n != 4 {
		t.Fatalf("InUse = %d after a pair was released, want 4", n)
	}
	short2, cancel2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()
	if _, err := s.AcquireWith(short2, "login-a", 2, limit); err == nil {
		t.Fatal("a new pair was given with 4 held under a limit of 2")
	}
}

// OnHostOf is M: the Providers that said they use the same login host, whether or not their claim was granted.
func TestHostsOnHostOfCountsTheStacksProvidersOnTheHost(t *testing.T) {
	h := NewHosts()
	now := time.Now()
	if _, ok := h.OnHostOf("a"); ok {
		t.Fatal("an unenrolled Provider has a count")
	}
	h.Enroll("a", now)
	if _, ok := h.OnHostOf("a"); ok {
		t.Fatal("a Provider that has not said which host it uses has a count")
	}
	h.Claim("login-1", "a")
	if n, ok := h.OnHostOf("a"); !ok || n != 1 {
		t.Fatalf("OnHostOf = %d, %v; want 1, true", n, ok)
	}
	h.Enroll("b", now.Add(time.Second))
	h.Claim("LOGIN-1", "b") // refused: a holds the host; still a master there
	h.Enroll("c", now.Add(2*time.Second))
	h.Claim("login-2", "c")
	if n, _ := h.OnHostOf("a"); n != 2 {
		t.Fatalf("OnHostOf(a) = %d, want 2: a and b share login-1", n)
	}
	if n, _ := h.OnHostOf("b"); n != 2 {
		t.Fatalf("OnHostOf(b) = %d, want 2 (a refused Provider's master counts)", n)
	}
	if n, _ := h.OnHostOf("c"); n != 1 {
		t.Fatalf("OnHostOf(c) = %d, want 1: another host", n)
	}
	h.Release("b")
	if n, _ := h.OnHostOf("a"); n != 1 {
		t.Fatalf("OnHostOf(a) = %d after b stopped, want 1", n)
	}
}

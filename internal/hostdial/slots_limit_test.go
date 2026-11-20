package hostdial

import (
	"context"
	"testing"
	"time"
)

// A Provider whose profile sets transport.loginHostBudget gives its login node a limit of 2 relays for each Allocation
// of its cap (HBEP-4, open question 17): at a cap of 5, five Allocations each hold their pair, and the sixth waits. The
// default stays 4 on every node with no limit.
func TestSlotsLimitLetsFiveAllocationsHoldTheirPairs(t *testing.T) {
	s := NewSlots(DefaultRelaysPerLogin)
	ctx := context.Background()
	s.SetLimit("login-a", 10) // a cap of 5
	for i := 1; i <= 5; i++ {
		short, cancel := context.WithTimeout(ctx, 2*time.Second)
		if _, err := s.Acquire(short, "login-a", 2); err != nil {
			cancel()
			t.Fatalf("Allocation %d of a cap of 5 did not get its pair: %v", i, err)
		}
		cancel()
	}
	if n := s.InUse("login-a"); n != 10 {
		t.Fatalf("InUse = %d, want 10: five pairs", n)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(short, "login-a", 2); err == nil {
		t.Fatal("a sixth pair on a node limited to 10 was given")
	}
	// Another node keeps the default of 4.
	if _, err := s.Acquire(ctx, "login-b", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, "login-b", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(short, "login-b", 2); err == nil {
		t.Fatal("a third pair on a node with no limit of its own was given")
	}
}

// A raised limit wakes an Allocation already waiting for its pair; a lowered one keeps the pairs held and holds the
// next back (HBEP-4: lowering a live stack's share frees its connections only once the Allocations above the new cap
// have drained); a cleared one is the default again.
func TestSlotsLimitIsReadAgainByWaiters(t *testing.T) {
	s := NewSlots(DefaultRelaysPerLogin)
	ctx := context.Background()
	rel := make([]func(), 0, 3)
	for range 2 {
		r, err := s.Acquire(ctx, "login-a", 2)
		if err != nil {
			t.Fatal(err)
		}
		rel = append(rel, r)
	}
	got := make(chan struct{})
	go func() {
		if r, err := s.Acquire(ctx, "login-a", 2); err == nil {
			rel = append(rel, r)
			close(got)
		}
	}()
	select {
	case <-got:
		t.Fatal("a third pair was given at the default limit of 4")
	case <-time.After(100 * time.Millisecond):
	}
	s.SetLimit("login-a", 6) // the budget is raised: a cap of 3
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting Allocation did not read the raised limit")
	}
	// Lowered below what is held: nothing is taken back, and the next pair waits.
	s.SetLimit("login-a", 2)
	if n := s.InUse("login-a"); n != 6 {
		t.Fatalf("InUse = %d after the limit was lowered, want the 6 held", n)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(short, "login-a", 2); err == nil {
		t.Fatal("a pair was given above the lowered limit")
	}
	if s.Limit("login-a") != 2 {
		t.Fatalf("Limit = %d, want 2", s.Limit("login-a"))
	}
	// A limit of 0 admits none, and waits rather than failing: the budget may rise.
	s.SetLimit("login-b", 0)
	if _, err := s.Acquire(short, "login-b", 2); err == nil || short.Err() == nil {
		t.Fatalf("a pair under a limit of 0: %v, want a wait that ends with the context", err)
	}
	s.ClearLimit("login-a")
	if s.Limit("login-a") != DefaultRelaysPerLogin {
		t.Fatalf("Limit = %d after ClearLimit, want the default %d", s.Limit("login-a"), DefaultRelaysPerLogin)
	}
	s.SetLimit("login-a", -1) // a negative n changes nothing
	if s.Limit("login-a") != DefaultRelaysPerLogin {
		t.Fatalf("a negative limit changed the limit to %d", s.Limit("login-a"))
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

// New file, not from KubeAI (plgrid-llmlab-hpc-burst spike S10).
//
// Checks that the fork compiles and that the copied CHWBL behaves as its
// documentation says. Expected values are worked out by hand from the rule
// "an endpoint is skipped when its load exceeds the bound", where KubeAI's
// bound for a new request is (MeanLoadPercentage/100) * (total in-flight + 1) / n.

package loadbalancer

import "testing"

func newGroup(t *testing.T, names ...string) *Group {
	t.Helper()
	G := NewGroup(256)
	G.SetEndpoints(names)
	if G.Members() != len(names) {
		t.Fatalf("members = %d, want %d", G.Members(), len(names))
	}
	return G
}

// A key always maps to the same endpoint while loads are within the bound.
func TestSameKeySameEndpointWhenUnloaded(t *testing.T) {
	G := newGroup(t, "a", "b", "c", "d")
	first, done, err := G.PickPrefixHash("conversation-1", 125)
	if err != nil {
		t.Fatal(err)
	}
	done()
	for i := 0; i < 20; i++ {
		got, done, err := G.PickPrefixHash("conversation-1", 125)
		if err != nil {
			t.Fatal(err)
		}
		done()
		if got != first {
			t.Fatalf("pick %d went to %s, want %s", i, got, first)
		}
	}
}

// With two endpoints and a bound of 1.25x: one key held open repeatedly.
// Request 1: total 0, so any load is accepted -> home (load 1 after).
// Request 2: home load 1, bound 1.25*(1+1)/2 = 1.25, 1 <= 1.25 -> home (2).
// Request 3: home load 2, bound 1.25*(2+1)/2 = 1.875, 2 > 1.875 -> other (1).
// Request 4: home load 2, bound 1.25*(3+1)/2 = 2.5, 2 <= 2.5 -> home (3).
func TestBoundSpillsToNextEndpoint(t *testing.T) {
	G := newGroup(t, "a", "b")
	home, _, err := G.PickPrefixHash("k", 125)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false, true} // is the pick the home endpoint? (requests 2..4)
	for i, wantHome := range want {
		got, _, err := G.PickPrefixHash("k", 125)
		if err != nil {
			t.Fatal(err)
		}
		if (got == home) != wantHome {
			t.Fatalf("request %d: got %s (home %s), want home=%v", i+2, got, home, wantHome)
		}
	}
	if G.TotalInFlight() != 4 {
		t.Fatalf("total in-flight = %d, want 4", G.TotalInFlight())
	}
}

// Pure affinity: an effectively infinite bound never spills.
func TestHugeBoundNeverSpills(t *testing.T) {
	G := newGroup(t, "a", "b", "c", "d")
	home, _, _ := G.PickPrefixHash("k", 1<<40)
	for i := 0; i < 100; i++ {
		got, _, _ := G.PickPrefixHash("k", 1<<40)
		if got != home {
			t.Fatalf("request %d spilled to %s", i, got)
		}
	}
	if n, _ := G.InFlight(home); n != 101 {
		t.Fatalf("home in-flight = %d, want 101", n)
	}
}

// The release function must still decrement after the endpoint was removed;
// otherwise the group total leaks and every later bound loosens.
func TestReleaseAfterRemovalDecrementsTotal(t *testing.T) {
	G := newGroup(t, "a", "b")
	addr, done, err := G.PickPrefixHash("k", 125)
	if err != nil {
		t.Fatal(err)
	}
	G.RemoveEndpoint(addr)
	if G.Members() != 1 {
		t.Fatalf("members = %d, want 1", G.Members())
	}
	if G.TotalInFlight() != 1 {
		t.Fatalf("total before release = %d, want 1 (removed endpoint's request still counted)", G.TotalInFlight())
	}
	done()
	if G.TotalInFlight() != 0 {
		t.Fatalf("total after release = %d, want 0", G.TotalInFlight())
	}
	got, _, _ := G.PickPrefixHash("k", 125)
	if got == addr {
		t.Fatalf("removed endpoint %s was selected", addr)
	}
}

// With no endpoints the wrapper must return an error, not block.
func TestNoEndpointsReturnsError(t *testing.T) {
	G := NewGroup(256)
	if _, _, err := G.PickPrefixHash("k", 125); err == nil {
		t.Fatal("want an error with no endpoints")
	}
}

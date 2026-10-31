package balancer

// Filters (HBEP-5, Load bound and cap: Filters). For a pinned name, a
// qualified model@provider or an ineligible Instance, the walk skips the
// members that do not match, and T and n stay those of the whole group. The
// rule on the walk is KubeAI's: accept the first admitted member whose load
// before the request is at most c * (T + 1) / n; if none is, take the first
// admitted member on the walk, as upstream falls back to its first candidate.
//
// Four members, key "k" with the walk w0, w1, w2, w3 taken from the fork,
// c = 1.25, no caps.

import (
	"errors"
	"testing"
)

// loadedGroup returns a group holding exactly loads, at c = 1.25, whose
// members carry the given providers (the attribute a filter reads).
func loadedGroup(t *testing.T, members []string, loads map[string]int, providers map[string]string) *Group {
	t.Helper()
	g := newGroupOf(members, noBound, 0)
	loadUp(t, groupAcquirer(g), loads)
	g.Update(withProviders(stateOf(members, tight, 0), providers))
	return g
}

// Loads w0 4, w1 4, w2 4, w3 1: T = 13, n = 4, bound 1.25 * 14 / 4 = 4.375.
//
//	No filter: w0 holds 4 <= 4.375: w0 (the plain group agrees).
//	Admit w2 and w3 only: w0 and w1 are skipped; w2 holds 4 <= 4.375: w2.
//
// Had T and n been those of the admitted members alone (T = 5, n = 2), the
// bound would be 1.25 * 6 / 2 = 3.75 and w2 (4) would fail: w3 would be taken.
// So the answer w2 shows that T and n stay those of the whole group.
func TestAFilterKeepsTAndNOfTheWholeGroup(t *testing.T) {
	w := forkWalk(t, four, "k")
	loads := map[string]int{w[0]: 4, w[1]: 4, w[2]: 4, w[3]: 1}

	f := forkOf(four)
	loadUp(t, forkAcquirer(f, noBound), loads)
	if addr, _, err := f.PickPrefixHash("k", tight); err != nil || addr != w[0] {
		t.Fatalf("the plain group picked %s (err %v), by hand w0 %s", addr, err, w[0])
	}

	g := loadedGroup(t, four, loads, map[string]string{w[0]: "pa", w[1]: "pa", w[2]: "pb", w[3]: "pb"})
	wantCounts(t, g, 13, "before the filtered request")
	r, err := g.TryPick("k", providerIs("pb"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Member != w[2] || r.Spilled {
		t.Fatalf("filtered pick: %s (spilled %v), want w2 %s", r.Member, r.Spilled, w[2])
	}
	wantInFlight(t, g, w[2], 5, "the chosen member")
	wantInFlight(t, g, w[0], 4, "a member the filter excluded")
	wantCounts(t, g, 14, "after the filtered request")
}

// Loads w0 0, w1 0, w2 6, w3 6: T = 12, n = 4, bound 1.25 * 13 / 4 = 4.0625.
//
//	No filter: w0 holds 0: w0.
//	Admit w2 and w3 only: w2 holds 6 > 4.0625 and so does w3: none is within
//	the bound, and the pick is the first admitted member on the walk, w2.
func TestAFilterWithNoAdmittedMemberWithinTheBoundTakesTheFirstOnTheWalk(t *testing.T) {
	w := forkWalk(t, four, "k")
	loads := map[string]int{w[2]: 6, w[3]: 6}

	f := forkOf(four)
	loadUp(t, forkAcquirer(f, noBound), loads)
	if addr, _, err := f.PickPrefixHash("k", tight); err != nil || addr != w[0] {
		t.Fatalf("the plain group picked %s (err %v), by hand w0 %s", addr, err, w[0])
	}

	g := loadedGroup(t, four, loads, map[string]string{w[0]: "pa", w[1]: "pa", w[2]: "pb", w[3]: "pb"})
	wantCounts(t, g, 12, "before the filtered request")
	r, err := g.TryPick("k", providerIs("pb"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Member != w[2] || r.Spilled {
		t.Fatalf("filtered pick with nothing within the bound: %s (spilled %v), want the first admitted w2 %s", r.Member, r.Spilled, w[2])
	}
	wantInFlight(t, g, w[2], 7, "the chosen member")
	wantCounts(t, g, 13, "after the filtered request")
}

// A request that no member may take, or when no member is eligible, is
// refused at once and reserves nothing.
func TestNoAdmittedMemberReservesNothing(t *testing.T) {
	g := New()
	g.Update(withProviders(stateOf(three, tight, 0), map[string]string{"a": "pa", "b": "pa", "c": "pa"}))
	mustPick(t, g, "held")
	if r, err := g.TryPick("k", func(string, Attrs) bool { return false }); !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("a filter that admits nobody: %v, %v, want ErrNoEligible", r, err)
	}
	if r, err := g.TryPick("k", func(m string, _ Attrs) bool { return m == "not-a-member" }); !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("a filter that admits a name outside the group: %v, %v, want ErrNoEligible", r, err)
	}
	if r, err := g.TryPick("k", providerIs("pb")); !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("a filter on a provider no member has: %v, %v, want ErrNoEligible", r, err)
	}
	wantCounts(t, g, 1, "after the refused requests")

	none := stateOf(three, tight, 0)
	for m := range none.Eligible {
		none.Eligible[m] = false
	}
	g.Update(none)
	if r, err := g.TryPick("k", nil); !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("no member eligible: %v, %v, want ErrNoEligible", r, err)
	}
	wantCounts(t, g, 1, "no member eligible: the held request keeps its count")
}

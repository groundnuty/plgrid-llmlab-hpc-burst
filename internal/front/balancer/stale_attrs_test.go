package balancer

import (
	"fmt"
	"slices"
	"testing"
)

// A release counts down under the attributes the reservation was counted under, not under those a later Update gave the same member name (the in-flight
// series of a replacement Instance, which has another UID, must not be changed by the release of a request of the one it replaced). OnStale is told, once
// per such release, so the old UID's series can be dropped when it is empty. Every expected call is typed by hand.
func TestAReleaseCountsDownUnderTheAttributesItWasCountedUnder(t *testing.T) {
	g := New()
	var counts, stale []string
	g.OnCount = func(a Attrs, ch Charge, delta int) { counts = append(counts, fmt.Sprintf("%s%+d", a.UID, delta)) }
	g.OnStale = func(a Attrs, ch Charge) { stale = append(stale, a.UID) }
	s := stateOf([]string{"m"}, tight, 0)
	s.Attrs = map[string]Attrs{"m": {UID: "u1"}}
	g.Update(s)
	r1, err := g.TryPick("k1", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Attrs = map[string]Attrs{"m": {UID: "u2"}} // an Instance of the same name and another UID replaces it
	g.Update(s)
	r2, err := g.TryPick("k2", nil)
	if err != nil {
		t.Fatal(err)
	}
	r1.Release()
	r2.Release()
	if want := []string{"u1+1", "u2+1", "u1-1", "u2-1"}; !slices.Equal(counts, want) {
		t.Errorf("OnCount calls %v, want %v", counts, want)
	}
	if want := []string{"u1"}; !slices.Equal(stale, want) {
		t.Errorf("OnStale calls %v, want %v: only r1 was released after its member was replaced", stale, want)
	}
}

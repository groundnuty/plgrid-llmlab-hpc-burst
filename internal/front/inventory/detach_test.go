package inventory

// A Provider that a served deployment no longer selects stops taking new
// requests, but its Instances keep their place in the group: a detach reaches
// the balancer as a skip (Eligible false, still a member), because a leave
// would release the open requests at once and the Instance would come back
// with a count of zero (HBEP-5, Request path, Eligible; Load bound and cap,
// Keeps the count through a skip). Each test first builds the view with the
// Provider selected and checks it eligible, then detaches it and rebuilds from
// that view's Joined, so a detachment that goes unnoticed fails.

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
)

const poolLabel = "example.test/pool"

// twoProviders is one served deployment "md" (model "m") over two batch
// Providers, helios-a and helios-b, each with a ready Instance (ia, ib) and the
// pool label "blue". The deployment selects the Providers by name.
func twoProviders() Snapshot {
	le := now.Add(48 * time.Hour)
	s := batchSnap()
	pa, pb := prov("helios-a", "gh200", "helios"), prov("helios-b", "gh200", "helios")
	pa.Labels[poolLabel], pb.Labels[poolLabel] = "blue", "blue"
	s.Providers = []v1alpha1.Provider{pa, pb}
	s.Deployments[0].Spec.Providers = v1alpha1.ProviderSelection{Names: []string{"helios-a", "helios-b"}}
	md := &s.Deployments[0]
	s.Instances = []v1alpha1.Instance{
		instance(md, "ia", "helios-a", v1alpha1.InstanceReady, &le),
		instance(md, "ib", "helios-b", v1alpha1.InstanceReady, &le),
	}
	return s
}

func buildFrom(s Snapshot, prev *View) *View {
	in := Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour}
	if prev != nil {
		in.Joined = prev.Joined
	}
	return Build(s, in)
}

// eligibility is each member's Eligible, by Instance name (no namespace).
func eligibility(v *View) map[string]bool {
	out := map[string]bool{}
	for _, m := range members(v) {
		out[m.Name] = m.Eligible
	}
	return out
}

func wantEligibility(t *testing.T, v *View, want map[string]bool, when string) {
	t.Helper()
	got := eligibility(v)
	if len(got) != len(want) {
		t.Fatalf("%s: members %v, want %v", when, got, want)
	}
	for k, w := range want {
		if g, ok := got[k]; !ok || g != w {
			t.Fatalf("%s: members %v, want %v", when, got, want)
		}
	}
}

func TestDetachedByNameIsNotEligibleButStaysAMember(t *testing.T) {
	s := twoProviders()
	v1 := buildFrom(s, nil)
	wantEligibility(t, v1, map[string]bool{"ia": true, "ib": true}, "both selected")

	s.Deployments[0].Spec.Providers.Names = []string{"helios-b"}
	v2 := buildFrom(s, v1)
	wantEligibility(t, v2, map[string]bool{"ia": false, "ib": true}, "helios-a dropped from providers.names")
	if !v2.Joined[s.Instances[0].UID] {
		t.Error("the detached Instance left Joined: the next build would drop it and release its requests")
	}
}

func TestDetachedByLabelIsNotEligibleButStaysAMember(t *testing.T) {
	s := twoProviders()
	s.Deployments[0].Spec.Providers = v1alpha1.ProviderSelection{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{poolLabel: "blue"}}}
	v1 := buildFrom(s, nil)
	wantEligibility(t, v1, map[string]bool{"ia": true, "ib": true}, "both match the selector")

	s.Providers[0].Labels[poolLabel] = "red"
	v2 := buildFrom(s, v1)
	wantEligibility(t, v2, map[string]bool{"ia": false, "ib": true}, "helios-a's label no longer matches")

	// Back to blue: the same Instance is eligible again, still the same member.
	s.Providers[0].Labels[poolLabel] = "blue"
	v3 := buildFrom(s, v2)
	wantEligibility(t, v3, map[string]bool{"ia": true, "ib": true}, "helios-a matches again")
}

// A name that selects a Provider and a selector that matches another are both
// honoured, and an Instance of a Provider that neither selects is not.
func TestAnAttachedProviderStaysEligible(t *testing.T) {
	s := twoProviders()
	s.Deployments[0].Spec.Providers = v1alpha1.ProviderSelection{Names: []string{"helios-a"},
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{poolLabel: "blue"}}}
	v1 := buildFrom(s, nil)
	wantEligibility(t, v1, map[string]bool{"ia": true, "ib": true}, "a by name, b by selector")

	s.Providers[1].Labels[poolLabel] = "red"
	v2 := buildFrom(s, v1)
	wantEligibility(t, v2, map[string]bool{"ia": true, "ib": false}, "b detached, a still named")
}

// An Instance whose Provider is not selected when it first becomes ready never
// joins: the group's n and its ring hold only Instances that can take requests.
func TestAnUnselectedProviderNeverJoins(t *testing.T) {
	s := twoProviders()
	s.Deployments[0].Spec.Providers.Names = []string{"helios-b"}
	v := buildFrom(s, nil)
	wantEligibility(t, v, map[string]bool{"ib": true}, "helios-a never selected")
	if v.Joined[s.Instances[0].UID] {
		t.Error("an Instance on an unselected Provider joined")
	}
}

// An unreadable selector selects nothing, but it must not make a joined
// Instance leave: that would release its open requests.
func TestAnUnreadableSelectorSkipsAndNeverLeaves(t *testing.T) {
	s := twoProviders()
	v1 := buildFrom(s, nil)
	wantEligibility(t, v1, map[string]bool{"ia": true, "ib": true}, "names")

	s.Deployments[0].Spec.Providers = v1alpha1.ProviderSelection{
		Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: poolLabel, Operator: "Bogus"}}}}
	v2 := buildFrom(s, v1)
	wantEligibility(t, v2, map[string]bool{"ia": false, "ib": false}, "unreadable selector")
	if len(v2.Problems) != 1 {
		t.Errorf("problems %q, want exactly one (not one per Provider or Instance)", v2.Problems)
	}
}

// groupState is the balancer state the router derives from a deployment
// (router.stateLocked, without pauses or caps): its members, which of them are
// eligible, and the tight bound.
func groupState(d *Deployment) balancer.State {
	st := balancer.State{Eligible: map[string]bool{}, Percent: d.TightPercent}
	for _, m := range d.Members {
		st.Members = append(st.Members, m.Key)
		st.Eligible[m.Key] = m.Eligible
	}
	return st
}

// keyHomedOn finds a conversation key whose home on the ring of names is name.
func keyHomedOn(t *testing.T, names []string, name string) string {
	t.Helper()
	r := balancer.NewRing(names)
	for i := 0; i < 100_000; i++ {
		if k := fmt.Sprintf("key-%d", i); r.Home(k) == name {
			return k
		}
	}
	t.Fatalf("no key homed on %s", name)
	return ""
}

// The detached Instance keeps its open requests counted until they end: 3 open
// on helios-a's Instance and 2 on helios-b's. After the detach the group still
// holds 3 and 2 (total 5, three views of the count equal), a request whose key
// is homed on the detached Instance goes to the other one, and the detached
// Instance's 3 requests end one by one: its count falls to 0 as each is
// released, and the total to 2.
func TestDetachKeepsTheOpenRequestsCounted(t *testing.T) {
	s := twoProviders()
	v1 := buildFrom(s, nil)
	d1 := v1.Models["m"].Served[0]
	g := balancer.New()
	g.Update(groupState(d1))
	a, b := "ns/ia", "ns/ib"
	keyA, keyB := keyHomedOn(t, []string{a, b}, a), keyHomedOn(t, []string{a, b}, b)

	// c = 1.25, two members: a member takes a request while its load before it
	// is at most 1.25 x (T + 1) / 2, T the total before. Alternating the keys,
	// each key's home admits it, by hand:
	//   A: T = 0, any load passes: A 1.    B: T = 1, bound 1.25, B holds 0: B 1.
	//   A: T = 2, bound 1.875, 1 <= it: A 2.   B: T = 3, bound 2.5: B 2.
	//   A: T = 4, bound 3.125, 2 <= it: A 3.
	var onA []*balancer.Reservation
	for _, k := range []string{keyA, keyB, keyA, keyB, keyA} {
		r, err := g.TryPick(k, nil)
		if err != nil {
			t.Fatal(err)
		}
		if k == keyA {
			if r.Member != a {
				t.Fatalf("a key homed on %s landed on %s", a, r.Member)
			}
			onA = append(onA, r)
		}
	}
	total, members, open := g.Counts()
	if total != 5 || members != 5 || open != 5 {
		t.Fatalf("before the detach: total %d, members' sum %d, open %d; want 5 each", total, members, open)
	}

	s.Deployments[0].Spec.Providers.Names = []string{"helios-b"}
	v2 := buildFrom(s, v1)
	if released := g.Update(groupState(v2.Models["m"].Served[0])); released != 0 {
		t.Fatalf("the detach released %d requests: it was treated as a leave", released)
	}
	if n, _ := g.InFlight(a); n != 3 || g.Open(a) != 3 {
		t.Fatalf("after the detach %s holds %d (open %d), want 3", a, n, g.Open(a))
	}
	total, members, open = g.Counts()
	if total != 5 || members != 5 || open != 5 {
		t.Fatalf("after the detach: total %d, members' sum %d, open %d; want 5 each", total, members, open)
	}

	r, err := g.TryPick(keyA, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Member != b {
		t.Fatalf("a key homed on the detached %s landed on %s, want %s", a, r.Member, b)
	}
	r.Release()

	for i, r := range onA {
		r.Release()
		if n, _ := g.InFlight(a); n != int64(3-i-1) {
			t.Fatalf("after releasing %d of its requests %s holds %d, want %d", i+1, a, n, 3-i-1)
		}
	}
	if total, members, open = g.Counts(); total != 2 || members != 2 || open != 2 {
		t.Fatalf("after the detached Instance's requests ended: total %d, members' sum %d, open %d; want 2 each", total, members, open)
	}
}

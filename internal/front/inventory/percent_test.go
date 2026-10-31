package inventory

// The load bound c is a decimal string, and KubeAI's setting, and the forked
// group's pick, take a whole percent. The front uses the configured value
// exactly and never rounds it (@hpc-burst-science-agent, 31 Oct 2026): a
// factor with up to two decimals is its percent, and one it cannot use
// exactly is refused, so nothing changes silently.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
)

// The expected percents are the configured digits, worked by hand. 1.15,
// 1.13 and 2.3 are the cases a float64 conversion gets wrong: 1.15 x 100 is
// 114.99999999999999 there.
func TestPercentUsesTheConfiguredValueExactly(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"1", 100}, {"1.0", 100}, {"1.00", 100}, {"01.5", 150},
		{"1.25", 125}, {"2.0", 200}, {"3", 300}, {"1.1", 110},
		{"1.15", 115}, {"1.13", 113}, {"2.3", 230}, {"1.01", 101},
		{"1.2500", 125}, {"1.010", 101}, // trailing zeros are the same value
		{"9999999999999999", 999999999999999900}, // the API's 16-character maximum
	} {
		got, err := Percent(v1alpha1.LoadFactor(c.in))
		if err != nil || got != c.want {
			t.Errorf("Percent(%q) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
}

// A factor the front cannot use exactly is refused, not rounded.
func TestPercentRefusesWhatItCannotUseExactly(t *testing.T) {
	for _, in := range []string{"1.006", "1.005", "1.019", "1.999", "1.2599", "2.675"} {
		if got, err := Percent(v1alpha1.LoadFactor(in)); err == nil {
			t.Errorf("Percent(%q) = %d, nil; want an error, not a rounded percent", in, got)
		}
	}
}

func TestPercentRefusesWhatTheAPIDoes(t *testing.T) {
	for _, in := range []string{"", "0", "0.99", "0.999", "abc", "1e2", "-1", "+1.5", " 1.5", "1.5 ", "1.", ".5", "1,5", "Inf", "NaN",
		"99999999999999999999"} {
		if got, err := Percent(v1alpha1.LoadFactor(in)); err == nil {
			t.Errorf("Percent(%q) = %d, nil; want an error", in, got)
		}
	}
}

// c = 1.01 over two members (T = 199, n = 2): the bound is 1.01 x (199 + 1)
// / 2 = 101, the configured value unchanged. With loads 101 and 98 the
// member holding 101 is admitted; with loads 102 and 97 the one holding 102
// is refused and the request goes to the one holding 97. A bound rounded to
// 100 or 102 would get one of the two wrong.
func TestATwoDecimalFactorIsTheBoundExactly(t *testing.T) {
	for _, c := range []struct {
		loadA, loadB int
		want         string
	}{
		{101, 98, "ns/ia"},
		{102, 97, "ns/ib"},
	} {
		s := twoProviders()
		tight, loose := v1alpha1.LoadFactor("1.01"), v1alpha1.LoadFactor("2.0")
		s.Deployments[0].Spec.Routing = &v1alpha1.Routing{LoadBound: &v1alpha1.LoadBound{Tight: &tight, Loose: &loose}}
		v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
		d := v.Models["m"].Served[0]

		a, b := "ns/ia", "ns/ib"
		keyA, keyB := keyHomedOn(t, []string{a, b}, a), keyHomedOn(t, []string{a, b}, b)
		g := balancer.New()
		open := groupState(d)
		open.Percent = 1 << 20 // no bound while the loads are set up: every key takes its home
		g.Update(open)
		for i := 0; i < c.loadA; i++ {
			mustPickOn(t, g, keyA, a)
		}
		for i := 0; i < c.loadB; i++ {
			mustPickOn(t, g, keyB, b)
		}
		g.Update(groupState(d)) // the configured bound

		r, err := g.TryPick(keyA, nil)
		if err != nil {
			t.Fatal(err)
		}
		if r.Member != c.want {
			t.Errorf("loads %d and %d at c = 1.01 (bound 101, tight percent %d): a key homed on %s landed on %s, want %s",
				c.loadA, c.loadB, d.TightPercent, a, r.Member, c.want)
		}
	}
}

// A deployment whose factor cannot be used exactly is not routed, and the
// problem is logged: the bound never changes silently.
func TestAnInexactFactorIsNotRouted(t *testing.T) {
	s := twoProviders()
	tight := v1alpha1.LoadFactor("1.006")
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{LoadBound: &v1alpha1.LoadBound{Tight: &tight}}
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	if m := v.Models["m"]; m != nil && len(m.Served) > 0 {
		t.Errorf("a deployment at c = 1.006 is routed with tight percent %d; want it refused", m.Served[0].TightPercent)
	}
	found := false
	for _, p := range v.Problems {
		found = found || strings.Contains(p, "1.006")
	}
	if !found {
		t.Errorf("no problem names the factor 1.006: %v", v.Problems)
	}
}

func mustPickOn(t *testing.T, g *balancer.Group, key, want string) {
	t.Helper()
	r, err := g.TryPick(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Member != want {
		t.Fatalf("while loading: key homed on %s landed on %s", want, r.Member)
	}
}

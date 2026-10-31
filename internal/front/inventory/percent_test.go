package inventory

// The load bound c is a decimal string ("1.006"), but KubeAI's setting, and
// the forked group's pick, take a whole percent. Percent therefore cuts the
// bound down to two decimals and never rounds it up: a member is admitted
// while its load before the request is at most c x (T + 1) / n (HBEP-5, Load
// bound and cap), and a rounded-up percent would admit one above that.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
)

// The expected percents are the first two decimals of the string, worked by
// hand. 1.15, 1.13 and 2.3 are the cases a float64 floor gets wrong: 1.15 x
// 100 is 114.99999999999999 there.
func TestPercentCutsTheDecimalsAndNeverRoundsUp(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"1", 100}, {"1.0", 100}, {"1.00", 100}, {"01.5", 150},
		{"1.25", 125}, {"2.0", 200}, {"3", 300}, {"1.1", 110},
		{"1.15", 115}, {"1.13", 113}, {"2.3", 230},
		{"1.01", 101}, {"1.006", 100}, {"1.005", 100}, {"1.019", 101},
		{"1.999", 199}, {"1.2599", 125}, {"2.675", 267},
		{"9999999999999999", 999999999999999900}, // the API's 16-character maximum
	} {
		got, err := Percent(v1alpha1.LoadFactor(c.in))
		if err != nil || got != c.want {
			t.Errorf("Percent(%q) = %d, %v; want %d", c.in, got, err, c.want)
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

// c = 1.006 over two members with loads 101 and 98 (T = 199, n = 2): the bound
// is 1.006 x (199 + 1) / 2 = 100.6, so the member holding 101 is refused and
// the request goes to the one holding 98. Rounded to the nearest percent the
// bound would be 101, which admits the 101. With loads 100 and 99 the bound is
// the same 100.6 and the member holding 100 is admitted: the floor to 100
// percent does not refuse it either (100 <= 1.00 x 200 / 2).
func TestAFractionalFactorNeverAdmitsAboveTheBound(t *testing.T) {
	for _, c := range []struct {
		loadA, loadB int
		want         string
	}{
		{101, 98, "ns/ib"},
		{100, 99, "ns/ia"},
	} {
		s := twoProviders()
		tight, loose := v1alpha1.LoadFactor("1.006"), v1alpha1.LoadFactor("2.0")
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
			t.Errorf("loads %d and %d at c = 1.006 (bound 100.6, tight percent %d): a key homed on %s landed on %s, want %s",
				c.loadA, c.loadB, d.TightPercent, a, r.Member, c.want)
		}
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

package main

// The lifecycle manager runs in host-dialed mode exactly when --front-mode is
// host-dialed, which caps each batch Provider's submit limit at 2 (HBEP-6,
// Host-dialed mode, Capacity). This checks only the wiring from the flag; the
// cap's effect is tested in internal/lifecycle (TestHostDialedSubmitLimit).

import "testing"

func TestManagerOptionsHostDialedFollowsTheFrontMode(t *testing.T) {
	for _, c := range []struct {
		mode           string
		wantHostDialed bool
		wantFront      bool
	}{
		{"host-dialed", true, true},
		{"direct", false, true},
		{"ssh", false, true},
		{"", false, false}, // no front configured
	} {
		o := managerOptions(flags{frontMode: c.mode}, nil, func(string) string { return "" })
		if o.HostDialed != c.wantHostDialed || o.FrontConfigured != c.wantFront {
			t.Errorf("--front-mode %q: HostDialed %v, FrontConfigured %v; want %v, %v", c.mode, o.HostDialed, o.FrontConfigured, c.wantHostDialed, c.wantFront)
		}
	}
}

// The host-dialed cap follows a profile's transport.loginHostBudget (HBEP-4, open question 17): the lifecycle manager's
// options carry M's source, the login-host register of the Providers' own controllers, and relay-keeper mode, in which
// the cap stays 2 whatever the budget (K2), exactly when --relay-keeper-socket is given.
func TestManagerOptionsCarryTheCapsSources(t *testing.T) {
	for _, c := range []struct {
		name       string
		f          flags
		wantKeeper bool
	}{
		{"host-dialed, no keeper", flags{frontMode: "host-dialed"}, false},
		{"host-dialed with the keeper's socket", flags{frontMode: "host-dialed", relayKeeperSocket: "/run/hb/keeper.sock"}, true},
	} {
		o := managerOptions(c.f, nil, func(string) string { return "" })
		if o.RelayKeeper != c.wantKeeper {
			t.Errorf("%s: RelayKeeper %v, want %v", c.name, o.RelayKeeper, c.wantKeeper)
		}
		if o.LoginHostProviders == nil {
			t.Fatalf("%s: no LoginHostProviders: a budget would make every Provider unavailable", c.name)
		}
		if _, known := o.LoginHostProviders("no-such-provider"); known {
			t.Errorf("%s: a Provider that was never enrolled has a login host", c.name)
		}
	}
}

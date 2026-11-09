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

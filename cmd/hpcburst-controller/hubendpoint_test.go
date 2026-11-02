package main

// hpcburst-controller lists checkpoints from HF_ENDPOINT's Hub when it is set,
// as hb-agent downloads from it, and from the default Hub when it is not
// (issue #146, ruling of 1 Nov 2026).

import "testing"

func TestManagerOptionsTakeTheHubFromHFEndpoint(t *testing.T) {
	for _, c := range []struct{ env, want string }{
		{"http://127.0.0.1:29538", "http://127.0.0.1:29538"},
		{"", ""}, // lifecycle then lists from https://huggingface.co
	} {
		getenv := func(k string) string {
			if k == "HF_ENDPOINT" {
				return c.env
			}
			return ""
		}
		if got := managerOptions(flags{}, nil, getenv).HubEndpoint; got != c.want {
			t.Errorf("HF_ENDPOINT=%q: HubEndpoint %q, want %q", c.env, got, c.want)
		}
	}
}

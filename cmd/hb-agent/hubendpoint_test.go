package main

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The agent stages from agent.json's hubEndpoint, which the controller writes
// from its own HF_ENDPOINT, even when the job's environment lacks HF_ENDPOINT
// or names another; only with none in agent.json does the job's HF_ENDPOINT
// count (ruling of 1 Nov 2026 on #156).
func TestHubEndpointPrefersAgentJSON(t *testing.T) {
	for _, c := range []struct{ json, env, want string }{
		{"http://127.0.0.1:29537", "", "http://127.0.0.1:29537"},
		{"http://127.0.0.1:29537", "http://127.0.0.1:29536", "http://127.0.0.1:29537"},
		{"", "http://127.0.0.1:29536", "http://127.0.0.1:29536"},
		{"", "", ""},
	} {
		getenv := func(k string) string {
			if k == "HF_ENDPOINT" {
				return c.env
			}
			return ""
		}
		if got := hubEndpoint(agentapi.AgentConfig{HubEndpoint: c.json}, getenv); got != c.want {
			t.Errorf("agent.json %q, HF_ENDPOINT %q: %q, want %q", c.json, c.env, got, c.want)
		}
	}
}

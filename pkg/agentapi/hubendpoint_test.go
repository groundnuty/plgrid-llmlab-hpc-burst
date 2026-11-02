package agentapi

import (
	"strings"
	"testing"
)

// The Hub origin in agent.json: https, or http only for a loopback host (a
// local Hub double), a scheme and a host only, as pkg/hubfiles requires.
func TestCheckHubEndpoint(t *testing.T) {
	for _, ok := range []string{"https://huggingface.co", "https://hub.example.org:8443", "https://hub.example.org/",
		"http://127.0.0.1:29537", "http://localhost:8080", "http://[::1]:9000"} {
		if err := CheckHubEndpoint(ok); err != nil {
			t.Errorf("CheckHubEndpoint(%q): %v, want accepted", ok, err)
		}
	}
	for _, bad := range []string{"", "huggingface.co", "http://hub.example.org", "ftp://hub.example.org",
		"https://hub.example.org/api", "https://user@hub.example.org", "https://hub.example.org?x=1", "https://hub.example.org#f",
		"http://10.0.0.1:8080"} {
		if err := CheckHubEndpoint(bad); err == nil {
			t.Errorf("CheckHubEndpoint(%q) accepted it", bad)
		}
	}
}

func TestAgentConfigHubEndpoint(t *testing.T) {
	base := AgentConfig{
		ProtocolVersion: ProtocolVersion, Tag: "hb-example-0123456789ab", Mode: DialHomeDirect, Endpoint: "front.example.org:29443",
		CertSPKISHA256: strings.Repeat("c", 64), FrpcDigest: strings.Repeat("d", 64),
		StagingDir: "/scratch/example/stage", DialHomeDir: "/home/example/.hpcburst/dial-home",
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("the base config without a hubEndpoint: %v", err)
	}
	set := base
	set.HubEndpoint = "http://127.0.0.1:29537"
	b, err := set.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeAgentConfig(b)
	if err != nil || got.HubEndpoint != "http://127.0.0.1:29537" {
		t.Errorf("round trip: hubEndpoint %q, %v", got.HubEndpoint, err)
	}
	bad := base
	bad.HubEndpoint = "http://hub.example.org"
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "hubEndpoint") {
		t.Errorf("Validate with a plain-http remote hubEndpoint: %v, want a hubEndpoint error", err)
	}
}

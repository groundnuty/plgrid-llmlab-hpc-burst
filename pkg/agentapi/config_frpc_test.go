package agentapi

import (
	"strings"
	"testing"
)

// The ssh dial-home mode accepts any valid port on the control-plane host's ssh
// server: the owner routes the clusters through a port forward on the gateway
// at its own port (`@hpc-burst-science-agent`, 31 Oct 2026). Port 22 stays the
// default, but Validate does not insist on it.
func TestSSHModeAcceptsAnyValidEndpointPort(t *testing.T) {
	for _, port := range []string{"1", "22", "2222", "29250", "65535"} {
		c := validConfig()
		c.Endpoint = "control.example:" + port
		if err := c.Validate(); err != nil {
			t.Errorf("ssh mode on endpoint port %s was refused: %v", port, err)
		}
	}
	for _, port := range []string{"0", "65536", "http", ""} {
		c := validConfig()
		c.Endpoint = "control.example:" + port
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "endpoint") {
			t.Errorf("ssh mode on endpoint port %q: error %v, want one naming the endpoint", port, err)
		}
	}
}

// A fully valid ssh-mode config runs frpc: both forwards' ports and frpc's
// digest are set. It encodes, decodes to the same values, and names frpc's
// staged path under the staging directory.
func TestValidSSHConfigThatRunsFrpc(t *testing.T) {
	c := validConfig()
	c.StagingDir = "/s"
	if err := c.Validate(); err != nil {
		t.Fatalf("a complete ssh-mode config was refused: %v", err)
	}
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeAgentConfig(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.FrpcDigest != "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" ||
		got.SSH.DialHomePort != 29229 || got.SSH.AgentPort != 29225 {
		t.Errorf("round trip: frpcDigest %q, ports %d and %d", got.FrpcDigest, got.SSH.AgentPort, got.SSH.DialHomePort)
	}
	// Written out by hand from HBEP-4, Submission: <stagingDir>/frpc/<digest>/frpc.
	const want = "/s/frpc/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/frpc"
	if p := got.FrpcPath(); p != want {
		t.Errorf("FrpcPath = %q, want %q", p, want)
	}
}

// Each missing or wrong value is refused with a reason that names it.
func TestFrpcSettingsAreRequired(t *testing.T) {
	direct := func(c *AgentConfig) { c.Mode, c.Endpoint, c.SSH = DialHomeDirect, "front.example:8443", nil }
	hex63 := strings.Repeat("a", 63)
	for _, tc := range []struct {
		name   string
		edit   func(*AgentConfig)
		reason string // a substring of the error
	}{
		{"ssh without a dial-home port", func(c *AgentConfig) { c.SSH.DialHomePort = 0 }, "ssh.dialHomePort 0"},
		{"ssh with a negative dial-home port", func(c *AgentConfig) { c.SSH.DialHomePort = -1 }, "ssh.dialHomePort -1"},
		{"ssh with dial-home port 65536", func(c *AgentConfig) { c.SSH.DialHomePort = 65536 }, "ssh.dialHomePort 65536"},
		{"ssh with the dial-home port of the agent endpoint", func(c *AgentConfig) { c.SSH.DialHomePort = c.SSH.AgentPort }, "ssh.dialHomePort 29225 is also ssh.agentPort"},
		{"ssh with no ssh settings", func(c *AgentConfig) { c.SSH = nil }, "ssh.dialHomePort"},
		{"ssh without a frpc digest", func(c *AgentConfig) { c.FrpcDigest = "" }, "frpcDigest"},
		{"ssh with a 63-character frpc digest", func(c *AgentConfig) { c.FrpcDigest = hex63 }, "frpcDigest"},
		{"ssh with a 65-character frpc digest", func(c *AgentConfig) { c.FrpcDigest = hex63 + "bb" }, "frpcDigest"},
		{"ssh with an uppercase frpc digest", func(c *AgentConfig) { c.FrpcDigest = strings.Repeat("A", 64) }, "frpcDigest"},
		{"ssh with a non-hex frpc digest", func(c *AgentConfig) { c.FrpcDigest = strings.Repeat("g", 64) }, "frpcDigest"},
		{"ssh with a prefixed frpc digest", func(c *AgentConfig) { c.FrpcDigest = "sha256:" + strings.Repeat("a", 57) }, "frpcDigest"},
		{"ssh with a trailing newline in the frpc digest", func(c *AgentConfig) { c.FrpcDigest = strings.Repeat("a", 64) + "\n" }, "frpcDigest"},
		{"direct without a frpc digest", func(c *AgentConfig) { direct(c); c.FrpcDigest = "" }, "frpcDigest"},
		{"direct with a short frpc digest", func(c *AgentConfig) { direct(c); c.FrpcDigest = hex63 }, "frpcDigest"},
	} {
		c := validConfig()
		ssh := *c.SSH
		c.SSH = &ssh
		tc.edit(&c)
		err := c.Validate()
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.reason) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.reason)
		}
		if _, err := c.Encode(); err == nil {
			t.Errorf("%s: encoded", tc.name)
		}
	}
}

// frpc runs in direct mode too (HBEP-6, Dial-home modes): a direct-mode config
// with a frpc digest is complete, and needs no dial-home port, because frpc
// dials the endpoint itself.
func TestDirectConfigThatRunsFrpc(t *testing.T) {
	c := validConfig()
	c.Mode, c.Endpoint, c.SSH = DialHomeDirect, "front.example:8443", nil
	if err := c.Validate(); err != nil {
		t.Fatalf("a complete direct-mode config was refused: %v", err)
	}
}

// An agent.json from a provider that does not know frpc has no frpcDigest, and
// the agent refuses it rather than start without the check.
func TestDecodeRefusesAnAgentJSONWithoutFrpcDigest(t *testing.T) {
	c := validConfig()
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	old := strings.Replace(string(b), `"frpcDigest": "`+c.FrpcDigest+`",`, "", 1)
	if old == string(b) {
		t.Fatalf("the test did not remove frpcDigest from:\n%s", b)
	}
	if _, err := DecodeAgentConfig([]byte(old)); err == nil || !strings.Contains(err.Error(), "frpcDigest") {
		t.Errorf("an agent.json without frpcDigest: error %v, want one naming frpcDigest", err)
	}
}

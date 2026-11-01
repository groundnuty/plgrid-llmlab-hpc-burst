package front

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestTunnelTakesExactlyTheUIDsTheAgentRegisters (PR #129 review, N8): the
// Tunnel dials only a UID that the agent would give frpc as its proxy's name
// (internal/agent/frpcconf.go, uidForm: ^[a-z0-9][a-z0-9-]{0,62}$). Each
// edge has its expected verdict from that rule, and the agent's own
// validation of frpc's configuration agrees with each one.
func TestTunnelTakesExactlyTheUIDsTheAgentRegisters(t *testing.T) {
	cases := []struct {
		uid  string
		want bool
	}{
		{"00000000-0000-4000-8000-000000000001", true}, // a Kubernetes UID
		{"a", true},
		{"0", true},
		{"a" + strings.Repeat("-", 62), true}, // 63 characters
		{strings.Repeat("a", 63), true},       // 63 characters
		{strings.Repeat("a", 64), false},      // 64 characters
		{"", false},                           // empty
		{"-a", false},                         // a leading hyphen
		{"A0000000-0000-4000-8000-000000000001", false}, // uppercase
		{"abc_def", false},
		{"abc:80", false},
		{"abc def", false},
		{"abc\r\nHost: x", false},
		{"abc.def", false},
		{"é", false},
	}
	dir := t.TempDir()
	base := agent.FrpcConfig{ServerAddr: "127.0.0.1", ServerPort: 1, LogFile: filepath.Join(dir, "frpc.log"), Token: newKey(t),
		CAFile: filepath.Join(dir, "cert.pem"), ServerName: agentapi.DialHomeServerName, Socket: filepath.Join(dir, "e.sock")}
	if err := base.Validate(); err == nil || !strings.Contains(err.Error(), "Instance UID") || strings.Contains(err.Error(), "\n") {
		t.Fatalf("the agent's validation with no UID: %v; the base must fail on the UID alone", err)
	}
	tun := NewTunnel("127.0.0.1:1", DefaultTunnelSetupTimeout) // only refused UIDs are passed to Dial, which never dials
	for _, c := range cases {
		err := checkUID(types.UID(c.uid))
		if (err == nil) != c.want {
			t.Errorf("checkUID(%q) = %v, want accepted %v", c.uid, err, c.want)
		}
		cfg := base
		cfg.UID = c.uid
		if agentOK := cfg.Validate() == nil; agentOK != c.want {
			t.Errorf("the agent's validation of UID %q: accepted %v, want %v", c.uid, agentOK, c.want)
		}
		if !c.want {
			if _, err := tun.Dial(context.Background(), types.UID(c.uid)); err == nil || !strings.Contains(err.Error(), "not a Kubernetes UID") {
				t.Errorf("Dial(%q) = %v, want the UID refused before any dial", c.uid, err)
			}
		}
	}
}

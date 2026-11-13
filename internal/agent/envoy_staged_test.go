package agent_test

import (
	"runtime"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// The agent looks for Envoy where the provider stages it: <stagingDir>/envoy/<sha256>/envoy, by the
// digest of the Envoy pinned in its own build for its architecture (#454). The sums are Envoy 1.40.2's
// published release sums, written out as envoy-pins.txt carries them.
func TestStagedEnvoyPathIsThePinsDigestUnderTheStaging(t *testing.T) {
	sum := map[string]string{
		"amd64": "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc",
		"arm64": "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf",
	}[runtime.GOARCH]
	if sum == "" {
		t.Skip("Envoy is pinned for amd64 and arm64")
	}
	if got, want := agent.StagedEnvoyPath("/staging"), "/staging/envoy/"+sum+"/envoy"; got != want {
		t.Errorf("StagedEnvoyPath = %q, want %q", got, want)
	}
	if got := agent.StagedEnvoyPath(""); got != "" {
		t.Errorf("with no staging directory: %q, want none", got)
	}
}

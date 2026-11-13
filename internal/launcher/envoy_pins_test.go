package launcher

import (
	"maps"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
)

// The Envoy digests by cpuArch are Envoy 1.40.2's published release sums, written out here as
// envoy-pins.txt carries them, keyed by the kernel's machine names the platform profiles use.
func TestEnvoyDigestsByCPUArch(t *testing.T) {
	got, err := EnvoyDigests()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"x86_64":  "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc",
		"aarch64": "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf",
	}
	if !maps.Equal(got, want) {
		t.Errorf("EnvoyDigests = %v, want %v", got, want)
	}
}

func TestEnvoyDigestsRefuseAnArchitectureWithNoCPUArchAndAnUnpinnedVersion(t *testing.T) {
	pins := []envoypin.Pin{{Version: "1.40.2", Arch: "riscv64", SHA256: strings.Repeat("a", 64)}}
	if _, err := envoyDigests(pins, "1.40.2"); err == nil || !strings.Contains(err.Error(), "riscv64") {
		t.Errorf("an architecture with no cpuArch: %v, want an error naming it", err)
	}
	pins = []envoypin.Pin{{Version: "1.40.2", Arch: "amd64", SHA256: strings.Repeat("a", 64)}}
	if _, err := envoyDigests(pins, "1.41.0"); err == nil {
		t.Error("a version with no pin gave digests")
	}
}

package launcher

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// hack/fetch-frp.sh --agent-builds names each frpc by cpuArch, and its cpu_arch function maps frp's
// architecture names to them, a second copy of cpuArchOf. The script is read as text, and neither
// side is computed from the other, so a mapping that drifts (an architecture added to one, or two
// names swapped) fails here. The pins themselves are not copied: the mode reads frp-pins.txt.
func TestFetchScriptMapsArchitecturesAsCPUArchOfDoes(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "hack", "fetch-frp.sh"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, m := range regexp.MustCompile(`(?m)^\s*(\w+)\) echo (\w+) ;;\s*$`).FindAllStringSubmatch(string(b), -1) {
		got[m[1]] = m[2]
	}
	if len(got) == 0 {
		t.Fatal("no architecture mapping found in hack/fetch-frp.sh's cpu_arch")
	}
	if !maps.Equal(got, cpuArchOf) {
		t.Errorf("hack/fetch-frp.sh maps %v, cpuArchOf maps %v", got, cpuArchOf)
	}
	// Written out: frp's amd64 is the kernel's x86_64, arm64 is aarch64.
	if got["amd64"] != "x86_64" || got["arm64"] != "aarch64" {
		t.Errorf("the script maps amd64 to %q and arm64 to %q, want x86_64 and aarch64", got["amd64"], got["arm64"])
	}
}

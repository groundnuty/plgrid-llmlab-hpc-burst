package launcher

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The scripts name each build by cpuArch: hack/fetch-frp.sh --agent-builds places frpc-<cpuArch>,
// hack/build-agents.sh places hb-agent-<cpuArch> and needs GOARCH for it, and hack/fetch-k3s.sh turns
// this machine's name into the architecture of k3s-pins.txt. All three read one table,
// hack/cpu-arch.sh's CPU_ARCH_PAIRS, the bash copy of cpuArchOf. The scripts are read as text, and
// neither side is computed from the other, so a mapping that drifts (an architecture added to one,
// or two names swapped) fails here. A script that spells its own map instead of sourcing the table
// is a third copy, and fails here too. The pins themselves are not copied: the mode reads
// frp-pins.txt.
func TestScriptsMapArchitecturesAsCPUArchOfDoes(t *testing.T) {
	read := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", "hack", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	table := read("cpu-arch.sh")
	got := map[string]string{}
	pairs := regexp.MustCompile(`(?m)^CPU_ARCH_PAIRS=\(([^)]*)\)\s*$`).FindStringSubmatch(table)
	if pairs == nil {
		t.Fatal("no CPU_ARCH_PAIRS found in hack/cpu-arch.sh")
	}
	for _, m := range regexp.MustCompile(`"(\w+) (\w+)"`).FindAllStringSubmatch(pairs[1], -1) {
		if _, dup := got[m[1]]; dup {
			t.Errorf("hack/cpu-arch.sh lists %s twice", m[1])
		}
		got[m[1]] = m[2]
	}
	if len(got) == 0 {
		t.Fatal("no architecture pair found in hack/cpu-arch.sh's CPU_ARCH_PAIRS")
	}
	if !maps.Equal(got, cpuArchOf) {
		t.Errorf("hack/cpu-arch.sh maps %v, cpuArchOf maps %v", got, cpuArchOf)
	}
	// Written out: frp's amd64 is the kernel's x86_64, arm64 is aarch64.
	if got["amd64"] != "x86_64" || got["arm64"] != "aarch64" {
		t.Errorf("the table maps amd64 to %q and arm64 to %q, want x86_64 and aarch64", got["amd64"], got["arm64"])
	}

	// The table is the only place. Each script sources it, and outside comments none has a case arm
	// that echoes a name or a pair of names of its own. build-agents.sh and fetch-k3s.sh go further:
	// they name no architecture at all, since they read every one from the table (fetch-frp.sh names
	// linux_amd64 as the one release its tests download).
	arm := regexp.MustCompile(`^\s*[^\s#][^)]*\)\s*echo\b|"(amd64|arm64)[ :=,]+(x86_64|aarch64)"|"(x86_64|aarch64)[ :=,]+(amd64|arm64)"`)
	archName := regexp.MustCompile(`\b(amd64|arm64|x86_64|aarch64)\b`)
	for _, name := range []string{"fetch-frp.sh", "fetch-k3s.sh", "build-agents.sh"} {
		text := read(name)
		if !regexp.MustCompile(`(?m)^\. .*cpu-arch\.sh"\s*$`).MatchString(text) {
			t.Errorf("hack/%s does not source hack/cpu-arch.sh", name)
		}
		for n, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if arm.MatchString(line) || (name != "fetch-frp.sh" && archName.MatchString(line)) {
				t.Errorf("hack/%s:%d has a map of its own outside hack/cpu-arch.sh's table: %s", name, n+1, strings.TrimSpace(line))
			}
		}
	}
}

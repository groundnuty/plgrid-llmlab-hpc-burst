package envoypin

// hack/fetch-envoy.sh --agent-builds DIR places the pinned Envoy of every architecture as
// DIR/envoy-<cpuArch>, the build the Provider's controller stages to a job (#454). Offline, as the
// tests beside it: a file:// release site and a pins file of the test's own. The two "binaries" are
// the published SHA-256 test vectors (FIPS 180-2): "abc" for amd64 and the empty string for arm64.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const emptySum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// buildsSite is a release site with both architectures' assets and a pins file naming both.
type buildsSite struct {
	site, pins, dest string
}

func newBuildsSite(t *testing.T, amd64, arm64 string) buildsSite {
	t.Helper()
	newEnvoySite(t) // the same preconditions: linux, curl
	s := buildsSite{site: t.TempDir(), dest: filepath.Join(t.TempDir(), "agent-builds")}
	rel := filepath.Join(s.site, "v1.40.2")
	if err := os.MkdirAll(rel, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"envoy-1.40.2-linux-x86_64": amd64, "envoy-1.40.2-linux-aarch_64": arm64} {
		if err := os.WriteFile(filepath.Join(rel, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s.pins = writePins(t, "1.40.2 amd64 envoy-1.40.2-linux-x86_64 "+abcSum+"\n1.40.2 arm64 envoy-1.40.2-linux-aarch_64 "+emptySum+"\n")
	return s
}

func (s buildsSite) env() []string {
	return []string{"ENVOY_RELEASE_BASE=file://" + s.site, "ENVOY_PINS_FILE=" + s.pins}
}

// entries is the names in DIR, sorted.
func entries(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

func TestFetchEnvoyPlacesEveryArchitecturesAgentBuild(t *testing.T) {
	s := newBuildsSite(t, "abc", "")
	out, code := runFetch(t, s.env(), "--agent-builds", s.dest)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	for name, want := range map[string]string{"envoy-x86_64": "abc", "envoy-aarch64": ""} {
		p := filepath.Join(s.dest, name)
		b, err := os.ReadFile(p)
		fi, serr := os.Stat(p)
		if err != nil || string(b) != want || serr != nil || fi.Mode().Perm() != 0o755 {
			t.Errorf("%s: %q, %v, %v; want %q, mode 755", name, b, err, fi, want)
		}
		if !strings.Contains(out, "verified "+name+" ("+p+") sha256 ") {
			t.Errorf("no verified line for %s:\n%s", name, out)
		}
	}
	if got := entries(t, s.dest); strings.Join(got, " ") != "envoy-aarch64 envoy-x86_64" {
		t.Errorf("the agent-builds directory holds %q, want exactly the two builds", got)
	}
	// A second run finds both in place and fetches nothing.
	out, code = runFetch(t, s.env(), "--agent-builds", s.dest)
	if code != 0 || strings.Contains(out, "fetching") || !strings.Contains(out, "ok       envoy-x86_64") || !strings.Contains(out, "ok       envoy-aarch64") {
		t.Errorf("the second run: exit %d, want both ok and nothing fetched:\n%s", code, out)
	}
}

// A build whose download does not match its pin is deleted and the run fails; the one fetched before it
// stays. The control is the run above, where both match.
func TestFetchEnvoyRefusesAnAgentBuildThatIsNotItsPin(t *testing.T) {
	s := newBuildsSite(t, "abc", "not the empty string")
	out, code := runFetch(t, s.env(), "--agent-builds", s.dest)
	if code != 1 || !strings.Contains(out, "checksum mismatch for envoy-1.40.2-linux-aarch_64") {
		t.Fatalf("exit %d, want 1 naming the aarch64 asset's checksum mismatch:\n%s", code, out)
	}
	if got := entries(t, s.dest); strings.Join(got, " ") != "envoy-x86_64" {
		t.Errorf("after the mismatch the directory holds %q, want only the matching envoy-x86_64, and no part file", got)
	}
}

// A pins file with two versions for one architecture is refused before anything is fetched: a build
// has one name per cpuArch.
func TestFetchEnvoyRefusesTwoVersionsForOneArchitectureInAgentBuilds(t *testing.T) {
	s := newBuildsSite(t, "abc", "")
	s.pins = writePins(t, "1.40.2 amd64 envoy-1.40.2-linux-x86_64 "+abcSum+"\n1.40.3 amd64 envoy-1.40.3-linux-x86_64 "+abcSum+"\n")
	out, code := runFetch(t, s.env(), "--agent-builds", s.dest)
	if code != 1 || !strings.Contains(out, "more than one Envoy version for amd64") || strings.Contains(out, "fetching") {
		t.Fatalf("exit %d, want 1 refusing the second version before any fetch:\n%s", code, out)
	}
	if got := entries(t, s.dest); len(got) != 0 {
		t.Errorf("the directory holds %q, want nothing", got)
	}
}

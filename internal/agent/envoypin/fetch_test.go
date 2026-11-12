package envoypin

// hack/fetch-envoy.sh reads envoy-pins.txt and installs the binary pinned for this machine's
// architecture. The download itself is not tested here (it needs the network and 107 MB); the
// checksum logic is, offline, against a file:// release site and a pins file of the test's own
// (ENVOY_RELEASE_BASE, ENVOY_PINS_FILE). The "binary" is the three bytes "abc", whose sum is the
// published SHA-256 test vector (FIPS 180-2), so no sum here is computed by the code under test or
// by the script.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const abcSum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

// otherArch is the pinned architecture that is not this machine's.
func otherArch(arch string) string {
	if arch == "amd64" {
		return "arm64"
	}
	return "amd64"
}

// envoySite is a file:// release site for one version, a pins file that names this machine's
// architecture with the sum of "abc", and a destination that does not exist yet.
type envoySite struct {
	arch, asset  string // this machine's Go architecture, and its release asset's name
	site, rel    string // the site, and the version's directory in it
	pins         string
	dest, target string
}

func newEnvoySite(t *testing.T) envoySite {
	t.Helper()
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		t.Skip("hack/fetch-envoy.sh pins linux amd64 and arm64")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		t.Fatal("hack/fetch-envoy.sh needs curl, which is not on PATH")
	}
	arch := runtime.GOARCH
	s := envoySite{arch: arch, asset: "envoy-1.40.2-linux-" + releaseArch[arch], site: t.TempDir()}
	s.rel = filepath.Join(s.site, "v1.40.2")
	if err := os.MkdirAll(s.rel, 0o755); err != nil {
		t.Fatal(err)
	}
	// The other architecture's line is well formed and must not be fetched.
	o := otherArch(arch)
	s.pins = writePins(t, "# version arch asset sha256\n"+
		"1.40.2 "+arch+" "+s.asset+" "+abcSum+"\n"+
		"1.40.2 "+o+" envoy-1.40.2-linux-"+releaseArch[o]+" "+strings.Repeat("0", 64)+"\n")
	s.dest = filepath.Join(t.TempDir(), "bin")
	s.target = filepath.Join(s.dest, "envoy-1.40.2", "envoy")
	return s
}

func writePins(t *testing.T, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "envoy-pins.txt")
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func (s envoySite) env() []string {
	return []string{"ENVOY_RELEASE_BASE=file://" + s.site, "ENVOY_PINS_FILE=" + s.pins}
}

// dead is the environment of a run whose release site does not exist.
func (s envoySite) dead() []string {
	return []string{"ENVOY_RELEASE_BASE=file://" + filepath.Join(s.site, "gone"), "ENVOY_PINS_FILE=" + s.pins}
}

func (s envoySite) publish(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(s.rel, s.asset), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// runFetch runs the script with env added to a copy of this process's environment that has no
// HPCBURST_ENVOY_DIR, and returns its output, both streams together, and its exit code.
func runFetch(t *testing.T, env []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join("..", "..", "..", "hack", "fetch-envoy.sh")}, args...)...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "HPCBURST_ENVOY_DIR=") && !strings.HasPrefix(kv, "ENVOY_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return out.String(), 0
	case errors.As(err, &ee):
		return out.String(), ee.ExitCode()
	}
	t.Fatalf("running hack/fetch-envoy.sh: %v", err)
	return "", -1
}

// expectInstalled: the target holds "abc", mode 755, and nothing else is in its directory.
func (s envoySite) expectInstalled(t *testing.T) {
	t.Helper()
	b, err := os.ReadFile(s.target)
	if err != nil || string(b) != "abc" {
		t.Errorf("%s holds %q (%v), want the pinned bytes", s.target, b, err)
	}
	if fi, err := os.Stat(s.target); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("%s: %v %v, want mode 755", s.target, fi, err)
	}
	s.expectOnly(t, "envoy")
}

// expectOnly: the version's directory holds exactly these names (no staged download left behind).
func (s envoySite) expectOnly(t *testing.T, names ...string) {
	t.Helper()
	ents, err := os.ReadDir(filepath.Dir(s.target))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range ents {
		got = append(got, e.Name())
	}
	if strings.Join(got, " ") != strings.Join(names, " ") {
		t.Errorf("%s holds %v, want %v", filepath.Dir(s.target), got, names)
	}
}

// A download with the wrong sha256 is refused and leaves nothing behind; the right one is installed;
// a second run finds it and downloads nothing, with the site gone.
func TestFetchEnvoyChecksTheDigest(t *testing.T) {
	s := newEnvoySite(t)
	s.publish(t, "bogus")
	out, code := runFetch(t, s.env(), s.dest)
	if code != 1 || !strings.Contains(out, "checksum mismatch for "+s.asset) {
		t.Fatalf("a wrong download: exit %d, want 1 and a checksum mismatch:\n%s", code, out)
	}
	if _, err := os.Stat(s.target); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a wrong download left %s: %v", s.target, err)
	}
	s.expectOnly(t)

	s.publish(t, "abc")
	out, code = runFetch(t, s.env(), s.dest)
	if code != 0 || !strings.Contains(out, "fetching envoy 1.40.2 from file://"+s.rel+"/"+s.asset) || !strings.Contains(out, "verified envoy 1.40.2 ("+s.target+") sha256 "+abcSum) {
		t.Fatalf("the pinned download: exit %d, want 0 after a fetch and a verification:\n%s", code, out)
	}
	s.expectInstalled(t)

	out, code = runFetch(t, s.dead(), s.dest)
	if code != 0 || !strings.Contains(out, "ok       envoy 1.40.2 ("+s.target+")") || strings.Contains(out, "fetching") {
		t.Fatalf("a warm directory with the site gone: exit %d, want 0 with an ok line and no fetch:\n%s", code, out)
	}
	s.expectInstalled(t)
}

// A file in place whose bytes are not the pinned ones (a damaged or planted cache entry of the right
// length and mode) is not trusted: with the site gone the run fails naming what it could not fetch,
// and with the site up the file is replaced. The same holds for a pinned file that is not executable.
func TestFetchEnvoyDownloadsAgainWhatDoesNotMatchItsPin(t *testing.T) {
	for _, c := range []struct {
		name  string
		bytes string
		mode  os.FileMode
	}{{"one byte changed", "abd", 0o755}, {"not executable", "abc", 0o644}} {
		t.Run(c.name, func(t *testing.T) {
			s := newEnvoySite(t)
			s.publish(t, "abc")
			if out, code := runFetch(t, s.env(), s.dest); code != 0 {
				t.Fatalf("filling the directory: exit %d:\n%s", code, out)
			}
			if err := os.WriteFile(s.target, []byte(c.bytes), c.mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(s.target, c.mode); err != nil {
				t.Fatal(err)
			}
			out, code := runFetch(t, s.dead(), s.dest)
			if code != 1 || strings.Contains(out, "ok       envoy") || !strings.Contains(out, "download of file://"+filepath.Join(s.site, "gone")+"/v1.40.2/"+s.asset+" failed") {
				t.Fatalf("site gone: exit %d, want 1 naming the asset it could not fetch and no ok line:\n%s", code, out)
			}
			out, code = runFetch(t, s.env(), s.dest)
			if code != 0 || !strings.Contains(out, "fetching envoy 1.40.2") || !strings.Contains(out, "verified envoy 1.40.2") {
				t.Fatalf("site up: exit %d, want it fetched and verified again:\n%s", code, out)
			}
			s.expectInstalled(t)
		})
	}
}

// With no ENVOY_PINS_FILE the script reads internal/agent/envoypin/envoy-pins.txt, the file this
// package embeds: against an empty site it asks for exactly the release asset that file pins for
// this machine, named here as Envoy names it, not read from the file.
func TestFetchEnvoyReadsThePinsFileThisPackageEmbeds(t *testing.T) {
	s := newEnvoySite(t)
	want := map[string]string{"amd64": "envoy-1.40.2-linux-x86_64", "arm64": "envoy-1.40.2-linux-aarch_64"}[runtime.GOARCH]
	out, code := runFetch(t, []string{"ENVOY_RELEASE_BASE=file://" + s.site}, s.dest)
	if code != 1 || !strings.Contains(out, "download of file://"+s.site+"/v1.40.2/"+want+" failed") {
		t.Fatalf("exit %d, want 1 naming file://%s/v1.40.2/%s:\n%s", code, s.site, want, out)
	}
}

// The script and the Go reader refuse the same malformed lines, so neither accepts a pin the other
// would refuse. Each is refused before anything is downloaded, even on a line for the other
// architecture.
func TestFetchEnvoyAndTheGoReaderRefuseTheSameLines(t *testing.T) {
	s := newEnvoySite(t)
	s.publish(t, "abc")
	mine := "1.40.2 " + s.arch + " " + s.asset + " " + abcSum
	o := otherArch(s.arch)
	oa := "envoy-1.40.2-linux-" + releaseArch[o]
	for _, c := range []struct{ name, line, script string }{
		{"three fields", "1.40.2 " + o + " " + abcSum, "line 2: 3 fields"},
		{"five fields", "1.40.2 " + o + " " + oa + " " + abcSum + " x", "line 2: 5 fields"},
		{"a 63-character sum", "1.40.2 " + o + " " + oa + " " + abcSum[:63], "line 2: sha256"},
		{"an uppercase sum", "1.40.2 " + o + " " + oa + " " + strings.ToUpper(abcSum), "line 2: sha256"},
		{"a v-prefixed version", "v1.40.2 " + o + " " + oa + " " + abcSum, "line 2: version"},
		{"the kernel's architecture name", "1.40.2 x86_64 envoy-1.40.2-linux-x86_64 " + abcSum, "line 2: architecture"},
		{"an asset of the wrong architecture", "1.40.2 " + o + " " + s.asset + " " + abcSum, "line 2: asset"},
	} {
		t.Run(c.name, func(t *testing.T) {
			text := mine + "\n" + c.line + "\n"
			if _, err := parse(text); err == nil {
				t.Errorf("the Go reader accepts %q", c.line)
			}
			s.pins = writePins(t, text)
			out, code := runFetch(t, s.env(), s.dest)
			if code != 1 || !strings.Contains(out, c.script) || strings.Contains(out, "fetching") {
				t.Errorf("the script: exit %d, want 1 with %q before any fetch:\n%s", code, c.script, out)
			}
		})
	}
	text := mine + "\n1.40.2 " + o + " " + oa + " " + abcSum + "\n"
	if _, err := parse(text); err != nil {
		t.Errorf("the Go reader refuses good lines: %v", err)
	}
	s.pins = writePins(t, text)
	if out, code := runFetch(t, s.env(), s.dest); code != 0 {
		t.Errorf("the script refuses good lines: exit %d:\n%s", code, out)
	}
}

func TestFetchEnvoyFailsWithNoPinForThisMachineOrNoDirectory(t *testing.T) {
	s := newEnvoySite(t)
	o := otherArch(s.arch)
	s.pins = writePins(t, "1.40.2 "+o+" envoy-1.40.2-linux-"+releaseArch[o]+" "+abcSum+"\n")
	out, code := runFetch(t, s.env(), s.dest)
	if code != 1 || !strings.Contains(out, "pins no Envoy binary for "+s.arch) {
		t.Errorf("no line for %s: exit %d, want 1:\n%s", s.arch, code, out)
	}
	out, code = runFetch(t, s.env())
	if code != 1 || !strings.Contains(out, "usage: hack/fetch-envoy.sh DIR") {
		t.Errorf("no DIR and no HPCBURST_ENVOY_DIR: exit %d, want 1 and the usage:\n%s", code, out)
	}
	s = newEnvoySite(t)
	s.publish(t, "abc")
	out, code = runFetch(t, append(s.env(), "HPCBURST_ENVOY_DIR="+s.dest))
	if code != 0 || !strings.Contains(out, "verified envoy 1.40.2 ("+s.target+")") {
		t.Errorf("DIR from HPCBURST_ENVOY_DIR: exit %d, want it installed at %s:\n%s", code, s.target, out)
	}
}

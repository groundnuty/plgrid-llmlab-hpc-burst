package testenv

// hack/fetch-frp.sh --agent-builds DIR places frpc for each pinned architecture as
// DIR/frpc-<cpuArch>, where the Provider's controller finds it (agentapi.FrpcBuildName). The tests
// run offline: a release laid out as <base>/v<version>/<tarball> in a temporary directory, which the
// script reads as a file:// mirror (FRP_RELEASE_BASE), and a pins file made for those tarballs
// (FRP_PINS_FILE). The sums in that pins file are computed here with crypto/sha256 from the bytes the
// test built; the script computes its own with sha256sum, so neither side checks itself. The real
// pins file and the real release are only used when an operator runs the script.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func sumHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// frpTarball is a release tarball holding frps and frpc under frp_<version>_linux_<arch>/.
func frpTarball(t *testing.T, version, arch string, frps, frpc []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	dir := "frp_" + version + "_linux_" + arch
	for _, f := range []struct {
		name string
		body []byte
	}{{"frps", frps}, {"frpc", frpc}} {
		if err := tw.WriteHeader(&tar.Header{Name: dir + "/" + f.name, Mode: 0o644, Size: int64(len(f.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(f.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// fakeRelease is a release of version 0.71.1 for the given architectures, the frpc of each a text
// naming it.
type fakeRelease struct {
	base, pins string            // the mirror's directory, and the pins file made for it
	frpc       map[string][]byte // by frp architecture
	tarball    map[string]string // the tarball's path, by frp architecture
}

func newFakeRelease(t *testing.T, archs ...string) fakeRelease {
	t.Helper()
	const version = "0.71.1"
	r := fakeRelease{base: t.TempDir(), frpc: map[string][]byte{}, tarball: map[string]string{}}
	if err := os.MkdirAll(filepath.Join(r.base, "v"+version), 0o755); err != nil {
		t.Fatal(err)
	}
	lines := []string{"# a pins file for a fake release"}
	for _, arch := range archs {
		frps, frpc := []byte("frps for "+arch+"\n"), []byte("frpc for "+arch+"\n")
		tgz := frpTarball(t, version, arch, frps, frpc)
		name := "frp_" + version + "_linux_" + arch + ".tar.gz"
		r.tarball[arch] = filepath.Join(r.base, "v"+version, name)
		if err := os.WriteFile(r.tarball[arch], tgz, 0o644); err != nil {
			t.Fatal(err)
		}
		r.frpc[arch] = frpc
		lines = append(lines, fmt.Sprintf("%s %s %s %s %s %s", version, arch, name, sumHex(tgz), sumHex(frps), sumHex(frpc)))
	}
	r.pins = filepath.Join(t.TempDir(), "frp-pins.txt")
	if err := os.WriteFile(r.pins, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return r
}

// runAgentBuilds runs the script in agent-builds mode and returns its output and exit code. Extra
// environment entries come last and win.
func runAgentBuilds(t *testing.T, args []string, env ...string) (string, int) {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(root, "hack", "fetch-frp.sh")}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	if err != nil {
		t.Fatalf("running hack/fetch-frp.sh: %v", err)
	}
	return string(out), 0
}

func (r fakeRelease) env() []string {
	return []string{"FRP_RELEASE_BASE=file://" + r.base, "FRP_PINS_FILE=" + r.pins}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// Both architectures are placed under their cpuArch names, each the frpc of its own tarball: the
// amd64 line as frpc-x86_64 and the arm64 line as frpc-aarch64, mode 755, with nothing else left in
// the directory (not frps, not the stage). A frpc already at a name with other content is replaced,
// another file is kept, and a second run fetches nothing: the mirror is gone and it still succeeds.
func TestFetchFrpAgentBuildsPlacesEachArchitectureUnderItsCPUArch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hack/fetch-frp.sh needs a GNU userland")
	}
	r := newFakeRelease(t, "amd64", "arm64")
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "other"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "frpc-x86_64"), []byte("a stale frpc\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	out, code := runAgentBuilds(t, []string{"--agent-builds", dest}, r.env()...)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	for arch, cpu := range map[string]string{"amd64": "x86_64", "arm64": "aarch64"} {
		p := filepath.Join(dest, "frpc-"+cpu)
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, r.frpc[arch]) {
			t.Errorf("%s holds %q (%v), want the frpc of the %s tarball, %q", p, got, err, arch, r.frpc[arch])
		}
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o755 {
			t.Errorf("%s: %v, %v; want mode 755", p, fi, err)
		}
		if !strings.Contains(out, "verified frpc-"+cpu) || !strings.Contains(out, sumHex(r.frpc[arch])) {
			t.Errorf("the output does not report frpc-%s with its sum:\n%s", cpu, out)
		}
	}
	if got := strings.Join(dirNames(t, dest), " "); got != "frpc-aarch64 frpc-x86_64 other" {
		t.Errorf("the directory holds %q, want frpc-aarch64, frpc-x86_64 and the other file only", got)
	}

	out, code = runAgentBuilds(t, []string{"--agent-builds", dest}, "FRP_RELEASE_BASE=file://"+filepath.Join(r.base, "gone"), "FRP_PINS_FILE="+r.pins)
	if code != 0 || strings.Count(out, "ok       frpc-") != 2 || strings.Contains(out, "fetching") {
		t.Errorf("second run: exit %d, want 0 and two ok lines with no fetch:\n%s", code, out)
	}
}

// A tarball that does not match its pin is never extracted and never placed; the line before it
// stays placed (an architecture is fetched on its own). "Never extracted" is judged from a tar on
// PATH that records its calls, with a control that the wrapper is reached.
func TestFetchFrpAgentBuildsRefusesATarballThatDoesNotMatchItsPin(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hack/fetch-frp.sh needs a GNU userland")
	}
	r := newFakeRelease(t, "amd64", "arm64")
	// Another valid archive under the arm64 name: only the pin stands between it and the install.
	if err := os.WriteFile(r.tarball["arm64"], frpTarball(t, "0.71.1", "arm64", []byte("frps\n"), []byte("a frpc that is not pinned\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	realTar, err := exec.LookPath("tar")
	if err != nil {
		t.Fatal(err)
	}
	wrapDir, tarLog := t.TempDir(), filepath.Join(t.TempDir(), "tar-calls")
	wrapper := "#!/bin/sh\necho \"$@\" >> '" + tarLog + "'\nexec '" + realTar + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(wrapDir, "tar"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	pathEnv := "PATH=" + wrapDir + ":" + os.Getenv("PATH")
	ctl := exec.Command("bash", "-c", "tar --version")
	ctl.Env = append(os.Environ(), pathEnv)
	if out, err := ctl.CombinedOutput(); err != nil {
		t.Fatalf("control: %v\n%s", err, out)
	}
	if b, _ := os.ReadFile(tarLog); strings.Count(string(b), "\n") != 1 {
		t.Fatalf("the tar wrapper is not reached through PATH: %q", b)
	}
	if err := os.Remove(tarLog); err != nil {
		t.Fatal(err)
	}

	dest := t.TempDir()
	out, code := runAgentBuilds(t, []string{"--agent-builds", dest}, append(r.env(), pathEnv)...)
	if code != 1 || !strings.Contains(out, "checksum mismatch for frp_0.71.1_linux_arm64.tar.gz") {
		t.Fatalf("exit %d, want 1 with a checksum mismatch for the arm64 tarball:\n%s", code, out)
	}
	if got := strings.Join(dirNames(t, dest), " "); got != "frpc-x86_64" {
		t.Errorf("the directory holds %q, want only the amd64 line's frpc-x86_64", got)
	}
	b, _ := os.ReadFile(tarLog)
	if strings.Count(string(b), "\n") != 1 || strings.Contains(string(b), "arm64") {
		t.Errorf("tar calls %q, want one, for the amd64 tarball: the arm64 one had no matching hash", b)
	}
}

// A frpc whose sum is not the pin's, inside a tarball that does match its own pin, is not placed.
func TestFetchFrpAgentBuildsRefusesAFrpcThatDoesNotMatchItsPin(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hack/fetch-frp.sh needs a GNU userland")
	}
	r := newFakeRelease(t, "amd64", "arm64")
	b, err := os.ReadFile(r.pins)
	if err != nil {
		t.Fatal(err)
	}
	// The arm64 line's frpc column becomes the amd64 frpc's sum.
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	f := strings.Fields(lines[2])
	f[5] = sumHex(r.frpc["amd64"])
	lines[2] = strings.Join(f, " ")
	if err := os.WriteFile(r.pins, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	out, code := runAgentBuilds(t, []string{"--agent-builds", dest}, r.env()...)
	if code != 1 || !strings.Contains(out, "does not match its pin") {
		t.Fatalf("exit %d, want 1 with a frpc that does not match its pin:\n%s", code, out)
	}
	if got := strings.Join(dirNames(t, dest), " "); got != "frpc-x86_64" {
		t.Errorf("the directory holds %q, want only the amd64 line's frpc-x86_64", got)
	}
}

// An architecture with no cpuArch name stops the run before the first download, and nothing is
// placed, not even for the lines that are known: the mirror does not exist, so any download would
// say so, and the output does not.
func TestFetchFrpAgentBuildsRefusesAnArchitectureWithNoCPUArchName(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hack/fetch-frp.sh needs a GNU userland")
	}
	r := newFakeRelease(t, "amd64", "riscv64")
	dest := t.TempDir()
	out, code := runAgentBuilds(t, []string{"--agent-builds", dest}, "FRP_RELEASE_BASE=file://"+filepath.Join(r.base, "gone"), "FRP_PINS_FILE="+r.pins)
	if code != 1 || !strings.Contains(out, "riscv64") || !strings.Contains(out, "no cpuArch name") || strings.Contains(out, "download of") {
		t.Fatalf("exit %d, want 1 naming riscv64 before any download:\n%s", code, out)
	}
	if got := dirNames(t, dest); len(got) != 0 {
		t.Errorf("the directory holds %v, want nothing", got)
	}
}

// The mode needs its directory, and a release the pins file does not hold is refused.
func TestFetchFrpAgentBuildsArguments(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hack/fetch-frp.sh needs a GNU userland")
	}
	if out, code := runAgentBuilds(t, []string{"--agent-builds"}); code != 1 || !strings.Contains(out, "usage") {
		t.Errorf("no directory: exit %d, want 1 with the usage:\n%s", code, out)
	}
	empty := filepath.Join(t.TempDir(), "frp-pins.txt")
	if err := os.WriteFile(empty, []byte("# no lines\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if out, code := runAgentBuilds(t, []string{"--agent-builds", dest}, "FRP_PINS_FILE="+empty); code != 1 || !strings.Contains(out, "is not pinned") {
		t.Errorf("a pins file with no line for the release: exit %d, want 1:\n%s", code, out)
	}
	if got := dirNames(t, dest); len(got) != 0 {
		t.Errorf("the directory holds %v, want nothing", got)
	}
}

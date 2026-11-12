package testenv

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
)

// The pinned Envoy itself. The expected sums are typed from spikes/s13/README.md (Provenance), where
// S13 recorded them from the release's signed checksums.txt.asc; the file is hashed here with
// crypto/sha256, not through package envoypin. The commit in the version line is the one S13's runs
// printed on x86_64 and on the aarch64 runner (spikes/s13/README.md, aarch64).
func TestEnvoyBinaryWithTheRealBinary(t *testing.T) {
	p := EnvoyBinary(t)
	if !filepath.IsAbs(p) || filepath.Base(p) != "envoy" || filepath.Base(filepath.Dir(p)) != "envoy-1.40.2" {
		t.Errorf("EnvoyBinary = %s, want an absolute DIR/envoy-1.40.2/envoy", p)
	}
	want := map[string]string{
		"amd64": "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc",
		"arm64": "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf",
	}[runtime.GOARCH]
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Errorf("%s has sha256 %s, want the signed checksum %s", p, got, want)
	}
	out, err := exec.Command(p, "--version").Output()
	if err != nil {
		t.Fatalf("%s --version: %v", p, err)
	}
	if !strings.Contains(string(out), "b2d89e0ae53916f28770771fbdd49c5c99c5c5a2/1.40.2/Clean/RELEASE/BoringSSL") {
		t.Errorf("%s --version printed %q, want release 1.40.2 at commit b2d89e0", p, out)
	}
	// A second call is the cached path and returns the same file.
	if p2 := EnvoyBinary(t); p2 != p {
		t.Errorf("a second EnvoyBinary = %s, want %s", p2, p)
	}
}

// fakeEnvoyDir is a directory as hack/fetch-envoy.sh lays it out, holding a shell script in the
// binary's place: never the pinned bytes.
func fakeEnvoyDir(t *testing.T, script string, mode os.FileMode) string {
	t.Helper()
	dir := t.TempDir()
	p := envoypin.BinaryPath(dir)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(script), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestFindEnvoy(t *testing.T) {
	impostor := fakeEnvoyDir(t, "#!/bin/sh\necho 'envoy  version: b2d89e0ae53916f28770771fbdd49c5c99c5c5a2/1.40.2/Clean/RELEASE/BoringSSL'\n", 0o755)
	notExec := fakeEnvoyDir(t, "#!/bin/sh\n", 0o644)
	dirInstead := t.TempDir()
	if err := os.MkdirAll(envoypin.BinaryPath(dirInstead), 0o755); err != nil {
		t.Fatal(err)
	}
	otherVersion := t.TempDir()
	if err := os.MkdirAll(filepath.Join(otherVersion, "envoy-1.40.1"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		dir      string
		absent   bool // may skip
		mismatch bool // wraps envoypin.ErrMismatch
		want     string
	}{
		{"unset", "", true, false, envEnvoyDir + " is not set"},
		{"no such directory", filepath.Join(t.TempDir(), "nope"), true, false, "envoy-1.40.2/envoy does not exist"},
		{"only another version", otherVersion, true, false, "envoy-1.40.2/envoy does not exist"},
		{"relative", "envoy-bin", false, false, "absolute"},
		{"not executable", notExec, false, false, "not an executable file"},
		{"a directory in its place", dirInstead, false, false, "not an executable file"},
		// It prints the right version line, so only the sha256 can refuse it.
		{"another binary of the right name", impostor, false, true, "the sha256 is not the pinned one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := findEnvoy(tc.dir, runtime.GOARCH)
			if err == nil {
				t.Fatal("findEnvoy accepted it")
			}
			if got := errors.Is(err, errEnvoyAbsent); got != tc.absent {
				t.Errorf("absent = %v, want %v (%v)", got, tc.absent, err)
			}
			if got := errors.Is(err, envoypin.ErrMismatch); got != tc.mismatch {
				t.Errorf("mismatch = %v, want %v (%v)", got, tc.mismatch, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
	if _, err := findEnvoy(impostor, "riscv64"); err == nil || errors.Is(err, errEnvoyAbsent) {
		t.Errorf("an unpinned architecture: %v, want an error that is not a skip", err)
	}
}

// The version check alone, on scripts: the sha256 check before it refuses any script, so it is
// reached in findEnvoy only by the real binary.
func TestCheckEnvoyVersion(t *testing.T) {
	for _, tc := range []struct{ name, line, want string }{
		{"the pinned release", "envoy  version: b2d89e0ae53916f28770771fbdd49c5c99c5c5a2/1.40.2/Clean/RELEASE/BoringSSL", ""},
		{"another release", "envoy  version: 0000000000000000000000000000000000000000/1.40.1/Clean/RELEASE/BoringSSL", "does not name release 1.40.2"},
		{"a release with a suffix", "envoy  version: 0000000000000000000000000000000000000000/1.40.20/Clean/RELEASE/BoringSSL", "does not name release 1.40.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := fakeEnvoyDir(t, "#!/bin/sh\necho '"+tc.line+"'\n", 0o755)
			err := checkEnvoyVersion(envoypin.BinaryPath(dir))
			if tc.want == "" && err != nil {
				t.Errorf("refused: %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Errorf("error %v, want one containing %q", err, tc.want)
			}
		})
	}
	dir := fakeEnvoyDir(t, "#!/bin/sh\nexit 3\n", 0o755)
	if err := checkEnvoyVersion(envoypin.BinaryPath(dir)); err == nil || !strings.Contains(err.Error(), "--version failed") {
		t.Errorf("a binary that exits 3: %v, want --version failed", err)
	}
}

func TestEnvoyRequired(t *testing.T) {
	for v, want := range map[string]bool{"": false, "0": false, "1": true} {
		if got, err := envoyRequired(v); err != nil || got != want {
			t.Errorf("envoyRequired(%q) = %v, %v; want %v", v, got, err, want)
		}
	}
	for _, v := range []string{"true", "yes", "2", "01", " 1", "1 ", "TRUE"} {
		if _, err := envoyRequired(v); err == nil {
			t.Errorf("envoyRequired(%q) was accepted; a value other than 1 must not quietly mean off", v)
		}
	}
}

// envoyProbeEnv is set by the tests below to run TestEnvoyBinaryProbe for real; without it the probe
// does nothing and passes.
const envoyProbeEnv = "TESTENV_ENVOY_PROBE"

// TestEnvoyBinaryProbe is run only as a subprocess of runEnvoyProbe. It calls EnvoyBinary, so the
// subprocess ends with whatever EnvoyBinary makes of its environment: a skip, a failure or a pass.
func TestEnvoyBinaryProbe(t *testing.T) {
	if os.Getenv(envoyProbeEnv) != "1" {
		return
	}
	t.Logf("probe got %s", EnvoyBinary(t))
}

// runEnvoyProbe runs this test binary on TestEnvoyBinaryProbe alone, with exactly the given Envoy
// variables (the caller's own are removed first), and returns its combined output and exit code: 0
// for a pass and a skip, 1 for a failed test.
func runEnvoyProbe(t *testing.T, dir, require string) (string, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestEnvoyBinaryProbe$", "-test.v", "-test.count=1")
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, envEnvoyDir+"=") && !strings.HasPrefix(kv, envRequireEnvoy+"=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, envoyProbeEnv+"=1")
	if dir != "" {
		cmd.Env = append(cmd.Env, envEnvoyDir+"="+dir)
	}
	if require != "" {
		cmd.Env = append(cmd.Env, envRequireEnvoy+"="+require)
	}
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
	t.Fatalf("running the probe: %v", err)
	return "", -1
}

func TestEnvoyBinarySkipsWhenAbsentAndNotRequired(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	for name, dir := range map[string]string{"unset": "", "no such directory": missing} {
		for _, require := range []string{"", "0"} {
			out, code := runEnvoyProbe(t, dir, require)
			if code != 0 || !strings.Contains(out, "--- SKIP: TestEnvoyBinaryProbe") {
				t.Errorf("%s, require=%q: exit %d, want a skip with exit 0:\n%s", name, require, code, out)
			}
			if !strings.Contains(out, "hack/fetch-envoy.sh") || !strings.Contains(out, envRequireEnvoy+"=1") {
				t.Errorf("%s, require=%q: the skip message does not say how to install or require:\n%s", name, require, out)
			}
		}
	}
}

func TestEnvoyBinaryFailsWhenAbsentAndRequired(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	for name, dir := range map[string]string{"unset": "", "no such directory": missing} {
		out, code := runEnvoyProbe(t, dir, "1")
		if code != 1 || !strings.Contains(out, "--- FAIL: TestEnvoyBinaryProbe") || strings.Contains(out, "--- SKIP") {
			t.Errorf("%s, require=1: exit %d, want a failure with exit 1 and no skip:\n%s", name, code, out)
		}
		if !strings.Contains(out, envRequireEnvoy+"=1 forbids skipping") {
			t.Errorf("%s, require=1: the failure does not say why:\n%s", name, out)
		}
	}
}

// A wrong binary fails in either mode: a skip would hide a broken install.
func TestEnvoyBinaryFailsOnAWrongBinaryEvenWhenNotRequired(t *testing.T) {
	impostor := fakeEnvoyDir(t, "#!/bin/sh\necho 'envoy  version: b2d89e0ae53916f28770771fbdd49c5c99c5c5a2/1.40.2/Clean/RELEASE/BoringSSL'\n", 0o755)
	for _, require := range []string{"", "1"} {
		out, code := runEnvoyProbe(t, impostor, require)
		if code != 1 || !strings.Contains(out, "--- FAIL: TestEnvoyBinaryProbe") || !strings.Contains(out, "the sha256 is not the pinned one") {
			t.Errorf("require=%q: exit %d, want a failure naming the sha256:\n%s", require, code, out)
		}
	}
}

func TestEnvoyBinaryRefusesAMistypedRequireFlag(t *testing.T) {
	// "true" must not read as off: with the binary missing it would skip and a CI run would pass.
	out, code := runEnvoyProbe(t, "", "true")
	if code != 1 || !strings.Contains(out, envRequireEnvoy+`="true"`) {
		t.Errorf("require=true: exit %d, want a failure naming the value:\n%s", code, out)
	}
}

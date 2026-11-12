package testenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
)

const (
	// envEnvoyDir names the directory hack/fetch-envoy.sh DIR installs into; the binary is
	// DIR/envoy-<version>/envoy (envoypin.BinaryPath).
	envEnvoyDir = "HPCBURST_ENVOY_DIR"

	// envRequireEnvoy, set to 1, turns "Envoy is not there" from a skip into a failure. CI sets it, so
	// a runner that lost its binary cannot pass by skipping every test that runs Envoy.
	envRequireEnvoy = "HPCBURST_REQUIRE_ENVOY"
)

// errEnvoyAbsent marks the one case that may skip: there is no Envoy to test with (the directory is
// not named, or holds no binary of the pinned version). A binary that is there but wrong (not
// executable, another sha256, does not run, another version) is a plain error, because a skip would
// hide a broken install.
var errEnvoyAbsent = errors.New("the Envoy binary is not available")

// EnvoyBinary returns the path of the pinned Envoy (envoypin.Version) under the directory that
// HPCBURST_ENVOY_DIR names, after checking that it is an executable file whose sha256 is the pin for
// this machine's architecture (internal/agent/envoypin/envoy-pins.txt) and whose --version names that
// release. The directory must be absolute; hack/fetch-envoy.sh DIR installs the binary into it.
//
// When HPCBURST_ENVOY_DIR is unset or holds no binary, the test is skipped with a message that says how
// to install it, unless HPCBURST_REQUIRE_ENVOY=1, which fails the test instead. HPCBURST_REQUIRE_ENVOY
// must be 1, 0 or unset: any other value fails, so that a mistyped "true" cannot turn the check off. A
// binary that exists but is wrong fails the test in either mode.
func EnvoyBinary(t testing.TB) string {
	t.Helper()
	require, err := envoyRequired(os.Getenv(envRequireEnvoy))
	if err != nil {
		t.Fatal(err)
	}
	p, err := findEnvoy(os.Getenv(envEnvoyDir), runtime.GOARCH)
	switch {
	case err == nil:
		return p
	case errors.Is(err, errEnvoyAbsent) && !require:
		t.Skipf("%v; install it with hack/fetch-envoy.sh DIR and set %s=DIR (%s=1 makes this a failure)",
			err, envEnvoyDir, envRequireEnvoy)
	case errors.Is(err, errEnvoyAbsent):
		t.Fatalf("%v; %s=1 forbids skipping: install it with hack/fetch-envoy.sh DIR and set %s=DIR",
			err, envRequireEnvoy, envEnvoyDir)
	default:
		t.Fatal(err)
	}
	return "" // not reached: Skipf and Fatal stop the test
}

// envoyRequired reads the value of HPCBURST_REQUIRE_ENVOY.
func envoyRequired(v string) (bool, error) {
	switch v {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	}
	return false, fmt.Errorf("%s=%q: the value must be 1 or unset (0 also means unset)", envRequireEnvoy, v)
}

// verifiedEnvoy remembers the binaries findEnvoy has checked, by path, size and modification time, so
// that the many tests of one process hash the 107 MB file once. A file changed in place gets another
// size or time and is checked again.
var verifiedEnvoy sync.Map // string -> struct{}

// findEnvoy checks dir and returns the binary's path. Every error that is not "absent" is final.
func findEnvoy(dir, goarch string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("%w: %s is not set", errEnvoyAbsent, envEnvoyDir)
	}
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("%s=%q must be an absolute path (a test runs in its package directory)", envEnvoyDir, dir)
	}
	p := envoypin.BinaryPath(dir)
	fi, err := os.Stat(p)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "", fmt.Errorf("%w: %s does not exist (%s=%s)", errEnvoyAbsent, p, envEnvoyDir, dir)
	case err != nil:
		return "", err
	case !fi.Mode().IsRegular() || fi.Mode()&0o111 == 0:
		return "", fmt.Errorf("%s is not an executable file (mode %v)", p, fi.Mode())
	}
	key := fmt.Sprintf("%s\x00%d\x00%d\x00%s", p, fi.Size(), fi.ModTime().UnixNano(), goarch)
	if _, ok := verifiedEnvoy.Load(key); ok {
		return p, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	err = envoypin.Check(f, goarch)
	f.Close()
	if err != nil {
		return "", fmt.Errorf("%s: %w (hack/fetch-envoy.sh installs the pinned binary)", p, err)
	}
	if err := checkEnvoyVersion(p); err != nil {
		return "", err
	}
	verifiedEnvoy.Store(key, struct{}{})
	return p, nil
}

// checkEnvoyVersion runs bin --version, whose line must name envoypin.Version as its release
// ("version: <commit>/1.40.2/Clean/RELEASE/BoringSSL").
func checkEnvoyVersion(bin string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	if err != nil {
		return fmt.Errorf("%s --version failed: %w", bin, err)
	}
	if !strings.Contains(string(out), "/"+envoypin.Version+"/") {
		return fmt.Errorf("%s --version printed %q, which does not name release %s", bin, strings.TrimSpace(string(out)), envoypin.Version)
	}
	return nil
}

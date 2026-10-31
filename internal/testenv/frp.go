package testenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	// envFrpDir names the directory that holds frps and frpc. hack/fetch-frp.sh DIR puts the
	// pinned release there.
	envFrpDir = "HPCBURST_FRP_DIR"

	// envRequireFrp, set to 1, turns "the frp binaries are not there" from a skip into a failure.
	// CI sets it, so a runner that lost its binaries cannot pass by skipping every tunnel test.
	envRequireFrp = "HPCBURST_REQUIRE_FRP"

	// frpVersion is the release hack/fetch-frp.sh installs (HBEP-6's tunnel). Both binaries must
	// print exactly this from --version.
	frpVersion = "0.71.1"
)

// errFrpAbsent marks the one case that may skip: there is no frp to test with (the directory is
// not named, or a binary is not in it). A binary that is there but wrong (not executable, not
// runnable, another version) is a plain error, because a skip would hide a broken install.
var errFrpAbsent = errors.New("frp binaries are not available")

// FrpBinaries returns the paths of frps and frpc from the directory named by HPCBURST_FRP_DIR,
// after checking that both are executable files and that each prints 0.71.1 from --version. The
// directory must be absolute; hack/fetch-frp.sh DIR installs the pinned release into it.
//
// When HPCBURST_FRP_DIR is unset or lacks a binary, the test is skipped with a message that says
// how to install them, unless HPCBURST_REQUIRE_FRP=1, which fails the test instead. HPCBURST_REQUIRE_FRP
// must be 1, 0 or unset: any other value fails, so that a mistyped "true" cannot turn the check off.
// A binary that exists but is not executable, does not run, or is another version fails the test in
// either mode.
func FrpBinaries(t testing.TB) (frps, frpc string) {
	t.Helper()
	require, err := frpRequired(os.Getenv(envRequireFrp))
	if err != nil {
		t.Fatal(err)
	}
	frps, frpc, err = findFrp(os.Getenv(envFrpDir))
	switch {
	case err == nil:
		return frps, frpc
	case errors.Is(err, errFrpAbsent) && !require:
		t.Skipf("%v; install them with hack/fetch-frp.sh DIR and set %s=DIR (%s=1 makes this a failure)",
			err, envFrpDir, envRequireFrp)
	case errors.Is(err, errFrpAbsent):
		t.Fatalf("%v; %s=1 forbids skipping: install them with hack/fetch-frp.sh DIR and set %s=DIR",
			err, envRequireFrp, envFrpDir)
	default:
		t.Fatal(err)
	}
	return "", "" // not reached: Skipf and Fatal stop the test
}

// frpRequired reads the value of HPCBURST_REQUIRE_FRP.
func frpRequired(v string) (bool, error) {
	switch v {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	}
	return false, fmt.Errorf("%s=%q: the value must be 1 or unset (0 also means unset)", envRequireFrp, v)
}

// findFrp checks dir and returns the two binaries' paths. Every error that is not "absent" is final.
func findFrp(dir string) (frps, frpc string, err error) {
	if dir == "" {
		return "", "", fmt.Errorf("%w: %s is not set", errFrpAbsent, envFrpDir)
	}
	if !filepath.IsAbs(dir) {
		return "", "", fmt.Errorf("%s=%q must be an absolute path (a test runs in its package directory)", envFrpDir, dir)
	}
	frps, frpc = filepath.Join(dir, "frps"), filepath.Join(dir, "frpc")
	for _, p := range []string{frps, frpc} {
		fi, err := os.Stat(p)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return "", "", fmt.Errorf("%w: %s does not exist (%s=%s)", errFrpAbsent, p, envFrpDir, dir)
		case err != nil:
			return "", "", err
		case !fi.Mode().IsRegular() || fi.Mode()&0o111 == 0:
			return "", "", fmt.Errorf("%s is not an executable file (mode %v)", p, fi.Mode())
		}
		if err := checkFrpVersion(p); err != nil {
			return "", "", err
		}
	}
	return frps, frpc, nil
}

// checkFrpVersion runs bin --version, which must print exactly frpVersion.
func checkFrpVersion(bin string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	if err != nil {
		return fmt.Errorf("%s --version failed: %w", bin, err)
	}
	if got := strings.TrimSpace(string(out)); got != frpVersion {
		return fmt.Errorf("%s --version printed %q, want %q (hack/fetch-frp.sh installs the pinned release)", bin, got, frpVersion)
	}
	return nil
}

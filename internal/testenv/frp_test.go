package testenv

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

// fakeFrpDir writes frps and frpc as shell scripts that print version from --version.
func fakeFrpDir(t *testing.T, version string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"frps", "frpc"} {
		script := "#!/bin/sh\necho " + version + "\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The positive control: with the real pinned binaries, FrpBinaries returns their paths and the
// test runs --version on each itself. It skips without HPCBURST_FRP_DIR (and fails with
// HPCBURST_REQUIRE_FRP=1, which is how CI runs it).
func TestFrpBinariesWithTheRealBinaries(t *testing.T) {
	frps, frpc := FrpBinaries(t)
	for _, p := range []string{frps, frpc} {
		if !filepath.IsAbs(p) {
			t.Errorf("%s is not absolute", p)
		}
		out, err := exec.Command(p, "--version").Output()
		if err != nil {
			t.Fatalf("%s --version: %v", p, err)
		}
		if got := strings.TrimSpace(string(out)); got != "0.71.1" {
			t.Errorf("%s --version printed %q, want 0.71.1", p, got)
		}
	}
	if filepath.Base(frps) != "frps" || filepath.Base(frpc) != "frpc" {
		t.Errorf("got frps=%s frpc=%s", frps, frpc)
	}
}

func TestFindFrp(t *testing.T) {
	good := fakeFrpDir(t, "0.71.1")
	frps, frpc, err := findFrp(good)
	if err != nil || frps != filepath.Join(good, "frps") || frpc != filepath.Join(good, "frpc") {
		t.Fatalf("findFrp(good) = %q, %q, %v", frps, frpc, err)
	}

	onlyFrps := fakeFrpDir(t, "0.71.1")
	if err := os.Remove(filepath.Join(onlyFrps, "frpc")); err != nil {
		t.Fatal(err)
	}
	notExec := fakeFrpDir(t, "0.71.1")
	if err := os.Chmod(filepath.Join(notExec, "frpc"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirInstead := fakeFrpDir(t, "0.71.1")
	_ = os.Remove(filepath.Join(dirInstead, "frps"))
	if err := os.Mkdir(filepath.Join(dirInstead, "frps"), 0o755); err != nil {
		t.Fatal(err)
	}
	failing := fakeFrpDir(t, "0.71.1")
	if err := os.WriteFile(filepath.Join(failing, "frpc"), []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	oneWrong := fakeFrpDir(t, "0.71.1")
	if err := os.WriteFile(filepath.Join(oneWrong, "frpc"), []byte("#!/bin/sh\necho 0.71.0\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		dir    string
		absent bool   // may skip
		want   string // in the error
	}{
		{"unset", "", true, envFrpDir + " is not set"},
		{"no such directory", filepath.Join(t.TempDir(), "nope"), true, "frps does not exist"},
		{"frpc missing", onlyFrps, true, "frpc does not exist"},
		{"relative", "frp-bin", false, "absolute"},
		{"frpc not executable", notExec, false, "not an executable file"},
		{"frps is a directory", dirInstead, false, "not an executable file"},
		{"frpc exits 3", failing, false, "--version failed"},
		{"older version", fakeFrpDir(t, "0.71.0"), false, `printed "0.71.0", want "0.71.1"`},
		{"newer version", fakeFrpDir(t, "0.72.0"), false, `printed "0.72.0"`},
		{"only frpc is wrong", oneWrong, false, `printed "0.71.0"`},
		{"a version with a suffix", fakeFrpDir(t, "0.71.1-rc1"), false, `printed "0.71.1-rc1"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := findFrp(tc.dir)
			if err == nil {
				t.Fatal("findFrp accepted it")
			}
			if got := errors.Is(err, errFrpAbsent); got != tc.absent {
				t.Errorf("absent = %v, want %v (%v)", got, tc.absent, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestFrpRequired(t *testing.T) {
	for v, want := range map[string]bool{"": false, "0": false, "1": true} {
		if got, err := frpRequired(v); err != nil || got != want {
			t.Errorf("frpRequired(%q) = %v, %v; want %v", v, got, err, want)
		}
	}
	for _, v := range []string{"true", "yes", "2", "01", " 1", "1 ", "TRUE"} {
		if _, err := frpRequired(v); err == nil {
			t.Errorf("frpRequired(%q) was accepted; a value other than 1 must not quietly mean off", v)
		}
	}
}

// frpProbeEnv is set by the tests below to run TestFrpBinariesProbe for real; without it the probe
// does nothing and passes.
const frpProbeEnv = "TESTENV_FRP_PROBE"

// TestFrpBinariesProbe is run only as a subprocess of runFrpProbe. It calls FrpBinaries, so the
// subprocess ends with whatever FrpBinaries makes of its environment: a skip, a failure or a pass.
func TestFrpBinariesProbe(t *testing.T) {
	if os.Getenv(frpProbeEnv) != "1" {
		return
	}
	frps, frpc := FrpBinaries(t)
	t.Logf("probe got frps=%s frpc=%s", frps, frpc)
}

// runFrpProbe runs this test binary on TestFrpBinariesProbe alone, with exactly the given frp
// variables (the caller's own HPCBURST_FRP_DIR and HPCBURST_REQUIRE_FRP are removed first), and
// returns its combined output and exit code. The exit code is the test binary's own, the one
// go test reports for the package: 0 for pass and skip, 1 for a failed test.
func runFrpProbe(t *testing.T, dir, require string) (string, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFrpBinariesProbe$", "-test.v", "-test.count=1")
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, envFrpDir+"=") && !strings.HasPrefix(kv, envRequireFrp+"=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, frpProbeEnv+"=1")
	if dir != "" {
		cmd.Env = append(cmd.Env, envFrpDir+"="+dir)
	}
	if require != "" {
		cmd.Env = append(cmd.Env, envRequireFrp+"="+require)
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

func TestFrpBinariesSkipsWhenAbsentAndNotRequired(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	for name, dir := range map[string]string{"unset": "", "no such directory": missing} {
		for _, require := range []string{"", "0"} {
			out, code := runFrpProbe(t, dir, require)
			if code != 0 || !strings.Contains(out, "--- SKIP: TestFrpBinariesProbe") {
				t.Errorf("%s, require=%q: exit %d, want a skip with exit 0:\n%s", name, require, code, out)
			}
			if !strings.Contains(out, "hack/fetch-frp.sh") || !strings.Contains(out, envRequireFrp+"=1") {
				t.Errorf("%s, require=%q: the skip message does not say how to install or require:\n%s", name, require, out)
			}
		}
	}
}

func TestFrpBinariesFailsWhenAbsentAndRequired(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	for name, dir := range map[string]string{"unset": "", "no such directory": missing} {
		out, code := runFrpProbe(t, dir, "1")
		if code != 1 || !strings.Contains(out, "--- FAIL: TestFrpBinariesProbe") || strings.Contains(out, "--- SKIP") {
			t.Errorf("%s, require=1: exit %d, want a failure with exit 1 and no skip:\n%s", name, code, out)
		}
		if !strings.Contains(out, envRequireFrp+"=1 forbids skipping") {
			t.Errorf("%s, require=1: the failure does not say why:\n%s", name, out)
		}
	}
}

func TestFrpBinariesRefusesAMistypedRequireFlag(t *testing.T) {
	// "true" must not read as off: with the binaries missing it would skip and a CI run would pass.
	out, code := runFrpProbe(t, filepath.Join(t.TempDir(), "nope"), "true")
	if code != 1 || !strings.Contains(out, "must be 1 or unset") || strings.Contains(out, "--- SKIP") {
		t.Errorf("exit %d, want a failure that names the bad value:\n%s", code, out)
	}
}

func TestFrpBinariesFailsOnABrokenInstallEvenWhenNotRequired(t *testing.T) {
	// A binary that is there but wrong must not turn into a skip.
	out, code := runFrpProbe(t, fakeFrpDir(t, "0.70.0"), "")
	if code != 1 || !strings.Contains(out, `printed "0.70.0"`) || strings.Contains(out, "--- SKIP") {
		t.Errorf("wrong version, require unset: exit %d, want a failure:\n%s", code, out)
	}
}

func TestFrpBinariesPassesWithTheRightVersion(t *testing.T) {
	dir := fakeFrpDir(t, "0.71.1")
	for _, require := range []string{"", "1"} {
		out, code := runFrpProbe(t, dir, require)
		want := "probe got frps=" + filepath.Join(dir, "frps") + " frpc=" + filepath.Join(dir, "frpc")
		if code != 0 || !strings.Contains(out, want) || strings.Contains(out, "--- SKIP") {
			t.Errorf("require=%q: exit %d, want a pass that logs %q:\n%s", require, code, want, out)
		}
	}
}

// hack/fetch-frp.sh must refuse a tarball with the wrong hash before extracting anything, exit 1,
// and leave no file behind (the directory itself stays: a run never removes it, since a concurrent
// run may be using it). FRP_TARBALL makes the test offline. "Before extracting" is checked with a
// tar on PATH that records every call; the corrupt tarball is a valid archive, so a script that
// extracted first would call it.
func TestFetchFrpRejectsACorruptTarball(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("hack/fetch-frp.sh pins linux_amd64 only")
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	// A valid gzip stream holding a tar with fake frps and frpc: it would extract fine, so only
	// the hash stands between it and an install.
	tgz := filepath.Join(t.TempDir(), "frp.tar.gz")
	build := exec.Command("bash", "-c", `
set -e
d=$(mktemp -d)
mkdir "$d/frp_0.71.1_linux_amd64"
printf '#!/bin/sh\necho 0.71.1\n' > "$d/frp_0.71.1_linux_amd64/frps"
cp "$d/frp_0.71.1_linux_amd64/frps" "$d/frp_0.71.1_linux_amd64/frpc"
tar -czf "$1" -C "$d" frp_0.71.1_linux_amd64
rm -rf "$d"
`, "bash", tgz)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the fake tarball: %v\n%s", err, out)
	}
	before, err := os.ReadFile(tgz)
	if err != nil {
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
	env := append(os.Environ(), "FRP_TARBALL="+tgz, "PATH="+wrapDir+":"+os.Getenv("PATH"))
	tarCalls := func() int {
		b, err := os.ReadFile(tarLog)
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return strings.Count(string(b), "\n")
	}
	// The control: a shell with the script's environment finds the wrapper first and it records the
	// call. Without this, "tar was never called" would also hold if the wrapper were never reached.
	ctl := exec.Command("bash", "-c", "tar --version")
	ctl.Env = env
	if out, err := ctl.CombinedOutput(); err != nil || tarCalls() != 1 {
		t.Fatalf("the tar wrapper is not reached through PATH: err=%v, %d recorded calls\n%s", err, tarCalls(), out)
	}
	if err := os.Remove(tarLog); err != nil {
		t.Fatal(err)
	}

	run := func(dest string) (string, int) {
		cmd := exec.Command("bash", filepath.Join(root, "hack", "fetch-frp.sh"), dest)
		cmd.Env = env
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

	t.Run("directory it creates", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "frp")
		out, code := run(dest)
		if code != 1 || !strings.Contains(out, "checksum mismatch") {
			t.Fatalf("exit %d, want 1 with a checksum mismatch:\n%s", code, out)
		}
		ents, err := os.ReadDir(dest)
		if err != nil || len(ents) != 0 {
			t.Errorf("%s holds %v (%v), want an empty directory", dest, ents, err)
		}
	})
	t.Run("existing directory with a file in it", func(t *testing.T) {
		dest := t.TempDir()
		if err := os.WriteFile(filepath.Join(dest, "other"), []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
		out, code := run(dest)
		if code != 1 || !strings.Contains(out, "checksum mismatch") {
			t.Fatalf("exit %d, want 1 with a checksum mismatch:\n%s", code, out)
		}
		ents, err := os.ReadDir(dest)
		if err != nil || len(ents) != 1 || ents[0].Name() != "other" {
			t.Errorf("the directory holds %v (%v), want only the file that was there", ents, err)
		}
	})
	if n := tarCalls(); n != 0 {
		t.Errorf("tar was called %d time(s): the script extracted a tarball whose hash it had not verified", n)
	}
	after, err := os.ReadFile(tgz)
	if err != nil || !bytes.Equal(before, after) {
		t.Errorf("the caller's FRP_TARBALL was changed or removed (%v)", err)
	}
}

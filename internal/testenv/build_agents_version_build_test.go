package testenv

// The real builds of hack/build-agents.sh with and without --version (issue #469): the agent reports
// the version it was given, the same version gives the same bytes from another path, and a build
// without one is, byte for byte, the build the script made before the option existed.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A version reaches the agent: the same version from the working tree and from a copy of the sources at
// another path, with no .git and a caller that asks for its own -ldflags, gives the same bytes for each
// cpuArch, so a release's agentDigest is reproducible per version. Those bytes differ from the build
// without a version (the version is in them), and on an x86_64 host the agent says it: -version
// prints the version where the build without one prints dev.
func TestBuildAgentsWithAVersionIsReproducibleAndReportsIt(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	copyRoot := sourceCopy(t)
	const version = "v0.1.0-alpha.1"
	plain, a, b := t.TempDir(), t.TempDir(), t.TempDir()
	for _, run := range []struct {
		root string
		args []string
		env  []string
	}{
		{root, []string{plain}, nil},
		{root, []string{"--version", version, a}, nil},
		{copyRoot, []string{"--version", version, b}, []string{"GOFLAGS=-ldflags=-X=main.version=from-the-caller", "GOAMD64=v3"}},
	} {
		if out, code := runBuildAgents(t, run.root, run.args, run.env...); code != 0 {
			t.Fatalf("hack/build-agents.sh %v from %s: exit %d:\n%s", run.args, run.root, code, out)
		}
	}
	for _, arch := range []string{"x86_64", "aarch64"} {
		name := "hb-agent-" + arch
		sa, sb, sp := fileSum(t, filepath.Join(a, name)), fileSum(t, filepath.Join(b, name)), fileSum(t, filepath.Join(plain, name))
		if sa != sb {
			t.Errorf("%s with version %s is %s built in the working tree and %s built from a copy at another path", name, version, sa, sb)
		}
		if sa == sp {
			t.Errorf("%s with version %s is the same bytes as without one (%s): the version is not in the build", name, version, sa)
		}
		requireBuilt(t, b, arch)
	}
	if runtime.GOARCH != "amd64" {
		t.Logf("not running hb-agent-x86_64 -version on %s", runtime.GOARCH)
		return
	}
	for dir, want := range map[string]string{a: "hb-agent " + version + ", protocol ", b: "hb-agent " + version + ", protocol ", plain: "hb-agent dev, protocol "} {
		got, err := exec.Command(filepath.Join(dir, "hb-agent-x86_64"), "-version").CombinedOutput()
		if err != nil || !strings.HasPrefix(string(got), want) {
			t.Errorf("%s -version: %q, %v; want a line that begins %q", filepath.Join(dir, "hb-agent-x86_64"), got, err, want)
		}
	}
}

// Without --version the build is the one the script made before the option existed: the bytes of the
// go build that script ran, with its environment and flags written out here, never read from the
// script. Any linker flag the option added to a build without it (an empty -X, say) would change them.
// A literal digest would move with every change to cmd/hb-agent and what it imports, so the pin is the
// command, not a sum.
func TestBuildAgentsWithoutAVersionIsTheBuildOfBefore(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if out, code := runBuildAgents(t, root, []string{dir}); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	// What the script read from the go command before it set GOENV=off, read the same way here.
	saved := map[string]string{}
	for _, v := range []string{"GOPATH", "GOMODCACHE", "GOCACHE", "GOPROXY"} {
		cmd := exec.Command("go", "env", v)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=go"+goModVersion(t))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go env %s: %v", v, err)
		}
		saved[v] = strings.TrimSpace(string(out))
	}
	ref := t.TempDir()
	for arch, want := range agentWant {
		ctx, cancel := context.WithTimeout(context.Background(), buildAgentsTimeout)
		// hack/build-agents.sh as it was before #469: its environment and its one go build line.
		cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-o", filepath.Join(ref, "hb-agent-"+arch), "./cmd/hb-agent")
		cmd.Dir = root
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
			"GOPATH=" + saved["GOPATH"], "GOMODCACHE=" + saved["GOMODCACHE"], "GOCACHE=" + saved["GOCACHE"], "GOPROXY=" + saved["GOPROXY"],
			"GOFLAGS=-mod=readonly", "GOTOOLCHAIN=go" + goModVersion(t), "GOENV=off", "GOEXPERIMENT=", "GOFIPS140=off",
			"CGO_ENABLED=0", "GOOS=linux", "GOARCH=" + want.goarch, "GOAMD64=v1", "GOARM64=v8.0", "GOWORK=off",
		}
		// The script passes these through from its caller, and none of them changes the bytes.
		for _, v := range []string{"TMPDIR", "GOTMPDIR", "GOROOT"} {
			if val, ok := os.LookupEnv(v); ok {
				cmd.Env = append(cmd.Env, v+"="+val)
			}
		}
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("the reference build for %s: %v\n%s", arch, err, out)
		}
		name := "hb-agent-" + arch
		if got, before := fileSum(t, filepath.Join(dir, name)), fileSum(t, filepath.Join(ref, name)); got != before {
			t.Errorf("%s without --version is %s, and the build of before is %s", name, got, before)
		}
	}
}

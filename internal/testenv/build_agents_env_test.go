package testenv

// What hack/build-agents.sh gives the go command, and what it does around it, tested with a go on PATH
// that logs each call and fakes the work: no compilation, no download, so every case runs in
// milliseconds and cannot depend on the machine's Go. The bytes that come out of the real go command
// are the subject of build_agents_test.go.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goShim is a go command that logs its calls to $SHIM_LOG. `go version` announces a download on stderr
// first, as the go command does when it fetches the toolchain it was asked for, and then prints the
// version of $GOTOOLCHAIN; `go env V...` prints shim-V for each; `go build ... -o OUT ...` writes
// the file OUT; every other call fails.
const goShim = `#!/bin/sh
case "$1" in
version)
  echo "go: downloading $GOTOOLCHAIN (linux/amd64)" >&2
  echo "go version $GOTOOLCHAIN linux/amd64"
  ;;
env)
  shift
  for v in "$@"; do echo "shim-$v"; done
  ;;
build)
  echo "build|GOFLAGS=$GOFLAGS|GOTOOLCHAIN=$GOTOOLCHAIN|GOENV=$GOENV|GOFIPS140=$GOFIPS140|GOEXPERIMENT=[$GOEXPERIMENT]|GOMODCACHE=$GOMODCACHE|GOCACHE=$GOCACHE|GOPATH=$GOPATH|GOPROXY=$GOPROXY|CGO_ENABLED=$CGO_ENABLED|GOARCH=$GOARCH|GOAMD64=$GOAMD64|GOWORK=$GOWORK|ARGS=$*" >> "$SHIM_LOG"
  out=
  prev=
  for a in "$@"; do
    [ "$prev" = -o ] && out=$a
    prev=$a
  done
  printf 'a fake build\n' > "$out"
  ;;
*)
  echo "unexpected go call: $*" >&2
  exit 1
  ;;
esac
`

// shimRoot is a repository root that holds only the two scripts and a go.mod with the given text,
// and the PATH and log entries that put the go shim in front.
func shimRoot(t *testing.T, goMod string) (root string, env []string, log string) {
	t.Helper()
	real, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	root = t.TempDir()
	for _, f := range []string{"hack/build-agents.sh", "hack/cpu-arch.sh"} {
		copyFile(t, filepath.Join(real, f), filepath.Join(root, f))
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(goShim), 0o755); err != nil {
		t.Fatal(err)
	}
	log = filepath.Join(t.TempDir(), "calls")
	return root, []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "SHIM_LOG=" + log}, log
}

// shimBuilds reads the build calls the shim logged as field maps (the ARGS field last, as it is).
func shimBuilds(t *testing.T, log string) []map[string]string {
	t.Helper()
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("the shim logged nothing: %v", err)
	}
	var out []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		call := map[string]string{}
		rest := line
		for rest != "" {
			if strings.HasPrefix(rest, "ARGS=") {
				call["ARGS"] = strings.TrimPrefix(rest, "ARGS=")
				break
			}
			field, tail, _ := strings.Cut(rest, "|")
			k, v, _ := strings.Cut(field, "=")
			call[k], rest = v, tail
		}
		out = append(out, call)
	}
	return out
}

const goModOK = "module example.test/m\n\ngo 1.26.0\n"

// The toolchain is read from the go line the way Go reads it: a comment, tabs, a patchless version, a
// release candidate and a go in a comment above the line all work, and the script and the test's own
// parser agree on each. A first run that downloads the toolchain succeeds: go says so on stderr, and
// only its stdout is the version.
func TestBuildAgentsReadsTheGoLineAsGoDoes(t *testing.T) {
	requireGNU(t)
	for _, c := range []struct{ name, goMod, want string }{
		{"plain", goModOK, "go1.26.0"},
		{"a comment", "module x\n\ngo 1.26.0 // pinned for CI\n", "go1.26.0"},
		{"tabs", "module x\n\ngo\t1.26.0\t// tab\n", "go1.26.0"},
		{"a comment with no space before it", "module x\n\ngo 1.26.0// pinned\n", "go1.26.0"},
		{"no patch number", "module x\n\ngo 1.26\n", "go1.26.0"},
		{"a release candidate", "module x\n\ngo 1.27rc1\n", "go1.27rc1"},
		{"go in a comment first", "module x\n// go 1.20 is the old one\ngo 1.26.0\ntoolchain go1.99.9\n", "go1.26.0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root, env, log := shimRoot(t, c.goMod)
			if v, ok := goDirective(c.goMod); !ok || "go"+v != c.want {
				t.Errorf("the test's parser reads %q (%v), want %q", v, ok, c.want)
			}
			out, code := runBuildAgents(t, root, []string{t.TempDir(), "x86_64"}, env...)
			if code != 0 {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			if got := shimBuilds(t, log)[0]["GOTOOLCHAIN"]; got != c.want {
				t.Errorf("the go command got GOTOOLCHAIN=%q, want %q", got, c.want)
			}
			// The download notice reached the user and did not fail the run.
			if !strings.Contains(out, "go: downloading "+c.want) || !strings.Contains(out, "built    hb-agent-x86_64") {
				t.Errorf("output:\n%s", out)
			}
		})
	}
	for name, goMod := range map[string]string{
		"no go line":        "module x\n",
		"a go in a comment": "module x\n// go 1.26.0\n",
		"a version that is": "module x\n\ngo latest\n",
	} {
		t.Run(name, func(t *testing.T) {
			root, env, log := shimRoot(t, goMod)
			if _, ok := goDirective(goMod); ok {
				t.Errorf("the test's parser reads a version from %q", goMod)
			}
			dir := t.TempDir()
			out, code := runBuildAgents(t, root, []string{dir}, env...)
			if code != 1 || !strings.Contains(out, "no toolchain to pin") || len(dirNames(t, dir)) != 0 {
				t.Errorf("exit %d, dir %v:\n%s", code, dirNames(t, dir), out)
			}
			if _, err := os.Stat(log); err == nil {
				t.Error("the go command was called")
			}
		})
	}
}

// Nothing the caller or the machine has saved reaches the build: the go command is given GOENV=off (its
// saved configuration is ignored), an empty GOEXPERIMENT, GOFIPS140=off, the pinned toolchain, the
// script's own GOFLAGS, and GOPATH, GOMODCACHE, GOCACHE and GOPROXY as the go command itself reported
// them before it stopped reading its configuration, so that a toolchain downloaded earlier is still found.
// The caller's GOFLAGS lose everything but -p, and the script says what it dropped.
func TestBuildAgentsGivesTheGoCommandAFixedEnvironment(t *testing.T) {
	requireGNU(t)
	root, env, log := shimRoot(t, goModOK)
	out, code := runBuildAgents(t, root, []string{t.TempDir()}, append(env,
		"GOFLAGS=-ldflags=-buildid=x -p=3 -tags=foo -gcflags=all=-N -overlay=/tmp/o.json", "GOEXPERIMENT=arenas", "GOFIPS140=latest",
		"GOENV=/tmp/a-saved-config", "GOAMD64=v3", "GOWORK=/tmp/a.work", "GOTOOLCHAIN=local", "CGO_ENABLED=1")...)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	builds := shimBuilds(t, log)
	if len(builds) != 2 {
		t.Fatalf("%d go build calls, want one for each cpuArch", len(builds))
	}
	for _, b := range builds {
		for k, want := range map[string]string{
			"GOFLAGS": "-mod=readonly -p=3", "GOTOOLCHAIN": "go1.26.0", "GOENV": "off", "GOFIPS140": "off", "GOEXPERIMENT": "[]",
			"GOMODCACHE": "shim-GOMODCACHE", "GOCACHE": "shim-GOCACHE", "GOPATH": "shim-GOPATH", "GOPROXY": "shim-GOPROXY",
			"CGO_ENABLED": "0", "GOAMD64": "v1", "GOWORK": "off",
		} {
			if got := b[k]; got != want {
				t.Errorf("go build got %s=%q, want %q", k, got, want)
			}
		}
		if !strings.Contains(b["ARGS"], "-trimpath") || !strings.Contains(b["ARGS"], "-buildvcs=false") || !strings.HasSuffix(b["ARGS"], "./cmd/hb-agent") {
			t.Errorf("go build ran with %q", b["ARGS"])
		}
	}
	if !strings.Contains(out, "ignoring GOFLAGS entries that can change the bytes (only -p=N is kept): -ldflags=-buildid=x -tags=foo -gcflags=all=-N -overlay=/tmp/o.json") {
		t.Errorf("the script does not say which GOFLAGS it dropped:\n%s", out)
	}

	// Without -p in the caller's GOFLAGS there is none, and nothing is said when nothing was dropped.
	root, env, log = shimRoot(t, goModOK)
	out, code = runBuildAgents(t, root, []string{t.TempDir(), "aarch64"}, append(env, "GOFLAGS=")...)
	if code != 0 || shimBuilds(t, log)[0]["GOFLAGS"] != "-mod=readonly" || strings.Contains(out, "ignoring GOFLAGS") {
		t.Errorf("exit %d, build %v:\n%s", code, shimBuilds(t, log)[0], out)
	}
}

// DIR may be relative to the working directory the script is run in, which is not the repository: the
// build lands in it, and the go command is given an absolute -o.
func TestBuildAgentsResolvesARelativeDirAgainstTheWorkingDirectory(t *testing.T) {
	requireGNU(t)
	root, env, log := shimRoot(t, goModOK)
	wd := t.TempDir()
	out, code := runBuildAgentsIn(t, root, wd, []string{"out/agent-builds", "aarch64"}, env...)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	placed := filepath.Join(wd, "out", "agent-builds", "hb-agent-aarch64")
	if b, err := os.ReadFile(placed); err != nil || string(b) != "a fake build\n" {
		t.Fatalf("%s: %q, %v\n%s", placed, b, err, out)
	}
	if got := dirNames(t, filepath.Join(wd, "out", "agent-builds")); len(got) != 1 {
		t.Errorf("the directory holds %v, want only the build", got)
	}
	if _, err := os.Stat(filepath.Join(root, "out")); err == nil {
		t.Error("a directory was made under the repository root")
	}
	o := shimBuilds(t, log)[0]["ARGS"]
	if !strings.Contains(o, " -o "+filepath.Join(wd, "out", "agent-builds")+"/.build.") {
		t.Errorf("go build was given a relative or foreign -o: %q", o)
	}
}

// A build already in place with the same bytes is kept, and its mode is set to 755 if it was anything else.
func TestBuildAgentsRepairsTheModeOfAnIdenticalBuild(t *testing.T) {
	requireGNU(t)
	root, env, _ := shimRoot(t, goModOK)
	dir := t.TempDir()
	if out, code := runBuildAgents(t, root, []string{dir, "x86_64"}, env...); code != 0 {
		t.Fatalf("first run, exit %d:\n%s", code, out)
	}
	path := filepath.Join(dir, "hb-agent-x86_64")
	for _, mode := range []os.FileMode{0o700, 0o777, 0o644, 0o755} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		out, code := runBuildAgents(t, root, []string{dir, "x86_64"}, env...)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if code != 0 || fi.Mode().Perm() != 0o755 || !strings.Contains(out, "ok       hb-agent-x86_64") {
			t.Errorf("mode %v: exit %d, now %v:\n%s", mode, code, fi.Mode().Perm(), out)
		}
		if repaired := strings.Contains(out, "mode set to 755"); repaired != (mode != 0o755) {
			t.Errorf("mode %v: the output says repaired=%v:\n%s", mode, repaired, out)
		}
	}
}

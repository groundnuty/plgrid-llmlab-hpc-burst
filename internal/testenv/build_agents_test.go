package testenv

// hack/build-agents.sh DIR cross-compiles cmd/hb-agent for each cpuArch and places it as
// DIR/hb-agent-<cpuArch> (agentapi.AgentBuildName), where the Provider's controller finds it. The
// tests build the real command, offline (GOPROXY=off), and read the results the way a node would
// meet them: the ELF header and program headers of each file, and the build settings Go embeds. A
// file that merely has the right name proves nothing, and neither does building twice from one
// tree: the second build below is from a copy of the sources at another path, with no .git, which
// is where -trimpath and -buildvcs=false decide the bytes. The expected machine of each cpuArch is
// written out in the test, not read from the script's table.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// buildAgentsTimeout bounds one run of the script: a cold build cache compiles the standard library
// for both architectures.
const buildAgentsTimeout = 8 * time.Minute

// agentWant is what a node needs to find in each build: the machine its cpuArch names, written out.
var agentWant = map[string]struct {
	machine elf.Machine
	goarch  string
}{
	"x86_64":  {elf.EM_X86_64, "amd64"},
	"aarch64": {elf.EM_AARCH64, "arm64"},
}

// runBuildAgents runs the copy of the script under root with the given arguments and returns its
// combined output and exit code. Extra environment entries come last and win.
func runBuildAgents(t *testing.T, root string, args []string, env ...string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), buildAgentsTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", append([]string{filepath.Join(root, "hack", "build-agents.sh")}, args...)...)
	cmd.Env = append(os.Environ(), append([]string{"GOPROXY=off"}, env...)...)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	if err != nil {
		t.Fatalf("running hack/build-agents.sh: %v", err)
	}
	return string(out), 0
}

// requireGNU skips on a machine the script does not run on (it needs bash 4, GNU coreutils and a
// linux Go toolchain's build).
func requireGNU(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("hack/build-agents.sh needs a GNU userland")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Fatalf("hack/build-agents.sh needs the go command: %v", err)
	}
}

func fileSum(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// copyFile copies src to dst with src's mode.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fi.Mode().Perm())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// sourceCopy copies what the script builds from into a new directory, which has another path and
// no .git: go.mod, go.sum, the two scripts, and every file but the tests of each package of this
// module that cmd/hb-agent imports for any of the architectures, with the files they embed. It
// is a copy of the working tree, not of a commit, so uncommitted edits are in it.
func sourceCopy(t *testing.T) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	// go list prints the directories it resolved, which differ from root when a parent of the
	// checkout is a symlink.
	if root, err = filepath.EvalSymlinks(root); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	for _, f := range []string{"go.mod", "go.sum", "hack/build-agents.sh", "hack/cpu-arch.sh"} {
		copyFile(t, filepath.Join(root, f), filepath.Join(dst, f))
	}
	dirs := map[string]bool{}
	embeds := map[string]bool{}
	for _, want := range agentWant {
		cmd := exec.Command("go", "list", "-deps", "-f",
			`{{if not .Standard}}D {{.Dir}}{{"\n"}}{{range .EmbedFiles}}E {{$.Dir}}/{{.}}{{"\n"}}{{end}}{{end}}`, "./cmd/hb-agent")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOPROXY=off", "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+want.goarch)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list for %s: %v", want.goarch, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			kind, path, _ := strings.Cut(line, " ")
			if path, err = filepath.EvalSymlinks(path); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(path, root+string(filepath.Separator)) {
				continue // a dependency outside this module is read from the module cache
			}
			if kind == "D" {
				dirs[path] = true
			} else {
				embeds[path] = true
			}
		}
	}
	if len(dirs) == 0 {
		t.Fatal("go list found no package of this module under cmd/hb-agent")
	}
	for dir := range dirs {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			copyFile(t, filepath.Join(dir, e.Name()), filepath.Join(dst, rel))
		}
	}
	for f := range embeds {
		rel, err := filepath.Rel(root, f)
		if err != nil {
			t.Fatal(err)
		}
		copyFile(t, f, filepath.Join(dst, rel))
	}
	return dst
}

// requireBuilt checks one placed build the way a node meets it.
func requireBuilt(t *testing.T, dir, cpuArch string) {
	t.Helper()
	want, ok := agentWant[cpuArch]
	if !ok {
		t.Fatalf("test has no expectation for %s", cpuArch)
	}
	path := filepath.Join(dir, "hb-agent-"+cpuArch)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Errorf("%s has mode %v, want 755", path, fi.Mode().Perm())
	}
	f, err := elf.Open(path)
	if err != nil {
		t.Fatalf("%s is not an ELF file: %v", path, err)
	}
	defer f.Close()
	if f.Machine != want.machine {
		t.Errorf("%s has machine %v, want %v", path, f.Machine, want.machine)
	}
	if f.Class != elf.ELFCLASS64 || f.Data != elf.ELFDATA2LSB || f.Type != elf.ET_EXEC {
		t.Errorf("%s is %v %v %v, want a little-endian 64-bit executable", path, f.Class, f.Data, f.Type)
	}
	// Static: a dynamically linked binary names an interpreter, and would not start on a node that
	// lacks the library it asks for.
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP || p.Type == elf.PT_DYNAMIC {
			t.Errorf("%s has a %v program header: it is dynamically linked", path, p.Type)
		}
	}
	bi, err := buildinfo.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, s := range bi.Settings {
		settings[s.Key] = s.Value
	}
	for k, v := range map[string]string{"GOOS": "linux", "GOARCH": want.goarch, "CGO_ENABLED": "0", "-trimpath": "true"} {
		if settings[k] != v {
			t.Errorf("%s was built with %s=%q, want %q", path, k, settings[k], v)
		}
	}
	for k := range settings {
		if strings.HasPrefix(k, "vcs") {
			t.Errorf("%s carries the build setting %s, so its bytes follow the git state", path, k)
		}
	}
	if !strings.HasSuffix(bi.Path, "/cmd/hb-agent") {
		t.Errorf("%s is the command %q, want cmd/hb-agent", path, bi.Path)
	}
}

// Both cpuArchs are built, each for its own machine and static, and nothing else is left in the
// directory (not the stage). The output names each file's sha256, which the test computes itself
// from the bytes. On an x86_64 host the x86_64 build also runs: -version prints its version and the
// protocol.
func TestBuildAgentsBuildsEachCPUArchForItsMachine(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "agent-builds") // the script makes it
	out, code := runBuildAgents(t, root, []string{dir})
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if got := dirNames(t, dir); strings.Join(got, " ") != "hb-agent-aarch64 hb-agent-x86_64" {
		t.Errorf("%s holds %v, want exactly hb-agent-aarch64 and hb-agent-x86_64", dir, got)
	}
	for _, arch := range []string{"x86_64", "aarch64"} {
		requireBuilt(t, dir, arch)
		line := fmt.Sprintf("hb-agent-%s (%s) sha256 %s", arch, filepath.Join(dir, "hb-agent-"+arch), fileSum(t, filepath.Join(dir, "hb-agent-"+arch)))
		if !strings.Contains(out, line) {
			t.Errorf("output lacks %q:\n%s", line, out)
		}
	}
	if !strings.Contains(out, "go version go") {
		t.Errorf("output does not name the Go toolchain:\n%s", out)
	}
	if runtime.GOARCH == "amd64" {
		got, err := exec.Command(filepath.Join(dir, "hb-agent-x86_64"), "-version").CombinedOutput()
		if err != nil || !strings.HasPrefix(string(got), "hb-agent dev, protocol ") {
			t.Errorf("hb-agent-x86_64 -version: %q, %v", got, err)
		}
	}
}

// The bytes do not depend on where the sources are or on the git state: the build in the working
// tree (which has a .git) and the build from a copy at another path (which has none) are the same
// file for each cpuArch, so the deployment's agentDigest is stable. A second run into the same
// directory finds the build in place and keeps it. The builds could not be the same without
// -trimpath (the paths differ), -buildvcs=false (one tree has a .git) or the pinned architecture
// levels (the copy's caller asks for others).
func TestBuildAgentsIsReproducibleFromAnotherPath(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	copyRoot := sourceCopy(t)
	if copyRoot == root {
		t.Fatal("the copy is the working tree")
	}
	if _, err := os.Stat(filepath.Join(copyRoot, ".git")); err == nil {
		t.Fatal("the copy has a .git")
	}
	a, b := t.TempDir(), t.TempDir()
	// The copy is built by a caller whose environment asks for other architecture levels, which the
	// script ignores (it pins Go's defaults), and for a go.work that names nothing.
	for _, run := range []struct {
		root, dir string
		env       []string
	}{{root, a, nil}, {copyRoot, b, []string{"GOAMD64=v3", "GOARM64=v8.2", "GOWORK=" + filepath.Join(copyRoot, "no-such.work")}}} {
		if out, code := runBuildAgents(t, run.root, []string{run.dir}, run.env...); code != 0 {
			t.Fatalf("building from %s: exit %d:\n%s", run.root, code, out)
		}
	}
	for _, arch := range []string{"x86_64", "aarch64"} {
		name := "hb-agent-" + arch
		if sa, sb := fileSum(t, filepath.Join(a, name)), fileSum(t, filepath.Join(b, name)); sa != sb {
			t.Errorf("%s is %s built in the working tree and %s built from a copy at another path", name, sa, sb)
		}
		requireBuilt(t, b, arch)
	}

	before, err := os.Stat(filepath.Join(a, "hb-agent-aarch64"))
	if err != nil {
		t.Fatal(err)
	}
	out, code := runBuildAgents(t, root, []string{a})
	if code != 0 {
		t.Fatalf("second run: exit %d:\n%s", code, out)
	}
	if !strings.Contains(out, "ok       hb-agent-aarch64") || strings.Contains(out, "built ") {
		t.Errorf("the second run did not keep the builds in place:\n%s", out)
	}
	after, err := os.Stat(filepath.Join(a, "hb-agent-aarch64"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Errorf("the second run replaced hb-agent-aarch64 with an identical file")
	}
}

// Naming cpuArchs builds only those. A stale file at a name is replaced by the build, another file
// in the directory is kept, and the stage never stays.
func TestBuildAgentsBuildsOnlyTheNamedCPUArchAndReplacesAStaleBuild(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, body := range map[string]string{"other": "keep", "hb-agent-aarch64": "a stale agent build\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out, code := runBuildAgents(t, root, []string{dir, "aarch64"})
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if got := dirNames(t, dir); strings.Join(got, " ") != "hb-agent-aarch64 other" {
		t.Errorf("%s holds %v, want hb-agent-aarch64 and other only", dir, got)
	}
	requireBuilt(t, dir, "aarch64")
	if b, _ := os.ReadFile(filepath.Join(dir, "other")); !bytes.Equal(b, []byte("keep")) {
		t.Errorf("the other file now holds %q", b)
	}
	if !strings.Contains(out, "built    hb-agent-aarch64") {
		t.Errorf("output does not say the stale build was replaced:\n%s", out)
	}
}

// A cpuArch the table does not know stops the run before anything is built, even when a known one
// comes first, and the message names it and the ones that exist.
func TestBuildAgentsRefusesAnUnknownCPUArchBeforeBuildingAnything(t *testing.T) {
	requireGNU(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"amd64", "arm64", "riscv64", "X86_64", ""} {
		dir := t.TempDir()
		out, code := runBuildAgents(t, root, []string{dir, "x86_64", bad})
		if code != 1 {
			t.Errorf("cpuArch %q: exit %d, want 1:\n%s", bad, code, out)
		}
		if bad != "" && !strings.Contains(out, "cpuArch "+bad+" is not one of: x86_64 aarch64") {
			t.Errorf("cpuArch %q: the message does not name it and the known ones:\n%s", bad, out)
		}
		if got := dirNames(t, dir); len(got) != 0 {
			t.Errorf("cpuArch %q: %s holds %v after a refusal, want nothing", bad, dir, got)
		}
	}
	if out, code := runBuildAgents(t, root, nil); code != 1 || !strings.Contains(out, "usage: hack/build-agents.sh DIR") {
		t.Errorf("no DIR: exit %d, want 1 and a usage line:\n%s", code, out)
	}
}

// A build that fails leaves the file at its name as it was and no stage behind, and the run exits 1.
// The sources are a copy in which cmd/hb-agent does not compile.
func TestBuildAgentsKeepsTheStaleBuildWhenTheCompilationFails(t *testing.T) {
	requireGNU(t)
	copyRoot := sourceCopy(t)
	if err := os.WriteFile(filepath.Join(copyRoot, "cmd", "hb-agent", "broken.go"), []byte("package main\n\nvar _ = notDefined\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	stale := filepath.Join(dir, "hb-agent-x86_64")
	if err := os.WriteFile(stale, []byte("the last good build\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	out, code := runBuildAgents(t, copyRoot, []string{dir})
	if code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	if !strings.Contains(out, "building cmd/hb-agent for x86_64 failed") || !strings.Contains(out, "notDefined") {
		t.Errorf("the output does not say which build failed and why:\n%s", out)
	}
	if b, _ := os.ReadFile(stale); string(b) != "the last good build\n" {
		t.Errorf("the stale build now holds %q", b)
	}
	if got := dirNames(t, dir); strings.Join(got, " ") != "hb-agent-x86_64" {
		t.Errorf("%s holds %v, want only the stale build", dir, got)
	}
}

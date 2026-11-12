package release

// The scripts the release workflow calls and the one the rehearsal uses (issue #401, PR B), run for real:
//
//   - hack/release-check.sh, against git repositories made here: a tag on origin/main passes, and each
//     way a tag cannot be released fails;
//   - hack/release-archive.sh, against a small checkout made here, with the real scripts beside it and
//     tiny programs in place of the four commands, so that the test builds in seconds. It checks the
//     layout and the checksums against literals, what `go version -m` shows of the host binaries, that a
//     second build is the same bytes, and that every refusal refuses, each for its own cause, before
//     anything is written to OUT;
//   - hack/release-unpack.sh and hack/rehearsal.sh's archive input, on archives written here and on the
//     one the build above made.
//
// Expected values are literals, or are computed by Go's crypto and archive packages, never by the scripts.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const fixtureTag = "v0.1.0-alpha.1"

var commitTime = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

// sharedDir holds the one archive that several tests read.
var sharedDir string

func TestMain(m *testing.M) {
	var err error
	if sharedDir, err = os.MkdirTemp("", "release-tests-"); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(sharedDir)
	os.Exit(code)
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// run runs a command in dir and returns its combined output and exit code.
func run(t *testing.T, dir string, env []string, name string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("%s %v: %v", name, args, err)
	return "", -1
}

// gitEnv makes git ignore the machine's configuration and use the fixed times of the fixtures.
func gitEnv() []string {
	d := commitTime.Format(time.RFC3339)
	return []string{
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.invalid", "GIT_AUTHOR_DATE=" + d,
		"GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.invalid", "GIT_COMMITTER_DATE=" + d,
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, code := run(t, dir, gitEnv(), "git", args...)
	if code != 0 {
		t.Fatalf("git %v in %s: exit %d: %s", args, dir, code, out)
	}
	return strings.TrimSpace(out)
}

func write(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func copyScript(t *testing.T, from, to string, names ...string) {
	t.Helper()
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(from, "hack", n))
		if err != nil {
			t.Fatal(err)
		}
		write(t, filepath.Join(to, "hack", n), string(b), 0o755)
	}
}

// ---- hack/release-check.sh ------------------------------------------------------------------------

// checkRepo is a checkout of a repository that has an origin, with main pushed.
type checkRepo struct {
	origin, dir string
}

func newCheckRepo(t *testing.T, modulePath string) *checkRepo {
	t.Helper()
	base := t.TempDir()
	r := &checkRepo{origin: filepath.Join(base, "origin.git"), dir: filepath.Join(base, "work")}
	git(t, base, "init", "-q", "--bare", "-b", "main", r.origin)
	git(t, base, "clone", "-q", r.origin, r.dir)
	git(t, r.dir, "checkout", "-q", "-b", "main")
	copyScript(t, repoRoot(t), r.dir, "release-check.sh")
	write(t, filepath.Join(r.dir, "go.mod"), "module "+modulePath+"\n\ngo 1.26.0\n", 0o644)
	git(t, r.dir, "add", "-A")
	git(t, r.dir, "commit", "-q", "-m", "first")
	git(t, r.dir, "push", "-q", "origin", "main")
	return r
}

// notes commits docs/releases/TAG.md with this text.
func (r *checkRepo) notes(t *testing.T, tag, text string) {
	t.Helper()
	write(t, filepath.Join(r.dir, "docs", "releases", tag+".md"), text, 0o644)
	git(t, r.dir, "add", "-A")
	git(t, r.dir, "commit", "-q", "-m", "notes for "+tag)
}

func (r *checkRepo) check(t *testing.T, tag string) (string, int) {
	t.Helper()
	return run(t, r.dir, gitEnv(), "bash", "hack/release-check.sh", tag)
}

// A tag on origin/main, with its notes, checked out, passes; every check says so.
func TestReleaseCheckPassesATagOnMainWithItsNotes(t *testing.T) {
	r := newCheckRepo(t, "github.com/example/module")
	r.notes(t, "v0.1.0-alpha.1", "The first alpha.\n")
	git(t, r.dir, "push", "-q", "origin", "main")
	git(t, r.dir, "tag", "v0.1.0-alpha.1")
	git(t, r.dir, "fetch", "-q", "origin")
	out, code := r.check(t, "v0.1.0-alpha.1")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	for _, want := range []string{"tag v0.1.0-alpha.1 is a version tag", "has the major version of github.com/example/module",
		"docs/releases/v0.1.0-alpha.1.md exists", "the checkout is the commit v0.1.0-alpha.1 names", "is on origin/main"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
}

// A tag pushed from a commit that main does not have releases nothing: the commit is on a branch only.
func TestReleaseCheckRefusesATagOffMain(t *testing.T) {
	r := newCheckRepo(t, "github.com/example/module")
	git(t, r.dir, "checkout", "-q", "-b", "topic")
	r.notes(t, "v0.1.0-alpha.1", "The first alpha.\n")
	git(t, r.dir, "push", "-q", "origin", "topic")
	git(t, r.dir, "tag", "v0.1.0-alpha.1")
	git(t, r.dir, "fetch", "-q", "origin")
	out, code := r.check(t, "v0.1.0-alpha.1")
	if code == 0 || !strings.Contains(out, "is not on origin/main") {
		t.Fatalf("exit %d, want a refusal naming origin/main:\n%s", code, out)
	}
}

// A tag on main passes the guard once main has moved on: the commit is an ancestor, not the tip.
func TestReleaseCheckPassesATagBehindTheTipOfMain(t *testing.T) {
	r := newCheckRepo(t, "github.com/example/module")
	r.notes(t, "v0.1.0-alpha.1", "The first alpha.\n")
	git(t, r.dir, "tag", "v0.1.0-alpha.1")
	write(t, filepath.Join(r.dir, "later.txt"), "a later commit\n", 0o644)
	git(t, r.dir, "add", "-A")
	git(t, r.dir, "commit", "-q", "-m", "later")
	git(t, r.dir, "push", "-q", "origin", "main")
	git(t, r.dir, "checkout", "-q", "v0.1.0-alpha.1")
	out, code := r.check(t, "v0.1.0-alpha.1")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
}

// Each of the other ways a tag cannot be released, with the others satisfied.
func TestReleaseCheckRefusesEachOtherCause(t *testing.T) {
	tag := "v0.1.0-alpha.1"
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, r *checkRepo) string // returns the tag to check
		want  string
	}{
		{"notes missing", func(t *testing.T, r *checkRepo) string {
			git(t, r.dir, "tag", tag)
			return tag
		}, "docs/releases/v0.1.0-alpha.1.md is missing or empty"},
		{"notes empty", func(t *testing.T, r *checkRepo) string {
			r.notes(t, tag, "")
			git(t, r.dir, "push", "-q", "origin", "main")
			git(t, r.dir, "tag", tag)
			return tag
		}, "docs/releases/v0.1.0-alpha.1.md is missing or empty"},
		{"the tag does not exist", func(t *testing.T, r *checkRepo) string {
			r.notes(t, tag, "notes\n")
			return tag
		}, "the tag v0.1.0-alpha.1 does not exist"},
		{"the checkout is not the tag's commit", func(t *testing.T, r *checkRepo) string {
			r.notes(t, tag, "notes\n")
			git(t, r.dir, "tag", tag)
			write(t, filepath.Join(r.dir, "later.txt"), "later\n", 0o644)
			git(t, r.dir, "add", "-A")
			git(t, r.dir, "commit", "-q", "-m", "later")
			git(t, r.dir, "push", "-q", "origin", "main")
			return tag
		}, "the tag v0.1.0-alpha.1 names"},
		{"no origin/main in the checkout", func(t *testing.T, r *checkRepo) string {
			r.notes(t, tag, "notes\n")
			git(t, r.dir, "tag", tag)
			git(t, r.dir, "update-ref", "-d", "refs/remotes/origin/main")
			return tag
		}, "origin/main is not in this checkout"},
		{"a major version the module path does not allow", func(t *testing.T, r *checkRepo) string {
			r.notes(t, "v2.0.0", "notes\n")
			git(t, r.dir, "push", "-q", "origin", "main")
			git(t, r.dir, "tag", "v2.0.0")
			return "v2.0.0"
		}, "has no /vN suffix, so its tags have major version 0 or 1, and v2.0.0 has 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newCheckRepo(t, "github.com/example/module")
			tagged := tc.setup(t, r)
			out, code := r.check(t, tagged)
			if code == 0 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want a refusal saying %q:\n%s", code, tc.want, out)
			}
		})
	}
}

// A module with a /v2 suffix takes major version 2 and no other.
func TestReleaseCheckTakesTheMajorVersionOfAModulePathWithASuffix(t *testing.T) {
	for _, tc := range []struct {
		tag  string
		pass bool
	}{{"v2.0.0", true}, {"v2.3.1-rc.1", true}, {"v1.0.0", false}, {"v3.0.0", false}} {
		t.Run(tc.tag, func(t *testing.T) {
			r := newCheckRepo(t, "github.com/example/module/v2")
			r.notes(t, tc.tag, "notes\n")
			git(t, r.dir, "push", "-q", "origin", "main")
			git(t, r.dir, "tag", tc.tag)
			out, code := r.check(t, tc.tag)
			if (code == 0) != tc.pass {
				t.Fatalf("exit %d, pass wanted %v:\n%s", code, tc.pass, out)
			}
			if !tc.pass && !strings.Contains(out, "takes major version 2") {
				t.Errorf("the refusal does not name major version 2:\n%s", out)
			}
		})
	}
}

// What is and is not a version tag.
func TestReleaseCheckReadsOnlyVersionTags(t *testing.T) {
	r := newCheckRepo(t, "github.com/example/module")
	for _, bad := range []string{"v1", "v1.2", "1.2.3", "v1.2.3+meta", "v01.2.3", "v1.2.3-", "v1.2.3-a..b", "v1.2.3-alpha_1", "release-1", ""} {
		out, code := r.check(t, bad)
		if code == 0 || !strings.Contains(out, "is not vMAJOR.MINOR.PATCH") && !strings.Contains(out, "usage") {
			t.Errorf("%q: exit %d, want a refusal of its form:\n%s", bad, code, out)
		}
	}
	for _, good := range []string{"v0.1.0-alpha.1", "v0.2.0-beta.1", "v1.0.0", "v1.10.20-rc.1.2"} {
		out, _ := r.check(t, good)
		if strings.Contains(out, "is not vMAJOR.MINOR.PATCH") {
			t.Errorf("%q was refused as no version tag:\n%s", good, out)
		}
		if !strings.Contains(out, "is a version tag") {
			t.Errorf("%q: the form was not accepted:\n%s", good, out)
		}
	}
}

// ---- hack/release-archive.sh ----------------------------------------------------------------------

// The files the fixture's seed holds, as the archive carries them, and what the pins files pin.
var seedFiles = map[string]string{
	"bin/k3s-1.36.5/k3s":        "a stand-in for the pinned k3s\n",
	"bin/frp-0.71.1/frps":       "a stand-in for the pinned frps\n",
	"agent-builds/frpc-x86_64":  "a stand-in for the pinned frpc, x86_64\n",
	"agent-builds/frpc-aarch64": "a stand-in for the pinned frpc, aarch64\n",
}

// The commit's own data files, as the archive carries them.
var dataFiles = map[string]string{
	"config/crd/example.yaml": "kind: CustomResourceDefinition\n",
	"profiles/example.yaml":   "kind: PlatformProfile\n",
	"profiles/README.md":      "The shipped profiles.\n",
	"examples/example.yaml":   "kind: ModelDeployment\n",
}

// archiveFiles is every file of the archive with its mode: the specified layout, written out.
var archiveFiles = map[string]int64{
	"agent-builds/frpc-aarch64":     0o755,
	"agent-builds/frpc-x86_64":      0o755,
	"agent-builds/hb-agent-aarch64": 0o755,
	"agent-builds/hb-agent-x86_64":  0o755,
	"bin/frp-0.71.1/frps":           0o755,
	"bin/hpcburst":                  0o755,
	"bin/hpcburst-controller":       0o755,
	"bin/hpcburst-front":            0o755,
	"bin/k3s-1.36.5/k3s":            0o755,
	"config/crd/example.yaml":       0o644,
	"examples/example.yaml":         0o644,
	"profiles/README.md":            0o644,
	"profiles/example.yaml":         0o644,
}

var archiveDirs = []string{"agent-builds/", "bin/", "bin/frp-0.71.1/", "bin/k3s-1.36.5/", "config/", "config/crd/", "examples/", "profiles/"}

// fixture is a small checkout that the release scripts can build from.
type fixture struct {
	dir  string // the checkout, a clone with a .git directory
	seed string // the pinned files, laid out as the archive lays them out
	out  string // where the archive goes: outside the checkout
}

func newFixture(t *testing.T) *fixture { return newFixtureIn(t, t.TempDir()) }

// newFixtureIn makes the fixture's checkout, seed and OUT under base.
func newFixtureIn(t *testing.T, base string) *fixture {
	t.Helper()
	f := &fixture{dir: filepath.Join(base, "repo"), seed: filepath.Join(base, "seed"), out: filepath.Join(base, "out")}
	goVer := strings.TrimPrefix(runtime.Version(), "go")
	write(t, filepath.Join(f.dir, "go.mod"), "module github.com/groundnuty/plgrid-llmlab-hpc-burst\n\ngo "+goVer+"\n", 0o644)
	for _, cmd := range []string{"hpcburst", "hpcburst-controller", "hpcburst-front", "hb-agent"} {
		write(t, filepath.Join(f.dir, "cmd", cmd, "main.go"), "package main\n\nfunc main() {}\n", 0o644)
	}
	for name, body := range dataFiles {
		write(t, filepath.Join(f.dir, name), body, 0o644)
	}
	copyScript(t, repoRoot(t), f.dir, "release-archive.sh", "build-agents.sh", "cpu-arch.sh", "fetch-k3s.sh", "fetch-frp.sh")
	// The pins files name the sha256 of the stand-ins, computed here.
	write(t, filepath.Join(f.dir, "internal", "launcher", "k3s-pins.txt"),
		"# fixture\nv1.36.5+k3s1  amd64  k3s  "+sum(seedFiles["bin/k3s-1.36.5/k3s"])+"\n", 0o644)
	write(t, filepath.Join(f.dir, "internal", "launcher", "frp-pins.txt"),
		"# fixture\n"+
			"0.71.1  amd64  frp_0.71.1_linux_amd64.tar.gz  "+sum("tarball amd64")+"  "+sum(seedFiles["bin/frp-0.71.1/frps"])+"  "+sum(seedFiles["agent-builds/frpc-x86_64"])+"\n"+
			"0.71.1  arm64  frp_0.71.1_linux_arm64.tar.gz  "+sum("tarball arm64")+"  "+sum("frps arm64")+"  "+sum(seedFiles["agent-builds/frpc-aarch64"])+"\n", 0o644)
	write(t, filepath.Join(f.dir, "docs", "releases", fixtureTag+".md"), "The summary of "+fixtureTag+".\n", 0o644)
	git(t, f.dir, "init", "-q", "-b", "main")
	git(t, f.dir, "add", "-A")
	git(t, f.dir, "commit", "-q", "-m", "the fixture")
	git(t, f.dir, "tag", fixtureTag)
	for name, body := range seedFiles {
		write(t, filepath.Join(f.seed, name), body, 0o755)
	}
	return f
}

// build runs the archive script with the seed and no fetching, and these options before OUT.
func (f *fixture) build(t *testing.T, opts ...string) (string, int) {
	t.Helper()
	args := append([]string{"hack/release-archive.sh"}, opts...)
	args = append(args, "--seed", f.seed, "--no-fetch", f.out)
	return run(t, f.dir, gitEnv(), "bash", args...)
}

// emptyOut fails the test if OUT holds anything: a refused build writes nothing, and the script's own
// temporary directory is gone.
func (f *fixture) emptyOut(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.out)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 0 {
		t.Errorf("OUT holds %v after a refused build, want nothing", names)
	}
}

type member struct {
	typ  byte
	mode int64
	uid  int
	gid  int
	mod  time.Time
	body []byte
}

// readArchive returns the archive's entries by name, in the order they are stored.
func readArchive(t *testing.T, path string) ([]string, map[string]member) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(zr)
	var names []string
	m := map[string]member{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
		m[h.Name] = member{typ: h.Typeflag, mode: h.Mode, uid: h.Uid, gid: h.Gid, mod: h.ModTime, body: body}
	}
	return names, m
}

// shared builds the fixture's archive once, for the tests that read it.
var (
	sharedOnce    sync.Once
	sharedFixture *fixture
	sharedArchive string
	sharedOutput  string
	sharedCode    int
)

func shared(t *testing.T) (*fixture, string) {
	t.Helper()
	sharedOnce.Do(func() {
		// t of the first test owns the fixture's temporary directory, which is gone after it, so the
		// shared one lives under sharedDir.
		base := filepath.Join(sharedDir, "fixture")
		if err := os.MkdirAll(base, 0o755); err != nil {
			t.Fatal(err)
		}
		f := newFixtureIn(t, base)
		sharedFixture = f
		sharedOutput, sharedCode = f.build(t, "--tag", fixtureTag)
		if sharedCode == 0 {
			sharedArchive = filepath.Join(f.out, "hpcburst-"+fixtureTag+"-linux-amd64.tar.gz")
		}
	})
	if sharedCode != 0 {
		t.Fatalf("the shared archive was not built: exit %d:\n%s", sharedCode, sharedOutput)
	}
	return sharedFixture, sharedArchive
}

// The archive holds exactly the specified layout, with the specified modes, owners and times, and the
// seed's and the commit's own files byte for byte.
func TestArchiveHoldsExactlyTheSpecifiedLayout(t *testing.T) {
	f, archive := shared(t)
	if entries, _ := os.ReadDir(f.out); len(entries) != 2 {
		t.Errorf("OUT holds %d entries, want the archive and SHA256SUMS", len(entries))
	}
	names, m := readArchive(t, archive)
	var files, dirs []string
	for _, n := range names {
		e := m[n]
		switch e.typ {
		case tar.TypeReg:
			files = append(files, n)
		case tar.TypeDir:
			dirs = append(dirs, n)
		default:
			t.Errorf("%s has the type %c: only files and directories", n, e.typ)
		}
		if e.uid != 0 || e.gid != 0 {
			t.Errorf("%s is owned by %d:%d, want 0:0", n, e.uid, e.gid)
		}
		if !e.mod.Equal(commitTime) {
			t.Errorf("%s has the time %v, want the commit's %v", n, e.mod, commitTime)
		}
	}
	var wantFiles []string
	for n := range archiveFiles {
		wantFiles = append(wantFiles, n)
	}
	sort.Strings(wantFiles)
	sort.Strings(files)
	if strings.Join(files, "\n") != strings.Join(wantFiles, "\n") {
		t.Errorf("the archive's files are\n%s\nwant\n%s", strings.Join(files, "\n"), strings.Join(wantFiles, "\n"))
	}
	sort.Strings(dirs)
	if strings.Join(dirs, "\n") != strings.Join(archiveDirs, "\n") {
		t.Errorf("the archive's directories are %v, want %v", dirs, archiveDirs)
	}
	for n, mode := range archiveFiles {
		if got := m[n].mode; got != mode {
			t.Errorf("%s has mode %o, want %o", n, got, mode)
		}
	}
	for _, n := range dirs {
		if m[n].mode != 0o755 {
			t.Errorf("%s has mode %o, want 755", n, m[n].mode)
		}
	}
	for n, want := range seedFiles {
		if string(m[n].body) != want {
			t.Errorf("%s holds %q, want the seed's %q", n, m[n].body, want)
		}
	}
	for n, want := range dataFiles {
		if string(m[n].body) != want {
			t.Errorf("%s holds %q, want the commit's %q", n, m[n].body, want)
		}
	}
}

// SHA256SUMS lists the archive, then every file in it, as sha256sum prints them, with sums that Go
// computes from the archive itself; and sha256sum -c accepts the archive and its unpacked files.
func TestArchiveChecksumsListTheArchiveAndEveryFile(t *testing.T) {
	f, archive := shared(t)
	raw, err := os.ReadFile(filepath.Join(f.out, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != len(archiveFiles)+1 {
		t.Fatalf("SHA256SUMS has %d lines, want the archive's and %d files'", len(lines), len(archiveFiles))
	}
	ab, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(ab)
	if want := hex.EncodeToString(h[:]) + "  " + filepath.Base(archive); lines[0] != want {
		t.Errorf("the first line is %q, want %q", lines[0], want)
	}
	_, m := readArchive(t, archive)
	var wantNames []string
	for n := range archiveFiles {
		wantNames = append(wantNames, n)
	}
	sort.Strings(wantNames)
	for i, n := range wantNames {
		hb := sha256.Sum256(m[n].body)
		if want := hex.EncodeToString(hb[:]) + "  " + n; lines[i+1] != want {
			t.Errorf("line %d is %q, want %q", i+2, lines[i+1], want)
		}
	}
	// The same check the file's reader would run, in a directory with the archive and what it unpacks to.
	dir := t.TempDir()
	if out, code := run(t, "", nil, "cp", archive, filepath.Join(f.out, "SHA256SUMS"), dir); code != 0 {
		t.Fatal(out)
	}
	if out, code := run(t, dir, nil, "tar", "-xzf", filepath.Base(archive)); code != 0 {
		t.Fatal(out)
	}
	if out, code := run(t, dir, nil, "sha256sum", "-c", "--quiet", "SHA256SUMS"); code != 0 {
		t.Errorf("sha256sum -c failed: %s", out)
	}
}

// The host binaries are what the release says they are: they report the tag, from a clean tree, with
// VCS stamping, static and trimmed, for linux amd64. The reader is Go's own, not the script's.
func TestArchiveHostBinariesReportTheTagCleanStaticAndTrimmed(t *testing.T) {
	_, archive := shared(t)
	dir := t.TempDir()
	if out, code := run(t, dir, nil, "tar", "-xzf", archive); code != 0 {
		t.Fatal(out)
	}
	for _, b := range []string{"hpcburst", "hpcburst-controller", "hpcburst-front"} {
		out, code := run(t, "", nil, "go", "version", "-m", filepath.Join(dir, "bin", b))
		if code != 0 {
			t.Fatalf("go version -m %s: %s", b, out)
		}
		for _, want := range []string{
			"\tmod\tgithub.com/groundnuty/plgrid-llmlab-hpc-burst\t" + fixtureTag + "\t",
			"\tbuild\tvcs.modified=false", "\tbuild\t-trimpath=true", "\tbuild\tCGO_ENABLED=0", "\tbuild\tGOOS=linux", "\tbuild\tGOARCH=amd64", "\tbuild\tvcs=git",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("go version -m %s lacks %q:\n%s", b, want, out)
			}
		}
		if strings.Contains(out, "+dirty") {
			t.Errorf("%s reports +dirty:\n%s", b, out)
		}
	}
}

// The same commit built again is the same archive, byte for byte.
func TestArchiveIsTheSameBytesWhenBuiltAgain(t *testing.T) {
	f, archive := shared(t)
	second := filepath.Join(t.TempDir(), "out2")
	out, code := run(t, f.dir, gitEnv(), "bash", "hack/release-archive.sh", "--tag", fixtureTag, "--seed", f.seed, "--no-fetch", second)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	a, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(second, filepath.Base(archive)))
	if err != nil {
		t.Fatal(err)
	}
	if sum(string(a)) != sum(string(b)) {
		t.Errorf("a second build of the same commit is another archive: %s and %s", sum(string(a)), sum(string(b)))
	}
}

// A seed that holds an frpc beside the frps, as a cache of hack/fetch-frp.sh's does, does not put it in
// the archive: the archive carries frps in bin/ and each architecture's frpc in agent-builds/.
func TestArchiveDropsAnFrpcThatASeedHoldsBesideTheFrps(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.seed, "bin", "frp-0.71.1", "frpc"), seedFiles["agent-builds/frpc-x86_64"], 0o755)
	out, code := f.build(t, "--tag", fixtureTag)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	names, _ := readArchive(t, filepath.Join(f.out, "hpcburst-"+fixtureTag+"-linux-amd64.tar.gz"))
	for _, n := range names {
		if n == "bin/frp-0.71.1/frpc" {
			t.Errorf("the archive holds %s", n)
		}
	}
	if len(names) != len(archiveFiles)+len(archiveDirs) {
		t.Errorf("the archive has %d entries, want the layout's %d", len(names), len(archiveFiles)+len(archiveDirs))
	}
}

// A run with no tag is the dry run the workflow's hand-started runs make: the archive is named by the
// pseudo-version of the commit, which its binaries report.
func TestArchiveWithoutATagIsNamedByThePseudoVersionOfTheCommit(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.dir, "later.txt"), "a commit after the tag\n", 0o644)
	git(t, f.dir, "add", "-A")
	git(t, f.dir, "commit", "-q", "-m", "after the tag")
	head := git(t, f.dir, "rev-parse", "HEAD")
	out, code := f.build(t)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	re := regexp.MustCompile(`^hpcburst-(v[0-9][0-9A-Za-z.-]*[-.][0-9]{14}-` + head[:12] + `)-linux-amd64\.tar\.gz$`)
	entries, err := os.ReadDir(f.out)
	if err != nil {
		t.Fatal(err)
	}
	var version string
	for _, e := range entries {
		if m := re.FindStringSubmatch(e.Name()); m != nil {
			version = m[1]
		}
	}
	if version == "" {
		t.Fatalf("no archive named by a pseudo-version of %s in %v", head[:12], entries)
	}
	dir := t.TempDir()
	if o, c := run(t, dir, nil, "tar", "-xzf", filepath.Join(f.out, "hpcburst-"+version+"-linux-amd64.tar.gz")); c != 0 {
		t.Fatal(o)
	}
	info, _ := run(t, "", nil, "go", "version", "-m", filepath.Join(dir, "bin", "hpcburst"))
	if !strings.Contains(info, "\tmod\tgithub.com/groundnuty/plgrid-llmlab-hpc-burst\t"+version+"\t") {
		t.Errorf("hpcburst does not report %s:\n%s", version, info)
	}
}

// Each refusal has its own cause, and refuses before anything is written to OUT.
func TestArchiveRefusesEachCauseBeforeWritingAnything(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, f *fixture) (opts []string)
		want  string
	}{
		{"a dirty checkout", func(t *testing.T, f *fixture) []string {
			write(t, filepath.Join(f.dir, "untracked.txt"), "x\n", 0o644)
			return []string{"--tag", fixtureTag}
		}, "the checkout is not clean"},
		{"a checkout whose .git is a file", func(t *testing.T, f *fixture) []string {
			wt := filepath.Join(filepath.Dir(f.dir), "worktree")
			git(t, f.dir, "worktree", "add", "-q", "--detach", wt)
			f.dir = wt
			return []string{"--tag", fixtureTag}
		}, ".git is not a directory"},
		{"an OUT inside the checkout", func(t *testing.T, f *fixture) []string {
			f.out = filepath.Join(f.dir, "dist")
			return []string{"--tag", fixtureTag}
		}, "is inside the checkout"},
		{"a tag that does not exist", func(t *testing.T, f *fixture) []string {
			return []string{"--tag", "v0.9.0"}
		}, "the tag v0.9.0 does not exist"},
		{"a tag that names another commit", func(t *testing.T, f *fixture) []string {
			write(t, filepath.Join(f.dir, "later.txt"), "later\n", 0o644)
			git(t, f.dir, "add", "-A")
			git(t, f.dir, "commit", "-q", "-m", "later")
			return []string{"--tag", fixtureTag}
		}, "the tag " + fixtureTag + " names"},
		{"a tag without its notes", func(t *testing.T, f *fixture) []string {
			git(t, f.dir, "tag", "v0.2.0-beta.1")
			return []string{"--tag", "v0.2.0-beta.1"}
		}, "docs/releases/v0.2.0-beta.1.md is missing or empty"},
		{"a tagged commit built as a dry run", func(t *testing.T, f *fixture) []string {
			return nil
		}, "is not a pseudo-version of this commit"},
		{"a commit without examples/", func(t *testing.T, f *fixture) []string {
			if err := os.RemoveAll(filepath.Join(f.dir, "examples")); err != nil {
				t.Fatal(err)
			}
			git(t, f.dir, "add", "-A")
			git(t, f.dir, "commit", "-q", "-m", "no examples")
			return nil
		}, "the commit has no examples/"},
		{"a tag of a major version the module path does not allow", func(t *testing.T, f *fixture) []string {
			write(t, filepath.Join(f.dir, "docs", "releases", "v2.0.0.md"), "notes\n", 0o644)
			git(t, f.dir, "add", "-A")
			git(t, f.dir, "commit", "-q", "-m", "notes")
			git(t, f.dir, "tag", "v2.0.0")
			return []string{"--tag", "v2.0.0"}
		}, "and the tag is v2.0.0 (Go stamps a tag only on the commit it names"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			opts := tc.setup(t, f)
			out, code := f.build(t, opts...)
			if code == 0 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want a refusal saying %q:\n%s", code, tc.want, out)
			}
			f.emptyOut(t)
		})
	}
}

// Each pinned file is checked against its pins file where it is packed, whatever put it there: the
// fetch scripts are off (--no-fetch), so these refusals are the archive script's own. Each case changes
// one file of the seed and leaves the pins files as they are.
func TestArchiveChecksEveryPinnedFileAgainstItsPinsFile(t *testing.T) {
	for _, tc := range []struct {
		file string
		want string
	}{
		{"bin/k3s-1.36.5/k3s", "k3s v1.36.5+k3s1 is not the pinned file"},
		{"bin/frp-0.71.1/frps", "frps 0.71.1 is not the pinned file"},
		{"agent-builds/frpc-x86_64", "frpc 0.71.1 x86_64 is not the pinned file"},
		{"agent-builds/frpc-aarch64", "frpc 0.71.1 aarch64 is not the pinned file"},
	} {
		t.Run("changed "+tc.file, func(t *testing.T) {
			f := newFixture(t)
			write(t, filepath.Join(f.seed, tc.file), seedFiles[tc.file]+"one more line\n", 0o755)
			out, code := f.build(t, "--tag", fixtureTag)
			if code == 0 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want a refusal saying %q:\n%s", code, tc.want, out)
			}
			f.emptyOut(t)
		})
		t.Run("missing "+tc.file, func(t *testing.T) {
			f := newFixture(t)
			if err := os.Remove(filepath.Join(f.seed, tc.file)); err != nil {
				t.Fatal(err)
			}
			out, code := f.build(t, "--tag", fixtureTag)
			if code == 0 || !strings.Contains(out, "is missing from the archive") {
				t.Fatalf("exit %d, want a refusal of the missing file:\n%s", code, out)
			}
			f.emptyOut(t)
		})
	}
	t.Run("a pins file that names another sum", func(t *testing.T) {
		f := newFixture(t)
		pins := filepath.Join(f.dir, "internal", "launcher", "k3s-pins.txt")
		write(t, pins, "v1.36.5+k3s1  amd64  k3s  "+sum("another k3s")+"\n", 0o644)
		git(t, f.dir, "add", "-A")
		git(t, f.dir, "commit", "-q", "-m", "pins")
		git(t, f.dir, "tag", "-f", fixtureTag)
		out, code := f.build(t, "--tag", fixtureTag)
		if code == 0 || !strings.Contains(out, "k3s v1.36.5+k3s1 is not the pinned file") {
			t.Fatalf("exit %d, want the k3s refusal:\n%s", code, out)
		}
	})
}

// ---- hack/release-unpack.sh and the rehearsal's archive input -------------------------------------

// A member of a hand-made archive.
type made struct {
	name string
	mode int64
	body string
	typ  byte
	link string
}

// goodArchive is the smallest archive the unpack script takes: the seven entries it must find.
func goodArchive() []made {
	return []made{
		{name: "bin/hpcburst", mode: 0o755, body: "host binary hpcburst\n"},
		{name: "bin/hpcburst-controller", mode: 0o755, body: "host binary hpcburst-controller\n"},
		{name: "bin/hpcburst-front", mode: 0o755, body: "host binary hpcburst-front\n"},
		{name: "bin/k3s-1.36.5/k3s", mode: 0o755, body: "k3s\n"},
		{name: "bin/frp-0.71.1/frps", mode: 0o755, body: "frps\n"},
		{name: "agent-builds/hb-agent-x86_64", mode: 0o755, body: "agent x86_64\n"},
		{name: "agent-builds/hb-agent-aarch64", mode: 0o755, body: "agent aarch64\n"},
		{name: "agent-builds/frpc-x86_64", mode: 0o755, body: "frpc x86_64\n"},
		{name: "agent-builds/frpc-aarch64", mode: 0o755, body: "frpc aarch64\n"},
		{name: "profiles/example.yaml", mode: 0o644, body: "profile\n"},
	}
}

func writeArchive(t *testing.T, path string, members []made) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, m := range members {
		typ := m.typ
		if typ == 0 {
			typ = tar.TypeReg
		}
		h := &tar.Header{Name: m.name, Mode: m.mode, Typeflag: typ, Linkname: m.link, ModTime: commitTime}
		if typ == tar.TypeReg {
			h.Size = int64(len(m.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if typ == tar.TypeReg {
			if _, err := tw.Write([]byte(m.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// sumsFor writes the SHA256SUMS of a hand-made archive, as the build script writes it.
func sumsFor(t *testing.T, archive string, members []made) {
	t.Helper()
	ab, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(ab)
	lines := []string{hex.EncodeToString(h[:]) + "  " + filepath.Base(archive)}
	var names []string
	body := map[string]string{}
	for _, m := range members {
		if m.typ == 0 || m.typ == tar.TypeReg {
			names = append(names, m.name)
			body[m.name] = m.body
		}
	}
	sort.Strings(names)
	for _, n := range names {
		lines = append(lines, sum(body[n])+"  "+n)
	}
	write(t, filepath.Join(filepath.Dir(archive), "SHA256SUMS"), strings.Join(lines, "\n")+"\n", 0o644)
}

func unpack(t *testing.T, archive, out, frp string) (string, int) {
	t.Helper()
	return run(t, repoRoot(t), nil, "bash", "hack/release-unpack.sh", archive, out, frp)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func wantMode(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	if fi.Mode().Perm() != mode {
		t.Errorf("%s has mode %o, want %o", path, fi.Mode().Perm(), mode)
	}
}

// The names the rehearsal's prebuilt mode takes, and the frp directory it expects, from an archive.
func TestUnpackPlacesTheBinariesAndTheFrpDirectoryUnderTheNamesTheRehearsalTakes(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "release.tar.gz")
	writeArchive(t, archive, goodArchive())
	out, frp := filepath.Join(dir, "out"), filepath.Join(dir, "frp")
	if o, code := unpack(t, archive, out, frp); code != 0 {
		t.Fatalf("exit %d: %s", code, o)
	}
	for name, want := range map[string]string{
		"hpcburst": "host binary hpcburst\n", "hpcburst-controller": "host binary hpcburst-controller\n",
		"hpcburst-front": "host binary hpcburst-front\n", "hb-agent": "agent x86_64\n",
	} {
		if got := readFile(t, filepath.Join(out, name)); got != want {
			t.Errorf("%s holds %q, want %q", name, got, want)
		}
		wantMode(t, filepath.Join(out, name), 0o755)
	}
	for name, want := range map[string]string{"frps": "frps\n", "frpc": "frpc x86_64\n"} {
		if got := readFile(t, filepath.Join(frp, name)); got != want {
			t.Errorf("%s in the frp directory holds %q, want %q", name, got, want)
		}
		wantMode(t, filepath.Join(frp, name), 0o755)
	}
	// The k3s directory is the archive's own bin/, where `up --bin-dir` looks.
	if got := readFile(t, filepath.Join(out, "archive", "bin", "k3s-1.36.5", "k3s")); got != "k3s\n" {
		t.Errorf("bin/k3s-1.36.5/k3s holds %q", got)
	}
	if got := readFile(t, filepath.Join(out, "archive", "bin", "frp-0.71.1", "frps")); got != "frps\n" {
		t.Errorf("bin/frp-0.71.1/frps holds %q", got)
	}
	if _, err := os.Stat(filepath.Join(out, "archive", "profiles", "example.yaml")); err != nil {
		t.Errorf("the rest of the archive did not unpack: %v", err)
	}
}

// An archive that lacks one of the entries the rehearsal needs is refused, by name, before anything is
// written.
func TestUnpackRefusesAnArchiveThatLacksAnEntry(t *testing.T) {
	for _, missing := range []string{
		"bin/hpcburst", "bin/hpcburst-controller", "bin/hpcburst-front", "bin/k3s-1.36.5/k3s", "bin/frp-0.71.1/frps",
		"agent-builds/hb-agent-x86_64", "agent-builds/frpc-x86_64",
	} {
		t.Run(missing, func(t *testing.T) {
			dir := t.TempDir()
			var members []made
			for _, m := range goodArchive() {
				if m.name != missing {
					members = append(members, m)
				}
			}
			archive := filepath.Join(dir, "release.tar.gz")
			writeArchive(t, archive, members)
			out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
			if code == 0 || !strings.Contains(out, "the archive has no "+missing) {
				t.Fatalf("exit %d, want a refusal naming %s:\n%s", code, missing, out)
			}
			if _, err := os.Stat(filepath.Join(dir, "out", "archive")); err == nil {
				t.Error("something was unpacked before the refusal")
			}
		})
	}
}

// An entry that would land outside the unpacked tree, or that is a link, is refused before anything is
// written.
func TestUnpackRefusesEntriesThatEscapeOrLink(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  made
		want string
	}{
		{"an absolute name", made{name: "/tmp/escaped", mode: 0o644, body: "x"}, "outside the unpacked tree"},
		{"a parent component", made{name: "bin/../../escaped", mode: 0o644, body: "x"}, "outside the unpacked tree"},
		{"a leading parent", made{name: "../escaped", mode: 0o644, body: "x"}, "outside the unpacked tree"},
		{"a symbolic link", made{name: "bin/link", typ: tar.TypeSymlink, link: "/etc/passwd"}, "entries other than files and directories"},
		{"a hard link", made{name: "bin/hard", typ: tar.TypeLink, link: "bin/hpcburst"}, "entries other than files and directories"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			archive := filepath.Join(dir, "release.tar.gz")
			writeArchive(t, archive, append(goodArchive(), tc.bad))
			out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
			if code == 0 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want a refusal saying %q:\n%s", code, tc.want, out)
			}
			if _, err := os.Stat(filepath.Join(dir, "out", "archive")); err == nil {
				t.Error("something was unpacked before the refusal")
			}
		})
	}
	t.Run("not an archive", func(t *testing.T) {
		dir := t.TempDir()
		write(t, filepath.Join(dir, "release.tar.gz"), "not gzip\n", 0o644)
		if out, code := unpack(t, filepath.Join(dir, "release.tar.gz"), filepath.Join(dir, "out"), filepath.Join(dir, "frp")); code == 0 || !strings.Contains(out, "is not a gzip tar archive") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
	t.Run("an OUT that already holds an unpacked archive", func(t *testing.T) {
		dir := t.TempDir()
		archive := filepath.Join(dir, "release.tar.gz")
		writeArchive(t, archive, goodArchive())
		out := filepath.Join(dir, "out")
		if o, code := unpack(t, archive, out, filepath.Join(dir, "frp")); code != 0 {
			t.Fatal(o)
		}
		if o, code := unpack(t, archive, out, filepath.Join(dir, "frp")); code == 0 || !strings.Contains(o, "already there") {
			t.Fatalf("exit %d:\n%s", code, o)
		}
	})
}

// With a SHA256SUMS beside the archive, the archive and the files it unpacks are checked against it.
func TestUnpackChecksTheArchiveAndItsFilesAgainstSHA256SUMS(t *testing.T) {
	setup := func(t *testing.T) (dir, archive string) {
		dir = t.TempDir()
		archive = filepath.Join(dir, "release.tar.gz")
		writeArchive(t, archive, goodArchive())
		sumsFor(t, archive, goodArchive())
		return dir, archive
	}
	t.Run("matching", func(t *testing.T) {
		dir, archive := setup(t)
		out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
		if code != 0 || !strings.Contains(out, "matches") || !strings.Contains(out, "every unpacked file matches") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
	t.Run("an archive that is not the one listed", func(t *testing.T) {
		dir, archive := setup(t)
		members := goodArchive()
		members[0].body = "another binary\n"
		writeArchive(t, archive, members) // the sums stay those of the first
		out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
		if code == 0 || !strings.Contains(out, "does not match its line") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		if _, err := os.Stat(filepath.Join(dir, "out", "archive")); err == nil {
			t.Error("something was unpacked before the refusal")
		}
	})
	t.Run("a file the sums do not list", func(t *testing.T) {
		dir, archive := setup(t)
		members := append(goodArchive(), made{name: "bin/extra", mode: 0o755, body: "unlisted\n"})
		writeArchive(t, archive, members)
		sumsFor(t, archive, goodArchive()) // the archive's own line is new; the files' lines are the old ones
		out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
		if code == 0 || !strings.Contains(out, "are not the files") || !strings.Contains(out, "bin/extra") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
	t.Run("a listed file that is not there", func(t *testing.T) {
		dir, archive := setup(t)
		members := append(goodArchive(), made{name: "bin/extra", mode: 0o755, body: "listed\n"})
		sumsFor(t, archive, members) // lists bin/extra, which the archive does not hold
		out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
		if code == 0 || !strings.Contains(out, "are not the files") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
	t.Run("a file whose sum differs", func(t *testing.T) {
		dir, archive := setup(t)
		members := goodArchive()
		members[3].body = "another k3s\n"
		writeArchive(t, archive, members)
		// The archive's own line is right; one file's line is the old one.
		ab := readFile(t, archive)
		lines := strings.Split(strings.TrimSuffix(readFile(t, filepath.Join(dir, "SHA256SUMS")), "\n"), "\n")
		lines[0] = sum(ab) + "  " + filepath.Base(archive)
		write(t, filepath.Join(dir, "SHA256SUMS"), strings.Join(lines, "\n")+"\n", 0o644)
		out, code := unpack(t, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
		if code == 0 || !strings.Contains(out, "an unpacked file does not match") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
	})
}

// rehearsal runs hack/rehearsal.sh from the repository, with the variables the rehearsal reads cleared
// (CI sets some of them) and these set.
func rehearsal(t *testing.T, env ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", "hack/rehearsal.sh")
	cmd.Dir = repoRoot(t)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HPCBURST_") || strings.HasPrefix(kv, "REHEARSAL_") {
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return string(out), ee.ExitCode()
	}
	t.Fatal(err)
	return "", -1
}

// REHEARSAL_ARCHIVE unpacks the archive the release script built into the names the prebuilt mode takes,
// and stops there when asked: the names, the modes and the bytes are those of the archive's entries.
func TestRehearsalArchiveInputUnpacksTheReleaseArchiveIntoThePrebuiltNames(t *testing.T) {
	_, archive := shared(t)
	out := filepath.Join(t.TempDir(), "out")
	o, code := rehearsal(t, "REHEARSAL_ARCHIVE="+archive, "REHEARSAL_OUT="+out, "REHEARSAL_UNPACK_ONLY=1")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, o)
	}
	_, m := readArchive(t, archive)
	for name, entry := range map[string]string{
		"hpcburst": "bin/hpcburst", "hpcburst-controller": "bin/hpcburst-controller", "hpcburst-front": "bin/hpcburst-front",
		"hb-agent": "agent-builds/hb-agent-x86_64",
	} {
		if got := readFile(t, filepath.Join(out, name)); got != string(m[entry].body) {
			t.Errorf("%s is not the archive's %s", name, entry)
		}
		wantMode(t, filepath.Join(out, name), 0o755)
	}
	if got := readFile(t, filepath.Join(out, "frp", "frps")); got != seedFiles["bin/frp-0.71.1/frps"] {
		t.Errorf("frps in the frp directory holds %q", got)
	}
	if got := readFile(t, filepath.Join(out, "frp", "frpc")); got != seedFiles["agent-builds/frpc-x86_64"] {
		t.Errorf("frpc in the frp directory holds %q", got)
	}
	wantMode(t, filepath.Join(out, "frp", "frps"), 0o755)
	wantMode(t, filepath.Join(out, "frp", "frpc"), 0o755)
	if got := readFile(t, filepath.Join(out, "archive", "bin", "k3s-1.36.5", "k3s")); got != seedFiles["bin/k3s-1.36.5/k3s"] {
		t.Errorf("k3s under archive/bin holds %q", got)
	}
	// The two directories the run would read, as the script says them: the archive's own bin/, where
	// k3s-1.36.5/k3s and frp-0.71.1/frps are, and the frp directory with frps and frpc.
	for _, want := range []string{"HPCBURST_BIN_DIR=" + filepath.Join(out, "archive", "bin"), "HPCBURST_FRP_DIR=" + filepath.Join(out, "frp")} {
		if !strings.Contains("\n"+o, "\n"+want+"\n") {
			t.Errorf("the output lacks the line %q:\n%s", want, o)
		}
	}
	// Unpack-only builds and runs nothing: no test binary, and no scope.
	if _, err := os.Stat(filepath.Join(out, "rehearsal.test")); err == nil {
		t.Error("unpack-only built rehearsal.test")
	}
	if !strings.Contains(o, "matches") {
		t.Errorf("the SHA256SUMS beside the archive was not checked:\n%s", o)
	}
}

// The archive gives the k3s and frp directories itself, and is one of two ways to the binaries.
func TestRehearsalArchiveInputRefusesTheVariablesItReplaces(t *testing.T) {
	_, archive := shared(t)
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{"a bin directory too", []string{"HPCBURST_BIN_DIR=/somewhere"}, "unset HPCBURST_BIN_DIR and HPCBURST_FRP_DIR"},
		{"an frp directory too", []string{"HPCBURST_FRP_DIR=/somewhere"}, "unset HPCBURST_BIN_DIR and HPCBURST_FRP_DIR"},
		{"prebuilt too", []string{"REHEARSAL_PREBUILT=1"}, "two ways to the binaries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := append([]string{"REHEARSAL_ARCHIVE=" + archive, "REHEARSAL_OUT=" + filepath.Join(t.TempDir(), "out"), "REHEARSAL_UNPACK_ONLY=1"}, tc.env...)
			o, code := rehearsal(t, env...)
			if code != 2 || !strings.Contains(o, tc.want) {
				t.Fatalf("exit %d, want 2 and %q:\n%s", code, tc.want, o)
			}
		})
	}
	t.Run("neither an archive nor the directories", func(t *testing.T) {
		o, code := rehearsal(t, "REHEARSAL_OUT="+filepath.Join(t.TempDir(), "out"))
		if code == 0 || !strings.Contains(o, "or set REHEARSAL_ARCHIVE") {
			t.Fatalf("exit %d:\n%s", code, o)
		}
	})
}

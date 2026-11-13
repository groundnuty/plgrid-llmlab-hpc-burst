package quickstart

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Step 2's release archive path (marked "once v0.1.0-alpha.1 is published") is checked by running
// its lines, for real, on an archive that has the layout the release scripts document. Nothing
// here claims the published archive works: that is the release rehearsal's to show. What the page
// can get wrong is its own lines, and they are run:
//   - the files the lines read are laid out as the launcher and the controller look for them, which
//     the launcher's own lookups and agentapi's names say, not this test: k3s and frps by asking
//     the launcher where it looks in an empty binary directory, the agent builds by agentapi's
//     names, the host binaries by the programs the page's build line builds;
//   - the fixture is held to what the repository's own unpacker accepts (hack/release-unpack.sh
//     refuses an archive that lacks an entry it needs, and one whose files SHA256SUMS does not
//     list), so a change to the release layout fails here before it fails a user;
//   - the lines are run in an empty directory, not a clone, with STATE and BIN set, and each place a
//     later step reads is looked at afterwards: the archive path needs no clone (issue #479), so
//     every file the later steps read must come from the archive.

// archiveBlock is the shell block that checks and unpacks the archive.
func archiveBlock(t *testing.T) block {
	t.Helper()
	return only(t, "block that checks and unpacks the release archive", func(b block) bool {
		return b.lang == "" && strings.Contains(b.text, "sha256sum -c")
	})
}

// buildPrograms are the host binaries the page's build line builds, by name.
func buildPrograms(t *testing.T) []string {
	t.Helper()
	b := only(t, "block with the go build line", func(b block) bool { return strings.Contains(b.text, "go build -o bin/") })
	m := regexp.MustCompile(`go build -o bin/((?: \./cmd/[a-z-]+)+)`).FindStringSubmatch(strings.ReplaceAll(b.text, "\n", " "))
	if m == nil {
		t.Fatalf("no go build -o bin/ line with programs in\n%s", b.text)
	}
	var out []string
	for _, p := range strings.Fields(m[1]) {
		out = append(out, path.Base(p))
	}
	return out
}

// launcherLayout returns where the launcher looks for k3s and for frps, relative to the binary
// directory. It asks the launcher about an empty directory, where both lookups fail and name the
// file they wanted. It cannot ask about a directory that holds the files: the launcher then checks
// them against its pins, and a fixture's files are not the pinned binaries.
func launcherLayout(t *testing.T) (k3s, frps string) {
	t.Helper()
	empty := t.TempDir()
	_, errK3s := launcher.ResolveBinary(empty, launcher.DefaultVersion)
	_, errFrps := launcher.ResolveFrps(empty, launcher.DefaultFrpVersion)
	rel := func(p string) string {
		r, err := filepath.Rel(empty, p)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	return rel(pathOf(t, errK3s)), rel(pathOf(t, errFrps))
}

// archiveCommitDirs are the commit's directories the archive carries whole, as HBEP-15's Releases
// lists them (config/crd/, profiles/ and examples/, the last by its amendment of 12 Nov 2026), typed
// here: hack/release-archive.sh's own list is not read.
var archiveCommitDirs = []string{"config/crd", "profiles", "examples"}

// archiveMembers maps each file name of the archive to its content: the files the page's lines
// read, and the commit's files under archiveCommitDirs, with their content, as the real archive
// carries them.
func archiveMembers(t *testing.T) map[string]string {
	t.Helper()
	k3s, frps := launcherLayout(t)
	m := map[string]string{
		"bin/" + k3s:  "the k3s binary",
		"bin/" + frps: "the frps binary",
	}
	for _, d := range archiveCommitDirs {
		n := 0
		err := filepath.WalkDir(filepath.Join(root, d), func(p string, e os.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			m[filepath.ToSlash(rel)] = string(b)
			n++
			return nil
		})
		if err != nil || n == 0 {
			t.Fatalf("the repository's %s/ gives the archive no file (%d, %v)", d, n, err)
		}
	}
	for _, prog := range buildPrograms(t) {
		m["bin/"+prog] = "the " + prog + " binary"
	}
	for _, arch := range []string{"x86_64", "aarch64"} {
		m["agent-builds/"+agentapi.AgentBuildName(arch)] = "the agent for " + arch
		m["agent-builds/"+agentapi.FrpcBuildName(arch)] = "frpc for " + arch
	}
	return m
}

// writeArchive writes the archive and, beside it, a SHA256SUMS in the form hack/release-archive.sh
// documents: the archive's line first, then one line for each file by its name in the archive.
func writeArchive(t *testing.T, dir, name string, members map[string]string) string {
	t.Helper()
	names := make([]string, 0, len(members))
	for n := range members {
		names = append(names, n)
	}
	sort.Strings(names)
	p := filepath.Join(dir, name)
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	var sums []string
	for _, n := range names {
		mode := int64(0o755)
		if strings.HasPrefix(n, "config/") || strings.HasPrefix(n, "profiles/") || strings.HasPrefix(n, "examples/") {
			mode = 0o644
		}
		body := members[n]
		if err := tw.WriteHeader(&tar.Header{Name: n, Mode: mode, Size: int64(len(body)), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0)}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
		sums = append(sums, fmt.Sprintf("%x  %s", sha256.Sum256([]byte(body)), n))
	}
	for _, c := range []interface{ Close() error }{tw, gz, f} {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	all := append([]string{fmt.Sprintf("%x  %s", sha256.Sum256(raw), name)}, sums...)
	if err := os.WriteFile(filepath.Join(dir, "SHA256SUMS"), []byte(strings.Join(all, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// archiveName is the archive file the page's tar line unpacks.
func archiveName(t *testing.T) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^tar -xzf (\S+)(?: -C \S+)?$`).FindStringSubmatch(archiveBlock(t).text)
	if m == nil {
		t.Fatalf("the archive block has no tar -xzf <archive> line:\n%s", archiveBlock(t).text)
	}
	return m[1]
}

// runArchiveBlock runs the page's lines in work, an empty directory (not a clone) that holds the
// archive and its SHA256SUMS, with the directories of the test as STATE and BIN, and returns what it
// printed and how it ended. The block runs under set -eu, as a user who stops at the first error would.
func runArchiveBlock(t *testing.T, work string, d dirs, home string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", "-c", "set -eu\n"+archiveBlock(t).text)
	cmd.Dir = work
	cmd.Env = append(baseEnv(home), "STATE="+d.state, "BIN="+d.bin)
	out, err := cmd.CombinedOutput()
	switch e := err.(type) {
	case nil:
		return string(out), 0
	case *exec.ExitError:
		return string(out), e.ExitCode()
	default:
		t.Fatalf("run the archive block: %v", e)
		return "", -1
	}
}

// The fixture is an archive the repository's own unpacker accepts, so the layout that the page's
// lines are run on is the release's.
func TestArchiveFixtureIsWhatTheReleaseUnpackerAccepts(t *testing.T) {
	dir := t.TempDir()
	archive := writeArchive(t, dir, archiveName(t), archiveMembers(t))
	script, err := filepath.Abs(filepath.Join(root, "hack", "release-unpack.sh"))
	if err != nil {
		t.Fatal(err)
	}
	_, home := testDirs(t)
	out, code := run(t, baseEnv(home), script, archive, filepath.Join(dir, "out"), filepath.Join(dir, "frp"))
	if code != 0 {
		t.Fatalf("hack/release-unpack.sh refuses the fixture (exit %d): the release layout changed, or this test's fixture did not follow it:\n%s", code, out)
	}
}

// The page's lines leave every file where the launcher and the controller look for it.
func TestArchiveLinesPlaceWhatTheLauncherAndTheControllerRead(t *testing.T) {
	members := archiveMembers(t)
	clone := t.TempDir() // the directory the page's lines run in: empty but for the download
	writeArchive(t, clone, archiveName(t), members)
	d, home := testDirs(t)
	if out, code := runArchiveBlock(t, clone, d, home); code != 0 {
		t.Fatalf("the archive lines fail (exit %d):\n%s", code, out)
	}

	// Where each later step reads: the host binaries beside each other in bin/ (up looks for the
	// controller and the front beside itself), k3s and frps in the binary directory, and the
	// agent builds in the state directory's agent-builds.
	k3s, frps := launcherLayout(t)
	want := map[string]string{
		filepath.Join(d.bin, k3s):  "the k3s binary",
		filepath.Join(d.bin, frps): "the frps binary",
	}
	for _, prog := range buildPrograms(t) {
		want[filepath.Join(clone, "bin", prog)] = "the " + prog + " binary"
	}
	builds := agentapi.Host{StateDir: d.state}.AgentBuildsDir()
	for _, arch := range []string{"x86_64", "aarch64"} {
		want[filepath.Join(builds, agentapi.AgentBuildName(arch))] = "the agent for " + arch
		want[filepath.Join(builds, agentapi.FrpcBuildName(arch))] = "frpc for " + arch
	}
	for p, content := range want {
		got, err := os.ReadFile(p)
		switch {
		case err != nil:
			t.Errorf("after the archive lines, %v", err)
		case string(got) != content:
			t.Errorf("%s holds %q; want the archive's file, %q", p, got, content)
		}
	}
	for _, prog := range buildPrograms(t) {
		if fi, err := os.Stat(filepath.Join(clone, "bin", prog)); err == nil && fi.Mode().Perm()&0o111 == 0 {
			t.Errorf("bin/%s is not executable", prog)
		}
	}
	if fi, err := os.Stat(builds); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("the agent-builds directory: want mode 700 (the state directory's), got %v (%v)", fi, err)
	}
}

// An archive that does not match its checksums stops the lines at the first one, before anything
// is unpacked or copied.
func TestArchiveLinesStopOnADamagedArchive(t *testing.T) {
	clone := t.TempDir()
	archive := writeArchive(t, clone, archiveName(t), archiveMembers(t))
	f, err := os.OpenFile(archive, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("damage"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	d, home := testDirs(t)
	out, code := runArchiveBlock(t, clone, d, home)
	if code == 0 {
		t.Fatalf("the archive lines pass on a damaged archive:\n%s", out)
	}
	for _, p := range []string{"bin", "agent-builds", "config", "profiles", "examples"} {
		if _, err := os.Stat(filepath.Join(clone, p)); err == nil {
			t.Errorf("the lines went past the checksum: %s exists", p)
		}
	}
}

// The archive and checksum names the page uses are the ones the release workflow makes: a glob in
// the workflow's upload step and its draft step, which this test reads as text, and the version
// in the page's heading and in the file name agree.
func TestArchiveNamesAreTheReleaseWorkflowsAndTheTagIsOne(t *testing.T) {
	name := archiveName(t)
	workflow := readRepoFile(t, ".github/workflows/release.yml")
	globs := regexp.MustCompile(`hpcburst-\*-linux-amd64\.tar\.gz`).FindAllString(workflow, -1)
	if len(globs) == 0 {
		t.Fatal("release.yml names no hpcburst-*-linux-amd64.tar.gz archive")
	}
	if ok, err := path.Match(globs[0], name); err != nil || !ok {
		t.Errorf("the page unpacks %q, which does not match the release workflow's %q (%v)", name, globs[0], err)
	}
	if !strings.Contains(workflow, "SHA256SUMS") || !strings.Contains(archiveBlock(t).text, " SHA256SUMS") {
		t.Errorf("the release workflow and the page must both name SHA256SUMS")
	}
	h := regexp.MustCompile(`(?m)^### Release archive \(once (v[0-9]\S*) is published\)$`).FindStringSubmatch(readRepoFile(t, "docs/quickstart.md"))
	if h == nil {
		t.Fatal("docs/quickstart.md has no heading 'Release archive (once <tag> is published)'")
	}
	if want := "hpcburst-" + h[1] + "-linux-amd64.tar.gz"; name != want {
		t.Errorf("the heading says %s is the release, so the archive is %s; the page unpacks %s", h[1], want, name)
	}
}

// laterReads are the repository paths that the blocks after the archive block read from the working
// directory, named as TestNamedPathsExist names them in code, and the host binaries under bin/.
var laterReads = regexp.MustCompile(`(?:examples/[a-z0-9-]+\.yaml|profiles/[a-z0-9-]+\.yaml|config/crd|\bbin/[a-z-]+)`)

// stepsText is the page from its start to the end of its numbered steps: up to the first level-2
// heading after "## 1." that is not itself a numbered step ("## Optional: ...", "## Helios (beta)",
// "## Beta: ..."). The archive path is those steps; the sections after them are options a reader
// may take, and each says what it needs.
func stepsText(t *testing.T) string {
	t.Helper()
	page := readRepoFile(t, "docs/quickstart.md")
	first := regexp.MustCompile(`(?m)^## 1\. `).FindStringIndex(page)
	if first == nil {
		t.Fatal(`docs/quickstart.md has no "## 1." step`)
	}
	rest := page[first[1]:]
	if end := regexp.MustCompile(`(?m)^## [^0-9]`).FindStringIndex(rest); end != nil {
		return page[:first[1]+end[0]]
	}
	return page
}

// stepBlocks are the fenced code blocks of the numbered steps (stepsText).
func stepBlocks(t *testing.T) []block {
	t.Helper()
	var out []block
	for _, m := range fence.FindAllStringSubmatch(stepsText(t), -1) {
		out = append(out, block{lang: m[1], text: m[2]})
	}
	return out
}

// The archive path needs no clone: no numbered step after the archive block runs a script of the
// repository, and every repository path those steps read is in the working directory once the archive
// lines have run there, in a directory that is not a clone. A path the archive does not carry, such as
// an example missing from examples/, fails here. The optional sections after the steps are not the
// archive path; TestASectionThatRunsARepositoryScriptSaysItNeedsAClone holds them to saying so.
func TestArchivePathNeedsNoClone(t *testing.T) {
	all := stepBlocks(t)
	at := -1
	for i, b := range all {
		if b.lang == "" && strings.Contains(b.text, "sha256sum -c") {
			at = i
		}
	}
	if at < 0 {
		t.Fatal("docs/quickstart.md has no archive block")
	}
	reads := map[string]bool{}
	for _, b := range all[at+1:] {
		if strings.Contains(b.text, "hack/") {
			t.Errorf("a step after the archive block runs a repository script, which an archive user does not have:\n%s", b.text)
		}
		for _, p := range laterReads.FindAllString(b.text, -1) {
			reads[p] = true
		}
	}
	var kinds []string
	for _, prefix := range []string{"bin/", "config/crd", "profiles/", "examples/"} {
		for p := range reads {
			if strings.HasPrefix(p, prefix) {
				kinds = append(kinds, prefix)
				break
			}
		}
	}
	if len(kinds) != 4 {
		t.Fatalf("the later steps read %v; want at least one path under each of bin/, config/crd, profiles/ and examples/, or this test checks nothing", reads)
	}

	work := t.TempDir()
	if _, err := os.Stat(filepath.Join(work, ".git")); err == nil {
		t.Fatal("the working directory is a clone")
	}
	writeArchive(t, work, archiveName(t), archiveMembers(t))
	d, home := testDirs(t)
	if out, code := runArchiveBlock(t, work, d, home); code != 0 {
		t.Fatalf("the archive lines fail (exit %d):\n%s", code, out)
	}
	for p := range reads {
		if _, err := os.Stat(filepath.Join(work, p)); err != nil {
			t.Errorf("a later step reads %s, which the unpacked archive does not hold: %v", p, err)
		}
	}
}

// A section after the numbered steps that runs a script of the repository needs a clone, which the
// archive path does not have, so the section's own text must say it needs a clone (the beta router
// section fetches Envoy's build with hack/fetch-envoy.sh: HBEP-15 ships no Envoy in the archive).
func TestASectionThatRunsARepositoryScriptSaysItNeedsAClone(t *testing.T) {
	page := readRepoFile(t, "docs/quickstart.md")
	after := page[len(stepsText(t)):]
	if after == "" {
		t.Fatal("docs/quickstart.md has no section after its numbered steps: this test would check nothing")
	}
	sections := regexp.MustCompile(`(?m)^## `).Split(after, -1)
	runs := 0
	for _, s := range sections {
		if s == "" {
			continue
		}
		heading, _, _ := strings.Cut(s, "\n")
		for _, m := range fence.FindAllStringSubmatch(s, -1) {
			if strings.Contains(m[2], "hack/") {
				runs++
				if !strings.Contains(s, "needs a clone") {
					t.Errorf("the section %q runs a repository script but doesn't say it needs a clone:\n%s", heading, m[2])
				}
			}
		}
	}
	if runs == 0 {
		t.Fatal("no section after the steps runs a repository script: the beta router section's fetch is gone, so this test checks nothing; drop or retarget it")
	}
}

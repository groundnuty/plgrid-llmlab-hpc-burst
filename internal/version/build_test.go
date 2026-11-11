package version

// The three host binaries, built for real: "go build" of each, inside this repository, with VCS
// stamping on (-buildvcs=true, so a build that cannot stamp fails instead of stamping nothing).
// Each one's "version" line is checked against git itself: the commit from "git rev-parse HEAD",
// the commit's time from git log in UTC, and the tag from the tags that point at HEAD. Nothing
// here uses this package's formatter or its seam: the binary reads its own build information.
//
// The builds use the outer toolchain's default flags, not the release workflow's CGO_ENABLED=0 and
// -trimpath (HBEP-15, Releases): the VCS stamp depends on neither, and a flag that the surrounding
// "go test ./..." does not use makes every compile action differ from its own, so the nested
// builds would recompile the module's whole dependency graph from scratch (issue #401, the review
// of PR 405). With the defaults they find what the outer build compiled in the build cache.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// stampedVCS reports whether the Go tool stamped VCS information into the binary, read with "go version -m", which
// shares no code with this package: a vcs.revision build setting.
func stampedVCS(t *testing.T, bin string) bool {
	t.Helper()
	out, err := exec.Command("go", "version", "-m", bin).CombinedOutput()
	if err != nil {
		t.Fatalf("go version -m %s: %v\n%s", bin, err, out)
	}
	return strings.Contains(string(out), "\tvcs.revision=")
}

// isWorktree reports whether root is a linked git worktree (or a submodule): its ".git" is a file, not a
// directory. The pinned Go, 1.26.0, stamps no VCS information there, whatever -buildvcs says, and builds
// with the "(devel)" version; a newer Go does stamp it. In a normal clone, as actions/checkout makes, the
// pinned Go stamps correctly.
func isWorktree(root string) bool {
	fi, err := os.Stat(filepath.Join(root, ".git"))
	return err == nil && !fi.IsDir()
}

// git runs git in the repository and returns its output, trimmed; ok is false when it cannot.
func git(t *testing.T, root string, args ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	cmd.Env = append(os.Environ(), "TZ=UTC", "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err == nil
}

// The versions the Go tool stamps (golang.org/ref/mod, Pseudo-versions): a release tag; or a
// pseudo-version, the base's next version, the commit's time and its first 12 hex digits, in
// three shapes: v0.0.0-<time>-<hash> before any tag, vX.Y.Z-pre.0.<time>-<hash> after a
// pre-release tag, and vX.Y.(Z+1)-0.<time>-<hash> after a release tag.
var (
	pseudoVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]*)?[.-][0-9]{14}-([0-9a-f]{12})$`)
	releaseTag    = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$`)
)

func TestThePseudoVersionPatternTakesWhatGoStamps(t *testing.T) {
	for _, tc := range []struct {
		v    string
		hash string // "" for no match
	}{
		{"v0.0.0-20261111201055-05cbafcb8ea4", "05cbafcb8ea4"},
		{"v0.1.0-alpha.1.0.20261112090000-1a2b3c4d5e6f", "1a2b3c4d5e6f"},
		{"v0.1.1-0.20261112090000-1a2b3c4d5e6f", "1a2b3c4d5e6f"},
		{"v0.2.0-beta.1.0.20261201000000-0123456789ab", "0123456789ab"},
		{"v0.1.0-alpha.1", ""},
		{"(devel)", ""},
		{"v0.0.0-2026111120105-05cbafcb8ea4", ""},
		{"v0.0.0-20261111201055-05cbafcb8ea", ""},
		{"v0.0.0-20261111201055-05cbafcb8ea4+dirty", ""},
	} {
		m := pseudoVersion.FindStringSubmatch(tc.v)
		switch {
		case tc.hash == "" && m != nil:
			t.Errorf("%q matched as a pseudo-version", tc.v)
		case tc.hash != "" && (m == nil || m[2] != tc.hash):
			t.Errorf("%q: match %v, want hash %q", tc.v, m, tc.hash)
		}
	}
}

func goVersion(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

// A binary built with VCS stamping off reports the documented no-VCS line, whatever the checkout and the Go
// release (it does not need git at all): the tag (devel), and unknown for the commit and the time. This is
// the case the pinned Go gives in a git worktree too.
func TestABinaryBuiltWithoutVCSStampingReportsDevelAndUnknown(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "hpcburst")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "./cmd/hpcburst")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build -buildvcs=false ./cmd/hpcburst: %v\n%s", err, out)
	}
	if stampedVCS(t, bin) {
		t.Fatal("go build -buildvcs=false stamped VCS information")
	}
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(out), "hpcburst (devel) unknown unknown\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTheBuiltBinariesReportTheCommitGitSees(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	head, ok := git(t, root, "rev-parse", "HEAD")
	if !ok {
		t.Skip("SKIPPED, NOT TESTED: this is not a git checkout (or git is missing), and a binary built here carries no VCS information")
	}
	when, ok := git(t, root, "log", "-1", "--format=%cd", "--date=format-local:%Y-%m-%dT%H:%M:%SZ", "HEAD")
	if !ok || when == "" {
		t.Fatal("git cannot give HEAD's commit time")
	}
	tagsAtHead, _ := git(t, root, "tag", "--points-at", "HEAD", "v*")
	statusBefore, ok := git(t, root, "status", "--porcelain")
	if !ok {
		t.Fatal("git status failed")
	}

	dir := t.TempDir()
	names := []string{"hpcburst", "hpcburst-controller", "hpcburst-front"}
	for _, name := range names {
		build := exec.Command("go", "build", "-buildvcs=true", "-o", filepath.Join(dir, name), "./cmd/"+name)
		build.Dir = root
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build ./cmd/%s: %v\n%s", name, err, out)
		}
	}
	stamped := map[string]bool{}
	for _, name := range names {
		stamped[name] = stampedVCS(t, filepath.Join(dir, name))
	}
	lines := map[string]string{}
	for _, name := range names {
		var out, errb bytes.Buffer
		cmd := exec.Command(filepath.Join(dir, name), "version")
		cmd.Stdout, cmd.Stderr = &out, &errb
		if err := cmd.Run(); err != nil {
			t.Fatalf("%s version: %v (stderr %q)", name, err, errb.String())
		}
		if errb.Len() != 0 {
			t.Errorf("%s version wrote to stderr: %q", name, errb.String())
		}
		lines[name] = out.String()
	}
	statusAfter, _ := git(t, root, "status", "--porcelain")
	stable := statusBefore == statusAfter

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			line := lines[name]
			if !stamped[name] {
				// The Go tool put no VCS information in this binary. Only a linked worktree under the pinned Go
				// does that; in a normal clone it is a failure of the stamping the release depends on.
				if !isWorktree(root) {
					t.Fatalf("the Go tool stamped no VCS information into %s, built with -buildvcs=true in a normal clone (.git is a directory)", name)
				}
				if want := name + " (devel) unknown unknown\n"; line != want {
					t.Errorf("an unstamped binary printed %q, want %q", line, want)
				}
				// What there is to compare with git is absent: the binary's own no-VCS line was checked above, and the
				// rest is skipped, with the reason (the pinned Go in a worktree; the tests of the seam and the
				// -buildvcs=false build cover the line everywhere).
				t.Skipf("SKIPPED, NOT COMPARED WITH GIT: %s stamped no VCS information into %s in this git worktree (the pinned Go, 1.26.0, stamps none there; a normal clone is stamped); its line is the documented %q", goVersion(t), name, name+" (devel) unknown unknown")
				return
			}
			if !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
				t.Fatalf("output %q is not one newline-terminated line", line)
			}
			f := strings.Fields(line)
			if len(f) != 4 && len(f) != 5 {
				t.Fatalf("output %q has %d fields, want 4 or 5 (binary, tag, commit, time, and dirty)", line, len(f))
			}
			if f[0] != name {
				t.Errorf("the first field is %q, want the binary %q", f[0], name)
			}
			switch tag := f[1]; {
			case tagsAtHead != "":
				if !strings.Contains("\n"+tagsAtHead+"\n", "\n"+tag+"\n") {
					t.Errorf("tag %q, but the tags at HEAD are %q", tag, tagsAtHead)
				}
			case pseudoVersion.MatchString(tag):
				// The pseudo-version names the commit by its first 12 hex digits: they must be git's.
				if m := pseudoVersion.FindStringSubmatch(tag); m[2] != head[:12] {
					t.Errorf("tag %q names commit %s, but HEAD is %s", tag, m[2], head)
				}
			case releaseTag.MatchString(tag):
				t.Errorf("tag %q is a release tag, but no v* tag points at HEAD", tag)
			default:
				t.Errorf("tag %q is neither a pseudo-version nor a tag at HEAD (a build with VCS stamping on is never (devel))", tag)
			}
			if strings.Contains(f[1], "+dirty") {
				t.Errorf("tag %q carries +dirty: dirtiness is its own field", f[1])
			}
			if f[2] != head {
				t.Errorf("commit %q, want git's HEAD %q", f[2], head)
			}
			if f[3] != when {
				t.Errorf("time %q, want the commit's time %q (UTC, from git log)", f[3], when)
			}
			if len(f) == 5 && f[4] != "dirty" {
				t.Errorf("the last field is %q, want dirty", f[4])
			}
			if stable {
				if want := statusBefore != ""; (len(f) == 5) != want {
					t.Errorf("dirty field present %v, but git status --porcelain is %q", len(f) == 5, statusBefore)
				}
			}
		})
	}
}

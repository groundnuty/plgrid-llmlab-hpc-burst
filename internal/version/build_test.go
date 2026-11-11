package version

// The three host binaries, built for real: "go build" of each, inside this repository, with VCS
// stamping on (-buildvcs=true, so a build that cannot stamp fails instead of stamping nothing), as
// the release workflow builds them (HBEP-15, Releases; CGO_ENABLED=0 and -trimpath). Each one's
// "version" line is checked against git itself: the commit from "git rev-parse HEAD", the commit's
// time from git log in UTC, and the tag from the tags that point at HEAD. Nothing here uses this
// package's formatter or its seam: the binary reads its own build information.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

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
		build := exec.Command("go", "build", "-buildvcs=true", "-trimpath", "-o", filepath.Join(dir, name), "./cmd/"+name)
		build.Dir = root
		build.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build ./cmd/%s: %v\n%s", name, err, out)
		}
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

package fakeslurm

// New. B1 of the PR 153 review: the run-mode file system is anchored to an
// os.Root, so a symbolic link a job made cannot lead an operation out of the
// root, and a path that is not cleaned is refused rather than resolved. The
// links are made by the test with os.Symlink, and every expected value is the
// state of the outside file on disk after the call: the outside file is the
// thing a link must never let the fake touch.

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	outsideContent = "outside-secret"
	insideContent  = "inside"
)

// linkFixture is a root with real files, a sibling directory outside it with
// a secret, and the links between them.
type linkFixture struct {
	root, outside string
	d             *diskFS
}

// newLinkFixture makes, under the root: real/file and real/sub (the targets
// of the links that stay in), and the links in-file (-> real/file) and in-dir
// (-> real), both relative; and the links that go out: out-file (relative,
// -> the outside secret), out-abs (absolute, -> the secret) and out-dir
// (-> the outside directory).
func newLinkFixture(t *testing.T) *linkFixture {
	t.Helper()
	parent := t.TempDir()
	f := &linkFixture{root: filepath.Join(parent, "root"), outside: filepath.Join(parent, "outside")}
	for _, d := range []string{f.root + "/real/sub", f.outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range map[string]string{f.root + "/real/file": insideContent, f.outside + "/secret": outsideContent} {
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{
		"in-file": "real/file", "in-dir": "real",
		"out-file": "../outside/secret", "out-abs": f.outside + "/secret", "out-dir": "../outside",
	} {
		if err := os.Symlink(target, filepath.Join(f.root, link)); err != nil {
			t.Fatal(err)
		}
	}
	f.d = newTestDiskFS(t, f.root)
	return f
}

// secretIntact fails the test unless the outside secret is as it was made:
// same content, same mode, still there, with nothing else beside it.
func (f *linkFixture) secretIntact(t *testing.T, doing string) {
	t.Helper()
	b, err := os.ReadFile(f.outside + "/secret")
	fi, serr := os.Stat(f.outside + "/secret")
	if err != nil || serr != nil || string(b) != outsideContent || fi.Mode().Perm() != 0o644 {
		t.Errorf("after %s the outside secret is %q (%v), mode %v (%v); want it untouched", doing, b, err, fi, serr)
	}
	if es, _ := os.ReadDir(f.outside); len(es) != 1 {
		t.Errorf("after %s the outside directory holds %d entries, want only the secret", doing, len(es))
	}
}

// The outside file stays out of reach of every operation, whether the link is
// the last component of the path or an ancestor of it.
func TestDiskFSRefusesLinksOutOfTheRoot(t *testing.T) {
	f := newLinkFixture(t)
	d, r := f.d, f.root
	// The same secret, named three ways: a final-component link that is
	// relative, one that is absolute, and a path through a directory link.
	// and, for a new file, a path through the directory link.
	for _, name := range []string{r + "/out-file", r + "/out-abs", r + "/out-dir/secret", r + "/out-dir/created"} {
		t.Run(strings.TrimPrefix(name, r+"/"), func(t *testing.T) {
			if d.isFile(name) || d.isDir(name) {
				t.Errorf("%s reads as present", name)
			}
			if _, found, _ := d.sum(name); found {
				t.Errorf("sum found %s", name)
			}
			if err := d.write(name, []byte("clobbered"), 0o644, ""); err == nil {
				t.Errorf("write through %s: no error", name)
			}
			f.secretIntact(t, "write through "+name)
			if err := d.put(name, []byte("clobbered"), 0o600); err == nil {
				t.Errorf("put through %s: no error", name)
			}
			f.secretIntact(t, "put through "+name)
			if found, _ := d.chmod(name, 0o600); found {
				t.Errorf("chmod found %s", name)
			}
			f.secretIntact(t, "chmod through "+name)
		})
	}
	// A rename moves a link itself, so only a path through a link that leads
	// out is refused, as its source or as its target.
	if err := d.rename(r+"/real/file", r+"/out-dir/stolen"); err == nil {
		t.Errorf("rename into out-dir: no error")
	}
	if err := d.rename(r+"/out-dir/secret", r+"/taken"); err == nil {
		t.Errorf("rename out of out-dir: no error")
	}
	f.secretIntact(t, "rename through out-dir")
	if got, err := os.ReadFile(r + "/real/file"); err != nil || string(got) != insideContent {
		t.Errorf("real/file is %q (%v) after the refused renames", got, err)
	}
	// Removal: a link is removed itself, never followed, and a path through
	// a link that leads out is refused.
	if err := d.removeTree(r + "/out-dir/secret"); err == nil {
		t.Errorf("removeTree through out-dir: no error")
	}
	f.secretIntact(t, "removeTree through out-dir")
	if err := d.removeTree(r + "/out-file"); err != nil {
		t.Errorf("removeTree of the link itself: %v", err)
	}
	if _, err := os.Lstat(r + "/out-file"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the link out-file after removeTree: %v, want it gone", err)
	}
	f.secretIntact(t, "removeTree of out-file")
	// A directory made through a link, or on top of one.
	if err := d.mkdirAll(r+"/out-dir/new/deep", 0o755); err == nil {
		t.Errorf("mkdirAll through out-dir: no error")
	}
	if err := d.mkdirAll(r+"/out-dir", 0o755); err == nil {
		t.Errorf("mkdirAll over the link out-dir: no error")
	}
	f.secretIntact(t, "mkdirAll through out-dir")
	// The snapshot does not follow a link to read the secret.
	files, err := d.snapshotFiles()
	if err != nil {
		t.Fatal(err)
	}
	for name, file := range files {
		if strings.Contains(string(file.Content), outsideContent) || strings.HasPrefix(name, f.outside) {
			t.Errorf("snapshot holds %s with %q, read through a link", name, file.Content)
		}
	}
}

// A link that resolves inside the root is followed (a Hugging Face cache is
// made of them); os.Root refuses an absolute one, so these are relative.
func TestDiskFSFollowsLinksInsideTheRoot(t *testing.T) {
	f := newLinkFixture(t)
	d, r := f.d, f.root
	want := sha256.Sum256([]byte(insideContent))
	if got, found, err := d.sum(r + "/in-file"); !found || err != nil || got != want {
		t.Errorf("sum through in-file: found %v, %v, %x; want the sum of %q", found, err, got, insideContent)
	}
	if !d.isFile(r+"/in-dir/file") || !d.isDir(r+"/in-dir/sub") {
		t.Errorf("in-dir/file is not a file, or in-dir/sub is not a directory")
	}
	if err := d.write(r+"/in-file", []byte("changed"), 0o644, ""); err != nil {
		t.Errorf("write through in-file: %v", err)
	}
	if err := d.write(r+"/in-dir/new", []byte("new"), 0o640, ""); err != nil {
		t.Errorf("create through in-dir: %v", err)
	}
	if err := d.mkdirAll(r+"/in-dir/sub/more", 0o755); err != nil {
		t.Errorf("mkdirAll through in-dir: %v", err)
	}
	if found, err := d.chmod(r+"/in-dir/new", 0o600); !found || err != nil {
		t.Errorf("chmod through in-dir: %v, %v", found, err)
	}
	if err := d.rename(r+"/in-dir/new", r+"/in-dir/moved"); err != nil {
		t.Errorf("rename through in-dir: %v", err)
	}
	for name, want := range map[string]string{r + "/real/file": "changed", r + "/real/moved": "new"} {
		if got, err := os.ReadFile(name); err != nil || string(got) != want {
			t.Errorf("%s is %q (%v), want %q", name, got, err, want)
		}
	}
	if fi, err := os.Stat(r + "/real/moved"); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the chmod through in-dir: %v, %v; want 600", fi, err)
	}
	if err := d.removeTree(r + "/in-dir/sub"); err != nil {
		t.Errorf("removeTree through in-dir: %v", err)
	}
	if _, err := os.Stat(r + "/real/sub"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("real/sub after removeTree through in-dir: %v", err)
	}
	f.secretIntact(t, "the operations through the links that stay inside")
}

// A path that is not cleaned is refused, not resolved: root/../outside/secret
// is the secret, and it does not pass for a path under the root.
func TestDiskFSRefusesUncleanedPaths(t *testing.T) {
	f := newLinkFixture(t)
	d, r := f.d, f.root
	for _, name := range []string{
		r + "/../outside/secret", r + "/real/../../outside/secret", r + "/real/../real/file",
		r + "/real/", r + "/./real/file", r + "//real/file",
	} {
		if d.isFile(name) || d.isDir(name) {
			t.Errorf("stat accepted %q", name)
		}
		if _, found, _ := d.sum(name); found {
			t.Errorf("sum accepted %q", name)
		}
		if d.write(name, []byte("x"), 0o644, "") == nil || d.put(name, []byte("x"), 0o644) == nil ||
			d.mkdirAll(name, 0o755) == nil || d.removeTree(name) == nil || d.rename(r+"/real/file", name) == nil {
			t.Errorf("a mutating call accepted %q", name)
		}
		if found, _ := d.chmod(name, 0o600); found {
			t.Errorf("chmod accepted %q", name)
		}
	}
	f.secretIntact(t, "the calls with uncleaned paths")
}

// The login's commands and a job's --output go through the same file system:
// a link that leads out refuses them, and the outside file is untouched.
func TestRunModeLinksOutOfTheRootRefuseCommandsAndOutputs(t *testing.T) {
	r := newRunRig(t, RunMode{})
	f := &linkFixture{root: r.root, outside: t.TempDir()}
	if err := os.WriteFile(f.outside+"/secret", []byte(outsideContent), 0o644); err != nil {
		t.Fatal(err)
	}
	for link, target := range map[string]string{"out-file": f.outside + "/secret", "out-dir": f.outside} {
		if err := os.Symlink(target, r.home+"/"+link); err != nil {
			t.Fatal(err)
		}
	}
	for _, line := range []string{
		"cat > " + r.home + "/out-file", "cat > " + r.home + "/out-dir/secret", "sha256sum " + r.home + "/out-file",
		"chmod 600 " + r.home + "/out-file", "rm -rf " + r.home + "/out-dir/secret", "mkdir -p " + r.home + "/out-dir/new",
		"mv -f " + r.home + "/out-file " + r.home + "/out-dir/moved",
	} {
		if _, _, code := r.runAs(r.s, line, []byte("clobbered")); code != 1 {
			t.Errorf("%q: exit %d, want 1", line, code)
		}
		f.secretIntact(t, line)
	}
	for _, out := range []string{"out-file", "out-dir/secret"} {
		id := r.submit("echo ran > "+r.home+"/ran", "--output="+r.home+"/"+out)
		j := r.job(id)
		if j.State != Failed || j.StartErr == nil || j.PID != 0 || !j.Finished {
			t.Errorf("--output=%s: state %s, StartErr %v, PID %d, finished %v; want FAILED at once with an error and no process", out, j.State, j.StartErr, j.PID, j.Finished)
		}
		f.secretIntact(t, "a job with --output="+out)
	}
	if _, err := os.Stat(r.home + "/ran"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a job whose output could not be opened ran: %v", err)
	}
}

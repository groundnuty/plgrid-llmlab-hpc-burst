package agentapi

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const goodKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// badKeys are contents a key file must never hold, each with a name. The
// content of each is a distinct marker so a message that repeats it shows.
var badKeys = map[string]string{
	"a trailing newline": goodKey + "\n",
	"63 hex characters":  goodKey[:63],
	"65 hex characters":  goodKey + "a",
	"uppercase hex":      strings.ToUpper(goodKey),
	"a non-hex letter":   goodKey[:63] + "g",
	"empty":              "",
	"a leading space":    " " + goodKey[:63],
	"a crowd":            strings.Repeat("x", 1<<20),
}

func TestNewKey(t *testing.T) {
	form := regexp.MustCompile(`^[0-9a-f]{64}$`) // this test's own expression
	seen := map[string]bool{}
	for range 50 {
		k, err := NewKey()
		if err != nil {
			t.Fatal(err)
		}
		if !form.MatchString(k) {
			t.Fatalf("%q is not 64 lowercase hex characters", k)
		}
		if seen[k] {
			t.Fatalf("%q was drawn twice", k)
		}
		seen[k] = true
	}
}

func TestCheckKeyAndCredential(t *testing.T) {
	for _, check := range []struct {
		name string
		f    func([]byte) error
	}{{"CheckKey", CheckKey}, {"CheckCredential", CheckCredential}} {
		if err := check.f([]byte(goodKey)); err != nil {
			t.Errorf("%s refused a key: %v", check.name, err)
		}
		for name, bad := range badKeys {
			err := check.f([]byte(bad))
			if err == nil {
				t.Errorf("%s accepted %s", check.name, name)
				continue
			}
			if bad != "" && len(bad) < 1000 && strings.Contains(err.Error(), bad) {
				t.Errorf("%s, %s: the error repeats the content: %v", check.name, name, err)
			}
		}
	}
}

func TestKeyFileRoundTrip(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "credentials", "instances", "uid-1")
	if err := WriteKeyFile(path, goodKey); err != nil {
		t.Fatal(err)
	}
	// What is on disk, read without ReadKeyFile: exactly the 64 bytes, mode 0600.
	b, err := os.ReadFile(path)
	if err != nil || string(b) != goodKey || len(b) != 64 {
		t.Fatalf("file holds %q (%d bytes), %v", b, len(b), err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o600 || !fi.Mode().IsRegular() {
		t.Fatalf("mode = %v, %v; want a regular file, 0600", fi.Mode(), err)
	}
	for _, d := range []string{"credentials", "credentials/instances"} {
		di, err := os.Stat(filepath.Join(root, d))
		if err != nil || !di.IsDir() || di.Mode().Perm() != 0o700 {
			t.Errorf("directory %s: %v, %v; want 0700", d, di, err)
		}
	}
	// Nothing but the key file is left in its directory (no temporary file).
	ents, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(ents) != 1 || ents[0].Name() != "uid-1" {
		t.Errorf("directory holds %v, %v", ents, err)
	}
	got, err := ReadKeyFile(path)
	if err != nil || got != goodKey {
		t.Errorf("ReadKeyFile = %q, %v", got, err)
	}
}

func TestWriteKeyFileReplacesAndRefuses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "k")
	// An existing file, with another mode and content, is replaced by a 0600 file.
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteKeyFile(path, goodKey); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != goodKey {
		t.Errorf("file holds %q", b)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
	}
	// A second key replaces the first.
	other := strings.Repeat("ab", 32)
	if err := WriteKeyFile(path, other); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadKeyFile(path); err != nil || got != other {
		t.Errorf("ReadKeyFile = %q, %v", got, err)
	}

	// A symlink at the path is replaced, not followed.
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := WriteKeyFile(link, goodKey); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(target); string(b) != "target" {
		t.Errorf("the write followed the link: target holds %q", b)
	}
	if li, err := os.Lstat(link); err != nil || !li.Mode().IsRegular() {
		t.Errorf("the link was not replaced by a file: %v, %v", li, err)
	}

	// A refused key leaves nothing behind, not even the parent directories.
	for name, bad := range badKeys {
		p := filepath.Join(dir, "new", "dir", "uid")
		err := WriteKeyFile(p, bad)
		if err == nil {
			t.Errorf("%s was written", name)
			continue
		}
		if bad != "" && len(bad) < 1000 && strings.Contains(err.Error(), bad) {
			t.Errorf("%s: the error repeats the key: %v", name, err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, "new")); !errors.Is(statErr, fs.ErrNotExist) {
			t.Errorf("%s: a refused key created %s", name, filepath.Join(dir, "new"))
		}
	}
}

// A write that fails after the temporary file exists removes it.
func TestWriteKeyFileFailureLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	// The path is a non-empty directory, so the rename cannot replace it.
	path := filepath.Join(dir, "k")
	if err := os.MkdirAll(filepath.Join(path, "inner"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteKeyFile(path, goodKey); err == nil {
		t.Fatal("a key was written over a directory")
	}
	ents, err := os.ReadDir(dir)
	if err != nil || len(ents) != 1 || ents[0].Name() != "k" {
		t.Errorf("directory holds %v, %v; want only k", ents, err)
	}
}

func TestReadKeyFileRefusesBadContent(t *testing.T) {
	dir := t.TempDir()
	for name, content := range badKeys {
		p := filepath.Join(dir, "bad")
		// Written by hand: WriteKeyFile refuses these.
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := ReadKeyFile(p)
		if err == nil {
			t.Errorf("%s was read as %q", name, got)
			continue
		}
		if got != "" {
			t.Errorf("%s: ReadKeyFile returned %q with an error", name, got)
		}
		if content != "" && strings.Contains(err.Error(), content) {
			t.Errorf("%s: the error repeats the content: %v", name, err)
		}
		if len(err.Error()) > 500 {
			t.Errorf("%s: the error is %d bytes long, so it echoes the content", name, len(err.Error()))
		}
	}
}

func TestReadKeyFileModes(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct {
		mode os.FileMode
		ok   bool
	}{
		// The rule: no permission bit for the group or others, and the owner can read.
		{0o600, true}, {0o400, true}, {0o700, true}, {0o500, true},
		{0o640, false}, {0o604, false}, {0o644, false}, {0o660, false}, {0o606, false}, {0o666, false}, {0o602, false},
		{0o200, false}, {0o000, false}, {0o300, false},
	} {
		p := filepath.Join(dir, "k")
		os.Remove(p)
		if err := os.WriteFile(p, []byte(goodKey), 0o600); err != nil {
			t.Fatal(err)
		}
		// Chmod, not WriteFile's perm, which the umask would narrow.
		if err := os.Chmod(p, c.mode); err != nil {
			t.Fatal(err)
		}
		got, err := ReadKeyFile(p)
		switch {
		case c.ok && (err != nil || got != goodKey):
			t.Errorf("mode %04o was refused: %q, %v", c.mode, got, err)
		case !c.ok && err == nil:
			t.Errorf("mode %04o was accepted", c.mode)
		case !c.ok && !strings.Contains(err.Error(), "mode"):
			t.Errorf("mode %04o: the error does not name the mode: %v", c.mode, err)
		}
	}
}

func TestReadKeyFileRefusesWhatIsNotAFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte(goodKey), 0o600); err != nil {
		t.Fatal(err)
	}
	// A symlink to a perfect key file.
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadKeyFile(link); err == nil {
		t.Errorf("a symlink was read as %q", got)
	} else if !strings.Contains(err.Error(), "symbolic link") {
		t.Errorf("the error does not say why: %v", err)
	}
	// A directory.
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadKeyFile(sub); err == nil || !strings.Contains(err.Error(), "directory") {
		t.Errorf("a directory: %v", err)
	}
	// A missing file: the error wraps fs.ErrNotExist so a caller can wait for it.
	if _, err := ReadKeyFile(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a missing file: %v does not wrap fs.ErrNotExist", err)
	}
}

// The owner check cannot be tried with a file of another owner without root,
// so the check is run with another user number.
func TestKeyFileOwnerCheck(t *testing.T) {
	p := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(p, []byte(goodKey), 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	euid := uint32(os.Geteuid())
	if err := checkKeyFileInfo(fi, euid); err != nil {
		t.Errorf("the owner's file was refused: %v", err)
	}
	if err := checkKeyFileInfo(fi, euid+1); err == nil || !strings.Contains(err.Error(), "owned by") {
		t.Errorf("another user's file was accepted: %v", err)
	}
}

// endless is a reader of x that counts what it was asked for.
type endless struct{ n int }

func (e *endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	e.n += len(p)
	return len(p), nil
}

// A file the size of a disk is not read to its end: at most 65 bytes are.
func TestReadKeyBytesStopsAfter65(t *testing.T) {
	var e endless
	b, err := readKeyBytes(&e)
	if err != nil || len(b) != 65 || e.n > 65 {
		t.Errorf("read %d bytes (asked of the source: %d), %v; want 65 and at most 65", len(b), e.n, err)
	}
}

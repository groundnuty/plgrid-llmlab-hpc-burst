package fakeslurm

// New. Every file and environment command of the grammar. Expected modes
// follow the umask rule: files 0666 &^ umask, directories 0777 &^ umask. The
// SHA-256 of "abc" is the value of FIPS 180-4's example.

import (
	"io/fs"
	"strings"
	"testing"
)

const sha256ABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func TestCatWritesStdinWithTheUmaskMode(t *testing.T) {
	r := newRig(t, Config{})
	if err := r.p.MkdirAll("/d", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, errOut, code := r.runAs(r.s, "cat > /d/plain", []byte("one")); code != 0 {
		t.Fatalf("cat: exit %d, %q", code, errOut)
	}
	if _, _, code := r.runAs(r.s, "umask 077 && cat > /d/private", []byte("two")); code != 0 {
		t.Fatal("cat after umask 077 failed")
	}
	files := r.p.Files()
	if f := files["/d/plain"]; string(f.Content) != "one" || f.Mode != 0o644 || f.Owner != "example-user" {
		t.Errorf("default file = %q mode %o owner %q, want one, 644 (0666 &^ 022), example-user", f.Content, f.Mode, f.Owner)
	}
	if f := files["/d/private"]; string(f.Content) != "two" || f.Mode != 0o600 {
		t.Errorf("file after umask 077 = %q mode %o, want two, 600", f.Content, f.Mode)
	}
	// The umask held for the first line only: a new line starts at 022 again.
	r.runAs(r.s, "cat > /d/later", []byte("x"))
	if m := r.p.Files()["/d/later"].Mode; m != 0o644 {
		t.Errorf("file on a later line has mode %o, want 644", m)
	}
	// A redirection to an existing file keeps its mode and takes the content.
	r.runAs(r.s, "cat > /d/private", []byte("three"))
	if f := r.p.Files()["/d/private"]; string(f.Content) != "three" || f.Mode != 0o600 {
		t.Errorf("rewritten file = %q mode %o, want three, 600", f.Content, f.Mode)
	}
}

func TestCatFailures(t *testing.T) {
	r := newRig(t, Config{})
	r.p.MkdirAll("/d", 0o755)
	for _, tc := range []struct {
		line string
		code int
		want string
	}{
		{"cat > /missing/f", 1, "No such file or directory"},
		{"cat > /d", 1, "Is a directory"},
		{"cat /d/f", 127, "command not found"}, // only "cat > file" exists
	} {
		_, errOut, code := r.runAs(r.s, tc.line, []byte("x"))
		if code != tc.code || !strings.Contains(errOut, tc.want) {
			t.Errorf("%q: exit %d stderr %q, want exit %d and %q", tc.line, code, errOut, tc.code, tc.want)
		}
	}
	if len(r.p.Files()) != 0 {
		t.Errorf("a failed cat left files: %v", r.p.Files())
	}
}

func TestMkdirP(t *testing.T) {
	r := newRig(t, Config{})
	r.must("mkdir -p /a/b/c")
	r.must("umask 077 && mkdir -p /p/q")
	r.must("mkdir -p /a/b/c") // idempotent
	r.must("mkdir -p rel/dir")
	want := map[string]fs.FileMode{
		"/": 0o755, "/a": 0o755, "/a/b": 0o755, "/a/b/c": 0o755, // 0777 &^ 022
		"/p": 0o700, "/p/q": 0o700, // 0777 &^ 077
		"/home/example-user/rel": 0o755, "/home/example-user/rel/dir": 0o755, // relative to HOME
	}
	got := r.p.Dirs()
	for d, m := range want {
		if got[d] != m {
			t.Errorf("directory %s = %o, want %o", d, got[d], m)
		}
	}
	r.p.WriteFile("/a/file", []byte("x"), 0o644)
	if _, errOut, code := r.run("mkdir -p /a/file/sub"); code != 1 || !strings.Contains(errOut, "File exists") {
		t.Errorf("mkdir -p through a file: exit %d stderr %q, want exit 1 File exists", code, errOut)
	}
}

func TestChmod(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/f", []byte("x"), 0o600)
	r.must("chmod 755 /d/f")
	r.must("chmod 700 /d")
	if m := r.p.Files()["/d/f"].Mode; m != 0o755 {
		t.Errorf("file mode %o, want 755", m)
	}
	if m := r.p.Dirs()["/d"]; m != 0o700 {
		t.Errorf("directory mode %o, want 700", m)
	}
	for _, line := range []string{"chmod 755 /nope", "chmod 999 /d/f", "chmod rwx /d/f"} {
		if _, _, code := r.run(line); code != 1 {
			t.Errorf("%q: exit %d, want 1", line, code)
		}
	}
}

func TestMv(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/a", []byte("A"), 0o640)
	r.p.WriteFile("/d/b", []byte("B"), 0o600)
	r.must("mv -f /d/a /d/b") // replaces, and carries a's mode
	files := r.p.Files()
	if _, ok := files["/d/a"]; ok {
		t.Errorf("/d/a is still there after mv")
	}
	if f := files["/d/b"]; string(f.Content) != "A" || f.Mode != 0o640 {
		t.Errorf("/d/b = %q mode %o, want A, 640", f.Content, f.Mode)
	}
	for _, line := range []string{
		"mv -f /d/nope /d/c",  // no source
		"mv -f /d/b /nodir/c", // no destination directory
		"mv -f /d/b /d",       // destination is a directory
		"mv /d/b /d/c",        // only mv -f exists
	} {
		if _, _, code := r.run(line); code == 0 {
			t.Errorf("%q succeeded, want a failure", line)
		}
	}
}

func TestRm(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/a/b/f1", []byte("1"), 0o600)
	r.p.WriteFile("/a/b/c/f2", []byte("2"), 0o600)
	r.p.WriteFile("/a/bc/f3", []byte("3"), 0o600) // shares the prefix "/a/b"
	r.must("rm -f /a/b/f1")
	r.must("rm -f /a/b/f1") // missing is fine
	if _, errOut, code := r.run("rm -f /a/b"); code != 1 || !strings.Contains(errOut, "Is a directory") {
		t.Errorf("rm -f of a directory: exit %d stderr %q, want exit 1 Is a directory", code, errOut)
	}
	r.must("rm -rf /a/b")
	r.must("rm -rf /a/b") // missing is fine
	files, dirs := r.p.Files(), r.p.Dirs()
	if _, ok := files["/a/b/c/f2"]; ok {
		t.Errorf("rm -rf left /a/b/c/f2")
	}
	if _, ok := dirs["/a/b"]; ok {
		t.Errorf("rm -rf left /a/b")
	}
	if _, ok := dirs["/a/b/c"]; ok {
		t.Errorf("rm -rf left /a/b/c")
	}
	if _, ok := files["/a/bc/f3"]; !ok {
		t.Errorf("rm -rf /a/b removed /a/bc/f3, a different directory")
	}
	if _, ok := dirs["/a"]; !ok {
		t.Errorf("rm -rf /a/b removed its parent /a")
	}
	r.p.WriteFile("/x/file", []byte("x"), 0o600)
	r.must("rm -rf /x/file") // rm -rf works on a file too
	if _, ok := r.p.Files()["/x/file"]; ok {
		t.Errorf("rm -rf of a file left it")
	}
}

func TestTestF(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/f", []byte("x"), 0o600)
	for _, tc := range []struct {
		line string
		code int
	}{
		{"test -f /d/f", 0},
		{"test -f /d", 1},      // a directory is not a regular file
		{"test -f /d/nope", 1}, // missing
		{"test -d /d", 2},      // only -f exists
	} {
		if _, _, code := r.run(tc.line); code != tc.code {
			t.Errorf("%q: exit %d, want %d", tc.line, code, tc.code)
		}
	}
}

func TestSha256sum(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/f", []byte("abc"), 0o600)
	if out := r.must("sha256sum /d/f"); out != sha256ABC+"  /d/f\n" {
		t.Errorf("sha256sum = %q, want %q", out, sha256ABC+"  /d/f\n")
	}
	out, errOut, code := r.run("sha256sum /d/missing")
	if code != 1 || out != "" || !strings.Contains(errOut, "No such file or directory") {
		t.Errorf("sha256sum of a missing file: exit %d stdout %q stderr %q, want exit 1, no output, an error", code, out, errOut)
	}
}

func TestPrintenv(t *testing.T) {
	r := newRig(t, Config{})
	if out := r.must("printenv HOME"); out != "/home/example-user\n" {
		t.Errorf("printenv HOME = %q", out)
	}
	if out, errOut, code := r.run("printenv NOPE"); code != 1 || out != "" || errOut != "" {
		t.Errorf("printenv of an unset variable: exit %d stdout %q stderr %q, want a silent exit 1", code, out, errOut)
	}
	empty := r.p.Login(Login{User: "u", Env: map[string]string{"EMPTY": ""}})
	if out, _, code := r.runAs(empty, "printenv EMPTY", nil); code != 0 || out != "\n" {
		t.Errorf("printenv of an empty variable: exit %d stdout %q, want exit 0 and a newline", code, out)
	}
}

func TestUmask(t *testing.T) {
	r := newRig(t, Config{})
	for _, line := range []string{"umask 8", "umask 7777", "umask rwx", "umask"} {
		if _, _, code := r.run(line); code == 0 {
			t.Errorf("%q succeeded, want a failure", line)
		}
	}
}

func TestChainStopsAtTheFirstFailure(t *testing.T) {
	r := newRig(t, Config{})
	_, _, code := r.run("test -f /nope && mkdir -p /never && printenv HOME")
	if code != 1 {
		t.Errorf("exit %d, want 1 (the failed test)", code)
	}
	if _, ok := r.p.Dirs()["/never"]; ok {
		t.Errorf("a command after a failure ran")
	}
	if n := len(r.p.Calls()); n != 1 {
		t.Errorf("%d calls logged, want 1: commands after a failure never begin", n)
	}
	// The exit code of the line is the last command's.
	out, _, code := r.run("mkdir -p /ok && printenv HOME")
	if code != 0 || out != "/home/example-user\n" {
		t.Errorf("chain of two: exit %d stdout %q", code, out)
	}
	if _, _, code := r.run("mkdir -p /ok2 && test -f /nope"); code != 1 {
		t.Errorf("chain ending in a failure: exit %d, want 1", code)
	}
}

func TestUnknownAndUnsupportedCommands(t *testing.T) {
	r := newRig(t, Config{})
	if _, errOut, code := r.run("ls /"); code != 127 || !strings.Contains(errOut, "command not found") {
		t.Errorf("ls: exit %d stderr %q, want exit 127 command not found", code, errOut)
	}
	for _, line := range []string{
		"rm -r /x", "rm /x", "mkdir /x", "mkdir -p", "mv /a /b", "chmod 755", "printenv", "printenv A B",
		"sha256sum", "sha256sum a b", "test", "umask 1 2",
	} {
		if _, _, code := r.run(line); code != 2 && code != 127 {
			t.Errorf("%q: exit %d, want a usage failure (2)", line, code)
		}
	}
	// A syntax error runs nothing and is not logged.
	if _, errOut, code := r.run("mkdir -p /x; mkdir -p /y"); code != 2 || !strings.Contains(errOut, "syntax error") {
		t.Errorf("syntax error: exit %d stderr %q, want exit 2", code, errOut)
	}
	if len(r.p.Dirs()) != 1 {
		t.Errorf("a line with a syntax error created directories: %v", r.p.Dirs())
	}
}

func TestFileSetupAPI(t *testing.T) {
	r := newRig(t, Config{})
	if err := r.p.WriteFile("/a/b/f", []byte("hello"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := r.p.WriteFile("relative", nil, 0o600); err == nil {
		t.Errorf("WriteFile of a relative path succeeded")
	}
	if err := r.p.WriteFile("/a/b", nil, 0o600); err == nil {
		t.Errorf("WriteFile over a directory succeeded")
	}
	if err := r.p.WriteFile("/a/b/f/g", nil, 0o600); err == nil {
		t.Errorf("WriteFile below a file succeeded")
	}
	// Files and Dirs return copies.
	files := r.p.Files()
	files["/a/b/f"].Content[0] = 'J'
	delete(files, "/a/b/f")
	if f := r.p.Files()["/a/b/f"]; string(f.Content) != "hello" || f.Mode != 0o640 {
		t.Errorf("the platform's file changed through the copy: %q mode %o", f.Content, f.Mode)
	}
	delete(r.p.Dirs(), "/a")
	if _, ok := r.p.Dirs()["/a"]; !ok {
		t.Errorf("the platform's directory set changed through the copy")
	}
	// Paths are cleaned.
	r.must("test -f /a/x/../b/f")
	if err := r.p.Remove("/a/b/f"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.p.Files()["/a/b/f"]; ok {
		t.Errorf("Remove left the file")
	}
	if err := r.p.Remove("/a/never"); err != nil {
		t.Errorf("Remove of a missing path: %v", err)
	}
}

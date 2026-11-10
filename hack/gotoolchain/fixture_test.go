package gotoolchain

// The fixtures of hack/fetch-go.sh's tests: a fake Go release on a file:// mirror, the pins and go.mod
// made for it, and the helpers that run the script and damage what it installed.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

const fakeVersion = "1.99.0"

// release is a fake Go release: the tarball on a file:// mirror, the pins and go.mod made for it, and
// the bytes of the files a test looks at afterwards.
type release struct {
	t       *testing.T
	mirror  string // directory the file:// mirror serves
	tarball string // its file name
	pins    string // the pins file
	gomod   string // the go.mod
	files   map[string][]byte
	tarSum  string
	goSum   string
}

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// fakeTree is what the fake tarball holds, by path inside it. go/bin/go is a script that reports the
// fake release, which is all the script and the check ask of it.
func fakeTree() (names []string, files map[string][]byte) {
	files = map[string][]byte{
		"go/VERSION":                      []byte("go" + fakeVersion + "\ntime 2026-01-01T00:00:00Z\n"),
		"go/bin/go":                       []byte("#!/bin/sh\necho 'go version go" + fakeVersion + " linux/amd64'\n"),
		"go/pkg/tool/linux_amd64/compile": []byte("fake compile, which the pin on go/bin/go does not cover\n"),
		"go/src/fmt/print.go":             []byte("package fmt\n"),
	}
	names = []string{"go/VERSION", "go/bin/go", "go/pkg/tool/linux_amd64/compile", "go/src/fmt/print.go"}
	return names, files
}

func buildTarball(t *testing.T, names []string, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	seen := map[string]bool{}
	var ordered []string
	for _, name := range names {
		for d := filepath.Dir(name); d != "."; d = filepath.Dir(d) {
			if !seen[d] {
				seen[d] = true
				ordered = append(ordered, d)
			}
		}
	}
	// Parents before children: a sort by path does it.
	sort.Strings(ordered)
	for _, d := range ordered {
		if err := tw.WriteHeader(&tar.Header{Name: d + "/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range names {
		mode := int64(0o644)
		if strings.Contains(name, "/bin/") || strings.HasSuffix(name, "/compile") {
			mode = 0o755
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: mode, Size: int64(len(files[name]))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(files[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func newRelease(t *testing.T) release {
	t.Helper()
	return newReleaseWith(t, nil)
}

// newReleaseWith is a release whose tarball and pins are built after mod has changed the files, for
// the cases where the verified tarball itself is wrong (another VERSION, a go that reports another
// release).
func newReleaseWith(t *testing.T, mod func(files map[string][]byte)) release {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("hack/fetch-go.sh pins linux_amd64 only")
	}
	root := t.TempDir()
	names, files := fakeTree()
	if mod != nil {
		mod(files)
	}
	tarBytes := buildTarball(t, names, files)
	r := release{
		t:       t,
		mirror:  filepath.Join(root, "mirror"),
		tarball: "go" + fakeVersion + ".linux-amd64.tar.gz",
		pins:    filepath.Join(root, "go-pins.txt"),
		gomod:   filepath.Join(root, "go.mod"),
		files:   files,
		tarSum:  sum(tarBytes),
		goSum:   sum(files["go/bin/go"]),
	}
	if err := os.MkdirAll(r.mirror, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(r.mirror, r.tarball), tarBytes, 0o644)
	r.writePins(r.tarSum, r.goSum)
	write(t, r.gomod, []byte("module example.com/m\n\ngo "+fakeVersion+"\n"), 0o644)
	return r
}

// tarballWith is this release's tarball rebuilt after mod has changed some files: a valid gzip and
// tar of the same names, so only its digest tells it from the pinned one. A byte flipped inside the
// real tarball breaks the gzip stream, which extraction would refuse even with no digest check.
func (r release) tarballWith(t *testing.T, mod func(files map[string][]byte)) []byte {
	t.Helper()
	names, _ := fakeTree()
	files := map[string][]byte{}
	for k, v := range r.files {
		files[k] = append([]byte(nil), v...)
	}
	mod(files)
	return buildTarball(t, names, files)
}

// damages are the two ways the tarball in a restored cache can be wrong: one byte changed, and a valid
// tarball with the same go/bin/go (so the pin on the binary agrees with it) and a different compiler.
var damages = []struct {
	name string
	do   func(t *testing.T, r release, path string)
}{
	{"one byte changed", func(t *testing.T, _ release, path string) { flip(t, path) }},
	{"a valid tarball with another compiler", func(t *testing.T, r release, path string) {
		write(t, path, r.tarballWith(t, func(f map[string][]byte) {
			f["go/pkg/tool/linux_amd64/compile"] = []byte("planted compile\n")
		}), 0o644)
	}},
}

func (r release) writePins(tarSum, goSum string) {
	r.t.Helper()
	line := fakeVersion + " amd64 " + r.tarball + " " + tarSum + " " + goSum + "\n"
	write(r.t, r.pins, []byte("# pins made for the fake release\n"+line), 0o644)
}

func write(t *testing.T, path string, b []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, b, mode); err != nil {
		t.Fatal(err)
	}
}

// live is the environment of a run against the mirror, dead of one whose site does not exist. The
// script's own variables are always set, so a developer's shell cannot reach the test.
func (r release) live() []string {
	return []string{"GO_RELEASE_BASE=file://" + r.mirror, "GO_PINS_FILE=" + r.pins, "GO_MOD_FILE=" + r.gomod}
}

func (r release) dead() []string {
	return []string{"GO_RELEASE_BASE=file://" + filepath.Join(r.mirror, "gone"), "GO_PINS_FILE=" + r.pins, "GO_MOD_FILE=" + r.gomod}
}

// run runs hack/fetch-go.sh with the arguments and returns its output and exit code.
func run(t *testing.T, env []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join("..", "fetch-go.sh")}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &ee):
		return string(out), ee.ExitCode()
	default:
		t.Fatalf("running hack/fetch-go.sh: %v", err)
		return "", -1
	}
}

// dirs is the pair of directories CI gives the script: where the tarball is downloaded and cached, and
// where the tree is installed.
type dirs struct{ dl, inst string }

func newDirs(t *testing.T) dirs {
	t.Helper()
	root := t.TempDir()
	return dirs{dl: filepath.Join(root, "go-download"), inst: filepath.Join(root, "go-toolchain")}
}

func (d dirs) install(t *testing.T, env []string) (string, int) {
	t.Helper()
	return run(t, env, d.dl, d.inst)
}

// names lists a directory's entries, none when it does not exist.
func names(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

// flip changes one byte in the middle of a file, keeping its length and mode: a cache entry that is
// damaged, or planted, and still looks like the right file.
func flip(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		t.Fatalf("reading %s: %v", path, err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, path, b, 0o755)
	if err := os.Chmod(path, fi.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
}

// expectTree: INSTALL_DIR/go holds exactly the bytes of the release, with the modes of the tarball.
func (r release) expectTree(t *testing.T, inst string) {
	t.Helper()
	for name, want := range r.files {
		p := filepath.Join(inst, name)
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s holds %q (%v), want %q", p, got, err, want)
		}
	}
	if fi, err := os.Stat(filepath.Join(inst, "go/bin/go")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("go/bin/go: %v, %v; want mode 755", fi, err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// toolsOnly makes a directory holding a link to each named tool, so that a PATH made of it has those
// tools and no go.
func toolsOnly(t *testing.T, tools ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, tool := range tools {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("no %s on this machine: %v", tool, err)
		}
		if err := os.Symlink(p, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

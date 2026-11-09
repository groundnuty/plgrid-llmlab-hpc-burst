package testenv

// CI restores the directory hack/fetch-frp.sh installs into from a cache, so that a GitHub outage does
// not fail every job (issue #290). The script treats that directory as untrusted: frps and frpc are
// checked against their pins on every run, whatever put them there. These tests run offline. The
// release is a file:// mirror of fake tarballs (FRP_RELEASE_BASE), a dead mirror is a file:// path
// that does not exist, and a pins file made for the fake tarballs replaces the real sums
// (FRP_PINS_FILE, read by the default mode too). The sums in that file come from crypto/sha256 over
// the bytes the test built, and the script computes its own with sha256sum, so neither checks itself.

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// frpCacheFixture is a fake release, the directory a first run filled from it (the "cache" that CI
// would save), and the environment of a run against a dead mirror.
type frpCacheFixture struct {
	r    fakeRelease
	dest string
	dead []string // the environment for a run whose release site does not exist
	live []string
}

func newFrpCacheFixture(t *testing.T) frpCacheFixture {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("hack/fetch-frp.sh pins linux_amd64 only")
	}
	r := newFakeRelease(t, "amd64")
	f := frpCacheFixture{
		r:    r,
		dest: filepath.Join(t.TempDir(), "frp"),
		// FRP_TARBALL= in both: the script prefers a tarball named there to any release site, and a
		// developer's shell may have one, whose digest is not the fake one.
		live: append(r.env(), "FRP_TARBALL="),
		dead: []string{"FRP_RELEASE_BASE=file://" + filepath.Join(r.base, "gone"), "FRP_PINS_FILE=" + r.pins, "FRP_TARBALL="},
	}
	out, code := runAgentBuilds(t, []string{f.dest}, f.live...)
	if code != 0 || !strings.Contains(out, "verified frp 0.71.1") {
		t.Fatalf("filling the directory from the live mirror: exit %d:\n%s", code, out)
	}
	f.expectInstalled(t)
	return f
}

// expectInstalled: both files are the ones the pinned tarball holds, executable.
func (f frpCacheFixture) expectInstalled(t *testing.T) {
	t.Helper()
	for name, want := range map[string][]byte{"frps": f.r.frps["amd64"], "frpc": f.r.frpc["amd64"]} {
		p := filepath.Join(f.dest, name)
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s holds %q (%v), want %q", p, got, err, want)
		}
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o755 {
			t.Errorf("%s: %v, %v; want mode 755", p, fi, err)
		}
	}
}

// flip changes one byte of a file, keeping its length and mode: a cache entry that is damaged, or
// planted, and still looks like the right file.
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
	b[0] ^= 0xff
	if err := os.Chmod(path, 0o755); err != nil { // the script installs some files read-only
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, fi.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
}

// A warm directory passes with the release site dead, and the twin shows that is the check at work:
// the same directory with one byte of frpc changed is refused, because the script verified what it
// found instead of trusting that it was there.
func TestFetchFrpWarmDirectoryNeedsNoDownload(t *testing.T) {
	f := newFrpCacheFixture(t)

	out, code := runAgentBuilds(t, []string{f.dest}, f.dead...)
	if code != 0 || !strings.Contains(out, "ok       frp 0.71.1") || strings.Contains(out, "fetching") {
		t.Fatalf("warm directory, dead site: exit %d, want 0 with an ok line and no fetch:\n%s", code, out)
	}
	f.expectInstalled(t)

	flip(t, filepath.Join(f.dest, "frpc"))
	out, code = runAgentBuilds(t, []string{f.dest}, f.dead...)
	if code == 0 || strings.Contains(out, "ok       frp") {
		t.Fatalf("a changed frpc was accepted from the warm directory: exit %d:\n%s", code, out)
	}
	if !strings.Contains(out, "frp_0.71.1_linux_amd64.tar.gz") || !strings.Contains(out, "failed") {
		t.Errorf("the failure does not name the file it could not download:\n%s", out)
	}
}

// A file with the wrong digest is refused and downloaded again, each of the two on its own, and the
// directory ends holding the pinned bytes.
func TestFetchFrpDownloadsAgainWhatDoesNotMatchItsPin(t *testing.T) {
	for _, name := range []string{"frps", "frpc"} {
		t.Run(name, func(t *testing.T) {
			f := newFrpCacheFixture(t)
			flip(t, filepath.Join(f.dest, name))
			out, code := runAgentBuilds(t, []string{f.dest}, f.live...)
			if code != 0 || !strings.Contains(out, "fetching frp 0.71.1") || !strings.Contains(out, "verified frp 0.71.1") {
				t.Fatalf("exit %d, want 0 after a fetch and a verification:\n%s", code, out)
			}
			f.expectInstalled(t)
		})
	}
}

// With nothing in the directory and the release site dead, the script fails and says which file it
// could not download; it leaves no file behind.
func TestFetchFrpMissWithADeadSiteFailsNamingTheFile(t *testing.T) {
	f := newFrpCacheFixture(t)
	empty := filepath.Join(t.TempDir(), "frp")
	out, code := runAgentBuilds(t, []string{empty}, f.dead...)
	if code != 1 || !strings.Contains(out, "download of ") || !strings.Contains(out, "frp_0.71.1_linux_amd64.tar.gz") || !strings.Contains(out, "failed") {
		t.Fatalf("exit %d, want 1 naming frp_0.71.1_linux_amd64.tar.gz:\n%s", code, out)
	}
	if got := dirNames(t, empty); len(got) != 0 {
		t.Errorf("the directory holds %v, want it empty", got)
	}
}

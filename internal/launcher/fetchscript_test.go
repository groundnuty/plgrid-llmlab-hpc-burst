package launcher

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The download itself is not tested here (it needs the network and 80 MB); the checksum logic is,
// against a local file:// release site and a pins file of the test's own. The expected hash is
// the published SHA-256 test vector for the three bytes "abc" (FIPS 180-2).
const abcSum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func runFetch(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join("..", "..", "hack", "fetch-k3s.sh")}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	return so.String(), se.String(), err
}

// k3sSite is a file:// release site holding one pinned version of k3s for this machine's
// architecture, a pins file of the test's own that names it (abcSum is the sum of "abc"), and a
// destination directory that does not exist yet.
type k3sSite struct {
	env          []string // K3S_RELEASE_BASE and K3S_PINS_FILE
	site, rel    string   // the site's directory, and the version's directory in it
	asset        string   // the release asset's name
	dest, target string   // the directory a script installs into, and the file it installs
	pinsFile     string
}

func newK3sSite(t *testing.T) k3sSite {
	t.Helper()
	if _, err := exec.LookPath("curl"); err != nil {
		skipOrFail(t, "curl is not installed")
	}
	site := t.TempDir()
	rel := filepath.Join(site, "v1.36.5+k3s1")
	if err := os.MkdirAll(rel, 0o755); err != nil {
		t.Fatal(err)
	}
	arch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}
	out, _ := exec.Command("uname", "-m").Output()
	a := arch[strings.TrimSpace(string(out))]
	if a == "" {
		skipOrFail(t, "unsupported architecture "+string(out))
	}
	asset := "k3s"
	if a == "arm64" {
		asset = "k3s-arm64"
	}
	pins := filepath.Join(t.TempDir(), "pins.txt")
	if err := os.WriteFile(pins, []byte("v1.36.5+k3s1 "+a+" "+asset+" "+abcSum+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "bin")
	return k3sSite{
		env:  []string{"K3S_RELEASE_BASE=file://" + site, "K3S_PINS_FILE=" + pins},
		site: site, rel: rel, asset: asset, dest: dest, target: filepath.Join(dest, "k3s-1.36.5", "k3s"),
		pinsFile: pins,
	}
}

func TestFetchScript(t *testing.T) {
	s := newK3sSite(t)
	site, rel, asset, env, dest, target := s.site, s.rel, s.asset, s.env, s.dest, s.target

	// A download with the wrong hash is refused and leaves nothing behind.
	if err := os.WriteFile(filepath.Join(rel, asset), []byte("bogus"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, se, err := runFetch(t, env, dest)
	if err == nil || !strings.Contains(se, "checksum mismatch") {
		t.Fatalf("a wrong download was accepted: %v, %q", err, se)
	}
	ents, _ := os.ReadDir(filepath.Join(dest, "k3s-1.36.5"))
	if len(ents) != 0 {
		t.Errorf("a failed download left files behind: %v", ents)
	}

	// The right bytes are installed, executable.
	if err := os.WriteFile(filepath.Join(rel, asset), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	so, se, err := runFetch(t, env, dest, "1.36.5")
	if err != nil || !strings.Contains(so, "verified") {
		t.Fatalf("fetch: %v, %q, %q", err, so, se)
	}
	fi, err := os.Stat(target)
	if err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("installed file: %v, %v", fi, err)
	}

	// A second run finds the right file and downloads nothing: remove the site to prove it.
	os.RemoveAll(site)
	so, se, err = runFetch(t, env, dest)
	if err != nil || !strings.Contains(so, "ok ") {
		t.Fatalf("second run: %v, %q, %q", err, so, se)
	}

	// A file in place with the wrong content is replaced only by a verified download; with the
	// site gone the script fails instead of keeping the bad file as if it were good.
	if err := os.WriteFile(target, []byte("tampered"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runFetch(t, env, dest); err == nil {
		t.Error("a tampered file in place was accepted")
	}
	// An unknown version matches no pin.
	if _, se, err := runFetch(t, env, dest, "9.9.9"); err == nil || !strings.Contains(se, "no pinned version matches") {
		t.Errorf("unknown version: %v, %q", err, se)
	}
}

// CI restores the install directory from a cache, so that a GitHub outage does not fail every job
// (issue #290). The script treats that directory as untrusted: each file is checked against its pin on
// every run, whatever put it there. A dead site is a file:// path that does not exist.
func TestFetchScriptWarmCache(t *testing.T) {
	s := newK3sSite(t)
	if err := os.WriteFile(filepath.Join(s.rel, s.asset), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	dead := []string{"K3S_RELEASE_BASE=file://" + filepath.Join(s.site, "gone"), "K3S_PINS_FILE=" + s.pinsFile}
	installed := func(want string) {
		t.Helper()
		got, err := os.ReadFile(s.target)
		fi, serr := os.Stat(s.target)
		if err != nil || string(got) != want || serr != nil || fi.Mode().Perm() != 0o755 {
			t.Errorf("%s holds %q (%v), mode %v (%v); want %q, mode 755", s.target, got, err, fi, serr, want)
		}
	}

	// The first run fills the directory from the live site, as the first CI job does.
	if so, se, err := runFetch(t, s.env, s.dest, "1.36.5"); err != nil || !strings.Contains(so, "verified") {
		t.Fatalf("filling the directory: %v, %q, %q", err, so, se)
	}
	installed("abc")

	// A warm directory passes with the site dead and downloads nothing.
	so, se, err := runFetch(t, dead, s.dest, "1.36.5")
	if err != nil || !strings.Contains(so, "ok ") || strings.Contains(so, "fetching") {
		t.Fatalf("warm directory, dead site: %v, want an ok line and no fetch:\n%q\n%q", err, so, se)
	}

	// The twin: the same directory with the file changed is refused, because the script checked what it
	// found instead of trusting that it was there. The failure names the download it could not do.
	if err := os.WriteFile(s.target, []byte("abd"), 0o755); err != nil {
		t.Fatal(err)
	}
	so, se, err = runFetch(t, dead, s.dest, "1.36.5")
	if err == nil || strings.Contains(so, "ok ") {
		t.Fatalf("a changed file was accepted from the warm directory: %v, %q, %q", err, so, se)
	}
	if !strings.Contains(se, "download of ") || !strings.Contains(se, "v1.36.5%2Bk3s1/"+s.asset) || !strings.Contains(se, "failed") {
		t.Errorf("the failure does not name the file it could not download:\n%s", se)
	}

	// With the site up, the same file is refused and downloaded again.
	so, se, err = runFetch(t, s.env, s.dest, "1.36.5")
	if err != nil || !strings.Contains(so, "fetching") || !strings.Contains(so, "verified") {
		t.Fatalf("a changed file with the site up: %v, want a fetch and a verification:\n%q\n%q", err, so, se)
	}
	installed("abc")

	// A miss with the site dead fails the same way and leaves nothing behind.
	if err := os.Remove(s.target); err != nil {
		t.Fatal(err)
	}
	so, se, err = runFetch(t, dead, s.dest, "1.36.5")
	if err == nil || !strings.Contains(se, "download of ") || !strings.Contains(se, s.asset) {
		t.Fatalf("a miss with the site dead: %v, want a failure naming %s:\n%q\n%q", err, s.asset, so, se)
	}
	if ents, _ := os.ReadDir(filepath.Dir(s.target)); len(ents) != 0 {
		t.Errorf("the failed download left files behind: %v", ents)
	}
}

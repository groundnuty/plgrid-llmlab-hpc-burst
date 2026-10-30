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

func TestFetchScript(t *testing.T) {
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
	env := []string{"K3S_RELEASE_BASE=file://" + site, "K3S_PINS_FILE=" + pins}
	dest := filepath.Join(t.TempDir(), "bin")
	target := filepath.Join(dest, "k3s-1.36.5", "k3s")

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

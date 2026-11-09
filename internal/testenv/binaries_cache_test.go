package testenv

// CI restores hack/test-binaries.sh's cache directory from a cache, so that a GitHub or dl.k8s.io
// outage does not fail every job (issue #290). The script treats it as untrusted: each file is checked
// against its pin on every run. These tests run offline against fake binaries. The release sites are
// file:// mirrors (K8S_RELEASE_BASE, ETCD_RELEASE_BASE), a dead site is a file:// path that does not
// exist, and a pins file made for the fakes replaces the real sums (TEST_BINARIES_PINS_FILE). Those
// sums come from crypto/sha256 over the bytes the test built; the script computes its own with
// sha256sum, so neither side checks itself.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

type k8sMirror struct {
	k8sBase, etcdBase, pins string
	apiserver, kcm, etcd    []byte
	k8sVersion, etcdVersion string
}

// scriptVar reads a NAME=value line of hack/test-binaries.sh, so that the fake mirror follows a
// version bump of the script instead of being a second copy of it.
func scriptVar(t *testing.T, root, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "hack", "test-binaries.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^` + name + `=(\S+)$`).FindSubmatch(b)
	if m == nil {
		t.Fatalf("hack/test-binaries.sh has no line %s=...", name)
	}
	return string(m[1])
}

func newK8sMirror(t *testing.T, root string) k8sMirror {
	t.Helper()
	m := k8sMirror{
		k8sBase: t.TempDir(), etcdBase: t.TempDir(),
		apiserver:  []byte("#!/bin/sh\necho kube-apiserver\n"),
		kcm:        []byte("#!/bin/sh\necho kube-controller-manager\n"),
		etcd:       []byte("#!/bin/sh\necho etcd\n"),
		k8sVersion: scriptVar(t, root, "K8S_VERSION"), etcdVersion: scriptVar(t, root, "ETCD_VERSION"),
	}
	bin := filepath.Join(m.k8sBase, "v"+m.k8sVersion, "bin", "linux", "amd64")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"kube-apiserver": m.apiserver, "kube-controller-manager": m.kcm} {
		if err := os.WriteFile(filepath.Join(bin, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	dir := "etcd-v" + m.etcdVersion + "-linux-amd64"
	if err := tw.WriteHeader(&tar.Header{Name: dir + "/etcd", Mode: 0o755, Size: int64(len(m.etcd))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(m.etcd); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	rel := filepath.Join(m.etcdBase, "v"+m.etcdVersion)
	if err := os.MkdirAll(rel, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rel, dir+".tar.gz"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	m.pins = filepath.Join(t.TempDir(), "pins")
	pins := "# pins for the fake binaries\n" +
		"SHA_KUBE_APISERVER=" + sumHex(m.apiserver) + "\n" +
		"SHA_KUBE_CONTROLLER_MANAGER=" + sumHex(m.kcm) + "\n" +
		"SHA_ETCD_TARBALL=" + sumHex(buf.Bytes()) + "\n" +
		"SHA_ETCD=" + sumHex(m.etcd) + "\n"
	if err := os.WriteFile(m.pins, []byte(pins), 0o644); err != nil {
		t.Fatal(err)
	}
	return m
}

// k8sRun is one run of the script. Environment entries come last and win; the two variables that
// name binaries are removed unless a run sets them, since a developer's shell may have them.
type k8sRun struct {
	root, cache string
	m           k8sMirror
}

func (r k8sRun) assets() string {
	return filepath.Join(r.cache, "envtest", "k8s", r.m.k8sVersion+"-linux-amd64")
}
func (r k8sRun) kcmPath() string {
	return filepath.Join(r.cache, "bin", "kube-controller-manager-"+r.m.k8sVersion)
}

func (r k8sRun) live() []string {
	return []string{"K8S_RELEASE_BASE=file://" + r.m.k8sBase, "ETCD_RELEASE_BASE=file://" + r.m.etcdBase}
}

func (r k8sRun) dead() []string {
	gone := filepath.Join(r.cache, "no-such-mirror")
	return []string{"K8S_RELEASE_BASE=file://" + gone, "ETCD_RELEASE_BASE=file://" + gone}
}

func (r k8sRun) run(t *testing.T, env ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(r.root, "hack", "test-binaries.sh"))
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "HPCBURST_ENVTEST_ASSETS=") && !strings.HasPrefix(e, "HPCBURST_KUBE_CONTROLLER_MANAGER=") {
			cmd.Env = append(cmd.Env, e)
		}
	}
	cmd.Env = append(cmd.Env, "HPCBURST_TOOLS_CACHE="+r.cache, "TEST_BINARIES_PINS_FILE="+r.m.pins)
	cmd.Env = append(cmd.Env, env...)
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return so.String(), se.String(), ee.ExitCode()
	}
	if err != nil {
		t.Fatalf("running hack/test-binaries.sh: %v", err)
	}
	return so.String(), se.String(), 0
}

// newK8sRun makes a fake mirror and fills the cache directory from it, as the first run of a CI job
// does; that run is also the cold-cache case.
func newK8sRun(t *testing.T) k8sRun {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("hack/test-binaries.sh needs a GNU userland")
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	r := k8sRun{root: root, cache: filepath.Join(t.TempDir(), "cache"), m: newK8sMirror(t, root)}
	so, se, code := r.run(t, r.live()...)
	if code != 0 || !strings.Contains(se, "downloading") {
		t.Fatalf("the first run, from the live mirror: exit %d, stdout %q, stderr:\n%s", code, so, se)
	}
	if !strings.Contains(so, "HPCBURST_ENVTEST_ASSETS="+r.assets()+"\n") || !strings.Contains(so, "HPCBURST_KUBE_CONTROLLER_MANAGER="+r.kcmPath()+"\n") {
		t.Fatalf("the script printed %q, want the two KEY=VALUE lines for %s and %s", so, r.assets(), r.kcmPath())
	}
	r.expectInstalled(t)
	return r
}

// expectInstalled: the three files hold the bytes the mirror served, read-only and executable.
func (r k8sRun) expectInstalled(t *testing.T) {
	t.Helper()
	for p, want := range map[string][]byte{
		filepath.Join(r.assets(), "kube-apiserver"): r.m.apiserver,
		filepath.Join(r.assets(), "etcd"):           r.m.etcd,
		r.kcmPath():                                 r.m.kcm,
	} {
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s holds %q (%v), want %q", p, got, err, want)
		}
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o555 {
			t.Errorf("%s: %v, %v; want mode 555", p, fi, err)
		}
	}
}

// files maps a name to its path in the cache directory and the name the failed download carries.
func (r k8sRun) files() map[string][2]string {
	return map[string][2]string{
		"kube-apiserver":          {filepath.Join(r.assets(), "kube-apiserver"), "kube-apiserver"},
		"kube-controller-manager": {r.kcmPath(), "kube-controller-manager"},
		"etcd":                    {filepath.Join(r.assets(), "etcd"), "etcd-v" + r.m.etcdVersion + "-linux-amd64.tar.gz"},
	}
}

// A warm cache passes with both release sites dead and downloads nothing. The twin: the same cache
// with one byte of a file changed is refused for each of the three files, with nothing printed for
// a test to use, so the pass above was the check at work and not a cache hit that skipped it.
func TestBinariesScriptWarmCacheNeedsNoDownload(t *testing.T) {
	r := newK8sRun(t)
	so, se, code := r.run(t, r.dead()...)
	if code != 0 || !strings.Contains(se, "ok       the pinned") || strings.Contains(se, "downloading") {
		t.Fatalf("warm cache, dead sites: exit %d, want 0 with an ok line and no download:\nstdout %q\nstderr:\n%s", code, so, se)
	}
	if !strings.Contains(so, "HPCBURST_ENVTEST_ASSETS="+r.assets()) {
		t.Errorf("stdout %q has no HPCBURST_ENVTEST_ASSETS line", so)
	}

	for name, f := range r.files() {
		t.Run("changed "+name, func(t *testing.T) {
			r := newK8sRun(t)
			p := r.files()[name][0]
			flip(t, p)
			so, se, code := r.run(t, r.dead()...)
			if code == 0 || so != "" {
				t.Fatalf("a changed %s was accepted from the warm cache: exit %d, stdout %q\n%s", name, code, so, se)
			}
			if !strings.Contains(se, "refusing") || !strings.Contains(se, "download of ") || !strings.Contains(se, f[1]) || !strings.Contains(se, "failed") {
				t.Errorf("stderr does not say the file was refused and name %s as the download that failed:\n%s", f[1], se)
			}
		})
	}
}

// A cached file with the wrong digest is refused and downloaded again, each of the three, and the
// cache ends holding the pinned bytes.
func TestBinariesScriptDownloadsAgainWhatDoesNotMatchItsPin(t *testing.T) {
	for _, name := range []string{"kube-apiserver", "kube-controller-manager", "etcd"} {
		t.Run(name, func(t *testing.T) {
			r := newK8sRun(t)
			p := r.files()[name][0]
			flip(t, p)
			so, se, code := r.run(t, r.live()...)
			if code != 0 || !strings.Contains(se, "refusing") || !strings.Contains(se, "downloading") || !strings.Contains(so, "HPCBURST_ENVTEST_ASSETS=") {
				t.Fatalf("exit %d, want 0 after refusing %s and downloading it again:\nstdout %q\nstderr:\n%s", code, name, so, se)
			}
			r.expectInstalled(t)
		})
	}
}

// With a file missing and its release site dead, the script fails and names the download, for each
// of the three files; nothing is printed for a test to use.
func TestBinariesScriptMissWithADeadSiteFailsNamingTheFile(t *testing.T) {
	for _, name := range []string{"kube-apiserver", "kube-controller-manager", "etcd"} {
		t.Run(name, func(t *testing.T) {
			r := newK8sRun(t)
			p, want := r.files()[name][0], r.files()[name][1]
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			so, se, code := r.run(t, r.dead()...)
			if code != 1 || so != "" || !strings.Contains(se, "download of ") || !strings.Contains(se, want) || !strings.Contains(se, "failed") {
				t.Fatalf("exit %d, want 1, no stdout, and a failed download of %s:\nstdout %q\nstderr:\n%s", code, want, so, se)
			}
		})
	}
}

// A file the environment names is not the script's to replace: with another digest it stays an
// error even when the release site is up, and the file is left as it was.
func TestBinariesScriptNeverReplacesAFileTheEnvironmentNames(t *testing.T) {
	r := newK8sRun(t)
	assets := t.TempDir()
	bad := []byte("#!/bin/sh\nexit 0\n")
	for _, name := range []string{"kube-apiserver", "etcd"} {
		if err := os.WriteFile(filepath.Join(assets, name), bad, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	env := append(r.live(), "HPCBURST_ENVTEST_ASSETS="+assets, "HPCBURST_KUBE_CONTROLLER_MANAGER="+r.kcmPath())
	so, se, code := r.run(t, env...)
	if code == 0 || so != "" || !strings.Contains(se, "sha256") || strings.Contains(se, "downloading") {
		t.Fatalf("exit %d, want a refusal with no download:\nstdout %q\nstderr:\n%s", code, so, se)
	}
	for _, name := range []string{"kube-apiserver", "etcd"} {
		if got, err := os.ReadFile(filepath.Join(assets, name)); err != nil || !bytes.Equal(got, bad) {
			t.Errorf("%s was changed to %q (%v), want it untouched", name, got, err)
		}
	}
}

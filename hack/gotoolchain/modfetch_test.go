package gotoolchain

// Tests of hack/go-mod-download.sh and of the GOPROXY the workflow sets (issue #346). CI caches no Go
// modules (issue #290), so a proxy error used to fail a job; the workflow now lets the go command fall
// through from the proxy to the module's origin on ANY error ("|"), and the script retries a bounded
// number of times and runs go mod verify.
//
// Each test runs the real script, with the real go command, the real git, and the GOPROXY the workflow
// itself holds (read from ci.yml, with the proxy's host replaced by a stand-in on a kernel-chosen
// loopback port). Nothing here reaches the internet:
//   - the proxy is a stand-in that answers 500, or resets an HTTP/2 stream in the middle of a zip, or
//     serves the module, as each test says;
//   - "direct" is the go command's own: the module path is a github.com one, so it runs git against
//     https://github.com/..., and a git config in the environment (url.<base>.insteadOf) sends that to a
//     repository on disk, or to a directory that does not exist when a test needs direct to be dead.
// The fixture's go.sum comes from a run through direct, and the stand-in's zip is built by archive/zip
// from the same files, so a proxy-served download passes only if the two independent constructions agree
// on the module's hash. GOSUMDB stays at its default: go.sum holds every hash, so no lookup is made.

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

const (
	fixMod     = "github.com/example-owner/fixture"
	fixVersion = "v1.0.0"
	fixGoMod   = "module " + fixMod + "\n\ngo 1.26.0\n"
	fixSource  = "package fixture\n\nconst Answer = 42\n"
	// wantGoproxy is what the workflow must set: the proxy first, then the module's own origin, joined
	// by a pipe. It is written here, not taken from the workflow.
	wantGoproxy = "https://proxy.golang.org|direct"
	proxyHost   = "https://proxy.golang.org"
)

// workflowEnv is the workflow's own top-level env.
func workflowEnv(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Env map[string]string `json:"env"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatalf("ci.yml is not valid YAML: %v", err)
	}
	return doc.Env
}

// ciGoproxy is the GOPROXY the workflow sets, with the proxy's host replaced by base. It fails when the
// value has no proxy host to replace, since a test of a different value would test nothing.
func ciGoproxy(t *testing.T, base string) string {
	t.Helper()
	v := workflowEnv(t)["GOPROXY"]
	if !strings.Contains(v, proxyHost) {
		t.Fatalf("the workflow's GOPROXY is %q, which does not name %s", v, proxyHost)
	}
	return strings.Replace(v, proxyHost, base, 1)
}

type behaviour int

const (
	serve       behaviour = iota // answer with the file
	answer500                    // HTTP 500
	resetStream                  // 200, half of the body, then an abort: RST_STREAM with INTERNAL_ERROR on HTTP/2
)

// proxyStandIn is a GOPROXY: the files of one module, and a rule that says how each request is answered.
type proxyStandIn struct {
	srv   *httptest.Server
	cert  string // the PEM file to trust, when the server speaks TLS
	files map[string][]byte
	rule  func(path string, nth int) behaviour // nth: how many requests for this path came before

	mu   sync.Mutex
	hits map[string]int
}

func (p *proxyStandIn) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	n := p.hits[r.URL.Path]
	p.hits[r.URL.Path]++
	p.mu.Unlock()
	body, known := p.files[r.URL.Path]
	switch p.rule(r.URL.Path, n) {
	case answer500:
		http.Error(w, "stand-in: internal server error", http.StatusInternalServerError)
	case resetStream:
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+4096))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	default:
		if !known {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}
}

// count is how many requests came for paths that end in suffix.
func (p *proxyStandIn) count(suffix string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for path, c := range p.hits {
		if strings.HasSuffix(path, suffix) {
			n += c
		}
	}
	return n
}

func newProxy(t *testing.T, useTLS bool, files map[string][]byte, rule func(path string, nth int) behaviour) *proxyStandIn {
	t.Helper()
	p := &proxyStandIn{files: files, rule: rule, hits: map[string]int{}}
	p.srv = httptest.NewUnstartedServer(p)
	p.srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	if useTLS {
		p.srv.EnableHTTP2 = true
		p.srv.StartTLS()
		p.cert = filepath.Join(t.TempDir(), "stand-in.pem")
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.srv.Certificate().Raw})
		if err := os.WriteFile(p.cert, pemBytes, 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		p.srv.Start()
	}
	t.Cleanup(p.srv.Close)
	return p
}

// moduleFiles is what a GOPROXY serves for the fixture: the .info, the .mod and the zip, built here, not
// by the go command.
func moduleFiles(t *testing.T, source string) map[string][]byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range []struct{ name, body string }{{"go.mod", fixGoMod}, {"fixture.go", source}} {
		w, err := zw.Create(fixMod + "@" + fixVersion + "/" + f.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(f.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	base := "/" + fixMod + "/@v/" + fixVersion
	return map[string][]byte{
		base + ".info": []byte(`{"Version":"` + fixVersion + `","Time":"2026-01-01T00:00:00Z"}`),
		base + ".mod":  []byte(fixGoMod),
		base + ".zip":  buf.Bytes(),
	}
}

const (
	modPath = "/" + fixMod + "/@v/" + fixVersion + ".mod"
	zipPath = "/" + fixMod + "/@v/" + fixVersion + ".zip"
)

// modRig is a module that requires the fixture, with its go.sum, and a git origin for the fixture.
type modRig struct {
	t        *testing.T
	root     string
	consumer string // the module the script downloads (GO_MOD_DIR)
	origin   string // what https://github.com/ becomes for git: <origin>/example-owner/fixture.git
	goBin    string
	cacheN   int
}

func newModRig(t *testing.T) *modRig {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("the go command is not on PATH: %v", err)
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Fatalf("git is not on PATH, and the go command's direct mode needs it: %v", err)
	}
	r := &modRig{t: t, root: t.TempDir(), goBin: goBin}
	r.origin = r.makeOrigin("origin", fixSource)
	r.consumer = filepath.Join(r.root, "consumer")
	if err := os.MkdirAll(r.consumer, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(r.consumer, "go.mod"), []byte("module example.invalid/consumer\n\ngo 1.26.0\n\nrequire "+fixMod+" "+fixVersion+"\n"), 0o644)
	write(t, filepath.Join(r.consumer, "consumer.go"), []byte("package consumer\n\nimport _ \""+fixMod+"\"\n"), 0o644)
	// go.sum is made through direct, with the checksum database off, once; every run under test then
	// checks against it, with the database at its default.
	env := r.env(r.root, "direct", r.origin, "GOSUMDB=off")
	cmd := exec.Command(goBin, "mod", "tidy")
	cmd.Dir = r.consumer
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("seeding go.sum: %v\n%s", err, out)
	}
	sums := string(mustRead(t, filepath.Join(r.consumer, "go.sum")))
	for _, want := range []string{fixMod + " " + fixVersion + " h1:", fixMod + " " + fixVersion + "/go.mod h1:"} {
		if !strings.Contains(sums, want) {
			t.Fatalf("go.sum lacks a %q line, so a run under test would reach for the checksum database:\n%s", want, sums)
		}
	}
	return r
}

// gitEnv is a git that reads no configuration of the machine it runs on.
func gitEnv(home string, extra ...string) []string {
	return append([]string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + home,
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
	}, extra...)
}

func (r *modRig) git(dir string, args ...string) {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = gitEnv(r.root)
	if out, err := cmd.CombinedOutput(); err != nil {
		r.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// makeOrigin makes <name>/example-owner/fixture.git, a bare repository whose tag v1.0.0 holds the
// fixture with this source, and returns <name>'s path.
func (r *modRig) makeOrigin(name, source string) string {
	r.t.Helper()
	base := filepath.Join(r.root, name)
	work := filepath.Join(r.root, "work-"+name)
	for _, d := range []string{work, filepath.Join(base, "example-owner")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			r.t.Fatal(err)
		}
	}
	r.git(work, "init", "-q", "-b", "main")
	write(r.t, filepath.Join(work, "go.mod"), []byte(fixGoMod), 0o644)
	write(r.t, filepath.Join(work, "fixture.go"), []byte(source), 0o644)
	r.git(work, "add", "-A")
	r.git(work, "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "fixture")
	r.git(work, "tag", fixVersion)
	r.git(work, "clone", "-q", "--bare", work, filepath.Join(base, "example-owner", "fixture.git"))
	return base
}

// env is the environment of one go command (or of the script): its own caches under dir, the GOPROXY it
// is given, and git sent to origin for every https://github.com/ URL. An origin that is not a directory
// makes direct dead. GOFLAGS is only -modcacherw (CI sets none), so the temp dirs can be removed; the
// checksum variables are not set unless extra does.
func (r *modRig) env(dir, goproxy, origin string, extra ...string) []string {
	r.t.Helper()
	r.cacheN++
	dir = filepath.Join(dir, "run"+strconv.Itoa(r.cacheN))
	for _, d := range []string{"home", "gopath", "mod", "cache", "tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			r.t.Fatal(err)
		}
	}
	// The go command's telemetry starts a detached child that writes under $HOME/.config/go/telemetry
	// after the command has exited, which raced the removal of this directory (a flake in 14 of 40
	// runs of one test: "directory not empty"). The mode file is where `go telemetry off` records its
	// choice, and GOENV=off below would hide go env -w's.
	telemetry := filepath.Join(dir, "home", ".config", "go", "telemetry")
	if err := os.MkdirAll(telemetry, 0o755); err != nil {
		r.t.Fatal(err)
	}
	write(r.t, filepath.Join(telemetry, "mode"), []byte("off\n"), 0o644)
	env := gitEnv(filepath.Join(dir, "home"),
		"GOPROXY="+goproxy,
		"GOENV=off", "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-modcacherw",
		"GOPATH="+filepath.Join(dir, "gopath"), "GOMODCACHE="+filepath.Join(dir, "mod"), "GOCACHE="+filepath.Join(dir, "cache"),
		"TMPDIR="+filepath.Join(dir, "tmp"),
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=url.file://"+origin+"/.insteadOf", "GIT_CONFIG_VALUE_0=https://github.com/",
	)
	if gr := os.Getenv("GOROOT"); gr != "" {
		env = append(env, "GOROOT="+gr)
	}
	return append(env, extra...)
}

// modCache is where a run's modules went.
func modCache(env []string) string {
	for _, e := range env {
		if v, ok := strings.CutPrefix(e, "GOMODCACHE="); ok {
			return v
		}
	}
	return ""
}

func cached(env []string) bool {
	_, err := os.Stat(filepath.Join(modCache(env), fixMod+"@"+fixVersion, "fixture.go"))
	return err == nil
}

// runScript runs hack/go-mod-download.sh on the fixture module and returns its output and exit code.
func (r *modRig) runScript(env []string, extra ...string) (string, int) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join("..", "go-mod-download.sh"))
	cmd.Env = append(append(env, "GO_MOD_DIR="+r.consumer, "GO_MOD_DOWNLOAD_PAUSE=0"), extra...)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &ee):
		return string(out), ee.ExitCode()
	default:
		r.t.Fatalf("running hack/go-mod-download.sh: %v\n%s", err, out)
		return "", -1
	}
}

// deadOrigin is a directory that does not exist, so git finds no repository for any github.com URL.
func (r *modRig) deadOrigin() string { return filepath.Join(r.root, "no-such-origin") }

func (r *modRig) liveEnv(goproxy string, extra ...string) []string {
	return r.env(r.root, goproxy, r.origin, extra...)
}

func (r *modRig) deadEnv(goproxy string, extra ...string) []string {
	return r.env(r.root, goproxy, r.deadOrigin(), extra...)
}

func always(b behaviour) func(string, int) behaviour {
	return func(string, int) behaviour { return b }
}

// zipFailsOnce serves everything, except that the first request for the zip is reset in the middle of
// its body: the failure of PR #344's run, which was on a zip.
func zipFailsOnce(path string, nth int) behaviour {
	if path == zipPath && nth == 0 {
		return resetStream
	}
	return serve
}

// A proxy that answers 500 to everything: with "|", every request falls through to direct, the download
// succeeds on its first attempt (so the pipe did it, not the retry), and the checksum check passes.
func TestModFetchFallsBackToDirectWhenTheProxyAnswers500(t *testing.T) {
	r := newModRig(t)
	p := newProxy(t, false, moduleFiles(t, fixSource), always(answer500))
	env := r.liveEnv(ciGoproxy(t, p.srv.URL))
	out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=1")
	if code != 0 {
		t.Fatalf("exit %d, want 0: a 500 from the proxy must fall through to direct:\n%s", code, out)
	}
	for _, want := range []string{"all modules verified", "downloaded and verified on attempt 1 of 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if !cached(env) {
		t.Errorf("the module is not in the module cache after a successful run")
	}
	if p.count(modPath) == 0 {
		t.Errorf("the stand-in proxy was never asked, so the fallback was not exercised")
	}
}

// A proxy that resets the HTTP/2 stream in the middle of the zip, the failure of issue #346: the proxy
// serves the .info and the .mod, then the zip dies halfway. The stand-in is over TLS with HTTP/2 so
// that the client sees a real RST_STREAM. With "|" the zip comes from direct, on the first attempt.
func TestModFetchFallsBackToDirectWhenTheProxyResetsTheStreamInTheZip(t *testing.T) {
	r := newModRig(t)
	p := newProxy(t, true, moduleFiles(t, fixSource), func(path string, _ int) behaviour {
		if path == zipPath {
			return resetStream
		}
		return serve
	})
	env := r.liveEnv(ciGoproxy(t, p.srv.URL), "SSL_CERT_FILE="+p.cert)
	out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=1")
	if code != 0 {
		t.Fatalf("exit %d, want 0: a stream reset in the zip must fall through to direct:\n%s", code, out)
	}
	if !strings.Contains(out, "downloaded and verified on attempt 1 of 1") {
		t.Errorf("the pipe should have done it on the first attempt:\n%s", out)
	}
	if !cached(env) {
		t.Errorf("the module is not in the module cache after a successful run")
	}
	if p.count(zipPath) == 0 {
		t.Errorf("the proxy was never asked for the zip, so the reset did not happen")
	}
}

// The same stand-ins with a comma, which is the go command's default: it falls through only on a 404 or
// a 410, so the 500 and the reset fail the download although direct is healthy. This is the control
// that makes the two tests above mean something, and it shows the stand-in produces the issue's error.
func TestModFetchWithACommaDoesNotFallBackOnA500OrAReset(t *testing.T) {
	cases := []struct {
		name    string
		useTLS  bool
		rule    func(string, int) behaviour
		wantErr string
	}{
		{"500", false, always(answer500), "500 Internal Server Error"},
		{"reset stream", true, func(path string, _ int) behaviour {
			if path == zipPath {
				return resetStream
			}
			return serve
		}, "INTERNAL_ERROR"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newModRig(t)
			p := newProxy(t, c.useTLS, moduleFiles(t, fixSource), c.rule)
			comma := strings.Replace(ciGoproxy(t, p.srv.URL), "|", ",", 1)
			if !strings.Contains(comma, ",direct") {
				t.Fatalf("the comma variant of the workflow's GOPROXY is %q", comma)
			}
			env := r.liveEnv(comma, "SSL_CERT_FILE="+p.cert)
			out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=1")
			if code == 0 {
				t.Fatalf("exit 0 with a comma: the control did not fail, so the pipe proves nothing:\n%s", out)
			}
			if !strings.Contains(out, c.wantErr) {
				t.Errorf("the failure does not show %q:\n%s", c.wantErr, out)
			}
			if cached(env) {
				t.Errorf("the module was downloaded although the proxy failed and the separator was a comma")
			}
		})
	}
}

// The retry on its own: direct is dead, so only the proxy can serve, and it fails the zip once. The first
// attempt fails, the second succeeds, and the stand-in saw the zip asked for twice.
func TestModFetchRetriesAZipThatFailedOnceWhenDirectIsDead(t *testing.T) {
	r := newModRig(t)
	p := newProxy(t, true, moduleFiles(t, fixSource), zipFailsOnce)
	env := r.deadEnv(ciGoproxy(t, p.srv.URL), "SSL_CERT_FILE="+p.cert)
	out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=3")
	if code != 0 {
		t.Fatalf("exit %d, want 0: the second attempt should have succeeded:\n%s", code, out)
	}
	if !strings.Contains(out, "downloaded and verified on attempt 2 of 3") {
		t.Errorf("want success on attempt 2 of 3:\n%s", out)
	}
	if got := p.count(zipPath); got != 2 {
		t.Errorf("the zip was asked for %d times, want 2 (one failure, one retry)", got)
	}
	if !cached(env) {
		t.Errorf("the module is not in the module cache")
	}
}

// Every path fails: the proxy answers 500 and direct is dead. The script must fail, after exactly as
// many attempts as it was allowed, counted by the requests the proxy saw (a module whose .mod fails is
// asked for once per attempt), never by time.
func TestModFetchFailsAfterExactlyTheAllowedAttemptsWhenEveryPathFails(t *testing.T) {
	for _, attempts := range []int{1, 3} {
		t.Run(strconv.Itoa(attempts), func(t *testing.T) {
			r := newModRig(t)
			p := newProxy(t, false, moduleFiles(t, fixSource), always(answer500))
			env := r.deadEnv(ciGoproxy(t, p.srv.URL))
			out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS="+strconv.Itoa(attempts))
			if code != 1 {
				t.Fatalf("exit %d, want 1: no path works, and the job must fail:\n%s", code, out)
			}
			if !strings.Contains(out, "giving up") {
				t.Errorf("the output does not say it gave up:\n%s", out)
			}
			if strings.Contains(out, "downloaded and verified") {
				t.Errorf("the output claims success:\n%s", out)
			}
			if got := p.count(modPath); got != attempts {
				t.Errorf("the proxy saw %d requests for the .mod, want %d (one per attempt)", got, attempts)
			}
			if cached(env) {
				t.Errorf("the module is in the cache although every path failed")
			}
		})
	}
}

// Direct's bytes are checked against go.sum like the proxy's: an origin whose tag holds other files than
// the ones go.sum pins fails the download, with the go command's own checksum error, and nothing is kept.
func TestModFetchChecksTheDirectDownloadAgainstGoSum(t *testing.T) {
	r := newModRig(t)
	tampered := r.makeOrigin("tampered", fixSource+"\nconst Backdoor = true\n")
	p := newProxy(t, false, moduleFiles(t, fixSource), always(answer500))
	env := r.env(r.root, ciGoproxy(t, p.srv.URL), tampered)
	out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=1")
	if code == 0 {
		t.Fatalf("exit 0: a module that is not what go.sum pins was accepted from direct:\n%s", out)
	}
	if !strings.Contains(out, "checksum mismatch") {
		t.Errorf("the failure is not the go command's checksum mismatch:\n%s", out)
	}
	if cached(env) {
		t.Errorf("the tampered module is in the module cache")
	}
}

// goWrapper puts a go on PATH that records its arguments and runs the real one, and can make go mod
// verify fail, so that the script's use of verify is seen directly.
func (r *modRig) goWrapper() (pathDir, logFile string) {
	r.t.Helper()
	pathDir = filepath.Join(r.root, "wrapper-bin")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		r.t.Fatal(err)
	}
	logFile = filepath.Join(r.root, "go-calls.log")
	script := "#!/bin/sh\n" +
		"echo \"$*\" >> \"$WRAP_LOG\"\n" +
		"if [ \"$1 $2\" = \"mod verify\" ] && [ -n \"${WRAP_FAIL_VERIFY:-}\" ]; then echo 'stand-in: mismatch in the cache' >&2; exit 1; fi\n" +
		"exec \"$REAL_GO\" \"$@\"\n"
	write(r.t, filepath.Join(pathDir, "go"), []byte(script), 0o755)
	return pathDir, logFile
}

func calls(t *testing.T, logFile string) []string {
	t.Helper()
	b, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

// The script runs go mod verify after the download, once, and a verify that fails fails the script,
// without a second download.
func TestModFetchVerifiesAfterTheDownloadAndAFailedVerifyFailsTheStep(t *testing.T) {
	for _, failVerify := range []bool{false, true} {
		name := "verify passes"
		if failVerify {
			name = "verify fails"
		}
		t.Run(name, func(t *testing.T) {
			r := newModRig(t)
			p := newProxy(t, false, moduleFiles(t, fixSource), always(serve))
			dir, logFile := r.goWrapper()
			extra := []string{"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"), "WRAP_LOG=" + logFile, "REAL_GO=" + r.goBin}
			if failVerify {
				extra = append(extra, "WRAP_FAIL_VERIFY=1")
			}
			env := r.liveEnv(ciGoproxy(t, p.srv.URL), extra...)
			out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=3")
			got := calls(t, logFile)
			want := []string{"mod download", "mod verify"}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("go was called as %q, want %q (one download, then one verify, no second download)", got, want)
			}
			if failVerify {
				if code != 1 || strings.Contains(out, "downloaded and verified") {
					t.Errorf("a failed verify gave exit %d, want 1, and no success line:\n%s", code, out)
				}
				return
			}
			if code != 0 {
				t.Errorf("exit %d, want 0:\n%s", code, out)
			}
		})
	}
}

// A setting that is no number in range is refused before any go command runs.
func TestModFetchRefusesBadSettingsBeforeRunningGo(t *testing.T) {
	r := newModRig(t)
	dir, logFile := r.goWrapper()
	for _, c := range []struct{ name, setting string }{
		{"attempts zero", "GO_MOD_DOWNLOAD_ATTEMPTS=0"},
		{"attempts a word", "GO_MOD_DOWNLOAD_ATTEMPTS=three"},
		{"attempts too many", "GO_MOD_DOWNLOAD_ATTEMPTS=11"},
		{"pause negative", "GO_MOD_DOWNLOAD_PAUSE=-1"},
		{"pause too long", "GO_MOD_DOWNLOAD_PAUSE=301"},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := r.liveEnv("off", "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "WRAP_LOG="+logFile, "REAL_GO="+r.goBin)
			// The setting goes last so that it wins over the test's own GO_MOD_DOWNLOAD_PAUSE=0.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", filepath.Join("..", "go-mod-download.sh"))
			cmd.Env = append(env, "GO_MOD_DIR="+r.consumer, c.setting)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 2 {
				t.Fatalf("got %v, want exit 2:\n%s", err, out)
			}
			if !strings.Contains(string(out), "go-mod-download.sh: GO_MOD_DOWNLOAD_") {
				t.Errorf("the refusal does not name the setting:\n%s", out)
			}
			if _, err := os.Stat(logFile); err == nil {
				t.Errorf("go was run although the setting was refused")
			}
		})
	}
}

// The pause before each retry is the one asked for, and it doubles. A sleep on PATH records its argument
// and returns at once, so the test counts the pauses and never waits for one. Every attempt fails
// (GOPROXY=off), so it also shows that go mod verify is never reached after a failed download.
func TestModFetchPausesBeforeEachRetryAndDoublesThePause(t *testing.T) {
	r := newModRig(t)
	dir, goLog := r.goWrapper()
	sleepLog := filepath.Join(r.root, "sleeps.log")
	write(t, filepath.Join(dir, "sleep"), []byte("#!/bin/sh\necho \"$1\" >> \"$SLEEP_LOG\"\n"), 0o755)
	env := r.liveEnv("off", "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "WRAP_LOG="+goLog, "REAL_GO="+r.goBin, "SLEEP_LOG="+sleepLog)
	out, code := r.runScript(env, "GO_MOD_DOWNLOAD_ATTEMPTS=4", "GO_MOD_DOWNLOAD_PAUSE=7")
	if code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	if got, want := strings.Join(strings.Fields(string(mustRead(t, sleepLog))), " "), "7 14 28"; got != want {
		t.Errorf("the script paused %q seconds, want %q (three pauses between four attempts, doubling)", got, want)
	}
	if got, want := strings.Join(calls(t, goLog), "|"), strings.Repeat("mod download|", 3)+"mod download"; got != want {
		t.Errorf("go was called as %q, want four downloads and no verify", got)
	}
}

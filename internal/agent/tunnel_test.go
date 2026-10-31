package agent_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// The tunnel tests run the real binaries on loopback (HBEP-6, Test plan):
// frps and frpc v0.71.1 (testenv.FrpBinaries), OpenSSH's sshd and ssh, and
// the rig's TLS terminator in the dial-home listener's place.

const launcherName = "dial-home.hpcburst.invalid"

// loopbackListens reports whether anything listens on 127.0.0.1:port, read
// from /proc/net/tcp. It is the tests' own observer, apart from the agent's
// check of ssh's sockets.
func loopbackListens(port int) bool {
	b, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return false
	}
	want := fmt.Sprintf("0100007F:%04X", port)
	for _, line := range strings.Split(string(b), "\n")[1:] {
		f := strings.Fields(line)
		if len(f) > 3 && f[1] == want && f[3] == "0A" {
			return true
		}
	}
	return false
}

// readConfigServer reads serverAddr and serverPort from frpc's
// configuration file, with the test's own pattern.
func readConfigServer(path string) (string, int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	addr := regexp.MustCompile(`(?m)^serverAddr = "([^"]*)"$`).FindSubmatch(b)
	port := regexp.MustCompile(`(?m)^serverPort = ([0-9]+)$`).FindSubmatch(b)
	if addr == nil || port == nil {
		return "", 0, fmt.Errorf("frpc's configuration has no server:\n%s", b)
	}
	p, _ := strconv.Atoi(string(port[1]))
	return string(addr[1]), p, nil
}

func configServer(t *testing.T, path string) (string, int) {
	t.Helper()
	addr, port, err := readConfigServer(path)
	if err != nil {
		t.Fatal(err)
	}
	return addr, port
}

// frpcVerifies runs frpc verify on the agent's rendered configuration.
func frpcVerifies(t *testing.T, frpc, path string) {
	t.Helper()
	out, err := exec.Command(frpc, "verify", "-c", path).CombinedOutput()
	if err != nil {
		t.Fatalf("frpc verify -c %s: %v: %s", path, err, out)
	}
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// ---- direct mode: frpc straight to the dial-home listener ----

type directRig struct {
	frps    *frpsProc
	term    *terminator
	frpc    *agent.Frpc
	frpcBn  string
	staged  string // the staged certificate's path
	mu      sync.Mutex
	starts  []agent.ProcessID
	stopRun func()
}

type directOptions struct {
	termCert  tls.Certificate // what the front presents
	staged    []byte          // the staged certificate
	pin       string          // agent.json's pin
	before    func()          // runs before frpc starts
	frpsLater bool            // frps starts only when the test calls frps.start
}

// newDirectRig runs frps, the terminator, the engine and frpc.
func newDirectRig(t *testing.T, o directOptions) *directRig {
	t.Helper()
	frpsBin, frpcBin := frpBins(t)
	w := newProcWatch()
	t.Cleanup(func() { w.assertGone(t) })
	dir := shortTemp(t)
	runDir := filepath.Join(dir, "run")
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &directRig{frpcBn: frpcBin}
	if o.frpsLater {
		r.frps = planFrps(t, frpsBin, dir, w)
	} else {
		r.frps = startFrps(t, frpsBin, dir, w)
	}
	r.term = startTerminator(t, o.termCert, "127.0.0.1:"+strconv.Itoa(r.frps.bindPort))
	startEngine(t, runDir, hInstUID)
	r.staged = writeFile(t, filepath.Join(dir, "dial-home.crt"), o.staged)
	server, err := agent.DirectServer("127.0.0.1:" + strconv.Itoa(r.term.port()))
	if err != nil {
		t.Fatal(err)
	}
	r.frpc = newFrpc(t, frpcBin, runDir, r.staged, o.pin, server, w)
	r.frpc.OnStart = func(id agent.ProcessID) {
		w.add("frpc", id)
		r.mu.Lock()
		r.starts = append(r.starts, id)
		r.mu.Unlock()
	}
	logs := &logBuf{}
	r.frpc.Logger = slog.New(slog.NewTextHandler(logs, nil))
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(r.frpc.LogPath())
			t.Logf("agent log:\n%s\nfrpc log:\n%s", logs.String(), b)
		}
	})
	if o.before != nil {
		o.before()
	}
	r.frpc.Serve(hInstUID)
	r.stopRun = runBackground(t, "frpc", r.frpc.Run)
	return r
}

func (r *directRig) frpcStarts() []agent.ProcessID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]agent.ProcessID(nil), r.starts...)
}

// endsCleanly ends the Instance and then the agent's frpc while frps still
// runs, and checks that frps drops the proxy and the frpc is gone, before
// the fixture's cleanup could hide either (Astra nit 3).
func (r *directRig) endsCleanly(t *testing.T) {
	t.Helper()
	r.frpc.Serve("")
	eventually(t, 10*time.Second, "frps to drop the ended Instance's proxy", func() bool { return !r.frps.online(t, hInstUID) })
	r.stopRun()
	for _, id := range r.frpcStarts() {
		if id.Alive() {
			t.Fatalf("frpc %d runs after the agent's frpc stopped", id.PID)
		}
	}
}

// frpc logs in and registers its proxy through the wss front; the front's
// CONNECT <UID>:80 gets 200 and the engine's reply through the agent's
// serving socket; an unknown UID gets 404 (HBEP-6, Test plan, Wiring). This
// is also the positive control of the certificate refusals below: the same
// rig with the pinned certificate on both ends.
func TestFrpcRegistersThroughTheWSSFront(t *testing.T) {
	a := makeCert(t, launcherName)
	r := newDirectRig(t, directOptions{termCert: a.tls, staged: a.pem, pin: a.pin})
	eventually(t, 15*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	connect, status, body, err := r.frps.request(hInstUID, "/health")
	if err != nil || connect != 200 || status != 200 || body != "engine for "+hInstUID+": /health" {
		t.Fatalf("through the tunnel: CONNECT %d, %d %q, %v", connect, status, body, err)
	}
	if connect, _, _, err := r.frps.request("ffffffff-0000-0000-0000-000000000000", "/health"); err != nil || connect != 404 {
		t.Fatalf("an unknown UID: CONNECT %d, %v; want 404", connect, err)
	}
	if n := r.term.frpConns.Load(); n < 1 {
		t.Fatalf("the terminator passed %d GET /~!frp to frps", n)
	}
	if n := r.term.others.Load(); n != 0 {
		t.Fatalf("the terminator saw %d requests other than GET /~!frp", n)
	}
	frpcVerifies(t, r.frpcBn, r.frpc.ConfigPath())
	if fi, err := os.Stat(r.frpc.ConfigPath()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("frpc's configuration holds the credential: %v, mode %v, want 600", err, fi.Mode().Perm())
	}
	r.endsCleanly(t)
}

// refused waits until frpc has tried the front at least twice, then checks
// that every TLS handshake failed, nothing reached frps and frps lists no
// proxy, so the front's CONNECT gets 404.
func (r *directRig) refused(t *testing.T) {
	t.Helper()
	eventually(t, 25*time.Second, "frpc to try the front twice", func() bool { return r.term.handshakes.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)
	if h, f := r.term.handshakes.Load(), r.term.failed.Load(); f != h {
		t.Fatalf("%d of %d TLS handshakes completed", h-f, h)
	}
	if r.frps.online(t, hInstUID) {
		t.Fatal("frps lists the proxy online")
	}
	if connect, _, _, err := r.frps.request(hInstUID, "/health"); err != nil || connect != 404 {
		t.Fatalf("CONNECT %d, %v; want 404", connect, err)
	}
	b, _ := os.ReadFile(r.frpc.LogPath())
	if !bytes.Contains(b, []byte("x509")) && !bytes.Contains(b, []byte("certificate")) {
		t.Fatalf("frpc's log names no certificate error:\n%s", b)
	}
}

// frpc facing another certificate than the staged one is refused: frpc's own
// verification fails the handshake (HBEP-6, Test plan, Certificate).
func TestFrpcRefusesAnotherCertificate(t *testing.T) {
	staged := makeCert(t, launcherName)
	other := makeCert(t, launcherName)
	r := newDirectRig(t, directOptions{termCert: other.tls, staged: staged.pem, pin: staged.pin})
	r.refused(t)
	if n := r.term.frpConns.Load(); n != 0 {
		t.Fatalf("%d GET /~!frp reached frps", n)
	}
}

// frpc with the right key but another server name is refused by frpc
// itself: the staged certificate, which names dial-home.hpcburst.invalid and
// passes the agent's own check, is the trust anchor, and the front presents a
// certificate signed by the same key that names another host, so only frpc's
// serverName check can refuse it (HBEP-6, Certificate checks).
func TestFrpcRefusesAnotherServerName(t *testing.T) {
	staged := makeCert(t, launcherName)
	other := makeCertWith(t, certOpts{names: []string{"other.hpcburst.invalid"}, key: staged.tls.PrivateKey.(*ecdsa.PrivateKey)})
	r := newDirectRig(t, directOptions{termCert: other.tls, staged: staged.pem, pin: staged.pin})
	r.refused(t)
	b, _ := os.ReadFile(r.frpc.LogPath())
	if !bytes.Contains(b, []byte(launcherName)) {
		t.Fatalf("frpc's log does not show the server-name refusal:\n%s", b)
	}
}

// frpc trusts the certificate the agent checked, never what is put at the
// staged path later (Opus N1): once frpc has logged in with the pinned
// certificate, the staged file is replaced with another and the front
// presents that one, and the path is cut. The same frpc tries again, and every
// handshake fails.
func TestFrpcTrustsOnlyTheCheckedCertificate(t *testing.T) {
	a := makeCert(t, launcherName)
	r := newDirectRig(t, directOptions{termCert: a.tls, staged: a.pem, pin: a.pin})
	eventually(t, 15*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	b := makeCert(t, launcherName)
	writeFile(t, r.staged, b.pem)
	r.term.setCert(b.tls)
	failed0 := r.term.failed.Load()
	r.term.cut()
	eventually(t, 25*time.Second, "frps to drop the cut session", func() bool { return !r.frps.online(t, hInstUID) })
	eventually(t, 25*time.Second, "frpc to try the front again", func() bool { return r.term.failed.Load() >= failed0+1 })
	time.Sleep(time.Second)
	if r.frps.online(t, hInstUID) {
		t.Fatal("frpc logged in again through a front presenting a certificate the agent never checked")
	}
	if n := len(r.frpcStarts()); n != 1 {
		t.Fatalf("frpc started %d times, want the same one throughout", n)
	}
}

// With loginFailExit = false a failed first login is retried by the same
// frpc process (HBEP-6, frpc in the job; Opus N6): frps is down when frpc
// starts, so its first logins fail; once frps runs, that frpc logs in, and
// it was started once.
func TestFrpcRetriesItsFirstLoginItself(t *testing.T) {
	a := makeCert(t, launcherName)
	r := newDirectRig(t, directOptions{termCert: a.tls, staged: a.pem, pin: a.pin, frpsLater: true})
	eventually(t, 15*time.Second, "frpc's first login to fail", func() bool { return r.term.frpConns.Load() >= 1 })
	time.Sleep(time.Second)
	if !r.frps.start(t) {
		t.Fatal("frps did not come up")
	}
	eventually(t, 30*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	if n := len(r.frpcStarts()); n != 1 {
		t.Fatalf("frpc started %d times; the first one should have retried its login", n)
	}
	r.endsCleanly(t)
}

// A restart of frps: the same frpc logs in again without help (HBEP-6, Test
// plan, Reconnects; Opus N7).
func TestFrpcLogsInAgainAfterFrpsRestarts(t *testing.T) {
	a := makeCert(t, launcherName)
	r := newDirectRig(t, directOptions{termCert: a.tls, staged: a.pem, pin: a.pin})
	eventually(t, 15*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	r.frps.restart(t)
	eventually(t, 40*time.Second, "frpc to log in to the restarted frps", func() bool { return r.frps.online(t, hInstUID) })
	if _, status, _, err := r.frps.request(hInstUID, "/health"); err != nil || status != 200 {
		t.Fatalf("through the tunnel after the restart: %d, %v", status, err)
	}
	if n := len(r.frpcStarts()); n != 1 {
		t.Fatalf("frpc started %d times; it should have logged in again by itself", n)
	}
	r.endsCleanly(t)
}

// Proxy variables in the agent's environment do not reach frpc: frp takes
// http_proxy as its proxy to frps (HBEP-6, frp facts), so with it frpc would
// dial the trap below instead of the front. frpc logs in and the trap sees
// no connection.
func TestFrpcIgnoresTheProxyVariables(t *testing.T) {
	a := makeCert(t, launcherName)
	trap := kernelListener(t)
	var trapped atomic.Int32
	go func() {
		for {
			c, err := trap.Accept()
			if err != nil {
				return
			}
			trapped.Add(1)
			c.Close()
		}
	}()
	t.Cleanup(func() { trap.Close() })
	proxy := "http://127.0.0.1:" + strconv.Itoa(portOf(trap))
	r := newDirectRig(t, directOptions{termCert: a.tls, staged: a.pem, pin: a.pin, before: func() {
		for _, v := range []string{"http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
			t.Setenv(v, proxy)
		}
	}})
	if r.frpc.Env != nil {
		t.Fatal("the rig must leave Env nil, so frpc starts from the agent's own environment")
	}
	eventually(t, 15*time.Second, "frpc to log in, or to dial the proxy", func() bool {
		return trapped.Load() > 0 || r.frps.online(t, hInstUID)
	})
	if n := trapped.Load(); n != 0 {
		t.Fatalf("frpc dialled the proxy from the agent's environment %d times", n)
	}
	if _, status, _, err := r.frps.request(hInstUID, "/health"); err != nil || status != 200 {
		t.Fatalf("through the tunnel: %d, %v", status, err)
	}
	if n := trapped.Load(); n != 0 {
		t.Fatalf("frpc dialled the proxy from the agent's environment %d times", n)
	}
}

// A direct-mode endpoint must name the front by a DNS name or an IPv4
// address: frpc's serverAddr takes no bracketed IPv6 literal (Opus N8).
func TestDirectServerRefusesAnIPv6Literal(t *testing.T) {
	if _, err := agent.DirectServer("[::1]:443"); err == nil {
		t.Fatal("an IPv6 literal endpoint was accepted")
	}
	if _, err := agent.DirectServer("front.example:443"); err != nil {
		t.Fatalf("a DNS name endpoint: %v", err)
	}
}

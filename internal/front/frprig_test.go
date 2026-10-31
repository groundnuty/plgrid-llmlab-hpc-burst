package front

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rig for the tests that run the real frps and frpc (v0.71.1, from
// testenv.FrpBinaries: they skip without HPCBURST_FRP_DIR, and fail with
// HPCBURST_REQUIRE_FRP=1, as in CI). frps is configured and started by the
// launcher's own code (RenderFrpsConfig through WriteFrpsConfig, StartFrps);
// frpc runs HBEP-6's configuration over wss through the front's dial-home
// listener. frps holds base+8 to base+11 of its port base: the bases below put
// them in 29440-29459, this work's block (#90, the front's tunnel). Every other
// listener takes a port from the kernel. Everything binds 127.0.0.1.
var frpRigBases = make(chan int, 5)

func init() {
	for _, b := range []int{29432, 29436, 29440, 29444, 29448} {
		frpRigBases <- b
	}
}

// frpRig is a real frps with a plugin stub that admits everything.
type frpRig struct {
	t    *testing.T
	host agentapi.Host
	dir  string
	pw   string
	proc *launcher.FrpsProc
	frpc string // the frpc binary
	cert string // the dial-home certificate, the one frpc trusts
	n    int    // engines and frpcs started, for unique file names
}

func newFrpRig(t *testing.T) *frpRig {
	t.Helper()
	frps, frpc := testenv.FrpBinaries(t)
	checkRootK3s(t)
	var base int
	select {
	case base = <-frpRigBases:
	case <-time.After(2 * time.Minute):
		t.Fatal("no frp rig port base came free")
	}
	t.Cleanup(func() { frpRigBases <- base }) // runs last: frps is stopped by then
	dir := shortDir(t)
	r := &frpRig{t: t, host: agentapi.Host{StateDir: dir, PortBase: base}, dir: dir, frpc: frpc}
	r.cert = writeDialHomeCert(t, r.host)
	startAdmitAllPlugin(t, r.host)
	var err error
	if r.pw, _, err = launcher.EnsureFrpsWebPassword(r.host); err != nil {
		t.Fatal(err)
	}
	cfg, err := launcher.WriteFrpsConfig(r.host, r.pw)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := launcher.ResolveFrpsFile(frps, launcher.DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bin.Close)
	if r.proc, err = launcher.StartFrps(launcher.FrpsSpec{Bin: bin, Config: cfg, Log: launcher.FrpsLogFile(r.host), Dir: dir}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.cont() // a test that stopped it may have failed before it went on
		if _, err := r.proc.Stop(context.Background(), 5*time.Second); err != nil {
			t.Errorf("stop frps: %v", err)
		}
	})
	for end := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if code, _ := r.web("", "/healthz"); code == http.StatusOK {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("frps's web server never answered /healthz: %s\n%s", r.proc.ExitStatus(), r.logTail())
		}
	}
	return r
}

// stop and cont freeze and resume frps with SIGSTOP and SIGCONT. A frozen frps
// still completes TCP handshakes from its listen backlog, so a dial succeeds
// and the CONNECT that follows waits for an answer.
func (r *frpRig) stop() { r.signal(syscall.SIGSTOP) }
func (r *frpRig) cont() { r.signal(syscall.SIGCONT) }

func (r *frpRig) signal(sig syscall.Signal) {
	select {
	case <-r.proc.Done():
		return
	default:
	}
	if err := syscall.Kill(r.proc.Rec().PID, sig); err != nil {
		r.t.Errorf("signal %v to frps: %v", sig, err)
	}
}

func (r *frpRig) logTail() string {
	b, _ := os.ReadFile(launcher.FrpsLogFile(r.host))
	if len(b) > 3000 {
		b = b[len(b)-3000:]
	}
	return string(b)
}

// web asks frps's web server; pass is "" for no credentials.
func (r *frpRig) web(pass, path string) (int, string) {
	req, err := http.NewRequest(http.MethodGet, "http://"+r.host.FrpsWebAddr()+path, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	if pass != "" {
		req.SetBasicAuth(agentapi.FrpsWebUser, pass)
	}
	c := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// proxyStatus is frps's own view of a tcpmux proxy: online, offline, or "".
func (r *frpRig) proxyStatus(name string) string {
	code, body := r.web(r.pw, "/api/proxy/tcpmux")
	if code != http.StatusOK {
		return ""
	}
	var list struct {
		Proxies []struct{ Name, Status string } `json:"proxies"`
	}
	if json.Unmarshal([]byte(body), &list) != nil {
		return ""
	}
	for _, p := range list.Proxies {
		if p.Name == name {
			return p.Status
		}
	}
	return ""
}

// engine starts an engine double on a Unix socket in the rig's directory, as
// the engine listens in the job (HBEP-6, Local listeners).
func (r *frpRig) engine(key string) (*enginetest.Engine, string) {
	r.t.Helper()
	r.n++
	sock := filepath.Join(r.dir, fmt.Sprintf("e%d.sock", r.n))
	return r.engineOn(sock, key), sock
}

// engineOn starts an engine double on the socket path, again after a kill.
func (r *frpRig) engineOn(sock, key string) *enginetest.Engine {
	r.t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		r.t.Fatal(err)
	}
	return enginetest.Serve(r.t, ln, enginetest.Options{Key: key})
}

// startFrpc runs frpc with HBEP-6's configuration for one Instance: wss to the
// dial-home listener at dialHome, the dial-home certificate as its only trust
// and dial-home.hpcburst.invalid as the server name, one tcpmux proxy named by
// the UID, served from the engine's socket. It runs from an empty environment
// (frp takes http_proxy as its proxy to frps otherwise) and is killed at the
// end of the test; kill ends it earlier, as a crashed job, and pause and
// resume freeze it.
func (r *frpRig) startFrpc(uid types.UID, sock, dialHome string) *frpcProc {
	r.t.Helper()
	r.n++
	host, port, err := net.SplitHostPort(dialHome)
	if err != nil {
		r.t.Fatal(err)
	}
	cfg := filepath.Join(r.dir, fmt.Sprintf("frpc%d.toml", r.n))
	text := fmt.Sprintf(`serverAddr = %q
serverPort = %s
loginFailExit = false
log.to = %q
metadatas.token = "credential-of-the-allocation"
transport.protocol = "wss"
transport.tls.trustedCaFile = %q
transport.tls.serverName = %q
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
transport.dialServerKeepalive = 30

[[proxies]]
name = %q
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = [%q]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = %q
`, host, port, cfg+".log", r.cert, agentapi.DialHomeServerName, string(uid), string(uid), sock)
	if err := os.WriteFile(cfg, []byte(text), 0o600); err != nil {
		r.t.Fatal(err)
	}
	out, err := os.Create(cfg + ".out")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { out.Close() })
	cmd := exec.Command(r.frpc, "-c", cfg)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	p := &frpcProc{t: r.t, cmd: cmd, log: cfg + ".log", done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	r.t.Cleanup(p.kill)
	return p
}

// waitOnline waits until frps lists the UID's proxy online.
func (r *frpRig) waitOnline(uid types.UID, p *frpcProc) {
	r.t.Helper()
	for end := time.Now().Add(20 * time.Second); r.proxyStatus(string(uid)) != "online"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			b, _ := os.ReadFile(p.log)
			r.t.Fatalf("frps never listed %s online (%q)\nfrpc log:\n%s\nfrps log:\n%s", uid, r.proxyStatus(string(uid)), b, r.logTail())
		}
	}
}

type frpcProc struct {
	t    *testing.T
	cmd  *exec.Cmd
	log  string
	done chan struct{}
	once sync.Once
}

// pause and resume freeze frpc with SIGSTOP and SIGCONT: a frozen path, which
// sends no FIN (HBEP-6, Test plan).
func (p *frpcProc) pause()  { _ = p.cmd.Process.Signal(syscall.SIGSTOP) }
func (p *frpcProc) resume() { _ = p.cmd.Process.Signal(syscall.SIGCONT) }

func (p *frpcProc) kill() {
	p.once.Do(func() {
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.t.Errorf("frpc (pid %d) did not exit after SIGKILL", p.cmd.Process.Pid)
		}
	})
}

// writeDialHomeCert writes a self-signed certificate for
// dial-home.hpcburst.invalid and its key where the launcher keeps the
// listener's (Host.AgentCertFile, Host.AgentKeyFile), and returns the
// certificate's path: frpc's trustedCaFile.
func writeDialHomeCert(t *testing.T, host agentapi.Host) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: agentapi.DialHomeServerName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{agentapi.DialHomeServerName}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	writeMode600(t, host.AgentCertFile(), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeMode600(t, host.AgentKeyFile(), string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})))
	return host.AgentCertFile()
}

// startAdmitAllPlugin serves frps's server plugin at base+8, admitting every
// login and proxy: the lifecycle controller's plugin decides admission in the
// product, and these tests are about the front's half. frps fails closed, so
// without it frpc never logs in.
func startAdmitAllPlugin(t *testing.T, host agentapi.Host) {
	t.Helper()
	ln, err := net.Listen("tcp", host.FrpsPluginAddr())
	if err != nil {
		t.Fatalf("the plugin stub cannot listen: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(agentapi.FrpsPluginPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}

// serveDialHome serves the front's dial-home listener for the rig in mode on a
// port the kernel chooses: TLS with the listener's certificate, /~!frp passed
// to the rig's frps. It returns the listener's address and the passthrough.
func (r *frpRig) serveDialHome(mode string) (string, *frpPassthrough) {
	r.t.Helper()
	tlsCfg, err := dialHomeTLS(mode, r.host)
	if err != nil {
		r.t.Fatal(err)
	}
	pass := newFrpPassthrough(r.host.FrpsBindAddr(), DefaultTunnelSetupTimeout, discardLog())
	dh, err := NewDialHome(mode, r.host, pass)
	if err != nil {
		r.t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.t.Fatal(err)
	}
	srv := &http.Server{Handler: dh, ReadHeaderTimeout: time.Minute}
	var served net.Listener = ln
	if tlsCfg != nil {
		served = tls.NewListener(ln, tlsCfg)
	}
	go func() { _ = srv.Serve(served) }()
	r.t.Cleanup(func() { _ = srv.Close(); pass.Close() })
	return ln.Addr().String(), pass
}

// shortDir is a temporary directory with a short path: a Unix socket's path
// must fit in 108 bytes, and t.TempDir's names follow the test's.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hbf")
	if err == nil && len(d) > 60 {
		os.RemoveAll(d)
		d, err = os.MkdirTemp("/tmp", "hbf")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// checkRootK3s asserts after the test that the root k3s of a machine that runs
// one still listens on 6443 and 10250 (IMPL-COMMON), when
// HPCBURST_TEST_ROOT_K3S=1 says there is one.
func checkRootK3s(t *testing.T) {
	t.Helper()
	if os.Getenv("HPCBURST_TEST_ROOT_K3S") != "1" {
		return
	}
	listening := func() string {
		out, err := exec.Command("ss", "-ltnH").Output()
		if err != nil {
			t.Fatalf("ss: %v", err)
		}
		var have []string
		for _, p := range []string{"6443", "10250"} {
			for _, l := range strings.Split(string(out), "\n") {
				if f := strings.Fields(l); len(f) >= 4 && strings.HasSuffix(f[3], ":"+p) {
					have = append(have, p)
					break
				}
			}
		}
		return strings.Join(have, ",")
	}
	if got := listening(); got != "6443,10250" {
		t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", got)
	}
	t.Cleanup(func() {
		if got := listening(); got != "6443,10250" {
			t.Errorf("the root k3s's listeners changed: now %q", got)
		}
	})
}

// tunFront is a front built from its parts over the rig's frps: the
// harness's router and proxy replaced by ones on a Tunnel, and a Prober that
// the test drives round by round, on the wall clock.
type tunFront struct {
	*harness
	rig    *frpRig
	tun    *Tunnel
	prober *Prober
	events *recordedPathEvents
	log    *slog.Logger
}

func newTunFront(t *testing.T, rig *frpRig) *tunFront {
	t.Helper()
	h := newHarnessReal(t, nil)
	h.host = rig.host // Instance keys go to the rig's credentials directory
	f := &tunFront{harness: h, rig: rig, events: &recordedPathEvents{}}
	f.log = slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.tun = NewTunnel(rig.host.FrpsTCPMuxAddr(), h.cfg.TunnelSetupTimeout.D())
	keys, err := ReadClientKeys(h.cfg.ClientKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	h.router = NewRouter(h.cfg, clock.Real{}, f.tun, h.rec, f.log)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clock.Real{}, Router: h.router, Transports: f.tun, Keys: keys, Sink: h.sink, Log: f.log})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + ln.Addr().String()
	f.prober = NewProber(h.cfg, f.tun, clock.Real{}, func() []v1alpha1.Instance { return h.snap.Instances }, f.events, f.log)
	return f
}

// waitRegistered runs probe rounds until the UID's transport is registered.
func (f *tunFront) waitRegistered(uid types.UID) {
	f.t.Helper()
	for end := time.Now().Add(20 * time.Second); !f.tun.Registered(uid); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			f.t.Fatalf("the probe never registered %s\n%s", uid, f.logs.String())
		}
		f.prober.Round(context.Background())
	}
}

// recordedPathEvents keeps what the probe reports.
type recordedPathEvents struct {
	mu      sync.Mutex
	probed  []ProbeResult
	failing []ProbeResult
}

func (e *recordedPathEvents) Probed(r ProbeResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.probed = append(e.probed, r)
}

func (e *recordedPathEvents) PathProbeFailing(_ *v1alpha1.Instance, r ProbeResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failing = append(e.failing, r)
}

func (e *recordedPathEvents) failings() []ProbeResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]ProbeResult(nil), e.failing...)
}

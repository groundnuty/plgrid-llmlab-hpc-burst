package frpsplugin

// The rig for the tests that run the real frps and frpc (the pinned v0.71.1, from
// testenv.FrpBinaries: HPCBURST_FRP_DIR, and HPCBURST_REQUIRE_FRP=1 turns a missing binary from a
// skip into a failure, which CI sets). frps is started with #106's own code: the exported
// launcher.EnsureFrpsWebPassword, WriteFrpsConfig, ResolveFrpsFile and StartFrps. The helpers of
// the launcher's own tests are not importable, so frpc's start and the port search are here.
//
// Ports: none is fixed. The kernel hands out one port P on 127.0.0.1; the block is P-8..P+3, so
// that P is the plugin's port (base+8, which the rig's own server holds) and P+1..P+3 are frps's
// bind, tcpmux and web ports, checked free just before frps starts. If frps still fails to start
// (another process took one in between), the rig tries again with a new P.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// claimBlock asks the kernel for a port P on 127.0.0.1 such that P+1..P+3 are free too, and returns
// the port base (P-8) with P's listener still open.
func claimBlock() (base int, plugin net.Listener, err error) {
	for try := 0; try < 100; try++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, nil, err
		}
		p := ln.Addr().(*net.TCPAddr).Port
		if p < 9 || p+3 > 65535 {
			ln.Close()
			continue
		}
		var held []net.Listener
		free := true
		for i := 1; i <= 3 && free; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+i))
			if err != nil {
				free = false
				break
			}
			held = append(held, l)
		}
		for _, l := range held {
			l.Close()
		}
		if free {
			return p - agentapi.FrpsPluginPortOffset, ln, nil
		}
		ln.Close()
	}
	return 0, nil, fmt.Errorf("the kernel gave no run of four free ports in 100 tries")
}

// captured is one plugin call as frps posted it.
type captured struct {
	Op      string
	QueryOp string
	Body    []byte
}

// capture wraps the handler under test, and records every request body before the handler sees it
// (and the answer after), so a test can see what frps really posted and what it was told.
type capture struct {
	next http.Handler
	mu   sync.Mutex
	in   []captured
	out  []string // the raw answers, in the same order
}

func (c *capture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var env struct{ Op string }
	_ = json.Unmarshal(body, &env)
	rec := &answerRecorder{ResponseWriter: w}
	c.next.ServeHTTP(rec, r)
	c.mu.Lock()
	c.in = append(c.in, captured{Op: env.Op, QueryOp: r.URL.Query().Get("op"), Body: body})
	c.out = append(c.out, rec.buf.String())
	c.mu.Unlock()
}

type answerRecorder struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (a *answerRecorder) Write(b []byte) (int, error) {
	a.buf.Write(b)
	return a.ResponseWriter.Write(b)
}

func (c *capture) calls(op string) []captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []captured
	for _, x := range c.in {
		if x.Op == op {
			out = append(out, x)
		}
	}
	return out
}

// events is a Notifier that records what it hears.
type events struct {
	mu   sync.Mutex
	list []string // "NewProxy uid"
}

func (e *events) ProxyChanged(op, name string) {
	e.mu.Lock()
	e.list = append(e.list, op+" "+name)
	e.mu.Unlock()
}

func (e *events) has(s string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, x := range e.list {
		if x == s {
			return true
		}
	}
	return false
}

func (e *events) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.list...)
}

// rig is a real frps whose plugin is the handler under test.
type rig struct {
	t       *testing.T
	host    agentapi.Host
	state   string
	frpc    string
	pw      string
	proc    *launcher.FrpsProc
	stopped bool
	cap     *capture
}

// newRig starts frps with h as its plugin (behind a capture of every call), and stops it at the end
// of the test. It also checks that the root k3s is as it was afterwards (IMPL-COMMON).
func newRig(t *testing.T, h http.Handler) *rig {
	t.Helper()
	frps, frpc := testenv.FrpBinaries(t)
	before := rootK3sListening(t)
	if os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1" && before != "6443,6444,10250" {
		t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", before)
	}
	t.Cleanup(func() {
		if after := rootK3sListening(t); after != before {
			t.Errorf("the root k3s's listeners changed: %q -> %q", before, after)
		}
	})
	state, err := os.MkdirTemp("", "fp") // short: the engine's Unix socket path must fit in 108 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	bin, err := launcher.ResolveFrpsFile(frps, launcher.DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bin.Close)

	r := &rig{t: t, state: state, frpc: frpc, cap: &capture{next: h}}
	var lastErr error
	for try := 0; try < 5; try++ {
		base, ln, err := claimBlock()
		if err != nil {
			t.Fatal(err)
		}
		host := agentapi.Host{StateDir: state, PortBase: base}
		if r.pw, _, err = launcher.EnsureFrpsWebPassword(host); err != nil {
			ln.Close()
			t.Fatal(err)
		}
		cfg, err := launcher.WriteFrpsConfig(host, r.pw)
		if err != nil {
			ln.Close()
			t.Fatal(err)
		}
		srv := &http.Server{Handler: r.cap, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		proc, err := launcher.StartFrps(launcher.FrpsSpec{Bin: bin, Config: cfg, Log: launcher.FrpsLogFile(host), Dir: state})
		if err != nil {
			srv.Close()
			t.Fatal(err)
		}
		if lastErr = waitHealthy(host, proc); lastErr == nil {
			r.host, r.proc = host, proc
			t.Cleanup(func() { _ = srv.Close() })
			t.Cleanup(func() {
				if !r.stopped {
					_, _ = proc.Stop(context.Background(), 3*time.Second)
				}
			})
			return r
		}
		_, _ = proc.Stop(context.Background(), 3*time.Second)
		srv.Close()
		t.Logf("frps did not come up on base %d (%v); trying another block", base, lastErr)
	}
	t.Fatalf("frps never came up in 5 tries: %v", lastErr)
	return nil
}

func waitHealthy(h agentapi.Host, p *launcher.FrpsProc) error {
	c := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		select {
		case <-p.Done():
			b, _ := os.ReadFile(launcher.FrpsLogFile(h))
			if len(b) > 1500 {
				b = b[len(b)-1500:]
			}
			return fmt.Errorf("frps exited at start: %s\n%s", p.ExitStatus(), b)
		default:
		}
		if resp, err := c.Get("http://" + h.FrpsWebAddr() + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
	}
	return fmt.Errorf("frps's web server never answered /healthz")
}

// stopFrps ends frps the way the launcher does.
func (r *rig) stopFrps() {
	r.t.Helper()
	r.stopped = true
	if _, err := r.proc.Stop(context.Background(), 5*time.Second); err != nil {
		r.t.Fatalf("stop frps: %v", err)
	}
}

// reader is the Reader the controller would use, for this frps.
func (r *rig) reader() *Reader {
	r.t.Helper()
	rd, err := NewHostReader(r.host)
	if err != nil {
		r.t.Fatal(err)
	}
	return rd
}

// frpcSpec is one frpc: the credential it presents, and the Instance UIDs it serves.
type frpcSpec struct {
	token string
	uids  []string
	// extra is TOML added to a proxy's table, by the proxy's name: how a test makes frpc send a
	// field the plugin must refuse (httpUser, loadBalancer.group, ...).
	extra map[string]string
}

// runFrpc writes frpc's configuration the way the agent does (HBEP-6, frpc in the job; plain tcp
// instead of wss, because there is no front here) and starts it. It returns frpc's log file and
// the function that kills it, as a crashed job would.
func (r *rig) runFrpc(s frpcSpec) (logFile string, kill func()) {
	r.t.Helper()
	sock := filepath.Join(r.state, "engine.sock") // nothing listens: no work connection is made
	logFile = filepath.Join(r.state, "frpc.log")
	var b strings.Builder
	fmt.Fprintf(&b, `serverAddr = "127.0.0.1"
serverPort = %d
loginFailExit = false
log.to = %q
metadatas.token = %q
transport.protocol = "tcp"
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
`, r.host.FrpsBindPort(), logFile, s.token)
	for _, uid := range s.uids {
		fmt.Fprintf(&b, `
[[proxies]]
name = %q
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = [%q]
%s
[proxies.plugin]
type = "unix_domain_socket"
unixPath = %q
`, uid, uid, s.extra[uid], sock)
	}
	cfg := filepath.Join(r.state, "frpc.toml")
	if err := os.WriteFile(cfg, []byte(b.String()), 0o600); err != nil {
		r.t.Fatal(err)
	}
	out, err := os.Create(cfg + ".out")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { out.Close() })
	cmd := exec.Command(r.frpc, "-c", cfg)
	cmd.Env = []string{"PATH=/usr/bin:/bin"} // frp reads http_proxy for its connection to frps otherwise
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	killed := false
	kill = func() {
		if killed {
			return
		}
		killed = true
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			r.t.Errorf("frpc (pid %d) did not exit after SIGKILL", cmd.Process.Pid)
		}
	}
	r.t.Cleanup(kill)
	return logFile, kill
}

// waitFor polls cond every 50 ms until it holds, and fails the test with what after d.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); ; time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("after %s: %s", d, what)
		}
	}
}

// rootK3sListening lists which of the root k3s's ports (6443, 6444, 10250) have a listener
// (IMPL-COMMON: after every test that starts anything, the root k3s must be as it was).
func rootK3sListening(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("ss", "-ltnH").Output()
	if err != nil {
		t.Fatalf("ss: %v", err)
	}
	var have []string
	for _, p := range []string{"6443", "6444", "10250"} {
		for _, l := range strings.Split(string(out), "\n") {
			if f := strings.Fields(l); len(f) >= 4 && strings.HasSuffix(f[3], ":"+p) {
				have = append(have, p)
				break
			}
		}
	}
	return strings.Join(have, ",")
}

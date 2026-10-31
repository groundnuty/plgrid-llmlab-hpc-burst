package agent_test

// The rig for the tunnel tests (HBEP-6, Test plan): real frps and frpc
// v0.71.1, a real OpenSSH sshd and ssh, and Go stand-ins for the front's
// dial-home listener (a TLS terminator that passes GET /~!frp to frps, as
// Envoy did in spike S1c) and for the agent endpoint. Everything is on
// loopback. The rig's own listeners (the terminator, the agent endpoint,
// sshd's) take ports the kernel chooses; frps, which needs its ports in its
// configuration, takes free ones in 29280-29289, scanning down from 29289.
// The forwards' local ends are the kernel's, as in production.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frps's ports come from 29280-29289, the part of the #60 block that main's
// fixed-port tests leave free (cmd/hb-agent and internal/agent/enginedouble
// bind 29290-29299). internal/agentserver scans 29280-29289 upwards, so the
// rig scans downwards.
const (
	blockFirst = 29280
	blockLast  = 29289
)

// kernelListener binds a port the kernel chooses on 127.0.0.1.
func kernelListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// blockPorts returns n ports of 29280-29289 that are free now, for frps,
// which binds them itself.
func blockPorts(t *testing.T, n int) []int {
	t.Helper()
	var out []int
	for p := blockLast; p >= blockFirst && len(out) < n; p-- {
		if ln, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(p)); err == nil {
			ln.Close()
			out = append(out, p)
		}
	}
	if len(out) < n {
		t.Fatalf("only %d free ports in 29280-29289, want %d", len(out), n)
	}
	return out
}

func portOf(ln net.Listener) int { return ln.Addr().(*net.TCPAddr).Port }

// ---- processes ----

// procWatch records every process the rig and the code under test start, by
// PID and start time, and checks at the end that none is left.
type procWatch struct {
	mu  sync.Mutex
	ids map[agent.ProcessID]string
}

func newProcWatch() *procWatch { return &procWatch{ids: map[agent.ProcessID]string{}} }

func (w *procWatch) add(name string, id agent.ProcessID) {
	w.mu.Lock()
	w.ids[id] = name
	w.mu.Unlock()
}

func (w *procWatch) recorder(name string) func(agent.ProcessID) {
	return func(id agent.ProcessID) { w.add(name, id) }
}

// alive returns the recorded processes whose name starts with prefix and
// that still run.
func (w *procWatch) alive(prefix string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for id, name := range w.ids {
		if strings.HasPrefix(name, prefix) && id.Alive() {
			out = append(out, fmt.Sprintf("%s %d", name, id.PID))
		}
	}
	return out
}

// assertGone waits a little for every recorded process to be gone, kills
// what is left and fails the test for it. It also fails for any frpc, frps,
// ssh or sshd among this test process's descendants, recorded or not.
func (w *procWatch) assertGone(t *testing.T) {
	t.Helper()
	w.mu.Lock()
	counts := map[string]int{}
	for _, name := range w.ids {
		counts[name]++
	}
	w.mu.Unlock()
	t.Logf("checking the processes the test recorded: %v", counts)
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		var left []string
		for id, name := range w.ids {
			if id.Alive() {
				left = append(left, fmt.Sprintf("%s %d", name, id.PID))
			}
		}
		w.mu.Unlock()
		for _, id := range descendants(os.Getpid()) {
			exe, err := os.Readlink("/proc/" + strconv.Itoa(id.PID) + "/exe")
			if err != nil {
				continue
			}
			switch filepath.Base(exe) {
			case "frpc", "frps", "ssh", "sshd":
				if id.Alive() {
					w.add("unrecorded "+filepath.Base(exe), id)
					left = append(left, fmt.Sprintf("unrecorded %s %d", filepath.Base(exe), id.PID))
				}
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			w.mu.Lock()
			for id := range w.ids {
				_ = killID(id)
			}
			w.mu.Unlock()
			t.Errorf("processes left after the test: %s", strings.Join(left, ", "))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// descendants returns the live descendants of pid, by parent PID.
func descendants(pid int) []agent.ProcessID {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	children := map[int][]int{}
	for _, e := range entries {
		p, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		if len(f) < 2 {
			continue
		}
		pp, _ := strconv.Atoi(f[1])
		children[pp] = append(children[pp], p)
	}
	var out []agent.ProcessID
	var walk func(int)
	walk = func(p int) {
		for _, c := range children[p] {
			if id, err := procgroup.IDOf(c); err == nil {
				out = append(out, id)
			}
			walk(c)
		}
	}
	walk(pid)
	return out
}

// ---- frps ----

type frpsProc struct {
	bindPort, muxPort, webPort int
	password                   string
	bin, config                string
	g                          *procgroup.Group
	log                        string
	w                          *procWatch
}

// planFrps chooses frps's ports and writes its configuration, with HBEP-6's
// loopback settings and S1's heartbeats and no plugin: the agent's tests need
// no admission. Nothing starts yet.
func planFrps(t *testing.T, bin, dir string, w *procWatch) *frpsProc {
	t.Helper()
	ports := blockPorts(t, 3)
	f := &frpsProc{bindPort: ports[0], muxPort: ports[1], webPort: ports[2], password: "test-web-password",
		bin: bin, log: filepath.Join(dir, "frps.log"), w: w}
	cfg := fmt.Sprintf(`bindAddr = "127.0.0.1"
bindPort = %d
proxyBindAddr = "127.0.0.1"
tcpmuxHTTPConnectPort = %d
webServer.addr = "127.0.0.1"
webServer.port = %d
webServer.user = %q
webServer.password = %q
log.to = %q
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatTimeout = 25
transport.tcpKeepalive = 30
`, f.bindPort, f.muxPort, f.webPort, agentapi.FrpsWebUser, f.password, f.log)
	f.config = writeFile(t, filepath.Join(dir, "frps.toml"), []byte(cfg))
	t.Cleanup(f.stop)
	return f
}

// start starts frps and waits for its /healthz; false when it did not come
// up, as when another package took one of its ports first.
func (f *frpsProc) start(t *testing.T) bool {
	t.Helper()
	g, err := procgroup.Start(procgroup.Spec{Path: f.bin, Args: []string{"-c", f.config}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.g = g
	f.w.add("frps", g.ID())
	if f.waitHealthy(5 * time.Second) {
		return true
	}
	f.stop()
	return false
}

// startFrps plans and starts frps, with other ports when another package took
// one of them first.
func startFrps(t *testing.T, bin, dir string, w *procWatch) *frpsProc {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		if f := planFrps(t, bin, dir, w); f.start(t) {
			return f
		}
	}
	t.Fatal("frps did not come up")
	return nil
}

// restart stops frps and starts it again on the same ports.
func (f *frpsProc) restart(t *testing.T) {
	t.Helper()
	f.stop()
	if !f.start(t) {
		t.Fatal("frps did not come up again")
	}
}

func (f *frpsProc) waitHealthy(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		select {
		case <-f.g.Exited():
			return false
		default:
		}
		resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(f.webPort) + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return true
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

func (f *frpsProc) stop() {
	if f.g == nil {
		return
	}
	f.g.Stop(2 * time.Second)
	<-f.g.Done()
}

// online reports whether frps lists uid's tcpmux proxy as online (HBEP-6,
// Path state: GET /api/proxy/tcpmux).
func (f *frpsProc) online(t *testing.T, uid string) bool {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(f.webPort)+"/api/proxy/tcpmux", nil)
	req.SetBasicAuth(agentapi.FrpsWebUser, f.password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("frps's proxy list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("frps's proxy list: HTTP %d", resp.StatusCode)
	}
	var list struct {
		Proxies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"proxies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("frps's proxy list: %v", err)
	}
	for _, p := range list.Proxies {
		if p.Name == uid && p.Status == "online" {
			return true
		}
	}
	return false
}

// request sends CONNECT <uid>:80 to frps's tcpmux port, as the front does
// (HBEP-6, The front's half), then GET path through the tunnel. It returns
// the CONNECT status, and on 200 the reply's status and body.
func (f *frpsProc) request(uid, path string) (connect int, status int, body string, err error) {
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(f.muxPort), 2*time.Second)
	if err != nil {
		return 0, 0, "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(8 * time.Second))
	fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return 0, 0, "", fmt.Errorf("CONNECT: %w", err)
	}
	if resp.StatusCode != 200 {
		return resp.StatusCode, 0, "", nil
	}
	fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: engine\r\nConnection: close\r\n\r\n", path)
	r2, err := http.ReadResponse(br, nil)
	if err != nil {
		return 200, 0, "", fmt.Errorf("through the tunnel: %w", err)
	}
	defer r2.Body.Close()
	b, err := io.ReadAll(r2.Body)
	return 200, r2.StatusCode, string(b), err
}

// ---- the dial-home listener ----

// terminator stands in for the front's dial-home listener (HBEP-6, The
// dial-home listener's paths): it terminates TLS with the launcher's
// certificate and passes a GET /~!frp request, with its request line
// unchanged, to frps's bind port. Every other request gets 404.
type terminator struct {
	ln         net.Listener
	frps       string
	cert       atomic.Pointer[tls.Certificate]
	handshakes atomic.Int32 // TLS handshakes attempted
	failed     atomic.Int32 // of them, failed
	frpConns   atomic.Int32 // GET /~!frp passed to frps
	others     atomic.Int32 // any other request
	wg         sync.WaitGroup
	mu         sync.Mutex
	conns      map[net.Conn]bool
}

func startTerminator(t *testing.T, cert tls.Certificate, frpsAddr string) *terminator {
	t.Helper()
	m := &terminator{ln: kernelListener(t), frps: frpsAddr, conns: map[net.Conn]bool{}}
	m.cert.Store(&cert)
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return m.cert.Load(), nil
	}}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			c, err := m.ln.Accept()
			if err != nil {
				return
			}
			m.wg.Add(1)
			go func() { defer m.wg.Done(); m.serve(tls.Server(c, cfg)) }()
		}
	}()
	t.Cleanup(m.close)
	return m
}

func (m *terminator) port() int { return portOf(m.ln) }

// setCert makes the terminator present another certificate from its next
// handshake.
func (m *terminator) setCert(c tls.Certificate) { m.cert.Store(&c) }

// cut closes every connection the terminator carries, as a dropped path does.
func (m *terminator) cut() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for c := range m.conns {
		c.Close()
	}
}

func (m *terminator) track(c net.Conn, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if on {
		m.conns[c] = true
	} else {
		delete(m.conns, c)
	}
}

func (m *terminator) serve(c *tls.Conn) {
	m.track(c, true)
	defer m.track(c, false)
	defer c.Close()
	m.handshakes.Add(1)
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := c.Handshake(); err != nil {
		m.failed.Add(1)
		return
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	if !strings.HasPrefix(line, "GET /~!frp ") {
		m.others.Add(1)
		_, _ = io.WriteString(c, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	m.frpConns.Add(1)
	_ = c.SetDeadline(time.Time{})
	up, err := net.DialTimeout("tcp4", m.frps, 2*time.Second)
	if err != nil {
		return
	}
	m.track(up, true)
	defer m.track(up, false)
	defer up.Close()
	if _, err := io.WriteString(up, line); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

func (m *terminator) close() {
	m.ln.Close()
	m.mu.Lock()
	for c := range m.conns {
		c.Close()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// ---- the agent endpoint ----

// endpoint stands in for the agent endpoint (base+3): TLS with the
// launcher's certificate, answering registrations and counting them.
type endpoint struct {
	srv       *http.Server
	ln        net.Listener
	registers atomic.Int32
}

func startEndpointStandIn(t *testing.T, cert tls.Certificate) *endpoint {
	t.Helper()
	e := &endpoint{ln: kernelListener(t)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+agentapi.PathRegister, func(w http.ResponseWriter, r *http.Request) {
		e.registers.Add(1)
		w.Header().Set("Content-Type", agentapi.ContentType)
		_ = json.NewEncoder(w).Encode(agentapi.RegisterResponse{ProtocolVersion: agentapi.ProtocolVersion})
	})
	e.srv = &http.Server{Handler: mux, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = e.srv.ServeTLS(e.ln, "", "") }()
	t.Cleanup(func() { _ = e.srv.Close() })
	return e
}

func (e *endpoint) port() int { return portOf(e.ln) }

// ---- sshd ----

// sshServer is a user-level OpenSSH sshd run in inetd mode (sshd -i), one
// process per connection, behind a listener the rig holds, so its port
// cannot be lost and the rig can refuse connections for a while ("down"):
// then it closes each connection before sshd sees it, and ssh fails before
// it authenticates, as with no sshd.
type sshServer struct {
	t      *testing.T
	ln     net.Listener
	sshd   string
	config string
	log    *os.File
	w      *procWatch
	down   atomic.Bool
	mu     sync.Mutex
	procs  []*exec.Cmd
	wg     sync.WaitGroup
	closed atomic.Bool
}

// sshdBinary returns sshd's path. A missing sshd fails the test when CI asks
// for the tunnel tests (HPCBURST_REQUIRE_FRP=1) and skips it, saying so,
// otherwise.
func sshdBinary(t *testing.T) string {
	t.Helper()
	p := os.Getenv("HPCBURST_SSHD")
	if p == "" {
		p = "/usr/sbin/sshd"
	}
	if _, err := os.Stat(p); err != nil {
		if os.Getenv("HPCBURST_REQUIRE_FRP") == "1" {
			t.Fatalf("no sshd at %s (HPCBURST_SSHD): the ssh-mode tunnel tests need OpenSSH's server, and HPCBURST_REQUIRE_FRP=1 forbids skipping them", p)
		}
		t.Skipf("no sshd at %s (set HPCBURST_SSHD): the ssh-mode tunnel tests need OpenSSH's server", p)
	}
	return p
}

// sshFixture is the dial-home directory, sshd's configuration and agent.json
// in ssh mode.
type sshFixture struct {
	dialHome string
	user     string
}

func keygen(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "hb-test", "-f", path).CombinedOutput()
	if err != nil {
		t.Fatalf("ssh-keygen: %v: %s", err, out)
	}
	pub, err := os.ReadFile(path + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(pub))
}

// startSSHServer writes the keys, the restricted key's line of HBEP-6
// (Dial-home modes) with the two permitted ports, and sshd's configuration,
// and listens.
func startSSHServer(t *testing.T, bin, dir string, permit []int, w *procWatch) (*sshServer, sshFixture) {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	sdir := filepath.Join(dir, "sshd")
	dh := filepath.Join(dir, "dial-home")
	for _, d := range []string{sdir, dh} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	hostPub := keygen(t, filepath.Join(sdir, "host_key"))
	clientPub := keygen(t, filepath.Join(dh, agentapi.DialHomeKeyFile))
	opts := "restrict,port-forwarding"
	for _, p := range permit {
		opts += fmt.Sprintf(`,permitopen="127.0.0.1:%d"`, p)
	}
	opts += `,permitlisten="127.0.0.1:1",command="/bin/false"`
	writeFile(t, filepath.Join(sdir, "authorized_keys"), []byte(opts+" "+clientPub+"\n"))
	s := &sshServer{t: t, ln: kernelListener(t), sshd: bin, w: w}
	f := strings.Fields(hostPub)
	writeFile(t, filepath.Join(dh, agentapi.DialHomeKnownHostsFile), []byte(fmt.Sprintf("[127.0.0.1]:%d %s %s\n", s.port(), f[0], f[1])))
	s.config = writeFile(t, filepath.Join(sdir, "sshd_config"), []byte(strings.Join([]string{
		"HostKey " + filepath.Join(sdir, "host_key"),
		"PidFile none",
		"AuthorizedKeysFile " + filepath.Join(sdir, "authorized_keys"),
		"UsePAM no", "StrictModes no",
		"PasswordAuthentication no", "KbdInteractiveAuthentication no", "PubkeyAuthentication yes",
		"AllowUsers " + u.Username,
		"AllowTcpForwarding local", "AllowStreamLocalForwarding no", "GatewayPorts no",
		"X11Forwarding no", "PermitTTY no", "PrintMotd no",
		// HBEP-6, Dead nodes: the operator's setting on the control-plane host.
		"ClientAliveInterval 10", "ClientAliveCountMax 3",
		"LogLevel VERBOSE", "",
	}, "\n")))
	if s.log, err = os.Create(filepath.Join(sdir, "sshd.log")); err != nil {
		t.Fatal(err)
	}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.close)
	return s, sshFixture{dialHome: dh, user: u.Username}
}

func (s *sshServer) port() int { return portOf(s.ln) }

func (s *sshServer) accept() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		if s.down.Load() {
			c.Close()
			continue
		}
		s.spawn(c)
	}
}

func (s *sshServer) spawn(c net.Conn) {
	defer c.Close()
	f, err := c.(*net.TCPConn).File()
	if err != nil {
		return
	}
	defer f.Close()
	cmd := exec.Command(s.sshd, "-i", "-e", "-f", s.config)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = f, f, s.log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return
	}
	if id, err := procgroup.IDOf(cmd.Process.Pid); err == nil {
		s.w.add("sshd", id)
	}
	s.mu.Lock()
	s.procs = append(s.procs, cmd)
	s.mu.Unlock()
	// sshd's children can leave its group with setsid and outlive it,
	// reparented away from this process: record them while it runs.
	waited := make(chan struct{})
	s.wg.Add(2)
	go func() { defer s.wg.Done(); _ = cmd.Wait(); close(waited) }()
	go func() {
		defer s.wg.Done()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			for _, id := range descendants(cmd.Process.Pid) {
				s.w.add("sshd child", id)
			}
			select {
			case <-waited:
				return
			case <-tick.C:
			}
		}
	}()
}

// recordChildren records the sshd processes' descendants, which can leave
// their group with setsid, so the end-of-test check sees them.
func (s *sshServer) recordChildren() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.procs {
		for _, id := range descendants(c.Process.Pid) {
			s.w.add("sshd child", id)
		}
	}
}

// close stops accepting, then stops every sshd and its descendants: TERM to
// each, KILL after a second, by recorded PID and start time.
func (s *sshServer) close() {
	if s.closed.Swap(true) {
		return
	}
	s.ln.Close()
	s.recordChildren()
	s.mu.Lock()
	var ids []agent.ProcessID
	for _, c := range s.procs {
		if id, err := procgroup.IDOf(c.Process.Pid); err == nil {
			ids = append(ids, id)
		}
		ids = append(ids, descendants(c.Process.Pid)...)
	}
	s.mu.Unlock()
	for _, id := range ids {
		if id.Alive() {
			_ = syscall.Kill(id.PID, syscall.SIGTERM)
		}
	}
	deadline := time.Now().Add(time.Second)
	for _, id := range ids {
		for id.Alive() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		_ = killID(id)
	}
	s.wg.Wait()
	s.log.Close()
}

// ---- the engine behind the agent's serving socket ----

// startEngine serves a small HTTP engine on a Unix socket and the agent's
// Forwarder on the run directory's serving socket in front of it, as the
// agent does for worker-0 (HBEP-7, What it runs).
func startEngine(t *testing.T, runDir, uid string) string {
	t.Helper()
	sock := filepath.Join(runDir, "worker-0.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "engine for %s: %s", uid, r.URL.Path)
	})}
	go func() { _ = srv.Serve(ln) }()
	fwd, err := agent.NewForwarder(agent.ServingSocket(runDir))
	if err != nil {
		t.Fatal(err)
	}
	fwd.SetTarget(sock)
	t.Cleanup(func() { fwd.Close(); srv.Close() })
	return fwd.Path()
}

// ---- frpc ----

// newFrpc is frpc's settings as the agent gets them: the staged binary and
// its digest, the staged certificate and agent.json's pin.
func newFrpc(t *testing.T, bin, runDir, certFile, pin string, server agent.FrpcServer, w *procWatch) *agent.Frpc {
	t.Helper()
	digest := fileDigest(t, bin)
	return &agent.Frpc{
		Binary: bin, FrpcDigest: digest, CertFile: certFile, CertSPKISHA256: pin,
		Credential: hCredential, RunDir: runDir, Socket: agent.ServingSocket(runDir),
		Server: server, MinBackoff: 300 * time.Millisecond, MaxBackoff: time.Second,
		StopGrace: 2 * time.Second, OnStart: w.recorder("frpc"),
	}
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// runBackground runs fn until the test's end, and waits for it.
func runBackground(t *testing.T, name string, fn func(context.Context) error) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("%s returned %v", name, err)
				}
			case <-time.After(15 * time.Second):
				t.Errorf("%s did not return within 15 s of its stop", name)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// eventually polls cond until it holds or d passes.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// frpBins returns frps and frpc (testenv.FrpBinaries).
func frpBins(t *testing.T) (string, string) {
	t.Helper()
	return testenv.FrpBinaries(t)
}

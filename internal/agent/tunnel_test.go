package agent_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
	frps   *frpsProc
	term   *terminator
	frpc   *agent.Frpc
	frpcBn string
}

// newDirectRig runs frps, the terminator with termCert, the engine and frpc
// with the staged certificate staged and agent.json's pin.
func newDirectRig(t *testing.T, termCert tls.Certificate, staged []byte, pin string, before func()) *directRig {
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
	r.frps = startFrps(t, frpsBin, dir, w)
	r.term = startTerminator(t, termCert, "127.0.0.1:"+strconv.Itoa(r.frps.bindPort))
	startEngine(t, runDir, hInstUID)
	cert := writeFile(t, filepath.Join(dir, "dial-home.crt"), staged)
	server, err := agent.DirectServer("127.0.0.1:" + strconv.Itoa(r.term.port()))
	if err != nil {
		t.Fatal(err)
	}
	r.frpc = newFrpc(t, frpcBin, runDir, cert, pin, server, w)
	logs := &logBuf{}
	r.frpc.Logger = slog.New(slog.NewTextHandler(logs, nil))
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(r.frpc.LogPath())
			t.Logf("agent log:\n%s\nfrpc log:\n%s", logs.String(), b)
		}
	})
	if before != nil {
		before()
	}
	r.frpc.Serve(hInstUID)
	runBackground(t, "frpc", r.frpc.Run)
	return r
}

// frpc logs in and registers its proxy through the wss front; the front's
// CONNECT <UID>:80 gets 200 and the engine's reply through the agent's
// serving socket; an unknown UID gets 404 (HBEP-6, Test plan, Wiring). This
// is also the positive control of the certificate refusals below: the same
// rig with the pinned certificate on both ends.
func TestFrpcRegistersThroughTheWSSFront(t *testing.T) {
	a := makeCert(t, launcherName)
	r := newDirectRig(t, a.tls, a.pem, a.pin, nil)
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
}

// refused waits until frpc has tried the front at least twice, then checks
// that every TLS handshake failed, nothing reached frps and frps lists no
// proxy, so the front's CONNECT gets 404.
func (r *directRig) refused(t *testing.T) {
	t.Helper()
	eventually(t, 15*time.Second, "frpc to try the front twice", func() bool { return r.term.handshakes.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)
	if h, f := r.term.handshakes.Load(), r.term.failed.Load(); f != h {
		t.Fatalf("%d of %d TLS handshakes completed", h-f, h)
	}
	if n := r.term.frpConns.Load(); n != 0 {
		t.Fatalf("%d GET /~!frp reached frps", n)
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
	r := newDirectRig(t, other.tls, staged.pem, staged.pin, nil)
	r.refused(t)
}

// frpc with the right certificate but another server name is refused: the
// staged, pinned certificate is the one the front presents, but it does not
// name dial-home.hpcburst.invalid, which frpc checks (HBEP-6, Certificate
// checks).
func TestFrpcRefusesAnotherServerName(t *testing.T) {
	c := makeCert(t, "other.hpcburst.invalid")
	r := newDirectRig(t, c.tls, c.pem, c.pin, nil)
	r.refused(t)
}

// Proxy variables in the agent's environment do not reach frpc: frp takes
// http_proxy as its proxy to frps (HBEP-6, frp facts), so with it frpc would
// dial the trap below instead of the front. frpc logs in and the trap sees
// no connection.
func TestFrpcIgnoresTheProxyVariables(t *testing.T) {
	a := makeCert(t, launcherName)
	trap := blockListener(t)
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
	r := newDirectRig(t, a.tls, a.pem, a.pin, func() {
		for _, v := range []string{"http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
			t.Setenv(v, proxy)
		}
	})
	if r.frpc.Env != nil {
		t.Fatal("the rig must leave Env nil, so frpc starts from the agent's own environment")
	}
	eventually(t, 15*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	if _, status, _, err := r.frps.request(hInstUID, "/health"); err != nil || status != 200 {
		t.Fatalf("through the tunnel: %d, %v", status, err)
	}
	if n := trapped.Load(); n != 0 {
		t.Fatalf("frpc dialled the proxy from the agent's environment %d times", n)
	}
}

// ---- ssh mode: two forwards and frpc ----

type sshRig struct {
	t       *testing.T
	frps    *frpsProc
	term    *terminator
	ep      *endpoint
	sshd    *sshServer
	control *agent.SSHForward
	tunnel  *agent.SSHForward
	frpc    *agent.Frpc
	client  *agent.Client
	w       *procWatch
	logs    *logBuf

	mu       sync.Mutex
	lastSSH  map[string]agent.ProcessID
	frpcIDs  []agent.ProcessID
	onFrpcAt func(*sshRig, agent.ProcessID) // called inside frpc's OnStart
}

type sshRigOptions struct {
	sshdDown bool                           // sshd refuses connections at first
	onFrpc   func(*sshRig, agent.ProcessID) // called at each frpc start, on frpc's goroutine
}

func newSSHRig(t *testing.T, o sshRigOptions) *sshRig {
	t.Helper()
	frpsBin, frpcBin := frpBins(t)
	sshdBin := sshdBinary(t)
	sshBin, err := exec.LookPath("ssh")
	if err != nil {
		if os.Getenv("HPCBURST_REQUIRE_FRP") == "1" {
			t.Fatalf("no ssh: the ssh-mode tunnel tests need OpenSSH's client: %v", err)
		}
		t.Skipf("no ssh: the ssh-mode tunnel tests need OpenSSH's client: %v", err)
	}
	r := &sshRig{t: t, w: newProcWatch(), logs: &logBuf{}, lastSSH: map[string]agent.ProcessID{}, onFrpcAt: o.onFrpc}
	t.Cleanup(func() { r.w.assertGone(t) })
	dir := shortTemp(t)
	runDir := filepath.Join(dir, "run")
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	launcher := makeCert(t, launcherName)
	r.frps = startFrps(t, frpsBin, dir, r.w)
	r.term = startTerminator(t, launcher.tls, "127.0.0.1:"+strconv.Itoa(r.frps.bindPort))
	r.ep = startEndpointStandIn(t, launcher.tls)
	var fix sshFixture
	r.sshd, fix = startSSHServer(t, sshdBin, dir, []int{r.ep.port(), r.term.port()}, r.w)
	r.sshd.down.Store(o.sshdDown)
	startEngine(t, runDir, hInstUID)
	cert := writeFile(t, filepath.Join(dir, "dial-home.crt"), launcher.pem)
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: hTag, Mode: agentapi.DialHomeSSH,
		// The ssh server's port is not 22: agent.json's endpoint names it.
		Endpoint:       "127.0.0.1:" + strconv.Itoa(r.sshd.port()),
		SSH:            &agentapi.SSHForward{User: fix.user, AgentPort: r.ep.port()},
		CertSPKISHA256: launcher.pin, StagingDir: filepath.Join(dir, "staging"), DialHomeDir: fix.dialHome,
	}
	logger := slog.New(slog.NewTextHandler(r.logs, nil))
	sshLog, err := os.Create(filepath.Join(dir, "ssh.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sshLog.Close() })
	forward := func(name string, remote int) *agent.SSHForward {
		return &agent.SSHForward{
			SSH: sshBin, Config: cfg, RemotePort: remote, Name: name, Log: sshLog, Logger: logger,
			MinBackoff: 300 * time.Millisecond, MaxBackoff: time.Second, StopGrace: 2 * time.Second,
			OnStart: func(id agent.ProcessID) {
				r.w.add("ssh "+name, id)
				r.mu.Lock()
				r.lastSSH[name] = id
				r.mu.Unlock()
			},
		}
	}
	r.control = forward("control", r.ep.port())
	r.tunnel = forward("tunnel", r.term.port())
	r.frpc = newFrpc(t, frpcBin, runDir, cert, launcher.pin, agent.ForwardServer(r.tunnel), r.w)
	r.frpc.Logger = logger
	r.frpc.OnStart = func(id agent.ProcessID) {
		r.w.add("frpc", id)
		r.mu.Lock()
		r.frpcIDs = append(r.frpcIDs, id)
		f := r.onFrpcAt
		r.mu.Unlock()
		if f != nil {
			f(r, id)
		}
	}
	r.client = agent.NewClient(agent.ClientOptions{
		Dial: r.control.Dial, Pin: launcher.pin,
		Credential: hCredential, Tag: hTag, JobID: hJobID, AgentInstanceID: strings.Repeat("a", 32),
	})
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(r.frpc.LogPath())
			sb, _ := os.ReadFile(filepath.Join(dir, "sshd", "sshd.log"))
			cb, _ := os.ReadFile(filepath.Join(dir, "ssh.log"))
			t.Logf("agent log:\n%s\nfrpc log:\n%s\nsshd log:\n%s\nssh log:\n%s", r.logs.String(), b, sb, cb)
		}
	})
	runBackground(t, "the control forward", r.control.Run)
	runBackground(t, "the tunnel forward", r.tunnel.Run)
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	t.Cleanup(cancelWatch)
	go agent.OnPortChange(watchCtx, r.control, func(int) { r.client.CloseIdle() })
	r.frpc.Serve(hInstUID)
	runBackground(t, "frpc", r.frpc.Run)
	return r
}

func (r *sshRig) ssh(name string) agent.ProcessID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastSSH[name]
}

func (r *sshRig) frpcs() []agent.ProcessID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]agent.ProcessID(nil), r.frpcIDs...)
}

func (r *sshRig) register(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := r.client.Register(ctx, agentapi.RegisterRequest{Node: "node-test", CPUArch: "x86_64"})
	return err
}

func (r *sshRig) servesThroughTheTunnel(t *testing.T, d time.Duration) {
	t.Helper()
	eventually(t, d, "the engine's reply through the tunnel", func() bool {
		connect, status, body, err := r.frps.request(hInstUID, "/health")
		return err == nil && connect == 200 && status == 200 && body == "engine for "+hInstUID+": /health"
	})
}

// killSSH kills one forward's ssh, as a lost connection does, and returns
// the forward's port at that moment.
func (r *sshRig) killSSH(t *testing.T, name string, f *agent.SSHForward) (agent.ProcessID, int) {
	t.Helper()
	st, _ := f.State()
	id := r.ssh(name)
	if !id.Alive() {
		t.Fatalf("the %s forward's ssh %+v does not run", name, id)
	}
	if err := syscall.Kill(id.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	return id, st.Port
}

// In ssh mode both forwards carry their traffic: the agent's registration
// goes through connection 1 to the agent endpoint, frpc registers its proxy
// through connection 2 and the dial-home listener, and the front's CONNECT
// reaches the engine. The control traffic never uses frpc's connection: the
// dial-home listener sees nothing but GET /~!frp (HBEP-6, Test plan, Wiring).
func TestTunnelSSHModeCarriesBothConnections(t *testing.T) {
	r := newSSHRig(t, sshRigOptions{})
	eventually(t, 20*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	r.servesThroughTheTunnel(t, 10*time.Second)
	if err := r.register(t); err != nil {
		t.Fatalf("registration through connection 1: %v", err)
	}
	if n := r.ep.registers.Load(); n != 1 {
		t.Fatalf("the agent endpoint saw %d registrations, want 1", n)
	}
	if n := r.term.others.Load(); n != 0 {
		t.Fatalf("the dial-home listener saw %d requests other than GET /~!frp", n)
	}
	st, _ := r.tunnel.State()
	cst, _ := r.control.State()
	if addr, port := configServer(t, r.frpc.ConfigPath()); addr != "127.0.0.1" || port != st.Port {
		t.Fatalf("frpc dials %s:%d, want connection 2's 127.0.0.1:%d", addr, port, st.Port)
	}
	for _, p := range []int{st.Port, cst.Port} {
		if p < 32768 || p == r.ep.port() || p == r.term.port() {
			t.Fatalf("a forward listens on %d, not on a port the kernel chose", p)
		}
	}
	r.sshd.recordChildren()
}

// frpc never starts before connection 2 listens (HBEP-6, Dial-home modes).
// sshd refuses every connection for 3 s, so ssh never authenticates and
// never binds its forward: no frpc starts in that window, though both
// forwards keep trying. Once sshd serves, frpc starts, after the test's own
// observer saw connection 2's port listen, and with the port listening at
// the moment frpc is started. With loginFailExit = false an frpc started
// early would still register in the end, so the outcome alone proves
// nothing; the window and the order do.
func TestTunnelStartsFrpcOnlyOnceConnection2Listens(t *testing.T) {
	var mu sync.Mutex
	var firstListen time.Time
	type start struct {
		at        time.Time
		listening bool
		port      int
	}
	var starts []start
	r := newSSHRig(t, sshRigOptions{sshdDown: true, onFrpc: func(r *sshRig, _ agent.ProcessID) {
		_, port, err := readConfigServer(r.frpc.ConfigPath())
		s := start{at: time.Now(), listening: err == nil && loopbackListens(port), port: port}
		mu.Lock()
		starts = append(starts, s)
		mu.Unlock()
	}})
	stopObs := make(chan struct{})
	obsDone := make(chan struct{})
	go func() {
		defer close(obsDone)
		for {
			st, _ := r.tunnel.State()
			if st.Port != 0 && loopbackListens(st.Port) {
				mu.Lock()
				if firstListen.IsZero() {
					firstListen = time.Now()
				}
				mu.Unlock()
				return
			}
			select {
			case <-stopObs:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() { close(stopObs); <-obsDone })

	time.Sleep(3 * time.Second)
	mu.Lock()
	early := len(starts)
	mu.Unlock()
	if early != 0 {
		t.Fatalf("frpc started %d times while connection 2 could not listen", early)
	}
	if st, _ := r.tunnel.State(); st.Listening || st.Port == 0 || loopbackListens(st.Port) {
		t.Fatalf("connection 2 is %+v while sshd refuses: the window tests nothing", st)
	}
	if n := countLines(r.logs.String(), `msg="ssh forward started" forward=tunnel`); n < 2 {
		t.Fatalf("the tunnel forward tried %d times in the window; want it retrying", n)
	}
	r.sshd.down.Store(false)
	eventually(t, 20*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	mu.Lock()
	defer mu.Unlock()
	if len(starts) == 0 || firstListen.IsZero() {
		t.Fatalf("frpc starts %d, connection 2 first listened at %v", len(starts), firstListen)
	}
	if !starts[0].listening {
		t.Fatalf("frpc started on port %d while nothing listened there", starts[0].port)
	}
	if starts[0].at.Before(firstListen) {
		t.Fatalf("frpc started at %v, before connection 2 listened at %v", starts[0].at, firstListen)
	}
	r.sshd.recordChildren()
}

func countLines(s, sub string) int {
	n := 0
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		if strings.Contains(sc.Text(), sub) {
			n++
		}
	}
	return n
}

// takePort binds 127.0.0.1:port as soon as the killed ssh has let it go,
// and holds it until the test's end, as another process on the node would.
func takePort(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ln, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(port))
		if err == nil {
			t.Cleanup(func() { ln.Close() })
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not take port %d: %v", port, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// A port taken while its forward is down (HBEP-6, Test plan, Port change).
// Connection 2: the forward binds a new port, frpc's configuration is
// rewritten with it, frpc is restarted, and the new frpc registers through
// the new port. Connection 1: the forward moves, the control client follows
// it, and frpc is not restarted.
func TestTunnelRestartsFrpcWhenConnection2sPortChanges(t *testing.T) {
	r := newSSHRig(t, sshRigOptions{})
	eventually(t, 20*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	r.servesThroughTheTunnel(t, 10*time.Second)
	first := r.frpcs()
	if len(first) != 1 {
		t.Fatalf("frpc started %d times before any port changed", len(first))
	}

	_, old := r.killSSH(t, "tunnel", r.tunnel)
	takePort(t, old)
	eventually(t, 20*time.Second, "connection 2 to listen on a new port", func() bool {
		st, _ := r.tunnel.State()
		return st.Listening && st.Port != old && st.PortChanges == 1
	})
	st, _ := r.tunnel.State()
	eventually(t, 20*time.Second, "a new frpc", func() bool { return len(r.frpcs()) == 2 })
	if first[0].Alive() {
		t.Fatalf("the frpc started for port %d still runs", old)
	}
	if _, port := configServer(t, r.frpc.ConfigPath()); port != st.Port {
		t.Fatalf("frpc's configuration has port %d, want connection 2's new %d", port, st.Port)
	}
	r.servesThroughTheTunnel(t, 20*time.Second)

	// Connection 1 moves the control client and leaves frpc alone.
	frpcNow := r.frpcs()[1]
	_, oldControl := r.killSSH(t, "control", r.control)
	takePort(t, oldControl)
	eventually(t, 20*time.Second, "connection 1 to listen on a new port", func() bool {
		st, _ := r.control.State()
		return st.Listening && st.Port != oldControl && st.PortChanges == 1
	})
	eventually(t, 20*time.Second, "a registration through connection 1's new port", func() bool { return r.register(t) == nil })
	if n := len(r.frpcs()); n != 2 || !frpcNow.Alive() {
		t.Fatalf("connection 1's port change restarted frpc: %d starts, the last alive %v", n, frpcNow.Alive())
	}
	r.servesThroughTheTunnel(t, 5*time.Second)
	r.sshd.recordChildren()
}

// Killing either forward's ssh reconnects it on the same port. frpc survives
// connection 1's loss, and the tunnel serves throughout; after connection 2's
// loss the same frpc logs in again by itself (HBEP-6, Dial-home modes).
func TestTunnelReconnectsEachForward(t *testing.T) {
	r := newSSHRig(t, sshRigOptions{})
	eventually(t, 20*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	r.servesThroughTheTunnel(t, 10*time.Second)
	if err := r.register(t); err != nil {
		t.Fatal(err)
	}
	frpc := r.frpcs()[0]

	killed, port := r.killSSH(t, "control", r.control)
	if _, status, _, err := r.frps.request(hInstUID, "/health"); err != nil || status != 200 {
		t.Fatalf("the tunnel during connection 1's loss: %d, %v", status, err)
	}
	eventually(t, 20*time.Second, "connection 1 to reconnect", func() bool {
		st, _ := r.control.State()
		id := r.ssh("control")
		return st.Listening && id != killed && id.Alive()
	})
	if st, _ := r.control.State(); st.Port != port || st.PortChanges != 0 {
		t.Fatalf("connection 1 came back as %+v, want port %d kept", st, port)
	}
	eventually(t, 20*time.Second, "a registration after connection 1's reconnect", func() bool { return r.register(t) == nil })
	if !frpc.Alive() || len(r.frpcs()) != 1 {
		t.Fatalf("frpc did not survive connection 1's loss: alive %v, %d starts", frpc.Alive(), len(r.frpcs()))
	}

	killed, port = r.killSSH(t, "tunnel", r.tunnel)
	eventually(t, 20*time.Second, "connection 2 to reconnect", func() bool {
		st, _ := r.tunnel.State()
		id := r.ssh("tunnel")
		return st.Listening && id != killed && id.Alive()
	})
	if st, _ := r.tunnel.State(); st.Port != port || st.PortChanges != 0 {
		t.Fatalf("connection 2 came back as %+v, want port %d kept", st, port)
	}
	r.servesThroughTheTunnel(t, 30*time.Second)
	if !frpc.Alive() || len(r.frpcs()) != 1 {
		t.Fatalf("connection 2's reconnect on the same port restarted frpc: alive %v, %d starts", frpc.Alive(), len(r.frpcs()))
	}
	r.sshd.recordChildren()
}

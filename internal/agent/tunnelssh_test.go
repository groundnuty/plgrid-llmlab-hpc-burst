package agent_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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
	sshBin  string
	fix     sshFixture
	cfg     agentapi.AgentConfig
	// stopFrpc, stopTunnel and stopControl stop the agent's parts.
	stopFrpc, stopTunnel, stopControl func()

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
	r.sshBin = sshBin
	var fix sshFixture
	r.sshd, fix = startSSHServer(t, sshdBin, dir, []int{r.ep.port(), r.term.port()}, r.w)
	r.fix = fix
	r.sshd.down.Store(o.sshdDown)
	startEngine(t, runDir, hInstUID)
	cert := writeFile(t, filepath.Join(dir, "dial-home.crt"), launcher.pem)
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: hTag, Mode: agentapi.DialHomeSSH,
		// The ssh server's port is not 22: agent.json's endpoint names it.
		Endpoint:       "127.0.0.1:" + strconv.Itoa(r.sshd.port()),
		SSH:            &agentapi.SSHForward{User: fix.user, AgentPort: r.ep.port(), DialHomePort: r.term.port()},
		CertSPKISHA256: launcher.pin, StagingDir: filepath.Join(dir, "staging"), DialHomeDir: fix.dialHome,
	}
	r.cfg = cfg
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
	r.stopControl = runBackground(t, "the control forward", r.control.Run)
	r.stopTunnel = runBackground(t, "the tunnel forward", r.tunnel.Run)
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	t.Cleanup(cancelWatch)
	go agent.OnPortChange(watchCtx, r.control, func(int) { r.client.CloseIdle() })
	r.frpc.Serve(hInstUID)
	r.stopFrpc = runBackground(t, "frpc", r.frpc.Run)
	return r
}

// listening waits until both forwards listen. A test that uses connection 1
// waits for it: frpc's login says only that connection 2 is up, and under
// load connection 1 can come up later (the CI failure of run 41244220574).
func (r *sshRig) listening(t *testing.T) {
	t.Helper()
	for _, f := range []*agent.SSHForward{r.control, r.tunnel} {
		eventually(t, 20*time.Second, "the "+f.Name+" forward to listen", func() bool {
			st, _ := f.State()
			return st.Listening
		})
	}
}

// endsCleanly ends the Instance, then stops the agent's frpc and both
// forwards while frps and sshd still run, and checks that frps drops the
// proxy and that sshd's sessions end by themselves, before the fixture's
// cleanup could hide either (Astra nit 3).
func (r *sshRig) endsCleanly(t *testing.T) {
	t.Helper()
	r.sshd.recordChildren()
	r.frpc.Serve("")
	eventually(t, 10*time.Second, "frps to drop the ended Instance's proxy", func() bool { return !r.frps.online(t, hInstUID) })
	r.stopFrpc()
	r.stopTunnel()
	r.stopControl()
	for _, prefix := range []string{"frpc", "ssh "} {
		if left := r.w.alive(prefix); len(left) != 0 {
			t.Fatalf("%v run after the agent's parts stopped", left)
		}
	}
	eventually(t, 10*time.Second, "sshd's sessions to end by themselves", func() bool { return len(r.w.alive("sshd")) == 0 })
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
	eventually(t, 20*time.Second, "the "+name+" forward to listen", func() bool {
		st, _ := f.State()
		return st.Listening
	})
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
	r.listening(t)
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
	lo, hi := ephemeralRange(t)
	for _, p := range []int{st.Port, cst.Port} {
		if p < lo || p > hi || p == r.ep.port() || p == r.term.port() || p == r.sshd.port() {
			t.Fatalf("a forward listens on %d, not on a port the kernel chose (%d-%d)", p, lo, hi)
		}
	}
	r.endsCleanly(t)
}

// frpc never starts before connection 2 listens (HBEP-6, Dial-home modes).
// sshd refuses every connection for 3 s, so ssh never authenticates and
// never binds its forward: no frpc starts in that window, though both
// forwards keep trying. Once sshd serves, frpc starts, and at the moment it
// is started (inside OnStart, synchronously) the test's own read of
// /proc/net/tcp finds the tunnel ssh's port listening. With loginFailExit =
// false an frpc started early would still register in the end, so the
// outcome alone proves nothing; the window and the synchronized observation
// do. No second clock is compared (Astra nit 4, Opus N5).
func TestTunnelStartsFrpcOnlyOnceConnection2Listens(t *testing.T) {
	var mu sync.Mutex
	type start struct {
		listening bool
		port      int
		err       error
	}
	var starts []start
	r := newSSHRig(t, sshRigOptions{sshdDown: true, onFrpc: func(r *sshRig, _ agent.ProcessID) {
		_, port, err := readConfigServer(r.frpc.ConfigPath())
		s := start{listening: err == nil && loopbackListens(port), port: port, err: err}
		mu.Lock()
		starts = append(starts, s)
		mu.Unlock()
	}})
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
	first := starts
	mu.Unlock()
	if len(first) == 0 {
		t.Fatal("frpc never started")
	}
	if !first[0].listening {
		t.Fatalf("frpc started on port %d while nothing listened there (%v)", first[0].port, first[0].err)
	}
	r.endsCleanly(t)
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
	r.endsCleanly(t)
}

// Killing either forward's ssh reconnects it on the same port. frpc survives
// connection 1's loss, and the tunnel serves throughout; after connection 2's
// loss the same frpc logs in again by itself (HBEP-6, Dial-home modes).
func TestTunnelReconnectsEachForward(t *testing.T) {
	r := newSSHRig(t, sshRigOptions{})
	eventually(t, 20*time.Second, "frps to list the Instance's proxy online", func() bool { return r.frps.online(t, hInstUID) })
	r.servesThroughTheTunnel(t, 10*time.Second)
	r.listening(t)
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
	r.endsCleanly(t)
}

// The restricted key's line (HBEP-6, Dial-home modes; Test plan, Key
// restriction; Opus N7): with it, the two permitted forwards work (the tests
// above); a forward to a third port, a remote forward and a command are each
// refused.
func TestTheRestrictedKeyRefusesEverythingElse(t *testing.T) {
	r := newSSHRig(t, sshRigOptions{})
	r.listening(t)
	third := kernelListener(t)
	var reached atomic.Int32
	go func() {
		for {
			c, err := third.Accept()
			if err != nil {
				return
			}
			reached.Add(1)
			c.Close()
		}
	}()
	t.Cleanup(func() { third.Close() })

	// A forward to a third port: ssh binds its end, sshd refuses the channel.
	f := &agent.SSHForward{SSH: r.sshBin, Config: r.cfg, RemotePort: portOf(third), Name: "third", Log: io.Discard,
		MinBackoff: 300 * time.Millisecond, MaxBackoff: time.Second, StopGrace: 2 * time.Second, OnStart: r.w.recorder("ssh third")}
	stop := runBackground(t, "the third forward", f.Run)
	eventually(t, 20*time.Second, "the third forward to listen", func() bool { st, _ := f.State(); return st.Listening })
	st, _ := f.State()
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(st.Port), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(make([]byte, 1)); n != 0 || err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("through a forward to a third port: read %d bytes, %v; want the channel refused", n, err)
	}
	c.Close()
	stop()
	if n := reached.Load(); n != 0 {
		t.Fatalf("a forward to a third port reached it %d times", n)
	}

	base := []string{"-F", "/dev/null", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + filepath.Join(r.fix.dialHome, agentapi.DialHomeKnownHostsFile),
		"-i", filepath.Join(r.fix.dialHome, agentapi.DialHomeKeyFile), "-p", strconv.Itoa(r.sshd.port())}
	dest := r.fix.user + "@127.0.0.1"
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"a remote forward", append(append([]string{"-N", "-o", "ExitOnForwardFailure=yes"}, base...), "-R", "127.0.0.1:0:127.0.0.1:"+strconv.Itoa(portOf(third)), dest)},
		{"a command", append(append([]string{}, base...), dest, "echo", "HB-KEY-MARKER")},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cmd := exec.CommandContext(ctx, r.sshBin, tc.args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		out, err := cmd.CombinedOutput() // waits for ssh and reaps it
		timedOut := ctx.Err() != nil
		cancel()
		if err == nil || timedOut {
			t.Fatalf("%s: ssh exited %v (timed out %v); want it refused:\n%s", tc.name, err, timedOut, out)
		}
		t.Logf("%s: refused: %v: %s", tc.name, err, strings.TrimSpace(string(out)))
		if strings.Contains(string(out), "HB-KEY-MARKER") {
			t.Fatalf("%s: the command ran:\n%s", tc.name, out)
		}
	}
	r.endsCleanly(t)
}

// ephemeralRange is the kernel's range for the ports it chooses.
func ephemeralRange(t *testing.T) (int, int) {
	t.Helper()
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		t.Fatalf("ip_local_port_range %q", b)
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil {
		t.Fatalf("ip_local_port_range %q", b)
	}
	return lo, hi
}

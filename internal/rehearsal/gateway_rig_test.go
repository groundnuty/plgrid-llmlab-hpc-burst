package rehearsal

// New. The gateway's test rig: a gateway over a real sshd, two echo servers
// standing for the agent endpoint and the front's dial-home listener, and the
// agent's own ssh arguments (internal/agent.SSHForward) to drive it. Every
// wait is on observable state (a forward listening, a process exiting, a
// reply) with a generous deadline.

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// echoServer is a loopback listener on a kernel-chosen port. It reads one line
// from each connection, answers tag+line and closes, and records what it saw,
// so a test can tell which listener a forward reached and in which direction
// bytes went.
type echoServer struct {
	tag  string
	ln   net.Listener
	port int
	wg   sync.WaitGroup

	mu    sync.Mutex
	conns int
	lines []string
}

func startEcho(t *testing.T, tag string) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &echoServer{tag: tag, ln: ln, port: ln.Addr().(*net.TCPAddr).Port}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			e.mu.Lock()
			e.conns++
			e.mu.Unlock()
			e.wg.Add(1)
			go e.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close(); e.wg.Wait() })
	return e
}

func (e *echoServer) serve(c net.Conn) {
	defer e.wg.Done()
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(waitDeadline))
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	e.mu.Lock()
	e.lines = append(e.lines, line)
	e.mu.Unlock()
	_, _ = io.WriteString(c, e.tag+line)
}

func (e *echoServer) accepted() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conns
}

func (e *echoServer) received() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.lines...)
}

// exchange sends payload and a newline to whatever dial reaches and reads to
// the end of the connection.
func exchange(dial func(context.Context) (net.Conn, error), payload string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), waitDeadline)
	defer cancel()
	c, err := dial(ctx)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(waitDeadline))
	if _, err := io.WriteString(c, payload+"\n"); err != nil {
		return "", err
	}
	b, err := io.ReadAll(c)
	return string(b), err
}

// dialPort dials 127.0.0.1:port.
func dialPort(port int) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp4", "127.0.0.1:"+strconv.Itoa(port))
	}
}

// freePort asks the kernel for a loopback port and releases it.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// syncBuf collects the output of ssh processes, for a failed test's log.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type gwRig struct {
	t     *testing.T
	base  string // a short directory of this test
	gwDir string // the gateway's directory
	dh    string // the dial-home directory WriteDialHome filled
	ssh   string
	gw    *Gateway
	agent *echoServer // stands for the agent endpoint (AgentPort)
	dial  *echoServer // stands for the dial-home listener (DialHomePort)
	out   *syncBuf    // what the ssh processes of the test printed
	watch *watch      // the end-of-test check that nothing is left
}

func newGatewayRig(t *testing.T) *gwRig {
	t.Helper()
	RequireTools(t)
	base, err := os.MkdirTemp("", "rg")
	if err != nil {
		t.Fatal(err)
	}
	sshBin, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	r := &gwRig{t: t, base: base, gwDir: filepath.Join(base, "g"), dh: filepath.Join(base, "dh"), ssh: sshBin, out: &syncBuf{}}
	if err := os.Mkdir(r.gwDir, 0o700); err != nil {
		t.Fatal(err)
	}
	w := startWatch(t, base)
	r.watch = w
	t.Cleanup(func() {
		w.finish()
		_ = os.RemoveAll(base) // this test's own directory
	})
	r.agent, r.dial = startEcho(t, "agent:"), startEcho(t, "dialhome:")
	r.gw, err = StartGateway(GatewayConfig{Dir: r.gwDir, AgentPort: r.agent.port, DialHomePort: r.dial.port})
	if err != nil {
		t.Fatalf("StartGateway: %v", err)
	}
	t.Cleanup(func() {
		if err := r.gw.Close(); err != nil {
			t.Errorf("Gateway.Close: %v", err)
		}
	})
	t.Cleanup(func() { // a diagnostic for a failed test; no test asserts on it
		if b, err := os.ReadFile(r.gw.files.log); t.Failed() && err == nil {
			t.Logf("sshd's log:\n%s\nssh's output:\n%s", b, r.out)
		}
	})
	if err := r.gw.WriteDialHome(r.dh); err != nil {
		t.Fatalf("WriteDialHome: %v", err)
	}
	return r
}

// config is agent.json in ssh mode for the gateway, with dir as the dial-home
// directory.
func (r *gwRig) config(dir string) agentapi.AgentConfig {
	return agentapi.AgentConfig{
		Mode: agentapi.DialHomeSSH, Endpoint: r.gw.Addr(), DialHomeDir: dir,
		SSH: &agentapi.SSHForward{User: r.gw.User(), AgentPort: r.agent.port, DialHomePort: r.dial.port},
	}
}

func (r *gwRig) forwarder(dir string, remote int) *agent.SSHForward {
	return &agent.SSHForward{SSH: r.ssh, Config: r.config(dir), RemotePort: remote, Name: "rehearsal", Log: r.out}
}

// args is the agent's own ssh argument vector for a forward to remote.
func (r *gwRig) args(dir string, remote int) []string {
	r.t.Helper()
	args, err := r.forwarder(dir, remote).Args(freePort(r.t))
	if err != nil {
		r.t.Fatal(err)
	}
	return args
}

// runningForward is one agent.SSHForward in its reconnect loop.
type runningForward struct {
	*agent.SSHForward
	t      *testing.T
	cancel context.CancelFunc
	done   chan struct{}
}

// forward runs the agent's own forward to remote until the test ends and
// waits until its ssh has authenticated and listens locally.
func (r *gwRig) forward(dir string, remote int) *runningForward {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &runningForward{SSHForward: r.forwarder(dir, remote), t: r.t, cancel: cancel, done: make(chan struct{})}
	go func() { defer close(f.done); _ = f.Run(ctx) }()
	r.t.Cleanup(f.stop)
	f.waitState("the forward to listen", func(s agent.ForwardState) bool { return s.Listening })
	return f
}

// stop ends the forward and waits until its ssh is gone and reaped.
func (f *runningForward) stop() {
	f.cancel()
	await(f.t, "the forward to stop", f.done)
}

func (f *runningForward) waitState(what string, ok func(agent.ForwardState) bool) {
	f.t.Helper()
	deadline := time.After(waitDeadline)
	for {
		st, changed := f.State()
		if ok(st) {
			return
		}
		select {
		case <-changed:
		case <-f.done:
			f.t.Fatalf("the forward's Run returned while waiting for %s", what)
		case <-deadline:
			f.t.Fatalf("timed out after %s waiting for %s", waitDeadline, what)
		}
	}
}

// sshProc is one ssh process the test runs itself.
type sshProc struct {
	t    *testing.T
	cmd  *exec.Cmd
	out  *syncBuf // standard output and standard error together
	done chan struct{}
	code int
}

// startSSH starts ssh with args. Standard input is empty. The process is
// killed, by its PID, and reaped when the test ends.
func (r *gwRig) startSSH(args []string) *sshProc {
	r.t.Helper()
	p := &sshProc{t: r.t, out: &syncBuf{}, done: make(chan struct{})}
	p.cmd = exec.Command(r.ssh, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	go func() {
		_ = p.cmd.Wait() // the exit code is read from the process state
		p.code = p.cmd.ProcessState.ExitCode()
		close(p.done)
	}()
	r.t.Cleanup(func() {
		_ = p.cmd.Process.Kill() // ESRCH after an exit is not an error here
		<-p.done
		r.out.Write([]byte(p.out.String()))
	})
	return p
}

func (p *sshProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// wait returns the exit code once ssh has exited by itself.
func (p *sshProc) wait(what string) int {
	p.t.Helper()
	await(p.t, what, p.done)
	return p.code
}

// gwProcs lists, with no waiting, the live processes of the gateway: an sshd
// below this test process, and any process whose command line or environment
// names the gateway's directory.
func (r *gwRig) gwProcs() []string {
	var left []string
	for _, p := range below(os.Getpid()) {
		if b, _ := os.ReadFile("/proc/" + strconv.Itoa(p.pid) + "/comm"); strings.HasPrefix(string(b), "sshd") {
			left = append(left, strconv.Itoa(p.pid)+" sshd")
		}
	}
	for _, p := range liveProcs() {
		if p.pid != os.Getpid() && namesDir(p.pid, r.gwDir) {
			left = append(left, strconv.Itoa(p.pid)+" (names the gateway's directory)")
		}
	}
	return left
}

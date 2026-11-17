package node

// The log as a real child: hpcburst-controller's eventlog subcommand, built once, run in a process
// group of its own on fixed loopback ports (a restarted log must come back on the same port), and
// stopped or killed through a pidfd opened right after its start. The wait status is read, so a log
// that died of something other than the signal sent fails the test (S8's harness did the same).
// Never a recorded PID, never a signal to a process group (IMPL-COMMON, 12 and 13 Nov 2026).
//
// Ports: #625's block is 28480-28519, and 28500-28509 belong to these tests (IMPL-COMMON).

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

const (
	clientPort  = 28500
	monitorPort = 28501
	// waitFor bounds every wait on observable state; the waits return as soon as it holds.
	waitFor = 60 * time.Second
)

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}

// controllerBinary builds hpcburst-controller once per run, or uses the one named by
// $HPCBURST_CONTROLLER_BIN.
func controllerBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("HPCBURST_CONTROLLER_BIN"); p != "" {
		return p
	}
	binOnce.Do(func() {
		if binDir, binErr = os.MkdirTemp("", "hbelog"); binErr != nil {
			return
		}
		out, err := exec.Command("go", "build", "-o", filepath.Join(binDir, "hpcburst-controller"), "github.com/groundnuty/plgrid-llmlab-hpc-burst/cmd/hpcburst-controller").CombinedOutput()
		if err != nil {
			binErr = fmt.Errorf("go build of hpcburst-controller: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return filepath.Join(binDir, "hpcburst-controller")
}

// logDir is the launcher's <state>/eventlog: the configuration, the token and the caps.
type logDir struct {
	dir, token string
}

func newLogDir(t *testing.T, caps eventlog.Caps) *logDir {
	t.Helper()
	d := &logDir{dir: filepath.Join(t.TempDir(), "eventlog")}
	if err := os.Mkdir(d.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	if d.token, err = eventlog.NewToken(); err != nil {
		t.Fatal(err)
	}
	conf, err := eventlog.ConfText(d.dir, clientPort, monitorPort, d.token)
	if err != nil {
		t.Fatal(err)
	}
	d.writeConf(t, conf)
	d.setCaps(t, caps)
	return d
}

func (d *logDir) confPath() string { return filepath.Join(d.dir, eventlog.ConfFile) }
func (d *logDir) capsPath() string { return filepath.Join(d.dir, eventlog.CapsFile) }

func (d *logDir) writeConf(t *testing.T, text string) {
	t.Helper()
	if err := os.WriteFile(d.confPath(), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (d *logDir) setCaps(t *testing.T, c eventlog.Caps) {
	t.Helper()
	if err := eventlog.WriteCaps(d.capsPath(), c); err != nil {
		t.Fatal(err)
	}
}

// syncBuffer is a writer whose snapshot can be read while the child still writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// logProc is one run of the eventlog subcommand.
type logProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	pidfd  int
	out    *syncBuffer
	exited chan struct{}
}

// startLog starts the log as a child in its own process group. The pidfd is opened at once: the
// child is ours and has not been waited for, so its PID cannot belong to anything else.
func startLog(t *testing.T, d *logDir) *logProc {
	t.Helper()
	cmd := exec.Command(controllerBinary(t), "eventlog", "--config", d.confPath(), "--caps-file", d.capsPath())
	cmd.Env = []string{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &logProc{t: t, cmd: cmd, out: &syncBuffer{}, exited: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.out, p.out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("pidfd_open: %v", err)
	}
	p.pidfd = fd
	go func() { _ = cmd.Wait(); close(p.exited) }()
	t.Cleanup(p.reap)
	return p
}

// reap kills what is still running, through the pidfd, and waits for it.
func (p *logProc) reap() {
	select {
	case <-p.exited:
	default:
		_ = unix.PidfdSendSignal(p.pidfd, syscall.SIGKILL, nil, 0)
		select {
		case <-p.exited:
		case <-time.After(30 * time.Second):
			p.t.Errorf("the log (pid %d) did not exit after SIGKILL", p.cmd.Process.Pid)
		}
	}
	_ = unix.Close(p.pidfd)
}

func (p *logProc) waitExit() syscall.WaitStatus {
	p.t.Helper()
	select {
	case <-p.exited:
	case <-time.After(waitFor):
		p.t.Fatalf("the log did not exit within %s; its output:\n%s", waitFor, p.out)
	}
	return p.cmd.ProcessState.Sys().(syscall.WaitStatus)
}

// ready waits for the READY line and returns its fields. The log that exits first fails the test.
func (p *logProc) ready() map[string]string {
	p.t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(p.out.String(), "\n") {
			if strings.HasPrefix(line, "READY ") {
				f := map[string]string{}
				for _, kv := range strings.Fields(line)[1:] {
					k, v, _ := strings.Cut(kv, "=")
					f[k] = v
				}
				return f
			}
		}
		select {
		case <-p.exited:
			p.t.Fatalf("the log exited before it was ready (%v); its output:\n%s", p.cmd.ProcessState, p.out)
		case <-time.After(20 * time.Millisecond):
		}
	}
	p.t.Fatalf("no READY line within %s; the log's output:\n%s", waitFor, p.out)
	return nil
}

// kill sends SIGKILL through the pidfd and requires the wait status to say SIGKILL.
func (p *logProc) kill() {
	p.t.Helper()
	if err := unix.PidfdSendSignal(p.pidfd, syscall.SIGKILL, nil, 0); err != nil {
		p.t.Fatalf("SIGKILL through the pidfd: %v", err)
	}
	if ws := p.waitExit(); !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		p.t.Fatalf("the log's wait status is %v, want killed by SIGKILL; its output:\n%s", ws, p.out)
	}
}

// stop sends SIGTERM through the pidfd and requires a clean exit, status 0.
func (p *logProc) stop() {
	p.t.Helper()
	if err := unix.PidfdSendSignal(p.pidfd, syscall.SIGTERM, nil, 0); err != nil {
		p.t.Fatalf("SIGTERM through the pidfd: %v", err)
	}
	if ws := p.waitExit(); !ws.Exited() || ws.ExitStatus() != 0 {
		p.t.Fatalf("the log's wait status after SIGTERM is %v, want exit 0; its output:\n%s", ws, p.out)
	}
}

// refused waits for the log to exit with the refusal's status and returns its output.
func (p *logProc) refused() string {
	p.t.Helper()
	ws := p.waitExit()
	if !ws.Exited() || ws.ExitStatus() != eventlog.ExitRefused {
		p.t.Fatalf("the log's wait status is %v, want exit %d (refused); its output:\n%s", ws, eventlog.ExitRefused, p.out)
	}
	return p.out.String()
}

func clientURL() string { return fmt.Sprintf("nats://127.0.0.1:%d", clientPort) }

// connect connects to the running log with the token.
func connect(t *testing.T, token string) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(clientURL(), nats.Token(token), nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("connect with the token: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return nc, js
}

// publish stores one message under id and returns the server's answer.
func publish(ctx context.Context, js jetstream.JetStream, subject, id string, payload []byte) error {
	_, err := js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: payload}, jetstream.WithMsgID(id))
	return err
}

func streamInfo(t *testing.T, js jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", name, err)
	}
	return info
}

// nothingListens fails unless the port refuses connections: the log is not serving.
func nothingListens(t *testing.T, port int) {
	t.Helper()
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second); err == nil {
		c.Close()
		t.Fatalf("something listens on port %d", port)
	}
}

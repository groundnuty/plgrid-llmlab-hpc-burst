// Package nodetest is the rig that runs the durable log as a real child for the tests of the
// programs that talk to it (HBEP-8, The durable log; issue #625): "hpcburst-controller eventlog",
// built once, run in a process group of its own on fixed loopback ports (a restarted log must come
// back on the same port), and stopped or killed through a pidfd opened right after its start. The
// wait status is read, so a log that died of something other than the signal sent fails the test.
// Never a recorded PID, never a signal to a process group (IMPL-COMMON, 12 and 13 Nov 2026).
//
// Origin: internal/eventlog/node's own tests (child_test.go, PR #639, this repository), whose
// helpers hard-code their ports and live in a _test file, so another package cannot import them.
// This copy takes the ports as a parameter. Those tests are left as they are.
//
// Ports: #625's block is 28480-28519 (IMPL-COMMON). 28500-28509 belong to internal/eventlog/node's
// own tests and 28510-28519 to the publisher's and the forwarder's rigs, which pass their pair here.
package nodetest

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

// WaitFor bounds every wait on observable state; the waits return as soon as it holds.
const WaitFor = 60 * time.Second

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

// Cleanup removes the controller binary a run built. The package's TestMain calls it after the
// tests; it does nothing when $HPCBURST_CONTROLLER_BIN named the binary.
func Cleanup() {
	if binDir != "" {
		os.RemoveAll(binDir)
	}
}

// ControllerBinary builds hpcburst-controller once per run, or uses the one named by
// $HPCBURST_CONTROLLER_BIN.
func ControllerBinary(t testing.TB) string {
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

// Ports are the log's client port and its monitoring port.
type Ports struct{ Client, Monitor int }

// Addr is the client address, as the front and the controllers dial it.
func (p Ports) Addr() string { return fmt.Sprintf("127.0.0.1:%d", p.Client) }

// URL is the client address as a NATS URL.
func (p Ports) URL() string { return "nats://" + p.Addr() }

// Dir is the launcher's <state>/eventlog: the configuration, the token and the caps, and the
// store the log makes beneath it.
type Dir struct {
	Path  string
	Token string
	Ports Ports
}

// NewDir makes the directory (mode 700) with a configuration for the ports, a new token, the
// token's own file (mode 600), and the caps.
func NewDir(t testing.TB, ports Ports, caps eventlog.Caps) *Dir {
	t.Helper()
	d := &Dir{Path: filepath.Join(t.TempDir(), "eventlog"), Ports: ports}
	if err := os.Mkdir(d.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	if d.Token, err = eventlog.NewToken(); err != nil {
		t.Fatal(err)
	}
	conf, err := eventlog.ConfText(d.Path, ports.Client, ports.Monitor, d.Token)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(d.ConfPath(), []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(d.TokenFile(), []byte(d.Token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	d.SetCaps(t, caps)
	return d
}

// ConfPath, CapsPath and TokenFile are the three files the launcher writes.
func (d *Dir) ConfPath() string  { return filepath.Join(d.Path, eventlog.ConfFile) }
func (d *Dir) CapsPath() string  { return filepath.Join(d.Path, eventlog.CapsFile) }
func (d *Dir) TokenFile() string { return filepath.Join(d.Path, eventlog.TokenFile) }

// SetCaps writes caps.json, which the log applies at its next start.
func (d *Dir) SetCaps(t testing.TB, c eventlog.Caps) {
	t.Helper()
	if err := eventlog.WriteCaps(d.CapsPath(), c); err != nil {
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

// Proc is one run of the eventlog subcommand.
type Proc struct {
	t      testing.TB
	cmd    *exec.Cmd
	pidfd  int
	out    *syncBuffer
	exited chan struct{}
}

// Start starts the log as a child in its own process group. The pidfd is opened at once: the
// child is ours and has not been waited for, so its PID cannot belong to anything else. The log
// is killed through the pidfd when the test ends, if it still runs.
func (d *Dir) Start(t testing.TB) *Proc {
	t.Helper()
	cmd := exec.Command(ControllerBinary(t), "eventlog", "--config", d.ConfPath(), "--caps-file", d.CapsPath())
	cmd.Env = []string{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &Proc{t: t, cmd: cmd, out: &syncBuffer{}, exited: make(chan struct{})}
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
func (p *Proc) reap() {
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

// Exited is closed once the log has exited and been waited for.
func (p *Proc) Exited() <-chan struct{} { return p.exited }

// Output is what the log has written so far.
func (p *Proc) Output() string { return p.out.String() }

func (p *Proc) waitExit() syscall.WaitStatus {
	p.t.Helper()
	select {
	case <-p.exited:
	case <-time.After(WaitFor):
		p.t.Fatalf("the log did not exit within %s; its output:\n%s", WaitFor, p.out)
	}
	return p.cmd.ProcessState.Sys().(syscall.WaitStatus)
}

// Ready waits for the READY line and returns its fields. The log that exits first fails the test.
func (p *Proc) Ready() map[string]string {
	p.t.Helper()
	deadline := time.Now().Add(WaitFor)
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
	p.t.Fatalf("no READY line within %s; the log's output:\n%s", WaitFor, p.out)
	return nil
}

// SendKill sends SIGKILL through the pidfd and returns at once. It is safe from any goroutine,
// the test's own or a hook's, and it fails nothing: WaitKilled reads the outcome.
func (p *Proc) SendKill() error {
	return unix.PidfdSendSignal(p.pidfd, syscall.SIGKILL, nil, 0)
}

// WaitKilled waits for the log to exit and requires the wait status to say SIGKILL, so a log that
// died of something else fails the test.
func (p *Proc) WaitKilled() {
	p.t.Helper()
	if ws := p.waitExit(); !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		p.t.Fatalf("the log's wait status is %v, want killed by SIGKILL; its output:\n%s", ws, p.out)
	}
}

// Kill sends SIGKILL through the pidfd and requires the wait status to say SIGKILL.
func (p *Proc) Kill() {
	p.t.Helper()
	if err := p.SendKill(); err != nil {
		p.t.Fatalf("SIGKILL through the pidfd: %v", err)
	}
	p.WaitKilled()
}

// Stop sends SIGTERM through the pidfd and requires a clean exit, status 0.
func (p *Proc) Stop() {
	p.t.Helper()
	if err := unix.PidfdSendSignal(p.pidfd, syscall.SIGTERM, nil, 0); err != nil {
		p.t.Fatalf("SIGTERM through the pidfd: %v", err)
	}
	if ws := p.waitExit(); !ws.Exited() || ws.ExitStatus() != 0 {
		p.t.Fatalf("the log's wait status after SIGTERM is %v, want exit 0; its output:\n%s", ws, p.out)
	}
}

// Connect connects a fresh client to the running log with the token: a client the test owns, never
// the code under test's.
func (d *Dir) Connect(t testing.TB) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(d.Ports.URL(), nats.Token(d.Token), nats.MaxReconnects(0))
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

// StreamInfo reads a stream's information through js.
func StreamInfo(t testing.TB, js jetstream.JetStream, name string) *jetstream.StreamInfo {
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

// Message is one message of a stream as a fresh reader sees it: its sequence, the subject it was
// published on, its id and its bytes. (Subject was added for the event publisher's tests, which
// check that each event is on its object's subject; #625, PR E2.)
type Message struct {
	Seq     uint64
	Subject string
	ID      string
	Payload []byte
}

// ReadAll reads every message of a stream, from its first sequence, through an ordered consumer of
// a fresh client: never through the publisher, whose acknowledgements prove only what it saw.
func (d *Dir) ReadAll(t testing.TB, stream string) []Message {
	t.Helper()
	_, js := d.Connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", stream, err)
	}
	if info.State.Msgs == 0 {
		return nil
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		t.Fatalf("ordered consumer on %s: %v", stream, err)
	}
	var out []Message
	for uint64(len(out)) < info.State.Msgs {
		msg, err := cons.Next(jetstream.FetchMaxWait(20 * time.Second))
		if err != nil {
			t.Fatalf("read %s: message %d of %d: %v", stream, len(out)+1, info.State.Msgs, err)
		}
		md, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, Message{Seq: md.Sequence.Stream, Subject: msg.Subject(), ID: msg.Headers().Get(nats.MsgIdHdr), Payload: append([]byte(nil), msg.Data()...)})
	}
	return out
}

// NothingListens fails unless the port refuses connections: the log is not serving.
func NothingListens(t testing.TB, port int) {
	t.Helper()
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second); err == nil {
		c.Close()
		t.Fatalf("something listens on port %d", port)
	}
}

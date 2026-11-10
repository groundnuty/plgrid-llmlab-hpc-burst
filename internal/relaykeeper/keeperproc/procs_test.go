package keeperproc

// The test's own processes, the keeper and the controller doubles, and how a
// test signals a process: never by a numeric PID (the science agent's rule
// after the incident of #308). A child the test started is held by the pidfd
// its clone returned (SysProcAttr.PidFD). A grandchild, a keeper's relay ssh,
// is pinned by a pidfd and verified, while it is held, as this test process's
// descendant along the expected chain (pin, copied from
// internal/hostdial/pins_test.go): only a verified pin is ever signalled.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// child is a process this test started, held by the pidfd its clone
// returned. Its standard input is the write end the test holds: closing it
// stops the child.
type child struct {
	name string
	cmd  *exec.Cmd
	fd   int
	pid  int
	in   *os.File
	done chan struct{}
}

// startChild starts cmd, whose standard input is the read end of a pipe whose
// write end is in, with a pidfd from clone. Its clean-up closes in, waits for
// the exit, and only past a deadline sends KILL through the pidfd; then it
// closes the pidfd.
func startChild(t *testing.T, name string, cmd *exec.Cmd, in *os.File) *child {
	t.Helper()
	fd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	cmd.WaitDelay = 10 * time.Second
	if err := cmd.Start(); err != nil {
		_ = in.Close()
		t.Fatalf("start the %s: %v", name, err)
	}
	c := &child{name: name, cmd: cmd, fd: fd, pid: cmd.Process.Pid, in: in, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(c.done)
	}()
	t.Cleanup(func() {
		_ = c.in.Close()
		if !c.wait(time.Minute) {
			t.Errorf("the %s did not end within a minute of its input's end: killed through its pidfd", name)
			_ = c.signal(syscall.SIGKILL)
			<-c.done
		}
		if c.fd >= 0 {
			_ = unix.Close(c.fd)
		}
	})
	if fd < 0 {
		t.Fatalf("the %s's start got no pidfd from clone", name)
	}
	return c
}

// signal sends sig through the child's pidfd.
func (c *child) signal(sig syscall.Signal) error { return unix.PidfdSendSignal(c.fd, sig, nil, 0) }

// wait reports whether the child has exited, and been reaped, within d.
func (c *child) wait(d time.Duration) bool {
	select {
	case <-c.done:
		return true
	case <-time.After(d):
		return false
	}
}

// kill sends KILL through the child's pidfd and waits for its exit.
func (c *child) kill(t *testing.T) {
	t.Helper()
	if err := c.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("KILL the %s through its pidfd: %v", c.name, err)
	}
	if !c.wait(30 * time.Second) {
		t.Fatalf("the %s did not exit within 30 s of its KILL", c.name)
	}
	if ws, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("the %s ended %v, not by its KILL", c.name, c.cmd.ProcessState)
	}
}

// lineLog keeps what a child writes, by line.
type lineLog struct {
	mu      sync.Mutex
	partial []byte
	lines   []string
}

func (l *lineLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partial = append(l.partial, p...)
	for {
		i := strings.IndexByte(string(l.partial), '\n')
		if i < 0 {
			break
		}
		l.lines = append(l.lines, string(l.partial[:i]))
		l.partial = l.partial[i+1:]
	}
	return len(p), nil
}

// mark is how many lines there are now: a later read since the mark sees
// only what came after it.
func (l *lineLog) mark() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

// since is the lines after mark m that hold every one of parts.
func (l *lineLog) since(m int, parts ...string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines[min(m, len(l.lines)):] {
		ok := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, line)
		}
	}
	return out
}

// waitLine waits until a line after mark m holds every one of parts, and
// returns it.
func (l *lineLog) waitLine(t *testing.T, m int, what string, parts ...string) string {
	t.Helper()
	var got []string
	waitFor(t, time.Minute, what, func() bool {
		got = l.since(m, parts...)
		return len(got) > 0
	})
	return got[0]
}

func (l *lineLog) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n") + "\n" + string(l.partial)
}

// The keeper's log lines that the tests read: hostdial's, with the relay's
// tag and channel (internal/hostdial/owner.go).
const (
	logStarted    = `msg="relay started"`
	logNotStarted = `msg="a relay did not start"`
	logSeeded     = `msg="a relay that ended for good is not started"`
	whyExpired    = "desired set has expired"
	whyNotListed  = "is not listed under tag"
	whyGate       = "the start was not allowed (Target.BeforeStart)"
)

// relayLog is the parts of a log line of the relay of tag on ch.
func relayLog(msg, tag string, ch agentapi.RelayChannel) []string {
	return []string{msg, "tag=" + tag, "channel=" + string(ch)}
}

// keeper is a relay keeper process, its state directory and its log.
type keeper struct {
	t     *testing.T
	w     *world
	dir   string
	c     *child
	log   *lineLog
	epoch int64
	sock  string
}

// startKeeper starts a keeper on the state directory dir, a new one when dir
// is "", and returns once its socket serves.
func (w *world) startKeeper(dir string) *keeper {
	t := w.t
	t.Helper()
	if dir == "" {
		d, err := os.MkdirTemp("", "kp")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(d) })
		dir = d
	}
	if n := len(relaykeeper.SocketPath(dir)); n > agentapi.MaxRelaySocketPath {
		t.Fatalf("the keeper's socket path is %d bytes, over %d: set a shorter TMPDIR", n, agentapi.MaxRelaySocketPath)
	}
	ready := filepath.Join(dir, "ready.json")
	if err := os.Remove(ready); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	cfg, _ := json.Marshal(keeperConfig{StateDir: dir, Control: w.targets[control].port(), Serving: w.targets[serving].port(), Ready: ready,
		MinBackoffMS: 200, MaxBackoffMS: 2000, StopGraceMS: 2000})
	cfgPath := filepath.Join(dir, "keeper.json")
	if err := os.WriteFile(cfgPath, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	k := &keeper{t: t, w: w, dir: dir, log: &lineLog{}}
	// The keeper's log is shown when the test fails, and with
	// HB_TEST_KEEPER_LOG=1 when it passes too.
	t.Cleanup(func() {
		if t.Failed() || os.Getenv("HB_TEST_KEEPER_LOG") == "1" {
			t.Logf("the keeper's log:\n%s", tail(k.log.all(), 300))
		}
	})
	cmd := exec.Command(exe, keeperRole, cfgPath)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, k.log, k.log
	k.c = startChild(t, "keeper", cmd, inW)
	_ = inR.Close()
	var r keeperReady
	waitFor(t, time.Minute, "the keeper's socket", func() bool {
		select {
		case <-k.c.done:
			t.Fatalf("the keeper exited before its socket served: %v", k.c.cmd.ProcessState)
		default:
		}
		b, err := os.ReadFile(ready)
		return err == nil && json.Unmarshal(b, &r) == nil
	})
	if r.PID != k.c.pid || r.Socket != relaykeeper.SocketPath(dir) {
		t.Fatalf("the keeper's ready file %+v names another process or socket", r)
	}
	k.epoch, k.sock = r.Epoch, r.Socket
	return k
}

// stop ends the keeper's input, as its supervisor's stop does in effect: it
// stops every relay by pidfd and exits 0.
func (k *keeper) stop() {
	k.t.Helper()
	_ = k.c.in.Close()
	if !k.c.wait(time.Minute) {
		_ = k.c.signal(syscall.SIGKILL)
		<-k.c.done
		k.t.Fatal("the keeper did not stop within a minute of its input's end: killed through its pidfd")
	}
	if code := k.c.cmd.ProcessState.ExitCode(); code != 0 {
		k.t.Fatalf("the keeper exited %d at its stop, want 0", code)
	}
}

// restart stops the keeper and starts another on its state directory.
func (k *keeper) restart() *keeper {
	k.t.Helper()
	k.stop()
	return k.w.startKeeper(k.dir)
}

// status is the keeper's status report, read over its socket from the test
// process, as the serving copy's collector reads it: read-only and unfenced.
func (k *keeper) status() wire.StatusReport {
	k.t.Helper()
	var r wire.StatusReport
	if err := wire.Exchange(context.Background(), k.sock, wire.KindStatus, struct{}{}, &r); err != nil {
		k.t.Fatalf("the keeper's status: %v", err)
	}
	return r
}

// provider is one Provider's part of the status, and whether it has one.
func (k *keeper) provider(p prov) (wire.ProviderStatus, bool) {
	k.t.Helper()
	for _, ps := range k.status().Providers {
		if ps.Provider == p.name {
			return ps, true
		}
	}
	return wire.ProviderStatus{}, false
}

// relaysOf are the Provider's relays of the Allocation tagged tag.
func (k *keeper) relaysOf(p prov, tag string) []wire.RelayState {
	k.t.Helper()
	ps, _ := k.provider(p)
	var out []wire.RelayState
	for _, r := range ps.Relays {
		if r.Tag == tag {
			out = append(out, r)
		}
	}
	return out
}

// upOf is the relay of tag's channel that runs, and whether one does.
func (k *keeper) upOf(p prov, tag string, ch agentapi.RelayChannel) (wire.RelayState, bool) {
	k.t.Helper()
	for _, r := range k.relaysOf(p, tag) {
		if r.Channel == ch && r.State == wire.StateUp && r.PID != 0 {
			return r, true
		}
	}
	return wire.RelayState{}, false
}

// waitUp waits until both relays of j run under the keeper, each with a
// process none of not names, and the job side has a session on both
// channels; it returns the relays.
func (k *keeper) waitUp(p prov, j job, not ...int) map[agentapi.RelayChannel]wire.RelayState {
	k.t.Helper()
	out := map[agentapi.RelayChannel]wire.RelayState{}
	waitFor(k.t, 90*time.Second, "both relays of "+j.tag+" up under the keeper", func() bool {
		for _, ch := range agentapi.RelayChannels {
			r, ok := k.upOf(p, j.tag, ch)
			if !ok || containsInt(not, r.PID) {
				return false
			}
			out[ch] = r
		}
		st := j.status(k.t)
		return st[control].Connected && st[serving].Connected
	})
	return out
}

func containsInt(s []int, n int) bool {
	for _, v := range s {
		if v == n {
			return true
		}
	}
	return false
}

// pins pins both relays' ssh processes, verified as the keeper's children.
func (k *keeper) pins(up map[agentapi.RelayChannel]wire.RelayState) map[agentapi.RelayChannel]*pinned {
	k.t.Helper()
	out := map[agentapi.RelayChannel]*pinned{}
	for ch, r := range up {
		p, err := pin(k.t, r.PID, k.c.pid)
		if err != nil {
			k.t.Fatalf("pin the %s relay's ssh %d as the keeper's child: %v", ch, r.PID, err)
		}
		out[ch] = p
	}
	return out
}

// double is a controller double process.
type double struct {
	t    *testing.T
	name string
	c    *child
	out  *os.File
	r    *bufio.Reader
	log  *lineLog
}

// startDouble starts a controller double on the keeper's socket sock.
func (w *world) startDouble(name, sock string) *double {
	t := w.t
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	d := &double{t: t, name: name, out: outR, r: bufio.NewReader(outR), log: &lineLog{}}
	t.Cleanup(func() {
		_ = outR.Close()
		if t.Failed() {
			if s := d.log.all(); strings.TrimSpace(s) != "" {
				t.Logf("the %s double's standard error:\n%s", name, tail(s, 50))
			}
		}
	})
	cmd := exec.Command(exe, controllerRole, sock)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, d.log
	d.c = startChild(t, name+" double", cmd, inW)
	_ = inR.Close()
	_ = outW.Close()
	return d
}

// do sends one command and returns the double's answer.
func (d *double) do(c command) answer {
	d.t.Helper()
	b, _ := json.Marshal(c)
	_ = d.c.in.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := d.c.in.Write(append(b, '\n')); err != nil {
		d.t.Fatalf("the %s double's command %s: %v", d.name, c.Op, err)
	}
	_ = d.out.SetReadDeadline(time.Now().Add(time.Minute))
	line, err := d.r.ReadBytes('\n')
	if err != nil {
		d.t.Fatalf("the %s double's answer to %s: %v", d.name, c.Op, err)
	}
	var a answer
	if err := json.Unmarshal(line, &a); err != nil {
		d.t.Fatalf("the %s double's answer to %s: %v", d.name, c.Op, err)
	}
	return a
}

// leadEvery is how often a leading double re-sends its set.
const leadEvery = 250

// set sends s once; lead sends it and goes on re-sending it, as a leader;
// pause stops a lead.
func (d *double) set(s wire.DesiredSet) answer { return d.do(command{Op: "set", Set: &s}) }
func (d *double) lead(s wire.DesiredSet) answer {
	return d.do(command{Op: "lead", Set: &s, EveryMS: leadEvery})
}
func (d *double) pause() { d.do(command{Op: "pause"}) }

// ack sends a once.
func (d *double) ack(a wire.Ack) answer { return d.do(command{Op: "ack", Ack: &a}) }

// kill sends KILL through the double's pidfd and waits for its exit.
func (d *double) kill() { d.c.kill(d.t) }

// mustReport is a's report, failing the test on anything else.
func mustReport(t *testing.T, a answer, what string) wire.Report {
	t.Helper()
	if a.Kind != string(wire.KindReport) || a.Report == nil {
		t.Fatalf("%s: answered %s %+v %s, want a report", what, a.Kind, a.Refused, a.Error)
	}
	return *a.Report
}

// mustRefuse is a's refusal, failing the test on anything else.
func mustRefuse(t *testing.T, a answer, what string) wire.Refused {
	t.Helper()
	if a.Kind != string(wire.KindRefused) || a.Refused == nil {
		t.Fatalf("%s: answered %s %s, want a refusal", what, a.Kind, a.Error)
	}
	return *a.Refused
}

// pinned is a process pinned by a pidfd (internal/hostdial/pins_test.go).
type pinned struct {
	pid, fd  int
	verified bool
}

// pin opens a pidfd on pid and registers its close, and only its close, at
// the test's end. Then it verifies, while it holds the pidfd, that the
// process descends from this test process through parents, its parent first:
// each process's parent is the next one (the last one's is this test
// process), and each started no earlier than its parent. Only a pin that
// passes is verified, and only then is its KILL at the test's end
// registered, which runs before the close. A pin that fails is returned with
// its error, and is never signalled.
func pin(t *testing.T, pid int, parents ...int) (*pinned, error) {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("pidfd_open of process %d: %w", pid, err)
	}
	p := &pinned{pid: pid, fd: fd}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if err := checkChain(readProcRow, os.Getpid(), p.pid, parents, p.exited); err != nil {
		return p, err
	}
	p.verified = true
	t.Cleanup(func() { _ = p.signal(syscall.SIGKILL) })
	return p, nil
}

// procRow is one read of a process's /proc row: its parent and its start
// time, in clock ticks since boot.
type procRow struct {
	pid, ppid int
	ticks     uint64
}

func readProcRow(pid int) (procRow, error) {
	ppid, ticks, err := statOf(pid)
	return procRow{pid: pid, ppid: ppid, ticks: ticks}, err
}

// checkChain walks up from pid to self along parents, each hop's row read
// once, giving both its start time, no earlier than its parent's, and its
// parent, the next hop; last, exited, the pidfd's view, must show the pinned
// process alive, so its own row was its own (internal/hostdial/pins_test.go).
func checkChain(read func(int) (procRow, error), self, pid int, parents []int, exited func() bool) error {
	chain := append(append([]int{pid}, parents...), self)
	child, err := read(pid)
	if err != nil {
		return err
	}
	for i := 1; i < len(chain); i++ {
		want := chain[i]
		if child.ppid != want {
			return fmt.Errorf("process %d's parent is %d, not %d", child.pid, child.ppid, want)
		}
		parent, err := read(want)
		if err != nil {
			return err
		}
		if parent.ticks > child.ticks {
			return fmt.Errorf("process %d started before its parent %d: a reused PID", child.pid, want)
		}
		child = parent
	}
	if exited() {
		return fmt.Errorf("process %d exited during the check", pid)
	}
	return nil
}

// errUnverified is a signal refused to a pin that failed its check.
var errUnverified = errors.New("the pin was not verified: not signalled")

// signal sends sig through the pin's pidfd; an unverified pin is refused.
func (p *pinned) signal(sig syscall.Signal) error {
	if !p.verified {
		return errUnverified
	}
	return unix.PidfdSendSignal(p.fd, sig, nil, 0)
}

// exited reports whether the pinned process has exited.
func (p *pinned) exited() bool {
	pfd := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(pfd, 0)
	return err == nil && n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0
}

// waitExit waits until the pinned process has exited.
func (p *pinned) waitExit(t *testing.T, d time.Duration, what string) {
	t.Helper()
	end := time.Now().Add(d)
	for {
		pfd := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
		n, err := unix.Poll(pfd, 100)
		if err != nil && err != unix.EINTR {
			t.Fatalf("poll %s's pidfd: %v", what, err)
		}
		if n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("%s did not exit within %s", what, d)
		}
	}
}

// kill sends KILL through a verified pin and waits for the exit.
func (p *pinned) kill(t *testing.T, what string) {
	t.Helper()
	if err := p.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("KILL %s through its pin: %v", what, err)
	}
	p.waitExit(t, 30*time.Second, what)
}

// statOf reads a process's parent and start time from /proc/<pid>/stat
// (fields 4 and 22, counted after the command name's last ')').
func statOf(pid int) (ppid int, ticks uint64, err error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, 0, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, 0, fmt.Errorf("/proc/%d/stat is short", pid)
	}
	if ppid, err = strconv.Atoi(f[1]); err != nil {
		return 0, 0, err
	}
	ticks, err = strconv.ParseUint(f[19], 10, 64)
	return ppid, ticks, err
}

// procState is a process's state letter from /proc ("" when it is gone).
func procState(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return ""
	}
	return s[i+2 : i+3]
}

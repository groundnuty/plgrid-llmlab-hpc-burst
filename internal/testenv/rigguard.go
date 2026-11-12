package testenv

// What the rigs that start a real frps need so that a killed test binary cannot poison the next
// rig (#484): a check that a rig's ports are free, a process identity that is the launcher's own
// record (PID, start time, boot ID) with a wait through a pidfd, and a child test binary that a
// test starts and kills.
//
// A test binary that `go test -timeout` panics, or that an outer time limit kills, runs no
// t.Cleanup. A frps it started in a session of its own then stays on the rig's fixed ports, and
// the next rig finds /healthz already answered by it, with plugins that point at a dead address,
// and fails naming the wrong cause. The rigs therefore refuse a port that something holds
// (RequireFreePorts), and the leak tests show that nothing outlives a killed test binary.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// FreePorts checks that every address can be listened on, by listening on it and closing it: not by
// asking whether something answers there, since a frps left by a killed test binary answers
// /healthz, and one that is frozen or half-dead answers nothing and still holds the port. The error
// names each address it could not listen on, with its port.
func FreePorts(addrs ...string) error {
	var bad []string
	for _, a := range addrs {
		ln, err := net.Listen("tcp", a)
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s (port %s): %v", a, portOf(a), err))
			continue
		}
		_ = ln.Close()
	}
	if len(bad) == 0 {
		return nil
	}
	return fmt.Errorf("not free: %s", strings.Join(bad, "; "))
}

func portOf(addr string) string {
	if _, p, err := net.SplitHostPort(addr); err == nil {
		return p
	}
	return "?"
}

// RequireFreePorts fails the test, naming each port, when an address of the rig cannot be listened
// on. rig says which rig it is ("the frp rig"). Something from an earlier run, a frps a killed test
// binary left behind, is the usual holder.
func RequireFreePorts(t testing.TB, rig string, addrs ...string) {
	t.Helper()
	if err := FreePorts(addrs...); err != nil {
		t.Fatalf("%s cannot start, because something else holds its ports (%v)\na frps left by a test binary that was killed holds them until it is stopped: find it by the record of its start, never by its name", rig, err)
	}
}

// ProcID identifies one process the way the launcher's records do: a PID alone is not an identity,
// since the kernel reuses PIDs, and the start time (clock ticks since boot, field 22 of
// /proc/<pid>/stat) together with the boot ID is. launcher.ProcRec has the same three values; this
// type is separate because the launcher's tests import this package.
type ProcID struct {
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	BootID     string `json:"boot_id"`
}

// Proc is a process held by a pidfd, after its identity was checked while the pidfd was held, so
// that nothing sent through it can reach another process that reused the PID.
type Proc struct {
	id ProcID
	fd int
}

// ErrProcGone says that no process matches the identity: it has exited, or the PID names another
// process now.
var ErrProcGone = errors.New("no process matches the identity")

// OpenProc takes a pidfd for the PID and confirms that it is the identified process. It returns
// ErrProcGone when it is not or is no more, and an error when that cannot be established.
func OpenProc(id ProcID) (*Proc, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, fmt.Errorf("read the boot ID: %w", err)
	}
	if got := strings.TrimSpace(string(boot)); id.BootID == "" || got != id.BootID {
		return nil, fmt.Errorf("the identity's boot ID %q is not this boot's %q: %w", id.BootID, got, ErrProcGone)
	}
	if id.PID <= 1 || id.StartTicks == 0 {
		return nil, fmt.Errorf("the identity %+v lacks a PID or a start time", id)
	}
	fd, err := unix.PidfdOpen(id.PID, 0)
	switch {
	case errors.Is(err, syscall.ESRCH), errors.Is(err, syscall.EINVAL):
		return nil, fmt.Errorf("pid %d: %w", id.PID, ErrProcGone)
	case err != nil:
		return nil, fmt.Errorf("pidfd_open %d: %w", id.PID, err)
	}
	p := &Proc{id: id, fd: fd}
	// The start time is read while the pidfd is held: if the process had gone and the PID been reused
	// before the pidfd was taken, the time is another's. One that exited after the pidfd was taken
	// is still the process it was.
	ticks, err := startTicks(id.PID)
	if err != nil || ticks != id.StartTicks {
		p.Close()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("pid %d: %w", id.PID, ErrProcGone)
	}
	return p, nil
}

// startTicks is the start time of the process, from /proc/<pid>/stat.
func startTicks(pid int) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	return parseStartTicks(string(b))
}

// parseStartTicks reads field 22 of a /proc/<pid>/stat line. The command name, field 2, may hold
// spaces and parentheses, so the fields are counted from the last closing parenthesis.
func parseStartTicks(stat string) (uint64, error) {
	i := strings.LastIndexByte(stat, ')')
	if i < 0 {
		return 0, fmt.Errorf("a stat line has no command name: %q", stat)
	}
	f := strings.Fields(stat[i+1:])
	// f[0] is field 3 (the state), so field 22 is f[19].
	if len(f) < 20 {
		return 0, fmt.Errorf("a stat line has %d fields after the command name, want at least 20: %q", len(f), stat)
	}
	return strconv.ParseUint(f[19], 10, 64)
}

// WaitExit waits until the process has exited, through the pidfd's readiness (poll), which the
// kernel signals when the process terminates, whether or not anyone has reaped it. d only bounds a
// wait that would never end. It reports whether the process exited.
func (p *Proc) WaitExit(d time.Duration) bool {
	end := time.Now().Add(d)
	for {
		left := time.Until(end)
		if left < 0 {
			left = 0
		}
		fds := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int(left.Milliseconds()))
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case err != nil:
			return false
		}
		return n > 0
	}
}

// Kill sends SIGKILL through the held pidfd. It is for a process this test found alive that must
// not stay: the pidfd was verified, so no other process is reached.
func (p *Proc) Kill() error { return unix.PidfdSendSignal(p.fd, unix.SIGKILL, nil, 0) }

// Close closes the pidfd.
func (p *Proc) Close() { _ = unix.Close(p.fd) }

// A Child is this test binary run on one test alone, as a subprocess of the test that started it,
// as the repo's other self-exec tests do (TestEnvoyBinaryProbe). The test it runs is a no-op unless
// it was started this way (ChildRecordPath). It writes a record, which the parent reads
// (WaitRecord), and then waits to be killed. The parent kills it through its pidfd (Kill): nothing
// is sent to a process group, and the child shares the parent's.
type Child struct {
	t      testing.TB
	cmd    *exec.Cmd
	pidfd  int
	record string
	out    string
	done   chan struct{}
	killed bool
}

// childRecordEnv is the variable by which the parent tells the child where to write its record.
const childRecordEnv = "HPCBURST_TESTENV_CHILD_RECORD"

// ChildRecordPath is where a test run as a Child writes its record, and false when the test was
// not started as one, which is when the test returns at once.
func ChildRecordPath() (string, bool) {
	p := os.Getenv(childRecordEnv)
	return p, p != ""
}

// WriteChildRecord writes v, as JSON, to the record path, by a temporary file and a rename, so the
// parent never reads half of it.
func WriteChildRecord(t testing.TB, v any) {
	t.Helper()
	path, ok := ChildRecordPath()
	if !ok {
		t.Fatal("WriteChildRecord outside a child test")
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// StartChild runs this test binary on the test named test alone, with env added to its own, and
// with its own test timeout, so a child that nobody kills ends by itself. The child is killed at the
// test's end if the test did not kill it.
func StartChild(t *testing.T, test string, env ...string) *Child {
	t.Helper()
	dir := t.TempDir()
	c := &Child{t: t, record: filepath.Join(dir, "record.json"), out: filepath.Join(dir, "output"), done: make(chan struct{}), pidfd: -1}
	out, err := os.Create(c.out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { out.Close() })
	c.cmd = exec.Command(os.Args[0], "-test.run=^"+test+"$", "-test.v", "-test.count=1", "-test.timeout=3m")
	c.cmd.Env = append(append(os.Environ(), childRecordEnv+"="+c.record), env...)
	c.cmd.Stdout, c.cmd.Stderr = out, out
	c.cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &c.pidfd}
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("start the child test binary: %v", err)
	}
	if c.pidfd < 0 {
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
		t.Fatal("the kernel gave no pidfd for the child test binary")
	}
	go func() { _ = c.cmd.Wait(); close(c.done) }()
	t.Cleanup(func() {
		if !c.killed {
			c.Kill()
		}
		_ = unix.Close(c.pidfd)
	})
	return c
}

// Output is what the child printed so far.
func (c *Child) Output() string {
	b, _ := os.ReadFile(c.out)
	return string(b)
}

// WaitRecord waits until the child has written its record and returns it, or fails the test when
// the child ended first (with what it printed) or d passed. d only bounds a wait that would never
// end: the record appears when the child's rig is up.
func (c *Child) WaitRecord(d time.Duration, into any) {
	c.t.Helper()
	end := time.Now().Add(d)
	for {
		if b, err := os.ReadFile(c.record); err == nil {
			if err := json.Unmarshal(b, into); err != nil {
				c.t.Fatalf("the child's record is not JSON: %v\n%s", err, b)
			}
			return
		}
		select {
		case <-c.done:
			c.t.Fatalf("the child ended before it wrote its record: %v\n%s", c.cmd.ProcessState, c.Output())
		default:
		}
		if time.Now().After(end) {
			c.t.Fatalf("the child wrote no record within %s\n%s", d, c.Output())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Kill sends SIGKILL to the child through its pidfd, as an outer time limit would, and waits until it
// is reaped.
func (c *Child) Kill() {
	c.t.Helper()
	c.killed = true
	_ = unix.PidfdSendSignal(c.pidfd, unix.SIGKILL, nil, 0)
	select {
	case <-c.done:
	case <-time.After(30 * time.Second):
		c.t.Errorf("the child did not end after SIGKILL")
	}
}

// Wait waits until the child ends by itself, and returns its exit code and what it printed. d only
// bounds a wait that would never end.
func (c *Child) Wait(d time.Duration) (code int, output string) {
	c.t.Helper()
	select {
	case <-c.done:
	case <-time.After(d):
		c.t.Fatalf("the child did not end within %s\n%s", d, c.Output())
	}
	return c.cmd.ProcessState.ExitCode(), c.Output()
}

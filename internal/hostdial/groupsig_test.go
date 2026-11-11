package hostdial

// Tests for group.go (issue 363). A guard test starts a real process of its
// own, builds the one state its guard refuses, and shows through a recording
// sink that nothing was SENT, with the refusal naming that guard. Its sink
// never delivers, so a guard that is removed (a mutant) cannot signal anything
// real either. The positive controls forward to the real group signal, and
// only ever to a group the test started.
//
// Each fixture is built so that its guard is the first in group.go's order
// that refuses it: the test asserts the guard's NAME, so a mutant that removes
// one guard is not hidden by a later one that refuses the same fixture.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// sinkCall is one group signal that reached the sink.
type sinkCall struct {
	pgid int
	sig  syscall.Signal
}

// sinkRecorder is a signal sink. It forwards to forward when that is set, and
// only records otherwise.
type sinkRecorder struct {
	forward func(pgid int, sig syscall.Signal) error
	onCall  func(pgid int) // called before forwarding, with the guards passed

	mu    sync.Mutex
	calls []sinkCall
}

func (r *sinkRecorder) sink(pgid int, sig syscall.Signal) error {
	r.mu.Lock()
	r.calls = append(r.calls, sinkCall{pgid, sig})
	r.mu.Unlock()
	if r.onCall != nil {
		r.onCall(pgid)
	}
	if r.forward != nil {
		return r.forward(pgid, sig)
	}
	return nil
}

func (r *sinkRecorder) got() []sinkCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sinkCall(nil), r.calls...)
}

// logBuf is a log sink a test can read while the code under test writes it.
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

func capturedLog() (*slog.Logger, *logBuf) {
	buf := &logBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// child is a process the test started and pinned, and cleans up.
type child struct {
	cmd   *exec.Cmd
	g     *groupLeader
	stdin io.WriteCloser
	logs  *logBuf
	once  sync.Once
	err   error
}

// startCat starts cat, with a pipe as its standard input, as a pinned group
// leader: it lives until finish closes the pipe. Nothing ever signals it.
func startCat(t *testing.T, rec *sinkRecorder) *child {
	t.Helper()
	log, logs := capturedLog()
	cmd := exec.Command("cat")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	g := newGroupLeader(log, rec.sink)
	if err := startLeader(g, cmd); err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: cmd, g: g, stdin: in, logs: logs}
	t.Cleanup(func() { c.finish() })
	return c
}

// finish ends the child (cat ends at the end of its input), reaps it and
// releases its pidfd; it returns Wait's result. It is safe to call twice.
func (c *child) finish() error {
	c.once.Do(func() {
		_ = c.stdin.Close()
		c.err = c.cmd.Wait()
		c.g.release()
	})
	return c.err
}

// startLeader starts cmd as the leader of a process group of its own, as
// startProcess does (no clone pidfd), and pins it.
func startLeader(g *groupLeader, cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return g.pin(cmd.Process.Pid)
}

// swap replaces *p by v until the test ends, or until the returned function is
// called. The kernel-call seams are package variables: tests that swap them
// run one at a time.
func swap[T any](t *testing.T, p *T, v T) (restore func()) {
	t.Helper()
	old := *p
	*p = v
	restore = func() { *p = old }
	t.Cleanup(restore)
	return restore
}

// procFailing is a procRead whose reads of /proc/<pid>/stat fail the way
// descriptor exhaustion does.
func procFailing(name string) ([]byte, error) {
	if strings.HasPrefix(name, "/proc/") && strings.HasSuffix(name, "/stat") {
		return nil, syscall.EMFILE
	}
	return os.ReadFile(name)
}

// refused asserts that signal(sig) on g was refused by the named guard, that
// the sink saw nothing, and that the log names the guard and the PGID.
func refused(t *testing.T, g *groupLeader, logs *logBuf, rec *sinkRecorder, guard string) error {
	t.Helper()
	err := g.signal(syscall.SIGTERM)
	var ge *groupGuardError
	if !errors.As(err, &ge) {
		t.Fatalf("signal returned %v, want a refusal by guard %q", err, guard)
	}
	if ge.Guard != guard {
		t.Errorf("refused by guard %q (%s), want %q", ge.Guard, ge.Detail, guard)
	}
	if ge.PGID != g.pgid {
		t.Errorf("the refusal names PGID %d, want %d", ge.PGID, g.pgid)
	}
	if calls := rec.got(); len(calls) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", calls)
	}
	out := logs.String()
	if want := "guard=" + guard; !strings.Contains(out, want) {
		t.Errorf("the log does not name the guard (%s): %s", want, out)
	}
	if want := fmt.Sprintf("pgid=%d", g.pgid); !strings.Contains(out, want) {
		t.Errorf("the log does not name the PGID (%s): %s", want, out)
	}
	return err
}

// Guard 1: a PGID of 1 or less. kill(2) would signal this group (0), every
// process (-1 is what 1 becomes), or one process by its PID (a negative PGID).
// The leader is live, pinned and ours, so only this guard refuses.
func TestGroupSignalRefusesAPGIDAtOrBelowOne(t *testing.T) {
	for _, pgid := range []int{0, 1, -1, -2} {
		t.Run(fmt.Sprintf("pgid_%d", pgid), func(t *testing.T) {
			rec := &sinkRecorder{}
			c := startCat(t, rec)
			c.g.pgid = pgid
			refused(t, c.g, c.logs, rec, guardPGID)
		})
	}
}

// Guard 1: this process's own group. Live, pinned, our child; only the number
// is wrong.
func TestGroupSignalRefusesOurOwnGroup(t *testing.T) {
	rec := &sinkRecorder{}
	c := startCat(t, rec)
	c.g.pgid = unix.Getpgrp()
	refused(t, c.g, c.logs, rec, guardOwnGroup)
}

// Guard 3: a leader that exited, was reaped, and whose pidfd is still held:
// the state that follows Wait. The real short-lived child is gone for real.
func TestGroupSignalRefusesALeaderThatExitedAndWasReaped(t *testing.T) {
	rec := &sinkRecorder{}
	log, logs := capturedLog()
	cmd := exec.Command("true")
	g := newGroupLeader(log, rec.sink)
	if err := startLeader(g, cmd); err != nil {
		t.Fatal(err)
	}
	defer g.release()
	if err := cmd.Wait(); err != nil { // reaped; the pidfd is still open
		t.Fatal(err)
	}
	err := refused(t, g, logs, rec, guardLeaderGone)
	if !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("the refusal is not an os.ErrProcessDone, which exec's Cancel treats as benign: %v", err)
	}
}

// Guard 2: a leader whose pidfd was released after the reap. The descriptor
// number may be another file's by now, so nothing is asked of it: the signal
// is refused before any check touches it.
func TestGroupSignalRefusesALeaderWhosePidfdWasReleased(t *testing.T) {
	rec := &sinkRecorder{}
	log, logs := capturedLog()
	cmd := exec.Command("true")
	g := newGroupLeader(log, rec.sink)
	if err := startLeader(g, cmd); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	g.release()
	refused(t, g, logs, rec, guardLeaderGone)
	if out := logs.String(); !strings.Contains(out, "pidfd was closed") {
		t.Errorf("the refusal is not the released-pidfd one: %s", out)
	}
}

// Guard 3: a leader that exited and is still a zombie, not reaped. /proc still
// has its row with the pinned start time and waitid still sees a child, so
// only the pidfd shows the exit.
func TestGroupSignalRefusesALeaderThatExitedAndIsNotReaped(t *testing.T) {
	rec := &sinkRecorder{}
	log, logs := capturedLog()
	cmd := exec.Command("true")
	g := newGroupLeader(log, rec.sink)
	if err := startLeader(g, cmd); err != nil {
		t.Fatal(err)
	}
	defer g.release()
	defer func() { _ = cmd.Wait() }()
	// Wait on observable state: the pidfd turns readable when it exits.
	waitExited(t, g.fd, 10*time.Second)
	refused(t, g, logs, rec, guardLeaderGone)
}

// Guard 4: the PID shows another start time than the pin. The pin is moved by
// one tick, the way a recycled PID would differ from it.
func TestGroupSignalRefusesALeaderWithAnotherStartTime(t *testing.T) {
	rec := &sinkRecorder{}
	c := startCat(t, rec)
	c.g.ticks++
	refused(t, c.g, c.logs, rec, guardStartTime)
}

// Guard 3: a live process that is not our child. cat is the grandchild of this
// test, started by a shell that waits for it; its pidfd is opened by its PID
// (read, never signalled). Alive, so the pidfd shows no exit; with a matching
// start time, so only waitid's ECHILD refuses. Nothing is signalled to end it:
// closing its input ends it.
func TestGroupSignalRefusesALeaderThatIsNotOurChild(t *testing.T) {
	rec := &sinkRecorder{}
	log, logs := capturedLog()
	sh := exec.Command("/bin/sh", "-c", "exec 3<&0; cat <&3 >/dev/null & echo $!; wait")
	in, err := sh.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := sh.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := sh.Start(); err != nil {
		t.Fatal(err)
	}
	var catPid int
	if _, err := fmt.Fscanln(bufio.NewReader(out), &catPid); err != nil || catPid <= 1 {
		_ = in.Close()
		_ = sh.Wait()
		t.Fatalf("no PID from the shell: %d, %v", catPid, err)
	}
	fd, openErr := unix.PidfdOpen(catPid, 0)
	t.Cleanup(func() {
		_ = in.Close() // cat ends at the end of its input, then the shell
		_ = sh.Wait()
		if openErr == nil {
			_ = unix.Close(fd)
		}
	})
	if openErr != nil {
		t.Fatal(openErr)
	}
	// pin refuses a process that is not our child, so the pin is made by
	// hand: the state the guard exists for is one that was pinned and is not
	// our child any more.
	ticks, err := groupStartTicks(catPid)
	if err != nil {
		t.Fatal(err)
	}
	g := newGroupLeader(log, rec.sink)
	g.pid, g.pgid, g.ticks, g.fd = catPid, catPid, ticks, fd
	refused(t, g, logs, rec, guardNotChild)
}

// pin itself checks that the process is our child (waitid on its pidfd): one
// that is not is refused at the pin, and then every signal is refused too.
func TestAPinRefusesAProcessThatIsNotOurChild(t *testing.T) {
	rec := &sinkRecorder{}
	log, logs := capturedLog()
	sh := exec.Command("/bin/sh", "-c", "exec 3<&0; cat <&3 >/dev/null & echo $!; wait")
	in, err := sh.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := sh.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := sh.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close(); _ = sh.Wait() })
	var catPid int
	if _, err := fmt.Fscanln(bufio.NewReader(out), &catPid); err != nil || catPid <= 1 {
		t.Fatalf("no PID from the shell: %d, %v", catPid, err)
	}
	g := newGroupLeader(log, rec.sink)
	err = g.pin(catPid)
	if err == nil || !strings.Contains(err.Error(), "not our child") {
		t.Fatalf("pin of a process that is not our child: got %v, want a refusal saying so", err)
	}
	refused(t, g, logs, rec, guardUnpinned)
}

// Guard 2: a leader whose pinning failed (no start time) is refused, whatever
// the rest looks like.
func TestGroupSignalRefusesALeaderThatWasNotPinned(t *testing.T) {
	rec := &sinkRecorder{}
	c := startCat(t, rec)
	log, logs := capturedLog()
	g := newGroupLeader(log, rec.sink)
	g.pid, g.pgid = c.g.pid, c.g.pgid
	g.pinErr = errors.New("no start time")
	refused(t, g, logs, rec, guardUnpinned)
}

// Positive controls: a live, pinned leader IS signalled, once, with the PGID
// of its own group, and the signal arrives (the child's wait status says so,
// which the sink's record cannot fake).
func TestGroupSignalReachesALivePinnedLeader(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(sig.String(), func(t *testing.T) {
			rec := &sinkRecorder{forward: killGroup}
			c := startCat(t, rec)
			if err := c.g.signal(sig); err != nil {
				t.Fatalf("a live pinned leader was refused: %v", err)
			}
			want := []sinkCall{{c.cmd.Process.Pid, sig}}
			if got := rec.got(); len(got) != 1 || got[0] != want[0] {
				t.Errorf("the sink saw %+v, want %+v", got, want)
			}
			var exit *exec.ExitError
			if err := c.finish(); !errors.As(err, &exit) {
				t.Fatalf("the child did not end by a signal: %v", err)
			}
			if got := exit.Sys().(syscall.WaitStatus).Signal(); got != sig {
				t.Errorf("the child ended by %v, want %v", got, sig)
			}
		})
	}
}

// endThroughPidfd ends a process the test started, through its pidfd, unless
// the pidfd was released already (the process was reaped): a signal never goes
// through a descriptor number that may be another file's by now.
func endThroughPidfd(g *groupLeader) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		_ = unix.PidfdSendSignal(g.fd, syscall.SIGKILL, nil, 0)
	}
}

// waitReadable waits up to d for the pidfd to turn readable, which it does
// when the process exits. A signal that interrupts the poll is not an answer
// (Go's own preemption signals do it under load): it waits on.
func waitReadable(fd int, d time.Duration) (bool, error) {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for end := time.Now().Add(d); ; {
		n, err := unix.Poll(fds, int(max(time.Until(end), 0).Milliseconds()))
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return n == 1, err
	}
}

// waitExited waits, on the pidfd, for the process to exit.
func waitExited(t *testing.T, fd int, d time.Duration) {
	t.Helper()
	if ok, err := waitReadable(fd, d); err != nil || !ok {
		t.Fatalf("the process did not exit within %s: exited=%v err=%v", d, ok, err)
	}
}

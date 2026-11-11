package sshrun

// Tests for how a transport starts and stops what group.go pins (PR 365 review
// round): the kernel probe at New, a refused stop that keeps the live master
// owned, and a failed pin whose cleanup can never hold startup up.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// A refused stop keeps the live master owned. A transient /proc read failure
// (EMFILE) makes the start-time guard refuse twice: Close returns an error
// that names the guard and the PGID, nothing is sent, the master runs on and
// its socket stays. Once /proc reads again, Close stops it with one SIGTERM.
func TestACloseThatIsRefusedKeepsTheMasterOwned(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	log, logs := capturedLog()
	s.log = log
	s.stopRetryPause = 10 * time.Millisecond
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	m := s.master
	s.mu.Unlock()
	t.Cleanup(func() { // if the test failed with the master running, end it through its pidfd
		endThroughPidfd(m.leader)
		<-m.done
	})
	pgid := m.cmd.Process.Pid
	restore := swap(t, &procRead, procFailing)

	err := s.Close()
	if err == nil {
		t.Fatal("Close reported success, with the master's stop refused")
	}
	if !strings.Contains(err.Error(), "guard \""+guardCannotTell+"\"") || !strings.Contains(err.Error(), fmt.Sprintf("process group %d", pgid)) {
		t.Errorf("Close's error names neither the guard nor the PGID: %v", err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardCannotTell) {
		t.Errorf("the refusal is not in the log: %s", out)
	}
	s.mu.Lock()
	owned, stuck := s.master == m, m.stuck
	s.mu.Unlock()
	if !owned || !stuck {
		t.Errorf("the live master is not kept: owned=%v stuck=%v", owned, stuck)
	}
	if _, err := os.Stat(s.socket); err != nil {
		t.Errorf("the master's socket was removed: %v", err)
	}
	if exited, err := waitReadable(m.leader.fd, 0); err != nil || exited {
		t.Errorf("the master's pidfd shows an exit: exited=%v err=%v", exited, err)
	}

	restore() // /proc reads again: Close again stops the master it still owns
	if err := s.Close(); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
	want := []sinkCall{{pgid, syscall.SIGTERM}}
	if got := rec.got(); len(got) != 1 || got[0] != want[0] {
		t.Errorf("the sink saw %+v, want %+v", got, want)
	}
	select {
	case <-m.done:
	default:
		t.Error("the second Close returned before the master was reaped")
	}
	s.mu.Lock()
	gone := s.master == nil
	s.mu.Unlock()
	if _, err := os.Stat(s.socket); gone && err == nil {
		t.Error("the socket is left after the master was stopped")
	}
	if !gone {
		t.Error("the master is still owned after it was stopped")
	}
}

// Ownership is released only on evidence that the master is gone, never on a
// window (PR 365 delta review). A filter that refuses the barrier's blocking
// waitid, but passed the probe, makes awaitExit fail on a LIVE master: its
// reaping flag goes true, so every signal is refused as "being reaped", and
// the refusal says "gone" without it being true. The stop must not take that
// for an exit: the master stays owned and stuck, with its socket, Close
// returns an error, and no second master starts.
func TestAMasterWhoseExitCannotBeAwaitedStaysOwned(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	log, logs := capturedLog()
	s.log = log
	s.stopRetryPause = 10 * time.Millisecond
	s.killGrace = 100 * time.Millisecond
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	orig := pidfdWaitid
	swap(t, &pidfdWaitid, func(fd, options int) error { // the blocking call fails; the polling one works
		if options&unix.WNOHANG == 0 {
			return syscall.EPERM
		}
		return orig(fd, options)
	})
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	m := s.master
	s.mu.Unlock()
	t.Cleanup(func() { // the stop was refused: end the master by hand, through its pidfd
		endThroughPidfd(m.leader)
		<-m.done
	})

	err := s.Close()
	if err == nil {
		t.Fatal("Close reported success for a live master that was never signalled")
	}
	if !strings.Contains(err.Error(), "not been seen to exit") {
		t.Errorf("Close's error does not say that no exit was seen: %v", err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "could not be awaited") {
		t.Errorf("the barrier's failure is not in the log: %s", out)
	}
	s.mu.Lock()
	owned, stuck := s.master == m, m.stuck
	s.mu.Unlock()
	if !owned || !stuck {
		t.Errorf("the live master is not kept: owned=%v stuck=%v", owned, stuck)
	}
	if _, err := os.Stat(s.socket); err != nil {
		t.Errorf("the master's socket was removed: %v", err)
	}
	select {
	case <-m.done:
		t.Error("the master ended, although nothing was sent to it")
	default:
	}
	if exited, err := waitReadable(m.leader.fd, 0); err != nil || exited {
		t.Errorf("the master's pidfd shows an exit: exited=%v err=%v", exited, err)
	}
}

// A refusal that does not last is not a failure of the stop: the stop is
// retried once. The first read of the master's /proc row fails (EMFILE) and
// the next ones succeed, so the first TERM is refused, and the retry, after a
// short pause, sends it. Close returns nil, with one signal sent.
func TestARefusedStopIsRetriedOnce(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	log, logs := capturedLog()
	s.log = log
	s.stopRetryPause = 10 * time.Millisecond
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	m := s.master
	s.mu.Unlock()
	t.Cleanup(func() {
		endThroughPidfd(m.leader)
		<-m.done
	})
	var failedOnce atomic.Bool
	swap(t, &procRead, func(name string) ([]byte, error) {
		if failedOnce.CompareAndSwap(false, true) {
			return nil, syscall.EMFILE
		}
		return os.ReadFile(name)
	})
	if err := s.Close(); err != nil {
		t.Fatalf("Close, whose first signal was refused only once: %v", err)
	}
	want := []sinkCall{{m.cmd.Process.Pid, syscall.SIGTERM}}
	if got := rec.got(); len(got) != 1 || got[0] != want[0] {
		t.Errorf("the sink saw %+v, want the retried %+v", got, want)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardCannotTell) {
		t.Errorf("the first refusal is not in the log: %s", out)
	}
	select {
	case <-m.done:
	default:
		t.Error("Close returned before the master was reaped")
	}
}

// No second master starts while a stuck one lives. The master's socket is
// deleted, so the next command fails and the transport tries to stop it; /proc
// cannot be read, so the stop is refused. The commands after it get
// ErrNotStarted and no second master starts. When /proc reads again, the next
// command stops the stuck master and starts a new one.
func TestNoSecondMasterStartsWhileAStuckOneLives(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	s.log, _ = capturedLog()
	s.stopRetryPause = 10 * time.Millisecond
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	run(t, s, "true")
	s.mu.Lock()
	first := s.master
	s.mu.Unlock()
	t.Cleanup(func() {
		endThroughPidfd(first.leader)
		<-first.done
	})
	if err := os.Remove(s.socket); err != nil {
		t.Fatal(err)
	}
	restore := swap(t, &procRead, procFailing)

	// The command that finds the socket gone; the stop it attempts is refused.
	_, _ = s.Run(context.Background(), slurm.Command{Line: "true", Timeout: 10 * time.Second})
	for i := 0; i < 2; i++ {
		_, err := s.Run(context.Background(), slurm.Command{Line: "true", Timeout: 10 * time.Second})
		if !errors.Is(err, slurm.ErrNotStarted) || !strings.Contains(err.Error(), "still running") {
			t.Errorf("command %d with a stuck master: got %v, want ErrNotStarted naming the stuck master", i+1, err)
		}
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 1 || s.Starts() != 1 {
		t.Errorf("%d masters started (Starts %d), want the first only while it is stuck", n, s.Starts())
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}

	restore()
	run(t, s, "true") // stops the stuck master, then starts a new one
	if n := len(fakeLogLines(r.dir, "master")); n != 2 || s.Starts() != 2 {
		t.Errorf("%d masters started (Starts %d), want 2 once the stuck one was stopped", n, s.Starts())
	}
	select {
	case <-first.done:
	default:
		t.Error("the stuck master was not stopped before the new one")
	}
	if got := rec.got(); len(got) != 1 || got[0].sig != syscall.SIGTERM || got[0].pgid != first.cmd.Process.Pid {
		t.Errorf("the sink saw %+v, want one SIGTERM to the first master's group", got)
	}
}

// A start whose leader cannot be pinned must not hold startup up. Here the
// start time cannot be read AND the kill through the pidfd fails, so the
// child runs on: the cleanup is bounded, startup returns an error that says
// the process may still run, and the failure is in the log.
func TestAFailedPinWhoseKillFailsDoesNotHangStartup(t *testing.T) {
	swap(t, &procRead, procFailing)
	swap(t, &pidfdSendSignal, func(int, unix.Signal, *unix.Siginfo, int) error { return syscall.EPERM })
	swap(t, &pinReapBound, 300*time.Millisecond)
	swap(t, &pinWaitDelay, 100*time.Millisecond)
	log, logs := capturedLog()
	cmd := exec.Command("cat")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close() }) // cat ends at the end of its input; the cleanup's wait reaps it
	g := newGroupLeader(log, (&sinkRecorder{}).sink)
	started := make(chan error, 1)
	go func() { started <- g.start(cmd) }()
	var startErr error
	select {
	case startErr = <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("startup hung on a failed pin whose kill failed")
	}
	if startErr == nil || !strings.Contains(startErr.Error(), "may still run") || !strings.Contains(startErr.Error(), "SIGKILL") {
		t.Errorf("start returned %v, want an error saying the SIGKILL failed and the process may still run", startErr)
	}
	if out := logs.String(); !strings.Contains(out, "could not be pinned") {
		t.Errorf("the failure is not in the log: %s", out)
	}
	if err := g.signal(syscall.SIGTERM); err == nil {
		t.Error("a group signal to a leader that was never pinned was not refused")
	}
}

// A start whose leader cannot be pinned and whose helper holds the leader's
// standard error: the leader is killed through its pidfd, but cmd.Wait would
// wait for the helper too (stderr is not a file). WaitDelay closes the pipes,
// so the cleanup ends at once and start says the process ended. The pin
// failure is made to come after the shell started its helper: the seam waits
// for the test, which has read the helper's PID by then.
func TestAFailedPinWithAHelperHoldingStderrDoesNotHangStartup(t *testing.T) {
	proceed := make(chan struct{})
	swap(t, &procRead, func(name string) ([]byte, error) {
		<-proceed
		return procFailing(name)
	})
	swap(t, &pinReapBound, 15*time.Second)
	swap(t, &pinWaitDelay, 200*time.Millisecond)
	log, _ := capturedLog()
	cmd := exec.Command("/bin/sh", "-c", "sleep 120 & echo $!; echo ready; wait")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr // not a file: Wait copies it, until every holder closes it
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	g := newGroupLeader(log, (&sinkRecorder{}).sink)
	started := make(chan error, 1)
	go func() { started <- g.start(cmd) }()
	rd := bufio.NewReader(out)
	var helper int
	var ready string
	if _, err := fmt.Fscanln(rd, &helper); err != nil {
		close(proceed)
		t.Fatalf("no helper PID from the shell: %v", err)
	}
	if _, err := fmt.Fscanln(rd, &ready); err != nil || ready != "ready" {
		close(proceed)
		t.Fatalf("the shell is not ready: %q %v", ready, err)
	}
	helperFd, err := unix.PidfdOpen(helper, 0)
	if err != nil {
		close(proceed)
		t.Fatal(err)
	}
	t.Cleanup(func() { // the helper outlives the test's subject: end it through its pidfd
		_ = unix.PidfdSendSignal(helperFd, syscall.SIGKILL, nil, 0)
		_ = unix.Close(helperFd)
	})
	close(proceed) // the pin fails now, with the helper holding stderr

	var startErr error
	select {
	case startErr = <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("startup hung on a failed pin whose leader left a helper holding stderr")
	}
	if startErr == nil {
		t.Fatal("start succeeded without a pin")
	}
	if strings.Contains(startErr.Error(), "did not end") || strings.Contains(startErr.Error(), "may still run") {
		t.Errorf("the cleanup did not end the leader in time: %v", startErr)
	}
	if cmd.ProcessState == nil {
		t.Error("the leader was not reaped")
	}
}

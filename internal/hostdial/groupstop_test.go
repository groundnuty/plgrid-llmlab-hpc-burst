package hostdial

// Tests for how a relay is started, pinned and stopped on alpha's default path
// (issue #370): the stop's TERM and KILL reach the group through the sink, a
// refused stop is reported and leaves the relay running, a refusal that does
// not last is retried once, a relay that cannot be pinned is refused and never
// signalled by its number, and "gone" needs evidence. The guards themselves
// are in groupsig_test.go, the barrier in groupbarrier_test.go.

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// startRelay starts cmd as startProcess does, with a recording sink that
// forwards to the real group signal (so a test that fails does not leave what
// it started), and registers the cleanup that ends it through its pidfd.
func startRelay(t *testing.T, cmd *exec.Cmd, rec *sinkRecorder) (*process, *logBuf) {
	t.Helper()
	log, logs := capturedLog()
	p, err := startGuardedProcess(cmd, log, rec.sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		endThroughPidfd(p.lead) // a no-op once the relay was reaped
		_ = p.Close()
		<-p.done
	})
	return p, logs
}

// A stop sends TERM and then, for a relay that ignores it, KILL, each through
// the guarded function, to the PGID of the relay's own group. The leader here
// is a shell that ignores TERM, with a group member (sleep, which inherits the
// ignored TERM) that only a GROUP signal reaches; the test holds the member's
// pidfd to see it end. This is the stop on the default path, which used to be
// two numeric kills.
func TestAStopSendsTermThenKillToTheGroup(t *testing.T) {
	rec := &sinkRecorder{forward: killGroup}
	cmd := exec.Command("/bin/sh", "-c", `trap "" TERM; sleep 120 >/dev/null & echo $!; echo ready; wait`)
	p, _ := startRelay(t, cmd, rec)
	rd := bufio.NewReader(p)
	var memberPid int
	var ready string
	if _, err := fmt.Fscanln(rd, &memberPid); err != nil {
		t.Fatalf("no member PID from the shell: %v", err)
	}
	if _, err := fmt.Fscanln(rd, &ready); err != nil || ready != "ready" {
		t.Fatalf("the shell is not ready: %q %v", ready, err)
	}
	memberFd, err := unix.PidfdOpen(memberPid, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unix.PidfdSendSignal(memberFd, syscall.SIGKILL, nil, 0)
		_ = unix.Close(memberFd)
	})

	if err := p.stop(100 * time.Millisecond); err != nil {
		t.Fatalf("stop: %v", err)
	}
	want := []sinkCall{{p.pid, syscall.SIGTERM}, {p.pid, syscall.SIGKILL}}
	got := rec.got()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("the sink saw %+v, want %+v", got, want)
	}
	select {
	case <-p.done:
	default:
		t.Error("stop returned before the relay was reaped")
	}
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
		t.Errorf("the relay did not end by SIGKILL: %v", cmd.ProcessState)
	}
	waitExited(t, memberFd, 10*time.Second) // the group member died with it
}

// A relay that exits on TERM is stopped by that one signal.
func TestAStopOfARelayThatExitsOnTermSendsOnlyTerm(t *testing.T) {
	rec := &sinkRecorder{forward: killGroup}
	p, _ := startRelay(t, exec.Command("cat"), rec)
	if err := p.stop(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 1 || got[0] != (sinkCall{p.pid, syscall.SIGTERM}) {
		t.Errorf("the sink saw %+v, want one SIGTERM to %d", got, p.pid)
	}
}

// A stop that a guard refuses is reported, with the guard and the PGID, and
// sends nothing, not another way either: the relay runs on, and once /proc
// reads again the stop that is made again ends it. The refusal is real: the
// start-time guard cannot read /proc (descriptor exhaustion does that).
func TestAStopThatIsRefusedIsReportedAndTheRelayRunsOn(t *testing.T) {
	rec := &sinkRecorder{forward: killGroup}
	p, logs := startRelay(t, exec.Command("cat"), rec)
	swap(t, &stopRetryPause, 10*time.Millisecond)
	restore := swap(t, &groupProcRead, procFailing)

	err := p.stop(100 * time.Millisecond)
	if err == nil {
		t.Fatal("stop reported success, with its signal refused")
	}
	if !strings.Contains(err.Error(), "guard \""+guardCannotTell+"\"") || !strings.Contains(err.Error(), fmt.Sprintf("process group %d", p.pid)) {
		t.Errorf("the error names neither the guard nor the PGID: %v", err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardCannotTell) {
		t.Errorf("the refusal is not in the log: %s", out)
	}
	select {
	case <-p.done:
		t.Error("the relay ended, although nothing was sent to it")
	default:
	}
	if exited, err := waitReadable(p.lead.fd, 0); err != nil || exited {
		t.Errorf("the relay's pidfd shows an exit: exited=%v err=%v", exited, err)
	}

	restore()
	if err := p.stop(30 * time.Second); err != nil {
		t.Fatalf("the stop made again: %v", err)
	}
	if got := rec.got(); len(got) != 1 || got[0] != (sinkCall{p.pid, syscall.SIGTERM}) {
		t.Errorf("the sink saw %+v, want one SIGTERM to %d", got, p.pid)
	}
}

// A refusal that does not last is not a failure of the stop: the stop is
// retried once. The first read of the relay's /proc row fails (EMFILE) and the
// next ones succeed, so the first TERM is refused and the retry sends it.
func TestARefusedStopIsRetriedOnce(t *testing.T) {
	rec := &sinkRecorder{forward: killGroup}
	p, logs := startRelay(t, exec.Command("cat"), rec)
	swap(t, &stopRetryPause, 10*time.Millisecond)
	var failedOnce atomic.Bool
	swap(t, &groupProcRead, func(name string) ([]byte, error) {
		if failedOnce.CompareAndSwap(false, true) {
			return nil, syscall.EMFILE
		}
		return os.ReadFile(name)
	})
	if err := p.stop(30 * time.Second); err != nil {
		t.Fatalf("stop, whose first signal was refused only once: %v", err)
	}
	if got := rec.got(); len(got) != 1 || got[0] != (sinkCall{p.pid, syscall.SIGTERM}) {
		t.Errorf("the sink saw %+v, want the retried SIGTERM to %d", got, p.pid)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardCannotTell) {
		t.Errorf("the first refusal is not in the log: %s", out)
	}
}

// "Gone" needs evidence, never a wait (as PR 365's delta review found for the
// master). A filter that refuses the barrier's blocking waitid but passed the
// probe makes awaitExit fail on a LIVE relay: its reaping flag goes true, so
// every signal is refused as "being reaped", and that refusal says the leader
// is gone without it being true. The stop must not take it for an exit: it
// returns an error saying no exit was seen, with the relay still running.
func TestARelayWhoseExitCannotBeAwaitedIsNotCalledGone(t *testing.T) {
	if err := groupCalls(); err != nil { // the probe passes before the filter
		t.Fatal(err)
	}
	rec := &sinkRecorder{forward: killGroup}
	orig := groupWaitid
	swap(t, &groupWaitid, func(fd, options int) error {
		if options&unix.WNOHANG == 0 {
			return syscall.EPERM
		}
		return orig(fd, options)
	})
	swap(t, &stopRetryPause, 10*time.Millisecond)
	p, logs := startRelay(t, exec.Command("cat"), rec)
	// The waiter sets reaping once its wait for the exit has failed: wait for
	// that state, so the stop below meets it and not the start before it.
	for end := time.Now().Add(10 * time.Second); !reapingSet(p.lead); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the waiter never set reaping after its wait for the exit failed")
		}
	}

	err := p.stop(100 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "not been seen to exit") {
		t.Fatalf("stop of a relay that is alive and cannot be signalled: got %v, want an error saying no exit was seen", err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "could not be awaited") {
		t.Errorf("the barrier's failure is not in the log: %s", out)
	}
	select {
	case <-p.done:
		t.Error("the relay ended, although nothing was sent to it")
	default:
	}
}

// A relay that cannot be pinned (its start time is unreadable) is refused and
// reported, and nothing signals its process: no signal ever goes to a numeric
// PID. The process traps every signal it can into a log and ends by itself,
// with exit 0, once its input closes, which the refusal does; so the log stays
// empty and the process is gone.
func TestARelayThatCannotBePinnedIsRefusedAndNothingIsSignalled(t *testing.T) {
	if err := groupCalls(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	trapLog, ended := filepath.Join(dir, "signals"), filepath.Join(dir, "ended")
	script := `for s in HUP INT QUIT USR1 USR2 PIPE ALRM TERM; do trap "echo $s >> $1" $s; done; read x; echo ended > $2; exit 0`
	swap(t, &groupProcRead, procFailing)
	log, logs := capturedLog()
	cmd := exec.Command("/bin/sh", "-c", script, "sh", trapLog, ended)
	p, err := startGuardedProcess(cmd, log, (&sinkRecorder{forward: killGroup}).sink)
	if p != nil || err == nil || !strings.Contains(err.Error(), "could not be pinned") {
		t.Fatalf("a start that cannot be pinned: %v, %v; want a refusal saying so", p, err)
	}
	if out := logs.String(); !strings.Contains(out, "could not be pinned") {
		t.Errorf("the refusal is not in the log: %s", out)
	}
	waitFile(t, ended, 30*time.Second)
	if b, err := os.ReadFile(trapLog); err == nil && len(b) != 0 {
		t.Errorf("the refused process got signals %q, want none", b)
	}
}

// reapingSet reads the leader's reaping flag under its mutex.
func reapingSet(g *groupLeader) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reaping
}

// waitFile waits for the file to exist.
func waitFile(t *testing.T, path string, d time.Duration) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(path); err == nil {
			return
		}
	}
	t.Fatalf("%s did not appear within %s", path, d)
}

// Alpha's relay keeps the attributes TestAlphasRelayLeadsItsOwnGroupAndItsStopSignalsIt
// pins: a group of its own, SIGKILL when this process dies, and NO clone pidfd:
// the leader's pidfd is taken by pidfd_open after the start.
func TestTheDefaultPathAsksCloneForNoPidfd(t *testing.T) {
	rec := &sinkRecorder{forward: killGroup}
	cmd := exec.Command("cat")
	p, _ := startRelay(t, cmd, rec)
	a := cmd.SysProcAttr
	if !a.Setpgid || a.Pdeathsig != syscall.SIGKILL || a.PidFD != nil {
		t.Errorf("the default path's relay started with Setpgid %v, Pdeathsig %v and PidFD %v; want true, SIGKILL and none", a.Setpgid, a.Pdeathsig, a.PidFD)
	}
	if pgid, err := syscall.Getpgid(p.pid); err != nil || pgid != p.pid {
		t.Errorf("the relay %d is in process group %d (%v), want a group of its own", p.pid, pgid, err)
	}
}

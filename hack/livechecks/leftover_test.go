package livechecks

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests run small scripts under the reaper (reaper_test.go), the way the script tests run
// hack/live-checks.sh, and check its report. Each leftover test makes the script wait until the
// process it leaves behind has written its pid, so a pid is on file whatever the load, and
// checks after the reaper exits that the process is gone: the reaper has killed and reaped it.
//
// Nothing here signals a process by a pid read from a file or a snapshot. A test that finds a
// process the reaper failed to stop has a last resort, killMarked: it kills the live processes
// of this test, found by a variable no other process has, and it runs even when the test ends
// in t.Fatal.

type reaped struct {
	kind    string
	details []string
	code    int
	dir     string
	mark    string // the NAME=value entry that only this test's processes carry
}

// hooks says which of the reaper's test hooks a run uses; both create the run's release file.
type hooks struct{ touch, latch bool }

// runReaped runs script (bash -c) under the reaper with the given grace and hooks, with PIDFILE
// and RELEASE (both in a fresh directory) in its environment, and returns the report and the
// exit code. When capture is not nil it is the script's standard output and error, as for the
// script tests.
func runReaped(t *testing.T, grace time.Duration, h hooks, capture *bytes.Buffer, script string) reaped {
	t.Helper()
	dir := t.TempDir()
	report, release := filepath.Join(dir, "report"), filepath.Join(dir, "release")
	mark := "PIDFILE=" + filepath.Join(dir, "pid")
	// The last resort, registered first so that it runs last, after the reaper has been told the
	// test is over and has collected what it could.
	t.Cleanup(func() { killMarked(mark) })
	o := reaperOpts{grace: grace}
	if h.touch {
		o.touch = release
	}
	if h.latch {
		o.latch = release
	}
	cmd := reaperCommand(t, report, o, dir, append(os.Environ(), mark, "RELEASE="+release), "-c", script)
	if capture != nil {
		cmd.Stdout, cmd.Stderr = capture, capture
		cmd.WaitDelay = 20 * time.Second // a regression that leaves the pipe open fails in 20 s, not 60
	}
	_ = cmd.Run()
	kind, details, err := readReport(report)
	if err != nil {
		t.Fatal(err)
	}
	return reaped{kind: kind, details: details, code: cmd.ProcessState.ExitCode(), dir: dir, mark: mark}
}

// gone fails the test when the process whose pid the script wrote is still there. A pid that
// another process now holds is not the process: it is told apart by the mark.
func (r reaped) gone(t *testing.T) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(r.dir, "pid"))
	if err != nil {
		t.Fatalf("the script wrote no pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatalf("the pid file holds %q", b)
	}
	for _, p := range markedPIDs(r.mark) {
		if p == pid {
			t.Errorf("process %d was still there after the reaper exited", pid)
		}
	}
}

func (r reaped) mustBeLeftover(t *testing.T) {
	t.Helper()
	if r.code != 0 {
		t.Fatalf("the script exited %d: it did not run as written, so this test proves nothing", r.code)
	}
	if r.kind != "leftover" || len(r.details) == 0 {
		t.Fatalf("report %q %v, want a leftover with the process named", r.kind, r.details)
	}
}

// waitForPid is the script's own wait, on the file the leftover process writes its pid to.
const waitForPid = "\nwhile [ ! -s \"$PIDFILE\" ]; do sleep 0.01; done"

// A process the script leaves behind for good is reported, and killed. Without this test,
// deleting the check (or making it say "clean") would leave every other test green.
func TestReaperReportsAProcessThatNeverExits(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{}, nil, `sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A child in a new session is in no process group of the script, so a group probe cannot see it
// (issue #185). It is still a descendant, so the reaper does.
func TestReaperSeesAChildInANewSession(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{}, nil, `setsid bash -c 'echo $$ > "$PIDFILE"; exec sleep 60' >/dev/null 2>&1 </dev/null &`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// `timeout` runs its command in a process group of its own, which a group probe never covered:
// the ssh calls and the test binary of hack/live-checks.sh run under it.
func TestReaperSeesAProcessRunUnderTimeout(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{}, nil, `timeout 60 sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A process whose parent has already exited has been given to the reaper by the kernel.
func TestReaperSeesAnOrphan(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{}, nil, `( sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE" )`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A grandchild is not the reaper's child until its parent dies. The reaper signals only its own
// children, so it kills the parent and then the grandchild it has been given, and both are gone.
// The report names both, from before either was killed.
func TestReaperKillsAGrandchildAfterItsParent(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{}, nil,
		`bash -c 'sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE"; wait' >/dev/null 2>&1 </dev/null &`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
	var sleeps int
	for _, d := range r.details {
		if d == "sleep" {
			sleeps++
		}
	}
	if sleeps != 1 || len(r.details) < 2 {
		t.Errorf("the report %v should name the grandchild (sleep) and its parent", r.details)
	}
}

// A leak that holds the captured standard output used to hide: Cmd.Run waits for the pipe to
// close, so the check started only after the leak had exited. The reaper judges the script's
// descendants itself and kills them, which closes the pipe.
func TestReaperSeesAProcessThatHoldsTheCapturedOutput(t *testing.T) {
	var out bytes.Buffer
	r := runReaped(t, 200*time.Millisecond, hooks{}, &out, `sleep 60 & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A process that is still exiting when the script's exit is seen is waited for, not reported.
// The child exits on the release file, and the reaper creates that file right after the first
// probe that finds the child alive (reapAll's touch). So the child is certainly alive at the first
// probe and certainly gone only after it: a detector that gives its verdict at the first probe
// reports the child and fails here, whatever the scheduler does.
func TestReaperWaitsOutAProcessThatIsStillExiting(t *testing.T) {
	r := runReaped(t, 30*time.Second, hooks{touch: true}, nil, `( while [ ! -e "$RELEASE" ]; do sleep 0.02; done ) >/dev/null 2>&1 </dev/null &`)
	if r.kind != "clean" {
		t.Errorf("a process that exits soon after the script was reported: %q %v", r.kind, r.details)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "release")); err != nil {
		t.Errorf("the reaper never saw the child alive, so it never released it: %v", err)
	}
}

// A child that is alive when the grace ends is a leftover, and stays one although it exits at
// once: the verdict is latched where the grace ends, not read from the cleanup that follows. The
// reaper creates the release file right after it latches and then waits until the child is a
// zombie, so the cleanup's first probe finds nothing alive to kill. A reaper that read the
// verdict from that cleanup would report "clean" here, every time.
func TestReaperKeepsTheVerdictOfAChildThatExitsRightAfterTheGrace(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, hooks{latch: true}, nil, `( while [ ! -e "$RELEASE" ]; do sleep 0.02; done ) >/dev/null 2>&1 </dev/null &`)
	if r.code != 0 || r.kind != "leftover" || len(r.details) == 0 {
		t.Errorf("report %q %v (script exit %d), want a leftover: the child was alive when the grace ended", r.kind, r.details, r.code)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "release")); err != nil {
		t.Errorf("the grace never ended with the child alive, so this test proves nothing: %v", err)
	}
}

// The script's exit code reaches the test unchanged, and a script killed by a signal reads as
// 128 plus the signal. The report of a clean run is "clean".
func TestReaperRelaysTheExitStatus(t *testing.T) {
	for _, c := range []struct {
		script string
		code   int
	}{{`exit 0`, 0}, {`exit 7`, 7}, {`kill -KILL $$`, 128 + int(syscall.SIGKILL)}} {
		r := runReaped(t, time.Second, hooks{}, nil, c.script)
		if r.code != c.code || r.kind != "clean" {
			t.Errorf("%q: exit %d, report %q; want exit %d, clean", c.script, r.code, r.kind, c.code)
		}
	}
}

// A signal sent to the reaper reaches the script, whose own handler decides what happens, and
// the reaper says so before the test goes on.
func TestReaperForwardsASignalToTheScript(t *testing.T) {
	dir := t.TempDir()
	report, ready := filepath.Join(dir, "report"), filepath.Join(dir, "ready")
	script := `trap 'exit 5' TERM; : > "$READY"; while :; do sleep 0.02; done`
	cmd := reaperCommand(t, report, reaperOpts{grace: time.Second}, dir, append(os.Environ(), "READY="+ready), "-c", script)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the script never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(ackPath(report)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reaper never acknowledged forwarding the signal")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, timedOut := waitBounded(cmd, stopBound); timedOut {
		t.Fatalf("the reaper over the trapping script did not exit within %s of the TERM it forwarded; the process tree, as it is now:\n%s", stopBound, hangDump(cmd.Process.Pid))
	}
	if got := cmd.ProcessState.ExitCode(); got != 5 {
		t.Errorf("exit %d, want 5: the script's trap did not run", got)
	}
	if kind, details, err := readReport(report); err != nil || kind != "clean" {
		t.Errorf("report %q %v, %v; want clean", kind, details, err)
	}
}

// A report that is not there is not a clean one: the reaper may have died before it wrote it.
func TestAMissingReaperReportIsAnError(t *testing.T) {
	if kind, _, err := readReport(filepath.Join(t.TempDir(), "none")); err == nil {
		t.Errorf("a missing report read as %q", kind)
	}
}

// The reaper signals only its own children: a pid read from a snapshot of /proc could belong to
// a grandchild that its parent reaped a moment later, and the pid to something else by then.
func TestTheReaperSignalsOnlyDirectChildren(t *testing.T) {
	table := map[int]procInfo{
		100: {ppid: 1}, 101: {ppid: 100}, 102: {ppid: 101}, 103: {ppid: 100}, 104: {ppid: 103}, 200: {ppid: 1},
	}
	if got, want := killTargets(100, table), []int{101, 103}; !reflect.DeepEqual(got, want) {
		t.Errorf("killTargets(100) = %v, want %v: grandchildren are never signalled", got, want)
	}
	if got, want := descendantsOf(100, table), []int{101, 102, 103, 104}; !reflect.DeepEqual(got, want) {
		t.Errorf("descendantsOf(100) = %v, want %v (it names; it does not signal)", got, want)
	}
}

// A test binary that dies leaves nothing: the reaper sees the pipe from the test close, kills
// what the script started (a command in its own session included) and exits. The test binary
// here is a second process, the runner (runnerMain), which is killed as a test timeout's kill
// would. The processes are found by the mark that only they carry, and the test waits on them
// going, not on a window.
func TestAKilledTestBinaryLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	mark := "LC_MARK=" + dir
	t.Cleanup(func() { killMarked(mark) }) // the last resort, if the reaper does not do it
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(dir, "ready")
	runner := exec.Command(self, filepath.Join(dir, "report"), ready, dir)
	runner.Env = append(os.Environ(), runnerEnv+"=1", mark)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the runner's script to start", func() bool {
		_, err := os.Stat(ready)
		return err == nil
	})
	waitForMarked(t, mark, 5, "the runner, the reaper, the script and two sleeps")
	if err := runner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = runner.Wait()
	waitUntil(t, "every process of the killed test to go", func() bool { return len(markedPIDs(mark)) == 0 })
}

// waitDeadline is how long a leftover test waits for a state before it gives up.
const waitDeadline = 30 * time.Second

// pollUntil polls cond every 10 ms and reports whether it held within d.
func pollUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// waitUntil waits, at most waitDeadline, for cond to hold: a wait on state, not a window.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !pollUntil(waitDeadline, cond) {
		t.Fatalf("gave up waiting for %s", what)
	}
}

// awaitCount polls count until it reaches want, for at most d. It returns the last count it read
// and whether that reached want.
func awaitCount(d time.Duration, want int, count func() int) (int, bool) {
	var n int
	ok := pollUntil(d, func() bool { n = count(); return n >= want })
	return n, ok
}

// waitForMarked waits, at most waitDeadline, until at least want live processes carry mark, then
// returns. The ready file is written while the setup is still starting: a process that has just
// been forked, or is in the middle of its exec, has no environment to read yet, and one that the
// script has not started yet is not there at all. So the count is a state to wait for, not a
// number to read once after the ready file (issue #287). A setup that really does not come up
// fails at the deadline, naming the last count; who names the processes that were wanted.
func waitForMarked(t *testing.T, mark string, want int, who string) {
	t.Helper()
	if n, ok := awaitCount(waitDeadline, want, func() int { return len(markedPIDs(mark)) }); !ok {
		t.Fatalf("%d processes carry the mark after %v, want at least %d (%s): the setup did not run, so this test proves nothing", n, waitDeadline, want, who)
	}
}

// The count of the leftover tests is waited for, not read once: a count that is short at the first
// read and right at the fourth passes. The counter is a fake that is short on its first reads, so
// the order is certain and no window decides it. A single read stops at the first call and fails.
func TestAwaitCountWaitsForALateCount(t *testing.T) {
	reads := []int{1, 1, 2, 3}
	calls := 0
	n, ok := awaitCount(waitDeadline, 3, func() int {
		v := reads[min(calls, len(reads)-1)]
		calls++
		return v
	})
	if !ok || n != 3 || calls != len(reads) {
		t.Errorf("awaitCount = %d, %v after %d reads; want 3, true after %d", n, ok, calls, len(reads))
	}
}

// A count that never gets there fails at the deadline, and the last count read is what the failure
// names. A deadline that is never reached, or a count that is not kept, would not. The fake
// records what it returned last, and the test compares with that, so no number of reads inside
// the 50 ms is assumed: a slow scheduler changes how many reads there are, not the verdict.
func TestAwaitCountGivesUpAtTheDeadlineWithTheLastCount(t *testing.T) {
	const d = 50 * time.Millisecond
	calls, last := 0, 0
	start := time.Now()
	n, ok := awaitCount(d, 3, func() int {
		calls++
		last = min(calls, 2) // 1, then 2 for ever
		return last
	})
	if ok || n != last || calls == 0 {
		t.Errorf("awaitCount = %d, %v after %d reads (the last read gave %d); want the last count read, and false", n, ok, calls, last)
	}
	if got := time.Since(start); got < d {
		t.Errorf("awaitCount gave up after %v, before its %v deadline", got, d)
	}
}

// A test that ends while its run is still going, a t.Fatal half way through, leaves nothing: the
// cleanup closes the pipe to the reaper and waits for it, and the reaper kills and reaps what the
// script started, a command in its own session included. A cleanup that killed the reaper's
// process group first would take the reaper before it had collected anything, and the command in
// the other session would survive.
func TestATestThatEndsMidRunLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	mark := "MID_MARK=" + dir
	t.Cleanup(func() { killMarked(mark) }) // the last resort, if the cleanup below does not do it
	t.Run("run", func(t *testing.T) {
		ready := filepath.Join(dir, "ready")
		script := `setsid sleep 600 >/dev/null 2>&1 </dev/null &
: > "$READY"
sleep 600`
		cmd := reaperCommand(t, filepath.Join(dir, "report"), reaperOpts{grace: 5 * time.Second}, dir, append(os.Environ(), mark, "READY="+ready), "-c", script)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		waitUntil(t, "the script to start", func() bool {
			_, err := os.Stat(ready)
			return err == nil
		})
		waitForMarked(t, mark, 3, "the reaper, the script and a sleep")
		// The subtest ends here, with the run going.
	})
	waitUntil(t, "every process of the ended run to go", func() bool { return len(markedPIDs(mark)) == 0 })
}

// The bounded wait of a stopped script (issue #494) gives up on one that does not stop, and its dump names what the tree is doing: a
// script that ignores TERM (here a bash named live-checks.sh, blocked reading a pipe the test holds open) shows TERM in its ignored
// signals and its parent, and the wait returns within its bound instead of waiting for ever. The expectations are the signal's number (TERM is 15,
// so bit 14 of the masks) and the bound given.
func TestAStoppedScriptThatDoesNotStopIsGivenUpOnAndItsDumpNamesIt(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pw.Close()
	defer pr.Close()
	cmd := exec.Command("bash", "-c", `trap '' TERM; read x`, "live-checks.sh")
	cmd.Stdin = pr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	// The script ignores TERM once its trap is in: wait on that state, not on a time.
	var dump string
	deadline := time.Now().Add(10 * time.Second)
	for {
		dump = hangDump(cmd.Process.Pid)
		if ignoresTerm(dump) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the script never showed TERM among its ignored signals:\n%s", dump)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The process's state is not asserted: a read of /proc can catch it running for the instant it takes to enter its read.
	if want := fmt.Sprintf("pid %d ppid %d state ", cmd.Process.Pid, os.Getpid()); !strings.Contains(dump, want) || !strings.Contains(dump, "live-checks.sh") {
		t.Errorf("the dump does not show the script with its parent:\n%s", dump)
	}
	// And the wait is bounded: signalled, the script still does not exit, so the wait ends at the bound, reporting it.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	type res struct {
		err      error
		timedOut bool
	}
	got := make(chan res, 1)
	go func() { e, to := waitBounded(cmd, 300*time.Millisecond); got <- res{e, to} }()
	select {
	case r := <-got:
		if !r.timedOut {
			t.Errorf("waitBounded returned (%v, timed out %t) for a script that ignores TERM, want it to give up at its bound", r.err, r.timedOut)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("waitBounded did not return: its wait is not bounded")
	}
}

// ignoresTerm reports whether the dump's SigIgn line has bit 14 (SIGTERM, signal 15) set.
func ignoresTerm(dump string) bool {
	for _, l := range strings.Split(dump, "\n") {
		if f := strings.Fields(l); len(f) == 2 && f[0] == "SigIgn:" {
			if v, err := strconv.ParseUint(f[1], 16, 64); err == nil && v&(1<<14) != 0 {
				return true
			}
		}
	}
	return false
}

// The dump names the lock a blocked process waits on and who holds it, a process of any tree (issue #494, since a wait on a host lock
// held by another package's test binary would look like a hang of this script). The test holds a flock on a file with a flock command of
// its own (outside the tree under dump), and starts a bash named live-checks.sh whose child flock waits on it: the dump of that bash must
// say that a pid waits on the lock, that the holder is the test's flock command, and the file's path. The expectations are the holder's
// PID and the file's path, both known to the test.
func TestTheDumpNamesTheLockAProcessWaitsOnAndItsHolder(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("no flock program on this host")
	}
	// holdLock starts a flock command holding a lock on a new file, and returns the file and the holder's PID once the lock is held.
	holdLock := func(name string) (string, int) {
		lock := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(lock, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		pr, pw, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { pw.Close(); pr.Close() })
		holder := exec.Command("flock", lock, "bash", "-c", "echo held; read x")
		holder.Stdin = pr
		out, err := holder.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = holder.Process.Kill(); _ = holder.Wait() })
		held := make(chan struct{})
		go func() {
			buf := make([]byte, 16)
			_, _ = out.Read(buf)
			close(held)
		}()
		select {
		case <-held:
		case <-time.After(30 * time.Second):
			t.Fatalf("the holder of %s never took the lock", name)
		}
		return lock, holder.Process.Pid
	}
	lock, holderPID := holdLock("host.lock")
	// Another lock, held by another process, that nothing in the tree under dump waits on or holds: it must not appear.
	other, otherPID := holdLock("other.lock")
	// The waiting flock is a CHILD of the bash named live-checks.sh (the bash cannot exec its last command: another follows), as the
	// script's remote() is a child of the script, so the dump has to walk the tree to find it.
	waiter := exec.Command("bash", "-c", `flock "$1" true; echo done`, "live-checks.sh", lock)
	if err := waiter.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.Process.Kill(); _ = waiter.Wait() })
	// Wait on the state: the dump shows the wait once the flock child has called flock(2).
	want := fmt.Sprintf("held by pid %d ", holderPID)
	var dump string
	deadline := time.Now().Add(10 * time.Second)
	for {
		dump = hangDump(waiter.Process.Pid)
		if strings.Contains(dump, "waits on lock") && strings.Contains(dump, want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the dump never named the lock and its holder (%q):\n%s", want, dump)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(dump, "the locked file: "+lock) {
		t.Errorf("the dump does not name the locked file %s:\n%s", lock, dump)
	}
	if want := fmt.Sprintf("ppid %d state ", waiter.Process.Pid); !strings.Contains(dump, want) {
		t.Errorf("the dump does not show the waiting flock as a child of the script (%q):\n%s", want, dump)
	}
	// What is not the tree's is not listed: not the other lock, not its holder, and no lock the tree holds (it holds none).
	if strings.Contains(dump, other) || strings.Contains(dump, fmt.Sprintf("pid %d ", otherPID)) || strings.Contains(dump, "holds:") {
		t.Errorf("the dump lists a lock or a holder that is not the waiting tree's:\n%s", dump)
	}
	t.Logf("the dump, as a hang would print it:\n%s", dump)
}

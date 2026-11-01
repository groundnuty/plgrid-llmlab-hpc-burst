package livechecks

import (
	"bytes"
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
	_ = cmd.Wait()
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
	if got, want := directChildrenOf(100, table), []int{101, 103}; !reflect.DeepEqual(got, want) {
		t.Errorf("directChildrenOf(100) = %v, want %v: grandchildren are never signalled", got, want)
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
	if n := len(markedPIDs(mark)); n < 5 {
		t.Fatalf("%d processes carry the mark, want at least the runner, the reaper, the script and two sleeps: the setup did not run, so this test proves nothing", n)
	}
	if err := runner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = runner.Wait()
	waitUntil(t, "every process of the killed test to go", func() bool { return len(markedPIDs(mark)) == 0 })
}

// waitUntil waits, at most 30 s, for cond to hold: a wait on state, not a window.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("gave up waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

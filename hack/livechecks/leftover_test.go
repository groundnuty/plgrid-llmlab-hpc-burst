package livechecks

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
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

type reaped struct {
	kind    string
	details []string
	code    int
	dir     string
}

// runReaped runs script (bash -c) under the reaper with the given grace and touch file, with
// PIDFILE and RELEASE (both in a fresh directory) in its environment, and returns the report and
// the exit code. When capture is not nil it is the script's standard output and error, as for the
// script tests.
func runReaped(t *testing.T, grace time.Duration, withTouch bool, capture *bytes.Buffer, script string) reaped {
	t.Helper()
	dir := t.TempDir()
	report, release := filepath.Join(dir, "report"), filepath.Join(dir, "release")
	touch := ""
	if withTouch {
		touch = release
	}
	env := append(os.Environ(), "PIDFILE="+filepath.Join(dir, "pid"), "RELEASE="+release)
	cmd := reaperCommand(t, report, grace, touch, dir, env, "-c", script)
	if capture != nil {
		cmd.Stdout, cmd.Stderr = capture, capture
		cmd.WaitDelay = 20 * time.Second // a regression that leaves the pipe open fails in 20 s, not 60
	}
	// The reaper's own group, if the test fails half way. The reaper kills what the script leaves.
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	_ = cmd.Run()
	kind, details, err := readReport(report)
	if err != nil {
		t.Fatal(err)
	}
	return reaped{kind: kind, details: details, code: cmd.ProcessState.ExitCode(), dir: dir}
}

// gone fails the test when the process whose pid the script wrote is still there.
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
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Errorf("process %d was still there after the reaper exited (kill 0: %v)", pid, err)
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
	r := runReaped(t, 200*time.Millisecond, false, nil, `sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A child in a new session is in no process group of the script, so a group probe cannot see it
// (issue #185). It is still a descendant, so the reaper does.
func TestReaperSeesAChildInANewSession(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, false, nil, `setsid bash -c 'echo $$ > "$PIDFILE"; exec sleep 60' >/dev/null 2>&1 </dev/null &`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// `timeout` runs its command in a process group of its own, which a group probe never covered:
// the ssh calls and the test binary of hack/live-checks.sh run under it.
func TestReaperSeesAProcessRunUnderTimeout(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, false, nil, `timeout 60 sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A process whose parent has already exited has been given to the reaper by the kernel.
func TestReaperSeesAnOrphan(t *testing.T) {
	r := runReaped(t, 200*time.Millisecond, false, nil, `( sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PIDFILE" )`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A leak that holds the captured standard output used to hide: Cmd.Run waits for the pipe to
// close, so the check started only after the leak had exited. The reaper judges the script's
// descendants itself and kills them, which closes the pipe.
func TestReaperSeesAProcessThatHoldsTheCapturedOutput(t *testing.T) {
	var out bytes.Buffer
	r := runReaped(t, 200*time.Millisecond, false, &out, `sleep 60 & echo $! > "$PIDFILE"`+waitForPid)
	r.mustBeLeftover(t)
	r.gone(t)
}

// A process that is still exiting when the script's exit is seen is waited for, not reported.
// The child exits on the release file, and the reaper creates that file right after the first
// probe that finds the child alive (reapAll's touch). So the child is certainly alive at the first
// probe and certainly gone only after it: a detector that probes once reports the child and fails
// here, whatever the scheduler does.
func TestReaperWaitsOutAProcessThatIsStillExiting(t *testing.T) {
	r := runReaped(t, 30*time.Second, true, nil, `( while [ ! -e "$RELEASE" ]; do sleep 0.02; done ) >/dev/null 2>&1 </dev/null &`)
	if r.kind != "clean" {
		t.Errorf("a process that exits soon after the script was reported: %q %v", r.kind, r.details)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "release")); err != nil {
		t.Errorf("the reaper never saw the child alive, so it never released it: %v", err)
	}
}

// The script's exit code reaches the test unchanged, and a script killed by a signal reads as
// 128 plus the signal. The report of a clean run is "clean".
func TestReaperRelaysTheExitStatus(t *testing.T) {
	for _, c := range []struct {
		script string
		code   int
	}{{`exit 0`, 0}, {`exit 7`, 7}, {`kill -KILL $$`, 128 + int(syscall.SIGKILL)}} {
		r := runReaped(t, time.Second, false, nil, c.script)
		if r.code != c.code || r.kind != "clean" {
			t.Errorf("%q: exit %d, report %q; want exit %d, clean", c.script, r.code, r.kind, c.code)
		}
	}
}

// A signal sent to the reaper reaches the script, whose own handler decides what happens.
func TestReaperForwardsASignalToTheScript(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	script := `trap 'exit 5' TERM; : > "$READY"; while :; do sleep 0.02; done`
	cmd := reaperCommand(t, filepath.Join(dir, "report"), time.Second, "", dir, append(os.Environ(), "READY="+ready), "-c", script)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
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
	_ = cmd.Wait()
	if got := cmd.ProcessState.ExitCode(); got != 5 {
		t.Errorf("exit %d, want 5: the script's trap did not run", got)
	}
}

// A report that is not there is not a clean one: the reaper may have died before it wrote it.
func TestAMissingReaperReportIsAnError(t *testing.T) {
	if kind, _, err := readReport(filepath.Join(t.TempDir(), "none")); err == nil {
		t.Errorf("a missing report read as %q", kind)
	}
}

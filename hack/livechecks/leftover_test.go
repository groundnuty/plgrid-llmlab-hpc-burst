package livechecks

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// leader runs script as bash does for the live-checks tests: the leader of its own process
// group, with no pipe on its output, so the wait ends when bash exits and not when its
// children close a pipe. It returns the group id after the leader has exited and been reaped,
// and kills what is left of the group when the test ends (by number, so with the PID-reuse limit
// that noLeftoverProcess notes).
func leader(t *testing.T, script string, env ...string) int {
	t.Helper()
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pgid := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) })
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the leader failed: %v", err)
	}
	return pgid
}

// A process the run leaves behind for good is reported. Without this test, deleting the
// check (or turning it into "never alive") would leave every other test green.
func TestLeftoverCheckReportsAProcessThatNeverExits(t *testing.T) {
	pgid := leader(t, `sleep 600 >/dev/null 2>&1 </dev/null &`)
	if !groupExists(pgid) {
		t.Fatal("the backgrounded process is not in the group, so this test proves nothing")
	}
	if !groupOutlives(pgid, 200*time.Millisecond) {
		t.Error("a process still running at the deadline was not reported")
	}
}

// A process that is still exiting when the script's exit is seen is waited for, not reported.
// The child exits on a file, and the file is written by pollGroup's hook, which runs right after
// the first probe that finds a member. So the child is certainly alive at the first probe, and
// certainly gone only after it: a detector that probes once reports the child and fails here,
// whatever the scheduler does. (Releasing it from a goroutine after a sleep would leave a window
// in which the child could exit before the first probe, and a one-shot detector would pass.)
func TestLeftoverCheckWaitsOutAProcessThatIsStillExiting(t *testing.T) {
	release := filepath.Join(t.TempDir(), "release")
	pgid := leader(t, `( while [ ! -e "$RELEASE" ]; do sleep 0.02; done ) >/dev/null 2>&1 </dev/null &`, "RELEASE="+release)
	if !groupExists(pgid) {
		t.Fatal("the late child is not in the group, so this test proves nothing: the old probe would pass too")
	}
	released := false
	outlives := pollGroup(pgid, 60*time.Second, func() {
		released = true
		if err := os.WriteFile(release, nil, 0o600); err != nil {
			t.Fatalf("the child cannot be released, so this test proves nothing: %v", err)
		}
	})
	if !released {
		t.Error("the detector never saw the child, so it never released it")
	}
	if outlives {
		t.Error("a process that exits soon after the script was reported as left behind")
	}
}

// A group with no member at all is not reported, and the answer comes at once, not at the deadline.
func TestLeftoverCheckAcceptsAnEmptyGroup(t *testing.T) {
	pgid := leader(t, `true`)
	if groupOutlives(pgid, 0) {
		t.Error("a group with no member was reported as left behind")
	}
}

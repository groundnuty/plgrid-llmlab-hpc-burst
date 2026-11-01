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
// and kills what is left of the group when the test ends.
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
// The child here exits on a file the test writes, so it is certainly alive when the leader
// exits (the old one-shot probe fails on it), and certainly gone once the file is there.
func TestLeftoverCheckWaitsOutAProcessThatIsStillExiting(t *testing.T) {
	release := filepath.Join(t.TempDir(), "release")
	pgid := leader(t, `( while [ ! -e "$RELEASE" ]; do sleep 0.02; done ) >/dev/null 2>&1 </dev/null &`, "RELEASE="+release)
	if !groupExists(pgid) {
		t.Fatal("the late child is not in the group, so this test proves nothing: the old probe would pass too")
	}
	go func() {
		time.Sleep(100 * time.Millisecond) // only so that the poll runs a few rounds; the verdict does not depend on it
		_ = os.WriteFile(release, nil, 0o600)
	}()
	if groupOutlives(pgid, 60*time.Second) {
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

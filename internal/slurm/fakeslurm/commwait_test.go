package fakeslurm

// The wait for a process's name (waitForComm, keeper_test.go; issue #619). A process that was only forked carries its parent's name until its
// exec; the kernel gives it the exec'd file's name then. TestABackgroundChildNamedWithANewlineEndsWhenItsHostIsKilled read the name of the
// loop its batch shell had just forked, the moment the shell wrote the loop's PID, and on CI read "sh\n", the shell's name, so the test raced
// its own rename. These tests use a child that is a shell until a gate is opened and the newline-named shell after, so each step is ordered
// by the wait's own reads (its tick), never by a sleep.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// gatedChild starts a shell, named "sh" by the kernel, that blocks reading a FIFO and then execs the shell again under a link whose name holds
// a newline, as the batch shell's loop does. It returns the child's PID and release, which opens the gate: the child takes its new name
// after that, and not before.
func gatedChild(t *testing.T) (pid int, release func()) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Symlink(jobShell, filepath.Join(dir, newlineName)); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(dir, "gate")
	if err := syscall.Mkfifo(gate, 0o600); err != nil {
		t.Fatal(err)
	}
	script := `read x < "$1"; n=$(printf "line\\nbreak"); exec "$2/$n" -c "sleep 60"`
	cmd := exec.Command(jobShell, "-c", script, "gated", gate, dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	// Opening the FIFO for writing waits for the child's open for reading, so the release reaches it whenever it gets there.
	return cmd.Process.Pid, func() {
		if err := os.WriteFile(gate, []byte("go\n"), 0); err != nil {
			t.Errorf("open the gate: %v", err)
		}
	}
}

// A process that has been forked and not exec'd yet holds the wait: it reads the shell's name three times, the child is released on the third
// read, and the wait returns only once the name is the new one, having read the old one first. The steps are ordered by the wait's tick.
func TestWaitForCommHoldsUntilTheProcessTakesItsName(t *testing.T) {
	pid, release := gatedChild(t)
	want := newlineName + "\n"
	released := false
	first, reads, err := waitForComm(pid, want, waitDeadline, func(reads int, got string) error {
		if got != "sh\n" {
			return fmt.Errorf("read %q before the gate was opened (read %d), want the shell's name %q", got, reads, "sh\n")
		}
		if reads == 3 && !released {
			released = true
			release()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("the wait: %v", err)
	}
	if first != "sh\n" {
		t.Errorf("the first read was %q, want the shell's name %q: the child was not forked and unnamed when the wait began", first, "sh\n")
	}
	if reads < 4 {
		t.Errorf("the wait returned after %d reads, want 4 or more: it read the old name three times before the gate opened", reads)
	}
	if got, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid)); err != nil || string(got) != want {
		t.Errorf("after the wait the child is named %q (%v), want %q", got, err, want)
	}
}

// A process that never takes the name fails the wait at its bound, naming the name it last read and the name it wanted, after more than one
// read: a wait that read once would pass the test above's first read and fail here. The bound is short: the test waits for the failure, not for
// a time.
func TestWaitForCommFailsNamingTheNameItLastRead(t *testing.T) {
	pid, _ := gatedChild(t)
	const bound = 150 * time.Millisecond
	_, reads, err := waitForComm(pid, newlineName+"\n", bound, nil)
	if err == nil {
		t.Fatal("a process that never took the name passed the wait")
	}
	for _, want := range []string{"is named \"sh\\n\"", "want \"line\\nbreak\\n\"", "within " + bound.String(), fmt.Sprintf("process %d", pid)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not say %q", err, want)
		}
	}
	if reads < 2 {
		t.Errorf("the wait read the name %d time(s) in %s, want it to read again", reads, bound)
	}
}

// A process that is gone fails the wait at once, as such: an exec that failed leaves a zombie, whose comm reads on as the shell's name, and the
// wait would otherwise run to its bound. The bound here is long, so an answer within it is the early return.
func TestWaitForCommFailsAtOnceForAProcessThatIsGone(t *testing.T) {
	cmd := exec.Command(jobShell, "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Wait() }) // not reaped until then: a zombie, which procGone reads as gone
	_, _, err := waitForComm(cmd.Process.Pid, newlineName+"\n", waitDeadline, nil)
	if err == nil || !strings.Contains(err.Error(), "is gone") {
		t.Errorf("a process that exited: %v, want an error saying it is gone", err)
	}
}

// An error from tick ends the wait with that error: the keeper test passes the helper host's end through it.
func TestWaitForCommReturnsWhatItsTickReturns(t *testing.T) {
	pid, _ := gatedChild(t)
	stop := errors.New("the host ended")
	_, reads, err := waitForComm(pid, newlineName+"\n", waitDeadline, func(reads int, _ string) error {
		if reads == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || reads != 2 {
		t.Errorf("the wait returned %v after %d reads, want the tick's error after 2", err, reads)
	}
}

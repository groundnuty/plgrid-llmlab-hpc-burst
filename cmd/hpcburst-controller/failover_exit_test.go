package main

// How the first copy of failover_test.go may end. The test kills it with
// SIGKILL once its sbatch has reached the fake Slurm. On a loaded runner the
// copy could miss its own Lease renewal first, lose leadership and exit by
// itself (the manager's "leader election lost"); the test's SIGKILL would then
// find an exited process, or one that is exited but not reaped yet. Such an
// exit is a failover trigger just as the kill is, so endMidSubmit accepts it,
// and nothing else. #737 asked for this; its logged failure was another race,
// in child.kill (child_kill_test.go). The rows below drive endMidSubmit with
// real sh children and real exits, which costs no Lease wait; the copy's real
// line and exit code were read from a copy that lost its Lease to a holder
// that kept renewing it.

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// lostLeadershipText is the error that controller-runtime's manager returns
// from Start when its leader elector gives up the Lease: errors.New("leader
// election lost") in v0.23.1, pkg/manager/internal.go:637 (initLeaderElector's
// OnStoppedLeading). run returns it, and controllerChild, like main, prints it
// after childErrPrefix and exits 1. Start can join it with a stop error, so it
// is matched anywhere after the prefix. The elector logs the cause first, as
// "Failed to renew lease" (client-go v0.35.3,
// tools/leaderelection/leaderelection.go:299), and that line alone does not
// say the process left: only the Lease of the lifecycle manager ends the
// process, since a Provider manager that loses its own is started again by its
// supervisor (internal/providerctl/supervisor.go, fail).
const lostLeadershipText = "leader election lost"

// lostLeaderExitCode is controllerChild's exit code for a run that returned an
// error.
const lostLeaderExitCode = 1

// lostLeadershipLine is the line of log that the process printed on its way
// out through lost leadership: its own error line, not a log entry about it.
func lostLeadershipLine(log string) (string, bool) {
	for _, line := range strings.Split(log, "\n") {
		if strings.HasPrefix(line, childErrPrefix) && strings.Contains(line, lostLeadershipText) {
			return line, true
		}
	}
	return "", false
}

// endMidSubmit ends the first copy, whose first sbatch reached the fake at
// midSubmit, with SIGKILL and reports how it ended. Two ends are right:
//   - killed: the copy was alive when the signal came, and its wait status is
//     SIGKILL (earlyExit false);
//   - earlyExit: the copy had already left on its own, and all of these hold:
//     it left at or after midSubmit, so it had reached the mid-submit point;
//     its wait status is a normal exit with lostLeaderExitCode; and its own
//     stderr shows the process ending through lost leadership.
//
// Any other end is an error that names the wait status and the end of the log:
// a crash, a panic, an exit before the mid-submit point or by another signal, or
// an exit with a code or a log that does not say lost leadership.
func (c *child) endMidSubmit(midSubmit time.Time) (earlyExit bool, ws syscall.WaitStatus, err error) {
	if err := c.sigkill(); err != nil {
		return false, 0, err
	}
	ws, ok := c.reaped(30 * time.Second)
	if !ok {
		return false, 0, fmt.Errorf("child %s did not exit within 30 s of SIGKILL", c.name)
	}
	if ws.Signaled() && ws.Signal() == syscall.SIGKILL {
		return false, ws, nil
	}
	// Not our kill: the copy ended by itself, before the signal took effect.
	reject := func(why string) (bool, syscall.WaitStatus, error) {
		return false, ws, fmt.Errorf("child %s ended by itself with %s, and %s; the end of its stderr:\n%s", c.name, waitStatusText(ws), why, c.tail(30))
	}
	switch {
	case !ws.Exited():
		return reject("that is not a normal exit")
	case ws.ExitStatus() != lostLeaderExitCode:
		return reject(fmt.Sprintf("that is not the exit status %d of a run that returned an error", lostLeaderExitCode))
	case c.exitedAt.Before(midSubmit):
		return reject(fmt.Sprintf("it left %s before its sbatch reached the fake", midSubmit.Sub(c.exitedAt).Round(time.Millisecond)))
	}
	if _, ok := lostLeadershipLine(c.log()); !ok {
		return reject("its stderr has no line of the process ending with " + `"` + lostLeadershipText + `"`)
	}
	return true, ws, nil
}

// shChild is startSh with its reaping started, as launchChild's children have,
// and its kill and reaping at the end of the test.
func shChild(t *testing.T, name, script string) *child {
	t.Helper()
	c := startSh(t, name, script)
	c.watch()
	t.Cleanup(func() { c.kill(t) })
	return c
}

// awaitExit waits until the process has been reaped.
func awaitExit(t *testing.T, c *child) {
	t.Helper()
	select {
	case <-c.exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("child %s did not exit within 30 s", c.name)
	}
}

// lostLine is what the controller prints when its run returns the manager's
// lost-leadership error.
var lostLine = childErrPrefix + " " + lostLeadershipText

func TestEndMidSubmit(t *testing.T) {
	// Each row's child is a real process. mark is when the first sbatch "reached
	// the fake": before the child starts, except where a row says otherwise.
	exitsAfter := func(t *testing.T, script string) (c *child, mark time.Time) {
		mark = time.Now()
		c = shChild(t, "A", script)
		awaitExit(t, c)
		return c, mark
	}

	t.Run("a copy that left through lost leadership after the mid-submit point is accepted", func(t *testing.T) {
		c, mark := exitsAfter(t, "echo 'a log line' >&2; echo '"+lostLine+"' >&2; exit 1")
		early, ws, err := c.endMidSubmit(mark)
		if err != nil || !early || !ws.Exited() || ws.ExitStatus() != 1 {
			t.Errorf("endMidSubmit = (early %t, %v, %v), want an accepted early exit with status 1", early, ws, err)
		}
	})

	t.Run("an exit with the same code and no such line is rejected, naming the end of the log", func(t *testing.T) {
		c, mark := exitsAfter(t, "echo 'panic: runtime error: index out of range' >&2; exit 1")
		early, _, err := c.endMidSubmit(mark)
		if err == nil || early {
			t.Fatalf("endMidSubmit = (early %t, %v), want a rejection", early, err)
		}
		if !strings.Contains(err.Error(), "panic: runtime error: index out of range") {
			t.Errorf("the rejection does not name the end of the log: %v", err)
		}
	})

	t.Run("a line that only logs the lost Lease is not the process leaving through it", func(t *testing.T) {
		c, mark := exitsAfter(t, "echo '2026-11-17T10:00:00Z ERROR component failed err=\""+lostLeadershipText+"\"' >&2; exit 1")
		if early, _, err := c.endMidSubmit(mark); err == nil || early {
			t.Errorf("endMidSubmit = (early %t, %v), want a rejection", early, err)
		}
	})

	t.Run("a copy ended by another signal is rejected, even with the line in its log", func(t *testing.T) {
		c, mark := exitsAfter(t, "echo '"+lostLine+"' >&2; kill -TERM $$")
		early, ws, err := c.endMidSubmit(mark)
		if err == nil || early {
			t.Fatalf("endMidSubmit = (early %t, %v, %v), want a rejection", early, ws, err)
		}
		if !ws.Signaled() || ws.Signal() != syscall.SIGTERM || !strings.Contains(err.Error(), "terminated") {
			t.Errorf("the rejection should report the SIGTERM end: %v (%v)", err, ws)
		}
	})

	t.Run("an exit with another code is rejected, even with the line in its log", func(t *testing.T) {
		c, mark := exitsAfter(t, "echo '"+lostLine+"' >&2; exit 2")
		if early, _, err := c.endMidSubmit(mark); err == nil || early {
			t.Errorf("endMidSubmit = (early %t, %v), want a rejection", early, err)
		}
	})

	t.Run("an exit before the mid-submit point is rejected, even with the line in its log", func(t *testing.T) {
		c := shChild(t, "A", "echo '"+lostLine+"' >&2; exit 1")
		awaitExit(t, c)
		mark := time.Now() // its sbatch "reaches the fake" only after the copy has left
		if early, _, err := c.endMidSubmit(mark); err == nil || early {
			t.Errorf("endMidSubmit = (early %t, %v), want a rejection", early, err)
		}
	})

	t.Run("a copy that is still running is killed by SIGKILL", func(t *testing.T) {
		mark := time.Now()
		c := shChild(t, "A", "echo started >&2; exec sleep 60")
		if !c.running() {
			t.Fatal("the child is not running")
		}
		early, ws, err := c.endMidSubmit(mark)
		if err != nil || early || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
			t.Errorf("endMidSubmit = (early %t, %v, %v), want it killed by SIGKILL", early, ws, err)
		}
		if !c.gone() {
			t.Error("the child's PID is still there after it was reaped")
		}
	})

	t.Run("a copy that has exited but is not reaped yet is accepted", func(t *testing.T) {
		mark := time.Now()
		c := startSh(t, "A", "echo '"+lostLine+"' >&2; exit 1")
		awaitZombie(t, c)
		// Its exit is known to the kernel and nobody has waited for it: the
		// signal reaches a zombie (it can succeed), and the status that
		// reaping then gives is the exit, not SIGKILL.
		time.AfterFunc(100*time.Millisecond, c.watch)
		early, ws, err := c.endMidSubmit(mark)
		if err != nil || !early || !ws.Exited() || ws.ExitStatus() != 1 {
			t.Errorf("endMidSubmit = (early %t, %v, %v), want an accepted early exit with status 1", early, ws, err)
		}
	})
}

// awaitZombie waits until the process has exited and nobody has waited for it.
func awaitZombie(t *testing.T, c *child) {
	t.Helper()
	path := fmt.Sprintf("/proc/%d/stat", c.cmd.Process.Pid)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		// pid (comm) state ...: the state follows the last ')'.
		s := string(b)
		if rest := s[strings.LastIndexByte(s, ')')+1:]; strings.HasPrefix(strings.TrimSpace(rest), "Z") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("child %s did not become a zombie within 30 s", c.name)
}

package fakeslurm

// New. Tests of how run mode ends a job's processes (doc.go, Run mode). As in
// run_test.go, expected values come from issue #146 and Slurm's documented
// behaviour, and every wait is on observable state.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// procGone reports whether a PID names no live process: no /proc entry, or a
// zombie waiting for its new parent to reap it.
func procGone(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	return len(f) > 0 && (f[0] == "Z" || f[0] == "X")
}

// waitGroupGone waits until the kernel reports no process in the group. It is
// a different thing from Finished: a zombie is a member until its parent reaps
// it, and an orphaned descendant is reaped by init, a little after the fake
// has found it dead.
func waitGroupGone(t *testing.T, pgid int) {
	t.Helper()
	waitFor(t, "process group "+itoa(pgid)+" to be empty", func() bool { return groupGone(pgid) })
}

// scancel ends the job CANCELLED at once, sends TERM to its processes and,
// after KillWait, KILL to whatever ignores the TERM.
func TestRunModeScancelSendsTermThenKill(t *testing.T) {
	t.Run("a job that ignores TERM gets KILL after the grace", func(t *testing.T) {
		// The shell ignores TERM, so nothing depends on how fast it runs a
		// handler: it lives until the KILL, and the grace is a lower bound.
		const grace = 300 * time.Millisecond
		r := newRunRig(t, RunMode{KillWait: grace})
		up := r.home + "/up"
		id := r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(r.home+"/never"), "--time=10")
		r.waitFile(up)
		r.must("scancel " + itoa(id))
		if j := r.job(id); j.State != Cancelled {
			t.Fatalf("after scancel: state %s, want CANCELLED", j.State)
		}
		if got := r.squeueJob(id)["job_state"]; !equalStrings(got, Cancelled) {
			t.Errorf("squeue job_state %v after scancel, want [CANCELLED]", got)
		}
		j := r.waitFinished(id)
		switch {
		case j.TermAt.IsZero() || j.KillAt.IsZero():
			t.Errorf("TermAt %v, KillAt %v; want both sent", j.TermAt, j.KillAt)
		case j.KillAt.Sub(j.TermAt) < grace:
			t.Errorf("KILL came %s after TERM, before the grace of %s", j.KillAt.Sub(j.TermAt), grace)
		}
		if j.ExitSignal != syscall.SIGKILL || j.State != Cancelled {
			t.Errorf("ended by %v in state %s, want SIGKILL and CANCELLED", j.ExitSignal, j.State)
		}
		waitGroupGone(t, j.PID)
	})
	t.Run("a job that dies of TERM gets no KILL", func(t *testing.T) {
		r := newRunRig(t, RunMode{KillWait: 10 * time.Minute})
		up := r.home + "/up"
		id := r.submit("echo up > " + up + "; " + waitLoop(r.home+"/never"))
		r.waitFile(up)
		r.must("scancel " + itoa(id))
		j := r.waitFinished(id)
		if j.TermAt.IsZero() || !j.KillAt.IsZero() || j.ExitSignal != syscall.SIGTERM || j.State != Cancelled {
			t.Errorf("TermAt %v, KillAt %v, signal %v, state %s; want TERM only, SIGTERM, CANCELLED", j.TermAt, j.KillAt, j.ExitSignal, j.State)
		}
	})
}

// The time limit, in RunMode.Minute units, ends a running job TIMEOUT and
// sends it TERM, no sooner than the limit after the job started: the wall
// time before the submission is a lower bound on the start. squeue's
// projected end_time is the start plus the limit in those units. A limit set
// by SetTimeLimit is in runlimit_test.go.
func TestRunModeTimeLimitGivesTimeout(t *testing.T) {
	const minute = 100 * time.Millisecond
	r := newRunRig(t, RunMode{Minute: minute, KillWait: 10 * time.Minute})
	before := time.Now()
	id := r.submit(waitLoop(r.home+"/never"), "--time=2")
	j := r.waitFinished(id)
	if j.State != Timeout || j.TermAt.IsZero() || j.ExitSignal != syscall.SIGTERM {
		t.Errorf("state %s, TermAt %v, signal %v; want TIMEOUT after a TERM", j.State, j.TermAt, j.ExitSignal)
	}
	if got := j.TermAt.Sub(before); got < 2*minute {
		t.Errorf("TERM came %s after the submission, before the limit of %s", got, 2*minute)
	}
	if got := r.squeueJob(id)["state_reason"]; got != "TimeLimit" {
		t.Errorf("squeue state_reason %v, want TimeLimit", got)
	}
}

// When the batch script ends, what it left behind is killed, as Slurm's
// cgroup does: a background child in a process group of its own too. The job
// is COMPLETED only once nothing is left.
func TestRunModeKillsWhatTheScriptLeaves(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newRunRig(t, RunMode{Env: map[string]string{helperEnv: "setpgid-sleep"}})
	child := r.home + "/child"
	id := r.submit(exe + " " + child + " & " + waitLoop(child) + "; exit 0")
	f := strings.Fields(r.waitFile(child))
	if len(f) != 2 {
		t.Fatalf("child file %q, want a PID and a process group", f)
	}
	pid, _ := strconv.Atoi(f[0])
	pgrp, _ := strconv.Atoi(f[1])
	j := r.waitFinished(id)
	if pgrp == j.PID {
		t.Fatalf("the child shares the batch shell's group %d; the test needs one of its own", pgrp)
	}
	if !procGone(pid) {
		t.Errorf("the background child %d (group %d) outlived the job, which is %s", pid, pgrp, j.State)
	}
	if j.State != Completed {
		t.Errorf("state %s, want COMPLETED", j.State)
	}
}

// A requeue kills the running execution, and the new one starts once it is
// gone, in a directory of its own, with SLURM_RESTART_COUNT set.
func TestRunModeRequeueRestartsTheScript(t *testing.T) {
	jobDir := filepath.Join(t.TempDir(), "j")
	r := newRunRig(t, RunMode{JobDir: jobDir})
	wrap := "echo \"${SLURM_RESTART_COUNT:-none} $TMPDIR\" > " + r.home + "/run-${SLURM_RESTART_COUNT:-0}; " + waitLoop(r.home+"/go")
	id := r.submit(wrap)
	first := r.waitFile(r.home + "/run-0")
	old := r.job(id).PID
	if err := r.p.Requeue(id); err != nil {
		t.Fatal(err)
	}
	second := r.waitFile(r.home + "/run-1")
	j := r.job(id)
	if first != "none "+jobDir+"/"+itoa(id)+"/tmp\n" || second != "1 "+jobDir+"/"+itoa(id)+".2/tmp\n" {
		t.Errorf("runs wrote %q and %q", first, second)
	}
	if j.PID == old || j.Executions != 2 || j.State != Running {
		t.Errorf("after the requeue: PID %d (was %d), %d executions, state %s; want a new process, 2, RUNNING", j.PID, old, j.Executions, j.State)
	}
	waitGroupGone(t, old)
	if err := os.WriteFile(r.home+"/go", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if j := r.waitFinished(id); j.State != Completed {
		t.Errorf("state %s, want COMPLETED", j.State)
	}
}

// Close kills every running job and waits for it; nothing starts after it.
func TestRunModeCloseEndsEveryProcess(t *testing.T) {
	r := newRunRig(t, RunMode{})
	up := r.home + "/up"
	id := r.submit("trap \"\" TERM; echo up > " + up + "; " + waitLoop(r.home+"/never"))
	r.waitFile(up)
	pid := r.job(id).PID
	if err := r.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitGroupGone(t, pid)
	late := r.submit("true")
	if j := r.job(late); j.State != Pending || j.PID != 0 {
		t.Errorf("a job submitted after Close: state %s, PID %d; want PENDING, no process", j.State, j.PID)
	}
	if err := r.p.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}

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

// scancel ends the job CANCELLED at once, sends TERM to its processes and,
// after KillWait, KILL to whatever ignores the TERM.
func TestRunModeScancelSendsTermThenKill(t *testing.T) {
	t.Run("a job that traps TERM gets KILL after the grace", func(t *testing.T) {
		const grace = 300 * time.Millisecond
		r := newRunRig(t, RunMode{KillWait: grace})
		up, term := r.home+"/up", r.home+"/term"
		id := r.submit("trap \"echo term >> "+term+"\" TERM; echo up > "+up+"; "+waitLoop(r.home+"/never"), "--time=10")
		r.waitFile(up)
		r.must("scancel " + itoa(id))
		if j := r.job(id); j.State != Cancelled {
			t.Fatalf("after scancel: state %s, want CANCELLED", j.State)
		}
		if got := r.squeueJob(id)["job_state"]; !equalStrings(got, Cancelled) {
			t.Errorf("squeue job_state %v after scancel, want [CANCELLED]", got)
		}
		r.waitFile(term) // the trap ran: the job got TERM and lived on
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
		if !groupGone(j.PID) {
			t.Errorf("process group %d still has processes", j.PID)
		}
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
// sends it TERM; squeue's projected end_time is the start plus the limit in
// those units. A limit lowered below the time already run ends it at once.
func TestRunModeTimeLimitGivesTimeout(t *testing.T) {
	t.Run("the limit passes", func(t *testing.T) {
		r := newRunRig(t, RunMode{Minute: 100 * time.Millisecond, KillWait: 10 * time.Minute})
		id := r.submit(waitLoop(r.home+"/never"), "--time=2")
		j := r.waitFinished(id)
		if j.State != Timeout || j.TermAt.IsZero() || j.ExitSignal != syscall.SIGTERM {
			t.Errorf("state %s, TermAt %v, signal %v; want TIMEOUT after a TERM", j.State, j.TermAt, j.ExitSignal)
		}
		if got := r.squeueJob(id)["state_reason"]; got != "TimeLimit" {
			t.Errorf("squeue state_reason %v, want TimeLimit", got)
		}
	})
	t.Run("a lowered limit", func(t *testing.T) {
		r := newRunRig(t, RunMode{Minute: 50 * time.Millisecond, KillWait: 10 * time.Minute})
		up := r.home + "/up"
		id := r.submit("echo up > "+up+"; "+waitLoop(r.home+"/never"), "--time=6000") // 300 s in 50 ms minutes
		r.waitFile(up)
		end := r.squeueJob(id)["end_time"].(map[string]any)
		if want := float64(t0.Add(300 * time.Second).Unix()); end["set"] != true || end["number"] != want {
			t.Errorf("projected end_time %v, want set at %v (start plus 6000 minutes of 50 ms)", end, want)
		}
		if j := r.job(id); j.State != Running {
			t.Fatalf("state %s before the limit is lowered, want RUNNING", j.State)
		}
		if err := r.p.SetTimeLimit(id, 1); err != nil {
			t.Fatal(err)
		}
		if j := r.waitFinished(id); j.State != Timeout {
			t.Errorf("state %s after the limit was lowered below the time run, want TIMEOUT", j.State)
		}
	})
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
	if !groupGone(old) {
		t.Errorf("the first execution's group %d still has processes", old)
	}
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
	if !groupGone(pid) {
		t.Errorf("process group %d still has processes after Close", pid)
	}
	late := r.submit("true")
	if j := r.job(late); j.State != Pending || j.PID != 0 {
		t.Errorf("a job submitted after Close: state %s, PID %d; want PENDING, no process", j.State, j.PID)
	}
	if err := r.p.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}

package fakeslurm

// Issue #211: pidfd_open answers EINVAL, not ESRCH, for a process that is mid-exit (measured on
// Linux 6.8.0: 499 EINVAL, 103,299 ESRCH and 1,359,369 successes in 3,000 start-and-exit cycles of
// sh -c 'exit 0'), so the sweep failed with "pidfd_open of process N: invalid argument" when a job's
// process exited between the listing and the pidfd, and Close returned that error. The science
// agent's rule (1 Nov 2026): EINVAL means "ended" only after a re-check with kill(pid, 0) that says
// ESRCH, and a zombie counts as ended if its row shows it; a live process that gets EINVAL is an
// error. The kernel cannot be made to answer EINVAL on demand, so the opener is a seam (procAPI.open)
// that returns it for a process the test chooses; everything else, the re-check included, is real
// in the tests that use processes. Expected values are written down by the tests.

import (
	"errors"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

// signalOne's answer to EINVAL, with every probe scripted: the events are the order the kernel was
// asked, and no signal is sent in any case, since a process that could not be pinned is never signalled.
func TestEINVALFromPidfdOpenIsEndedOnlyWhenTheRecheckAgrees(t *testing.T) {
	zombie := func(int) (procRow, bool, error) { return member, false, nil }
	unreadable := errors.New("row unreadable (scripted)")
	for _, c := range []struct {
		name    string
		s       *scripted
		want    string
		wantErr string // "" for no error
	}{
		{"kill(0) says no such process: it ended mid-exit", &scripted{openErr: syscall.EINVAL, probeErr: syscall.ESRCH, rowNow: sameNow}, "open 41,probe 41", ""},
		{"kill(0) finds it and its row shows a zombie", &scripted{openErr: syscall.EINVAL, rowNow: zombie}, "open 41,probe 41,row 41", ""},
		{"kill(0) finds it, another user's, and its row shows a zombie", &scripted{openErr: syscall.EINVAL, probeErr: syscall.EPERM, rowNow: zombie}, "open 41,probe 41,row 41", ""},
		{"kill(0) finds it and its row is live: not ended", &scripted{openErr: syscall.EINVAL, rowNow: sameNow}, "open 41,probe 41,row 41", "pidfd_open of process 41: invalid argument"},
		{"kill(0) cannot tell", &scripted{openErr: syscall.EINVAL, probeErr: syscall.EIO, rowNow: sameNow}, "open 41,probe 41", "pidfd_open of process 41: invalid argument (re-check: kill(41, 0): input/output error)"},
		{"the row cannot be read", &scripted{openErr: syscall.EINVAL, rowNow: func(int) (procRow, bool, error) { return procRow{}, false, unreadable }}, "open 41,probe 41,row 41", "pidfd_open of process 41: invalid argument (re-check: read process 41: row unreadable (scripted))"},
		// Unchanged: only EINVAL is re-checked.
		{"ESRCH is still ended with no re-check", &scripted{openErr: syscall.ESRCH}, "open 41", ""},
		{"EPERM is still an error with no re-check", &scripted{openErr: syscall.EPERM}, "open 41", "pidfd_open of process 41: operation not permitted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := signalProcs(c.s.api(), []procRow{member}, syscall.SIGKILL)
			if got := strings.Join(c.s.events, ","); got != c.want {
				t.Errorf("events %q, want %q", got, c.want)
			}
			switch {
			case c.wantErr == "" && err != nil:
				t.Errorf("error %v, want none", err)
			case c.wantErr != "" && (err == nil || err.Error() != c.wantErr):
				t.Errorf("error %v, want %q", err, c.wantErr)
			}
		})
	}
}

// einvalFor is a host procAPI whose opener answers EINVAL for the PIDs given and counts the answers.
func einvalFor(answers *atomic.Int64, pids ...int) *procAPI {
	api := hostProc()
	open := api.open
	api.open = func(pid int) (int, error) {
		for _, p := range pids {
			if p == pid {
				answers.Add(1)
				return -1, syscall.EINVAL
			}
		}
		return open(pid)
	}
	return api
}

// With real processes and the kernel's own re-check (kill(pid, 0) and the /proc row), only the
// opener is scripted. A process that is alive is not taken as ended, and is left alone; one that
// exited and was reaped, and a zombie, are.
func TestEINVALFromPidfdOpenWithRealProcesses(t *testing.T) {
	start := func(name string, args ...string) *exec.Cmd {
		cmd := exec.Command(name, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		return cmd
	}
	rowOf := func(api *procAPI, pid int) procRow {
		r, live, err := api.row(pid)
		if err != nil || !live {
			t.Fatalf("row of %d: live %v, %v", pid, live, err)
		}
		return r
	}

	t.Run("a live process is not taken as ended", func(t *testing.T) {
		live := start("sleep", "120")
		var answers atomic.Int64
		api := einvalFor(&answers, live.Process.Pid)
		err := signalProcs(api, []procRow{rowOf(api, live.Process.Pid)}, syscall.SIGKILL)
		if err == nil || !strings.Contains(err.Error(), "pidfd_open of process") || !strings.Contains(err.Error(), "invalid argument") {
			t.Errorf("error %v, want the pidfd_open EINVAL for a live process", err)
		}
		if answers.Load() != 1 {
			t.Errorf("the opener answered EINVAL %d times, want once", answers.Load())
		}
		if err := syscall.Kill(live.Process.Pid, 0); err != nil || procState(live.Process.Pid) == "Z" {
			t.Errorf("the live process: kill(0) %v, state %q; want it alive, unsignalled", err, procState(live.Process.Pid))
		}
	})

	t.Run("a process that exited and was reaped is ended", func(t *testing.T) {
		done := start("true")
		pid := done.Process.Pid
		var answers atomic.Int64
		api := einvalFor(&answers, pid)
		listed := procRow{pid: pid, ppid: 1, sid: pid, ticks: 1}
		_ = done.Wait()
		if err := signalProcs(api, []procRow{listed}, syscall.SIGKILL); err != nil {
			t.Errorf("error %v, want none for a process that is gone", err)
		}
		if answers.Load() != 1 {
			t.Errorf("the opener answered EINVAL %d times, want once", answers.Load())
		}
	})

	t.Run("a zombie is ended", func(t *testing.T) {
		z := start("true")
		pid := z.Process.Pid
		waitFor(t, "the process to be a zombie", func() bool { return procState(pid) == "Z" })
		var answers atomic.Int64
		api := einvalFor(&answers, pid)
		if err := signalProcs(api, []procRow{{pid: pid, ppid: 1, sid: pid, ticks: 1}}, syscall.SIGKILL); err != nil {
			t.Errorf("error %v, want none for a zombie", err)
		}
		if answers.Load() != 1 {
			t.Errorf("the opener answered EINVAL %d times, want once", answers.Load())
		}
	})
}

// Close, on a whole run-mode platform, when a process of the job is listed and has gone by the time it
// is pinned: the scripted listing holds a row for a PID that no process has, once, and the opener answers
// EINVAL for it, as the kernel does for a process mid-exit. The sweep re-checks, finds nothing, and ends
// cleanly: the job is finished with no cleanup error and Close succeeds. Without the re-check the sweep
// stops with "pidfd_open of process N: invalid argument". The test does not wait for the job to finish
// and then close, which is the workaround: it shows the injected EINVAL was answered, and that the sweep
// listed the row.
func TestCloseSucceedsWhenAListedProcessEndsMidExit(t *testing.T) {
	r := newRunRigCfg(t, Config{Run: &RunMode{}, NotListed: NotListedAthena}, true)
	const ghost = 1<<22 - 1 // a PID that no process has
	var leader, einval, listed atomic.Int64
	proc := r.p.knobs.proc
	host, open := proc.list, proc.open
	proc.scanned = func(l int) { leader.Store(int64(l)) }
	proc.list = func() ([]procRow, error) {
		rows, err := host()
		if listed.CompareAndSwap(0, 1) { // the process shows in the first listing only
			rows = append(rows, procRow{pid: ghost, ppid: 1, sid: int(leader.Load()), ticks: 1})
		}
		return rows, err
	}
	proc.open = func(pid int) (int, error) {
		if pid == ghost {
			einval.Add(1)
			return -1, syscall.EINVAL
		}
		return open(pid)
	}
	id := r.submit("exit 0")
	// The sweep ends one way or the other: every process gone, or an error it cannot get past.
	waitFor(t, "the sweep to end", func() bool { j := r.job(id); return j.Finished || j.CleanupErr != nil })
	j := r.job(id)
	if !j.Finished || j.CleanupErr != nil || j.State != Completed {
		t.Errorf("job %d: finished %v, state %s, cleanup error %v; want finished, COMPLETED, no error", id, j.Finished, j.State, j.CleanupErr)
	}
	if einval.Load() != 1 {
		t.Errorf("the opener answered EINVAL %d times, want once: the window was not driven", einval.Load())
	}
	if err := r.p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

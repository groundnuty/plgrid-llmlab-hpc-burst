package fakeslurm

// New. Run mode: what happens when a job's batch shell exits, by itself or
// because the fake killed it.

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// wait follows the batch shell to the end of the execution:
//
//  1. It waits for the shell to exit without reaping it, so the shell stays a
//     zombie and keeps its PID, which is the session ID the sweep selects by.
//  2. It records the exit at once, before the sweep, which can take seconds:
//     an execution still RUNNING ended by itself, COMPLETED for exit 0 and
//     FAILED otherwise, and a scancel that arrives now finds a finished job
//     instead of turning it into CANCELLED. One the fake ended (scancel, time
//     limit, a control) keeps the state it was given.
//  3. It kills whatever is left in the session, as Slurm's cgroup does when a
//     job's batch script ends, and removes the execution's directory.
//  4. It reaps the shell, under the lock that keeps every scan of /proc out
//     while it does, and marks the execution done, unless the sweep failed:
//     processes that survive KILL, or a /proc that cannot be read, leave it
//     not done (Finished stays false) with the error in the Job and in Close.
func (p *Platform) wait(r *jobRun) {
	defer close(r.settled)
	if err := waitExit(r.pid); err != nil {
		p.waitFailed(r, err)
		return
	}
	status, err := p.knobs.proc.exitStatus(r.pid)
	if err != nil {
		p.waitFailed(r, err)
		return
	}
	p.mu.Lock()
	p.recordExit(r, status)
	p.mu.Unlock()
	close(r.exited)

	sweepErr := sweepSession(p.knobs.proc, r.pid, p.knobs.sweepLimit, p.knobs.sweepInterval)
	rmErr := os.RemoveAll(r.dir)
	p.mu.Lock()
	r.sweepErr = sweepErr // before the reap: Processes tells a failed sweep from one in progress
	p.mu.Unlock()
	p.reap(r)

	p.mu.Lock()
	defer p.mu.Unlock()
	r.cleanupErr = errors.Join(r.cleanupErr, sweepErr, rmErr)
	r.done = sweepErr == nil
	p.schedule(p.clock.Now())
}

// waitFailed ends an execution whose shell could not be waited for or whose
// status could not be read. Nothing is known about its processes, and the
// shell may not be a zombie, so its PID is no anchor: every scan of the
// execution is shut out first, then the shell is killed through its pidfd (a
// shell that is still alive would otherwise be waited for under the lock that
// Close's KILL needs) and reaped. The execution is not swept and not done, and
// the error is kept. This path is not tested: waitid on a child does not
// fail.
func (p *Platform) waitFailed(r *jobRun, cause error) {
	r.procMu.Lock()
	r.leaderReaped = true
	r.procMu.Unlock()
	_ = r.cmd.Process.Kill() // no-op if it has exited
	_ = r.cmd.Wait()
	status := syscall.WaitStatus(0)
	if ps := r.cmd.ProcessState; ps != nil {
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok {
			status = ws
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recordExit(r, status)
	r.sweepErr = cause
	r.cleanupErr = errors.Join(r.cleanupErr, cause)
	close(r.exited)
	p.schedule(p.clock.Now())
}

// reap reaps the batch shell. Every scan of the session has finished before
// it returns and none starts afterwards (jobRun.scanLive).
func (p *Platform) reap(r *jobRun) {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	_ = r.cmd.Wait() // the status was recorded from the zombie; Wait only reaps
	r.leaderReaped = true
}

// recordExit stores the batch shell's exit and ends the job if it is still
// running. The caller holds mu.
func (p *Platform) recordExit(r *jobRun, status syscall.WaitStatus) {
	r.exitCode, r.exitSignal = -1, 0
	if status.Signaled() {
		r.exitSignal = status.Signal()
	} else {
		r.exitCode = status.ExitStatus()
	}
	r.exitSeen = true // stop() has nothing left to do: the sweep kills the rest
	if r.limit != nil {
		r.limit.Stop()
	}
	if r.e.state == Running {
		state := Completed
		if r.exitCode != 0 {
			state = Failed
		}
		_ = p.endJob(r.j, p.clock.Now(), state, 0) // RUNNING, so it can end
	}
}

// waitExit blocks until the process has exited, without reaping it: waitid
// with WNOWAIT leaves the zombie, and with it the PID, in place.
func waitExit(pid int) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

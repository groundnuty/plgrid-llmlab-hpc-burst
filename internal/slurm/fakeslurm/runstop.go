package fakeslurm

// New. Run mode: how a job's processes end, by themselves, by scancel or the
// time limit, by a control, or at Close.

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// wait reaps the batch shell, kills whatever is left of the job, as Slurm
// does when a job's batch script ends, and then records the end. An execution
// that is still RUNNING ended by itself: COMPLETED for exit 0, FAILED
// otherwise. One the fake ended (scancel, time limit, a control) keeps the
// state it was given.
func (p *Platform) wait(r *jobRun) {
	_ = r.cmd.Wait() // the exit status is read from ProcessState below
	close(r.reaped)
	code, sig := exitStatus(r.cmd.ProcessState)
	sweepErr := sweep(r.pid)
	rmErr := os.RemoveAll(r.dir)
	p.mu.Lock()
	r.exitCode, r.exitSignal, r.done = code, sig, true
	r.sweepErr = errors.Join(sweepErr, rmErr)
	if r.limit != nil {
		r.limit.Stop()
	}
	now := p.clock.Now()
	if r.e.state == Running {
		state := Completed
		if code != 0 {
			state = Failed
		}
		_ = p.endJob(r.j, now, state, 0) // RUNNING, so it can end
	}
	p.schedule(now)
	p.mu.Unlock()
	close(r.finished)
}

// exitStatus is the batch shell's exit code, or -1 and the signal that ended
// it.
func exitStatus(ps *os.ProcessState) (int, syscall.Signal) {
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return -1, ws.Signal()
	}
	return ps.ExitCode(), 0
}

// sweep kills every process left in the job's session until none is left.
// One that survives a KILL for 30 s (stuck in the kernel) is an error.
func sweep(sid int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		rows := sessionProcs(sid)
		if len(rows) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d processes of the job survive SIGKILL", len(rows))
		}
		signalProcs(rows, syscall.SIGKILL)
		time.Sleep(10 * time.Millisecond)
	}
}

// stop ends the execution's processes: with a grace, CONT and TERM to all of
// them, then KILL to what is left after the grace (Slurm's KillWait); with
// none, KILL at once. A second stop with no grace turns a TERM phase into the
// KILL at once. The caller holds mu; the signals are sent outside it.
func (p *Platform) stop(r *jobRun, grace time.Duration) {
	if r == nil || r.done || r.killing || (r.stopping && grace > 0) {
		return
	}
	r.stopping = true
	if grace <= 0 {
		r.killing = true
	}
	go p.terminate(r, grace)
}

func (p *Platform) terminate(r *jobRun, grace time.Duration) {
	if grace > 0 {
		t := time.Now()
		p.mu.Lock()
		r.termAt = t
		p.mu.Unlock()
		rows := sessionProcs(r.pid)
		signalProcs(rows, syscall.SIGCONT)
		signalProcs(rows, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		select {
		case <-r.reaped: // the waiter kills what is left
			timer.Stop()
			return
		case <-timer.C:
		}
		p.mu.Lock()
		if r.killing || r.done {
			p.mu.Unlock()
			return
		}
		r.killing = true
		p.mu.Unlock()
	}
	t := time.Now()
	p.mu.Lock()
	r.killAt = t
	p.mu.Unlock()
	signalProcs(sessionProcs(r.pid), syscall.SIGKILL)
}

// timeLimitHit ends a job that is still running at its time limit TIMEOUT,
// then TERM and KILL after KillWait, as Slurm does.
func (p *Platform) timeLimitHit(r *jobRun) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.e.state == Running && !r.done {
		_ = p.endJob(r.j, p.clock.Now(), Timeout, p.run.KillWait) // RUNNING, so it can end
	}
}

// rearmLimit sets a running execution's time limit timer for a new limit,
// counted from its start; a limit already passed fires at once. The caller
// holds mu.
func (p *Platform) rearmLimit(r *jobRun, minutes int) {
	if r == nil || r.done || r.e.state != Running {
		return
	}
	if r.limit != nil {
		r.limit.Stop()
		r.limit = nil
	}
	if minutes <= 0 {
		return
	}
	left := time.Until(r.started.Add(time.Duration(minutes) * p.run.Minute))
	r.limit = time.AfterFunc(left, func() { p.timeLimitHit(r) })
}

// Processes lists the PIDs of the job's current execution that are still
// alive: its session and their descendants. Empty outside run mode, before
// the job starts and once its processes are gone.
func (p *Platform) Processes(id int) []int {
	p.mu.Lock()
	j := p.find(id)
	var pid int
	if j != nil && j.cur().run != nil && j.cur().run.cmd != nil {
		pid = j.cur().run.pid
	}
	p.mu.Unlock()
	if pid == 0 {
		return nil
	}
	var out []int
	for _, r := range sessionProcs(pid) {
		out = append(out, r.pid)
	}
	sort.Ints(out)
	return out
}

// closeTimeout bounds Close's wait for the jobs' processes.
const closeTimeout = 60 * time.Second

// Close ends run mode: no job starts any more, every job's processes get KILL,
// and Close waits until they are gone. It returns an error naming the jobs
// whose processes outlived the wait or the sweep. Job states are left as they
// are. Outside run mode Close does nothing. A test that uses run mode closes
// the Platform in its cleanup; a process that exits without Close leaves its
// jobs running, as a Slurm job outlives the login session.
func (p *Platform) Close() error {
	p.mu.Lock()
	p.closed = true
	var runs []*jobRun
	for _, j := range p.jobs {
		for _, e := range j.execs {
			if r := e.run; r != nil {
				runs = append(runs, r)
				p.stop(r, 0)
			}
		}
	}
	p.mu.Unlock()
	var errs []error
	timeout := time.After(closeTimeout)
	for i, r := range runs {
		select {
		case <-r.finished:
		case <-timeout:
			var left []string
			for _, r := range runs[i:] {
				select {
				case <-r.finished:
				default:
					left = append(left, strconv.Itoa(r.j.id))
				}
			}
			return errors.Join(append(errs, fmt.Errorf("fakeslurm: Close: the processes of jobs %s still run after %s", strings.Join(left, ","), closeTimeout))...)
		}
		p.mu.Lock()
		if r.sweepErr != nil {
			errs = append(errs, fmt.Errorf("fakeslurm: job %d: %w", r.j.id, r.sweepErr))
		}
		p.mu.Unlock()
	}
	return errors.Join(errs...)
}

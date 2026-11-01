package fakeslurm

// New. Run mode: how the fake ends a job's processes (scancel, the time limit,
// a control, Close) and how it lists them.

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// stop ends the execution's processes: with a grace, CONT and TERM to all of
// them, then KILL to what is left after the grace (Slurm's KillWait); with
// none, KILL at once. A second stop with no grace turns a TERM phase into the
// KILL at once. Once the batch shell has exited there is nothing to start:
// the waiter kills what is left. The caller holds mu; the signals are sent
// outside it.
func (p *Platform) stop(r *jobRun, grace time.Duration) {
	if r == nil || r.done || r.exitSeen || r.killing || (r.stopping && grace > 0) {
		return
	}
	r.stopping = true
	if grace <= 0 {
		r.killing = true
	}
	go p.terminate(r, grace)
}

// terminate is the TERM phase and then the KILL phase of stop. Every scan of
// the session goes through jobRun.scanLive, so none runs once the batch shell
// is reaped, however late this goroutine gets to it.
func (p *Platform) terminate(r *jobRun, grace time.Duration) {
	if h := p.knobs.terminateEnter; h != nil {
		h(r)
	}
	if h := p.knobs.terminateDone; h != nil {
		defer h(r)
	}
	if grace > 0 {
		if !r.scanLive(func() { p.signalSession(r, true, syscall.SIGCONT, syscall.SIGTERM) }) {
			return
		}
		timer := time.NewTimer(grace)
		select {
		case <-r.exited: // the waiter kills what is left
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
	r.scanLive(func() { p.signalSession(r, false, syscall.SIGKILL) })
}

// signalSession lists the session and sends it the signals, and records when
// the TERM (term) or the KILL went, in the job's snapshot. The caller is in
// scanLive. A /proc that cannot be read or a signal that cannot be sent is
// kept for the job's cleanup error.
func (p *Platform) signalSession(r *jobRun, term bool, sigs ...syscall.Signal) {
	t := time.Now()
	p.mu.Lock()
	if term {
		r.termAt = t
	} else {
		r.killAt = t
	}
	p.mu.Unlock()
	rows, err := sessionRows(p.knobs.proc, r.pid)
	for _, sig := range sigs {
		if err == nil {
			err = signalProcs(p.knobs.proc, rows, sig)
		}
	}
	if err != nil {
		p.mu.Lock()
		r.cleanupErr = errors.Join(r.cleanupErr, err)
		p.mu.Unlock()
	}
}

// timeLimitHit is the callback of a time limit timer. A timer that has already
// started cannot be stopped, so the callback decides for itself, under mu,
// whether the limit has passed: it recomputes the deadline from the start and
// the job's limit as it is now, and does nothing if the limit was raised or
// removed while the callback waited for the lock.
func (p *Platform) timeLimitHit(r *jobRun) {
	if h := p.knobs.limitFired; h != nil {
		h(r)
	}
	p.mu.Lock()
	ended := p.expireLimit(r)
	p.mu.Unlock()
	if h := p.knobs.limitDecided; h != nil {
		h(r, ended)
	}
}

// expireLimit ends a job still running past its time limit TIMEOUT, then TERM
// and KILL after KillWait, as Slurm does, and reports whether it did. The
// caller holds mu.
func (p *Platform) expireLimit(r *jobRun) bool {
	if r.done || r.exitSeen || r.e.state != Running || r.j.timeLimit <= 0 {
		return false
	}
	deadline := r.started.Add(time.Duration(r.j.timeLimit) * p.run.Minute)
	if time.Now().Before(deadline) {
		return false
	}
	return p.endJob(r.j, p.clock.Now(), Timeout, p.run.KillWait) == nil // RUNNING, so it can end
}

// rearmLimit sets a running execution's time limit timer for a new limit,
// counted from its start. A limit already passed ends the job TIMEOUT before
// it returns. The caller holds mu.
func (p *Platform) rearmLimit(r *jobRun, minutes int) {
	if r == nil || r.done || r.exitSeen || r.e.state != Running {
		return
	}
	if r.limit != nil {
		r.limit.Stop() // a callback already waiting for mu checks the limit itself
		r.limit = nil
	}
	if minutes <= 0 {
		return
	}
	left := time.Until(r.started.Add(time.Duration(minutes) * p.run.Minute))
	if left <= 0 {
		p.expireLimit(r)
		return
	}
	r.limit = time.AfterFunc(left, func() { p.timeLimitHit(r) })
}

// Processes lists the PIDs of the job's current execution that are still
// alive: its session and their descendants. It is empty outside run mode,
// before the job starts and once the execution's shell is reaped, and it is an
// error when /proc cannot be read: an unreadable table does not mean no
// process.
func (p *Platform) Processes(id int) ([]int, error) {
	p.mu.Lock()
	var r *jobRun
	if j := p.find(id); j != nil {
		r = j.cur().run
	}
	started := r != nil && r.cmd != nil
	p.mu.Unlock()
	if !started {
		return nil, nil
	}
	var out []int
	var err error
	ran := r.scanLive(func() {
		var rows []procRow
		if rows, err = sessionRows(p.knobs.proc, r.pid); err == nil {
			for _, row := range rows {
				out = append(out, row.pid)
			}
		}
	})
	if !ran {
		// The shell is reaped and nothing is tracked any more: none is left,
		// unless the sweep could not tell.
		p.mu.Lock()
		err = r.sweepErr
		p.mu.Unlock()
		if err != nil {
			err = fmt.Errorf("fakeslurm: job %d: its processes are not known to be gone: %w", id, err)
		}
	}
	sort.Ints(out)
	return out, err
}

// closeTimeout bounds Close's wait for the jobs' processes.
const closeTimeout = 60 * time.Second

// Close ends run mode: no job starts any more, every job's processes get KILL,
// and Close waits until they are gone. A job that is still RUNNING when Close
// kills it ends FAILED (killed by a signal), as it does when anything else
// kills its shell; a job that already ended keeps its state. It returns an
// error naming the jobs whose processes outlived the wait or the sweep, or
// whose processes could not be listed. Outside run mode Close does nothing. A
// test that uses run mode closes the Platform in its cleanup; a process that
// exits without Close leaves its jobs running, as a Slurm job outlives the
// login session.
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
		case <-r.settled:
		case <-timeout:
			var left []string
			for _, r := range runs[i:] {
				select {
				case <-r.settled:
				default:
					left = append(left, strconv.Itoa(r.j.id))
				}
			}
			return errors.Join(append(errs, fmt.Errorf("fakeslurm: Close: the processes of jobs %s still run after %s", strings.Join(left, ","), closeTimeout))...)
		}
		p.mu.Lock()
		if r.cleanupErr != nil {
			errs = append(errs, fmt.Errorf("fakeslurm: job %d: %w", r.j.id, r.cleanupErr))
		}
		p.mu.Unlock()
	}
	return errors.Join(errs...)
}

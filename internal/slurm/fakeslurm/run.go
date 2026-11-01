package fakeslurm

// New. Run mode (doc.go, Run mode): each job's --wrap runs as a local process
// with a job environment, and the job's state follows the process.

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// RunMode puts a Platform in run mode. Every path is absolute.
type RunMode struct {
	// Root is a real, existing directory. The file system is the directory
	// tree under it, at the same paths: a login's HOME and SCRATCH must be
	// under it, so the files the plugin stages are where the job reads them.
	Root string
	// JobDir holds one directory per execution, JobDir/<job ID> for the
	// first and JobDir/<job ID>.<n> for the n-th after requeues: the batch
	// script, TMPDIR (tmp) and one directory per job variable (its name in
	// lower case). It is removed when the execution's processes are gone, as
	// Helios frees $MEMFS when the job ends. Default Root. Keep it short: an
	// engine's unix socket in TMPDIR must fit in 108 bytes.
	JobDir string
	// Env is added to every job's environment, after the login's own.
	Env map[string]string
	// JobVars are the job variables the cluster sets inside jobs (HBEP-4,
	// Submission: tmpDir), such as Helios's MEMFS for a job that requests -C
	// memfs.
	JobVars []JobVar
	// KillWait is the time between the TERM of a scancel or a time limit and
	// the KILL of whatever is left; default 30 s, Slurm's KillWait default.
	KillWait time.Duration
	// Minute is the wall time of one minute of --time: the time limit's
	// timer and a running job's projected end_time in squeue use it. Default
	// one minute. sacct's time limit and every reply's time_limit stay in
	// minutes, so a Provider that derives an ended job's hard end from
	// time_limit gets the unscaled one; keep the default for a full stack.
	Minute time.Duration
}

// JobVar is one job variable: set inside a job to a fresh directory of the
// execution, and unset on the login node.
type JobVar struct {
	// Name is the variable, for example MEMFS.
	Name string
	// Feature, when set, is the node feature a job must request with
	// --constraint for the variable to be set; "" sets it in every job.
	Feature string
}

// jobShell is the shell of the batch script: sbatch --wrap writes the command
// into a "#!/bin/sh" script.
const jobShell = "/bin/sh"

// defaultPath is the job's PATH when neither the login nor Env sets one.
const defaultPath = "/usr/local/bin:/usr/bin:/bin"

var varName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// normalizeRun checks a RunMode and fills in its defaults. It panics on one it
// cannot use, as New does: that is a test bug.
func normalizeRun(r RunMode) *RunMode {
	if !path.IsAbs(r.Root) || path.Clean(r.Root) != r.Root || r.Root == "/" {
		panic(fmt.Sprintf("fakeslurm: RunMode.Root %q is not an absolute, cleaned directory below /", r.Root))
	}
	if fi, err := os.Stat(r.Root); err != nil || !fi.IsDir() {
		panic(fmt.Sprintf("fakeslurm: RunMode.Root %q is not an existing directory", r.Root))
	}
	if r.JobDir == "" {
		r.JobDir = r.Root
	}
	if !path.IsAbs(r.JobDir) || path.Clean(r.JobDir) != r.JobDir {
		panic(fmt.Sprintf("fakeslurm: RunMode.JobDir %q is not an absolute, cleaned path", r.JobDir))
	}
	// A job variable's directory is its name in lower case, beside tmp and
	// slurm_script, so those two names and duplicates are refused.
	dirs := map[string]bool{"tmp": true, "slurm_script": true}
	for _, v := range r.JobVars {
		d := strings.ToLower(v.Name)
		if !varName.MatchString(v.Name) || v.Name == "TMPDIR" || v.Name == "PATH" || strings.HasPrefix(v.Name, "SLURM_") || dirs[d] {
			panic(fmt.Sprintf("fakeslurm: JobVar %q is not a variable name the fake may set, or its directory %q is taken", v.Name, d))
		}
		dirs[d] = true
	}
	if r.KillWait <= 0 {
		r.KillWait = 30 * time.Second
	}
	if r.Minute <= 0 {
		r.Minute = time.Minute
	}
	env := make(map[string]string, len(r.Env))
	for k, v := range r.Env {
		env[k] = v
	}
	r.Env = env
	r.JobVars = append([]JobVar(nil), r.JobVars...)
	return &r
}

// runKnobs are the parts of run mode a test replaces: the kernel interface,
// the sweep's timings and hooks at the points where goroutines meet. New sets
// the real ones; a test changes a field before it submits a job.
type runKnobs struct {
	proc *procAPI
	// sweepLimit is how long processes may survive KILL before the sweep
	// gives up (Slurm's UnkillableStepTimeout is a minute; the fake's is
	// 30 s), sweepInterval the pause between its listings.
	sweepLimit, sweepInterval time.Duration
	// Hooks, called with no lock held: terminateEnter when a terminator
	// goroutine starts and terminateDone when it ends, limitFired when a time
	// limit timer's callback starts and limitDecided when it has decided
	// whether the job ended.
	terminateEnter, terminateDone func(r *jobRun)
	limitFired                    func(r *jobRun)
	limitDecided                  func(r *jobRun, ended bool)
}

func hostKnobs() runKnobs {
	return runKnobs{proc: hostProc(), sweepLimit: 30 * time.Second, sweepInterval: 10 * time.Millisecond}
}

// jobRun is one execution's process. Platform.mu guards its fields, except
// those marked otherwise; the channels need no lock.
//
// Its lifetime is: started, then the batch shell exits (exitSeen: the exit is
// recorded and the job ends COMPLETED or FAILED, unless something else ended
// it first), then the sweep clears what the shell left, then the shell is
// reaped (leaderReaped) and the directory removed, then done. The shell stays
// a zombie until the sweep has finished, because a zombie keeps its PID, which
// is the session ID the sweep selects by.
type jobRun struct {
	j       *job
	e       *execution
	cmd     *exec.Cmd
	pid     int    // the batch shell: its PID, process group and session
	dir     string // the execution's directory under JobDir
	started time.Time
	limit   *time.Timer
	// stopping is set when a TERM or KILL phase has begun; killing when the
	// KILL has.
	stopping, killing bool
	termAt, killAt    time.Time
	exitCode          int
	exitSignal        syscall.Signal
	// exitSeen is set when the batch shell has exited and its status is
	// recorded; done when, after that, no process of the execution is left
	// and the shell is reaped. A sweep that fails leaves done false.
	exitSeen, done bool
	// startErr is why the execution could not start; cleanupErr what went
	// wrong clearing its processes or its directory.
	startErr, cleanupErr error
	// sweepErr is why the processes of the execution are not known to be
	// gone (the sweep failed); it is set before the shell is reaped.
	sweepErr error
	exited   chan struct{} // closed when the batch shell has exited
	settled  chan struct{} // closed when the waiter has finished, done or not

	// procMu serializes every scan of /proc for this execution with the reap
	// of the batch shell, and guards leaderReaped. It is taken before
	// Platform.mu where both are needed, never after.
	procMu       sync.Mutex
	leaderReaped bool
}

// scanLive runs fn, which scans /proc for the execution's session and may
// signal what it finds, only while the batch shell has not been reaped, and
// reports whether it ran. The reap takes the same lock, so no scan is in
// flight when the shell is reaped and none starts after: past that point the
// session ID is a free number that another process may hold.
func (r *jobRun) scanLive(fn func()) bool {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	if r.leaderReaped {
		return false
	}
	fn()
	return true
}

// minute is the wall time of one minute of --time.
func (p *Platform) minute() time.Duration {
	if p.run != nil {
		return p.run.Minute
	}
	return time.Minute
}

// killWait is the time between TERM and KILL.
func (p *Platform) killWait() time.Duration {
	if p.run != nil {
		return p.run.KillWait
	}
	return 0
}

// schedule starts every pending job that may run, by job ID, in run mode: not
// held, a free slot in its partition, and no earlier execution of it still
// dying. A job that becomes eligible starts within the command that made it
// so (sbatch, scontrol release, FreePhantoms, or the end of an execution).
// The caller holds mu.
func (p *Platform) schedule(now time.Time) {
	if p.run == nil || p.closed {
		return
	}
	for _, j := range p.jobs {
		if j.state() != Pending || j.held || p.dying(j) {
			continue
		}
		if n := p.slotsOf(j.partition); n > 0 && p.running(j.partition)+p.phantoms[j.partition] >= n {
			continue
		}
		if err := j.markStart(now); err != nil {
			continue
		}
		p.launch(j, now)
	}
}

// dying reports whether an earlier execution of j still has processes.
func (p *Platform) dying(j *job) bool {
	for _, e := range j.execs[:len(j.execs)-1] {
		if e.run != nil && !e.run.done {
			return true
		}
	}
	return false
}

// launch starts the batch script of j's running execution. A job that cannot
// start (its directory or an output file cannot be made) ends FAILED at once,
// as a job whose output file Slurm cannot open does. The caller holds mu.
func (p *Platform) launch(j *job, now time.Time) {
	e := j.cur()
	r := &jobRun{j: j, e: e, exited: make(chan struct{}), settled: make(chan struct{})}
	e.run = r
	if err := p.startProcess(r); err != nil {
		r.done, r.exitSeen, r.startErr = true, true, err
		close(r.exited)
		close(r.settled)
		_ = p.endJob(j, now, Failed, 0)
		return
	}
	go p.wait(r)
	if j.timeLimit > 0 {
		r.limit = time.AfterFunc(time.Duration(j.timeLimit)*p.run.Minute, func() { p.timeLimitHit(r) })
	}
}

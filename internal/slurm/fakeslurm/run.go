package fakeslurm

// New. Run mode (doc.go, Run mode): each job's --wrap runs as a local process
// with a job environment, and the job's state follows the process.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// jobRun is one execution's process. Platform.mu guards its fields; the
// channels need no lock.
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
	done              bool
	// startErr is why the execution could not start; sweepErr what went
	// wrong clearing its processes or its directory.
	startErr, sweepErr error
	reaped             chan struct{} // closed when the batch shell is reaped
	finished           chan struct{} // closed when every process is gone
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
	r := &jobRun{j: j, e: e, reaped: make(chan struct{}), finished: make(chan struct{})}
	e.run = r
	if err := p.startProcess(r); err != nil {
		r.done, r.startErr = true, err
		close(r.reaped)
		close(r.finished)
		_ = p.endJob(j, now, Failed, 0)
		return
	}
	go p.wait(r)
	if j.timeLimit > 0 {
		r.limit = time.AfterFunc(time.Duration(j.timeLimit)*p.run.Minute, func() { p.timeLimitHit(r) })
	}
}

// startProcess makes the execution's directory, writes the batch script and
// starts it under /bin/sh in a session of its own, so it leads its own
// process group too. On a failure it removes the directory it made.
func (p *Platform) startProcess(r *jobRun) error {
	j := r.j
	name := strconv.Itoa(j.id)
	if n := len(j.execs); n > 1 {
		name += "." + strconv.Itoa(n)
	}
	dir := filepath.Join(p.run.JobDir, name)
	if err := os.MkdirAll(p.run.JobDir, 0o700); err != nil {
		return err
	}
	// Mkdir, not MkdirAll: a directory left by another Platform on the same
	// JobDir fails the job rather than mixing two jobs' files.
	if err := os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	if err := p.startIn(r, dir); err != nil {
		return errors.Join(err, os.RemoveAll(dir))
	}
	r.dir = dir
	return nil
}

// startIn fills the execution's directory and starts the batch script.
func (p *Platform) startIn(r *jobRun, dir string) error {
	j := r.j
	tmp := filepath.Join(dir, "tmp")
	vars := map[string]string{"TMPDIR": tmp}
	for _, v := range p.run.JobVars {
		if v.Feature == "" || hasFeature(j.constraint, v.Feature) {
			vars[v.Name] = filepath.Join(dir, strings.ToLower(v.Name))
		}
	}
	for _, d := range vars {
		if err := os.Mkdir(d, 0o700); err != nil {
			return err
		}
	}
	script := filepath.Join(dir, "slurm_script")
	if err := os.WriteFile(script, []byte("#!"+jobShell+"\n"+j.wrap+"\n"), 0o700); err != nil {
		return err
	}
	submitDir := j.env["HOME"]
	if !p.fs.isDir(submitDir) {
		submitDir = p.run.Root
	}
	out, errOut, err := p.openOutputs(j, submitDir)
	if err != nil {
		return err
	}
	defer out.Close()
	if errOut != out {
		defer errOut.Close()
	}
	cmd := exec.Command(jobShell, script)
	cmd.Dir = submitDir
	cmd.Env = p.jobEnv(j, submitDir, vars)
	cmd.Stdout, cmd.Stderr = out, errOut // files, so no copy goroutine waits on a grandchild
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	r.cmd, r.pid, r.started = cmd, cmd.Process.Pid, time.Now()
	return nil
}

// hasFeature reports whether a --constraint value asks for feature: the value
// itself, or one of an AND list joined by & or ,.
func hasFeature(constraint, feature string) bool {
	for _, f := range strings.FieldsFunc(constraint, func(c rune) bool { return c == '&' || c == ',' }) {
		if f == feature {
			return true
		}
	}
	return false
}

// openOutputs opens the job's --output and --error files, relative to the
// submit directory, by default slurm-<job ID>.out for both, truncated for the
// first execution and appended to after a requeue. Both must be under Root.
func (p *Platform) openOutputs(j *job, submitDir string) (out, errOut *os.File, err error) {
	resolve := func(name string) string {
		if name == "" {
			name = "slurm-" + strconv.Itoa(j.id) + ".out"
		}
		if !path.IsAbs(name) {
			name = submitDir + "/" + name
		}
		return path.Clean(name)
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if len(j.execs) > 1 {
		flag = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	}
	open := func(name string) (*os.File, error) {
		if !p.fs.(*diskFS).in(name) {
			return nil, fmt.Errorf("output file %s is outside the run-mode root", name)
		}
		return os.OpenFile(name, flag, 0o644)
	}
	outName, errName := resolve(j.output), resolve(j.output)
	if j.errPath != "" {
		errName = resolve(j.errPath)
	}
	if out, err = open(outName); err != nil {
		return nil, nil, err
	}
	if errName == outName {
		return out, out, nil // one file for both, as Slurm opens it once
	}
	if errOut, err = open(errName); err != nil {
		out.Close()
		return nil, nil, err
	}
	return out, errOut, nil
}

// jobEnv is the job's environment: PATH, the login's environment (sbatch
// exports it by default), RunMode.Env, then what Slurm and the cluster set.
func (p *Platform) jobEnv(j *job, submitDir string, vars map[string]string) []string {
	env := map[string]string{"PATH": defaultPath}
	for k, v := range j.env {
		env[k] = v
	}
	for k, v := range p.run.Env {
		env[k] = v
	}
	id := strconv.Itoa(j.id)
	slurmVars := map[string]string{
		"SLURM_JOB_ID": id, "SLURM_JOBID": id, "SLURM_JOB_NAME": j.name,
		"SLURM_JOB_USER": j.user, "SLURM_JOB_PARTITION": j.partition, "SLURM_JOB_ACCOUNT": j.account,
		"SLURM_SUBMIT_DIR": submitDir, "SLURM_CPUS_PER_TASK": strconv.Itoa(j.cpus),
		"SLURM_JOB_NUM_NODES": strconv.Itoa(j.nodes), "SLURM_NNODES": strconv.Itoa(j.nodes),
		"SLURM_MEM_PER_NODE": strconv.FormatInt(j.memMiB, 10),
	}
	if j.restartCnt > 0 {
		slurmVars["SLURM_RESTART_COUNT"] = strconv.Itoa(j.restartCnt)
	}
	for k, v := range slurmVars {
		env[k] = v
	}
	for k, v := range vars {
		env[k] = v
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k + "=" + env[k]
	}
	return out
}

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

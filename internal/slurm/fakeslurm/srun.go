package fakeslurm

// New. "srun [-u] --overlap --jobid=<id> [--ntasks=1] [--time=<t>] <program>
// [args...]": a step in the session user's own running job, run as a host
// process (run mode only). The step's process lives in srunstep.go; Stream,
// the streaming form of the same command, in srunstream.go.

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// srunArgs is a parsed srun command line.
type srunArgs struct {
	jobID   int
	minutes int  // --time in minutes, rounded up; 0 for none
	unbuf   bool // -u: standard output is not held back until a newline (Stream)
	program string
	args    []string
}

// parseSrun reads the options before the program and takes the rest as the
// program's own words. It returns the message of the first problem, or "".
// Only -u, --overlap, --jobid=<digits>, --ntasks=1 and --time=<t> are known,
// in any order; any other option is refused, so one the plugin should never
// send cannot pass silently. The program's own options (bash -s) follow its name
// and are not read.
func parseSrun(args []string) (srunArgs, string) {
	var a srunArgs
	overlap, haveID := false, false
	i := 0
	for ; i < len(args) && strings.HasPrefix(args[i], "-"); i++ {
		name, val, hasVal := strings.Cut(args[i], "=")
		switch {
		case name == "-u" && !hasVal:
			a.unbuf = true
		case name == "--overlap" && !hasVal:
			overlap = true
		case name == "--jobid" && hasVal:
			id, err := strconv.Atoi(val)
			if err != nil || !allDigits(val) {
				return a, fmt.Sprintf("invalid job id %q: --jobid takes the digits of a job ID", val)
			}
			a.jobID, haveID = id, true
		case name == "--ntasks" && hasVal:
			if val != "1" {
				return a, fmt.Sprintf("--ntasks=%s: the fake runs one task", val)
			}
		case name == "--time" && hasVal:
			m, err := parseTimeLimit(val)
			if err != nil {
				return a, "invalid --time: " + err.Error()
			}
			a.minutes = m
		default:
			return a, fmt.Sprintf("the fake takes only -u, --overlap, --jobid=<id>, --ntasks=1 and --time=<t> before the program, not %q", args[i])
		}
	}
	switch {
	case !overlap:
		return a, "--overlap is required: the fake runs a step beside the job's own processes"
	case !haveID:
		return a, "--jobid=<id> is required: the fake runs a step only in an existing job"
	case i >= len(args):
		return a, "no program to run"
	}
	a.program, a.args = args[i], args[i+1:]
	return a, ""
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// stepPlan is a checked srun line: the options, the job's execution and the
// program's path.
type stepPlan struct {
	a    srunArgs
	r    *jobRun
	prog string
}

// planStep reads an srun line's words and runs srun's checks in order: the
// options, run mode, the job and the program. On a refusal it has written the
// message to standard error and returns its exit code with ok false, and
// nothing has started. The caller holds mu.
func (sh *shell) planStep(args []string) (pl stepPlan, code int, ok bool) {
	a, msg := parseSrun(args)
	if msg != "" {
		return pl, sh.fail(1, "srun: error: %s", msg), false
	}
	p := sh.p
	if p.run == nil {
		return pl, sh.fail(1, "srun: error: the fake runs a step only in run mode (Config.Run): outside it there is no process to run in"), false
	}
	r, msg := sh.stepJob(a.jobID)
	if msg != "" {
		return pl, sh.fail(1, "srun: error: Unable to confirm allocation for job %d: %s", a.jobID, msg), false
	}
	if j := r.j; j.stepLimit > 0 && j.steps >= j.stepLimit {
		// Slurm's ESLURM_STEP_LIMIT (slurm_errno.c, 24.11: "Step limit
		// reached for this job"); the line around it is the fake's own.
		return pl, sh.fail(1, "srun: error: Unable to create step for job %d: Step limit reached for this job", a.jobID), false
	}
	r.j.steps++
	prog, err := lookPath(a.program, envValue(r.env, "PATH"), r.submitDir)
	if err != nil {
		code := 127 // the shell's codes: not found, found but not executable
		if errors.Is(err, fs.ErrPermission) {
			code = 126
		}
		return pl, sh.fail(code, "srun: error: execve(): %s: %s", a.program, errnoText(err)), false
	}
	return stepPlan{a: a, r: r, prog: prog}, 0, true
}

// spec is the step's start, read from the job under mu.
func (pl stepPlan) spec(p *Platform) stepSpec {
	return stepSpec{
		path: pl.prog, argv0: pl.a.program, args: pl.a.args, dir: pl.r.submitDir, env: stepEnv(pl.r.env, p.run.StepEnv),
		proc: p.knobs.proc, sweepLimit: p.knobs.sweepLimit, sweepInterval: p.knobs.sweepInterval,
	}
}

// srun runs a step in the user's own running job. It needs run mode, and
// refuses every job that is not the calling user's or not RUNNING with a live
// process, running nothing. The step is a host process of its own (its own
// session, apart from the job's), so it is not among the job's Processes and
// changes nothing of the job's state or accounting; what the program does to
// the job's processes is its own doing. mu is let go of while it runs.
func (sh *shell) srun(args []string) int {
	pl, code, ok := sh.planStep(args)
	if !ok {
		return code
	}
	p := sh.p
	timeout := sh.timeout
	if timeout <= 0 {
		timeout = provider.CommandTimeout // as the transport's default
	}
	spec := pl.spec(p)
	spec.stdin = sh.stdin
	st, err := startStep(spec)
	if err != nil {
		return sh.fail(1, "srun: error: the step could not start: %v", err)
	}
	var out stepOutcome
	func() {
		p.mu.Unlock() // the step may run for the whole timeout; nothing else waits for it
		defer p.mu.Lock()
		out = st.finish(sh.ctx, timeout, time.Duration(pl.a.minutes)*p.minute(), pl.r.exited)
	}()
	sh.now = p.clock.Now()
	return sh.stepResult(out, timeout)
}

// stepResult turns the outcome of a step into the line's reply: the program's
// own output and exit status, or the reason srun ended it.
func (sh *shell) stepResult(out stepOutcome, timeout time.Duration) int {
	if out.err != nil {
		sh.runErr = fmt.Errorf("%w: fakeslurm: srun: the step's processes are not known to be gone: %v", slurm.ErrTransport, out.err)
		return 1
	}
	switch out.end {
	case stepCtxEnded:
		if sh.ctx.Err() != nil {
			sh.runErr = ctxErr(sh.ctx)
		} else {
			sh.runErr = fmt.Errorf("%w after %s", slurm.ErrTimeout, timeout)
		}
		return 1
	}
	sh.stdout.Write(out.stdout)
	sh.stderr.Write(out.stderr)
	switch out.end {
	case stepLimited:
		return sh.fail(1, "srun: error: *** STEP CANCELLED DUE TO TIME LIMIT ***")
	case stepJobEnded:
		return sh.fail(1, "srun: error: Job step aborted: the job's batch shell has exited")
	case stepOverOutput:
		return sh.fail(1, "srun: error: the step wrote more than %d MiB to standard output or error; the fake killed it", maxStepOutput>>20)
	}
	if out.status.Signaled() {
		return 128 + int(out.status.Signal()) // the shell's code for a signal
	}
	return out.status.ExitStatus()
}

// stepJob checks that job id is the calling user's and RUNNING with a live
// process, in scancel's order, and returns its execution. Otherwise it returns
// the reason, in the words of Slurm's error table (slurm_errno.c, 24.11:
// ESLURM_INVALID_JOB_ID, ESLURM_ACCESS_DENIED, ESLURM_ALREADY_DONE and
// ESLURM_JOB_PENDING).
func (sh *shell) stepJob(id int) (*jobRun, string) {
	p := sh.p
	j := p.find(id)
	switch {
	case j == nil || j.purged(sh.now, p.cfg.MinJobAge):
		return nil, "Invalid job id specified"
	case j.user != sh.s.user:
		return nil, "Access/permission denied"
	case IsTerminal(j.state()):
		return nil, "Job/step already completing or completed"
	case j.state() == Pending:
		return nil, "Job is pending execution"
	}
	r := j.cur().run
	if j.state() != Running || r == nil || r.cmd == nil || r.exitSeen || r.done || p.closed {
		return nil, "job is not running"
	}
	return r, ""
}

// envValue is the last value of name in an environment list.
func envValue(env []string, name string) string {
	v := ""
	for _, kv := range env {
		if k, x, ok := strings.Cut(kv, "="); ok && k == name {
			v = x
		}
	}
	return v
}

// lookPath finds program the way a shell does, in the job's PATH (the Go
// process's own PATH is not the job's): a name with a slash is used as it is,
// and a relative path or PATH entry is relative to dir. It returns an error
// that errnoText can word: ENOENT for none, EACCES for one that is there but
// not executable.
func lookPath(name, pathVar, dir string) (string, error) {
	abs := func(f string) string {
		if !path.IsAbs(f) {
			f = path.Join(dir, f)
		}
		return f
	}
	if strings.Contains(name, "/") {
		f := abs(name)
		return f, execErr(f)
	}
	denied := ""
	for _, d := range strings.Split(pathVar, ":") {
		if d == "" {
			d = "."
		}
		f := path.Join(abs(d), name)
		switch err := execErr(f); {
		case err == nil:
			return f, nil
		case errors.Is(err, fs.ErrPermission) && denied == "":
			denied = f
		}
	}
	if denied != "" {
		return denied, syscall.EACCES
	}
	return "", syscall.ENOENT
}

// execErr is nil for a regular file the user may execute.
func execErr(f string) error {
	fi, err := os.Stat(f)
	switch {
	case err != nil:
		return err
	case !fi.Mode().IsRegular():
		return syscall.EACCES
	}
	return unix.Access(f, unix.X_OK)
}

// errnoText words an errno the way a shell does.
func errnoText(err error) string {
	switch {
	case errors.Is(err, fs.ErrPermission):
		return "Permission denied"
	case errors.Is(err, fs.ErrNotExist):
		return "No such file or directory"
	}
	return err.Error()
}

// stepEnv is a step's environment: the job's, with each of over set in place
// of the job's value or added.
func stepEnv(job []string, over map[string]string) []string {
	out := make([]string, 0, len(job)+len(over))
	for _, kv := range job {
		if k, _, ok := strings.Cut(kv, "="); ok {
			if _, replaced := over[k]; replaced {
				continue
			}
		}
		out = append(out, kv)
	}
	for _, k := range slices.Sorted(maps.Keys(over)) {
		out = append(out, k+"="+over[k])
	}
	return out
}

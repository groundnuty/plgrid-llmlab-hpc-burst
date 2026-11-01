package fakeslurm

// New. Run mode: how a job's batch script starts, with its directory, its
// output files and its environment.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

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
	submitDir := path.Clean(j.env["HOME"]) // "." for an unset HOME: not absolute, so not a directory
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
	// Truncated for the first execution and appended to after a requeue. The
	// open is non-blocking and refuses anything but a regular file (a FIFO
	// with no reader would block the submission, with Platform.mu held), and
	// it is anchored to the root, so a link a job made cannot lead it out.
	appendMode := len(j.execs) > 1
	d := p.fs.(*diskFS)
	open := func(name string) (*os.File, error) {
		rel, ok := d.rel(name)
		if !ok {
			return nil, fmt.Errorf("output file %s is outside the run-mode root", name)
		}
		flag := os.O_WRONLY | os.O_CREATE
		if appendMode {
			flag |= os.O_APPEND
		}
		f, err := d.openRegular(rel, flag, 0o644)
		if err != nil {
			return nil, fmt.Errorf("output file %s: %w", name, cause(err))
		}
		if !appendMode {
			if err := f.Truncate(0); err != nil {
				f.Close()
				return nil, fmt.Errorf("output file %s: %w", name, err)
			}
		}
		return f, nil
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

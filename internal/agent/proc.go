package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// killedExitCode is the exit code reported for a process killed by a signal
// (agentapi.WorkerReport.ExitCode).
const killedExitCode = -1

// Proc is one part's process tree, started in its own process group so that a
// stop reaches every process in it: Apptainer's starter, the engine and the
// engine's own children. A stop is TERM to the group, then KILL to the group
// after the stop grace (HBEP-7, Restarts and stops; L-3.3), as spike S2
// phase B's job script stopped its stacks by PID tree, never by name.
type Proc struct {
	cmd   *exec.Cmd
	pid   int
	start time.Time
	done  chan struct{}
	code  int // valid once done is closed
}

// ProcSpec is what StartProc runs.
type ProcSpec struct {
	Path string
	Args []string // argv after the program
	Env  []string
	Dir  string
	// Log receives the process's standard output and error.
	Log io.Writer
}

// StartProc starts the process in a new process group. Its exit is collected
// by a goroutine; Done is closed once it has exited, and ExitCode then holds
// its code.
func StartProc(s ProcSpec, now time.Time) (*Proc, error) {
	cmd := exec.Command(s.Path, s.Args...)
	cmd.Env = s.Env
	cmd.Dir = s.Dir
	cmd.Stdout = s.Log
	cmd.Stderr = s.Log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", s.Path, err)
	}
	p := &Proc{cmd: cmd, pid: cmd.Process.Pid, start: now, done: make(chan struct{})}
	go p.wait()
	return p, nil
}

func (p *Proc) wait() {
	err := p.cmd.Wait()
	p.code = exitCodeOf(p.cmd.ProcessState, err)
	close(p.done)
}

// exitCodeOf returns the process's exit code, or killedExitCode when a signal
// ended it.
func exitCodeOf(st *os.ProcessState, err error) int {
	if st == nil {
		return killedExitCode
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return killedExitCode
	}
	if code := st.ExitCode(); code >= 0 {
		return code
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return killedExitCode
	}
	return killedExitCode
}

// PID is the process group leader's PID.
func (p *Proc) PID() int { return p.pid }

// Start is when the agent started it.
func (p *Proc) Start() time.Time { return p.start }

// Done is closed once the process has exited.
func (p *Proc) Done() <-chan struct{} { return p.done }

// ExitCode is the exit code; valid once Done is closed.
func (p *Proc) ExitCode() int { return p.code }

// Term sends TERM to the process group: the polite stop.
func (p *Proc) Term() { p.signal(syscall.SIGTERM) }

// Kill sends KILL to the process group.
func (p *Proc) Kill() { p.signal(syscall.SIGKILL) }

// signal signals the whole group. The group's ID is the leader's PID, which
// stays reserved while any member lives, so the signal never reaches another
// user's process. ESRCH (the group is gone) is not an error.
func (p *Proc) signal(sig syscall.Signal) {
	_ = syscall.Kill(-p.pid, sig)
}

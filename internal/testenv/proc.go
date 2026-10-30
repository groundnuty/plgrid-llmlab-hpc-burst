package testenv

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// proc is a child process the control plane runs.
type proc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

// startProc starts argv with stdout and stderr appended to logPath.
func startProc(name, logPath string, argv ...string) (*proc, error) {
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// stop sends SIGTERM to the process and, after grace, SIGKILL. It addresses
// the process by the PID it started, never by name.
func (p *proc) stop(grace time.Duration) error {
	if !p.alive() {
		return nil
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
		return nil
	case <-time.After(grace):
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
		return fmt.Errorf("%s did not stop within %s and was killed", p.name, grace)
	case <-time.After(10 * time.Second):
		return fmt.Errorf("%s did not die after SIGKILL", p.name)
	}
}

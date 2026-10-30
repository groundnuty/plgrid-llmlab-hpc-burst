// Package harness holds what the tests and the benchmark share: scratch
// directories, a capacity gate for the shared VM, and child-process roles
// (embedded server host, publisher) started by re-executing the test binary.
package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Scratch is the base directory for store directories and logs. It must not be
// under /home (98 percent full on the VM).
func Scratch() string {
	if v := os.Getenv("S8_SCRATCH"); v != "" {
		return v
	}
	return "/tmp/hpc-burst-tools/spikes/s8/data"
}

// NewDir makes a fresh directory under Scratch.
func NewDir(name string) (string, error) {
	if err := os.MkdirAll(Scratch(), 0o700); err != nil {
		return "", err
	}
	return os.MkdirTemp(Scratch(), name+"-")
}

// Load1 reads the one-minute load average.
func Load1() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return -1
	}
	f := strings.Fields(string(b))
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

// MemAvailGB reads MemAvailable in GiB.
func MemAvailGB() float64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return -1
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "MemAvailable:") {
			f := strings.Fields(l)
			kb, _ := strconv.ParseFloat(f[1], 64)
			return kb / 1024 / 1024
		}
	}
	return -1
}

// WaitForCapacity blocks until the one-minute load is at most maxLoad and at
// least minMemGB is available, re-checking every 60 seconds (the shared-VM rule).
func WaitForCapacity(ctx context.Context, maxLoad, minMemGB float64) error {
	for {
		l, m := Load1(), MemAvailGB()
		if l >= 0 && l <= maxLoad && m >= minMemGB {
			return nil
		}
		fmt.Fprintf(os.Stderr, "s8: waiting for capacity: load1=%.2f (max %.1f) mem_avail=%.1fGiB (min %.0f)\n", l, maxLoad, m, minMemGB)
		select {
		case <-time.After(60 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Child is a re-executed copy of the current binary running one role.
type Child struct {
	Cmd   *exec.Cmd
	Stdin io.WriteCloser
	Lines chan string // stdout lines; closed at EOF
	done  chan struct{}
}

// Spawn starts the current binary in the given role. wrap, if non-empty, is a
// command prefix such as strace and its flags.
func Spawn(role string, env map[string]string, wrap ...string) (*Child, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args := append(append([]string{}, wrap...), exe)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ(), "S8_ROLE="+role)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	c := &Child{Cmd: cmd, Stdin: stdin, Lines: make(chan string, 1<<16), done: make(chan struct{})}
	go func() {
		defer close(c.Lines)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			c.Lines <- sc.Text()
		}
	}()
	go func() { _ = cmd.Wait(); close(c.done) }()
	return c, nil
}

// PID of the child (the wrapper's PID when wrap was used).
func (c *Child) PID() int { return c.Cmd.Process.Pid }

// Kill sends SIGKILL to the child, by PID, and waits for it to be reaped.
func (c *Child) Kill() {
	_ = c.Cmd.Process.Signal(syscall.SIGKILL)
	<-c.done
}

// Exited reports whether the child has been reaped.
func (c *Child) Exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Wait waits for the child to exit, SIGKILLing it after timeout.
func (c *Child) Wait(timeout time.Duration) {
	select {
	case <-c.done:
	case <-time.After(timeout):
		c.Kill()
	}
}

// Stop asks the child to exit (closes its stdin) and SIGKILLs it after timeout.
func (c *Child) Stop(timeout time.Duration) {
	_ = c.Stdin.Close()
	c.Wait(timeout)
}

// WaitLine returns the first stdout line with the given prefix.
func (c *Child) WaitLine(prefix string, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	for {
		select {
		case l, ok := <-c.Lines:
			if !ok {
				return "", fmt.Errorf("child exited before printing %q", prefix)
			}
			if strings.HasPrefix(l, prefix) {
				return l, nil
			}
		case <-deadline:
			return "", fmt.Errorf("no %q line within %s", prefix, timeout)
		}
	}
}

// Package harness holds what the tests and the benchmark share: scratch
// directories, a capacity gate for the shared VM, and child-process roles
// (embedded server host, publisher) started by re-executing the test binary.
package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Scratch is the base directory for store directories and logs. It defaults to a
// directory under the system temp directory; set S8_SCRATCH to put the stores on
// a different (large, fast) disk.
func Scratch() string {
	if v := os.Getenv("S8_SCRATCH"); v != "" {
		return v
	}
	return filepath.Join(os.TempDir(), "s8-data")
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
//
// Its standard output is read through a pipe owned by Child, not by os/exec, so
// that reaping the process can never close the pipe while lines are still
// unread (os/exec closes the read end of a StdoutPipe when Wait returns, which
// silently discards whatever the reader had not yet consumed). Every line the
// child wrote is delivered on Lines, in order; Lines is closed once the child's
// stdout reached end of file, and only then is Err meaningful.
type Child struct {
	Cmd   *exec.Cmd
	Stdin io.WriteCloser
	Lines chan string // stdout lines; closed at EOF or on a scan error
	done  chan struct{}
	// scanErr is set before Lines is closed: read it only after Lines is closed.
	scanErr error
	// waitErr is set before done is closed.
	waitErr error
}

// SpawnOpts tunes how a Child's stdout is read. The zero value is right for
// production use; the tests shrink the limits to make a slow reader or an
// over-long line deterministic.
type SpawnOpts struct {
	LinesCap int // capacity of the Lines channel (default 65536)
	MaxLine  int // longest accepted line in bytes (default 1 MiB); a longer one is a scan error
}

// Spawn starts the current binary in the given role. wrap, if non-empty, is a
// command prefix such as strace and its flags.
func Spawn(role string, env map[string]string, wrap ...string) (*Child, error) {
	return SpawnWith(SpawnOpts{}, role, env, wrap...)
}

// SpawnWith is Spawn with explicit reading limits.
func SpawnWith(o SpawnOpts, role string, env map[string]string, wrap ...string) (*Child, error) {
	if o.LinesCap == 0 {
		o.LinesCap = 1 << 16
	}
	if o.MaxLine == 0 {
		o.MaxLine = 1 << 20
	}
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
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = pw // an *os.File: os/exec hands it to the child as is and never closes pr
	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		return nil, err
	}
	pw.Close() // the parent's copy of the write end, so pr reaches EOF when the child's last writer exits
	c := &Child{Cmd: cmd, Stdin: stdin, Lines: make(chan string, o.LinesCap), done: make(chan struct{})}
	go func() {
		defer close(c.Lines)
		defer pr.Close()
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, min(o.MaxLine, 64<<10)), o.MaxLine)
		for sc.Scan() {
			c.Lines <- sc.Text()
		}
		c.scanErr = sc.Err()
	}()
	go func() { c.waitErr = cmd.Wait(); close(c.done) }()
	return c, nil
}

// Err is the error that ended the reading of the child's stdout (for example a
// line longer than the limit), or nil if it ended at end of file. Call it only
// after Lines is closed.
func (c *Child) Err() error { return c.scanErr }

// PID of the child (the wrapper's PID when wrap was used).
func (c *Child) PID() int { return c.Cmd.Process.Pid }

// Kill sends SIGKILL to the child, by PID, and waits for it to be reaped. It
// returns the error from sending the signal, which is os.ErrProcessDone when the
// child had already exited on its own. Cleanup code may ignore the result.
func (c *Child) Kill() error {
	err := c.Cmd.Process.Signal(syscall.SIGKILL)
	<-c.done
	return err
}

// KillStrict is Kill for a test that needs the crash to be a real SIGKILL: it
// fails if the signal could not be sent, or if the child ended in any way other
// than being killed by SIGKILL (for example because it had already exited).
func (c *Child) KillStrict() error {
	if err := c.Kill(); err != nil {
		return fmt.Errorf("harness: sending SIGKILL to pid %d: %w", c.PID(), err)
	}
	sig, err := c.TerminatedBy()
	if err != nil {
		return err
	}
	if sig != syscall.SIGKILL {
		return fmt.Errorf("harness: pid %d was terminated by %v, want SIGKILL", c.PID(), sig)
	}
	return nil
}

// TerminatedBy returns the signal that ended the child, or an error saying how
// else it ended. It waits for the child to be reaped.
func (c *Child) TerminatedBy() (syscall.Signal, error) {
	<-c.done
	var ee *exec.ExitError
	if !errors.As(c.waitErr, &ee) {
		if c.waitErr != nil {
			return 0, fmt.Errorf("harness: waiting for pid %d: %w", c.PID(), c.waitErr)
		}
		return 0, fmt.Errorf("harness: pid %d exited with status 0, it was not killed by a signal", c.PID())
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	if !ok {
		return 0, fmt.Errorf("harness: pid %d: no wait status on this platform", c.PID())
	}
	if !ws.Signaled() {
		return 0, fmt.Errorf("harness: pid %d exited with status %d, it was not killed by a signal", c.PID(), ws.ExitStatus())
	}
	return ws.Signal(), nil
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
		_ = c.Kill()
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

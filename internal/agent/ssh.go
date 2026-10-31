package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// SSHForward keeps the agent's dial-home forward in ssh mode (HBEP-7,
// Registration): ssh -N -L over port 22, in a reconnect loop, with the
// restricted key and the pinned host key that the operator placed in the
// dial-home directory once (agentapi.DialHomeKeyFile,
// agentapi.DialHomeKnownHostsFile).
//
// The local end of the forward is a Unix socket in the job's private run
// directory, not a loopback port, so no port can clash with another job on a
// shared node (L-7.7). The remote end is the agent endpoint's loopback port,
// the only target the key's authorized_keys line permits besides the front's.
type SSHForward struct {
	// SSH is the ssh binary.
	SSH string
	// Socket is the local Unix socket the forward listens on.
	Socket string
	// Config is agent.json; Mode must be DialHomeSSH.
	Config agentapi.AgentConfig
	// Log receives ssh's output.
	Log io.Writer
	// Logger reports reconnects.
	Logger *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between attempts (defaults 1 s
	// and 30 s).
	MinBackoff, MaxBackoff time.Duration
	// StopGrace is how long a stopped ssh has after TERM before KILL
	// (default 5 s).
	StopGrace time.Duration
	// OnStart, when set, is told each ssh process the loop starts.
	OnStart func(ProcessID)
}

// ProcessID identifies one process across PID reuse: its PID and its start
// time, the 22nd field of /proc/<pid>/stat, in clock ticks since boot.
type ProcessID struct {
	PID       int
	StartTime uint64
}

// procStartTime reads a process's start time from /proc/<pid>/stat.
func procStartTime(pid int) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// Fields after the command name, which is in parentheses and may hold
	// spaces: state is field 3, start time field 22.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, fmt.Errorf("/proc/%d/stat: no command name", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, fmt.Errorf("/proc/%d/stat: %d fields after the name", pid, len(f))
	}
	return strconv.ParseUint(f[19], 10, 64)
}

// Alive reports whether the process with this PID and start time still runs:
// a process with the PID and another start time is another process, and a
// zombie runs nothing.
func (p ProcessID) Alive() bool {
	st, err := procStartTime(p.PID)
	if err != nil || st != p.StartTime {
		return false
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(p.PID) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	return i >= 0 && i+2 < len(s) && s[i+2] != 'Z'
}

// Args returns ssh's argument vector, without the program.
func (s SSHForward) Args() ([]string, error) {
	host, port, err := net.SplitHostPort(s.Config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("ssh endpoint %q: %w", s.Config.Endpoint, err)
	}
	if s.Config.SSH == nil {
		return nil, fmt.Errorf("agent.json has no ssh settings")
	}
	dir := s.Config.DialHomeDir
	return []string{
		"-N",
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + filepath.Join(dir, agentapi.DialHomeKnownHostsFile),
		"-o", "ExitOnForwardFailure=yes",
		"-o", "StreamLocalBindUnlink=yes",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-o", "ConnectTimeout=20",
		"-i", filepath.Join(dir, agentapi.DialHomeKeyFile),
		"-p", port,
		"-L", s.Socket + ":127.0.0.1:" + strconv.Itoa(s.Config.SSH.AgentPort),
		s.Config.SSH.User + "@" + host,
	}, nil
}

// Run keeps the forward up until ctx ends: it starts ssh, waits for it to
// exit, and starts it again after a backoff that doubles up to MaxBackoff and
// starts again from MinBackoff after a forward that lasted a minute.
func (s SSHForward) Run(ctx context.Context) error {
	args, err := s.Args()
	if err != nil {
		return err
	}
	minB, maxB := s.MinBackoff, s.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB < minB {
		maxB = 30 * time.Second
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// No descriptor the agent inherited reaches ssh (HBEP-6, teardown: an
	// orphan must hold nothing of its parent's).
	if err := CloseInheritedFDs(); err != nil {
		logger.Warn("cannot mark inherited file descriptors close-on-exec", "err", err)
	}
	backoff := minB
	for {
		started := time.Now()
		err := s.once(ctx, args)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > time.Minute {
			backoff = minB
		}
		logger.Warn("ssh forward ended; reconnecting", "after", backoff, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxB)
	}
}

// once runs one ssh process until it exits or ctx ends. It is recorded by PID
// and start time, and always reaped: on ctx's end it gets TERM to its process
// group, then KILL after StopGrace, and once returns only after it has
// exited. The process group is signalled only while the process is
// unreaped, so its PID cannot have been reused.
func (s SSHForward) once(ctx context.Context, args []string) error {
	grace := s.StopGrace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	cmd := exec.Command(s.SSH, args...)
	cmd.Stdout = s.Log
	cmd.Stderr = s.Log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	id := ProcessID{PID: cmd.Process.Pid}
	if st, err := procStartTime(id.PID); err == nil {
		id.StartTime = st
	}
	if s.Logger != nil {
		s.Logger.Info("ssh forward started", "pid", id.PID, "startTime", id.StartTime)
	}
	if s.OnStart != nil {
		s.OnStart(id)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = syscall.Kill(-id.PID, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(grace):
			_ = syscall.Kill(-id.PID, syscall.SIGKILL)
			<-done
		}
		_ = os.Remove(s.Socket)
		if s.Logger != nil {
			s.Logger.Info("ssh forward stopped", "pid", id.PID, "startTime", id.StartTime)
		}
		return ctx.Err()
	}
}

// Dial connects to the forward's local socket.
func (s SSHForward) Dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", s.Socket)
}

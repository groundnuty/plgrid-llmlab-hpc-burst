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

func (s SSHForward) once(ctx context.Context, args []string) error {
	cmd := exec.Command(s.SSH, args...)
	cmd.Stdout = s.Log
	cmd.Stderr = s.Log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		_ = os.Remove(s.Socket)
		return ctx.Err()
	}
}

// Dial connects to the forward's local socket.
func (s SSHForward) Dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", s.Socket)
}

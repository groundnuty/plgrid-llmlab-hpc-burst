// Package sshrun is the Slurm plugin's transport (HBEP-4, Transport): it runs
// command lines on a login node over one OpenSSH master connection per login
// node and credential, owned by this process.
//
//   - ssh always runs with -F /dev/null: the values come from the Provider's
//     parsed credentials file (slurm.SSHConfig) as -o options, so neither the
//     user's ~/.ssh/config, its host aliases and shared masters, nor the
//     system's ssh_config can change the connection.
//   - The master is "ssh -N" with ControlMaster=yes and a ControlPath private
//     to the Provider, under a short directory of mode 700. It carries no
//     port forward and no other session (ClearAllForwardings).
//   - Each command is a mux client with ControlMaster=no. Its ProxyCommand is
//     /bin/false, so a client that cannot reach the master fails instead of
//     opening a second connection to the login node.
//   - BatchMode, IdentitiesOnly, IdentityAgent=none and StrictHostKeyChecking
//     are always on; the child's environment is scrubbed, so no ssh-agent
//     socket reaches it.
//   - At most MaxConcurrent commands run at once (HBEP-4: four).
//   - When the master fails it is restarted on the next command, with
//     exponential backoff from 1 s to 1 min; commands during the backoff fail
//     with slurm.ErrNotStarted without starting ssh.
//
// Errors never carry ssh's standard error, which names the host and the user;
// they say what kind of failure it was.
//
// New code for issue 59; the multiplexing follows www-2027's per-cluster ssh
// master and HBEP-4's Transport section.
package sshrun

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// maxSocketPath is the longest ControlPath accepted: a Unix socket's path is
// limited to 108 bytes on Linux, and ssh adds a temporary suffix while it
// binds.
const maxSocketPath = 90

// alias is the host argument given to ssh. With -F /dev/null it names no
// configuration; HostName says where to connect.
const alias = "hpcburst-login"

// Options configure an SSH transport.
type Options struct {
	// Name identifies the transport's owner, the Provider; it enters the
	// ControlPath, so two Providers never share a master even with the same
	// credentials.
	Name string
	// Config is the Provider's parsed credentials.
	Config slurm.SSHConfig
	// ControlDir holds the master's socket. It is created with mode 700 when
	// missing and must be owned by this user with no group or other bits. It
	// must be short: the socket path is capped at 90 bytes.
	ControlDir string
	// Binary is the ssh client; "" means "ssh" on PATH.
	Binary string
	// Env is the child's environment; nil means PATH and HOME of this
	// process only.
	Env []string
	// MaxConcurrent is the most commands at once; 0 means
	// provider.MaxConcurrentCommands.
	MaxConcurrent int
	// HostLock, when set, is a lock file held (flock) around connecting and
	// around each command, so that only one ssh call is in flight per host
	// across processes. Tests against shared hosts set it; production leaves
	// it empty.
	HostLock string
	// MinBackoff and MaxBackoff bound the reconnect backoff; zero means 1 s
	// and 1 min.
	MinBackoff, MaxBackoff time.Duration
	// ConnectTimeout bounds how long a new master may take to come up; zero
	// means 30 s.
	ConnectTimeout time.Duration
	// MinLifetime is how long a master must have lived for its death to count
	// as a dropped connection rather than a failed one; zero means 30 s. A
	// master that dies sooner raises the backoff, so a login node that drops
	// connections at once is not hammered.
	MinLifetime time.Duration
}

// SSH is a slurm.Runner over one OpenSSH master. It is safe for concurrent
// use.
type SSH struct {
	o      Options
	socket string
	sem    chan struct{}

	mu       sync.Mutex
	master   *master
	gen      uint64
	failures int
	nextTry  time.Time
	closed   bool
	starts   int
}

var (
	_ slurm.Runner    = (*SSH)(nil)
	_ slurm.Connector = (*SSH)(nil)
)

type master struct {
	cmd     *exec.Cmd
	done    chan struct{}
	started time.Time
}



func (m *master) alive() bool {
	select {
	case <-m.done:
		return false
	default:
		return true
	}
}

// New returns a transport. It makes no connection until the first command.
func New(o Options) (*SSH, error) {
	if o.Name == "" {
		return nil, errors.New("sshrun: no name")
	}
	if o.Binary == "" {
		o.Binary = "ssh"
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = provider.MaxConcurrentCommands
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Minute
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 30 * time.Second
	}
	if o.MinLifetime <= 0 {
		o.MinLifetime = 30 * time.Second
	}
	if o.Env == nil {
		o.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	}
	for _, kv := range o.Env {
		if strings.HasPrefix(kv, "SSH_AUTH_SOCK=") {
			return nil, errors.New("sshrun: the environment must not carry SSH_AUTH_SOCK")
		}
	}
	if err := privateDir(o.ControlDir); err != nil {
		return nil, err
	}
	c := o.Config
	sum := sha256.Sum256([]byte(strings.Join([]string{o.Name, c.HostName, c.User, strconv.Itoa(c.Port), c.IdentityFile}, "\x00")))
	socket := filepath.Join(o.ControlDir, hex.EncodeToString(sum[:])[:16]+".sock")
	if len(socket) > maxSocketPath {
		return nil, fmt.Errorf("sshrun: the control socket path would be %d bytes, above %d; choose a shorter control directory", len(socket), maxSocketPath)
	}
	return &SSH{o: o, socket: socket, sem: make(chan struct{}, o.MaxConcurrent)}, nil
}

// privateDir makes dir, mode 700, or checks that it is private.
func privateDir(dir string) error {
	if dir == "" || !filepath.IsAbs(dir) {
		return errors.New("sshrun: the control directory must be an absolute path")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("sshrun: create the control directory: %w", err)
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("sshrun: the control directory: %w", err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.IsDir() || fi.Mode().Perm()&0o077 != 0 || !ok || int(st.Uid) != os.Geteuid() {
		return errors.New("sshrun: the control directory must be a directory of this user with mode 700")
	}
	return nil
}

// common are the options of every ssh invocation.
func (s *SSH) common() []string {
	c := s.o.Config
	a := []string{"-F", "/dev/null",
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=yes",
		"-o", "IdentityAgent=none",
		"-o", "ClearAllForwardings=yes",
		"-o", "ForwardAgent=no",
		"-o", "ForwardX11=no",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UpdateHostKeys=no",
		"-o", "ServerAliveInterval=30",
		"-o", "ServerAliveCountMax=4",
		"-o", "ConnectTimeout=20",
		"-o", "LogLevel=ERROR",
		"-o", "ControlPath=" + s.socket,
		"-o", "HostName=" + c.HostName,
		"-o", "User=" + c.User,
		"-o", "IdentityFile=" + c.IdentityFile,
	}
	if c.Port != 0 {
		a = append(a, "-o", "Port="+strconv.Itoa(c.Port))
	}
	if c.UserKnownHostsFile != "" {
		a = append(a, "-o", "UserKnownHostsFile="+c.UserKnownHostsFile)
	}
	return a
}

func (s *SSH) masterArgs() []string {
	return append(s.common(), "-o", "ControlMaster=yes", "-o", "ControlPersist=no", "-N", "-T", alias)
}

func (s *SSH) clientArgs(line string) []string {
	return append(s.common(), "-o", "ControlMaster=no", "-o", "ProxyCommand=/bin/false", "-T", alias, "--", line)
}

func (s *SSH) controlArgs(op string) []string {
	return append(s.common(), "-O", op, alias)
}

// Generation implements slurm.Runner: it counts the masters this transport
// has brought up.
func (s *SSH) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// Starts returns how many times a master was started, successfully or not.
func (s *SSH) Starts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts
}

func (s *SSH) backoff() time.Duration {
	d := s.o.MinBackoff
	for i := 1; i < s.failures && d < s.o.MaxBackoff; i++ {
		d *= 2
	}
	if d > s.o.MaxBackoff {
		d = s.o.MaxBackoff
	}
	return d
}

// ensureMaster brings the master up when it is not running, unless the
// backoff after a failure has not passed.
func (s *SSH) ensureMaster(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("%w: the transport is closed", slurm.ErrNotStarted)
	}
	if s.master != nil && s.master.alive() {
		return nil
	}
	if s.master != nil {
		if time.Since(s.master.started) < s.o.MinLifetime {
			s.failures++
			s.nextTry = time.Now().Add(s.backoff())
		} else {
			s.failures = 0
		}
		s.stopMaster()
	}
	if time.Now().Before(s.nextTry) {
		return fmt.Errorf("%w: reconnecting after a failed connection, next attempt in %s", slurm.ErrNotStarted, time.Until(s.nextTry).Round(time.Second))
	}
	unlock, err := s.lockHost(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	s.starts++
	m, why := s.startMaster()
	if m == nil {
		s.failures++
		s.nextTry = time.Now().Add(s.backoff())
		return fmt.Errorf("%w: %s", slurm.ErrNotStarted, why)
	}
	// failures is reset only once a master has lived past MinLifetime, so a
	// master that comes up and drops at once keeps raising the backoff.
	s.master, s.nextTry = m, time.Time{}
	s.gen++
	return nil
}

// startMaster starts ssh -N and waits until its control socket answers. On
// failure it returns nil and a description that names no host.
func (s *SSH) startMaster() (*master, string) {
	_ = os.Remove(s.socket)
	var stderr bytes.Buffer
	cmd := exec.Command(s.o.Binary, s.masterArgs()...)
	cmd.Env = s.o.Env
	cmd.Stderr = &limited{w: &stderr, n: 4096}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	if err := cmd.Start(); err != nil {
		return nil, "the ssh client could not be started"
	}
	m := &master{cmd: cmd, done: make(chan struct{}), started: time.Now()}
	go func() { _ = cmd.Wait(); close(m.done) }()
	deadline := time.Now().Add(s.o.ConnectTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-m.done:
			return nil, "the connection failed: " + classify(stderr.String())
		case <-time.After(100 * time.Millisecond):
		}
		if s.check() {
			return m, ""
		}
	}
	s.killMaster(m)
	return nil, "the connection did not come up within " + s.o.ConnectTimeout.String()
}

// check asks the master whether it is running (ssh -O check).
func (s *SSH) check() bool {
	cmd := exec.Command(s.o.Binary, s.controlArgs("check")...)
	cmd.Env = s.o.Env
	return cmd.Run() == nil
}

func (s *SSH) killMaster(m *master) {
	if m == nil || m.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-m.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-m.cmd.Process.Pid, syscall.SIGKILL)
		<-m.done
	}
}

// stopMaster stops the current master. The caller holds s.mu.
func (s *SSH) stopMaster() {
	s.killMaster(s.master)
	s.master = nil
	_ = os.Remove(s.socket)
}

// Connect implements slurm.Connector: it brings the master up when it is not
// running, subject to the backoff.
func (s *SSH) Connect(ctx context.Context) error { return s.ensureMaster(ctx) }

// Close stops the master. Later commands fail with ErrNotStarted.
func (s *SSH) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.stopMaster()
	return nil
}

// Run implements slurm.Runner.
func (s *SSH) Run(ctx context.Context, c slurm.Command) (slurm.Result, error) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return slurm.Result{}, fmt.Errorf("%w: %v", slurm.ErrNotStarted, ctx.Err())
	}
	defer func() { <-s.sem }()
	if err := s.ensureMaster(ctx); err != nil {
		return slurm.Result{}, err
	}
	unlock, err := s.lockHost(ctx)
	if err != nil {
		return slurm.Result{}, err
	}
	defer unlock()
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = provider.CommandTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(cctx, s.o.Binary, s.clientArgs(c.Line)...)
	cmd.Env = s.o.Env
	if c.Stdin != nil {
		cmd.Stdin = bytes.NewReader(c.Stdin)
	}
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 2 * time.Second
	err = cmd.Run()
	res := slurm.Result{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if errors.Is(cctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		return res, fmt.Errorf("%w after %s", slurm.ErrTimeout, timeout)
	}
	if ctx.Err() != nil {
		return res, fmt.Errorf("%w: %v", slurm.ErrTransport, ctx.Err())
	}
	var exit *exec.ExitError
	switch {
	case err == nil:
		return res, nil
	case errors.As(err, &exit):
		code := exit.ExitCode()
		if code == 255 || code < 0 {
			// 255 is ssh's own failure; a signal ends the client. The
			// command may have run.
			return res, fmt.Errorf("%w: %s", slurm.ErrTransport, classify(stderr.String()))
		}
		res.ExitCode = code
		return res, nil
	default:
		return res, fmt.Errorf("%w: the ssh client could not be started", slurm.ErrNotStarted)
	}
}

// classify names the kind of an ssh failure from its standard error, without
// repeating any of it.
func classify(stderr string) string {
	switch s := strings.ToLower(stderr); {
	case strings.Contains(s, "host key verification failed"), strings.Contains(s, "remote host identification has changed"):
		return "host key verification failed"
	case strings.Contains(s, "permission denied"):
		return "authentication was refused"
	case strings.Contains(s, "connection refused"):
		return "the connection was refused"
	case strings.Contains(s, "timed out"):
		return "the connection timed out"
	case strings.Contains(s, "could not resolve"), strings.Contains(s, "name or service not known"):
		return "the host name could not be resolved"
	case strings.Contains(s, "control socket"), strings.Contains(s, "mux_client"), strings.Contains(s, "master"):
		return "the master connection is gone"
	case strings.Contains(s, "closed by"), strings.Contains(s, "connection reset"), strings.Contains(s, "broken pipe"):
		return "the connection was closed"
	case strings.TrimSpace(s) == "":
		return "ssh failed without a message"
	default:
		return "ssh failed"
	}
}

// lockHost takes the host lock, when one is configured.
func (s *SSH) lockHost(ctx context.Context) (func(), error) {
	if s.o.HostLock == "" {
		return func() {}, nil
	}
	f, err := os.OpenFile(s.o.HostLock, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: the host lock cannot be opened", slurm.ErrNotStarted)
	}
	deadline := time.Now().Add(10 * time.Minute)
	for {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
		}
		if time.Now().After(deadline) {
			f.Close()
			return nil, fmt.Errorf("%w: the host lock was not free within 10 min", slurm.ErrNotStarted)
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, fmt.Errorf("%w: %v", slurm.ErrNotStarted, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// limited writes at most n bytes to w and drops the rest.
type limited struct {
	w *bytes.Buffer
	n int
}

func (l *limited) Write(p []byte) (int, error) {
	if room := l.n - l.w.Len(); room > 0 {
		if len(p) > room {
			l.w.Write(p[:room])
		} else {
			l.w.Write(p)
		}
	}
	return len(p), nil
}

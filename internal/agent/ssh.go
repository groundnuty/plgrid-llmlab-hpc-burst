package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// SSHForward keeps one of the agent's two forwards in ssh dial-home mode
// (HBEP-6, Dial-home modes): ssh -N -L over the control-plane host's ssh
// server, in a reconnect loop of its own, with the restricted key and the
// pinned host key that the operator placed in the dial-home directory once
// (agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile).
//
//   - Connection 1 forwards to the agent endpoint's loopback port (base+3,
//     agent.json's ssh.agentPort), for the agent's control traffic.
//   - Connection 2 forwards to the dial-home listener's loopback port
//     (base+7), for frpc's wss.
//
// The local end listens on the node's IPv4 loopback address, on a port the
// forward asks the kernel for once, at its start, never a fixed or derived
// port (L-2.4, L-7.7). The forward keeps that port for its life, and each
// reconnect binds it again. When another process has taken it while the
// forward was down, the forward takes a new port from the kernel and says so
// through State, so its user moves: the control client dials the new port,
// and frpc is restarted with it.
//
// An SSHForward is used through a pointer and run once.
type SSHForward struct {
	// SSH is the ssh binary.
	SSH string
	// Config is agent.json: the endpoint (the ssh server, host:port), the
	// ssh user and the dial-home directory.
	Config agentapi.AgentConfig
	// RemotePort is the loopback port on the control-plane host that the
	// forward reaches.
	RemotePort int
	// Name names the forward in the log ("control", "tunnel").
	Name string
	// Log receives ssh's output.
	Log io.Writer
	// Logger reports reconnects and port changes.
	Logger *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between attempts (defaults 1 s
	// and 30 s).
	MinBackoff, MaxBackoff time.Duration
	// StopGrace is how long a stopped ssh has after TERM before KILL
	// (default 5 s).
	StopGrace time.Duration
	// OnStart, when set, is told each ssh process the loop starts.
	OnStart func(ProcessID)

	st forwardState
}

// ProcessID identifies one process across PID reuse: its PID and its start
// time (procgroup.ID).
type ProcessID = procgroup.ID

// ForwardState is a forward's local end at one moment.
type ForwardState struct {
	// Port is the local port on 127.0.0.1; 0 until the forward has taken one.
	Port int
	// Listening is true while ssh's forward listens on Port: ssh is
	// connected and authenticated, and binds the forward only then.
	Listening bool
	// PortChanges counts the ports the forward gave up because another
	// process held them.
	PortChanges int
}

// forwardState is a ForwardState with a broadcast of its changes.
type forwardState struct {
	mu      sync.Mutex
	s       ForwardState
	changed chan struct{}
}

func (f *forwardState) get() (ForwardState, <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.changed == nil {
		f.changed = make(chan struct{})
	}
	return f.s, f.changed
}

func (f *forwardState) update(fn func(*ForwardState)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	before := f.s
	fn(&f.s)
	if f.s == before {
		return
	}
	if f.changed != nil {
		close(f.changed)
	}
	f.changed = make(chan struct{})
}

// State returns the forward's state and a channel that is closed at its next
// change.
func (s *SSHForward) State() (ForwardState, <-chan struct{}) { return s.st.get() }

// Args returns ssh's argument vector for a forward from localPort, without
// the program.
func (s *SSHForward) Args(localPort int) ([]string, error) {
	host, port, err := net.SplitHostPort(s.Config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("ssh endpoint %q: %w", s.Config.Endpoint, err)
	}
	if s.Config.SSH == nil {
		return nil, errors.New("agent.json has no ssh settings")
	}
	if s.RemotePort < 1 || s.RemotePort > 65535 {
		return nil, fmt.Errorf("the forward's remote port %d is not a port", s.RemotePort)
	}
	if localPort < 1 || localPort > 65535 {
		return nil, fmt.Errorf("the forward's local port %d is not a port", localPort)
	}
	dir := s.Config.DialHomeDir
	return []string{
		"-N",
		// No user or system ssh configuration: a ControlMaster there would
		// fork a master outside the forward's process group, and a
		// ProxyJump or LocalForward would change the path.
		"-F", "/dev/null",
		"-o", "ControlMaster=no",
		"-o", "ControlPath=none",
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + filepath.Join(dir, agentapi.DialHomeKnownHostsFile),
		// A failed bind ends ssh at once, so a taken port shows at once.
		"-o", "ExitOnForwardFailure=yes",
		// HBEP-6, Dial-home modes: ssh's own liveness.
		"-o", "ServerAliveInterval=10",
		"-o", "ServerAliveCountMax=3",
		"-o", "ConnectTimeout=20",
		"-i", filepath.Join(dir, agentapi.DialHomeKeyFile),
		"-p", port,
		// The node's IPv4 loopback address, named: ssh's default binds
		// "localhost", which can also take ::1.
		"-L", "127.0.0.1:" + strconv.Itoa(localPort) + ":127.0.0.1:" + strconv.Itoa(s.RemotePort),
		s.Config.SSH.User + "@" + host,
	}, nil
}

// Run keeps the forward up until ctx ends: it starts ssh, waits for it to
// exit, and starts it again after a backoff that doubles up to MaxBackoff and
// starts again from MinBackoff after a forward that lasted a minute. Before
// each start it checks that its port can still be bound, and takes a new one
// from the kernel if not. Run returns only once its ssh is gone and reaped.
func (s *SSHForward) Run(ctx context.Context) error {
	if _, err := s.Args(1); err != nil {
		return err
	}
	defer s.releasePort()
	minB, maxB := s.MinBackoff, s.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB < minB {
		maxB = max(30*time.Second, minB)
	}
	logger := s.logger()
	// No descriptor the agent inherited reaches ssh (HBEP-6, teardown: an
	// orphan must hold nothing of its parent's).
	if err := CloseInheritedFDs(); err != nil {
		logger.Warn("cannot mark inherited file descriptors close-on-exec", "err", err)
	}
	backoff := minB
	for {
		started := time.Now()
		err := s.attempt(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > time.Minute {
			backoff = minB
		}
		logger.Warn("ssh forward ended; reconnecting", "forward", s.Name, "after", backoff, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxB)
	}
}

func (s *SSHForward) logger() *slog.Logger {
	if s.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Logger
}

// attempt takes the forward's port, keeping it when it can be bound again,
// and runs one ssh on it.
func (s *SSHForward) attempt(ctx context.Context) error {
	port, err := s.localPort()
	if err != nil {
		return err
	}
	args, err := s.Args(port)
	if err != nil {
		return err
	}
	return s.once(ctx, args, port)
}

// localPort returns the port the next ssh binds: the forward's own while it
// can be bound again, else a new one from the kernel (HBEP-6, A port that
// cannot be bound again).
func (s *SSHForward) localPort() (int, error) {
	cur, _ := s.st.get()
	if cur.Port != 0 {
		err := forwardPorts.canBindAgain(s, cur.Port)
		if err == nil {
			return cur.Port, nil
		}
		s.logger().Warn("the forward's local port is taken; moving to a new one", "forward", s.Name, "port", cur.Port, "err", err)
	}
	// The old port stays this forward's until the new one is taken.
	p, err := forwardPorts.take(s)
	if err != nil {
		return 0, err
	}
	if cur.Port != 0 {
		forwardPorts.giveUp(s, cur.Port)
	}
	s.st.update(func(f *ForwardState) {
		if f.Port != 0 {
			f.PortChanges++
		}
		f.Port = p
		f.Listening = false
	})
	s.logger().Info("ssh forward's local port", "forward", s.Name, "port", p)
	return p, nil
}

// releasePort gives the forward's port back to the agent's pool when Run
// ends; the port stays in State.
func (s *SSHForward) releasePort() {
	cur, _ := s.st.get()
	if cur.Port != 0 {
		forwardPorts.giveUp(s, cur.Port)
	}
}

// once runs one ssh process until it exits or ctx ends, in a process group
// the forward owns until every member is gone (package procgroup). It is
// recorded by PID and start time. While it runs, once watches for its forward
// to listen on port. On ctx's end it gets TERM to its group, then KILL after
// StopGrace; when it exits by itself, whatever is left of its group is
// stopped the same way. once returns only after the whole group is gone and
// the leader is reaped (HBEP-6, teardown).
func (s *SSHForward) once(ctx context.Context, args []string, port int) error {
	grace := s.StopGrace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	g, err := procgroup.Start(procgroup.Spec{Path: s.SSH, Args: args, Stdout: s.Log, Stderr: s.Log}, time.Now())
	if err != nil {
		return err
	}
	id := g.ID()
	s.logger().Info("ssh forward started", "forward", s.Name, "pid", id.PID, "startTime", id.StartTime, "port", port)
	if s.OnStart != nil {
		s.OnStart(id)
	}
	watched := make(chan struct{})
	stopWatch := make(chan struct{})
	go func() {
		defer close(watched)
		s.watchListen(g, port, stopWatch)
	}()
	// notListening ends the watch, then clears Listening: at the leader's
	// exit at once, not once the rest of its group is gone, which can take
	// the whole grace, so frpc never starts on a forward that has gone.
	var once sync.Once
	notListening := func() {
		once.Do(func() {
			close(stopWatch)
			<-watched
			s.st.update(func(f *ForwardState) { f.Listening = false })
		})
	}
	defer notListening()
	select {
	case <-g.Exited():
		notListening()
		g.Stop(grace)
		<-g.Done()
		return fmt.Errorf("ssh exited with code %d", g.ExitCode())
	case <-ctx.Done():
		notListening()
		g.Stop(grace)
		<-g.Done()
		s.logger().Info("ssh forward stopped", "forward", s.Name, "pid", id.PID, "startTime", id.StartTime)
		return ctx.Err()
	}
}

// listenPoll is how often a started ssh is checked for its forward's
// listener.
const listenPoll = 20 * time.Millisecond

// watchListen marks the forward listening once ssh's own socket listens on
// port, and returns then, or when ssh exits or stop is closed.
func (s *SSHForward) watchListen(g *procgroup.Group, port int, stop <-chan struct{}) {
	t := time.NewTicker(listenPoll)
	defer t.Stop()
	for {
		if listensOn(g.PID(), port) {
			s.st.update(func(f *ForwardState) { f.Listening = true })
			s.logger().Info("ssh forward listens", "forward", s.Name, "port", port)
			return
		}
		select {
		case <-stop:
			return
		case <-g.Exited():
			return
		case <-t.C:
		}
	}
}

// Dial connects to the forward's local port as it is now. After a port
// change the next dial reaches the new port; a caller that keeps idle
// connections closes them (Client.CloseIdle).
func (s *SSHForward) Dial(ctx context.Context) (net.Conn, error) {
	st, _ := s.st.get()
	if st.Port == 0 {
		return nil, errors.New("the ssh forward has no local port yet")
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp4", "127.0.0.1:"+strconv.Itoa(st.Port))
}

// OnPortChange calls fn with the forward's new local port each time it
// changes, until ctx ends. The control client uses it to drop its idle
// connections to the old port.
func OnPortChange(ctx context.Context, f *SSHForward, fn func(port int)) {
	st, ch := f.State()
	port := st.Port
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
		}
		st, ch = f.State()
		if st.Port != port && port != 0 {
			fn(st.Port)
		}
		port = st.Port
	}
}

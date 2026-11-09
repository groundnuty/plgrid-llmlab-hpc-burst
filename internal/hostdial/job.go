package hostdial

import (
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// DefaultWait is how long a connection accepted while its channel has no
// session waits for one (HBEP-6, Dial-home modes, host-dialed mode: "waits up
// to 30 s for one, then is closed").
const DefaultWait = 30 * time.Second

// Session settings, the same on both ends. A keepalive every 10 s, as ssh's
// ServerAliveInterval on the relay; a write that blocks, or a keepalive with
// no answer, for 30 s ends the session, so a frozen path is noticed within
// 40 s, and the controller starts the relay again.
const (
	keepAliveInterval = 10 * time.Second
	writeTimeout      = 30 * time.Second
)

func sessionConfig(l *slog.Logger, side string) *yamux.Config {
	c := yamux.DefaultConfig()
	c.KeepAliveInterval = keepAliveInterval
	c.ConnectionWriteTimeout = writeTimeout
	c.LogOutput = nil
	c.Logger = log.New(logWriter{l: l, side: side}, "", 0)
	return c
}

// logWriter turns yamux's log lines into debug records of the logger.
type logWriter struct {
	l    *slog.Logger
	side string
}

func (w logWriter) Write(p []byte) (int, error) {
	if w.l != nil {
		w.l.Debug("yamux", "side", w.side, "msg", string(p))
	}
	return len(p), nil
}

// maxSocketPath is the longest Unix socket path: 108 bytes with the NUL.
const maxSocketPath = 107

// JobOptions configure the job side.
type JobOptions struct {
	// Dir is the job's relay directory, agentapi.RelayDir. It is created
	// with mode 700 when missing; one that exists must be a directory of this
	// user (it is then set to mode 700). Its parent must exist.
	Dir string
	// Wait is how long a connection waits for a session; zero means
	// DefaultWait.
	Wait time.Duration
	// Logger reports sessions and refused connections; nil means none.
	Logger *slog.Logger
}

// Job is the job side of host-dialed mode: one loopback listener and one
// Unix socket per channel, and each channel's current session.
type Job struct {
	o     JobOptions
	chans map[agentapi.RelayChannel]*channel
	wg    sync.WaitGroup

	closeOnce sync.Once
	closed    chan struct{}
}

// channel is one channel's listeners and session.
type channel struct {
	name  agentapi.RelayChannel
	tcp   net.Listener
	relay net.Listener
	j     *Job

	mu      sync.Mutex
	cur     *yamux.Session
	changed chan struct{} // closed when cur changes
	starts  uint64
	conns   map[net.Conn]struct{}
}

// StartJob makes the relay directory and binds both channels' listeners. The
// caller closes the Job, which also removes the directory's sockets.
func StartJob(o JobOptions) (*Job, error) {
	if o.Wait <= 0 {
		o.Wait = DefaultWait
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if s := agentapi.RelaySocket(o.Dir, agentapi.RelayControl); len(s) > maxSocketPath {
		return nil, fmt.Errorf("the relay socket %s is %d bytes, above the %d a Unix socket path allows", s, len(s), maxSocketPath)
	}
	if err := privateDir(o.Dir, true); err != nil {
		return nil, err
	}
	j := &Job{o: o, chans: map[agentapi.RelayChannel]*channel{}, closed: make(chan struct{})}
	for _, name := range agentapi.RelayChannels {
		c, err := j.bind(name)
		if err != nil {
			_ = j.Close()
			return nil, err
		}
		j.chans[name] = c
	}
	for _, c := range j.chans {
		j.wg.Add(2)
		go c.acceptRelays()
		go c.acceptConns()
	}
	return j, nil
}

// bind binds a channel's loopback listener, on a port the kernel chooses,
// and its Unix socket, mode 600, in the relay directory. A socket left there
// by an earlier agent of the same job is removed first.
func (j *Job) bind(name agentapi.RelayChannel) (*channel, error) {
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("the %s channel's loopback listener: %w", name, err)
	}
	sock := agentapi.RelaySocket(j.o.Dir, name)
	if err := os.Remove(sock); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = tcp.Close()
		return nil, fmt.Errorf("the %s channel's old socket: %w", name, err)
	}
	relay, err := net.Listen("unix", sock)
	if err != nil {
		_ = tcp.Close()
		return nil, fmt.Errorf("the %s channel's relay socket: %w", name, err)
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		_ = tcp.Close()
		_ = relay.Close()
		return nil, fmt.Errorf("the %s channel's relay socket: %w", name, err)
	}
	return &channel{name: name, tcp: tcp, relay: relay, j: j, changed: make(chan struct{}), conns: map[net.Conn]struct{}{}}, nil
}

// Addr is a channel's loopback listener, 127.0.0.1:<port>: the control
// client dials the control channel's, and frpc's serverPort is the serving
// channel's port. It never changes while the Job runs.
func (j *Job) Addr(ch agentapi.RelayChannel) string { return j.chans[ch].tcp.Addr().String() }

// Port is a channel's loopback port.
func (j *Job) Port(ch agentapi.RelayChannel) int { return j.chans[ch].tcp.Addr().(*net.TCPAddr).Port }

// Sessions is how many sessions a channel has had: each relay that connected.
func (j *Job) Sessions(ch agentapi.RelayChannel) uint64 {
	c := j.chans[ch]
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

// Connected reports whether a channel has a live session now.
func (j *Job) Connected(ch agentapi.RelayChannel) bool {
	c := j.chans[ch]
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur != nil && !c.cur.IsClosed()
}

// Close stops both channels: the listeners, every session and every
// connection, and removes the sockets. It waits for the goroutines.
func (j *Job) Close() error {
	j.closeOnce.Do(func() {
		close(j.closed)
		for _, c := range j.chans {
			_ = c.tcp.Close()
			_ = c.relay.Close() // removes the socket file
			c.mu.Lock()
			if c.cur != nil {
				_ = c.cur.Close()
			}
			for conn := range c.conns {
				_ = conn.Close()
			}
			c.mu.Unlock()
		}
	})
	j.wg.Wait()
	return nil
}

// acceptRelays takes each relay that connects to the channel's socket as the
// channel's session, replacing the older one, which it closes (HBEP-6: "A
// newer relay for a channel replaces the older, which the agent closes").
func (c *channel) acceptRelays() {
	defer c.j.wg.Done()
	for {
		conn, err := c.relay.Accept()
		if err != nil {
			return
		}
		if err := peerIsUs(conn); err != nil {
			c.j.o.Logger.Warn("a relay connection was refused", "channel", c.name, "err", err)
			_ = conn.Close()
			continue
		}
		sess, err := yamux.Client(conn, sessionConfig(c.j.o.Logger, "job"))
		if err != nil {
			_ = conn.Close()
			continue
		}
		c.mu.Lock()
		select {
		case <-c.j.closed:
			c.mu.Unlock()
			_ = sess.Close()
			return
		default:
		}
		old := c.cur
		c.cur = sess
		c.starts++
		close(c.changed)
		c.changed = make(chan struct{})
		n := c.starts
		c.mu.Unlock()
		if old != nil {
			_ = old.Close()
		}
		c.j.o.Logger.Info("relay session up", "channel", c.name, "session", n, "replaced", old != nil)
		c.j.wg.Add(1)
		go c.watch(sess, n)
	}
}

// watch forgets a session once it closes, so a connection waits for the next.
func (c *channel) watch(sess *yamux.Session, n uint64) {
	defer c.j.wg.Done()
	select {
	case <-sess.CloseChan():
	case <-c.j.closed:
		return
	}
	c.mu.Lock()
	if c.cur == sess {
		c.cur = nil
		close(c.changed)
		c.changed = make(chan struct{})
	}
	c.mu.Unlock()
	c.j.o.Logger.Info("relay session ended", "channel", c.name, "session", n)
}

// acceptConns serves each connection the channel's loopback listener takes.
func (c *channel) acceptConns() {
	defer c.j.wg.Done()
	for {
		conn, err := c.tcp.Accept()
		if err != nil {
			return
		}
		c.mu.Lock()
		c.conns[conn] = struct{}{}
		c.mu.Unlock()
		c.j.wg.Add(1)
		go func() {
			defer c.j.wg.Done()
			defer func() {
				c.mu.Lock()
				delete(c.conns, conn)
				c.mu.Unlock()
				_ = conn.Close()
			}()
			st, err := c.open()
			if err != nil {
				c.j.o.Logger.Warn("a connection found no relay session and is closed", "channel", c.name, "err", err)
				return
			}
			join(conn, st)
		}()
	}
}

// open opens a stream on the channel's session, waiting up to Wait for one.
// A session that ends between the look and the open is waited past, within
// the same deadline.
func (c *channel) open() (*yamux.Stream, error) {
	deadline := time.NewTimer(c.j.o.Wait)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		sess, changed := c.cur, c.changed
		c.mu.Unlock()
		if sess != nil && !sess.IsClosed() {
			st, err := sess.OpenStream()
			if err == nil {
				return st, nil
			}
		}
		select {
		case <-changed:
		case <-deadline.C:
			return nil, fmt.Errorf("no relay session within %s", c.j.o.Wait)
		case <-c.j.closed:
			return nil, errors.New("the job side is closing")
		}
	}
}

// join copies both ways between a and b, passing each direction's end on as a
// half-close, and returns once both directions have ended.
func join(a, b net.Conn) {
	done := make(chan struct{}, 2)
	copyHalf := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		closeWrite(dst)
		done <- struct{}{}
	}
	go copyHalf(a, b)
	go copyHalf(b, a)
	<-done
	<-done
}

// closeWrite ends dst's writing side: a TCP or Unix half-close, a stream's
// FIN, or a full close for anything else.
func closeWrite(c net.Conn) {
	switch w := c.(type) {
	case interface{ CloseWrite() error }:
		_ = w.CloseWrite()
	case *yamux.Stream:
		_ = w.Close() // yamux's Close is a half-close: it sends FIN and keeps reading
	default:
		_ = c.Close()
	}
}

// privateDir makes dir with mode 700 (create) or checks it, and refuses one
// that is a link, not a directory, or another user's.
func privateDir(dir string, create bool) error {
	if dir == "" {
		return errors.New("no relay directory")
	}
	if create {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("the relay directory: %w", err)
		}
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("the relay directory: %w", err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	switch {
	case !fi.IsDir():
		return fmt.Errorf("the relay directory %s is not a directory", dir)
	case !ok || int(st.Uid) != os.Geteuid():
		return fmt.Errorf("the relay directory %s belongs to another user", dir)
	}
	if fi.Mode().Perm() != 0o700 {
		if !create {
			return fmt.Errorf("the relay directory %s has mode %o, want 700", dir, fi.Mode().Perm())
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("the relay directory: %w", err)
		}
	}
	return nil
}

// peerIsUs refuses a Unix connection from another user. The directory's mode
// already keeps them out; this holds if it is ever loosened.
func peerIsUs(conn net.Conn) error {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return err
	}
	var cred *syscall.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if cerr != nil {
		return cerr
	}
	if int(cred.Uid) != os.Geteuid() {
		return fmt.Errorf("the peer is user %d, not this user", cred.Uid)
	}
	return nil
}

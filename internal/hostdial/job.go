package hostdial

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatedier/yamux"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// DefaultWait is how long a connection accepted while its channel has no
// session waits for one (HBEP-6, Dial-home modes, host-dialed mode: "waits up
// to 30 s for one, then is closed").
const DefaultWait = 30 * time.Second

// Session settings, the same on both ends (HBEP-6, The wire: "a 10 s
// keepalive and a 10 s write timeout"). A keepalive with no answer, or a write
// that blocks, for 10 s ends the session, and the controller starts the relay
// again.
const (
	keepAliveInterval = 10 * time.Second
	writeTimeout      = 10 * time.Second
)

// exchangeTimeout bounds each line of the rendezvous's exchange: the greeting,
// the hello and the answer. Both ends are on the job's node.
const exchangeTimeout = 10 * time.Second

// maxLine bounds a line of the exchange.
const maxLine = 256

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

// DevShm is the socket directory's second choice, when the job's TMPDIR is
// unset or too long for a socket path.
const DevShm = "/dev/shm"

// socketName is the agent's socket in its random directory.
const socketName = "relay.sock"

// JobOptions configure the job side.
type JobOptions struct {
	// TmpDir is the job's TMPDIR, under which the socket's random directory
	// goes; DevShm when it is empty or the socket's path there would pass
	// agentapi.MaxRelaySocketPath.
	TmpDir string
	// AllocDir is the Allocation's directory, where the rendezvous file goes
	// (agentapi.RelaySocketFile).
	AllocDir string
	// Allocation is the Allocation's UID, which the agent's greeting names.
	Allocation string
	// Wait is how long a connection waits for a session; zero means
	// DefaultWait.
	Wait time.Duration
	// Logger reports sessions and refused relays; nil means none.
	Logger *slog.Logger
}

// Job is the job side of host-dialed mode (HBEP-6, The job side): one
// loopback listener per channel, one Unix socket for the relays in a
// directory with a random name, mode 700, whose path is published in the
// Allocation's directory, and each channel's current session with its fence.
type Job struct {
	o      JobOptions
	dir    string // the socket's directory
	sock   string // the socket's path
	relays net.Listener
	chans  map[agentapi.RelayChannel]*channel
	wg     sync.WaitGroup

	closeOnce sync.Once
	closed    chan struct{}
}

// channel is one channel's listener, session and fence.
type channel struct {
	name agentapi.RelayChannel
	tcp  net.Listener
	j    *Job

	mu      sync.Mutex
	cur     *yamux.Session
	changed chan struct{} // closed when cur changes
	starts  uint64
	// fenced is set once a relay was accepted; epoch and seq are its (E, N),
	// the highest seen on this channel.
	fenced     bool
	epoch, seq int64
	refused    uint64
	conns      map[net.Conn]struct{}
}

// StartJob makes the socket's directory and the socket, binds both
// channels' loopback listeners, and publishes the socket's path in the
// Allocation's directory. The caller closes the Job, which also removes the
// directory and the rendezvous file.
func StartJob(o JobOptions) (*Job, error) {
	if o.Wait <= 0 {
		o.Wait = DefaultWait
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.AllocDir == "" || o.Allocation == "" {
		return nil, errors.New("hostdial: the job side needs the Allocation's directory and UID")
	}
	dir, err := socketDir(o.TmpDir)
	if err != nil {
		return nil, err
	}
	j := &Job{o: o, dir: dir, sock: filepath.Join(dir, socketName), chans: map[agentapi.RelayChannel]*channel{}, closed: make(chan struct{})}
	fail := func(err error) (*Job, error) {
		_ = j.Close()
		return nil, err
	}
	if j.relays, err = net.Listen("unix", j.sock); err != nil {
		return fail(fmt.Errorf("the relay socket: %w", err))
	}
	if err := os.Chmod(j.sock, 0o600); err != nil {
		return fail(fmt.Errorf("the relay socket: %w", err))
	}
	for _, name := range agentapi.RelayChannels {
		tcp, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return fail(fmt.Errorf("the %s channel's loopback listener: %w", name, err))
		}
		j.chans[name] = &channel{name: name, tcp: tcp, j: j, changed: make(chan struct{}), conns: map[net.Conn]struct{}{}}
	}
	if err := publish(o.AllocDir, j.sock); err != nil {
		return fail(err)
	}
	j.wg.Add(1)
	go j.acceptRelays()
	for _, c := range j.chans {
		j.wg.Add(1)
		go c.acceptConns()
	}
	return j, nil
}

// socketDir makes the socket's directory, with a random name and mode 700,
// under tmp, or under DevShm when tmp is empty or the socket's path there
// would be too long.
func socketDir(tmp string) (string, error) {
	var errs []error
	for _, root := range []string{tmp, DevShm} {
		if root == "" || !filepath.IsAbs(root) {
			continue
		}
		// os.MkdirTemp appends up to 10 digits.
		if len(filepath.Join(root, "hb-relay-0123456789", socketName)) > agentapi.MaxRelaySocketPath {
			errs = append(errs, fmt.Errorf("the socket under %s would pass %d bytes", root, agentapi.MaxRelaySocketPath))
			continue
		}
		dir, err := os.MkdirTemp(root, "hb-relay-")
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := privateDir(dir, true); err != nil {
			_ = os.Remove(dir)
			errs = append(errs, err)
			continue
		}
		return dir, nil
	}
	return "", fmt.Errorf("hostdial: no directory for the relay socket: %w", errors.Join(errs...))
}

// publish writes the socket's path to the rendezvous file, mode 600, by a
// temporary file in the same directory and a rename.
func publish(allocDir, sock string) error {
	f, err := os.CreateTemp(allocDir, ".relay-socket-*")
	if err != nil {
		return fmt.Errorf("the rendezvous file: %w", err)
	}
	_, werr := f.WriteString(sock + "\n")
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Chmod(f.Name(), 0o600)
	}
	if werr == nil {
		werr = os.Rename(f.Name(), agentapi.RelaySocketFile(allocDir))
	}
	if werr != nil {
		_ = os.Remove(f.Name())
		return fmt.Errorf("the rendezvous file: %w", werr)
	}
	return nil
}

// Addr is a channel's loopback listener, 127.0.0.1:<port>: the control
// client dials the control channel's, and frpc's serverPort is the serving
// channel's port. It never changes while the Job runs.
func (j *Job) Addr(ch agentapi.RelayChannel) string { return j.chans[ch].tcp.Addr().String() }

// Port is a channel's loopback port.
func (j *Job) Port(ch agentapi.RelayChannel) int { return j.chans[ch].tcp.Addr().(*net.TCPAddr).Port }

// Socket is the relay socket's path, as the rendezvous file names it.
func (j *Job) Socket() string { return j.sock }

// Sessions is how many sessions a channel has had: each relay it accepted.
func (j *Job) Sessions(ch agentapi.RelayChannel) uint64 {
	c := j.chans[ch]
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

// Refused is how many relays a channel refused as superseded.
func (j *Job) Refused(ch agentapi.RelayChannel) uint64 {
	c := j.chans[ch]
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refused
}

// Connected reports whether a channel has a live session now.
func (j *Job) Connected(ch agentapi.RelayChannel) bool {
	c := j.chans[ch]
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur != nil && !c.cur.IsClosed()
}

// Close stops both channels: the listeners, every session and every
// connection, and removes the rendezvous file while it still names this
// socket, and the socket's directory. It waits for the goroutines.
func (j *Job) Close() error {
	j.closeOnce.Do(func() {
		close(j.closed)
		if j.relays != nil {
			_ = j.relays.Close() // removes the socket file
		}
		for _, c := range j.chans {
			_ = c.tcp.Close()
			c.mu.Lock()
			if c.cur != nil {
				_ = c.cur.Close()
			}
			for conn := range c.conns {
				_ = conn.Close()
			}
			c.mu.Unlock()
		}
		if b, err := os.ReadFile(agentapi.RelaySocketFile(j.o.AllocDir)); err == nil && strings.TrimSpace(string(b)) == j.sock {
			_ = os.Remove(agentapi.RelaySocketFile(j.o.AllocDir))
		}
		_ = os.RemoveAll(j.dir)
	})
	j.wg.Wait()
	return nil
}

// acceptRelays takes each relay that connects to the socket through the
// exchange (handshake), each on its own goroutine.
func (j *Job) acceptRelays() {
	defer j.wg.Done()
	for {
		conn, err := j.relays.Accept()
		if err != nil {
			return
		}
		j.wg.Add(1)
		go func() {
			defer j.wg.Done()
			j.handshake(conn)
		}()
	}
}

// handshake is the agent's side of the exchange: it refuses another user's
// peer (SO_PEERCRED), greets with the Allocation's UID, reads the relay's
// hello, and answers RelaySuperseded to a relay whose (E, N) is not above the
// channel's highest, or RelayAccepted to one that is, whose session then
// replaces the channel's older one, which is closed (HBEP-6, Fencing).
func (j *Job) handshake(conn net.Conn) {
	log := j.o.Logger
	if err := peerIsUs(conn); err != nil {
		log.Warn("a relay connection was refused", "err", err)
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
	if _, err := io.WriteString(conn, agentapi.RelayGreeting(j.o.Allocation)+"\n"); err != nil {
		_ = conn.Close()
		return
	}
	line, err := readLine(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	ch, epoch, seq, err := agentapi.ParseRelayHello(line)
	if err != nil {
		log.Warn("a relay's hello was refused", "err", err)
		_ = conn.Close()
		return
	}
	c := j.chans[ch]
	c.mu.Lock()
	select {
	case <-j.closed:
		c.mu.Unlock()
		_ = conn.Close()
		return
	default:
	}
	if c.fenced && (epoch < c.epoch || epoch == c.epoch && seq <= c.seq) {
		c.refused++
		c.mu.Unlock()
		log.Info("a relay was refused as superseded", "channel", ch, "epoch", epoch, "seq", seq, "kept", fmt.Sprintf("%d/%d", c.epoch, c.seq))
		_, _ = io.WriteString(conn, agentapi.RelaySuperseded+"\n")
		_ = conn.Close()
		return
	}
	if _, err := io.WriteString(conn, agentapi.RelayAccepted+"\n"); err != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})
	sess, err := yamux.Client(conn, sessionConfig(log, "job"))
	if err != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return
	}
	old := c.cur
	c.cur, c.fenced, c.epoch, c.seq = sess, true, epoch, seq
	c.starts++
	close(c.changed)
	c.changed = make(chan struct{})
	n := c.starts
	c.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	log.Info("relay session up", "channel", ch, "session", n, "epoch", epoch, "seq", seq, "replaced", old != nil)
	select {
	case <-sess.CloseChan():
	case <-j.closed:
		return
	}
	c.mu.Lock()
	if c.cur == sess {
		c.cur = nil
		close(c.changed)
		c.changed = make(chan struct{})
	}
	c.mu.Unlock()
	log.Info("relay session ended", "channel", ch, "session", n)
}

// readLine reads one line, without its newline, a byte at a time, so nothing
// after it is taken from conn.
func readLine(conn net.Conn) (string, error) {
	var b bytes.Buffer
	one := make([]byte, 1)
	for b.Len() < maxLine {
		if _, err := io.ReadFull(conn, one); err != nil {
			return "", err
		}
		if one[0] == '\n' {
			return b.String(), nil
		}
		b.WriteByte(one[0])
	}
	return "", fmt.Errorf("a line longer than %d bytes", maxLine)
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
			st, gone, err := c.open()
			if err != nil {
				c.j.o.Logger.Warn("a connection found no relay session and is closed", "channel", c.name, "err", err)
				return
			}
			join(conn, st, gone)
		}()
	}
}

// open opens a stream on the channel's session, waiting up to Wait for one,
// and returns it with the session's close channel. A session that ends
// between the look and the open is waited past, within the same deadline.
func (c *channel) open() (*yamux.Stream, <-chan struct{}, error) {
	deadline := time.NewTimer(c.j.o.Wait)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		sess, changed := c.cur, c.changed
		c.mu.Unlock()
		if sess != nil && !sess.IsClosed() {
			st, err := sess.OpenStream()
			if err == nil {
				return st, sess.CloseChan(), nil
			}
		}
		select {
		case <-changed:
		case <-deadline.C:
			return nil, nil, fmt.Errorf("no relay session within %s", c.j.o.Wait)
		case <-c.j.closed:
			return nil, nil, errors.New("the job side is closing")
		}
	}
}

// join copies both ways between a and b, passing each direction's end on as a
// half-close, and returns once both directions have ended. When gone is
// closed (the session ended), it closes both, so a copy blocked on a peer
// that sends nothing (a long poll, an idle tunnel) ends too.
func join(a, b net.Conn, gone <-chan struct{}) {
	done := make(chan struct{}, 2)
	copyHalf := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		closeWrite(dst)
		done <- struct{}{}
	}
	go copyHalf(a, b)
	go copyHalf(b, a)
	for n := 0; n < 2; {
		select {
		case <-done:
			n++
		case <-gone:
			_ = a.Close()
			_ = b.Close()
			gone = nil
		}
	}
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

// peerUID reads a Unix connection's peer's user; a test replaces it to show
// that another user's peer is refused.
var peerUID = func(conn net.Conn) (int, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return os.Geteuid(), nil
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if cerr != nil {
		return 0, cerr
	}
	return int(cred.Uid), nil
}

// peerIsUs refuses a Unix connection from another user (SO_PEERCRED).
func peerIsUs(conn net.Conn) error {
	uid, err := peerUID(conn)
	if err != nil {
		return err
	}
	if uid != os.Geteuid() {
		return fmt.Errorf("the peer is user %d, not this user", uid)
	}
	return nil
}

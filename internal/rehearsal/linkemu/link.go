// Package linkemu is a shaped link on loopback for the rehearsal's bulk-path tests (HBEP-6, Test
// plan, The bulk path: "on loopback through S17's link, in its fair mode, at 48 MB/s and a 2.3 ms
// round trip"; #448, part 7).
//
// It is spike S17's link emulator in its fair mode (spikes/s17/cmd/s17link/main.go, this repository,
// commit 5b4e76a6, issue #413), run in process instead of as a command: the bucket, the slack and the
// pipe are S17's code, and the FIFO queue mode, the statistics file and the flags are left out. S17's
// s17link is itself spike S1c's shared-rate shaper (spikes/s1c/cmd/s1cbench/shaper.go) with a one-way
// delay added.
//
// Each direction of the link has one rate that every connection shares. A chunk read from one side
// waits for its turn on the link, takes n/rate there, and is delivered delay later. Each connection
// direction has at most one chunk waiting, so connections take turns, as at a fair-queueing router:
// S17's "fair" discipline. An end of stream is passed on as a half-close after every chunk before it.
package linkemu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Config is a link: a rate in each direction, a one-way delay, the largest chunk sent at once, and
// the receive buffer of the link's own sockets.
type Config struct {
	UpRate, DownRate float64       // bytes per second; 0 is unlimited. Up is from the connecting side.
	Delay            time.Duration // one-way delay added in each direction
	Chunk            int           // largest piece read and sent at once; 0 is 16 KiB (S17's)
	RcvBuf           int           // SO_RCVBUF of the link's sockets; 0 is 256 KiB (S17's), negative the kernel's
}

// S17 is the link HBEP-6's bulk-path test plan names: 48 MB/s each way and a 2.3 ms round trip.
var S17 = Config{UpRate: 48e6, DownRate: 48e6, Delay: 1150 * time.Microsecond}

// bucket is one direction of the link: a fixed rate shared by every connection that uses it (S17's).
type bucket struct {
	mu   sync.Mutex
	rate float64 // bytes per second; 0 = unlimited
	next time.Time
	// slack is how far the link's schedule may lag behind the clock: a sender that slept past its
	// chunk's time (a sleep's overshoot, about a millisecond on a loaded host) catches up by that
	// much instead of losing it, so the link keeps its rate; after an idle stretch, at most slack's
	// worth of bytes goes at once.
	slack time.Duration
}

// defaultSlack bounds a catch-up: 2 ms is about 96 kB at 48 MB/s.
const defaultSlack = 2 * time.Millisecond

// reserve books n bytes on the link at now and returns when they have been sent: after any chunk
// already booked, n/rate later.
func (b *bucket) reserve(now time.Time, n int) time.Time {
	if b.rate <= 0 {
		return now
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if earliest := now.Add(-b.slack); b.next.Before(earliest) {
		b.next = earliest
	}
	b.next = b.next.Add(time.Duration(float64(n) / b.rate * float64(time.Second)))
	return b.next
}

// Link is a running link: each Listen is one listening side whose connections are carried to their
// upstream through it. Every map of a Link shares its two buckets, as one physical link does.
type Link struct {
	up, down *bucket
	delay    time.Duration
	chunk    int
	rcvbuf   int

	mu     sync.Mutex
	lns    []net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	// Bytes carried, for a test's check that its traffic took the link.
	Up, Down atomic.Int64
}

// New returns a link with no listener yet.
func New(c Config) (*Link, error) {
	if c.UpRate < 0 || c.DownRate < 0 || c.Delay < 0 || c.Chunk < 0 {
		return nil, errors.New("linkemu: no negative rate, delay or chunk")
	}
	if c.Chunk == 0 {
		c.Chunk = 16 * 1024
	}
	if c.RcvBuf == 0 {
		c.RcvBuf = 256 * 1024
	}
	return &Link{up: &bucket{rate: c.UpRate, slack: defaultSlack}, down: &bucket{rate: c.DownRate, slack: defaultSlack},
		delay: c.Delay, chunk: c.Chunk, rcvbuf: c.RcvBuf, conns: map[net.Conn]struct{}{}}, nil
}

// Listen opens a listener on a loopback port the kernel chooses and carries each connection to it
// to upstream, which must be on 127.0.0.1 too. It returns the listener's address.
func (l *Link) Listen(upstream string) (string, error) {
	if err := requireLoopback(upstream); err != nil {
		return "", err
	}
	lc := net.ListenConfig{Control: l.control}
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		ln.Close()
		return "", errors.New("linkemu: the link is closed")
	}
	l.lns = append(l.lns, ln)
	l.wg.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				l.serve(c, upstream)
			}()
		}
	}()
	return ln.Addr().String(), nil
}

// Close stops every listener, cuts every connection, and waits for them.
func (l *Link) Close() {
	l.mu.Lock()
	l.closed = true
	for _, ln := range l.lns {
		ln.Close()
	}
	for c := range l.conns {
		c.Close()
	}
	l.mu.Unlock()
	l.wg.Wait()
}

// control bounds the receive buffer of every socket the link opens, so a sender's own queue builds
// in its own socket buffer, as before a real bottleneck, not in the link.
func (l *Link) control(_, _ string, rc syscall.RawConn) error {
	if l.rcvbuf <= 0 {
		return nil
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, l.rcvbuf)
	}); err != nil {
		return err
	}
	return serr
}

func (l *Link) track(cs ...net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	for _, c := range cs {
		l.conns[c] = struct{}{}
	}
	return true
}

func (l *Link) untrack(cs ...net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range cs {
		delete(l.conns, c)
	}
}

func (l *Link) serve(c net.Conn, upstream string) {
	d := net.Dialer{Timeout: 5 * time.Second, Control: l.control}
	u, err := d.Dial("tcp4", upstream)
	if err != nil {
		c.Close()
		return
	}
	if !l.track(c, u) {
		c.Close()
		u.Close()
		return
	}
	defer l.untrack(c, u)
	for _, x := range []net.Conn{c, u} {
		_ = x.(*net.TCPConn).SetNoDelay(true)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); l.pipe(u, c, l.up, &l.Up) }()
	go func() { defer wg.Done(); l.pipe(c, u, l.down, &l.Down) }()
	wg.Wait()
	c.Close()
	u.Close()
}

type piece struct {
	b  []byte
	at time.Time
}

// pipe copies src to dst through one direction of the link: each chunk waits for its turn on the
// link, is sent at the link's rate, and is delivered delay after it was sent. An end of stream is
// passed on as a half-close, after every chunk before it (S17's pipe, in fair mode).
func (l *Link) pipe(dst, src net.Conn, b *bucket, count *atomic.Int64) {
	line := make(chan piece, 256)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range line {
			time.Sleep(time.Until(p.at))
			if _, err := dst.Write(p.b); err != nil {
				for range line { // drain, so the reader never blocks
				}
				return
			}
		}
	}()
	buf := make([]byte, l.chunk)
	var rerr error
	for {
		n, err := src.Read(buf)
		if n > 0 {
			read := time.Now()
			sent := b.reserve(read, n)
			time.Sleep(time.Until(sent))
			// A chunk sent on a schedule that lags the clock (catching up) still arrives no
			// sooner than delay after it was read.
			if sent.Before(read) {
				sent = read
			}
			line <- piece{b: append([]byte(nil), buf[:n]...), at: sent.Add(l.delay)}
			count.Add(int64(n))
		}
		if err != nil {
			rerr = err
			break
		}
	}
	close(line)
	<-done
	if errors.Is(rerr, io.EOF) {
		_ = dst.(*net.TCPConn).CloseWrite()
	} else {
		dst.Close()
	}
}

func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("linkemu: address %q: %w", addr, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("linkemu: address %q is not on 127.0.0.1 (loopback only)", addr)
	}
	return nil
}

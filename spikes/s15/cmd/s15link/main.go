// Command s15link is spike S15's link emulator: spike S1c's shared-rate shaper
// (spikes/s1c/cmd/s1cbench/shaper.go, at commit 3f863bad of this repository)
// with a fixed one-way delay added per direction, a bounded receive buffer of
// its own, and per-connection statistics.
//
//	s15link -map 127.0.0.1:A=127.0.0.1:B [-map ...] -up-rate R -down-rate R -delay D -rcvbuf N -stats F
//
// "Up" is from the connecting side to the upstream, "down" the reverse; every
// connection of every map shares one rate per direction, as one link does. A
// chunk of n bytes occupies the link for n/rate, connections taking turns (at
// most one chunk of each waits for the link: per-connection queues, as in a
// fair-queueing router, S1c's choice), and is delivered delay later. The
// emulator's receive buffer is bounded (-rcvbuf, SO_RCVBUF on the listening
// and the dialed sockets), so a sender's queue builds in its own socket buffer,
// as it would before a real bottleneck, not in the emulator.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// bucket is one direction of the link: a fixed rate shared by every
// connection that uses it.
type bucket struct {
	mu   sync.Mutex
	rate float64 // bytes per second; 0 = unlimited
	next time.Time
	// slack is how far the link's schedule may lag behind the clock: a sender
	// that slept past its chunk's time (a sleep's overshoot, about a
	// millisecond on a loaded host) catches up by that much instead of losing
	// it, so the link keeps its rate; after an idle stretch, at most slack's
	// worth of bytes goes at once.
	slack time.Duration
}

// defaultSlack bounds a catch-up: 2 ms is about 96 kB at 48 MB/s.
const defaultSlack = 2 * time.Millisecond

// reserve books n bytes on the link at now and returns when they have been
// sent: after any chunk already booked, n/rate later.
func (b *bucket) reserve(now time.Time, n int) time.Time {
	if b == nil || b.rate <= 0 {
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

// connStats are one connection's counts, for the stats file.
type connStats struct {
	ID       int64  `json:"id"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	OpenNs   int64  `json:"open_unix_ns"`
	CloseNs  int64  `json:"close_unix_ns"` // 0 while open
	Up       int64  `json:"bytes_up"`
	Down     int64  `json:"bytes_down"`
}

type link struct {
	up, down *bucket
	delay    time.Duration
	chunk    int
	rcvbuf   int

	mu    sync.Mutex
	conns []*liveConn
	ids   atomic.Int64
}

type liveConn struct {
	s        connStats
	up, down atomic.Int64
	closed   atomic.Int64
}

func main() {
	var maps multiFlag
	flag.Var(&maps, "map", "listen=upstream, both 127.0.0.1:port (repeatable)")
	upRate := flag.Float64("up-rate", 0, "bytes/s shared by all connections, connecting side to upstream (0 = unlimited)")
	downRate := flag.Float64("down-rate", 0, "bytes/s shared by all connections, upstream to connecting side (0 = unlimited)")
	delay := flag.Duration("delay", 0, "one-way delay added in each direction")
	chunk := flag.Int("chunk", 16*1024, "largest piece read and sent at once")
	rcvbuf := flag.Int("rcvbuf", 256*1024, "SO_RCVBUF of the emulator's own sockets (0: the kernel's)")
	statsPath := flag.String("stats", "", "where to write per-connection statistics, every 2 s and at exit")
	flag.Parse()
	if err := run(maps, *upRate, *downRate, *delay, *chunk, *rcvbuf, *statsPath); err != nil {
		fmt.Fprintln(os.Stderr, "s15link:", err)
		os.Exit(1)
	}
}

func run(maps []string, upRate, downRate float64, delay time.Duration, chunk, rcvbuf int, statsPath string) error {
	if len(maps) == 0 || chunk < 1 || delay < 0 || upRate < 0 || downRate < 0 {
		return errors.New("want at least one -map, -chunk >= 1 and no negative rate or delay")
	}
	l := newLink(upRate, downRate, delay, chunk, rcvbuf)
	errc := make(chan error, len(maps))
	for _, m := range maps {
		listen, upstream, ok := strings.Cut(m, "=")
		if !ok {
			return fmt.Errorf("-map %q: want listen=upstream", m)
		}
		if _, err := l.listen(listen, upstream, errc); err != nil {
			return err
		}
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case err := <-errc:
			_ = l.writeStats(statsPath)
			return err
		case <-tick.C:
			_ = l.writeStats(statsPath)
		case <-term:
			return l.writeStats(statsPath)
		}
	}
}

func newLink(upRate, downRate float64, delay time.Duration, chunk, rcvbuf int) *link {
	return &link{up: &bucket{rate: upRate, slack: defaultSlack}, down: &bucket{rate: downRate, slack: defaultSlack}, delay: delay, chunk: chunk, rcvbuf: rcvbuf}
}

// listen serves one map: each connection to listen is carried to upstream
// through the link. An accept that fails ends the map and is sent to errc.
func (l *link) listen(listen, upstream string, errc chan<- error) (net.Listener, error) {
	for _, a := range []string{listen, upstream} {
		if err := requireLoopback(a); err != nil {
			return nil, err
		}
	}
	lc := net.ListenConfig{Control: l.control}
	ln, err := lc.Listen(context.Background(), "tcp4", listen)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				errc <- err
				return
			}
			go l.serve(c, listen, upstream)
		}
	}()
	return ln, nil
}

// control bounds the receive buffer of every socket the emulator opens.
func (l *link) control(_, _ string, rc syscall.RawConn) error {
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

func (l *link) serve(c net.Conn, listen, upstream string) {
	d := net.Dialer{Timeout: 5 * time.Second, Control: l.control}
	u, err := d.Dial("tcp4", upstream)
	if err != nil {
		c.Close()
		return
	}
	for _, x := range []net.Conn{c, u} {
		_ = x.(*net.TCPConn).SetNoDelay(true)
	}
	lc := &liveConn{s: connStats{ID: l.ids.Add(1), Listen: listen, Upstream: upstream, OpenNs: time.Now().UnixNano()}}
	l.mu.Lock()
	l.conns = append(l.conns, lc)
	l.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); l.pipe(u, c, l.up, &lc.up) }()
	go func() { defer wg.Done(); l.pipe(c, u, l.down, &lc.down) }()
	wg.Wait()
	c.Close()
	u.Close()
	lc.closed.Store(time.Now().UnixNano())
}

type piece struct {
	b  []byte
	at time.Time
}

// pipe copies src to dst through one direction of the link: each chunk waits
// for its turn on the link, is sent at the link's rate, and is delivered delay
// after it was sent. An end of stream is passed on as a half-close, after
// every chunk before it.
func (l *link) pipe(dst, src net.Conn, b *bucket, count *atomic.Int64) {
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
			// A chunk sent on a schedule that lags the clock (catching up) still
			// arrives no sooner than delay after it was read.
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

// writeStats writes every connection's counts to path through a temporary
// file and a rename.
func (l *link) writeStats(path string) error {
	if path == "" {
		return nil
	}
	l.mu.Lock()
	out := make([]connStats, 0, len(l.conns))
	for _, c := range l.conns {
		s := c.s
		s.Up, s.Down, s.CloseNs = c.up.Load(), c.down.Load(), c.closed.Load()
		out = append(out, s)
	}
	l.mu.Unlock()
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s15link-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q: %w", addr, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("address %q is not on 127.0.0.1 (loopback only)", addr)
	}
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

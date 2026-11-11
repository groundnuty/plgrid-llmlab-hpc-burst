// Command s17link is spike S17's link emulator: spike S1c's shared-rate shaper
// (spikes/s1c/cmd/s1cbench/shaper.go, at commit 3f863bad of this repository)
// with a fixed one-way delay added per direction, a bounded receive buffer of
// its own, per-connection statistics (spike S15's s15link), and a second queue
// discipline, a single FIFO (S17).
//
//	s17link -map 127.0.0.1:A=127.0.0.1:B [-map ...] -up-rate R -down-rate R -delay D -rcvbuf N -stats F
//	        [-queue fair|fifo] [-queue-bytes N] [-queue-stats F]
//
// "Up" is from the connecting side to the upstream, "down" the reverse; every
// connection of every map shares one rate per direction, as one link does. A
// chunk of n bytes occupies the link for n/rate and is delivered delay later.
// A map's listen address may use port 0: the kernel chooses, and the chosen
// address is written to -addrs.
//
// -queue fair (the default, S15's and S1c's): connections take turns, and at
// most one chunk of each waits for the link, as in a fair-queueing router. A
// small flow's chunk waits behind at most one chunk of every other flow, so a
// bulk flow cannot delay a token flow by more than a chunk's time. That
// flatters a layout that puts the bulk bytes on a connection of their own.
//
// -queue fifo: one bounded byte queue per direction that every connection's
// chunks join in arrival order, with no per-flow fairness, as at a plain
// drop-tail router's buffer. The queue holds at most -queue-bytes, a chunk in
// transmission included; a reader whose chunk does not fit waits for room, in
// arrival order among the waiting, which is the backpressure that stands for a
// sender's loss and slowdown on loopback. A small flow's chunk then waits behind
// everything queued before it: about -queue-bytes over the rate when a bulk
// flow keeps the queue full.
//
// The emulator's receive buffer is bounded (-rcvbuf, SO_RCVBUF on the listening
// and the dialed sockets), so a sender's own queue builds in its own socket
// buffer, as it would before a real bottleneck, not in the emulator.
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
	// upQ and downQ are the FIFO queues of -queue fifo (nil in fair mode).
	upQ, downQ *fifoQueue
	delay      time.Duration
	chunk      int
	rcvbuf     int

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
	queue := flag.String("queue", "fair", "queue discipline: fair (per-connection turns) or fifo (one shared FIFO, with no per-flow fairness)")
	queueBytes := flag.Int("queue-bytes", 0, "fifo: the queue's size in bytes per direction, a chunk in transmission included (at least -chunk)")
	queueStats := flag.String("queue-stats", "", "fifo: where to write the queues' statistics (max occupancy, time full), every 2 s and at exit")
	addrs := flag.String("addrs", "", "where to write the listen addresses actually bound (JSON, one per -map, in order), once they are")
	flag.Parse()
	if err := run(maps, *upRate, *downRate, *delay, *chunk, *rcvbuf, *statsPath, *queue, *queueBytes, *queueStats, *addrs); err != nil {
		fmt.Fprintln(os.Stderr, "s17link:", err)
		os.Exit(1)
	}
}

func run(maps []string, upRate, downRate float64, delay time.Duration, chunk, rcvbuf int, statsPath, queue string, queueBytes int, queueStats, addrsPath string) error {
	if len(maps) == 0 || chunk < 1 || delay < 0 || upRate < 0 || downRate < 0 {
		return errors.New("want at least one -map, -chunk >= 1 and no negative rate or delay")
	}
	l := newLink(upRate, downRate, delay, chunk, rcvbuf)
	switch queue {
	case "fair":
		if queueBytes != 0 {
			return errors.New("-queue-bytes applies to -queue fifo")
		}
	case "fifo":
		if upRate <= 0 || downRate <= 0 {
			return errors.New("-queue fifo needs a rate in each direction: an unlimited link has no queue")
		}
		if queueBytes < chunk {
			return fmt.Errorf("-queue-bytes %d is below the chunk size %d", queueBytes, chunk)
		}
		l.setFIFO(queueBytes)
	default:
		return fmt.Errorf("-queue %q: want fair or fifo", queue)
	}
	errc := make(chan error, len(maps))
	var bound []string
	for _, m := range maps {
		listen, upstream, ok := strings.Cut(m, "=")
		if !ok {
			return fmt.Errorf("-map %q: want listen=upstream", m)
		}
		ln, err := l.listen(listen, upstream, errc)
		if err != nil {
			return err
		}
		bound = append(bound, ln.Addr().String())
	}
	if addrsPath != "" {
		if err := writeJSON(addrsPath, bound); err != nil {
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
			_ = l.writeQueueStats(queueStats)
			return err
		case <-tick.C:
			_ = l.writeStats(statsPath)
			_ = l.writeQueueStats(queueStats)
		case <-term:
			err := l.writeStats(statsPath)
			if qerr := l.writeQueueStats(queueStats); err == nil {
				err = qerr
			}
			return err
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
	if q := l.queueOf(b); q != nil {
		l.pipeFIFO(dst, src, q, count)
		return
	}
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
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s17link-*")
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

// setFIFO puts both directions on a FIFO queue of n bytes each.
func (l *link) setFIFO(n int) {
	l.upQ, l.downQ = newFIFO(l.up, n, l.chunk, l.delay), newFIFO(l.down, n, l.chunk, l.delay)
}

// queueOf is the FIFO queue of one direction's bucket, nil in fair mode.
func (l *link) queueOf(b *bucket) *fifoQueue {
	switch b {
	case l.up:
		return l.upQ
	case l.down:
		return l.downQ
	}
	return nil
}

// flow is one connection's one direction through a FIFO queue: the line its
// chunks are delivered on, in order, and the chunks of it still in the queue.
type flow struct {
	line chan piece
	pend sync.WaitGroup
}

// qitem is a chunk in the queue.
type qitem struct {
	b    []byte
	read time.Time // when the reader had it, before it waited for room
	fl   *flow
}

type waiter struct {
	it *qitem
	ch chan struct{}
}

// fifoQueue is one direction of the link in -queue fifo mode. Chunks are
// admitted in the order their readers asked, while there is room, and sent in
// the order admitted, at the bucket's rate, by one drain goroutine: no flow
// has a queue of its own, so none is served ahead of another.
type fifoQueue struct {
	b     *bucket
	size  int
	chunk int
	delay time.Duration

	mu      sync.Mutex
	occ     int // bytes queued, the one in transmission included
	waiters []*waiter
	items   []*qitem
	wake    chan struct{}
	// Statistics.
	maxOcc  int
	last    time.Time
	fullNs  int64 // time with less room than one chunk
	busyNs  int64 // time with anything queued
	chunks  int64
	bytes   int64
	waitMax time.Duration // longest time a chunk waited from read to sent
}

func newFIFO(b *bucket, size, chunk int, delay time.Duration) *fifoQueue {
	q := &fifoQueue{b: b, size: size, chunk: chunk, delay: delay, wake: make(chan struct{}, 1), last: time.Now()}
	go q.drain()
	return q
}

// account adds the time since the last change to the busy and full totals;
// the caller holds mu.
func (q *fifoQueue) account(now time.Time) {
	dt := now.Sub(q.last).Nanoseconds()
	if dt > 0 {
		if q.occ > 0 {
			q.busyNs += dt
		}
		if q.size-q.occ < q.chunk {
			q.fullNs += dt
		}
	}
	q.last = now
}

// admit returns once it has put the chunk in the queue, after every chunk
// that asked before it. The chunk may not exceed the queue's size.
func (q *fifoQueue) admit(it *qitem) {
	w := &waiter{it: it, ch: make(chan struct{})}
	q.mu.Lock()
	q.waiters = append(q.waiters, w)
	q.pump()
	q.mu.Unlock()
	<-w.ch
}

// pump admits waiting chunks, oldest first, while the oldest fits; the caller
// holds mu. A chunk that does not fit holds back those behind it: arrival
// order, as a FIFO's.
func (q *fifoQueue) pump() {
	now := time.Now()
	for len(q.waiters) > 0 && q.occ+len(q.waiters[0].it.b) <= q.size {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.account(now)
		q.occ += len(w.it.b)
		q.maxOcc = max(q.maxOcc, q.occ)
		q.items = append(q.items, w.it)
		close(w.ch)
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

// drain sends the queue's chunks one after another at the bucket's rate.
func (q *fifoQueue) drain() {
	for {
		q.mu.Lock()
		if len(q.items) == 0 {
			q.mu.Unlock()
			<-q.wake
			continue
		}
		it := q.items[0]
		q.items = q.items[1:]
		q.mu.Unlock()

		sent := q.b.reserve(time.Now(), len(it.b))
		time.Sleep(time.Until(sent))
		// A chunk sent on a schedule that lags the clock (catching up) still
		// arrives no sooner than delay after it was read.
		if sent.Before(it.read) {
			sent = it.read
		}
		q.mu.Lock()
		q.account(time.Now())
		q.occ -= len(it.b)
		q.chunks++
		q.bytes += int64(len(it.b))
		q.waitMax = max(q.waitMax, time.Since(it.read))
		q.pump()
		q.mu.Unlock()
		it.fl.line <- piece{b: it.b, at: sent.Add(q.delay)}
		it.fl.pend.Done()
	}
}

// queueStat is one queue's statistics for the stats file.
type queueStat struct {
	Mode      string  `json:"mode"`
	SizeBytes int     `json:"queue_bytes"`
	MaxBytes  int     `json:"max_occupancy_bytes"`
	FullS     float64 `json:"seconds_full"` // with less room than one chunk
	BusyS     float64 `json:"seconds_busy"` // with anything queued
	Chunks    int64   `json:"chunks"`
	Bytes     int64   `json:"bytes"`
	WaitMaxMs float64 `json:"chunk_wait_max_ms"` // from read to sent, the queue's wait and the wait for room
}

func (q *fifoQueue) stat() queueStat {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.account(time.Now())
	return queueStat{Mode: "fifo", SizeBytes: q.size, MaxBytes: q.maxOcc, FullS: float64(q.fullNs) / 1e9, BusyS: float64(q.busyNs) / 1e9,
		Chunks: q.chunks, Bytes: q.bytes, WaitMaxMs: float64(q.waitMax) / 1e6}
}

// pipeFIFO is pipe for a FIFO direction: each chunk read joins the shared
// queue (waiting for room, in turn), and is delivered delay after it was sent.
func (l *link) pipeFIFO(dst, src net.Conn, q *fifoQueue, count *atomic.Int64) {
	fl := &flow{line: make(chan piece, 1024)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range fl.line {
			time.Sleep(time.Until(p.at))
			if _, err := dst.Write(p.b); err != nil {
				for range fl.line { // drain, so the queue never blocks on this flow
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
			fl.pend.Add(1)
			q.admit(&qitem{b: append([]byte(nil), buf[:n]...), read: read, fl: fl})
			count.Add(int64(n))
		}
		if err != nil {
			rerr = err
			break
		}
	}
	fl.pend.Wait() // every chunk of this flow has been sent
	close(fl.line)
	<-done
	if errors.Is(rerr, io.EOF) {
		_ = dst.(*net.TCPConn).CloseWrite()
	} else {
		dst.Close()
	}
}

// writeQueueStats writes both queues' statistics to path through a temporary
// file and a rename; nothing in fair mode.
func (l *link) writeQueueStats(path string) error {
	if path == "" || l.upQ == nil {
		return nil
	}
	return writeJSON(path, map[string]queueStat{"up": l.upQ.stat(), "down": l.downQ.stat()})
}

// writeJSON writes v to path through a temporary file and a rename.
func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s17link-*")
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

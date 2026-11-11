// Command s16link is spike S16's link emulator: a copy of spikes/s15/cmd/s15link
// at this repository's commit eaa225ed, with a discipline switch and a
// per-second series added. s15link is spike S1c's shared-rate shaper
// (spikes/s1c/cmd/s1cbench/shaper.go, at commit 3f863bad of this repository)
// with a fixed one-way delay per direction, a bounded receive buffer of its
// own, and per-connection statistics.
//
//	s16link -map 127.0.0.1:A=127.0.0.1:B [-map ...] -up-rate R -down-rate R -delay D -rcvbuf N
//	        [-discipline fair|fifo] [-buffer-up D -buffer-down D] [-stats F] [-series F]
//
// "Up" is from the connecting side to the upstream, "down" the reverse; every
// connection of every map shares one rate per direction, as one link does. A
// chunk of n bytes occupies the link for n/rate and is delivered delay later.
// A rate of 0 means unlimited in either discipline: no schedule, no buffer,
// no wait. The emulator's receive buffer is bounded (-rcvbuf, SO_RCVBUF on
// the listening and the dialed sockets), so a sender's queue builds in its own
// socket buffer, as it would before a real bottleneck, not in the emulator.
//
// Two disciplines (-discipline):
//
//	fair  (default, s15link's behaviour): each connection reads one chunk, books
//	      it on the direction's shared schedule, sleeps until it is sent, then
//	      reads the next. A connection has at most one chunk waiting, so
//	      connections take turns, as in a fair-queueing router (S1c's choice).
//	fifo  one shared queue per direction with a byte capacity, -buffer-up and
//	      -buffer-down, each given as a duration of that direction's rate
//	      (100ms at 10 MB/s is 1,000,000 bytes). Each connection's reader reads a
//	      chunk, waits until the buffer has room for it (in arrival order across
//	      all connections: a later arrival never overtakes an earlier one; nothing
//	      is dropped, since the emulator terminates TCP), books it on the schedule
//	      at that moment, hands it to the connection's own writer and reads the
//	      next chunk at once. The writer sends chunks in order at their booked time
//	      plus -delay; a chunk's bytes leave the buffer at its booked send time,
//	      before the delay. So one backlogged connection can book up to the buffer
//	      ahead, and a small chunk arriving on another connection waits behind all
//	      of it, up to buffer/rate: a FIFO bottleneck's head-of-line blocking. A
//	      chunk larger than the whole buffer is admitted alone, when the buffer is
//	      empty. -buffer-up and -buffer-down are required in fifo for a direction
//	      with a rate, and refused in fair, where they would do nothing.
//
// -series F appends, every elapsed second since the emulator started, one JSON
// line to F, and a last one at exit (SIGTERM or SIGINT):
//
//	{"t": unix seconds, "up_bytes": N, "down_bytes": N, "up_queue_max": N, "down_queue_max": N, "conns": N}
//
// up_bytes and down_bytes are the bytes whose booked send time (the end of the
// chunk's slot on the schedule) fell in the row's second, so they count what
// the link carried, not what was read; a chunk booked ahead in fifo counts in
// the second it will be sent. The last row also takes every byte still booked
// ahead, so the rows' sum is every byte booked. *_queue_max is the largest
// number of bytes waiting for the link during the second: in fifo the buffer's
// content, in fair the chunks booked and not yet sent. conns is the number of
// open connections when the row is written.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
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

// bucket is one direction of the link's schedule: a fixed rate shared by every
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

// direction is one direction of the link: its schedule, in fifo the shared
// buffer in front of it, and the per-second counts the series reports.
type direction struct {
	b        *bucket
	fifo     bool
	capBytes int64 // fifo: the buffer's capacity
	epoch    time.Time
	clock    func() time.Time // time.Now; a test's fake clock

	mu      sync.Mutex
	track   bool            // counting for the series
	waiting int64           // bytes booked and not yet sent (fifo: the buffer's content)
	queue   []*ticket       // fifo: arrivals waiting for room, oldest first
	cur     int64           // the second being counted, since epoch
	qmax    int64           // the largest waiting seen in second cur
	closed  map[int64]int64 // finished seconds' largest waiting, until written as a row
	booked  map[int64]int64 // second -> bytes whose booked send time falls in it
	next    int64           // the first second not yet written as a row
}

// ticket is one arrival waiting for room in the fifo buffer.
type ticket struct {
	n     int64
	sent  time.Time
	ready chan struct{}
}

func newDirection(rate float64, isFIFO bool, capBytes int64, epoch time.Time) *direction {
	return &direction{
		b: &bucket{rate: rate, slack: defaultSlack}, fifo: isFIFO, capBytes: capBytes,
		epoch: epoch, clock: time.Now,
		closed: map[int64]int64{}, booked: map[int64]int64{},
	}
}

func (d *direction) limited() bool { return d.b.rate > 0 }

// index is the second of t since the epoch.
func (d *direction) index(t time.Time) int64 {
	if !t.After(d.epoch) {
		return 0
	}
	return int64(t.Sub(d.epoch) / time.Second)
}

// advance closes the seconds that have ended by now: each keeps the largest
// content it saw, and the next starts with the content it inherits. d.mu held.
func (d *direction) advance(now time.Time) {
	if !d.track {
		return
	}
	for idx := d.index(now); d.cur < idx; {
		d.closed[d.cur] = d.qmax
		d.cur++
		d.qmax = d.waiting
	}
}

// fits reports whether a chunk of n bytes may enter the buffer: it fits, or the
// buffer is empty (a chunk larger than the buffer goes through alone).
func (d *direction) fits(n int64) bool { return d.waiting == 0 || d.waiting+n <= d.capBytes }

// admit books n bytes on the schedule now and counts them as waiting. d.mu held.
func (d *direction) admit(n int64) time.Time {
	now := d.clock()
	d.advance(now)
	sent := d.b.reserve(now, int(n))
	d.waiting += n
	d.qmax = max(d.qmax, d.waiting)
	if d.track {
		// A chunk booked slightly in the past (slack) or whose second has
		// already been written goes into the first row still to be written, so
		// no byte is lost or counted twice.
		d.booked[max(d.index(sent), d.next)] += n
	}
	return sent
}

// book returns when n bytes will have been sent on the link. In fifo it first
// waits, in arrival order, until the buffer has room; the bytes are booked at
// that moment. A rate of 0 is unlimited: no wait, no buffer.
func (d *direction) book(n int) time.Time {
	d.mu.Lock()
	if !d.limited() {
		now := d.clock()
		d.advance(now)
		if d.track {
			d.booked[max(d.index(now), d.next)] += int64(n)
		}
		d.mu.Unlock()
		return now
	}
	if d.fifo && (len(d.queue) > 0 || !d.fits(int64(n))) {
		t := &ticket{n: int64(n), ready: make(chan struct{})}
		d.queue = append(d.queue, t)
		d.mu.Unlock()
		<-t.ready
		return t.sent
	}
	sent := d.admit(int64(n))
	d.mu.Unlock()
	return sent
}

// release takes n sent bytes out of the buffer and admits, in order, the
// arrivals that now fit.
func (d *direction) release(n int) {
	if !d.limited() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.advance(d.clock())
	d.waiting -= int64(n)
	for len(d.queue) > 0 && d.fits(d.queue[0].n) {
		t := d.queue[0]
		d.queue[0] = nil
		d.queue = d.queue[1:]
		t.sent = d.admit(t.n)
		close(t.ready)
	}
}

// startTracking begins the counts for the series.
func (d *direction) startTracking() {
	d.mu.Lock()
	d.track = true
	d.mu.Unlock()
}

// second is one finished second's counts.
type second struct{ bytes, queueMax int64 }

// drain returns the seconds that ended by now, oldest first, and removes them.
// final also returns one last entry: everything still booked, and the largest
// content of the second in progress.
func (d *direction) drain(now time.Time, final bool) []second {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.advance(now)
	var out []second
	for ; d.next < d.cur; d.next++ {
		out = append(out, second{d.booked[d.next], d.closed[d.next]})
		delete(d.booked, d.next)
		delete(d.closed, d.next)
	}
	if final {
		var rest int64
		for k, v := range d.booked {
			rest += v
			delete(d.booked, k)
		}
		out = append(out, second{rest, d.qmax})
	}
	return out
}

// queued is the bytes waiting for the link now.
func (d *direction) queued() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.waiting
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
	up, down *direction
	delay    time.Duration
	chunk    int
	rcvbuf   int
	fifo     bool
	epoch    time.Time

	mu    sync.Mutex
	conns []*liveConn
	ids   atomic.Int64
	open  atomic.Int64
}

type liveConn struct {
	s        connStats
	up, down atomic.Int64
	closed   atomic.Int64
}

// config is the emulator's settings.
type config struct {
	maps                 []string
	upRate, downRate     float64
	delay                time.Duration
	chunk, rcvbuf        int
	statsPath            string
	seriesPath           string
	discipline           string
	bufUp, bufDown       time.Duration
	bufUpSet, bufDownSet bool
}

const (
	fair = "fair"
	fifo = "fifo"
)

func main() {
	var c config
	var maps multiFlag
	flag.Var(&maps, "map", "listen=upstream, both 127.0.0.1:port (repeatable)")
	flag.Float64Var(&c.upRate, "up-rate", 0, "bytes/s shared by all connections, connecting side to upstream (0 = unlimited)")
	flag.Float64Var(&c.downRate, "down-rate", 0, "bytes/s shared by all connections, upstream to connecting side (0 = unlimited)")
	flag.DurationVar(&c.delay, "delay", 0, "one-way delay added in each direction")
	flag.IntVar(&c.chunk, "chunk", 16*1024, "largest piece read and sent at once")
	flag.IntVar(&c.rcvbuf, "rcvbuf", 256*1024, "SO_RCVBUF of the emulator's own sockets (0: the kernel's)")
	flag.StringVar(&c.statsPath, "stats", "", "where to write per-connection statistics, every 2 s and at exit")
	flag.StringVar(&c.seriesPath, "series", "", "append one JSON row per second, and one at exit, to this file")
	flag.StringVar(&c.discipline, "discipline", fair, "fair (connections take turns) or fifo (one shared queue with a buffer)")
	flag.DurationVar(&c.bufUp, "buffer-up", 0, "fifo: the up buffer, as a duration of -up-rate (required for a limited direction)")
	flag.DurationVar(&c.bufDown, "buffer-down", 0, "fifo: the down buffer, as a duration of -down-rate (required for a limited direction)")
	flag.Parse()
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "buffer-up":
			c.bufUpSet = true
		case "buffer-down":
			c.bufDownSet = true
		}
	})
	c.maps = maps
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	if err := run(c, term); err != nil {
		fmt.Fprintln(os.Stderr, "s16link:", err)
		os.Exit(1)
	}
}

// validate checks the settings at the boundary, so that no flag is ignored in
// silence.
func (c config) validate() error {
	if len(c.maps) == 0 || c.chunk < 1 || c.delay < 0 || c.upRate < 0 || c.downRate < 0 {
		return errors.New("want at least one -map, -chunk >= 1 and no negative rate or delay")
	}
	switch c.discipline {
	case fair:
		if c.bufUpSet || c.bufDownSet {
			return errors.New("-buffer-up and -buffer-down belong to -discipline fifo; fair has no buffer")
		}
	case fifo:
		for _, d := range []struct {
			name string
			rate float64
			buf  time.Duration
			set  bool
		}{{"up", c.upRate, c.bufUp, c.bufUpSet}, {"down", c.downRate, c.bufDown, c.bufDownSet}} {
			if d.set && d.buf < 0 {
				return fmt.Errorf("-buffer-%s %v: want a duration of at least 0", d.name, d.buf)
			}
			if d.rate > 0 && !d.set {
				return fmt.Errorf("-discipline fifo needs -buffer-%s (a duration of -%s-rate) for a limited direction", d.name, d.name)
			}
		}
	default:
		return fmt.Errorf("-discipline %q: want fair or fifo", c.discipline)
	}
	return nil
}

// run serves until a signal arrives on term, or an accept fails.
func run(c config, term <-chan os.Signal) error {
	if err := c.validate(); err != nil {
		return err
	}
	l := newLink(c)
	var s *series
	if c.seriesPath != "" {
		var err error
		if s, err = l.startSeries(c.seriesPath); err != nil {
			return err
		}
	}
	finish := func(err error) error {
		var serr error
		if s != nil {
			serr = s.Close()
		}
		return errors.Join(err, l.writeStats(c.statsPath), serr)
	}
	errc := make(chan error, len(c.maps))
	for _, m := range c.maps {
		listen, upstream, ok := strings.Cut(m, "=")
		if !ok {
			return finish(fmt.Errorf("-map %q: want listen=upstream", m))
		}
		if _, err := l.listen(listen, upstream, errc); err != nil {
			return finish(err)
		}
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case err := <-errc:
			return finish(err)
		case <-tick.C:
			_ = l.writeStats(c.statsPath)
		case <-term:
			return finish(nil)
		}
	}
}

// bytesOf is the capacity of a buffer of d at rate bytes/s.
func bytesOf(rate float64, d time.Duration) int64 {
	return int64(math.Round(rate * d.Seconds()))
}

func newLink(c config) *link {
	epoch := time.Now()
	isFifo := c.discipline == fifo
	return &link{
		up:    newDirection(c.upRate, isFifo, bytesOf(c.upRate, c.bufUp), epoch),
		down:  newDirection(c.downRate, isFifo, bytesOf(c.downRate, c.bufDown), epoch),
		delay: c.delay, chunk: c.chunk, rcvbuf: c.rcvbuf, fifo: isFifo, epoch: epoch,
	}
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
	l.open.Add(1)
	pipe := l.pipeFair
	if l.fifo {
		pipe = l.pipeFIFO
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pipe(u, c, l.up, &lc.up) }()
	go func() { defer wg.Done(); pipe(c, u, l.down, &lc.down) }()
	wg.Wait()
	c.Close()
	u.Close()
	lc.closed.Store(time.Now().UnixNano())
	l.open.Add(-1)
}

type piece struct {
	b  []byte
	at time.Time
}

// pipeFair copies src to dst through one direction of the link: each chunk
// waits for its turn on the link, is sent at the link's rate, and is delivered
// delay after it was sent. An end of stream is passed on as a half-close, after
// every chunk before it. This is s15link's pipe; the direction only adds the
// counts for the series.
func (l *link) pipeFair(dst, src net.Conn, d *direction, count *atomic.Int64) {
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
			sent := d.book(n)
			time.Sleep(time.Until(sent))
			d.release(n)
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

// pieceQueue hands chunks from a connection's reader to its writer. Its size
// is bounded in bytes, so a receiver that stops reading stops the reader too
// instead of growing the emulator's memory.
type pieceQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []piece
	bytes  int64
	limit  int64
	closed bool
}

func newPieceQueue(limit int64) *pieceQueue {
	q := &pieceQueue{limit: limit}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push adds p, waiting while the queue holds its limit; it reports false when
// the queue was aborted.
func (q *pieceQueue) push(p piece) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.bytes >= q.limit && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return false
	}
	q.items = append(q.items, p)
	q.bytes += int64(len(p.b))
	q.cond.Broadcast()
	return true
}

// pop returns the oldest chunk, waiting for one; it reports false when the
// queue is closed and empty.
func (q *pieceQueue) pop() (piece, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return piece{}, false
	}
	p := q.items[0]
	q.items[0] = piece{}
	q.items = q.items[1:]
	q.bytes -= int64(len(p.b))
	q.cond.Broadcast()
	return p, true
}

// close ends the queue after the chunks it holds.
func (q *pieceQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// abort ends the queue and drops what it holds.
func (q *pieceQueue) abort() {
	q.mu.Lock()
	q.closed = true
	q.items, q.bytes = nil, 0
	q.cond.Broadcast()
	q.mu.Unlock()
}

// queueLimit bounds one connection's chunks that are booked and not yet
// written: the buffer's bytes, plus what the delay line holds, plus a margin.
func (l *link) queueLimit(d *direction) int64 {
	if !d.limited() {
		return 256 << 20
	}
	return d.capBytes + int64(d.b.rate*l.delay.Seconds()) + 1<<20 + 4*int64(l.chunk)
}

// pipeFIFO copies src to dst through one direction of the link as a member of
// the direction's shared FIFO queue: the reader books each chunk when the
// buffer has room, hands it to the writer and reads on at once; the writer
// sends the chunks in order at their booked time plus delay. The chunk's bytes
// leave the buffer at the booked send time, through a timer, so they leave
// whatever the writer is doing and a dead connection never holds the buffer.
func (l *link) pipeFIFO(dst, src net.Conn, d *direction, count *atomic.Int64) {
	q := newPieceQueue(l.queueLimit(d))
	done := make(chan struct{})
	var failed atomic.Bool
	go func() {
		defer close(done)
		for {
			p, ok := q.pop()
			if !ok {
				return
			}
			time.Sleep(time.Until(p.at))
			if _, err := dst.Write(p.b); err != nil {
				failed.Store(true)
				q.abort()
				_ = src.SetReadDeadline(time.Now()) // wake the reader
				return
			}
			count.Add(int64(len(p.b)))
		}
	}()
	buf := make([]byte, l.chunk)
	var rerr error
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if failed.Load() {
				rerr = errors.New("writer failed")
				break
			}
			sent := d.book(n)
			if d.limited() {
				time.AfterFunc(time.Until(sent), func() { d.release(n) })
			}
			// A chunk sent on a schedule that lags the clock (catching up) still
			// arrives no sooner than delay after it was booked.
			at := sent
			if now := time.Now(); at.Before(now) {
				at = now
			}
			if !q.push(piece{b: append([]byte(nil), buf[:n]...), at: at.Add(l.delay)}) {
				rerr = errors.New("writer failed")
				break
			}
		}
		if err != nil {
			rerr = err
			break
		}
	}
	q.close()
	<-done
	if errors.Is(rerr, io.EOF) {
		_ = dst.(*net.TCPConn).CloseWrite()
	} else {
		dst.Close()
	}
}

// seriesRow is one line of the -series file.
type seriesRow struct {
	T            float64 `json:"t"`
	UpBytes      int64   `json:"up_bytes"`
	DownBytes    int64   `json:"down_bytes"`
	UpQueueMax   int64   `json:"up_queue_max"`
	DownQueueMax int64   `json:"down_queue_max"`
	Conns        int64   `json:"conns"`
}

// series writes a row per elapsed second.
type series struct {
	l    *link
	f    *os.File
	stop chan struct{}
	done chan struct{}
	once sync.Once

	mu   sync.Mutex
	rows int64 // seconds written so far
	err  error // the first write error
}

// startSeries begins the rows in the file at path, appending to it. Call it
// before any traffic: bytes booked earlier are not counted.
func (l *link) startSeries(path string) (*series, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l.up.startTracking()
	l.down.startTracking()
	s := &series{l: l, f: f, stop: make(chan struct{}), done: make(chan struct{})}
	go s.loop()
	return s, nil
}

func (s *series) loop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		at := s.l.epoch.Add(time.Duration(s.rows+1) * time.Second)
		s.mu.Unlock()
		timer := time.NewTimer(time.Until(at))
		select {
		case <-timer.C:
			s.emit(time.Now(), false)
		case <-s.stop:
			timer.Stop()
			return
		}
	}
}

// emit writes the rows for the seconds that ended by now; final adds the last
// row, for the second in progress, stamped now.
func (s *series) emit(now time.Time, final bool) {
	up, down := s.l.up.drain(now, final), s.l.down.drain(now, final)
	conns := s.l.open.Load()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range up {
		t := s.l.epoch.Add(time.Duration(s.rows+1) * time.Second)
		if final && i == len(up)-1 {
			t = now
		} else {
			s.rows++
		}
		row := seriesRow{
			T:       float64(t.UnixNano()) / 1e9,
			UpBytes: up[i].bytes, DownBytes: down[i].bytes,
			UpQueueMax: up[i].queueMax, DownQueueMax: down[i].queueMax,
			Conns: conns,
		}
		b, err := json.Marshal(row)
		if err == nil {
			_, err = s.f.Write(append(b, '\n'))
		}
		if err != nil && s.err == nil {
			s.err = err
			fmt.Fprintln(os.Stderr, "s16link: series:", err)
		}
	}
}

// Close stops the rows, writes the last one and closes the file. It returns the
// first error the file gave.
func (s *series) Close() error {
	s.once.Do(func() {
		close(s.stop)
		<-s.done
		s.emit(time.Now(), true)
		if err := s.f.Close(); err != nil {
			s.mu.Lock()
			if s.err == nil {
				s.err = err
			}
			s.mu.Unlock()
		}
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
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
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s16link-*")
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

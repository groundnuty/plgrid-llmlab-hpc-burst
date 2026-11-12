package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// disciplines are run against the same expectations wherever the behaviour is
// shared, and apart where it is the difference.
var disciplines = []string{fair, fifo}

// linkCfg is a validated-shape config: fifo gets its buffers, as durations of
// the direction's rate.
func linkCfg(disc string, up, down float64, delay time.Duration, chunk int, bufUp, bufDown time.Duration) config {
	c := config{
		maps: []string{"127.0.0.1:0=127.0.0.1:1"}, discipline: disc,
		upRate: up, downRate: down, delay: delay, chunk: chunk, rcvbuf: 64 << 10,
	}
	if disc == fifo {
		c.bufUp, c.bufUpSet = bufUp, true
		c.bufDown, c.bufDownSet = bufDown, true
	}
	return c
}

// serveLink validates c, builds its link and listens on a kernel-chosen
// loopback port, carrying to upstream.
func serveLink(t *testing.T, c config, upstream string) (*link, string) {
	t.Helper()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	l := newLink(c)
	errc := make(chan error, 1)
	ln, err := l.listen("127.0.0.1:0", upstream, errc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return l, ln.Addr().String()
}

// waitFor polls cond until it holds, failing the test at the deadline. A test
// waits on observable state this way, never on a fixed sleep.
func waitFor(t *testing.T, what string, deadline time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("timed out after %v waiting for %s", deadline, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// dial opens a client connection that is reset, not lingered on, when the
// test ends, so a backlog left in its socket does not keep the emulator busy.
func dial(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	tc := c.(*net.TCPConn)
	t.Cleanup(func() { _ = tc.SetLinger(0); tc.Close() })
	return tc
}

// sink is an upstream that counts what it receives, per connection, with the
// time of every read.
type sink struct {
	addr  string
	mu    sync.Mutex
	conns []*sinkConn
	total atomic.Int64
}

type mark struct {
	at  time.Time
	cum int64
}

type sinkConn struct {
	got   atomic.Int64
	mu    sync.Mutex
	marks []mark
}

func newSink(t *testing.T) *sink {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{addr: ln.Addr().String()}
	var open sync.Map
	t.Cleanup(func() {
		ln.Close()
		open.Range(func(k, _ any) bool { k.(net.Conn).Close(); return true })
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			open.Store(c, nil)
			sc := &sinkConn{}
			s.mu.Lock()
			s.conns = append(s.conns, sc)
			s.mu.Unlock()
			go func() {
				defer c.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						now := time.Now()
						sc.mu.Lock()
						cum := sc.got.Add(int64(n))
						sc.marks = append(sc.marks, mark{now, cum})
						sc.mu.Unlock()
						s.total.Add(int64(n))
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return s
}

// conn is the i-th connection the sink accepted, or nil.
func (s *sink) conn(i int) *sinkConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < len(s.conns) {
		return s.conns[i]
	}
	return nil
}

// reached is the time of the first read after which the connection had n bytes.
func (c *sinkConn) reached(n int64) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.marks {
		if m.cum >= n {
			return m.at, true
		}
	}
	return time.Time{}, false
}

// cumAt is how many bytes the connection had by time t.
func (c *sinkConn) cumAt(t time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cum int64
	for _, m := range c.marks {
		if m.at.After(t) {
			break
		}
		cum = m.cum
	}
	return cum
}

// echo serves an upstream that sends back what it reads, and half-closes.
func echo(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
				_ = c.(*net.TCPConn).CloseWrite()
			}()
		}
	}()
	return ln.Addr().String()
}

// randBytes is n random bytes.
func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// readRows parses a series file.
func readRows(t *testing.T, path string) []seriesRow {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []seriesRow
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var r seriesRow
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("series line %q: %v", line, err)
		}
		rows = append(rows, r)
	}
	return rows
}

// fakeClock is a settable clock for the deterministic tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) set(t time.Time) {
	f.mu.Lock()
	f.t = t
	f.mu.Unlock()
}

func TestReserveSharesOneRateInTurn(t *testing.T) {
	b := &bucket{rate: 1000}
	t0 := time.Unix(100, 0)
	if got := b.reserve(t0, 500); !got.Equal(t0.Add(500 * time.Millisecond)) {
		t.Fatalf("first 500 bytes at 1000 B/s: sent at %v, want t0+500ms", got.Sub(t0))
	}
	// A second connection's chunk booked at the same moment waits for the first.
	if got := b.reserve(t0, 500); !got.Equal(t0.Add(time.Second)) {
		t.Fatalf("second chunk: sent at %v, want t0+1s", got.Sub(t0))
	}
	// After an idle stretch the link starts afresh.
	if got := b.reserve(t0.Add(3*time.Second), 100); !got.Equal(t0.Add(3100 * time.Millisecond)) {
		t.Fatalf("after idle: sent at %v, want t0+3.1s", got.Sub(t0))
	}
	// With slack, the schedule may lag the clock by up to slack: after idle,
	// up to slack's worth goes at once; a sender that overslept catches up
	// instead of losing the time; beyond slack the time is lost.
	sl := &bucket{rate: 1000, slack: 200 * time.Millisecond}
	if got := sl.reserve(t0, 100); !got.Equal(t0.Add(-100 * time.Millisecond)) {
		t.Fatalf("from idle: done at %v, want t0-100ms (within slack, at once)", got.Sub(t0))
	}
	if got := sl.reserve(t0, 300); !got.Equal(t0.Add(200 * time.Millisecond)) {
		t.Fatalf("booked behind it: done at %v, want t0+200ms", got.Sub(t0))
	}
	if got := sl.reserve(t0.Add(350*time.Millisecond), 100); !got.Equal(t0.Add(300 * time.Millisecond)) {
		t.Fatalf("an oversleep of 150 ms: done at %v, want t0+300ms (caught up)", got.Sub(t0))
	}
	if got := sl.reserve(t0.Add(time.Second), 100); !got.Equal(t0.Add(900 * time.Millisecond)) {
		t.Fatalf("beyond slack: done at %v, want t0+900ms (now - slack + 100 ms)", got.Sub(t0))
	}
	if got := (&bucket{}).reserve(t0, 1<<20); !got.Equal(t0) {
		t.Fatal("rate 0 is unlimited")
	}
}

// fakeFIFO is a fifo direction at 1000 B/s with an exact schedule (no slack) on
// a clock the test sets.
func fakeFIFO(capBytes int64, epoch time.Time) (*direction, *fakeClock) {
	clk := &fakeClock{t: epoch}
	d := newDirection(1000, true, capBytes, epoch)
	d.b.slack = 0
	d.clock = clk.now
	return d, clk
}

// The buffer admits in arrival order: a small chunk that would fit does not
// overtake a larger one waiting for room, and a chunk larger than the whole
// buffer passes when the buffer is empty. Booked times follow from the rate
// (1000 B/s) and the sizes alone.
func TestFIFOAdmitsInArrivalOrderAndOversizePassesAlone(t *testing.T) {
	t0 := time.Unix(1000, 0)
	d, clk := fakeFIFO(100, t0)

	if got := d.book(80); !got.Equal(t0.Add(80 * time.Millisecond)) {
		t.Fatalf("80 B into an empty 100 B buffer: sent at %v, want t0+80ms", got.Sub(t0))
	}
	type booked struct {
		name string
		sent time.Time
	}
	res := make(chan booked, 2)
	go func() { res <- booked{"sixty", d.book(60)} }() // 80+60 > 100: waits
	waitFor(t, "the 60 B chunk to queue", 10*time.Second, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.queue) == 1
	})
	go func() { res <- booked{"ten", d.book(10)} }() // 80+10 <= 100, but it arrived later
	waitFor(t, "the 10 B chunk to queue behind it", 10*time.Second, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.queue) == 2
	})
	if got := d.queued(); got != 80 {
		t.Fatalf("buffer holds %d B with two arrivals waiting, want 80 (the 10 B chunk must not slip in)", got)
	}

	clk.set(t0.Add(80 * time.Millisecond))
	d.release(80)
	sent := map[string]time.Time{}
	for i := 0; i < 2; i++ {
		r := <-res // the goroutines finish in either order; the booked times tell the admission order
		sent[r.name] = r.sent
	}
	// Admitted in arrival order: 60 B booked right behind the first chunk
	// (80 ms + 60 ms), then 10 B behind it. Had the 10 B chunk slipped in, it
	// would be booked at 90 ms and the 60 B chunk after it, at 150 ms.
	if got := sent["sixty"]; !got.Equal(t0.Add(140 * time.Millisecond)) {
		t.Fatalf("the 60 B chunk: sent at %v, want t0+140ms", got.Sub(t0))
	}
	if got := sent["ten"]; !got.Equal(t0.Add(150 * time.Millisecond)) {
		t.Fatalf("the 10 B chunk: sent at %v, want t0+150ms", got.Sub(t0))
	}
	if got := d.queued(); got != 70 {
		t.Fatalf("buffer holds %d B after both were admitted, want 60+10", got)
	}

	// A chunk larger than the whole buffer goes through when it is empty, and
	// holds the others back until it has been sent.
	over, _ := fakeFIFO(100, t0)
	if got := over.book(500); !got.Equal(t0.Add(500 * time.Millisecond)) {
		t.Fatalf("500 B into an empty 100 B buffer: sent at %v, want t0+500ms", got.Sub(t0))
	}
	small := make(chan time.Time, 1)
	go func() { small <- over.book(1) }()
	waitFor(t, "the 1 B chunk to queue behind the oversize one", 10*time.Second, func() bool {
		over.mu.Lock()
		defer over.mu.Unlock()
		return len(over.queue) == 1
	})
	over.release(500)
	if got := <-small; !got.Equal(t0.Add(501 * time.Millisecond)) {
		t.Fatalf("1 B behind the oversize chunk: sent at %v, want t0+501ms", got.Sub(t0))
	}
}

// The series counts a chunk in the second it is sent, not the one it was read
// in, keeps the largest waiting content per second, and loses no byte. The
// expected rows are worked out by hand from the schedule (1000 B/s, no slack).
func TestSeriesCountsBytesInTheSecondTheyAreSent(t *testing.T) {
	e := time.Unix(2000, 0)
	at := func(ms int) time.Time { return e.Add(time.Duration(ms) * time.Millisecond) }
	d, clk := fakeFIFO(5000, e)
	d.startTracking()

	clk.set(at(500))
	if got := d.book(1000); !got.Equal(at(1500)) { // sent in second 1
		t.Fatalf("1000 B at 0.5 s: sent at %v, want 1.5 s", got.Sub(e))
	}
	clk.set(at(700))
	if got := d.book(500); !got.Equal(at(2000)) { // sent in second 2
		t.Fatalf("500 B at 0.7 s: sent at %v, want 2.0 s", got.Sub(e))
	}
	clk.set(at(1500))
	d.release(1000)
	clk.set(at(2000))
	d.release(500)

	// Seconds 0 and 1 have ended. Nothing is sent in second 0, though 1500 B
	// were booked in it; second 1 sends 1000 B and starts with 1500 B waiting.
	got := d.drain(at(2500), false)
	want := []second{{0, 1500}, {1000, 1500}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rows for seconds 0 and 1 = %+v, want %+v", got, want)
	}

	clk.set(at(2600))
	if got := d.book(200); !got.Equal(at(2800)) {
		t.Fatalf("200 B at 2.6 s: sent at %v, want 2.8 s", got.Sub(e))
	}
	clk.set(at(3000))
	if got := d.book(2000); !got.Equal(at(5000)) { // far ahead: second 5
		t.Fatalf("2000 B at 3.0 s: sent at %v, want 5.0 s", got.Sub(e))
	}
	// Second 2 sent 500 B (booked in second 0) and 200 B; it started with 500 B
	// waiting. The last row takes what is still booked ahead (2000 B) and the
	// largest content of the second in progress (200 B + 2000 B booked at 3.0 s).
	got = d.drain(at(3500), true)
	want = []second{{700, 500}, {2000, 2200}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rows for second 2 and the last = %+v, want %+v", got, want)
	}
}

// A chunk booked in the past by slack, after its second's row was written,
// counts in the next row: no byte is lost.
func TestSeriesKeepsEveryByteWhenAChunkIsBookedIntoAWrittenSecond(t *testing.T) {
	e := time.Unix(3000, 0)
	d := newDirection(1000, false, 0, e)
	d.b.slack = 10 * time.Millisecond
	clk := &fakeClock{t: e.Add(100 * time.Millisecond)}
	d.clock = clk.now
	d.startTracking()

	if got := d.drain(e.Add(1000500*time.Microsecond), false); len(got) != 1 || got[0].bytes != 0 {
		t.Fatalf("row 0 = %+v, want one row with no bytes", got)
	}
	// At 1.001 s the schedule is idle: 1 B is booked at now - slack + 1 ms =
	// 0.992 s, which is in second 0, whose row is written.
	clk.set(e.Add(1001 * time.Millisecond))
	if got := d.book(1); !got.Equal(e.Add(992 * time.Millisecond)) {
		t.Fatalf("1 B booked at 1.001 s with 10 ms slack: sent at %v, want 0.992 s", got.Sub(e))
	}
	got := d.drain(e.Add(2500*time.Millisecond), false)
	if len(got) != 1 || got[0].bytes != 1 {
		t.Fatalf("row 1 = %+v, want the 1 B that was booked into second 0 after its row", got)
	}
}

// B writes a few bytes while A has a backlog queued. In fifo they wait behind
// the bytes A had booked ahead, which fill the buffer: no sooner than about
// buffer/rate. In fair they wait for at most one chunk of A's.
func TestFIFOBacklogDelaysASmallWriteByTheBufferAndFairDoesNot(t *testing.T) {
	const rate, chunk = 200e3, 4096
	const buffer = 500 * time.Millisecond
	capBytes := int64(rate * buffer.Seconds()) // 100,000 B
	// What B must wait behind in fifo: A's buffer full, less up to one chunk
	// (the chunk A has waiting is admitted first) and a margin of three chunks
	// for timers that fire late. Fair is far below it (one chunk is 20 ms).
	margin := int64(4 * chunk)
	floor := time.Duration(float64(capBytes-margin) / rate * float64(time.Second))
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			sk := newSink(t)
			l, addr := serveLink(t, linkCfg(disc, rate, 0, 0, chunk, buffer, 0), sk.addr)
			a := dial(t, addr)
			backlog := randBytes(t, 2_000_000) // far more than the buffer
			go func() { _, _ = a.Write(backlog) }()

			// A is carrying a backlog: in fifo its reader has filled the buffer;
			// in fair it has been sending for a few chunks.
			waitFor(t, "A's backlog", 20*time.Second, func() bool {
				if disc == fifo {
					return l.up.queued() >= capBytes-chunk
				}
				c := sk.conn(0)
				return c != nil && c.got.Load() >= 3*chunk
			})
			b := dial(t, addr)
			wrote := time.Now()
			if _, err := b.Write([]byte("BXY")); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "B's 3 bytes at the upstream", 30*time.Second, func() bool {
				c := sk.conn(1)
				return c != nil && c.got.Load() >= 3
			})
			arrived, _ := sk.conn(1).reached(3)
			took := arrived.Sub(wrote)
			aGot := sk.conn(0).cumAt(arrived)
			t.Logf("B's bytes arrived %v after the write; A's upstream had %d bytes by then", took, aGot)
			if disc == fifo {
				if took < floor {
					t.Errorf("fifo: B's bytes arrived after %v, before the %v the buffer's bytes ahead of it take at %v B/s", took, floor, rate)
				}
				if aGot < capBytes-margin {
					t.Errorf("fifo: B's bytes arrived with only %d of A's bytes delivered, want at least %d (the buffer, less a margin)", aGot, capBytes-margin)
				}
			} else if took >= floor {
				t.Errorf("fair: B's bytes took %v, not within about one chunk's time (%v); floor of fifo's wait is %v", took, time.Duration(float64(chunk)/rate*float64(time.Second)), floor)
			}
		})
	}
}

// With the sender saturating a limited link, each full second carries the
// rate, the rows add up to what the receiver got, and in fifo the buffer is
// full but never beyond its bytes.
func TestSeriesShowsTheLinkAtItsCapAndTheBufferWithinItsBytes(t *testing.T) {
	const rate, chunk, total = 1e6, 4096, 5_000_000
	const buffer = 100 * time.Millisecond
	capBytes := int64(rate * buffer.Seconds()) // 100,000 B
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			sk := newSink(t)
			c := linkCfg(disc, rate, 0, 0, chunk, buffer, 0)
			l, addr := serveLink(t, c, sk.addr)
			path := filepath.Join(t.TempDir(), "series.jsonl")
			s, err := l.startSeries(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })

			cl := dial(t, addr)
			payload := randBytes(t, total)
			wrote := time.Now()
			go func() { _, _ = cl.Write(payload) }()
			waitFor(t, "every byte at the upstream", 60*time.Second, func() bool { return sk.total.Load() == total })
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			rows := readRows(t, path)
			if len(rows) < 3 {
				t.Fatalf("%d rows for a %d-second transfer", len(rows), total/int(rate))
			}

			// Row k is the second [epoch+k, epoch+k+1): its t is the end of it,
			// one second after the previous, and the last row comes at exit.
			final := rows[len(rows)-1]
			full := rows[:len(rows)-1]
			for k, r := range full {
				want := float64(l.epoch.UnixNano())/1e9 + float64(k+1)
				if math.Abs(r.T-want) > 1e-3 {
					t.Errorf("row %d: t = %.4f, want %.4f (epoch + %d s)", k, r.T, want, k+1)
				}
			}
			if final.T < full[len(full)-1].T {
				t.Errorf("last row t = %.4f, before the row before it (%.4f)", final.T, full[len(full)-1].T)
			}

			var up, down int64
			for _, r := range rows {
				up += r.UpBytes
				down += r.DownBytes
				if r.DownQueueMax != 0 {
					t.Errorf("down_queue_max = %d with nothing sent down", r.DownQueueMax)
				}
				if disc == fifo && r.UpQueueMax > capBytes {
					t.Errorf("up_queue_max = %d, beyond the %d B buffer", r.UpQueueMax, capBytes)
				}
				if disc == fair && r.UpQueueMax > chunk {
					t.Errorf("up_queue_max = %d, beyond one chunk (%d B) for one connection in fair", r.UpQueueMax, chunk)
				}
			}
			if up != total || down != 0 {
				t.Errorf("rows add up to %d up, %d down; the upstream received %d, and nothing went down", up, down, total)
			}

			// Rows that lie wholly inside the transfer (a margin of 0.1 s at each
			// end): the transfer's bytes at the rate take total/rate seconds.
			dur := time.Duration(float64(total) / rate * float64(time.Second))
			inside := 0
			for k, r := range full {
				start := l.epoch.Add(time.Duration(k) * time.Second)
				if start.Before(wrote.Add(100*time.Millisecond)) || start.Add(time.Second).After(wrote.Add(dur-100*time.Millisecond)) {
					continue
				}
				inside++
				if math.Abs(float64(r.UpBytes)-rate) > 0.1*rate {
					t.Errorf("row %d: up_bytes = %d, not within 10%% of the %v B/s rate", k, r.UpBytes, rate)
				}
				if r.Conns != 1 {
					t.Errorf("row %d: conns = %d, want 1", k, r.Conns)
				}
				if disc == fifo && r.UpQueueMax < capBytes-2*chunk {
					t.Errorf("row %d: up_queue_max = %d, but a saturating sender keeps the %d B buffer full", k, r.UpQueueMax, capBytes)
				}
				if disc == fair && r.UpQueueMax < chunk/2 {
					t.Errorf("row %d: up_queue_max = %d, want about one chunk (%d B) booked and not yet sent", k, r.UpQueueMax, chunk)
				}
			}
			if inside < 3 {
				t.Fatalf("only %d rows lie inside the transfer; the test needs at least 3 to show the link at its cap", inside)
			}
		})
	}
}

// A buffer smaller than a chunk still passes every byte, one chunk at a time;
// with several connections the buffer never holds more than its bytes, and a
// saturating load fills it.
func TestFIFOBufferNeverHoldsMoreThanItsBytes(t *testing.T) {
	const rate, chunk, conns, per = 1e6, 4096, 4, 100_000
	for _, tc := range []struct {
		name    string
		buffer  time.Duration
		bound   int64 // the most the buffer may hold
		atLeast int64 // the least its peak must reach under this load
	}{
		// 1,000 B: a 4,096 B chunk goes through alone, so the peak stays at one chunk.
		{"smaller than a chunk", time.Millisecond, chunk, 1},
		// 20,000 B: full to within one chunk, never beyond.
		{"several chunks", 20 * time.Millisecond, 20_000, 20_000 - chunk},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sk := newSink(t)
			l, addr := serveLink(t, linkCfg(fifo, rate, 0, 0, chunk, tc.buffer, 0), sk.addr)
			path := filepath.Join(t.TempDir(), "series.jsonl")
			s, err := l.startSeries(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			for i := 0; i < conns; i++ {
				c := dial(t, addr)
				payload := randBytes(t, per)
				go func() { _, _ = c.Write(payload) }()
			}
			waitFor(t, "every byte at the upstream", 60*time.Second, func() bool { return sk.total.Load() == conns*per })
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			var peak, up int64
			for _, r := range readRows(t, path) {
				peak = max(peak, r.UpQueueMax)
				up += r.UpBytes
			}
			if peak > tc.bound {
				t.Errorf("the buffer held %d B at most, beyond %d", peak, tc.bound)
			}
			if peak < tc.atLeast {
				t.Errorf("the buffer's peak was %d B under a saturating load, want at least %d", peak, tc.atLeast)
			}
			if up != conns*per {
				t.Errorf("rows add up to %d B, want %d", up, conns*per)
			}
			waitFor(t, "the buffer to empty once everything was sent", 10*time.Second, func() bool { return l.up.queued() == 0 })
		})
	}
}

// A connection that dies leaves nothing in the shared buffer: its booked
// bytes leave at their send times, so another connection goes on.
func TestFIFODeadConnectionDoesNotHoldTheBuffer(t *testing.T) {
	const rate, chunk = 1e6, 4096
	bad, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bad.Close() })
	die := make(chan struct{})
	go func() { // an upstream that resets its connections when told to
		for {
			c, err := bad.Accept()
			if err != nil {
				return
			}
			go func() {
				<-die
				_ = c.(*net.TCPConn).SetLinger(0)
				c.Close()
			}()
		}
	}()
	good := newSink(t)
	c := linkCfg(fifo, rate, 0, 0, chunk, 50*time.Millisecond, 0)
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	l := newLink(c)
	errc := make(chan error, 2)
	lnBad, err := l.listen("127.0.0.1:0", bad.Addr().String(), errc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lnBad.Close() })
	lnGood, err := l.listen("127.0.0.1:0", good.addr, errc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lnGood.Close() })

	a := dial(t, lnBad.Addr().String())
	backlog := randBytes(t, 1_000_000)
	go func() { _, _ = a.Write(backlog) }()
	// A dies with a full buffer's bytes booked and not yet sent.
	waitFor(t, "A to fill the buffer", 20*time.Second, func() bool { return l.up.queued() >= 50_000-chunk })
	close(die)
	const want = 200_000
	payload := randBytes(t, want)
	b := dial(t, lnGood.Addr().String())
	go func() { _, _ = b.Write(payload) }()
	waitFor(t, "B's bytes at its upstream, beside a dying connection", 60*time.Second, func() bool { return good.total.Load() == want })
	waitFor(t, "the shared buffer to empty", 30*time.Second, func() bool { return l.up.queued() == 0 })
}

// A rate of 0 is unlimited in either discipline: bytes cross unchanged and with
// no wait, no buffer, and nothing waiting in the series.
func TestRateZeroPassesBytesUnchangedAndAtOnce(t *testing.T) {
	const size = 4 << 20
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			// fifo's buffers are given (0 s) to show that an unlimited direction
			// ignores them.
			l, addr := serveLink(t, linkCfg(disc, 0, 0, 0, 16*1024, 0, 0), echo(t))
			path := filepath.Join(t.TempDir(), "series.jsonl")
			s, err := l.startSeries(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			payload := randBytes(t, size)
			c := dial(t, addr)
			start := time.Now()
			go func() { _, _ = c.Write(payload); _ = c.CloseWrite() }()
			got, err := io.ReadAll(c)
			if err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			if !bytes.Equal(got, payload) {
				t.Fatalf("got %d bytes back, not the %d sent, or not the same", len(got), size)
			}
			// A link of 2 MB/s each way would need 2 s per way for this payload.
			if limit := time.Duration(float64(size) / 2e6 * float64(time.Second)); took >= limit {
				t.Errorf("%d bytes took %v through an unlimited link, as long as a 2 MB/s one (%v)", size, took, limit)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			var up, down int64
			for _, r := range readRows(t, path) {
				up += r.UpBytes
				down += r.DownBytes
				if r.UpQueueMax != 0 || r.DownQueueMax != 0 {
					t.Errorf("queue max %d up, %d down on an unlimited link, want 0 (no buffer)", r.UpQueueMax, r.DownQueueMax)
				}
			}
			if up != size || down != size {
				t.Errorf("rows add up to %d up, %d down, want %d each", up, down, size)
			}
		})
	}
}

// Random data crosses intact, both ways, several connections at once, with a
// delay, in both disciplines.
func TestRandomBytesCrossUnchanged(t *testing.T) {
	const rate, size, conns = 4e6, 256 << 10, 3
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			_, addr := serveLink(t, linkCfg(disc, rate, rate, 2*time.Millisecond, 4096, 10*time.Millisecond, 10*time.Millisecond), echo(t))
			var wg sync.WaitGroup
			errs := make(chan string, conns)
			for i := 0; i < conns; i++ {
				payload := randBytes(t, size+i*1000)
				c := dial(t, addr)
				wg.Add(1)
				go func() {
					defer wg.Done()
					go func() { _, _ = c.Write(payload); _ = c.CloseWrite() }()
					got, err := io.ReadAll(c)
					if err != nil || !bytes.Equal(got, payload) {
						errs <- "a connection's echo differs from what it sent"
					}
				}()
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				t.Error(e)
			}
		})
	}
}

// Bytes pass intact both ways, an end of stream passes on, and neither the
// rate nor the delay is beaten. Only lower bounds are checked: a loaded host
// can make the link slower than set, never faster.
func TestLinkCarriesBytesIntactAtNoMoreThanItsRateAndNoLessThanItsDelay(t *testing.T) {
	const rate, delay = 2e6, 20 * time.Millisecond
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			l, addr := serveLink(t, linkCfg(disc, rate, rate, delay, 16*1024, 100*time.Millisecond, 100*time.Millisecond), echo(t))
			c := dial(t, addr)

			// One byte there and back: at least two one-way delays.
			start := time.Now()
			if _, err := c.Write([]byte{7}); err != nil {
				t.Fatal(err)
			}
			one := make([]byte, 1)
			if _, err := io.ReadFull(c, one); err != nil || one[0] != 7 {
				t.Fatalf("the echoed byte: %v %v", one, err)
			}
			if rtt := time.Since(start); rtt < 2*delay {
				t.Fatalf("a round trip took %v, less than two delays of %v", rtt, delay)
			}

			// 1 MiB there and back, at 2 MB/s each way: at least 1 MiB / 2 MB/s.
			payload := randBytes(t, 1<<20)
			start = time.Now()
			go func() {
				_, _ = c.Write(payload)
				_ = c.CloseWrite()
			}()
			got, err := io.ReadAll(c)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("got %d bytes back, not the %d sent", len(got), len(payload))
			}
			floor := time.Duration(float64(len(payload)) / rate * float64(time.Second))
			if took := time.Since(start); took < floor {
				t.Fatalf("1 MiB took %v, faster than the link's %v", took, floor)
			}

			// The connection is counted, with its bytes each way.
			c.Close()
			waitFor(t, "the connection's counts", 10*time.Second, func() bool {
				l.mu.Lock()
				defer l.mu.Unlock()
				if len(l.conns) != 1 {
					return false
				}
				lc := l.conns[0]
				return lc.up.Load() == 1+1<<20 && lc.down.Load() == 1+1<<20 && lc.closed.Load() != 0
			})
		})
	}
}

// Two connections share one rate: together they take as long as their sum.
func TestTwoConnectionsShareTheRate(t *testing.T) {
	const rate = 2e6
	for _, disc := range disciplines {
		t.Run(disc, func(t *testing.T) {
			_, addr := serveLink(t, linkCfg(disc, rate, rate, 0, 16*1024, 100*time.Millisecond, 100*time.Millisecond), echo(t))
			payload := make([]byte, 512*1024)
			start := time.Now()
			done := make(chan error, 2)
			for i := 0; i < 2; i++ {
				go func() {
					c, err := net.Dial("tcp4", addr)
					if err != nil {
						done <- err
						return
					}
					defer c.Close()
					go func() { _, _ = c.Write(payload); _ = c.(*net.TCPConn).CloseWrite() }()
					got, err := io.ReadAll(c)
					if err == nil && len(got) != len(payload) {
						err = io.ErrUnexpectedEOF
					}
					done <- err
				}()
			}
			for i := 0; i < 2; i++ {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			floor := time.Duration(float64(2*len(payload)) / rate * float64(time.Second))
			if took := time.Since(start); took < floor {
				t.Fatalf("two transfers of %d bytes took %v, faster than one shared link's %v", len(payload), took, floor)
			}
		})
	}
}

func TestSettingsAreValidatedAtTheBoundary(t *testing.T) {
	ok := config{maps: []string{"127.0.0.1:1=127.0.0.1:2"}, chunk: 1, discipline: fair}
	with := func(f func(*config)) config { c := ok; f(&c); return c }
	for _, tc := range []struct {
		name string
		c    config
		want string // a part of the error; "" means valid
	}{
		{"fair by default", ok, ""},
		{"fair with rates", with(func(c *config) { c.upRate, c.downRate = 1e6, 2e6 }), ""},
		{"fair refuses a buffer, which would do nothing", with(func(c *config) { c.bufUp, c.bufUpSet = time.Second, true }), "-buffer-up"},
		{"fair refuses a down buffer", with(func(c *config) { c.bufDownSet = true }), "-buffer-down"},
		{"fifo needs the up buffer of a limited direction", with(func(c *config) { c.discipline, c.upRate = fifo, 1e6 }), "-buffer-up"},
		{"fifo needs the down buffer of a limited direction", with(func(c *config) {
			c.discipline, c.downRate, c.bufUp, c.bufUpSet = fifo, 1e6, time.Second, true
		}), "-buffer-down"},
		{"fifo refuses a negative buffer", with(func(c *config) { c.discipline, c.bufUp, c.bufUpSet = fifo, -time.Second, true }), "-buffer-up"},
		{"fifo with both buffers", with(func(c *config) {
			c.discipline, c.upRate, c.downRate = fifo, 1e6, 1e6
			c.bufUp, c.bufUpSet, c.bufDown, c.bufDownSet = time.Second, true, time.Second, true
		}), ""},
		{"fifo with unlimited directions needs no buffer", with(func(c *config) { c.discipline = fifo }), ""},
		{"an unknown discipline", with(func(c *config) { c.discipline = "lifo" }), "lifo"},
		{"no map", with(func(c *config) { c.maps = nil }), "-map"},
		{"a negative rate", with(func(c *config) { c.upRate = -1 }), "negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.c.validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("valid settings refused: %v", err)
			case tc.want != "" && err == nil:
				t.Fatalf("accepted; want an error naming %q", tc.want)
			case tc.want != "" && !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

func TestListenRefusesAnythingButLoopback(t *testing.T) {
	l := newLink(linkCfg(fair, 0, 0, 0, 1024, 0, 0))
	errc := make(chan error, 1)
	for _, a := range [][2]string{{"0.0.0.0:0", "127.0.0.1:1"}, {"127.0.0.1:0", "10.0.0.1:80"}, {"127.0.0.1:0", "localhost:80"}} {
		if ln, err := l.listen(a[0], a[1], errc); err == nil {
			ln.Close()
			t.Errorf("listen %s upstream %s was accepted, want a loopback-only refusal", a[0], a[1])
		}
	}
}

// At SIGTERM or SIGINT run writes the last series row and the stats file.
func TestRunWritesTheLastSeriesRowAndTheStatsAtTermination(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			dir := t.TempDir()
			c := config{
				maps:  []string{"127.0.0.1:0=127.0.0.1:1"},
				chunk: 1024, discipline: fifo, upRate: 1e6, bufUp: 10 * time.Millisecond, bufUpSet: true,
				seriesPath: filepath.Join(dir, "series.jsonl"), statsPath: filepath.Join(dir, "stats.json"),
			}
			term := make(chan os.Signal, 1)
			result := make(chan error, 1)
			go func() { result <- run(c, term) }()
			waitFor(t, "the series file", 10*time.Second, func() bool {
				_, err := os.Stat(c.seriesPath)
				return err == nil
			})
			term <- sig
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("run did not return after the signal")
			}
			rows := readRows(t, c.seriesPath)
			if len(rows) == 0 {
				t.Fatal("no series row at exit")
			}
			if last := rows[len(rows)-1]; last.Conns != 0 || last.UpBytes != 0 || last.T <= 0 {
				t.Errorf("last row = %+v, want an idle link's row with a time", last)
			}
			if b, err := os.ReadFile(c.statsPath); err != nil || strings.TrimSpace(string(b)) != "[]" {
				t.Errorf("stats file at exit = %q, %v; want []", b, err)
			}
		})
	}
}

func TestRunRefusesInvalidSettings(t *testing.T) {
	c := config{maps: []string{"127.0.0.1:0=127.0.0.1:1"}, chunk: 1024, discipline: fifo, upRate: 1e6}
	if err := run(c, make(chan os.Signal)); err == nil || !strings.Contains(err.Error(), "-buffer-up") {
		t.Fatalf("run with a limited fifo direction and no buffer: %v, want an error naming -buffer-up", err)
	}
}

// A listen that fails ends the run with its error, and the series still gets
// its last row.
func TestRunWritesTheLastSeriesRowWhenItCannotListen(t *testing.T) {
	taken, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	path := filepath.Join(t.TempDir(), "series.jsonl")
	c := config{
		maps:  []string{taken.Addr().String() + "=127.0.0.1:1"},
		chunk: 1024, discipline: fair, seriesPath: path,
	}
	if err := run(c, make(chan os.Signal)); err == nil || !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("run on a taken port: %v, want an address-in-use error", err)
	}
	if rows := readRows(t, path); len(rows) != 1 {
		t.Fatalf("%d series rows after a failed start, want the last row only", len(rows))
	}
}

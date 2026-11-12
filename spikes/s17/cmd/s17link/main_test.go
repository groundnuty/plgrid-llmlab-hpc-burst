package main

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"
)

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

// Bytes pass intact both ways, an end of stream passes on, and neither the
// rate nor the delay is beaten. Only lower bounds are checked: a loaded host
// can make the link slower than set, never faster.
func TestLinkCarriesBytesIntactAtNoMoreThanItsRateAndNoLessThanItsDelay(t *testing.T) {
	const rate, delay = 2e6, 20 * time.Millisecond
	l := newLink(rate, rate, delay, 16*1024, 64*1024)
	errc := make(chan error, 1)
	ln, err := l.listen("127.0.0.1:0", echo(t), errc)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.Dial("tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

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
	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	go func() {
		_, _ = c.Write(payload)
		_ = c.(*net.TCPConn).CloseWrite()
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
	deadline := time.Now().Add(10 * time.Second)
	for {
		l.mu.Lock()
		n := len(l.conns)
		var up, down, closed int64
		if n == 1 {
			up, down, closed = l.conns[0].up.Load(), l.conns[0].down.Load(), l.conns[0].closed.Load()
		}
		l.mu.Unlock()
		if n == 1 && up == 1+1<<20 && down == 1+1<<20 && closed != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats: %d connections, up %d, down %d, closed %d; want 1, %d, %d, set", n, up, down, closed, 1+1<<20, 1+1<<20)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Two connections share one rate: together they take as long as their sum.
func TestTwoConnectionsShareTheRate(t *testing.T) {
	const rate = 2e6
	l := newLink(rate, rate, 0, 16*1024, 64*1024)
	errc := make(chan error, 1)
	ln, err := l.listen("127.0.0.1:0", echo(t), errc)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payload := make([]byte, 512*1024)
	start := time.Now()
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			c, err := net.Dial("tcp4", ln.Addr().String())
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
}

// sink serves an upstream that discards what it reads and tells, on arrivals, when each read that got bytes
// returned (a full channel drops the news, so an unread sink never blocks its reader).
func sink(t *testing.T) (addr string, arrivals chan time.Time) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	arrivals = make(chan time.Time, 64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 32*1024)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						select {
						case arrivals <- time.Now():
						default:
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), arrivals
}

// The discriminator between the two disciplines (the "proves nothing" item of issue #413: a FIFO run whose
// emulator applies per-flow fairness flatters a layout with separate connections). A saturating flow and a
// one-byte flow share the link. The small flow's byte waits about the queue's size over the rate behind the
// bulk flow's bytes in FIFO mode, and about one chunk's time in fair mode.
func TestAFifoQueueDelaysASmallFlowByItsContentsAndFairQueuesDoNot(t *testing.T) {
	const (
		rate  = 4e6
		chunk = 16 * 1024
		size  = 512 * 1024 // 128 ms of the link
	)
	queueTime := time.Duration(float64(size) / rate * float64(time.Second))
	chunkTime := time.Duration(float64(chunk) / rate * float64(time.Second))
	delays := func(fifo bool) []time.Duration {
		l := newLink(rate, rate, 0, chunk, 64*1024)
		if fifo {
			l.setFIFO(size)
		}
		bulkUp, _ := sink(t)
		tokenUp, tokenFirst := sink(t)
		errc := make(chan error, 2)
		bulkLn, err := l.listen("127.0.0.1:0", bulkUp, errc)
		if err != nil {
			t.Fatal(err)
		}
		tokenLn, err := l.listen("127.0.0.1:0", tokenUp, errc)
		if err != nil {
			t.Fatal(err)
		}
		defer bulkLn.Close()
		defer tokenLn.Close()
		bulk, err := net.Dial("tcp4", bulkLn.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer bulk.Close()
		go func() { _, _ = bulk.Write(make([]byte, 6<<20)) }() // 1.5 s of the link, so it saturates it
		// The queue is full, or (fair mode) the link has been carrying the bulk flow for a while: observable
		// state, not a sleep.
		waitUntil(t, 20*time.Second, "the bulk flow saturating the link", func() bool {
			if fifo {
				l.upQ.mu.Lock()
				defer l.upQ.mu.Unlock()
				return l.upQ.occ >= size-chunk
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			return len(l.conns) > 0 && l.conns[0].up.Load() > 256*1024
		})
		token, err := net.Dial("tcp4", tokenLn.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer token.Close()
		var out []time.Duration
		for i := 0; i < 3; i++ {
			sent := time.Now()
			if _, err := token.Write([]byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
			select {
			case at := <-tokenFirst:
				out = append(out, at.Sub(sent))
			case <-time.After(20 * time.Second):
				t.Fatal("the small flow's byte never arrived")
			}
		}
		return out
	}
	median := func(d []time.Duration) time.Duration { return sorted(d)[len(d)/2] }
	fifo, fair := delays(true), delays(false)
	t.Logf("queue time %v, chunk time %v; the small flow's delays: FIFO %v, fair %v", queueTime, chunkTime, fifo, fair)
	// FIFO: behind most of the queue (the queue's size, less the chunk that was in transmission, and the
	// small flow's own chunk on top). Only a lower bound is firm: a loaded host makes the link slower.
	if m := median(fifo); m < queueTime/2 {
		t.Errorf("FIFO: the small flow's byte arrived after %v, under half of the queue's %v: the queue did not delay it", m, queueTime)
	}
	// Fair: one chunk of the bulk flow ahead of it at most, a few chunk times with scheduling slack.
	if m := median(fair); m > queueTime/2 {
		t.Errorf("fair: the small flow's byte arrived after %v, over half of the FIFO queue's %v: the fair link queued it behind the bulk flow", m, queueTime)
	}
}

func sorted(d []time.Duration) []time.Duration {
	out := append([]time.Duration(nil), d...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("not within %v: %s", d, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Bytes pass intact and in order through a FIFO queue, two flows at once, the queue never holds more than its
// size, and its statistics show it was full (the run's evidence that the discipline was in force).
func TestAFifoQueueCarriesTwoFlowsIntactWithinItsSize(t *testing.T) {
	const rate, size = 4e6, 64 * 1024
	l := newLink(rate, rate, 5*time.Millisecond, 16*1024, 64*1024)
	l.setFIFO(size)
	errc := make(chan error, 1)
	ln, err := l.listen("127.0.0.1:0", echo(t), errc)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payloads := [][]byte{make([]byte, 768*1024), make([]byte, 256*1024)}
	for _, p := range payloads {
		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, len(payloads))
	start := time.Now()
	for _, p := range payloads {
		go func() {
			c, err := net.Dial("tcp4", ln.Addr().String())
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			go func() { _, _ = c.Write(p); _ = c.(*net.TCPConn).CloseWrite() }()
			got, err := io.ReadAll(c)
			if err == nil && !bytes.Equal(got, p) {
				err = io.ErrUnexpectedEOF
			}
			done <- err
		}()
	}
	for range payloads {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// Both flows share one rate each way: the bytes cannot cross faster than the link carries them (a floor
	// only: a loaded host makes the link slower, never faster).
	floor := time.Duration(float64(768*1024+256*1024) / rate * float64(time.Second))
	if took := time.Since(start); took < floor {
		t.Errorf("%d bytes crossed in %v, faster than the link's %v", 768*1024+256*1024, took, floor)
	}
	st := l.upQ.stat()
	if st.MaxBytes > size || st.MaxBytes < size-16*1024 || st.FullS <= 0 || st.Bytes != int64(768*1024+256*1024) {
		t.Errorf("the up queue's statistics: %+v; want a maximum between %d and %d, some time full, and %d bytes", st, size-16*1024, size, 768*1024+256*1024)
	}
}

// The flags refuse a queue that cannot work: a size below one chunk (no chunk would ever fit), a size in
// fair mode, an unlimited rate in FIFO mode, an unknown discipline.
func TestRunRefusesAQueueThatCannotWork(t *testing.T) {
	for name, c := range map[string]struct {
		queue string
		size  int
		rate  float64
	}{
		"below one chunk":     {"fifo", 100, 1e6},
		"a size in fair mode": {"fair", 1 << 20, 1e6},
		"no rate":             {"fifo", 1 << 20, 0},
		"unknown":             {"lifo", 1 << 20, 1e6},
	} {
		err := run([]string{"127.0.0.1:0=127.0.0.1:1"}, c.rate, c.rate, 0, 16*1024, 0, "", c.queue, c.size, "", "")
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

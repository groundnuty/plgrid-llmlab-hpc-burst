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

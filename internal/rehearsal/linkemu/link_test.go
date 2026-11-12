package linkemu

// Tests of the link's properties. A shaped link can't be made faster than its rate or its delay, so
// each test checks a lower bound on elapsed time, which the link's own clock guarantees, and an upper
// bound only to catch a hang.

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// echoUpstream echoes every connection's bytes and half-closes after the client's end of stream.
func echoUpstream(t *testing.T) string {
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

// sinkUpstream sends n random bytes to each connection, then closes it.
func sinkUpstream(t *testing.T, n int) (string, []byte) {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
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
				_, _ = c.Write(data)
			}()
		}
	}()
	return ln.Addr().String(), data
}

func newLink(t *testing.T, c Config) *Link {
	t.Helper()
	l, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return l
}

// TestLinkCarriesBytesBothWaysAndHalfCloses: what is sent arrives unchanged in both directions, and
// an end of stream reaches the other side after every byte before it.
func TestLinkCarriesBytesBothWaysAndHalfCloses(t *testing.T) {
	l := newLink(t, Config{UpRate: 64e6, DownRate: 64e6, Delay: time.Millisecond})
	addr, err := l.Listen(echoUpstream(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	data := make([]byte, 1<<20)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = c.Write(data)
		_ = c.(*net.TCPConn).CloseWrite()
	}()
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("the echo through the link: %d bytes, equal %v; want the %d sent", len(got), bytes.Equal(got, data), len(data))
	}
	if l.Up.Load() != int64(len(data)) || l.Down.Load() != int64(len(data)) {
		t.Errorf("the link counted %d up and %d down, want %d each", l.Up.Load(), l.Down.Load(), len(data))
	}
}

// download reads every byte one connection to addr gets, and returns them and how long it took.
func download(t *testing.T, addr string) ([]byte, time.Duration) {
	t.Helper()
	start := time.Now()
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Error(err)
		return nil, 0
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))
	b, err := io.ReadAll(c)
	if err != nil {
		t.Errorf("download: %v", err)
	}
	return b, time.Since(start)
}

// TestLinkKeepsItsRate: 4 MB down a link of 8 MB/s takes at least half a second, less the bucket's
// 2 ms slack; with no rate set it takes a fraction of that (the control: the bound is the link's).
func TestLinkKeepsItsRate(t *testing.T) {
	up, data := sinkUpstream(t, 4_000_000)
	shaped := newLink(t, Config{DownRate: 8e6})
	addr, err := shaped.Listen(up)
	if err != nil {
		t.Fatal(err)
	}
	got, took := download(t, addr)
	if !bytes.Equal(got, data) {
		t.Fatalf("got %d bytes, want the %d sent", len(got), len(data))
	}
	if took < 495*time.Millisecond {
		t.Errorf("4 MB at 8 MB/s took %s, below the rate's 500 ms", took)
	}
	free := newLink(t, Config{})
	addr, err = free.Listen(up)
	if err != nil {
		t.Fatal(err)
	}
	if _, took2 := download(t, addr); took2 >= took {
		t.Errorf("the unlimited link took %s, no faster than the shaped one's %s: the rate is not what slows it", took2, took)
	}
}

// TestLinkSharesOneRateAcrossConnections: two downloads at once share one rate, so 2 MB each at
// 8 MB/s in all take at least half a second together.
func TestLinkSharesOneRateAcrossConnections(t *testing.T) {
	up, _ := sinkUpstream(t, 2_000_000)
	l := newLink(t, Config{DownRate: 8e6})
	addr, err := l.Listen(up)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); download(t, addr) }()
	}
	wg.Wait()
	if took := time.Since(start); took < 495*time.Millisecond {
		t.Errorf("two 2 MB downloads at 8 MB/s in all took %s, below the shared rate's 500 ms", took)
	}
}

// TestLinkAddsItsDelay: a round trip of one byte through a link with a 20 ms one-way delay takes at
// least 40 ms.
func TestLinkAddsItsDelay(t *testing.T) {
	l := newLink(t, Config{Delay: 20 * time.Millisecond})
	addr, err := l.Listen(echoUpstream(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	b := []byte{42}
	start := time.Now()
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	if rtt := time.Since(start); rtt < 40*time.Millisecond {
		t.Errorf("a round trip took %s, below twice the 20 ms delay", rtt)
	}
}

// TestLinkIsLoopbackOnlyAndCloses: an upstream off loopback is refused; Close cuts a live connection
// and returns, and a later Listen is refused.
func TestLinkIsLoopbackOnlyAndCloses(t *testing.T) {
	l := newLink(t, Config{})
	if _, err := l.Listen("192.0.2.1:80"); err == nil {
		t.Error("an upstream off loopback was accepted")
	}
	addr, err := l.Listen(echoUpstream(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(c, one); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err := c.Read(one); err == nil {
		t.Error("a connection survived Close")
	}
	if _, err := l.Listen(echoUpstream(t)); err == nil {
		t.Error("a closed link took a new listener")
	}
}

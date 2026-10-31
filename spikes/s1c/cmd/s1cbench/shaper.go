package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// bucket is one link of a fixed rate shared by every connection that uses it. A chunk of n bytes
// occupies the link for n/rate; a chunk that arrives while the link is busy waits until the link is
// free. Each connection direction has at most one chunk waiting, so connections take turns: a
// shared bottleneck with per-connection queues (like a fair-queueing router), not one FIFO.
type bucket struct {
	mu   sync.Mutex
	rate float64 // bytes per second; 0 = unlimited
	next time.Time
}

func (b *bucket) wait(n int) {
	if b == nil || b.rate <= 0 {
		return
	}
	b.mu.Lock()
	now := time.Now()
	if b.next.Before(now) {
		b.next = now
	}
	b.next = b.next.Add(time.Duration(float64(n) / b.rate * float64(time.Second)))
	done := b.next
	b.mu.Unlock()
	time.Sleep(time.Until(done))
}

// runShaper proxies each -map listen=upstream pair. Data from the connecting side to the upstream
// ("up") shares one -rate bucket across all maps and connections; the other direction shares
// -down-rate. Ends of stream are passed on as half-closes.
func runShaper(args []string) error {
	fs := flag.NewFlagSet("shaper", flag.ContinueOnError)
	var maps multiFlag
	fs.Var(&maps, "map", "listen=upstream, both 127.0.0.1:port (repeatable)")
	rate := fs.Float64("rate", 0, "bytes/s shared by all connections, connecting side to upstream (0 = unlimited)")
	downRate := fs.Float64("down-rate", 0, "bytes/s shared by all connections, upstream to connecting side")
	chunk := fs.Int("chunk", 16*1024, "largest piece read and sent at once")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(maps) == 0 {
		return fmt.Errorf("at least one -map")
	}
	up, down := &bucket{rate: *rate}, &bucket{rate: *downRate}
	errc := make(chan error, len(maps))
	for _, m := range maps {
		listen, upstream, ok := strings.Cut(m, "=")
		if !ok {
			return fmt.Errorf("-map %q: want listen=upstream", m)
		}
		for _, a := range []string{listen, upstream} {
			if err := requireLoopback(a); err != nil {
				return err
			}
		}
		ln, err := net.Listen("tcp4", listen)
		if err != nil {
			return err
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					errc <- err
					return
				}
				go shapeConn(c, upstream, up, down, *chunk)
			}
		}()
	}
	return <-errc
}

func shapeConn(c net.Conn, upstream string, up, down *bucket, chunk int) {
	u, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		c.Close()
		return
	}
	for _, x := range []net.Conn{c, u} {
		_ = x.(*net.TCPConn).SetNoDelay(true)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pipe(u, c, up, chunk) }()
	go func() { defer wg.Done(); pipe(c, u, down, chunk) }()
	wg.Wait()
	c.Close()
	u.Close()
}

func pipe(dst, src net.Conn, b *bucket, chunk int) {
	buf := make([]byte, chunk)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			b.wait(n)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				src.Close()
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				_ = dst.(*net.TCPConn).CloseWrite()
			} else {
				dst.Close()
			}
			return
		}
	}
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

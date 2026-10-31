package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Expected values below come from the inputs each test sets (a fixture's token count, a configured
// rate), never from the code under test.

func TestPercentileNearestRank(t *testing.T) {
	xs := []float64{5, 1, 4, 2, 3, 10, 9, 8, 7, 6}
	d := summarize(xs)
	if d.P50 != 5 || d.P95 != 10 || d.Min != 1 || d.Max != 10 || d.N != 10 || d.P5 != 1 {
		t.Fatalf("got %+v", d)
	}
	if (summarize(nil) != dist{}) {
		t.Fatal("empty sample must give zeros")
	}
}

func TestRequireLoopback(t *testing.T) {
	for _, bad := range []string{"0.0.0.0:1", ":29400", "10.0.0.1:1", "localhost:1"} {
		if requireLoopback(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if requireLoopback("127.0.0.1:29400") != nil {
		t.Error("loopback refused")
	}
}

// fakeEngine streams want content chunks in the simulator's SSE shape, gap apart.
func fakeEngine(t *testing.T, want int, gap time.Duration, cut bool) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"finish_reason\":null,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		fl.Flush()
		for i := 0; i < want; i++ {
			time.Sleep(gap)
			fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"finish_reason\":null,\"delta\":{\"content\":\"w%d \"}}]}\n\n", i)
			fl.Flush()
		}
		if cut {
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"finish_reason\":\"length\",\"delta\":{}}]}\n\ndata: [DONE]\n\n")
	}))
	return ln.Addr().String()
}

func TestStreamOnceCountsTokensAndTiming(t *testing.T) {
	addr := fakeEngine(t, 20, 10*time.Millisecond, false)
	r := streamOnce(context.Background(), newClient(addr, ""), "sim", 20)
	if !r.Complete() {
		t.Fatalf("not complete: %+v err=%v", len(r.Tokens), r.Err)
	}
	if r.TTFT < 10*time.Millisecond {
		t.Fatalf("TTFT %v shorter than the fixture's first gap", r.TTFT)
	}
	if span := r.Tokens[19] - r.Tokens[0]; span < 19*10*time.Millisecond {
		t.Fatalf("19 gaps of 10 ms took only %v", span)
	}
}

func TestStreamOnceCutIsAFailure(t *testing.T) {
	addr := fakeEngine(t, 5, time.Millisecond, true)
	r := streamOnce(context.Background(), newClient(addr, ""), "sim", 5)
	if r.Complete() {
		t.Fatal("a reply without finish_reason and [DONE] counted as complete")
	}
	addr = fakeEngine(t, 4, time.Millisecond, false)
	if r := streamOnce(context.Background(), newClient(addr, ""), "sim", 5); r.Complete() {
		t.Fatal("4 tokens for a 5-token request counted as complete")
	}
}

// fakeMux answers CONNECT like frps's tcpmux port: 200 and a pipe to backend for host "inst",
// 404 for anything else.
func fakeMux(t *testing.T, backend string) string {
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
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				line, _ := br.ReadString('\n')
				for {
					l, err := br.ReadString('\n')
					if err != nil || l == "\r\n" {
						break
					}
				}
				if !strings.HasPrefix(line, "CONNECT inst:80 ") {
					fmt.Fprint(c, "HTTP/1.1 404 Not Found\r\n\r\n")
					return
				}
				b, err := net.Dial("tcp4", backend)
				if err != nil {
					return
				}
				defer b.Close()
				fmt.Fprint(c, "HTTP/1.1 200 OK\r\n\r\n")
				go io.Copy(b, br)
				io.Copy(c, b)
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestDialPathThroughMux(t *testing.T) {
	mux := fakeMux(t, fakeEngine(t, 3, time.Millisecond, false))
	if r := streamOnce(context.Background(), newClient(mux, "inst"), "sim", 3); !r.Complete() {
		t.Fatalf("stream through CONNECT failed: %v", r.Err)
	}
	if out, _ := probeOnce(mux, "nobody", "/", time.Second); out != "refused" {
		t.Fatalf("unknown host: probe outcome %q, want refused", out)
	}
}

func TestProbeTimeoutIsNotRefused(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { // accepts, answers the CONNECT, then never replies: a tunnel nobody detected as dead
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = readHeaderBlock(c)
				fmt.Fprint(c, "HTTP/1.1 200 OK\r\n\r\n")
				time.Sleep(3 * time.Second)
				c.Close()
			}(c)
		}
	}()
	if out, _ := probeOnce(ln.Addr().String(), "inst", "/health", 300*time.Millisecond); out != "timeout" {
		t.Fatalf("hung path: outcome %q, want timeout", out)
	}
}

// bulkThrough measures the rate one bulk transfer gets through a shaper of the given rate.
func shaperAddr(t *testing.T, upstream string, rate float64) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	b := &bucket{rate: rate}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go shapeConn(c, upstream, &bucket{}, b, 16*1024)
		}
	}()
	return ln.Addr().String()
}

func bulkServer(t *testing.T) string {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go http.Serve(ln, http.HandlerFunc(bulkHandler))
	return ln.Addr().String()
}

func TestShaperSharesOneRate(t *testing.T) {
	const rate = 1 << 20 // 1 MiB/s, set here
	sh := shaperAddr(t, bulkServer(t), rate)
	type res struct {
		n    int64
		f, e time.Time
	}
	ch := make(chan res, 2)
	start := time.Now()
	for i := 0; i < 2; i++ { // two connections, each wanting 1 MiB
		go func() {
			n, f, e, _ := bulkFetch(sh, "", 1<<20, 0, 10*time.Second)
			ch <- res{n, f, e}
		}()
	}
	a, b := <-ch, <-ch
	if a.n != 1<<20 || b.n != 1<<20 {
		t.Fatalf("short reads: %d %d", a.n, b.n)
	}
	el := time.Since(start).Seconds()
	// 2 MiB at a shared 1 MiB/s takes about 2 s; separate per-connection limits would take 1 s.
	if el < 1.8 || el > 2.6 {
		t.Fatalf("2 MiB through a shared 1 MiB/s link took %.2f s, want about 2", el)
	}
}

func TestSlowReaderReadsSlowly(t *testing.T) {
	const rate = 64 * 1024
	n, f, e, err := bulkFetch(bulkServer(t), "", 1<<30, rate, 2*time.Second)
	if err == nil {
		t.Fatal("a slow reader of 1 GiB must give up at -bulk-max")
	}
	got := float64(n) / e.Sub(f).Seconds()
	if got > 1.3*rate || got < 0.6*rate {
		t.Fatalf("slow reader read %.0f B/s, want about %d", got, rate)
	}
}

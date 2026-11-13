package main

import (
	"bufio"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRelayCopiesBothWaysAndCounts(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "w.sock")
	ul, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ul.Close()
	go func() { // an echo server that answers after the client half-closes
		for {
			c, err := ul.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				b, _ := io.ReadAll(c)
				_, _ = c.Write(append([]byte("echo:"), b...))
			}()
		}
	}()
	ln, err := net.Listen("tcp", "127.0.0.1:0") // kernel-chosen
	if err != nil {
		t.Fatal(err)
	}
	p := &relayPair{listen: ln.Addr().String(), socket: sock}
	var wg sync.WaitGroup
	go serveRelay(ln, p, &wg)
	defer ln.Close()
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", p.listen)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write([]byte("hello"))
		_ = c.(*net.TCPConn).CloseWrite()
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		got, err := bufio.NewReader(c).ReadString(0)
		c.Close()
		if err != io.EOF || got != "echo:hello" {
			t.Fatalf("round trip %d: %q %v", i, got, err)
		}
	}
	if n := p.conns.Load(); n != 3 {
		t.Fatalf("counted %d connections, want 3", n)
	}
}

func TestRelayMapRefusesNonLoopback(t *testing.T) {
	for _, bad := range []string{"[::]:1=/s", ":1=/s", "127.0.0.1:1", "example.org:1=/s"} {
		if _, err := parseRelayMap(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	ps, err := parseRelayMap("127.0.0.1:30424=/r/worker-0.sock,127.0.0.1:30425=/r/worker-1.sock")
	if err != nil || len(ps) != 2 || ps[1].socket != "/r/worker-1.sock" {
		t.Fatalf("%v %v", ps, err)
	}
}

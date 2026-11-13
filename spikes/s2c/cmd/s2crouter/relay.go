package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// The relay: one loopback TCP listener per worker, each copying bytes both ways to the worker's
// engine socket. vllm-router takes only http:// worker URLs and the workers serve on Unix sockets as
// the agent starts them (vLLM --uds), so the two vllm-router arms reach the workers through it; the
// Envoy arms do not. It adds one loopback hop to those arms' time to first token (README, Method).
// It counts the connections it accepted per listener, so an arm can show it was used.

type relayPair struct {
	listen, socket string
	conns          atomic.Int64
}

func parseRelayMap(s string) ([]*relayPair, error) {
	var out []*relayPair
	for _, e := range strings.Split(s, ",") {
		l, sock, ok := strings.Cut(e, "=")
		if !ok || l == "" || sock == "" {
			return nil, fmt.Errorf("-map entry %q: want 127.0.0.1:<port>=<socket>", e)
		}
		host, _, err := net.SplitHostPort(l)
		if err != nil || host != "127.0.0.1" {
			return nil, fmt.Errorf("-map entry %q: listen on 127.0.0.1 only", e)
		}
		out = append(out, &relayPair{listen: l, socket: sock})
	}
	return out, nil
}

// serveRelay accepts on ln until it is closed, piping each connection to the pair's socket.
func serveRelay(ln net.Listener, p *relayPair, wg *sync.WaitGroup) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		p.conns.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			u, err := net.Dial("unix", p.socket)
			if err != nil {
				return
			}
			defer u.Close()
			done := make(chan struct{}, 2)
			pipe := func(dst, src net.Conn) {
				_, _ = io.Copy(dst, src)
				if cw, ok := dst.(interface{ CloseWrite() error }); ok {
					_ = cw.CloseWrite()
				}
				done <- struct{}{}
			}
			go pipe(u, c)
			go pipe(c, u)
			<-done
			<-done
		}()
	}
}

func relayMain(args []string) int {
	fs := flag.NewFlagSet("relay", flag.ContinueOnError)
	m := fs.String("map", "", "127.0.0.1:<port>=<socket>,...")
	counts := fs.String("counts", "", "file for the connection counts, written on SIGTERM; <file>.ready once listening")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *m == "" || *counts == "" {
		return fail("relay: -map and -counts are required")
	}
	pairs, err := parseRelayMap(*m)
	if err != nil {
		return fail("relay: %v", err)
	}
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	var lns []net.Listener
	var wg sync.WaitGroup
	for _, p := range pairs {
		ln, err := net.Listen("tcp", p.listen)
		if err != nil {
			for _, l := range lns {
				l.Close()
			}
			return fail("relay: %v", err)
		}
		lns = append(lns, ln)
		go serveRelay(ln, p, &wg)
	}
	if err := os.WriteFile(*counts+".ready", nil, 0o644); err != nil {
		return fail("relay: %v", err)
	}
	<-sigs
	for _, l := range lns {
		l.Close()
	}
	var b strings.Builder
	for _, p := range pairs {
		fmt.Fprintf(&b, "%s\t%d\n", p.listen, p.conns.Load())
	}
	if err := os.WriteFile(*counts, []byte(b.String()), 0o644); err != nil {
		return fail("relay: %v", err)
	}
	return 0
}

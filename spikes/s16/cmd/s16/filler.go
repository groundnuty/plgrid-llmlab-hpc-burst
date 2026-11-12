package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"net"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The filler: bulk bytes on the same capped link as the tunnels, in both
// directions, so the link sits at its cap while every tunnel carries its
// token streams (a 4-CPU runner cannot fill 50 Mb/s or more with tokens;
// approved by team-lead on 11 Nov 2026). The source dials -conns connections
// through the link; the sink, on the node's side, accepts them. Each end
// writes a fixed random block as fast as the link takes it and reads and
// drops what comes. The link's shaper sets the rate; the filler only offers.

type fillerCounts struct {
	Conns int   `json:"conns"`
	Sent  int64 `json:"sent_bytes"`
	Recv  int64 `json:"recv_bytes"`
	T     int64 `json:"t_ns"`
}

func runFiller(args []string) error {
	fs := flag.NewFlagSet("filler", flag.ContinueOnError)
	role := fs.String("role", "", "source (dials the link) or sink (listens behind it)")
	addr := fs.String("addr", "", "source: the link's filler port; sink: where to listen")
	conns := fs.Int("conns", 4, "source: connections")
	out := fs.String("out", "", "the byte counts, rewritten every 2 s and at the end")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireLoopback(*addr); err != nil {
		return err
	}
	if (*role != "source" && *role != "sink") || *out == "" || *conns < 1 {
		return errors.New("-role source|sink, -addr, -out and -conns >= 1 are required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	block := make([]byte, 32<<10)
	if _, err := rand.Read(block); err != nil {
		return err
	}
	var sent, recv atomic.Int64
	var open atomic.Int64
	var wg sync.WaitGroup
	serve := func(c net.Conn) {
		defer wg.Done()
		open.Add(1)
		defer open.Add(-1)
		go func() { <-ctx.Done(); c.Close() }()
		go func() {
			buf := make([]byte, 32<<10)
			for {
				n, err := c.Read(buf)
				recv.Add(int64(n))
				if err != nil {
					c.Close()
					return
				}
			}
		}()
		for ctx.Err() == nil {
			n, err := c.Write(block)
			sent.Add(int64(n))
			if err != nil {
				c.Close()
				return
			}
		}
	}
	write := func() error {
		return writeJSONFile(*out, fillerCounts{Conns: int(open.Load()), Sent: sent.Load(), Recv: recv.Load(), T: time.Now().UnixNano()})
	}
	if *role == "sink" {
		ln, err := net.Listen("tcp4", *addr)
		if err != nil {
			return err
		}
		go func() { <-ctx.Done(); ln.Close() }()
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				wg.Add(1)
				go serve(c)
			}
		}()
	} else {
		for i := 0; i < *conns; i++ {
			c, err := net.DialTimeout("tcp4", *addr, 10*time.Second)
			if err != nil {
				cancel()
				wg.Wait()
				return err
			}
			wg.Add(1)
			go serve(c)
		}
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			_ = write()
		case <-ctx.Done():
			wg.Wait()
			return write()
		}
	}
}

package testenv

import (
	"net"
	"sync"
	"testing"
)

// HoldRelay passes every TCP connection made to it on to target (frps's bind port), both ways.
// Once held (Hold), it stops passing what frps sends on the first connection, frpc's control
// connection, until Release. It reproduces #368 on purpose (internal/launcher's
// frps_held_test.go): frps lists a proxy online while frpc has not handled the answer to it. That
// needs transport.tcpMux off at both ends, so that each work connection is a connection of its
// own and passes freely. It listens on a port the kernel chooses, on 127.0.0.1, and closes
// everything at the end of the test.
type HoldRelay struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	n      int           // connections accepted and joined to target
	gate   chan struct{} // open while held, nil otherwise
	conns  []net.Conn
	closed bool
	// releaseOn, when not 0, is the connection (1 is the first) whose end on frpc's side releases
	// the hold (ReleaseWhenFrpcEnds).
	releaseOn int
}

// StartHoldRelay starts a relay to target, not held, and closes it and its connections when the
// test ends.
func StartHoldRelay(t *testing.T, target string) *HoldRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &HoldRelay{ln: ln, target: target}
	go r.serve()
	t.Cleanup(r.close)
	return r
}

// Port is the relay's port, which frpc dials as frps's.
func (r *HoldRelay) Port() int { return r.ln.Addr().(*net.TCPAddr).Port }

func (r *HoldRelay) serve() {
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", r.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = c.Close()
			_ = up.Close()
			return
		}
		r.n++
		idx := r.n
		r.conns = append(r.conns, c, up)
		r.mu.Unlock()
		frpcEnded := func() {
			r.mu.Lock()
			hit := r.releaseOn == idx
			r.mu.Unlock()
			if hit {
				r.Release()
			}
		}
		go r.pipe(up, c, false, frpcEnded)
		go r.pipe(c, up, idx == 1, nil)
	}
}

// pipe copies src to dst until either ends, then closes both; srcEnded, when not nil, runs when
// src ends. With holdable, each chunk read while the relay is held waits for the release before it
// is written.
func (r *HoldRelay) pipe(dst, src net.Conn, holdable bool, srcEnded func()) {
	defer func() { _ = src.Close(); _ = dst.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if holdable {
				r.mu.Lock()
				g := r.gate
				r.mu.Unlock()
				if g != nil {
					<-g
				}
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			if srcEnded != nil {
				srcEnded()
			}
			return
		}
	}
}

// ReleaseWhenFrpcEnds makes the end of connection n on frpc's side release the hold.
func (r *HoldRelay) ReleaseWhenFrpcEnds(n int) {
	r.mu.Lock()
	r.releaseOn = n
	r.mu.Unlock()
}

// Accepted is how many connections the relay has accepted and joined to its target.
func (r *HoldRelay) Accepted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Hold stops passing what frps sends on the first connection: each chunk read from then on waits
// for Release.
func (r *HoldRelay) Hold() {
	r.mu.Lock()
	if r.gate == nil {
		r.gate = make(chan struct{})
	}
	r.mu.Unlock()
}

// Release passes what the hold kept, and everything after it.
func (r *HoldRelay) Release() {
	r.mu.Lock()
	if r.gate != nil {
		close(r.gate)
		r.gate = nil
	}
	r.mu.Unlock()
}

func (r *HoldRelay) close() {
	r.mu.Lock()
	r.closed = true
	conns := r.conns
	r.mu.Unlock()
	r.Release()
	_ = r.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

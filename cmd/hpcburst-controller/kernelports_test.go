package main

// A model of the kernel's port table, for the tests of the port allocator (held_test.go) and of the
// tunnel rig's move to a fresh block (tunnel_frps_block_test.go).
//
// holdBlock asks the kernel for a port (":0") and then for the ports around it. Which port the
// kernel offers, and which ports strangers hold, decide whether a pick overlaps an earlier block;
// on a real machine neither is the test's to choose, so a test that needs the overlap would pass
// whenever the kernel happened not to offer it. This model binds the way the kernel does (a
// second bind of a port in use fails with "address already in use", a close frees it), and gives
// the ":0" binds the test's scripted ports before its own sequence.

import (
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
)

type kernelPorts struct {
	t       *testing.T
	mu      sync.Mutex
	bound   map[int]*modelListener // a port in use; a nil listener is a stranger's socket
	offers  []int                  // what the next binds of port 0 get, in order
	offered []int                  // the offers handed out so far
	seq     int                    // the port a bind of port 0 gets once the offers are used up
}

// newKernelPorts makes holdBlock and holdLoopback bind through the model for the test. Binds of
// port 0 get the scripted offers, then seq, seq+50, ... (blocks of the sequence never overlap).
func newKernelPorts(t *testing.T, seq int) *kernelPorts {
	t.Helper()
	kp := &kernelPorts{t: t, bound: map[int]*modelListener{}, seq: seq}
	setVars(t, &listenTCP, kp.listen)
	return kp
}

func (kp *kernelPorts) listen(network, addr string) (net.Listener, error) {
	_, ps, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(ps)
	if err != nil {
		return nil, err
	}
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if port == 0 {
		port = kp.choose()
	} else if _, used := kp.bound[port]; used {
		return nil, &net.OpError{Op: "listen", Net: network, Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
			Err: os.NewSyscallError("bind", syscall.EADDRINUSE)}
	}
	ln := &modelListener{kp: kp, port: port}
	kp.bound[port] = ln
	return ln, nil
}

// choose is the port the kernel offers a bind of port 0. It never offers a port in use.
func (kp *kernelPorts) choose() int {
	if len(kp.offers) > 0 {
		p := kp.offers[0]
		kp.offers = kp.offers[1:]
		if _, used := kp.bound[p]; used {
			kp.t.Fatalf("the script offers port %d, which is in use: the kernel never does", p)
		}
		kp.offered = append(kp.offered, p)
		return p
	}
	for {
		p := kp.seq
		kp.seq += 50
		if _, used := kp.bound[p]; !used {
			return p
		}
	}
}

// stranger puts a socket of another process on the port, which it holds for the rest of the test.
func (kp *kernelPorts) stranger(port int) {
	kp.t.Helper()
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if _, used := kp.bound[port]; used {
		kp.t.Fatalf("port %d is in use already", port)
	}
	kp.bound[port] = nil
}

// strangerLeaves frees a port a stranger held: a client socket whose connection ended.
func (kp *kernelPorts) strangerLeaves(port int) {
	kp.t.Helper()
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if l, used := kp.bound[port]; !used || l != nil {
		kp.t.Fatalf("no stranger holds port %d", port)
	}
	delete(kp.bound, port)
}

// strangerHolds reports whether a stranger's socket is on the port.
func (kp *kernelPorts) strangerHolds(port int) bool {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	l, used := kp.bound[port]
	return used && l == nil
}

// heldAt reports whether addr is held, as a block's reservation: a listener of the model is bound
// to it, open, and it is the one heldAddrs has for addr.
func (kp *kernelPorts) heldAt(addr string) bool {
	port := portOf(kp.t, addr)
	kp.mu.Lock()
	l := kp.bound[port]
	kp.mu.Unlock()
	heldAddrs.mu.Lock()
	e := heldAddrs.m[addr]
	heldAddrs.mu.Unlock()
	return l != nil && e != nil && e.ln == net.Listener(l)
}

// releasedAt reports whether addr is released: heldAddrs has no entry for it and no listener of
// the model is bound to it (a stranger's socket may be).
func (kp *kernelPorts) releasedAt(addr string) bool {
	port := portOf(kp.t, addr)
	kp.mu.Lock()
	l := kp.bound[port]
	kp.mu.Unlock()
	heldAddrs.mu.Lock()
	e := heldAddrs.m[addr]
	heldAddrs.mu.Unlock()
	return l == nil && e == nil
}

func portOf(t testing.TB, addr string) int {
	t.Helper()
	_, ps, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(ps)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// modelListener is a bound port of the model: it accepts nothing, and closing it frees the port.
type modelListener struct {
	kp   *kernelPorts
	port int
}

func (l *modelListener) Accept() (net.Conn, error) {
	return nil, errors.New("a model listener accepts nothing")
}

func (l *modelListener) Close() error {
	l.kp.mu.Lock()
	defer l.kp.mu.Unlock()
	if l.kp.bound[l.port] == l {
		delete(l.kp.bound, l.port)
	}
	return nil
}

func (l *modelListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: l.port}
}

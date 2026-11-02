//go:build linux

package ctltest

import (
	"net"
	"net/netip"
	"syscall"
	"testing"
	"time"
)

// startFullQueue listens with a backlog of 0 and fills the accept queue, which on Linux holds one connection: the
// kernel drops every later SYN, and a connect neither completes nor fails until the caller's own timeout.
func startFullQueue(t testing.TB, host string, port int) ReachStub {
	t.Helper()
	ap, err := netip.ParseAddr(host)
	if err != nil || !ap.Is4() {
		t.Fatalf("the full-queue stub needs an IPv4 address, got %q", host)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	sa := &syscall.SockaddrInet4{Port: port, Addr: ap.As4()}
	if err := syscall.Bind(fd, sa); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Fatalf("listen: %v", err)
	}
	got, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatalf("getsockname: %v", err)
	}
	stub := ReachStub{Host: host, Port: got.(*syscall.SockaddrInet4).Port}
	// The one connection the queue holds, never accepted. The next connect must not complete: check it, so a kernel
	// that behaves otherwise fails here and not as a probe that "timed out" for another reason.
	filler, err := net.DialTimeout("tcp", stub.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("the first connection to the full-queue stub: %v", err)
	}
	t.Cleanup(func() { filler.Close() })
	if c, err := net.DialTimeout("tcp", stub.Addr(), 500*time.Millisecond); err == nil {
		c.Close()
		t.Fatal("the full-queue stub accepted a second connection: this kernel does not drop SYNs for a full accept queue")
	}
	return stub
}

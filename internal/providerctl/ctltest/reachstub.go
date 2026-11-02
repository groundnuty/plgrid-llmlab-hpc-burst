package ctltest

// Loopback stand-ins for the gateway's forwarded port, for the tests of the reach probe (issue #225;
// internal/providerctl/conformance, hack/livechecks). Each is a real socket the real probe connects to, so an
// outcome the test asserts is one the probe's own commands saw: a stub that always connected would prove nothing.

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// ReachKind is what a stand-in does when the probe connects.
type ReachKind string

const (
	// StubSSH sends an SSH banner as soon as the connection is made.
	StubSSH ReachKind = "ssh"
	// StubTLS answers a TLS ClientHello with a TLS handshake record and sends nothing first.
	StubTLS ReachKind = "tls"
	// StubData sends a line that is not an SSH banner as soon as the connection is made.
	StubData ReachKind = "data"
	// StubSilent accepts the connection and says nothing, whatever it is sent.
	StubSilent ReachKind = "silent"
	// StubRefused is a port nothing listens on: the connect is refused.
	StubRefused ReachKind = "refused"
	// StubTimeout is a listener whose accept queue is full and never drained, so the kernel drops every further SYN
	// and a connect hangs until the caller gives up (Linux).
	StubTimeout ReachKind = "timeout"
	// StubUnresolved is a name under .invalid (RFC 6761), which never resolves.
	StubUnresolved ReachKind = "unresolved"
)

// ReachStub is a running stand-in: the host and port a probe is told to connect to.
type ReachStub struct {
	Host string
	Port int
	// accepted counts the connections the stand-in accepted; nil for a kind that listens for none.
	accepted *atomic.Int64
}

// Accepted is how many connections the stand-in has accepted: a test that asserts a probe's outcome checks it saw
// the probe, so an outcome is never one that a stub of another address produced.
func (s ReachStub) Accepted() int {
	if s.accepted == nil {
		return 0
	}
	return int(s.accepted.Load())
}

// counted is a listener that counts what it accepts.
type counted struct {
	net.Listener
	n *atomic.Int64
}

func (c counted) Accept() (net.Conn, error) {
	conn, err := c.Listener.Accept()
	if err == nil {
		c.n.Add(1)
	}
	return conn, err
}

// Addr is "host:port".
func (s ReachStub) Addr() string { return net.JoinHostPort(s.Host, strconv.Itoa(s.Port)) }

// StartReachStub starts a stand-in of the kind on host (an address of the loopback network, such as 127.0.0.1 or
// any 127.x.y.z on Linux; StubUnresolved ignores it and returns a name under .invalid) and port (0 for the kernel's
// choice), and stops it when the test ends. The speaking kinds count what they accept (Accepted).
func StartReachStub(t testing.TB, kind ReachKind, host string, port int) ReachStub {
	t.Helper()
	switch kind {
	case StubUnresolved:
		return ReachStub{Host: "no-such-gateway.invalid", Port: 9}
	case StubRefused:
		l := listen(t, host, port)
		s := addrOf(l)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		return s
	case StubTimeout:
		return startFullQueue(t, host, port)
	case StubTLS:
		return startTLS(t, host, port)
	case StubSSH, StubData, StubSilent:
		return startPlain(t, kind, host, port)
	}
	t.Fatalf("unknown stub kind %q", kind)
	return ReachStub{}
}

func listen(t testing.TB, host string, port int) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("the stub cannot listen: %v", err)
	}
	return l
}

func addrOf(l net.Listener) ReachStub {
	a := l.Addr().(*net.TCPAddr)
	return ReachStub{Host: a.IP.String(), Port: a.Port}
}

func countedListen(t testing.TB, host string, port int) (counted, ReachStub) {
	t.Helper()
	l := counted{Listener: listen(t, host, port), n: new(atomic.Int64)}
	s := addrOf(l)
	s.accepted = l.n
	return l, s
}

// startPlain serves the kinds that speak (or keep silent) in clear.
func startPlain(t testing.TB, kind ReachKind, host string, port int) ReachStub {
	t.Helper()
	l, stub := countedListen(t, host, port)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				switch kind {
				case StubSSH:
					_, _ = io.WriteString(c, "SSH-2.0-ExampleGateway_1.0\r\n")
				case StubData:
					_, _ = io.WriteString(c, "220 example gateway ready\r\n")
				}
				_, _ = io.Copy(io.Discard, c) // until the probe closes
			}()
		}
	}()
	t.Cleanup(func() { l.Close(); wg.Wait() })
	return stub
}

// startTLS serves an HTTPS server on the address: it speaks only after the ClientHello.
func startTLS(t testing.TB, host string, port int) ReachStub {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.Listener.Close()
	l, stub := countedListen(t, host, port)
	srv.Listener = l
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the probe leaves mid-handshake, which is logged otherwise
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return stub
}

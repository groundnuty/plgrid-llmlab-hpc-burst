package front

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// DialFunc opens a connection to an Instance's serving path.
type DialFunc func(ctx context.Context) (net.Conn, error)

// Transports is the seam to HBEP-6's dial-home tunnel (HBEP-5, Reachability):
// a byte stream to each Instance's serving path, keyed by the Instance's UID
// from its registration. An Instance with no registered transport is not
// routable. How the bytes travel is HBEP-6's; the front keeps the HTTP layer
// (keep-alive, idle time-outs, the no-byte-written rule) for itself.
type Transports interface {
	// Dialer returns how to reach the Instance, or false when it has no
	// registered transport.
	Dialer(uid types.UID) (DialFunc, bool)
	// Subscribe registers f, called after every registration or removal.
	Subscribe(f func())
}

// StaticTransports is a Transports whose entries are TCP addresses set by
// hand, registered at once with no probe and dialled with no set-up deadline
// of their own. The tests use it for engine doubles on loopback; the front
// itself uses HBEP-6's Tunnel.
type StaticTransports struct {
	mu   sync.Mutex
	m    map[types.UID]string
	subs []func()
}

// NewStaticTransports returns an empty registry.
func NewStaticTransports() *StaticTransports { return &StaticTransports{m: map[types.UID]string{}} }

// Set registers addr (host:port) as the Instance's serving path.
func (s *StaticTransports) Set(uid types.UID, addr string) {
	s.mu.Lock()
	s.m[uid] = addr
	subs := append([]func(){}, s.subs...)
	s.mu.Unlock()
	for _, f := range subs {
		f()
	}
}

// Remove drops the Instance's transport, as a lost tunnel does.
func (s *StaticTransports) Remove(uid types.UID) {
	s.mu.Lock()
	delete(s.m, uid)
	subs := append([]func(){}, s.subs...)
	s.mu.Unlock()
	for _, f := range subs {
		f()
	}
}

// Dialer implements Transports.
func (s *StaticTransports) Dialer(uid types.UID) (DialFunc, bool) {
	s.mu.Lock()
	addr, ok := s.m[uid]
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}, true
}

// Subscribe implements Transports.
func (s *StaticTransports) Subscribe(f func()) {
	s.mu.Lock()
	s.subs = append(s.subs, f)
	s.mu.Unlock()
}

// dialError marks a failure to open a connection. Nothing of the request was
// written, so the front may send it once more (HBEP-5, open question 5).
type dialError struct{ err error }

func (e *dialError) Error() string { return "connect: " + e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// isDialError reports whether err is a connection that never opened: refused,
// unreachable, past connectTimeout toward LLMLab, or, toward an Instance, a
// tunnel set-up that frps refused or that passed tunnelSetupTimeout.
func isDialError(err error) bool {
	var d *dialError
	return errors.As(err, &d)
}

// upstreamTransport builds the HTTP transport toward a managed provider,
// whose connection set-up is bounded by connectTimeout (HBEP-5, Time-outs
// and keep-alive: connectTimeout applies to LLMLab only).
func upstreamTransport(dial func(ctx context.Context, network, addr string) (net.Conn, error), cfg Config, proxy bool) *http.Transport {
	return newUpstream(dial, cfg.ConnectTimeout.D(), cfg, proxy)
}

// newUpstream builds the HTTP transport toward one upstream. Its idle
// connections are dropped after upstreamIdleTimeout (4 s), below vLLM's 5 s
// keep-alive, so the front, which reuses connections, holds them the shorter
// time (L-2.5). It sets no response-header or total time-out: the front's
// deadlines apply per request. Compression is off, so bodies pass unchanged.
// setup, when positive, bounds each dial; toward an Instance it is zero,
// because the Instance's DialFunc bounds its own set-up (Tunnel.Dial:
// tunnelSetupTimeout over the dial to frps and the CONNECT exchange).
func newUpstream(dial func(ctx context.Context, network, addr string) (net.Conn, error), setup time.Duration, cfg Config, proxy bool) *http.Transport {
	t := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if setup > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, setup)
				defer cancel()
			}
			c, err := dial(ctx, network, addr)
			if err != nil {
				return nil, &dialError{err: err}
			}
			return &countingConn{Conn: c}, nil
		},
		IdleConnTimeout:     cfg.UpstreamIdleTimeout.D(),
		MaxIdleConnsPerHost: 64,
		DisableCompression:  true,
		TLSHandshakeTimeout: setup,
		ForceAttemptHTTP2:   false,
	}
	if proxy {
		t.Proxy = http.ProxyFromEnvironment
	}
	return t
}

// instanceTransports keeps one HTTP transport per Instance UID over its
// registered dialer.
type instanceTransports struct {
	cfg Config
	src Transports
	mu  sync.Mutex
	m   map[types.UID]*http.Transport
}

func newInstanceTransports(cfg Config, src Transports) *instanceTransports {
	it := &instanceTransports{cfg: cfg, src: src, m: map[types.UID]*http.Transport{}}
	src.Subscribe(it.prune)
	return it
}

// get returns the Instance's transport, or false with no registration.
func (it *instanceTransports) get(uid types.UID) (*http.Transport, bool) {
	dial, ok := it.src.Dialer(uid)
	if !ok {
		return nil, false
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	if t, ok := it.m[uid]; ok {
		return t, true
	}
	t := newUpstream(func(ctx context.Context, _, _ string) (net.Conn, error) {
		d, ok := it.src.Dialer(uid) // the current registration, after a reconnect
		if !ok {
			d = dial
		}
		return d(ctx)
	}, 0, it.cfg, false)
	it.m[uid] = t
	return t, true
}

// prune closes the transports of Instances that lost their registration.
func (it *instanceTransports) prune() {
	it.mu.Lock()
	defer it.mu.Unlock()
	for uid, t := range it.m {
		if _, ok := it.src.Dialer(uid); !ok {
			t.CloseIdleConnections()
			delete(it.m, uid)
		}
	}
}

// countingConn counts the bytes written to a connection, so a request can
// tell whether any byte of it reached the connection.
type countingConn struct {
	net.Conn
	written atomic.Int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.written.Add(int64(n))
	return n, err
}

// countingOf finds the countingConn under a connection the transport hands
// to httptrace: the conn itself, or the one under its TLS layer.
func countingOf(c net.Conn) *countingConn {
	if t, ok := c.(*tls.Conn); ok {
		c = t.NetConn()
	}
	cc, _ := c.(*countingConn)
	return cc
}

// errWritten refuses Go's own resend of a request that was written.
var errWritten = errors.New("the connection failed after the request was written; it is not sent again")

// withOneReplay sets the request's body and GetBody so Go's transport can
// resend it only when no byte of it reached the connection: the one resend
// HBEP-5 allows, a reused connection closed before the write (open question
// 5). Go resends on its own a request whose GetBody is set when nothing was
// written, but also, when the request carries Idempotency-Key or
// X-Idempotency-Key, after it was written and the connection then failed
// (Request.isReplayable). The client's headers pass unchanged, so GetBody
// refuses once the attempt wrote a byte (PR #95 review: Astra B1, Opus B2).
// The count comes from the connection, not from httptrace's WroteHeaders,
// which fires while the headers are still in the transport's buffer, so it
// would also refuse the resend of a request that wrote nothing.
func withOneReplay(req *http.Request, body []byte) *http.Request {
	var conn *countingConn
	var base int64
	// GotConn and GetBody both run on the RoundTrip goroutine, before the
	// attempt's write and after its failure; only the count is shared.
	trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
		if conn = countingOf(i.Conn); conn != nil {
			base = conn.written.Load()
		}
	}}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		if conn == nil || conn.written.Load() != base {
			return nil, errWritten
		}
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return req
}

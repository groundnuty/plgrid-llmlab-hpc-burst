package front

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

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
// hand. The tests use it for engine doubles on loopback; until HBEP-6's
// tunnel exists for alpha, it is also the only implementation, so no
// Instance is routable in production (HBEP-5, open question 13).
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
// unreachable, or past connectTimeout.
func isDialError(err error) bool {
	var d *dialError
	return errors.As(err, &d)
}

// upstreamTransport builds the HTTP transport toward one upstream. Its idle
// connections are dropped after upstreamIdleTimeout (4 s), below vLLM's 5 s
// keep-alive, so the front, which reuses connections, holds them the shorter
// time (L-2.5). It sets no response-header or total time-out: the front's
// deadlines apply per request. Compression is off, so bodies pass unchanged.
func upstreamTransport(dial func(ctx context.Context, network, addr string) (net.Conn, error), cfg Config, proxy bool) *http.Transport {
	// connectTimeout bounds every dial here, the Instances' too. HBEP-5 gives
	// it to LLMLab only and bounds the tunnel set-up toward an Instance by
	// tunnelSetupTimeout (5 s); that belongs to the tunnel transport of
	// #90, which replaces StaticTransports' plain dial.
	connect := cfg.ConnectTimeout.D()
	t := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, connect)
			defer cancel()
			c, err := dial(ctx, network, addr)
			if err != nil {
				return nil, &dialError{err: err}
			}
			return c, nil
		},
		IdleConnTimeout:     cfg.UpstreamIdleTimeout.D(),
		MaxIdleConnsPerHost: 64,
		DisableCompression:  true,
		TLSHandshakeTimeout: connect,
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
	t := upstreamTransport(func(ctx context.Context, _, _ string) (net.Conn, error) {
		d, ok := it.src.Dialer(uid) // the current registration, after a reconnect
		if !ok {
			d = dial
		}
		return d(ctx)
	}, it.cfg, false)
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

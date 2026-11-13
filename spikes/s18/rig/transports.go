package rig

import (
	"bytes"
	"context"
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// Transports is the front's seam to each Instance (internal/front's Transports), the spike's own: each
// Instance's dial is set by the rig, to its double's socket (arm a) or to its retry layer (arms b-d).
type Transports struct {
	mu   sync.Mutex
	dial map[types.UID]front.DialFunc
	subs []func()
}

// NewTransports returns an empty registry.
func NewTransports() *Transports { return &Transports{dial: map[types.UID]front.DialFunc{}} }

// Set registers uid's dial.
func (t *Transports) Set(uid types.UID, d front.DialFunc) {
	t.mu.Lock()
	t.dial[uid] = d
	subs := append([]func(){}, t.subs...)
	t.mu.Unlock()
	for _, f := range subs {
		f()
	}
}

// Dialer implements front.Transports.
func (t *Transports) Dialer(uid types.UID) (front.DialFunc, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	d, ok := t.dial[uid]
	return d, ok
}

// Subscribe implements front.Transports.
func (t *Transports) Subscribe(f func()) {
	t.mu.Lock()
	t.subs = append(t.subs, f)
	t.mu.Unlock()
}

// DialUnix dials a Unix socket.
func DialUnix(socket string) front.DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}
}

// idPattern is a request's ID in its prompt: the client's s18id-<run>-<n>/c<attempt>.
var idPattern = regexp.MustCompile(`s18id-[0-9A-Za-z]+-[0-9]+/c[0-9]+`)

// IDIn returns the first request ID in b, "" if none.
func IDIn(b []byte) string { return string(idPattern.Find(b)) }

// Cutter is fault F3: the tunnel closes after a marked request was written, before its response
// headers. It cuts once per request ID, on the first connection the ID is written to, so a retry of the
// same request passes: F3 is one failure, not a persistent one. While a marked request is in flight
// every byte the engine sends back is held, so the reader never sees the headers; once the engine's own
// log shows the request admitted (or after HoldFor), the connection is closed and the reader gets an
// error. The engine has then begun the request; its record says so, and that it was cancelled.
type Cutter struct {
	Marker  string        // "" : no F3
	HoldFor time.Duration // at most this long for the engine's admitted record
	// Times is how many connections of one ID are cut, 1 when unset: F3 is one failure. F3x2, a
	// tunnel that flaps under one request, cuts its first two, to measure arm (d)'s false stops (Q2).
	Times int

	mu   sync.Mutex
	done map[string]int
}

// claim reports whether id is cut now: one of its first Times marked writes.
func (c *Cutter) claim(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil {
		c.done = map[string]int{}
	}
	if id == "" || c.done[id] >= max(1, c.Times) {
		return false
	}
	c.done[id]++
	return true
}

// Wrap wraps a dial with the cutter; admitted reports whether the engine behind it has recorded id
// admitted.
func (c *Cutter) Wrap(d front.DialFunc, admitted func(id string) bool) front.DialFunc {
	if c == nil || c.Marker == "" {
		return d
	}
	return func(ctx context.Context) (net.Conn, error) {
		conn, err := d(ctx)
		if err != nil {
			return nil, err
		}
		cc := &cutConn{Conn: conn, c: c, admitted: admitted, ch: make(chan chunk, 4), quit: make(chan struct{})}
		cc.cond = sync.NewCond(&cc.mu)
		go cc.pump()
		return cc, nil
	}
}

// errCut is what the reader gets once F3 cut the connection.
var errCut = errors.New("s18: F3 closed the connection after the request was written")

type chunk struct {
	b   []byte
	err error
}

type cutConn struct {
	net.Conn
	c        *Cutter
	admitted func(string) bool

	wmu sync.Mutex
	cur []byte // the request being written

	mu      sync.Mutex
	cond    *sync.Cond
	hold    bool
	cut     bool
	pending []byte
	perr    error

	ch       chan chunk
	quit     chan struct{}
	quitOnce sync.Once
}

func (cc *cutConn) pump() {
	for {
		b := make([]byte, 32<<10)
		n, err := cc.Conn.Read(b)
		select {
		case cc.ch <- chunk{b[:n], err}:
		case <-cc.quit:
			return
		}
		if err != nil {
			return
		}
	}
}

func (cc *cutConn) Write(p []byte) (int, error) {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	cc.cur = append(cc.cur, p...)
	var marked string
	if bytes.Contains(cc.cur, []byte(cc.c.Marker)) {
		if id := IDIn(cc.cur); cc.c.claim(id) {
			// Held before a byte of it reaches the engine, so no reply byte can pass first.
			cc.mu.Lock()
			cc.hold = true
			cc.mu.Unlock()
			marked = id
		}
	}
	n, err := cc.Conn.Write(p)
	if marked != "" {
		go cc.decide(marked)
	}
	if l := requestLen(cc.cur); l > 0 {
		cc.cur = append([]byte(nil), cc.cur[l:]...)
	}
	return n, err
}

// decide waits for the engine's admitted record of id, at most HoldFor, then cuts.
func (cc *cutConn) decide(id string) {
	deadline := time.Now().Add(cc.c.HoldFor)
	for time.Now().Before(deadline) && !cc.admitted(id) {
		time.Sleep(5 * time.Millisecond)
	}
	cc.mu.Lock()
	cc.cut, cc.hold = true, false
	cc.cond.Broadcast()
	cc.mu.Unlock()
	_ = cc.Close()
}

func (cc *cutConn) Read(p []byte) (int, error) {
	for {
		cc.mu.Lock()
		for cc.hold {
			cc.cond.Wait()
		}
		if cc.cut {
			cc.mu.Unlock()
			return 0, errCut
		}
		if len(cc.pending) > 0 {
			n := copy(p, cc.pending)
			cc.pending = cc.pending[n:]
			cc.mu.Unlock()
			return n, nil
		}
		if cc.perr != nil {
			err := cc.perr
			cc.mu.Unlock()
			return 0, err
		}
		cc.mu.Unlock()
		select {
		case ck := <-cc.ch:
			cc.mu.Lock()
			cc.pending = append(cc.pending, ck.b...)
			if ck.err != nil {
				cc.perr = ck.err
			}
			cc.mu.Unlock()
		case <-cc.quit:
			return 0, net.ErrClosed
		}
	}
}

func (cc *cutConn) Close() error {
	cc.quitOnce.Do(func() { close(cc.quit) })
	return cc.Conn.Close()
}

// requestLen is the length of the whole HTTP/1.1 request at the start of b, its headers and its
// Content-Length body, or 0 while it is incomplete.
func requestLen(b []byte) int {
	end := bytes.Index(b, []byte("\r\n\r\n"))
	if end < 0 {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(b[:end]), "\r\n") {
		name, val, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, _ = strconv.Atoi(strings.TrimSpace(val))
		}
	}
	if len(b) < end+4+n {
		return 0
	}
	return end + 4 + n
}

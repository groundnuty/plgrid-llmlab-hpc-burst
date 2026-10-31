package front

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// errNoProxy is frps's 404 to a CONNECT: no tunnel is registered for that
// Instance UID (HBEP-6, frp facts: an unknown domain gets 404).
var errNoProxy = errors.New("frps has no proxy for this Instance (404)")

// Tunnel is the Transports of HBEP-6's dial-home tunnel (HBEP-5,
// Reachability; HBEP-6, The front's half). It reaches an Instance through
// frps's tcpmux port on loopback with CONNECT <UID>:80, and it counts an
// Instance's transport as registered only while the front's probe through it
// passes (Prober). Routing never rests on frps's view or on the Instance's
// TunnelConnected condition alone.
type Tunnel struct {
	muxAddr string
	setup   time.Duration

	mu   sync.Mutex
	reg  map[types.UID]bool
	subs []func()
}

// NewTunnel returns a Tunnel that dials frps's tcpmux port at muxAddr and
// bounds each connection's set-up by setup (tunnelSetupTimeout). No Instance
// is registered until a probe passes.
func NewTunnel(muxAddr string, setup time.Duration) *Tunnel {
	return &Tunnel{muxAddr: muxAddr, setup: setup, reg: map[types.UID]bool{}}
}

// Dial opens a connection to the Instance's engine through frps (HBEP-6, The
// front's half): it dials the tcpmux port, sends CONNECT <UID>:80 HTTP/1.1
// with Host <UID>:80, and reads frps's reply. On 200 the connection carries
// plain HTTP to the engine.
//
// tunnelSetupTimeout runs from the dial until the 200 or the 404, and it is
// the only deadline on the exchange: frps writes the 200 once the UID is
// routed, before it takes a work connection to frpc, so it bounds only the
// front's exchange with frps on loopback. A refused dial, a 404, another
// status or the deadline is an error, and no byte of a request was written:
// the HTTP transport turns it into a dialError, which is resent once and
// counts toward the Instance's pause (HBEP-5, open question 5; Pauses).
func (t *Tunnel) Dial(ctx context.Context, uid types.UID) (net.Conn, error) {
	if err := checkUID(uid); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, t.setup)
	defer cancel()
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", t.muxAddr)
	if err != nil {
		return nil, t.setupErr(ctx, uid, "dial frps", err)
	}
	deadline, _ := ctx.Deadline()
	_ = c.SetDeadline(deadline)
	// A request cut while it waits (its client left, the front stops) ends
	// the exchange at once rather than at the deadline.
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	ok := false
	defer func() {
		if !ok {
			stop()
			_ = c.Close()
		}
	}()
	authority := string(uid) + ":80"
	if _, err := io.WriteString(c, "CONNECT "+authority+" HTTP/1.1\r\nHost: "+authority+"\r\n\r\n"); err != nil {
		return nil, t.setupErr(ctx, uid, "send CONNECT", err)
	}
	// frps reads the CONNECT through its own buffer and drops what follows
	// it (pkg/util/tcpmux/httpconnect.go), so nothing is written before the
	// reply.
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return nil, t.setupErr(ctx, uid, "read frps's reply", err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("CONNECT %s: %w", uid, errNoProxy)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("CONNECT %s: frps answered %s", uid, resp.Status)
	}
	if !stop() {
		// The context ended as the reply came: the deadline is in the past.
		return nil, t.setupErr(ctx, uid, "read frps's reply", ctx.Err())
	}
	_ = c.SetDeadline(time.Time{})
	ok = true
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: c, r: br}, nil
	}
	return c, nil
}

// setupErr names the step that failed, and says when the set-up deadline
// cut it. The connection's deadline is the context's, and may fire first.
func (t *Tunnel) setupErr(ctx context.Context, uid types.UID, step string, err error) error {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return fmt.Errorf("CONNECT %s: %s: cancelled: %w", uid, step, err)
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("CONNECT %s: %s: no answer within tunnelSetupTimeout (%s): %w", uid, step, t.setup, err)
	}
	return fmt.Errorf("CONNECT %s: %s: %w", uid, step, err)
}

// checkUID refuses a UID that cannot stand in a request line as it is. A
// Kubernetes UID is a UUID; anything else is a bug upstream, so it fails at
// once rather than reaching frps.
func checkUID(uid types.UID) error {
	if uid == "" || len(uid) > 128 {
		return fmt.Errorf("instance UID %q is empty or too long for a CONNECT", uid)
	}
	for _, r := range string(uid) {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return fmt.Errorf("instance UID %q holds a character other than a letter, a digit or a hyphen", uid)
		}
	}
	return nil
}

// bufferedConn is a connection whose first bytes were already read into r.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// Dialer implements Transports: the CONNECT dialer, while the Instance's
// probe passes.
func (t *Tunnel) Dialer(uid types.UID) (DialFunc, bool) {
	if !t.Registered(uid) {
		return nil, false
	}
	return func(ctx context.Context) (net.Conn, error) { return t.Dial(ctx, uid) }, true
}

// Registered reports whether the Instance's transport counts as registered.
func (t *Tunnel) Registered(uid types.UID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reg[uid]
}

// Subscribe implements Transports.
func (t *Tunnel) Subscribe(f func()) {
	t.mu.Lock()
	t.subs = append(t.subs, f)
	t.mu.Unlock()
}

// setRegistered records the probe's verdict and returns the previous one.
// Subscribers are called only on a change, and outside the lock: the router
// they rebuild calls Dialer.
func (t *Tunnel) setRegistered(uid types.UID, on bool) (was bool) {
	t.mu.Lock()
	was = t.reg[uid]
	if on {
		t.reg[uid] = true
	} else {
		delete(t.reg, uid)
	}
	subs := append([]func(){}, t.subs...)
	t.mu.Unlock()
	if was != on {
		for _, f := range subs {
			f()
		}
	}
	return was
}

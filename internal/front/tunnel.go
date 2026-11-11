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
	"regexp"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// errNoProxy is frps's 404 to a CONNECT: no tunnel is registered for that
// Instance UID (HBEP-6, frp facts: an unknown domain gets 404).
var errNoProxy = errors.New("frps has no proxy for this Instance (404)")

// errReregistered is a dial that found, after frps's 200, that the Instance's
// proxy was registered again since the dial was routed: frps may already route
// the CONNECT to a registration frpc has not started, whose work connection
// would end before any reply (#368, #371). No byte of a request was written,
// so it is a dial error, resent once (HBEP-5, open question 5).
var errReregistered = errors.New("the Instance's tunnel was registered again after this request was routed; nothing was sent")

// Tunnel is the Transports of HBEP-6's dial-home tunnel (HBEP-5,
// Reachability; HBEP-6, The front's half). It reaches an Instance through
// frps's tcpmux port on loopback with CONNECT <UID>:80, and it counts an
// Instance's transport as registered only while the front's probe through it
// passes (Prober). Routing never rests on frps's view or on the Instance's
// TunnelConnected condition alone.
//
// Each UID has a registration epoch (#371; HBEP-6, Admission; HBEP-5). frps
// posts every NewProxy and CloseProxy it admits to the front's plugin
// (frpsPlugin) before it registers the proxy, and the plugin calls Bump: the
// epoch moves on and the UID stops counting as registered. A route proven in an
// earlier epoch no longer counts. A probe registers the UID only if the epoch
// has not moved since the probe began (setRegisteredAt), and a dial that a
// router obtained earlier checks the epoch after frps's 200 and before any byte
// of a request (Dialer).
type Tunnel struct {
	muxAddr string
	setup   time.Duration

	mu   sync.Mutex
	reg  map[types.UID]bool
	subs []func()
	// epoch is each UID's registration epoch; a UID that was never bumped is
	// at 0. Values other than 0 come from last, so none is given twice, and a
	// UID the front stops probing gets a fresh one (forget) rather than going
	// back to 0. An entry is kept for every UID that was ever bumped or probed:
	// a few bytes per Instance over the front's life.
	epoch map[types.UID]uint64
	last  uint64
}

// NewTunnel returns a Tunnel that dials frps's tcpmux port at muxAddr and
// bounds each connection's set-up by setup (tunnelSetupTimeout). No Instance
// is registered until a probe passes.
func NewTunnel(muxAddr string, setup time.Duration) *Tunnel {
	return &Tunnel{muxAddr: muxAddr, setup: setup, reg: map[types.UID]bool{}, epoch: map[types.UID]uint64{}}
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
	return t.dial(ctx, uid, nil)
}

// dial is Dial. With routed set, it also refuses the connection when the
// UID's epoch is no longer *routed by the time frps's 200 has been read: the
// CONNECT may then have reached a registration that frpc has not started.
func (t *Tunnel) dial(ctx context.Context, uid types.UID, routed *uint64) (net.Conn, error) {
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
	// A context that ends while the exchange waits ends it at once rather
	// than at the deadline. That cuts the probe's dial when the front stops,
	// but not a proxied request's: net/http dials with the request's context
	// detached from its cancellation (getConn's context.WithoutCancel,
	// net/http/transport.go:1596 in Go 1.27.3), so a CONNECT goes on up to
	// tunnelSetupTimeout after its client left, and the connection it opens
	// then waits in the idle pool for upstreamIdleTimeout.
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
	// frps's plugin call for a new registration comes before frps registers the
	// proxy, which comes before it routes this CONNECT and writes the 200. So if
	// frps routed the CONNECT to a registration newer than the one this dial was
	// routed for, the epoch had moved before the 200 was read.
	if routed != nil && t.Epoch(uid) != *routed {
		return nil, fmt.Errorf("CONNECT %s: %w", uid, errReregistered)
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

// uidForm is the agent's rule for the Instance UID it gives frpc as the
// proxy's name and its tcpmux domain (internal/agent/frpcconf.go, uidForm),
// matched exactly: a Kubernetes UID's alphabet, lowercase.
var uidForm = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// checkUID refuses a UID that frpc could not have registered, so it fails at
// once rather than reaching frps; a Kubernetes UID is a lowercase UUID, and
// anything else is a bug upstream.
func checkUID(uid types.UID) error {
	if !uidForm.MatchString(string(uid)) {
		return fmt.Errorf("instance UID %q is not a Kubernetes UID (%s), so no frpc registers it", uid, uidForm)
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
	t.mu.Lock()
	on, e := t.reg[uid], t.epoch[uid]
	t.mu.Unlock()
	if !on {
		return nil, false
	}
	// The dialer carries the epoch it was handed in: one kept past a new
	// registration (a router's, an Instance transport's) fails its next dial
	// cleanly instead of reaching a registration no probe has proven.
	return func(ctx context.Context) (net.Conn, error) { return t.dial(ctx, uid, &e) }, true
}

// Epoch returns the UID's registration epoch.
func (t *Tunnel) Epoch(uid types.UID) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.epoch[uid]
}

// Bump moves the UID's epoch on and takes away its registration: frps is
// about to register the UID's proxy again, or has closed it (frpsPlugin). It
// takes the lock for a few map writes only, and calls the subscribers in a
// goroutine of their own, because frps waits for the plugin's answer on the
// frpc session's control connection. They need not have run first: a dialer
// they hold carries the old epoch, and its next dial fails. It reports whether
// the UID was registered.
func (t *Tunnel) Bump(uid types.UID) (wasRegistered bool) {
	t.mu.Lock()
	t.last++
	t.epoch[uid] = t.last
	was := t.reg[uid]
	delete(t.reg, uid)
	subs := append([]func(){}, t.subs...)
	t.mu.Unlock()
	if was {
		go func() {
			for _, f := range subs {
				f()
			}
		}()
	}
	return was
}

// forget moves the UID's epoch on, for an Instance the front no longer probes,
// whose registration the prober has just taken away: a dialer handed out
// before can then never match again, even if the UID comes back.
func (t *Tunnel) forget(uid types.UID) {
	t.mu.Lock()
	t.last++
	t.epoch[uid] = t.last
	t.mu.Unlock()
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
	was, _ = t.setRegisteredAt(uid, on, nil)
	return was
}

// setRegisteredAt is setRegistered for a verdict reached in a given epoch: it
// registers the UID only while the epoch is still at, so a probe that passed
// through a registration replaced while it ran proves nothing (applied is
// false, and nothing changes). Taking a registration away always applies.
func (t *Tunnel) setRegisteredAt(uid types.UID, on bool, at *uint64) (was, applied bool) {
	t.mu.Lock()
	was = t.reg[uid]
	if on && at != nil && t.epoch[uid] != *at {
		t.mu.Unlock()
		return was, false
	}
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
	return was, true
}

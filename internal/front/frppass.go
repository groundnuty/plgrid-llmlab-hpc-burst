package front

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// frpWebsocketPath is the path of frpc's websocket upgrade. frps's bind port
// takes a connection as frp's websocket transport when its first bytes are
// exactly "GET /~!frp" (HBEP-6, frp facts: server/service.go, and
// pkg/util/net/websocket.go's FrpWebsocketPath, at frp v0.71.1).
const frpWebsocketPath = "/~!frp"

// frpPassthrough is the dial-home listener's half of the tunnel (HBEP-6,
// Dial-home modes: the dial-home listener's paths). The listener has
// terminated TLS with the launcher's certificate, in both modes; a GET /~!frp
// websocket upgrade is passed to frps's bind port on loopback, the request
// line unchanged, and the connection is then spliced both ways for as long as
// either side keeps it. Every other request gets 404.
type frpPassthrough struct {
	addr string        // frps's bind port (frpsBindAddr)
	dial time.Duration // how long the dial to frps may take
	log  *slog.Logger

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func newFrpPassthrough(addr string, dial time.Duration, log *slog.Logger) *frpPassthrough {
	return &frpPassthrough{addr: addr, dial: dial, log: log, conns: map[net.Conn]struct{}{}}
}

// isFrpUpgrade reports whether r is frpc's websocket upgrade: GET, the exact
// request target /~!frp (never a re-encoded or longer one), and Upgrade:
// websocket with Connection: upgrade.
func isFrpUpgrade(r *http.Request) bool {
	return r.Method == http.MethodGet && r.RequestURI == frpWebsocketPath && r.ProtoMajor == 1 &&
		headerHasToken(r.Header, "Upgrade", "websocket") && headerHasToken(r.Header, "Connection", "upgrade")
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

func (p *frpPassthrough) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isFrpUpgrade(r) {
		http.NotFound(w, r)
		return
	}
	up, err := net.DialTimeout("tcp", p.addr, p.dial)
	if err != nil {
		p.log.Warn("frps's bind port cannot be reached; the tunnel's upgrade is refused", "frps", p.addr, "error", err.Error())
		http.Error(w, "the tunnel server is not reachable", http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = up.Close()
		http.Error(w, "the dial-home listener cannot pass the tunnel through", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		_ = up.Close()
		p.log.Warn("the tunnel's upgrade could not take over its connection", "error", err.Error())
		return
	}
	// The server's header deadline is still on the connection; the tunnel
	// lives as long as its heartbeats keep it.
	_ = conn.SetDeadline(time.Time{})
	if !p.track(conn, up) {
		_ = conn.Close()
		_ = up.Close()
		return
	}
	defer p.untrack(conn, up)
	// The request line goes first and unchanged, so frps's match on its
	// first bytes sees "GET /~!frp" (HBEP-6). Go's server moved Host out of
	// the header map, so it is written back.
	var head bytes.Buffer
	head.WriteString(r.Method + " " + r.RequestURI + " " + r.Proto + "\r\n")
	head.WriteString("Host: " + r.Host + "\r\n")
	_ = r.Header.Write(&head)
	head.WriteString("\r\n")
	// Bytes the server read past the headers belong to the tunnel.
	if n := rw.Reader.Buffered(); n > 0 {
		b, _ := rw.Reader.Peek(n)
		head.Write(b)
	}
	if _, err := up.Write(head.Bytes()); err != nil {
		// untrack only forgets them, and the client's deadline is cleared.
		_ = conn.Close()
		_ = up.Close()
		p.log.Warn("the tunnel's upgrade could not be passed to frps", "error", err.Error())
		return
	}
	splice(conn, up)
}

// splice copies both ways until either side ends, then closes both.
func splice(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

func (p *frpPassthrough) track(cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	p.wg.Add(1)
	return true
}

func (p *frpPassthrough) untrack(cs ...net.Conn) {
	p.mu.Lock()
	for _, c := range cs {
		delete(p.conns, c)
	}
	p.mu.Unlock()
	p.wg.Done()
}

// Close cuts every tunnel passed through and refuses new ones, then waits
// for their handlers. http.Server.Shutdown does not track a hijacked
// connection, so the front calls this once its requests have ended: the
// tunnels carry those requests until then. Each frpc logs in again when the
// front is back (HBEP-6, Test plan: Reconnects).
func (p *frpPassthrough) Close() {
	p.mu.Lock()
	p.closed = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

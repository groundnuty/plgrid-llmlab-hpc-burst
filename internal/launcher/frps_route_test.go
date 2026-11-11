package launcher

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// A proxy that frps lists online is not yet a route (#368). Read at frp v0.71.1:
//   - frps lists a proxy online once it is in the proxy manager and in the stats (server/
//     control.go:886 and :766; server/http/controller.go:234-251), and answers a CONNECT for it
//     with 200 as soon as its route exists (control.go:876; pkg/util/vhost/vhost.go:184-193). Only
//     after both does it send frpc the NewProxyResp (control.go:768).
//   - frpc takes a work connection for the proxy only once it has handled that NewProxyResp, which
//     sets the proxy's phase to running (client/proxy/proxy_wrapper.go:139-165); before, it
//     closes the connection it is handed (proxy_wrapper.go:266-278).
//   - frpc reads transport.poolCount = 0 as unset and makes it 1 (pkg/config/v1/client.go:153),
//     so frps asks for one spare work connection at the login (server/control.go:446, :690-692)
//     and gives it to the first user connection without asking frpc (control.go:606-612). frpc
//     reads the StartWorkConn on that connection (server/proxy/proxy.go:176) in another goroutine
//     (client/control.go:146-161) than the one that handles the control stream's NewProxyResp
//     (pkg/msg/handler.go:170-188).
//
// So a CONNECT sent as soon as the proxy is listed online can get its 200 and then the end of the
// connection before any byte of a reply, since frps closes the user connection when the work
// connection ends (server/proxy/proxy.go:268-292). The product never routes on the listing: the
// front routes only after its own probe, a GET through the tunnel, has passed
// (internal/front/tunnel.go:150-157, probe.go:225; HBEP-6, The front's half). The tests wait the
// same way, with provenRoute.

// errClosedBeforeReply is that race's signature: frps answered the CONNECT with 200, and the
// connection ended before any byte of the reply. A reset counts as well as a close: frps may
// close the user connection with the request still unread, and the kernel then resets it.
var errClosedBeforeReply = errors.New("frps answered the CONNECT with 200, then the connection ended before any byte of a reply")

// tcpmuxGet sends CONNECT host:80 to frps's tcpmux port at addr, as the front does (HBEP-6, The
// front's half), and on a 200 sends GET path through the tunnel. It returns the CONNECT's status
// line and, after a 200, the reply's status code and the first 64 bytes of its body. Everything
// happens before deadline.
func tcpmuxGet(addr, host, path string, deadline time.Time) (status string, code int, body string, err error) {
	c, err := net.DialTimeout("tcp", addr, time.Until(deadline))
	if err != nil {
		return "", 0, "", err
	}
	defer c.Close()
	if err := c.SetDeadline(deadline); err != nil {
		return "", 0, "", err
	}
	if _, err := fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", host, host); err != nil {
		return "", 0, "", err
	}
	rd := bufio.NewReader(c)
	line, err := rd.ReadString('\n')
	status = strings.TrimSpace(line)
	if err != nil {
		return status, 0, "", fmt.Errorf("the CONNECT's status line: %w", err)
	}
	if !strings.Contains(status, " 200 ") {
		return status, 0, "", nil
	}
	for {
		l, err := rd.ReadString('\n')
		if err != nil {
			return status, 0, "", fmt.Errorf("the CONNECT's headers: %w", err)
		}
		if strings.TrimSpace(l) == "" {
			break
		}
	}
	if _, err := fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, host); err != nil {
		if endedEarly(err) {
			return status, 0, "", errClosedBeforeReply
		}
		return status, 0, "", fmt.Errorf("writing the request through the tunnel: %w", err)
	}
	if _, err := rd.Peek(1); err != nil {
		if endedEarly(err) {
			return status, 0, "", errClosedBeforeReply
		}
		return status, 0, "", fmt.Errorf("waiting for the reply through the tunnel: %w", err)
	}
	resp, err := http.ReadResponse(rd, nil)
	if err != nil {
		return status, 0, "", fmt.Errorf("the reply through the tunnel: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return status, resp.StatusCode, string(b), fmt.Errorf("the reply's body: %w", err)
	}
	return status, resp.StatusCode, string(b), nil
}

// endedEarly reports whether err is the end of the connection: a close, or a reset by the peer.
func endedEarly(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// provenRoute waits until a CONNECT for host through frps's tcpmux port at addr, and a GET path
// through it, bring back a 200 with the body want: only a reply that crossed the tunnel proves the
// route, since frps lists the proxy online, and answers its CONNECT with 200, before frpc can
// serve it (above). It retries errClosedBeforeReply alone, on a new connection each time; any
// other outcome (a CONNECT status other than 200, a timeout, another error, another reply) ends it
// at once with that error. When bound runs out it returns the last error. It returns how many
// attempts it made.
func provenRoute(addr, host, path, want string, bound time.Duration) (attempts int, err error) {
	var last error
	for end := time.Now().Add(bound); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		attempts++
		deadline := time.Now().Add(10 * time.Second)
		if deadline.After(end) {
			deadline = end
		}
		status, code, body, err := tcpmuxGet(addr, host, path, deadline)
		switch {
		case errors.Is(err, errClosedBeforeReply):
			last = err
			continue
		case err != nil:
			return attempts, fmt.Errorf("CONNECT %s: %q: %w", host, status, err)
		case !strings.Contains(status, " 200 "):
			return attempts, fmt.Errorf("CONNECT %s: %q, want 200", host, status)
		case code != http.StatusOK || body != want:
			return attempts, fmt.Errorf("CONNECT %s: the reply through the tunnel is %d %q, want 200 %q", host, code, body, want)
		}
		return attempts, nil
	}
	return attempts, fmt.Errorf("CONNECT %s: no reply crossed the tunnel in %d attempts within %s; the last: %v", host, attempts, bound, last)
}

// tailFile returns the last 2000 bytes of the file at path, or why it cannot be read.
func tailFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	if len(b) > 2000 {
		b = b[len(b)-2000:]
	}
	return string(b)
}

// holdRelay passes every TCP connection made to it on to target (frps's bind port), both ways. Once
// held, it stops passing what frps sends on the first connection, frpc's control connection,
// until release. That needs transport.tcpMux off at both ends, so that each work connection is a
// connection of its own and passes freely.
type holdRelay struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	n      int           // connections accepted and joined to target
	gate   chan struct{} // open while held, nil otherwise
	conns  []net.Conn
	closed bool
}

func startHoldRelay(t *testing.T, target string) *holdRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &holdRelay{ln: ln, target: target}
	go r.serve()
	t.Cleanup(r.close)
	return r
}

func (r *holdRelay) port() int { return r.ln.Addr().(*net.TCPAddr).Port }

func (r *holdRelay) serve() {
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
		control := r.n == 1
		r.conns = append(r.conns, c, up)
		r.mu.Unlock()
		go r.pipe(up, c, false)
		go r.pipe(c, up, control)
	}
}

// pipe copies src to dst until either ends, then closes both. With holdable, each chunk read
// while the relay is held waits for the release before it is written.
func (r *holdRelay) pipe(dst, src net.Conn, holdable bool) {
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
			return
		}
	}
}

func (r *holdRelay) accepted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

func (r *holdRelay) hold() {
	r.mu.Lock()
	if r.gate == nil {
		r.gate = make(chan struct{})
	}
	r.mu.Unlock()
}

func (r *holdRelay) release() {
	r.mu.Lock()
	if r.gate != nil {
		close(r.gate)
		r.gate = nil
	}
	r.mu.Unlock()
}

func (r *holdRelay) close() {
	r.mu.Lock()
	r.closed = true
	conns := r.conns
	r.mu.Unlock()
	r.release()
	_ = r.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

// TestFrpsRealListsAProxyOnlineBeforeFrpcCanServeIt is #368 on purpose: frps lists the proxy
// online and answers its CONNECT with 200 while frpc has not yet handled frps's NewProxyResp, and
// no reply crosses. A relay between frpc and frps holds that answer: it stops passing what frps
// sends on frpc's control connection from the plugin's NewProxy call, which frps makes before it
// registers the proxy and answers frpc (server/control.go:745, :748, :768). So the old wait
// (online, then one CONNECT) fails every time, provenRoute reports no route while the answer is
// held, and it passes once the answer is let through.
//
// frps and frpc run with transport.tcpMux off, so that the relay sees the control connection and
// the work connections apart: with it on, every connection from frpc is a yamux session whose
// streams carry both (server/service.go:746-770). That changes how frps's messages travel, not
// what frpc does with them: the code that closes the early work connection is the same either
// way (client/control.go:124-176, client/proxy/proxy_wrapper.go:266-278).
func TestFrpsRealListsAProxyOnlineBeforeFrpcCanServeIt(t *testing.T) {
	frps, frpc := testenv.FrpBinaries(t)
	r := newFrpsRigEdited(t, frpsRigBase2, 0, frps, nil, func(cfg []byte) []byte {
		// At the top, before the first table, where the launcher's other transport keys are.
		return append([]byte("transport.tcpMux = false\n"), cfg...)
	})
	h := r.host
	const uid = "uid-4711" // stands for an Instance's UID

	sock := filepath.Join(r.state, "e.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	engine := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		fmt.Fprintf(w, "engine-ok %s", q.URL.Path)
	})}
	go func() { _ = engine.Serve(ln) }()
	t.Cleanup(func() { _ = engine.Close() })

	relay := startHoldRelay(t, h.FrpsBindAddr())
	armed := make(chan error, 1)
	var once sync.Once
	r.stub.setBeforeReply(func(op string) {
		if op != "NewProxy" {
			return
		}
		once.Do(func() {
			// frps asks for the spare work connection at the login, on the control connection.
			// Hold only once frpc has dialled it: a held request would leave the CONNECT
			// waiting for frps's userConnTimeout instead, which is another failure.
			for end := time.Now().Add(10 * time.Second); relay.accepted() < 2; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(end) {
					armed <- fmt.Errorf("frpc made %d connections through the relay, want the control connection and the spare work connection", relay.accepted())
					return
				}
			}
			relay.hold()
			armed <- nil
		})
	})

	frpcLog := filepath.Join(r.state, "frpc.log")
	cfg := filepath.Join(r.state, "frpc.toml")
	// transport.poolCount = 0 as the product writes it (internal/agent/frpcconf.go); frpc reads it
	// as 1. frpc's own heartbeat timeout outlasts the hold, which also holds frps's pongs.
	frpcConf := fmt.Sprintf(`serverAddr = "127.0.0.1"
serverPort = %d
loginFailExit = false
log.to = %q
metadatas.token = "tok-test"
transport.protocol = "tcp"
transport.tcpMux = false
transport.poolCount = 0
transport.heartbeatInterval = 1
transport.heartbeatTimeout = 30

[[proxies]]
name = %q
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = [%q]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = %q
`, relay.port(), frpcLog, uid, uid, sock)
	if err := os.WriteFile(cfg, []byte(frpcConf), 0o600); err != nil {
		t.Fatal(err)
	}
	killFrpc := startFrpc(t, frpc, cfg)

	r.stub.waitOp(t, "NewProxy", 15*time.Second)
	select {
	case err := <-armed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the plugin's NewProxy call never armed the hold")
	}

	// The old wait passes: frps lists the proxy online, and frpc has not started it.
	for end := time.Now().Add(15 * time.Second); r.proxies()[uid] != "online"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("frps never listed %s online: %v\nfrps log:\n%s", uid, r.proxies(), r.logTail())
		}
	}
	if log := tailFile(frpcLog); strings.Contains(log, "start proxy success") {
		t.Fatalf("frpc started the proxy while frps's answer to it was held:\n%s", log)
	}

	// The old wait's CONNECT: 200, then the end of the connection before any reply. The deadline
	// is under frps's userConnTimeout (10 s, frp's default, which the launcher keeps). An frps
	// with no work connection to give closes the request then, which looks the same on the wire,
	// so an end before the deadline is frpc's close of the spare (proxy_wrapper.go:275-276).
	status, code, body, err := tcpmuxGet(h.FrpsTCPMuxAddr(), uid, "/health", time.Now().Add(5*time.Second))
	if !errors.Is(err, errClosedBeforeReply) || !strings.Contains(status, " 200 ") {
		t.Fatalf("CONNECT %s while frps's answer to frpc is held: %q, %d %q, %v; want 200, then the end of the connection before any reply (#368)\nfrps log:\n%s",
			uid, status, code, body, err, r.logTail())
	}

	// provenRoute does not take that for a route. frps's request for a new work connection is
	// held too now (control.go:615, :635), so its next attempt waits until the bound.
	if n, err := provenRoute(h.FrpsTCPMuxAddr(), uid, "/health", "engine-ok /health", 2*time.Second); err == nil {
		t.Fatalf("provenRoute found a route in %d attempts while frps's answer to frpc was held", n)
	} else {
		t.Logf("while the answer was held, provenRoute gave up after %d attempts: %v", n, err)
	}

	// Let the answer through: frpc starts the proxy, and a reply crosses.
	relay.release()
	if n, err := provenRoute(h.FrpsTCPMuxAddr(), uid, "/health", "engine-ok /health", 15*time.Second); err != nil {
		t.Fatalf("no route after the release: %v\nfrpc log:\n%s\nfrps log:\n%s", err, tailFile(frpcLog), r.logTail())
	} else {
		t.Logf("after the release the route was proven at attempt %d", n)
	}
	// frpc logs the start after it has set the phase (client/control.go:170-174), so the line can
	// trail the first reply.
	for end := time.Now().Add(10 * time.Second); !strings.Contains(tailFile(frpcLog), "start proxy success"); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("a reply crossed, but frpc's log never said it started the proxy:\n%s", tailFile(frpcLog))
		}
	}

	killFrpc()
	r.stop()
	if !gone(r.proc.Rec().PID) || len(procsMentioning(r.state)) != 0 {
		t.Errorf("processes left: pid %d gone=%v, mentioning the state dir: %v", r.proc.Rec().PID, gone(r.proc.Rec().PID), procsMentioning(r.state))
	}
}

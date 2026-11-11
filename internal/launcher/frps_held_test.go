package launcher

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// The reproduction of #368 (frps_route_test.go): a relay holds frps's answer to frpc, so that
// frps lists the proxy online while frpc cannot yet serve it.

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
	// releaseOn, when not 0, is the connection (1 is the first) whose end on frpc's side releases
	// the hold (releaseWhenFrpcEnds).
	releaseOn int
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
		idx := r.n
		r.conns = append(r.conns, c, up)
		r.mu.Unlock()
		frpcEnded := func() {
			r.mu.Lock()
			hit := r.releaseOn == idx
			r.mu.Unlock()
			if hit {
				r.release()
			}
		}
		go r.pipe(up, c, false, frpcEnded)
		go r.pipe(c, up, idx == 1, nil)
	}
}

// pipe copies src to dst until either ends, then closes both; srcEnded, when not nil, runs when
// src ends. With holdable, each chunk read while the relay is held waits for the release before it
// is written.
func (r *holdRelay) pipe(dst, src net.Conn, holdable bool, srcEnded func()) {
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
			if srcEnded != nil {
				srcEnded()
			}
			return
		}
	}
}

// releaseWhenFrpcEnds makes the end of connection n on frpc's side release the hold.
func (r *holdRelay) releaseWhenFrpcEnds(n int) {
	r.mu.Lock()
	r.releaseOn = n
	r.mu.Unlock()
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

// heldTunnel is the real frps and frpc, with transport.tcpMux off, and a holdRelay between them
// that holds what frps sends on frpc's control connection from the plugin's NewProxy call, which
// frps makes before it registers the proxy and answers frpc (server/control.go:745, :748, :768).
// It is armed only once frpc has dialled the spare work connection: frps asks for it at the login,
// on the control connection, and a held request would leave the first CONNECT waiting for frps's
// userConnTimeout instead, which is another failure.
//
// tcpMux is off so that the relay sees the control connection and the work connections apart:
// with it on, every connection from frpc is a yamux session whose streams carry both
// (server/service.go:746-770). That changes how frps's messages travel, not what frpc does with
// them: the code that closes an early work connection is the same either way (client/control.go:
// 124-176, client/proxy/proxy_wrapper.go:266-278).
type heldTunnel struct {
	r       *frpsRig
	relay   *holdRelay
	frpcLog string
	kill    func()
}

// startHeldTunnel starts it for a proxy named uid and returns once frps lists the proxy online
// while the hold keeps frpc from starting it.
func startHeldTunnel(t *testing.T, uid string) *heldTunnel {
	t.Helper()
	frps, frpc := testenv.FrpBinaries(t)
	r := newFrpsRigEdited(t, frpsRigBase2, 0, frps, nil, func(cfg []byte) []byte {
		// At the top, before the first table, where the launcher's other transport keys are.
		return append([]byte("transport.tcpMux = false\n"), cfg...)
	})
	h := r.host

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

	ht := &heldTunnel{r: r, relay: relay, frpcLog: filepath.Join(r.state, "frpc.log")}
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
`, relay.port(), ht.frpcLog, uid, uid, sock)
	if err := os.WriteFile(cfg, []byte(frpcConf), 0o600); err != nil {
		t.Fatal(err)
	}
	ht.kill = startFrpc(t, frpc, cfg)

	r.stub.waitOp(t, "NewProxy", 15*time.Second)
	select {
	case err := <-armed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the plugin's NewProxy call never armed the hold")
	}
	// frps lists the proxy online, and frpc has not started it.
	for end := time.Now().Add(15 * time.Second); r.proxies()[uid] != "online"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("frps never listed %s online: %v\nfrps log:\n%s", uid, r.proxies(), r.logTail())
		}
	}
	if log := tailFile(ht.frpcLog); strings.Contains(log, "start proxy success") {
		t.Fatalf("frpc started the proxy while frps's answer to it was held:\n%s", log)
	}
	return ht
}

// waitStarted waits until frpc's log says it started the proxy. frpc logs that after it has set
// the phase (client/control.go:170-174), so the line can trail the first reply.
func (ht *heldTunnel) waitStarted(t *testing.T) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !strings.Contains(tailFile(ht.frpcLog), "start proxy success"); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("a reply crossed, but frpc's log never said it started the proxy:\n%s", tailFile(ht.frpcLog))
		}
	}
}

// stop kills frpc, stops frps, and checks that nothing is left.
func (ht *heldTunnel) stop(t *testing.T) {
	t.Helper()
	ht.kill()
	ht.r.stop()
	if pid := ht.r.proc.Rec().PID; !gone(pid) || len(procsMentioning(ht.r.state)) != 0 {
		t.Errorf("processes left: pid %d gone=%v, mentioning the state dir: %v", pid, gone(pid), procsMentioning(ht.r.state))
	}
}

// TestFrpsRealListsAProxyOnlineBeforeFrpcCanServeIt is #368 on purpose: frps lists the proxy
// online and answers its CONNECT with 200 while frpc has not yet handled frps's NewProxyResp, and
// no reply crosses (heldTunnel). So the old wait (online, then one CONNECT) fails every time,
// provenRoute reports no route while the answer is held, and it passes once the answer is let
// through.
func TestFrpsRealListsAProxyOnlineBeforeFrpcCanServeIt(t *testing.T) {
	const uid = "uid-4711" // stands for an Instance's UID
	ht := startHeldTunnel(t, uid)
	mux := ht.r.host.FrpsTCPMuxAddr()

	// The old wait's CONNECT: 200, then the end of the connection before any reply. The deadline
	// is under frps's userConnTimeout (10 s, frp's default, which the launcher keeps). An frps
	// with no work connection to give closes the request then, which looks the same on the wire,
	// so an end before the deadline is frpc's close of the spare (proxy_wrapper.go:275-276).
	status, code, body, err := tcpmuxGet(mux, uid, "/health", time.Now().Add(5*time.Second))
	if !errors.Is(err, errClosedBeforeReply) || !strings.Contains(status, " 200 ") {
		t.Fatalf("CONNECT %s while frps's answer to frpc is held: %q, %d %q, %v; want 200, then the end of the connection before any reply (#368)\nfrps log:\n%s",
			uid, status, code, body, err, ht.r.logTail())
	}

	// provenRoute does not take that for a route. frps's request for a new work connection is
	// held too now (control.go:615, :635), so its next attempt waits until the bound.
	if n, err := provenRoute(mux, uid, "/health", "engine-ok /health", 2*time.Second); err == nil {
		t.Fatalf("provenRoute found a route in %d attempts while frps's answer to frpc was held", n)
	} else {
		t.Logf("while the answer was held, provenRoute gave up after %d attempts: %v", n, err)
	}

	// Let the answer through: frpc starts the proxy, and a reply crosses.
	ht.relay.release()
	if n, err := provenRoute(mux, uid, "/health", "engine-ok /health", 15*time.Second); err != nil {
		t.Fatalf("no route after the release: %v\nfrpc log:\n%s\nfrps log:\n%s", err, tailFile(ht.frpcLog), ht.r.logTail())
	} else {
		t.Logf("after the release the route was proven at attempt %d", n)
	}
	ht.waitStarted(t)
	ht.stop(t)
}

// TestFrpsRealProvenRouteRetriesAnEarlyCloseThenPasses: provenRoute's retry, end to end. The hold
// is released by frpc's own close of the spare work connection (the relay's second connection),
// which frpc makes while it has not started the proxy. So provenRoute's first attempt ends in an
// early close every time (the spare was its work connection), and its second crosses: after the
// release frpc handles the NewProxyResp before the held request for a new work connection
// (pkg/msg/handler.go:170-188; client/control.go:233-234), so every work connection it opens
// from then on is for a started proxy. A provenRoute that did not retry fails here.
func TestFrpsRealProvenRouteRetriesAnEarlyCloseThenPasses(t *testing.T) {
	const uid = "uid-4712" // stands for an Instance's UID
	ht := startHeldTunnel(t, uid)
	ht.relay.releaseWhenFrpcEnds(2)

	n, err := provenRoute(ht.r.host.FrpsTCPMuxAddr(), uid, "/health", "engine-ok /health", 15*time.Second)
	if err != nil || n != 2 {
		t.Fatalf("provenRoute = %d attempts, %v; want a route at the second attempt, after the early close of the first\nfrpc log:\n%s\nfrps log:\n%s",
			n, err, tailFile(ht.frpcLog), ht.r.logTail())
	}
	ht.waitStarted(t)
	ht.stop(t)
}

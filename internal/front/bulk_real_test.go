package front

// The bulk path's trap 4 on the real frp v0.71.1 (the plan of #448, part 6; HBEP-6:658): the front
// with the bulk path over the launcher's two frps, a token frpc and a bulk frpc for one Instance,
// each serving an engine double of its own, through the production dial-home listener. The bulk frpc
// is killed through its pidfd after it carried a request, while frps-bulk's CloseProxy to the front's
// bulk plugin is HELD: the front has heard nothing of the loss, so the next non-streamed request can
// only reach the token frpc by falling back. M1 measured CloseProxy about 4 ms after a kill, so
// without the hold this would test the unregister, not the fallback.
//
// The tcpmux-double tests in bulk_test.go are the fast unit layer: routing, counting and the traps'
// rules, with a 404 standing in for a gone proxy. This one is the layer they can't be: that a dead
// bulk frpc's pooled connection, on real frp at the production upstreamIdleTimeout, is gone before
// the next request reuses it, so the request falls back rather than being written into it.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// closeProxyHold serves the front's bulk plugin path for frps-bulk and passes each call to the
// front's bulk plugin, except that, once armed, it holds every CloseProxy until released.
type closeProxyHold struct {
	plugin  http.Handler
	mu      sync.Mutex
	armed   bool
	held    chan string // the proxy name of each CloseProxy held
	release chan struct{}
	passed  chan struct{} // a value once each held call, released, has reached the front's plugin
}

func (h *closeProxyHold) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var call struct {
		Op      string `json:"op"`
		Content struct {
			ProxyName string `json:"proxy_name"`
		} `json:"content"`
	}
	_ = json.Unmarshal(b, &call)
	h.mu.Lock()
	hold := h.armed && call.Op == opCloseProxy
	release := h.release
	h.mu.Unlock()
	if hold {
		h.held <- call.Content.ProxyName
		<-release
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	h.plugin.ServeHTTP(w, r)
	if hold {
		h.passed <- struct{}{}
	}
}

func (h *closeProxyHold) arm() {
	h.mu.Lock()
	h.armed = true
	h.mu.Unlock()
}

// unixEngine serves an engine double on a Unix socket, which an frpc serves as the Instance.
func unixEngine(t *testing.T, path, key string) *enginetest.Engine {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	return enginetest.Serve(t, ln, enginetest.Options{Key: key})
}

// realBulkFront is the front with the bulk path over the rig's two real frps, one Instance, and its
// token and bulk frpc through the production dial-home listener.
type realBulkFront struct {
	*bulkFront
	rig      *bulkRig
	uid      types.UID
	dir      string
	dialHome string
}

// newRealBulkFront starts it. pluginMux, when not nil, is served on a kernel port and given to the
// rig: the first frps posts to its FrontFrpsPluginPath when token is set, and the second to its
// FrontFrpsBulkPluginPath; the caller mounts the front's plugins on it once built (mount).
func newRealBulkFront(t *testing.T, pluginMux *http.ServeMux, token bool, mount func(f *bulkFront)) *realBulkFront {
	t.Helper()
	var frontToken, frontBulk string
	if pluginMux != nil {
		pln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		psrv := &http.Server{Handler: pluginMux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = psrv.Serve(pln) }()
		t.Cleanup(func() { _ = psrv.Close() })
		frontBulk = pln.Addr().String()
		if token {
			frontToken = frontBulk
		}
	}
	rig := newBulkRigFronts(t, frontToken, frontBulk)
	h := newHarnessReal(t, nil)
	key := newKey(t)
	dir := shortDir(t)
	f := &bulkFront{harness: h, engToken: unixEngine(t, filepath.Join(dir, "token.sock"), key), engBulk: unixEngine(t, filepath.Join(dir, "bulk.sock"), key)}
	f.log = slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.tun = NewTunnel(rig.host.FrpsTCPMuxAddr(), h.cfg.TunnelSetupTimeout.D())
	keys, err := ReadClientKeys(h.cfg.ClientKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	h.router = NewRouter(h.cfg, clock.Real{}, f.tun, h.rec, f.log, h.metrics)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clock.Real{}, Router: h.router, Transports: f.tun, Keys: keys, Sink: h.sink, Metrics: h.metrics, Log: f.log})
	source := func() []v1alpha1.Instance { return append([]v1alpha1.Instance(nil), h.snap.Instances...) }
	f.bulk = newBulkPath(h.cfg, rig.host.FrpsBulkTCPMuxAddr(), clock.Real{}, source, f.log)
	h.proxy.bulk = f.bulk
	h.metrics.enableBulk(f.bulk.tun.registeredCount)
	f.prober = NewProber(h.cfg, f.tun, clock.Real{}, source, nil, f.log)
	if mount != nil {
		mount(f)
	}
	cln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(cln) }()
	t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + cln.Addr().String()
	if h.cfg.UpstreamIdleTimeout.D() != DefaultUpstreamIdleTimeout {
		t.Fatalf("upstreamIdleTimeout is %s, not the production %s: the test is about the production pool", h.cfg.UpstreamIdleTimeout.D(), DefaultUpstreamIdleTimeout)
	}
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md-0", "m-0", 1, "helios-a")
	uid := h.instance("md-0", "md-0-0", "helios-a", nil, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	h.inst(uid).Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonProxyOnline, LastTransitionTime: metav1.Now()}}
	h.apply()
	f.uids, f.keys = append(f.uids, uid), append(f.keys, "ns/md-0-0")
	return &realBulkFront{bulkFront: f, rig: rig, uid: uid, dir: dir,
		dialHome: serveSplit(t, ModeHostDialed, rig.host, rig.host.FrpsBindAddr(), rig.host.FrpsBulkBindAddr(), true)}
}

// startFrpc starts the token frpc, or the bulk frpc with bulk, for the Instance.
func (r *realBulkFront) startFrpc(bulk bool) (*os.Process, <-chan struct{}) {
	r.t.Helper()
	if bulk {
		_, p, done := r.rig.startFrpcProc(string(r.uid), filepath.Join(r.dir, "bulk.sock"), r.dialHome, bulkName, r.rig.bulkCert, true)
		return p, done
	}
	_, p, done := r.rig.startFrpcProc(string(r.uid), filepath.Join(r.dir, "token.sock"), r.dialHome, agentapi.DialHomeServerName, r.rig.first, false)
	return p, done
}

// register runs probe rounds until both transports count as registered.
func (r *realBulkFront) register(what string) {
	r.t.Helper()
	waitUntilFor(r.t, 60*time.Second, what, func() bool {
		r.prober.Round(context.Background())
		r.bulk.prober.Round(context.Background())
		return r.tun.Registered(r.uid) && r.bulk.tun.Registered(r.uid)
	})
}

// TestRealBulkFrpcKilledFallsBackOnce (trap 4, HBEP-6:658 as amended on #448): after the bulk
// transport carried a request, the bulk frpc is killed through its pidfd with frps-bulk's CloseProxy
// held, so the front still has the bulk transport registered. frps deletes the proxy before it calls
// the plugin, so a new bulk dial fails and the request falls back: served by the SAME Instance's
// token frpc, one fallback, no pause, the bulk transport out of use. But a request written into the
// pooled bulk connection in the milliseconds before frp closes it gets 502 upstream_closed instead,
// never resent: also no pause, the bulk transport out of use, and the next request on the token
// tunnel. Each round checks its branch's invariants, then lets the CloseProxy through, restarts the
// bulk frpc and re-registers it; the test repeats until a round has shown the fallback on the real
// frp, up to fallbackRounds, and fails if none does.
func TestRealBulkFrpcKilledFallsBackOnce(t *testing.T) {
	const fallbackRounds = 5
	hold := &closeProxyHold{held: make(chan string, 8), release: make(chan struct{}), passed: make(chan struct{}, 8)}
	pmux := http.NewServeMux()
	pmux.Handle(agentapi.FrontFrpsBulkPluginPath, hold)
	r := newRealBulkFront(t, pmux, false, func(f *bulkFront) { hold.plugin = f.bulk.plugin(f.log) })
	f, uid := r.bulkFront, r.uid
	t.Cleanup(func() {
		hold.mu.Lock()
		defer hold.mu.Unlock()
		select {
		case <-hold.release:
		default:
			close(hold.release)
		}
	})
	r.startFrpc(false)
	bulkProc, bulkDone := r.startFrpc(true)
	r.register("both transports to register through the real frps")
	sawFallback := false
	for round := 1; round <= fallbackRounds && !sawFallback; round++ {
		b0, fb0, _ := f.bulkCounts()
		if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
			t.Fatalf("round %d, before the loss: %d through %q; the round needs a bulk transport that carried a request", round, code, by)
		}
		hold.mu.Lock()
		hold.armed, hold.release = true, make(chan struct{})
		release := hold.release
		hold.mu.Unlock()
		if err := bulkProc.Kill(); err != nil {
			t.Fatalf("round %d: SIGKILL to the bulk frpc through its pidfd: %v", round, err)
		}
		<-bulkDone
		select {
		case name := <-hold.held:
			if name != string(uid) {
				t.Fatalf("round %d: frps-bulk's CloseProxy names %q, want %s", round, name, uid)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: frps-bulk never posted CloseProxy for the killed bulk frpc", round)
		}
		if !f.bulk.tun.Registered(uid) {
			t.Fatalf("round %d: the bulk transport is unregistered while its CloseProxy is held: this would test the unregister, not the fallback", round)
		}
		tokenBefore := len(f.engToken.Requests())
		resp := f.post("/v1/chat/completions", chatPlain)
		body := readAll(t, resp)
		_, fb, _ := f.bulkCounts()
		switch {
		case resp.StatusCode == http.StatusOK && len(f.engToken.Requests()) == tokenBefore+1 && fb == fb0+1:
			sawFallback = true // the fallback: the same Instance's token frpc served it
		case resp.StatusCode == http.StatusBadGateway && strings.Contains(body, "upstream_closed") && fb == fb0:
			// The pooled connection, written before frp closed it: never resent.
			if n := len(f.engToken.Requests()); n != tokenBefore {
				t.Errorf("round %d: the 502 request was sent again through the token transport (%d requests there)", round, n-tokenBefore)
			}
			if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "token" {
				t.Errorf("round %d: the request after the 502: %d through %q, want the token tunnel", round, code, by)
			}
		default:
			t.Fatalf("round %d, after the kill: %d %.200s, token requests +%d, fallbacks +%v; want a fallback through the token frpc or a 502 upstream_closed", round, resp.StatusCode, body, len(f.engToken.Requests())-tokenBefore, fb-fb0)
		}
		if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
			t.Errorf("round %d: the dead bulk frpc counted toward the pause: failures %d, paused %v", round, n, f.router.Paused(f.keys[0]))
		}
		if f.bulk.tun.Registered(uid) {
			t.Errorf("round %d: the bulk transport is still registered after the loss", round)
		}
		if b, _, _ := f.bulkCounts(); b < b0+1 {
			t.Errorf("round %d: bulk %v -> %v, want the first request counted", round, b0, b)
		}
		t.Logf("round %d: %s", round, map[bool]string{true: "the fallback, through the token frpc", false: "502 upstream_closed on the pooled connection"}[sawFallback])

		hold.mu.Lock()
		hold.armed = false
		hold.mu.Unlock()
		close(release)
		// The held CloseProxy reaches the front's plugin before the bulk frpc returns, so it can't
		// take the new registration away.
		select {
		case <-hold.passed:
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: the released CloseProxy never reached the front's plugin", round)
		}
		bulkProc, bulkDone = r.startFrpc(true)
		r.register("the restarted bulk frpc to register again")
	}
	if !sawFallback {
		t.Fatalf("no round of %d showed the fallback on the real frp: every request after the kill met the pooled connection", fallbackRounds)
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
		t.Errorf("after the bulk frpc came back: %d through %q, want the bulk transport", code, by)
	}
}

// TestRealBulkEpochsAreSeparate (HBEP-6:657, on the real frp): both frps post to the front's plugins,
// the first to the token path and frps-bulk to the bulk path, as the launcher configures them. The
// bulk frpc's kill (frps-bulk's CloseProxy) and its return (NewProxy) move only the bulk epoch, and
// leave the token transport registered in its epoch; the token frpc's kill and return move only the
// token epoch, and leave the bulk transport registered in its epoch.
func TestRealBulkEpochsAreSeparate(t *testing.T) {
	pmux := http.NewServeMux()
	r := newRealBulkFront(t, pmux, true, func(f *bulkFront) {
		pmux.Handle(agentapi.FrontFrpsPluginPath, &frpsPlugin{tun: f.tun, prober: f.prober, log: f.log})
		pmux.Handle(agentapi.FrontFrpsBulkPluginPath, f.bulk.plugin(f.log))
	})
	f, uid := r.bulkFront, r.uid
	tokenProc, tokenDone := r.startFrpc(false)
	bulkProc, bulkDone := r.startFrpc(true)
	r.register("both transports to register through the real frps")

	for _, c := range []struct {
		name               string
		proc               *os.Process
		done               <-chan struct{}
		moved, kept        *Tunnel
		movedName, keptNam string
		bulk               bool
	}{
		{"the bulk frpc", bulkProc, bulkDone, f.bulk.tun, f.tun, "bulk", "token", true},
		{"the token frpc", tokenProc, tokenDone, f.tun, f.bulk.tun, "token", "bulk", false},
	} {
		before, keptBefore := c.moved.Epoch(uid), c.kept.Epoch(uid)
		if err := c.proc.Kill(); err != nil {
			t.Fatalf("SIGKILL to %s through its pidfd: %v", c.name, err)
		}
		<-c.done
		waitUntilFor(t, 30*time.Second, c.name+"'s CloseProxy to move the "+c.movedName+" epoch", func() bool { return c.moved.Epoch(uid) != before })
		if c.kept.Epoch(uid) != keptBefore || !c.kept.Registered(uid) {
			t.Errorf("%s's CloseProxy: the %s epoch %d -> %d, registered %v; want it untouched", c.name, c.keptNam, keptBefore, c.kept.Epoch(uid), c.kept.Registered(uid))
		}
		closed := c.moved.Epoch(uid)
		r.startFrpc(c.bulk)
		waitUntilFor(t, 30*time.Second, c.name+"'s NewProxy to move the "+c.movedName+" epoch", func() bool { return c.moved.Epoch(uid) != closed })
		if c.kept.Epoch(uid) != keptBefore || !c.kept.Registered(uid) {
			t.Errorf("%s's NewProxy: the %s epoch %d -> %d, registered %v; want it untouched", c.name, c.keptNam, keptBefore, c.kept.Epoch(uid), c.kept.Registered(uid))
		}
		r.register(c.name + " to register again")
	}
}

// curConns is the UID's current connections on the frps whose web server is web: the user
// connections frps has given a work connection (frp v0.71.1, server/proxy/proxy.go,
// handleUserTCPConnection), -1 when the proxy isn't listed or the list can't be read.
func curConns(web, pw, uid string) int64 {
	req, _ := http.NewRequest(http.MethodGet, "http://"+web+"/api/proxy/tcpmux", nil)
	req.SetBasicAuth(agentapi.FrpsWebUser, pw)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var v struct {
		Proxies []struct {
			Name     string `json:"name"`
			CurConns int64  `json:"curConns"`
		} `json:"proxies"`
	}
	if json.NewDecoder(resp.Body).Decode(&v) != nil {
		return -1
	}
	for _, p := range v.Proxies {
		if p.Name == uid {
			return p.CurConns
		}
	}
	return -1
}

// drainSpare makes sure the frps at mux holds no spare work connection for uid once its frpc is
// stopped. frp keeps one (it reads poolCount = 0 as 1, HBEP-6's erratum of 11 Nov 2026), and a user
// connection given it would wait on the stopped frpc until the front's idleTimeout (HBEP-6's frozen
// work connection, a stated limit) instead of being closed at userConnTimeout. A dummy connection is
// opened straight to frps and either takes the spare, which frps's curConns shows, or, with none, is
// closed by frps at userConnTimeout. Either way frps then has none, and the stopped frpc can't make
// another. The dummy stays open until the test ends.
func drainSpare(t *testing.T, mux, web, pw, uid string) {
	t.Helper()
	before := curConns(web, pw, uid)
	if before < 0 {
		t.Fatalf("frps at %s does not list %s", web, uid)
	}
	c, err := net.DialTimeout("tcp", mux, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid)
	closed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, c) // the 200, then EOF only if frps closes it
		close(closed)
	}()
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		select {
		case <-closed:
			t.Logf("%s had no spare work connection: frps closed the dummy at userConnTimeout", mux)
			return
		default:
		}
		if curConns(web, pw, uid) > before {
			t.Logf("the dummy took %s's spare work connection", mux)
			return
		}
		if time.Now().After(end) {
			t.Fatalf("the dummy connection to %s neither took a work connection nor was closed", mux)
		}
	}
}

// TestRealBulkCutBeforeHeadersRaisesNoPause (trap 3 on the real frp, HBEP-6:661 and :517): the bulk
// frpc, the test's own child, is stopped through its handle (SIGSTOP), so frps-bulk answers a new
// CONNECT and then waits userConnTimeout (frp's default, 10 s; the launcher sets none) for a work
// connection that never comes, and closes the user connection after the front wrote the request.
// The front answers 502 upstream_closed, never sends it again, takes the bulk transport out of use
// and counts nothing toward the pause. The control is the same cut on the token transport, with the
// token frpc stopped: it counts one failure toward the pause, as HBEP-5 says.
//
// No request runs before the stops, so neither of the front's pools holds a connection whose work
// connection was made before the stop: each request opens a new one. And frp's spare work connection is
// taken by a dummy connection first (drainSpare), so frps's own wait is what cuts it, every time: with
// the spare left there, a request given it waits on the stopped frpc until the front's idleTimeout.
func TestRealBulkCutBeforeHeadersRaisesNoPause(t *testing.T) {
	r := newRealBulkFront(t, nil, false, nil)
	f, uid := r.bulkFront, r.uid
	tokenProc, _ := r.startFrpc(false)
	bulkProc, _ := r.startFrpc(true)
	r.register("both transports to register through the real frps")
	stop := func(name string, p *os.Process) {
		t.Helper()
		if err := p.Signal(syscall.SIGSTOP); err != nil {
			t.Fatalf("SIGSTOP to %s through its handle: %v", name, err)
		}
		t.Cleanup(func() { _ = p.Signal(syscall.SIGCONT) })
	}
	cut := func(want string) {
		t.Helper()
		start := time.Now()
		resp := f.post("/v1/chat/completions", chatPlain)
		body := readAll(t, resp)
		if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "upstream_closed") {
			t.Fatalf("%s: %d %s, want 502 upstream_closed", want, resp.StatusCode, body)
		}
		// frps's own wait cut it, not the front's: it took about userConnTimeout.
		if took := time.Since(start); took < 9*time.Second {
			t.Fatalf("%s: answered after %s, before frps's 10 s userConnTimeout: something else cut it", want, took)
		}
	}

	stop("the bulk frpc", bulkProc)
	drainSpare(t, r.rig.host.FrpsBulkTCPMuxAddr(), r.rig.host.FrpsBulkWebAddr(), r.rig.pwBulk, string(uid))
	cut("the bulk request with the bulk frpc stopped")
	if f.bulk.tun.Registered(uid) {
		t.Error("the bulk transport is still registered after its request was cut before the headers")
	}
	if _, ok := f.bulk.prober.reprobeAt(); !ok {
		t.Error("the bulk transport is not on the re-probe schedule")
	}
	if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
		t.Errorf("the bulk cut counted toward the pause: failures %d, paused %v", n, f.router.Paused(f.keys[0]))
	}
	if b, fb, _ := f.bulkCounts(); b != 1 || fb != 0 {
		t.Errorf("metrics: bulk %v, fallback %v; want 1 (written) and 0", b, fb)
	}
	if n := len(f.engToken.Requests()); n != 0 {
		t.Errorf("the cut bulk request was sent again through the token transport: %d requests there", n)
	}

	// The control: the same cut on the token transport counts toward the pause.
	stop("the token frpc", tokenProc)
	drainSpare(t, r.rig.host.FrpsTCPMuxAddr(), r.rig.host.FrpsWebAddr(), r.rig.pwFirst, string(uid))
	cut("the token request with the token frpc stopped")
	if n := f.router.failures(f.keys[0]); n != 1 {
		t.Errorf("the token cut counted %d failures toward the pause, want 1: the control can't tell the bulk cut apart", n)
	}
}

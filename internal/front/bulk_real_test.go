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
	h.mu.Unlock()
	if hold {
		h.held <- call.Content.ProxyName
		<-h.release
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	h.plugin.ServeHTTP(w, r)
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

// TestRealBulkFrpcKilledFallsBackOnce (trap 4, HBEP-6:658): after the bulk transport carried a
// request, the bulk frpc is killed through its pidfd with frps-bulk's CloseProxy held, so the front
// still has the bulk transport registered. The next non-streamed request is served by the SAME
// Instance's token frpc, counts one fallback, starts no failure run, and takes the bulk transport out
// of use. Once the CloseProxy is released and the bulk frpc runs again, a probe registers it again
// and the next request takes it.
func TestRealBulkFrpcKilledFallsBackOnce(t *testing.T) {
	hold := &closeProxyHold{held: make(chan string, 8), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-hold.release:
		default:
			close(hold.release)
		}
	})
	pmux := http.NewServeMux()
	pmux.Handle(agentapi.FrontFrpsBulkPluginPath, hold)
	r := newRealBulkFront(t, pmux, false, func(f *bulkFront) { hold.plugin = f.bulk.plugin(f.log) })
	f, uid := r.bulkFront, r.uid
	r.startFrpc(false)
	bulkProc, bulkDone := r.startFrpc(true)
	r.register("both transports to register through the real frps")
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
		t.Fatalf("before the loss: %d through %q; the test needs a bulk transport that carried a request", code, by)
	}

	hold.arm()
	if err := bulkProc.Kill(); err != nil {
		t.Fatalf("SIGKILL to the bulk frpc through its pidfd: %v", err)
	}
	<-bulkDone
	select {
	case name := <-hold.held:
		if name != string(uid) {
			t.Fatalf("frps-bulk's CloseProxy names %q, want %s", name, uid)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("frps-bulk never posted CloseProxy for the killed bulk frpc")
	}
	if !f.bulk.tun.Registered(uid) {
		t.Fatal("the bulk transport is unregistered while its CloseProxy is held: this would test the unregister, not the fallback")
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "token" {
		t.Fatalf("after the kill: %d through %q, want 200 from the same Instance's token frpc", code, by)
	}
	if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
		t.Errorf("the dead bulk frpc counted toward the pause: failures %d, paused %v", n, f.router.Paused(f.keys[0]))
	}
	if f.bulk.tun.Registered(uid) {
		t.Error("the bulk transport is still registered after its dial failed")
	}
	if b, fb, _ := f.bulkCounts(); b != 1 || fb != 1 {
		t.Errorf("metrics: bulk %v, fallback %v; want 1 and 1", b, fb)
	}

	close(hold.release)
	r.startFrpc(true)
	r.register("the restarted bulk frpc to register again")
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

// TestRealBulkCutBeforeHeadersRaisesNoPause (trap 3 on the real frp, HBEP-6:661 and :517): the bulk
// frpc, the test's own child, is stopped through its handle (SIGSTOP), so frps-bulk answers a new
// CONNECT and then waits userConnTimeout (frp's default, 10 s; the launcher sets none) for a work
// connection that never comes, and closes the user connection after the front wrote the request.
// The front answers 502 upstream_closed, never sends it again, takes the bulk transport out of use
// and counts nothing toward the pause. The control is the same cut on the token transport, with the
// token frpc stopped: it counts one failure toward the pause, as HBEP-5 says.
//
// No request runs before the stops, so neither pool holds a connection whose work connection was made
// before the stop: each request opens a new one, and frps's wait is what cuts it.
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
	cut("the token request with the token frpc stopped")
	if n := f.router.failures(f.keys[0]); n != 1 {
		t.Errorf("the token cut counted %d failures toward the pause, want 1: the control can't tell the bulk cut apart", n)
	}
}

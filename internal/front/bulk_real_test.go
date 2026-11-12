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
	"path/filepath"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	pln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pmux := http.NewServeMux()
	pmux.Handle(agentapi.FrontFrpsBulkPluginPath, hold)
	psrv := &http.Server{Handler: pmux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = psrv.Serve(pln) }()
	t.Cleanup(func() { _ = psrv.Close() })
	rig := newBulkRigWith(t, pln.Addr().String())

	// The front with the bulk path over the rig's two tcpmux ports (newBulkFront's shape).
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
	hold.plugin = f.bulk.plugin(f.log)
	cln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(cln) }()
	t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + cln.Addr().String()
	f.prober = NewProber(h.cfg, f.tun, clock.Real{}, source, nil, f.log)
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

	dialHome := serveSplit(t, ModeHostDialed, rig.host, rig.host.FrpsBindAddr(), rig.host.FrpsBulkBindAddr(), true)
	rig.startFrpc(string(uid), filepath.Join(dir, "token.sock"), dialHome, agentapi.DialHomeServerName, rig.first, false)
	_, bulkProc, bulkDone := rig.startFrpcProc(string(uid), filepath.Join(dir, "bulk.sock"), dialHome, bulkName, rig.bulkCert, true)
	register := func(what string) {
		t.Helper()
		waitUntilFor(t, 60*time.Second, what, func() bool {
			f.prober.Round(context.Background())
			f.bulk.prober.Round(context.Background())
			return f.tun.Registered(uid) && f.bulk.tun.Registered(uid)
		})
	}
	register("both transports to register through the real frps")
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
	rig.startFrpc(string(uid), filepath.Join(dir, "bulk.sock"), dialHome, bulkName, rig.bulkCert, true)
	register("the restarted bulk frpc to register again")
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
		t.Errorf("after the bulk frpc came back: %d through %q, want the bulk transport", code, by)
	}
}

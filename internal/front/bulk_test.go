package front

// Tests of the front's bulk transport (HBEP-6, The bulk path; #448, part 6). The front runs on the
// wall clock over two tcpmux doubles, one for each frps, each handing its tunnels' requests to an
// engine double of its own. In the product both frpc serve one engine socket; here each tunnel has
// its own engine, so every test sees which tunnel carried a request. Probes are driven by hand
// (Prober.Round, Reprobe), so a test decides when a transport registers.
//
// What these tests cannot show: that the bulk path protects token streams from a large reply. That
// needs a reply larger than frp's 6 MiB stream window on a shaped link, which is part 7's
// end-to-end test (HBEP-6, Test plan). These tests are about routing, failures and counts.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/convkey"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// bulkFront is a front with the bulk path over two tcpmux doubles.
type bulkFront struct {
	*harness
	tun         *Tunnel // the token transport
	prober      *Prober
	bulk        *bulkPath
	muxToken    *muxDouble
	muxBulk     *muxDouble
	engToken    *enginetest.Engine
	engBulk     *enginetest.Engine
	uids        []types.UID
	keys        []string // the router's target keys, ns/md-0-<i>
	log         *slog.Logger
	withoutBulk bool
}

// newBulkFront serves one deployment, md-0 of model m-0, with n ready Instances md-0-<i>, with
// TunnelConnected True, on both tunnels, and registers both transports with a first probe round.
// noBulk builds the same front without the bulk path, as the control of the tests that need one.
// edit, when set, changes the configuration first.
func newBulkFront(t *testing.T, n int, noBulk bool, edit ...func(*Config)) *bulkFront {
	t.Helper()
	var e func(*Config)
	if len(edit) > 0 {
		e = edit[0]
	}
	h := newHarnessReal(t, e)
	key := newKey(t)
	f := &bulkFront{harness: h, withoutBulk: noBulk}
	f.log = slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	serve := func() *enginetest.Engine {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return enginetest.Serve(t, ln, enginetest.Options{Key: key})
	}
	f.engToken, f.engBulk = serve(), serve()
	f.muxToken, f.muxBulk = startMuxDouble(t), startMuxDouble(t)
	f.muxToken.forwardTo(f.engToken.Addr())
	f.muxBulk.forwardTo(f.engBulk.Addr())
	f.tun = NewTunnel(f.muxToken.ln.Addr().String(), h.cfg.TunnelSetupTimeout.D())
	keys, err := ReadClientKeys(h.cfg.ClientKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	h.router = NewRouter(h.cfg, clock.Real{}, f.tun, h.rec, f.log, h.metrics)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clock.Real{}, Router: h.router, Transports: f.tun, Keys: keys, Sink: h.sink, Metrics: h.metrics, Log: f.log})
	source := func() []v1alpha1.Instance { return append([]v1alpha1.Instance(nil), h.snap.Instances...) }
	if !noBulk {
		f.bulk = newBulkPath(h.cfg, f.muxBulk.ln.Addr().String(), clock.Real{}, source, f.log)
		h.proxy.bulk = f.bulk
		h.metrics.enableBulk(f.bulk.tun.registeredCount)
	}
	cln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(cln) }()
	t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + cln.Addr().String()
	f.prober = NewProber(h.cfg, f.tun, clock.Real{}, source, nil, f.log)

	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md-0", "m-0", int32(n), "helios-a")
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("md-0-%d", i)
		uid := h.instance("md-0", name, "helios-a", nil, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
		h.inst(uid).Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue,
			Reason: v1alpha1.ReasonProxyOnline, LastTransitionTime: metav1.Now()}}
		f.muxToken.set(uid, http.StatusOK)
		f.muxBulk.set(uid, http.StatusOK)
		f.uids, f.keys = append(f.uids, uid), append(f.keys, "ns/"+name)
	}
	h.apply()
	f.prober.Round(context.Background())
	if !noBulk {
		f.bulk.prober.Round(context.Background())
	}
	waitRouterState(t, "every Instance to be routable", func() bool {
		for _, u := range f.uids {
			if !f.tun.Registered(u) || (!noBulk && !f.bulk.tun.Registered(u)) {
				return false
			}
		}
		return true
	})
	return f
}

// send posts body to path, with the extra headers hdr, and returns the status and which engine got
// the request: "token", "bulk", or "" when neither did.
func (f *bulkFront) send(path, body string, hdr ...string) (int, string) {
	f.t.Helper()
	nt, nb := len(f.engToken.Requests()), len(f.engBulk.Requests())
	resp := f.post(path, body, hdr...)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch {
	case len(f.engBulk.Requests()) > nb && len(f.engToken.Requests()) == nt:
		return resp.StatusCode, "bulk"
	case len(f.engToken.Requests()) > nt && len(f.engBulk.Requests()) == nb:
		return resp.StatusCode, "token"
	case len(f.engToken.Requests()) == nt && len(f.engBulk.Requests()) == nb:
		return resp.StatusCode, ""
	}
	f.t.Fatalf("one request reached both engines")
	return 0, ""
}

const (
	chatPlain  = `{"model":"m-0","messages":[{"role":"user","content":"hello"}]}`
	chatStream = `{"model":"m-0","stream":true,"messages":[{"role":"user","content":"hello"}]}`
)

// bulkCounts scrapes the bulk path's three series from /metrics: bulk, fallback, registered. A
// series that is absent reads -1.
func (f *bulkFront) bulkCounts() (bulk, fallback, registered float64) {
	f.t.Helper()
	rec := httptest.NewRecorder()
	f.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	bulk, fallback, registered = -1, -1, -1
	for _, l := range strings.Split(rec.Body.String(), "\n") {
		var v float64
		switch {
		case strings.HasPrefix(l, `hpcburst_front_bulk_requests_total{path="bulk"} `):
			fmt.Sscan(strings.Fields(l)[1], &v)
			bulk = v
		case strings.HasPrefix(l, `hpcburst_front_bulk_requests_total{path="fallback"} `):
			fmt.Sscan(strings.Fields(l)[1], &v)
			fallback = v
		case strings.HasPrefix(l, "hpcburst_front_bulk_registered "):
			fmt.Sscan(strings.Fields(l)[1], &v)
			registered = v
		}
	}
	return bulk, fallback, registered
}

// ---- the request-time rule (HBEP-6, The bulk path) ----

// TestNonStreamedRequestTakesBulkWhenRegistered: a routed POST whose top-level stream is not true
// takes the bulk transport: stream absent, false, null, and the string "true" (which is not true).
// stream true takes the token transport.
func TestNonStreamedRequestTakesBulkWhenRegistered(t *testing.T) {
	f := newBulkFront(t, 1, false)
	for _, c := range []struct{ body, want string }{
		{chatPlain, "bulk"},
		{`{"model":"m-0","stream":false,"messages":[{"role":"user","content":"hello"}]}`, "bulk"},
		{`{"model":"m-0","stream":null,"messages":[{"role":"user","content":"hello"}]}`, "bulk"},
		{`{"model":"m-0","stream":"true","messages":[{"role":"user","content":"hello"}]}`, "bulk"},
		{`{"model":"m-0","messages":[{"role":"user","content":"hello"}],"options":{"stream":true}}`, "bulk"},
		{chatStream, "token"},
	} {
		if code, by := f.send("/v1/chat/completions", c.body); by != c.want {
			t.Errorf("%s: %d through %q, want %q", c.body, code, by, c.want)
		}
	}
}

// TestBulkCoversThePassthroughPaths: the rule covers every routed POST: the listed endpoints and the
// passthrough of any other path under /v1/ (HBEP-6: "the listed endpoints and the passthrough").
func TestBulkCoversThePassthroughPaths(t *testing.T) {
	f := newBulkFront(t, 1, false)
	for _, path := range []string{"/v1/completions", "/v1/embeddings", "/v1/responses", "/v1/some/other/path"} {
		body := `{"model":"m-0","prompt":"hello","input":"hello"}`
		if _, by := f.send(path, body); by != "bulk" {
			t.Errorf("POST %s went through %q, want the bulk transport", path, by)
		}
	}
}

// TestUnregisteredBulkIsNotUsed: an Instance whose bulk transport was never registered (no bulk
// proxy: an Allocation staged before the bulk path) takes the token transport for every request,
// and nothing counts as a fallback, since nothing tried the bulk transport.
func TestUnregisteredBulkIsNotUsed(t *testing.T) {
	f := newBulkFront(t, 1, false)
	f.bulk.unregister(f.uids[0])
	f.muxBulk.drop(f.uids[0])
	f.bulk.prober.Round(context.Background())
	if f.bulk.tun.Registered(f.uids[0]) {
		t.Fatal("a probe of a missing bulk proxy registered it")
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "token" {
		t.Errorf("with no bulk registration: %d through %q, want 200 through the token transport", code, by)
	}
	if _, fb, _ := f.bulkCounts(); fb != 0 {
		t.Errorf("fallbacks %v, want 0: nothing tried the bulk transport", fb)
	}
}

// TestBulkSharesTheCap: a bulk request takes one place in the Instance's in-flight count while it
// runs, as any request does (HBEP-6: "A bulk request takes one place in the Instance's cap").
func TestBulkSharesTheCap(t *testing.T) {
	f := newBulkFront(t, 1, false)
	hold := make(chan struct{})
	f.engBulk.Set(enginetest.Behaviour{Hold: hold})
	done := make(chan string, 1)
	go func() { _, by := f.send("/v1/chat/completions", chatPlain); done <- by }()
	waitRouterState(t, "the bulk request to be in flight", func() bool {
		_, _, n := f.routingOf(f.uids[0])
		return n == 1
	})
	close(hold)
	if by := <-done; by != "bulk" {
		t.Errorf("the held request went through %q", by)
	}
	waitRouterState(t, "the in-flight count to fall back to 0", func() bool {
		_, _, n := f.routingOf(f.uids[0])
		return n == 0
	})
}

// routingOf is tunFront.routing for this front.
func (f *bulkFront) routingOf(uid types.UID) (member, eligible bool, inFlight int64) {
	return (&tunFront{harness: f.harness}).routing(uid)
}

// TestBulkAndTokenPoolsNeverShareAConnection: each transport keeps its own pool, so keep-alive can't
// reuse the wrong tunnel: within upstreamIdleTimeout a bulk request, a streamed one and a second bulk
// one open one connection on each engine, and the second bulk request reuses the first's.
func TestBulkAndTokenPoolsNeverShareAConnection(t *testing.T) {
	f := newBulkFront(t, 1, false)
	for _, c := range []struct{ body, want string }{{chatPlain, "bulk"}, {chatStream, "token"}, {chatPlain, "bulk"}} {
		if _, by := f.send("/v1/chat/completions", c.body); by != c.want {
			t.Fatalf("%s went through %q", c.body, by)
		}
	}
	if b, tk := len(f.engBulk.Conns()), len(f.engToken.Conns()); b != 1 || tk != 1 {
		t.Errorf("connections: bulk engine %d, token engine %d; want 1 each (the second bulk request reuses the first's)", b, tk)
	}
}

// ---- epochs (HBEP-6, Test plan: "a bulk NewProxy and CloseProxy move only the bulk epoch") ----

// pluginCall posts frps's call op for uid to the plugin h serves, and requires an acceptance.
func pluginCall(t *testing.T, h *frpsPlugin, op string, uid types.UID) {
	t.Helper()
	rec := httptest.NewRecorder()
	body := fmt.Sprintf(`{"version":"0.1.0","op":%q,"content":{"proxy_name":%q}}`, op, uid)
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, h.servedPath(), strings.NewReader(body)))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"reject":true`) {
		t.Fatalf("the plugin answered %d %s", rec.Code, rec.Body.String())
	}
}

// TestBulkPluginMovesOnlyTheBulkEpoch: a bulk NewProxy or CloseProxy, at the bulk plugin's path,
// moves the UID's bulk epoch and takes the bulk registration away, and leaves the token epoch and
// registration as they were; a token NewProxy or CloseProxy does the reverse.
func TestBulkPluginMovesOnlyTheBulkEpoch(t *testing.T) {
	f := newBulkFront(t, 1, false)
	uid := f.uids[0]
	tokenPlugin := &frpsPlugin{tun: f.tun, prober: f.prober, log: f.log}
	for _, op := range []string{opNewProxy, opCloseProxy} {
		f.prober.Round(context.Background())
		f.bulk.prober.Round(context.Background())
		te, be := f.tun.Epoch(uid), f.bulk.tun.Epoch(uid)
		pluginCall(t, f.bulk.plugin(f.log), op, uid)
		if f.bulk.tun.Epoch(uid) == be || f.bulk.tun.Registered(uid) {
			t.Errorf("bulk %s: the bulk epoch %d -> %d, registered %v; want moved and unregistered", op, be, f.bulk.tun.Epoch(uid), f.bulk.tun.Registered(uid))
		}
		if f.tun.Epoch(uid) != te || !f.tun.Registered(uid) {
			t.Errorf("bulk %s: the token epoch %d -> %d, registered %v; want untouched", op, te, f.tun.Epoch(uid), f.tun.Registered(uid))
		}
		f.bulk.prober.Reprobe(context.Background())
		if !f.bulk.tun.Registered(uid) {
			t.Fatalf("bulk %s: the bulk re-probe did not register it again", op)
		}
		te, be = f.tun.Epoch(uid), f.bulk.tun.Epoch(uid)
		pluginCall(t, tokenPlugin, op, uid)
		if f.tun.Epoch(uid) == te || f.tun.Registered(uid) {
			t.Errorf("token %s: the token epoch did not move, or the token registration stayed", op)
		}
		if f.bulk.tun.Epoch(uid) != be || !f.bulk.tun.Registered(uid) {
			t.Errorf("token %s: the bulk epoch %d -> %d, registered %v; want untouched", op, be, f.bulk.tun.Epoch(uid), f.bulk.tun.Registered(uid))
		}
	}
	if tokenPlugin.servedPath() != "/frp/v1/front" || f.bulk.plugin(f.log).servedPath() != "/frp/v1/front/bulk" {
		t.Errorf("plugin paths %q and %q", tokenPlugin.servedPath(), f.bulk.plugin(f.log).servedPath())
	}
}

// ---- #448's trap: a fallback that isn't the resend ----

// TestBulkDialErrorFallsBackWithoutPause: the bulk transport is registered and has carried a request;
// then the bulk proxy goes (frps answers 404, as after a kill whose CloseProxy the front has not
// heard). The next non-streamed request is served by the SAME Instance's token transport, starts no
// failure run, counts one fallback, and leaves the bulk transport unregistered; the next one goes to
// the token transport directly. When the bulk proxy is back, a probe registers it again.
//
// upstreamIdleTimeout is 1 ns here, so no pooled connection is reused (Go drops an idle one older
// than it before use): the test double's 404 takes the proxy away from new CONNECTs only, while a
// killed frpc's connections close, and frps's CloseProxy makes the front's plugin drop the pool's.
// The request after the loss must meet the dropped proxy at its dial, which is what this tests.
func TestBulkDialErrorFallsBackWithoutPause(t *testing.T) {
	f := newBulkFront(t, 1, false, func(c *Config) { c.UpstreamIdleTimeout = Duration(time.Nanosecond) })
	uid := f.uids[0]
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
		t.Fatalf("before the loss: %d through %q; the test needs a registered bulk transport that carried a request", code, by)
	}
	f.muxBulk.drop(uid)
	if !f.bulk.tun.Registered(uid) {
		t.Fatal("the bulk transport is not registered when its dial fails: this would not be a fallback")
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "token" {
		t.Fatalf("after the loss: %d through %q, want 200 through the same Instance's token transport", code, by)
	}
	if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
		t.Errorf("the bulk dial error counted toward the pause: failures %d, paused %v", n, f.router.Paused(f.keys[0]))
	}
	if f.bulk.tun.Registered(uid) {
		t.Error("the bulk transport is still registered after its dial failed")
	}
	if b, fb, reg := f.bulkCounts(); b != 1 || fb != 1 || reg != 0 {
		t.Errorf("metrics: bulk %v, fallback %v, registered %v; want 1, 1, 0", b, fb, reg)
	}
	if _, by := f.send("/v1/chat/completions", chatPlain); by != "token" {
		t.Errorf("the next request went through %q, want the token transport directly", by)
	}
	if _, fb, _ := f.bulkCounts(); fb != 1 {
		t.Errorf("fallbacks %v after a request that did not try the bulk transport, want still 1", fb)
	}
	f.muxBulk.set(uid, http.StatusOK)
	f.bulk.prober.Round(context.Background())
	if !f.bulk.tun.Registered(uid) {
		t.Fatal("the bulk proxy is back, and a probe did not register it again")
	}
	if _, by := f.send("/v1/chat/completions", chatPlain); by != "bulk" {
		t.Errorf("after the re-registration the request went through %q", by)
	}
}

// TestBulkEpochFenceFallsBack: a bulk dial obtained before a bulk re-registration fails on the epoch
// fence after frps's 200 with nothing written, and the request takes the token transport, counting
// nothing.
func TestBulkEpochFenceFallsBack(t *testing.T) {
	f := newBulkFront(t, 1, false)
	uid := f.uids[0]
	held, release := f.muxBulk.holdNext(t, uid, "connect")
	done := make(chan [2]any, 1)
	go func() { c, by := f.send("/v1/chat/completions", chatPlain); done <- [2]any{c, by} }()
	select {
	case <-held:
	case r := <-done:
		t.Fatalf("the request ended (%v through %q) without its CONNECT reaching the bulk frps", r[0], r[1])
	case <-time.After(failAfter):
		t.Fatal("the request's CONNECT never reached the bulk frps")
	}
	pluginCall(t, f.bulk.plugin(f.log), opNewProxy, uid) // frps registers the bulk proxy again
	release()
	r := <-done
	if r[0] != http.StatusOK || r[1] != "token" {
		t.Errorf("a dial fenced by a new bulk registration: %v through %q, want 200 through the token transport", r[0], r[1])
	}
	if n := f.router.failures(f.keys[0]); n != 0 {
		t.Errorf("the fence counted %d failures", n)
	}
	if _, fb, _ := f.bulkCounts(); fb != 1 {
		t.Errorf("fallbacks %v, want 1", fb)
	}
}

// sessionFor returns a session header value whose request the router sends to the Instance key
// first, by the router's own choice (its ring and the conversation key), so a test can aim a
// request at one Instance of several.
func (f *bulkFront) sessionFor(target string) string {
	f.t.Helper()
	n, err := naming.Parse("m-0")
	if err != nil {
		f.t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		sid := fmt.Sprintf("session-%d", i)
		key, _ := convkey.Key(http.Header{"X-Session-Id": {sid}}, f.cfg.SessionHeaders, []byte(`[{"role":"user","content":"hello"}]`))
		tg, rerr := f.router.choose(context.Background(), n, key, "")
		if rerr != nil {
			f.t.Fatalf("choose: %s", rerr.message)
		}
		got := tg.key()
		tg.release()
		if got == target {
			return sid
		}
	}
	f.t.Fatalf("no session of 256 is routed to %s first", target)
	return ""
}

// TestFallbackLeavesTheOneResend (#448's trap): the fallback isn't the request's resend. Instance A's
// bulk dial fails, and so does its token dial (both proxies gone, both transports still
// registered): the request is still resent once, to the next target B, which serves it, as HBEP-5
// says. A counts one failure, its token dial's; the bulk dial counts none.
func TestFallbackLeavesTheOneResend(t *testing.T) {
	f := newBulkFront(t, 2, false)
	a := f.uids[0]
	sid := f.sessionFor(f.keys[0])
	f.muxBulk.drop(a)
	f.muxToken.drop(a)
	if !f.bulk.tun.Registered(a) || !f.tun.Registered(a) {
		t.Fatal("A's transports are not both registered when their dials fail")
	}
	code, by := f.send("/v1/chat/completions", chatPlain, "X-Session-Id", sid)
	if code != http.StatusOK || by == "" {
		t.Fatalf("the request: %d through %q, want 200 from B after the one resend", code, by)
	}
	if n := f.router.failures(f.keys[0]); n != 1 {
		t.Errorf("A's failures %d, want 1: the token dial error counts, the bulk one doesn't", n)
	}
	if n := f.router.failures(f.keys[1]); n != 0 {
		t.Errorf("B's failures %d, want 0", n)
	}
	if _, fb, _ := f.bulkCounts(); fb != 1 {
		t.Errorf("fallbacks %v, want 1", fb)
	}
}

// ---- #448's trap: a bulk cut before headers that doesn't pause ----

// TestBulkClosedBeforeHeadersRaisesNoPause: a bulk request the engine closes before its response
// headers gets 502 upstream_closed with cutBy tunnel, unregisters the bulk transport and puts it on
// the re-probe schedule, and counts nothing toward the pause. Three in a row (pauseAfterFailures),
// each after the bulk transport registered again, leave the Instance unpaused, and none is replayed.
// The control, the same three closes on a front without the bulk path, pauses it.
func TestBulkClosedBeforeHeadersRaisesNoPause(t *testing.T) {
	for _, noBulk := range []bool{false, true} {
		f := newBulkFront(t, 1, noBulk)
		uid, key := f.uids[0], f.keys[0]
		eng := f.engBulk
		if noBulk {
			eng = f.engToken
		}
		eng.Set(enginetest.Behaviour{CloseBeforeHeaders: true})
		for i := 0; i < f.cfg.PauseAfterFailures; i++ {
			if !noBulk && !f.bulk.tun.Registered(uid) {
				t.Fatalf("close %d: the bulk transport is not registered", i)
			}
			resp := f.post("/v1/chat/completions", chatPlain)
			body := readAll(t, resp)
			if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "upstream_closed") {
				t.Errorf("without bulk %v, close %d: %d %s, want 502 upstream_closed", noBulk, i, resp.StatusCode, body)
			}
			if noBulk {
				continue
			}
			if f.bulk.tun.Registered(uid) {
				t.Errorf("close %d: the bulk transport is still registered", i)
			}
			if _, ok := f.bulk.prober.reprobeAt(); !ok {
				t.Errorf("close %d: the bulk transport is not on the re-probe schedule", i)
			}
			f.bulk.prober.Reprobe(context.Background())
		}
		paused := f.router.Paused(key)
		switch {
		case !noBulk && (paused || f.router.failures(key) != 0):
			t.Errorf("three bulk closes before headers: paused %v, failures %d; want neither", paused, f.router.failures(key))
		case noBulk && !paused:
			t.Errorf("control: three token closes before headers did not pause the Instance (failures %d): the test can't fail", f.router.failures(key))
		}
		if !noBulk {
			if n := len(f.engToken.Requests()); n != 0 {
				t.Errorf("a closed bulk request was replayed on the token transport: %d requests there", n)
			}
			if n := len(f.engBulk.Requests()); n != f.cfg.PauseAfterFailures {
				t.Errorf("the bulk engine saw %d requests, want %d, one per send", n, f.cfg.PauseAfterFailures)
			}
		}
	}
}

// TestBulkIdleCutUnregistersWithoutPause (the plan's R5, HBEP-6:518): a bulk request that gets no
// byte within idleTimeout, as on a frozen bulk work connection, is cut by the front as on the token
// path (504, front_idle), and it also unregisters the bulk transport and re-probes it; the cut counts
// nothing toward the pause.
func TestBulkIdleCutUnregistersWithoutPause(t *testing.T) {
	f := newBulkFront(t, 1, false, func(c *Config) { c.IdleTimeout = Duration(300 * time.Millisecond) })
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	f.engBulk.Set(enginetest.Behaviour{Hold: hold})
	resp := f.post("/v1/chat/completions", chatPlain)
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusGatewayTimeout || !strings.Contains(body, "request_timeout") {
		t.Fatalf("a bulk request with no byte past idleTimeout: %d %s, want 504 request_timeout", resp.StatusCode, body)
	}
	if f.bulk.tun.Registered(f.uids[0]) {
		t.Error("the idle cut left the bulk transport registered")
	}
	if _, ok := f.bulk.prober.reprobeAt(); !ok {
		t.Error("the idle cut did not put the bulk transport on the re-probe schedule")
	}
	if n := f.router.failures(f.keys[0]); n != 0 || f.router.Paused(f.keys[0]) {
		t.Errorf("the idle cut counted toward the pause: failures %d", n)
	}
}

// TestBulkFiveHundredCountsTowardPause: a response's own outcome counts as on the token path: three
// 5xx answers through the bulk transport pause the Instance.
func TestBulkFiveHundredCountsTowardPause(t *testing.T) {
	f := newBulkFront(t, 1, false)
	f.engBulk.Set(enginetest.Behaviour{Status: http.StatusInternalServerError, ErrorBody: `{"error":{"message":"boom"}}`})
	for i := 0; i < f.cfg.PauseAfterFailures; i++ {
		if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusInternalServerError || by != "bulk" {
			t.Fatalf("send %d: %d through %q", i, code, by)
		}
	}
	if !f.router.Paused(f.keys[0]) {
		t.Errorf("three 5xx through the bulk transport did not pause the Instance (failures %d)", f.router.failures(f.keys[0]))
	}
}

// TestBulkProbeFailuresRaiseNoEventAndNoPause: failing bulk probes unregister the bulk transport only:
// no PathProbeFailing, no pause, and the token transport keeps serving.
func TestBulkProbeFailuresRaiseNoEventAndNoPause(t *testing.T) {
	f := newBulkFront(t, 1, false)
	uid := f.uids[0]
	f.muxBulk.set(uid, http.StatusServiceUnavailable)
	for i := 0; i < f.cfg.ProbeFailures; i++ {
		f.bulk.prober.Round(context.Background())
	}
	if f.bulk.tun.Registered(uid) {
		t.Fatal("probeFailures failed bulk probes left the bulk transport registered")
	}
	if !f.tun.Registered(uid) || f.router.Paused(f.keys[0]) {
		t.Errorf("the token transport: registered %v, paused %v", f.tun.Registered(uid), f.router.Paused(f.keys[0]))
	}
	for _, e := range f.rec.Events() {
		if e.Code == reasonPathProbeFailing {
			t.Errorf("a failing bulk probe raised %s", e.Code)
		}
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "token" {
		t.Errorf("with the bulk transport unregistered: %d through %q", code, by)
	}
}

// ---- metrics (HBEP-6, The bulk path, Metrics; ruling R1) ----

// TestBulkMetrics: requests written to the bulk transport count path=bulk, and the gauge counts the
// Instances whose bulk transport is registered; both start at 0 with the bulk path on.
func TestBulkMetrics(t *testing.T) {
	f := newBulkFront(t, 2, false)
	if b, fb, reg := f.bulkCounts(); b != 0 || fb != 0 || reg != 2 {
		t.Errorf("at the start: bulk %v, fallback %v, registered %v; want 0, 0, 2", b, fb, reg)
	}
	f.send("/v1/chat/completions", chatPlain)
	f.send("/v1/chat/completions", chatStream)
	if b, _, _ := f.bulkCounts(); b != 1 {
		t.Errorf("bulk %v after one non-streamed and one streamed request, want 1", b)
	}
	f.bulk.unregister(f.uids[1])
	if _, _, reg := f.bulkCounts(); reg != 1 {
		t.Errorf("registered %v after one Instance lost its bulk registration, want 1", reg)
	}
}

// TestNoBulkSeriesWithoutBulkPath: without the bulk path /metrics has no bulk series (S5: the
// metrics are as before). The control: with it, both counters and the gauge are there.
func TestNoBulkSeriesWithoutBulkPath(t *testing.T) {
	without := newBulkFront(t, 1, true)
	if b, fb, reg := without.bulkCounts(); b != -1 || fb != -1 || reg != -1 {
		t.Errorf("without the bulk path: bulk %v, fallback %v, registered %v; want all absent", b, fb, reg)
	}
	with := newBulkFront(t, 1, false)
	if b, fb, reg := with.bulkCounts(); b != 0 || fb != 0 || reg != 1 {
		t.Errorf("with the bulk path: bulk %v, fallback %v, registered %v; want 0, 0, 1", b, fb, reg)
	}
}

// TestAClientGoneDuringTheBulkDialCountsNothing (R1; the review of #461): a client that leaves while
// the bulk CONNECT is still held wrote nothing on the bulk transport, so the request counts neither
// path="bulk" nor path="fallback". Go's transport returns the context's cause there, not the dial
// error, so the count must come from the bytes written. The control: a request that completes
// counts path="bulk" once.
func TestAClientGoneDuringTheBulkDialCountsNothing(t *testing.T) {
	f := newBulkFront(t, 1, false)
	uid := f.uids[0]
	held, release := f.muxBulk.holdNext(t, uid, "connect")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url+"/v1/chat/completions", strings.NewReader(chatPlain))
		if err != nil {
			done <- err
			return
		}
		req.Header.Set("Authorization", "Bearer "+f.clientKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-held:
	case <-time.After(failAfter):
		t.Fatal("the request's CONNECT never reached the bulk frps")
	}
	cancel()
	<-done
	release()
	waitRouterState(t, "the request to leave the Instance's cap", func() bool {
		_, _, n := f.routingOf(uid)
		return n == 0
	})
	if b, fb, _ := f.bulkCounts(); b != 0 || fb != 0 {
		t.Errorf("a client gone during the bulk dial counted bulk %v, fallback %v; want 0, 0: nothing was written", b, fb)
	}
	if code, by := f.send("/v1/chat/completions", chatPlain); code != http.StatusOK || by != "bulk" {
		t.Fatalf("the control request: %d through %q", code, by)
	}
	if b, _, _ := f.bulkCounts(); b != 1 {
		t.Errorf("a completed bulk request counted bulk %v, want 1", b)
	}
}

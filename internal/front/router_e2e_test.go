package front

// HBEP-5's Test plan, Beta, the router: Headers, Worker health (one worker killed while a stream runs)
// and Restart, end to end. The front relays through a byte bridge into the agent's serving socket
// (agenttest.Router.Bridge, standing in for frps and frpc, which carry bytes unchanged; the Bytes test
// in internal/agent runs the real frps and frpc), then the agent's forwarder in router mode, the Envoy
// the agent starts from its pin with its own config, and an engine double as worker-0 on its Unix
// socket, health-checked through the agent's readiness endpoint. Everything runs on the wall clock,
// since Envoy does.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agenttest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

const reInst = "ns/md-0" // the Instance's key in the front's router

// reFix is a front with one Instance behind the agent's router.
type reFix struct {
	*harness
	e   *enginetest.Engine
	r   *agenttest.Router
	uid types.UID
	key string
}

func newReFix(t *testing.T, b enginetest.Behaviour) *reFix {
	t.Helper()
	h := newHarnessReal(t, nil)
	f := &reFix{harness: h, key: newKey(t)}
	f.r = agenttest.StartRouter(t, func(ln net.Listener) {
		f.e = enginetest.Serve(t, ln, enginetest.Options{Key: f.key})
	})
	f.e.Set(b)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	f.uid = h.instance("md", "md-0", "helios-a", nil, f.key, instanceOpts{})
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	h.trans.Set(f.uid, f.r.Bridge(t, ln))
	f.setHealthy(1)
	return f
}

// setHealthy sets the Instance behind the router and its healthy workers, as the lifecycle
// controller writes them, and hands the snapshot to the router.
func (f *reFix) setHealthy(n int32) {
	in := f.inst(f.uid)
	in.Spec.Launch.Router = v1alpha1.InstanceRouterEnvoy
	in.Status.HealthyWorkers = &n
	f.apply()
}

const reBody = `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`

// Headers: the client's x-envoy-* and X-Forwarded-For don't reach the worker through the front and the
// real Envoy, its other headers do, the worker gets the Instance key and the session key, and the
// request did cross Envoy (its x-request-id). A request without the Instance key, sent into the
// agent's serving socket, gets the worker's own 401 back through Envoy.
func TestRouterHeadersEndToEnd(t *testing.T) {
	f := newReFix(t, enginetest.Behaviour{Tokens: 2})
	resp := ehPost(t, f.harness, reBody, ehStripped(), ehKept())
	if got := readAll(t, resp); resp.StatusCode != http.StatusOK {
		t.Fatalf("through the front and the router: status %d: %s", resp.StatusCode, got)
	}
	via := f.e.Requests()
	if len(via) != 1 {
		t.Fatalf("the worker saw %d requests, want 1", len(via))
	}
	got := via[0].Header
	for k := range got {
		if l := strings.ToLower(k); strings.HasPrefix(l, "x-envoy-") || l == "x-forwarded-for" {
			t.Errorf("the worker received %s: %q", k, got[k])
		}
	}
	ehWant(t, "through the router", got, "X-Client-Trace", []string{"t-1"})
	ehWant(t, "through the router", got, "X-Envoyish", []string{"kept"})
	ehWant(t, "through the router", got, "Authorization", []string{"Bearer " + f.key})
	if got.Get("X-Session-Id") == "" {
		t.Error("the worker received no X-Session-Id")
	}
	if got.Get("X-Request-Id") == "" {
		t.Error("the worker received no x-request-id: the request did not cross Envoy")
	}

	status, body := reRaw(t, f.r.Serving, "POST /v1/chat/completions HTTP/1.1\r\nHost: instance\r\nContent-Type: application/json\r\nContent-Length: "+
		fmt.Sprint(len(reBody))+"\r\nConnection: close\r\n\r\n"+reBody)
	if status != http.StatusUnauthorized {
		t.Errorf("a request without the Instance key, through the serving socket: %d %q, want the worker's 401", status, body)
	}
	if n := len(f.e.Requests()); n != 2 {
		t.Errorf("the worker saw %d requests, want the keyed one and the refused one", n)
	}
}

// The control of the Headers test, and what the router path does on its own with the client's
// headers (measured on Envoy 1.40.2 with the agent's config): sent into the agent's serving socket
// with no front, X-Envoy-Upstream-Rq-Timeout-Ms and x-envoy-retry-on are dropped and not acted on (a
// 1 ms timeout does not cut a worker 800 ms slow), and X-Forwarded-For is dropped by the forwarder (Go's
// httputil.ReverseProxy drops the X-Forwarded-* headers when it rewrites). But X-Envoy-Original-Path
// reaches the worker: the front's stripping is what removes it, so the Headers test sees a front that
// stops stripping.
func TestTheRouterPathOnItsOwnAndTheClientsHeaders(t *testing.T) {
	f := newReFix(t, enginetest.Behaviour{Tokens: 2, FirstToken: 800 * time.Millisecond})
	send := func(header string) (int, http.Header) {
		t.Helper()
		before := len(f.e.Requests())
		status, body := reRaw(t, f.r.Serving, fmt.Sprintf("POST /v1/chat/completions HTTP/1.1\r\nHost: instance\r\nContent-Type: application/json\r\nAuthorization: Bearer %s\r\n%s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
			f.key, header, len(reBody), reBody))
		via := f.e.Requests()
		if len(via) != before+1 {
			t.Fatalf("sent with %q: %d %.100q, and the worker saw %d requests, want 1", header, status, body, len(via)-before)
		}
		return status, via[len(via)-1].Header
	}
	if status, got := send("X-Envoy-Upstream-Rq-Timeout-Ms: 1"); status != http.StatusOK || got.Get("X-Envoy-Upstream-Rq-Timeout-Ms") != "" {
		t.Errorf("X-Envoy-Upstream-Rq-Timeout-Ms 1 to a worker 800 ms slow: %d, the worker saw %q; want 200 and none", status, got.Get("X-Envoy-Upstream-Rq-Timeout-Ms"))
	}
	if _, got := send("x-envoy-retry-on: 5xx,reset"); got.Get("X-Envoy-Retry-On") != "" {
		t.Errorf("x-envoy-retry-on reached the worker: %q", got.Get("X-Envoy-Retry-On"))
	}
	if _, got := send("X-Forwarded-For: 10.1.2.3"); got.Get("X-Forwarded-For") != "" {
		t.Errorf("X-Forwarded-For reached the worker: %q", got.Get("X-Forwarded-For"))
	}
	if _, got := send("X-Envoy-Original-Path: /admin"); got.Get("X-Envoy-Original-Path") != "/admin" {
		t.Errorf("X-Envoy-Original-Path sent with no front reached the worker as %q, want /admin: the Headers test needs a header only the front removes", got.Get("X-Envoy-Original-Path"))
	}
}

// reRaw sends req on a new connection to the Unix socket at path and returns the status and body.
func reRaw(t *testing.T, path, req string) (int, string) {
	t.Helper()
	c, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// Worker health with one worker killed while a stream runs (HBEP-5, Test plan, Beta, the router,
// Worker health): the stream is in flight first, so a run with nothing in flight cannot pass. The
// worker stops (its server closes its listener and every connection, as a killed process's sockets
// do): the stream is cut, with the front's stream_cut event and the record's upstream cut; the next
// requests fail; after pauseAfterFailures failures in a row the Instance is paused; and Envoy holds the
// worker failing within about 10 s.
func TestRouterWorkerKilledWhileAStreamRuns(t *testing.T) {
	f := newReFix(t, enginetest.Behaviour{Tokens: 400, PerToken: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url+"/v1/chat/completions",
		strings.NewReader(`{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	resp, err := ctHTTP().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(first, "data: ") {
		t.Fatalf("the stream's first line %q, %v: the stream must be in flight before the kill", first, err)
	}
	if n := f.e.Open(); n < 1 {
		t.Fatalf("the worker has %d requests open: the stream must be in flight before the kill", n)
	}
	f.e.Close() // the worker dies
	killed := time.Now()
	rest, _ := io.ReadAll(br)
	raw := first + string(rest)
	if strings.Contains(raw, "[DONE]") || !strings.Contains(raw, `"stream_cut"`) {
		t.Errorf("the killed worker's stream ended without the front's stream_cut event, or with [DONE]: %.300q", raw[max(0, len(raw)-300):])
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	if rec.CutBy == nil || *rec.CutBy != "upstream" {
		t.Errorf("the cut stream's record has cutBy %v, want upstream", rec.CutBy)
	}
	// The next requests fail, through Envoy, until the Instance is paused: the cut counted as the first.
	sent := 0
	for !f.router.Paused(reInst) {
		if sent == f.cfg.PauseAfterFailures {
			t.Fatalf("after the cut and %d more failed requests the Instance is not paused (pauseAfterFailures %d, failures %d)",
				sent, f.cfg.PauseAfterFailures, f.router.failures(reInst))
		}
		r := ehPost(t, f.harness, reBody)
		body := readAll(t, r)
		if r.StatusCode < 500 {
			t.Fatalf("a request after the kill: %d %q, want a failure", r.StatusCode, body)
		}
		sent++
	}
	t.Logf("paused after the cut and %d failed requests", sent)
	f.r.WaitWorker(t, "failed_active", 30*time.Second)
	if took := time.Since(killed); took > 12*time.Second {
		t.Errorf("Envoy's active check held the killed worker failing %v after the kill, want within about 10 s", took)
	} else {
		t.Logf("Envoy's active check held the killed worker failing within %v of the kill", took)
	}
}

// Restart (HBEP-5, Test plan, Beta, the router): after a router restart, the worker is back before the
// Instance is routable again. The lifecycle controller writes healthyWorkers 0 before it raises the
// router's generation, and the count again from the new router's report: in between, the front routes
// nothing to the Instance, even once the new Envoy serves. The worker counts every request it gets.
func TestRouterRestartKeepsTheInstanceUnroutableUntilTheWorkerIsBack(t *testing.T) {
	f := newReFix(t, enginetest.Behaviour{Tokens: 2})
	if r := ehPost(t, f.harness, reBody); readAll(t, r) == "" || r.StatusCode != http.StatusOK {
		t.Fatalf("before the restart: %d", r.StatusCode)
	}
	f.setHealthy(0) // the router rung's first write
	f.r.KillEnvoy(t)
	before := len(f.e.Requests())
	r := ehPost(t, f.harness, reBody)
	readAll(t, r)
	if r.StatusCode == http.StatusOK || len(f.e.Requests()) != before {
		t.Errorf("with healthyWorkers 0 and the router down: %d, and the worker saw %d new requests; want no route", r.StatusCode, len(f.e.Requests())-before)
	}
	f.r.RestartEnvoy(t) // the agent starts the router again; it holds worker-0 healthy
	r = ehPost(t, f.harness, reBody)
	readAll(t, r)
	if r.StatusCode == http.StatusOK || len(f.e.Requests()) != before {
		t.Errorf("with healthyWorkers still 0 and the new router serving: %d, and the worker saw %d new requests; want no route until the controller writes the count", r.StatusCode, len(f.e.Requests())-before)
	}
	f.setHealthy(1) // the controller, from the new router's report
	r = ehPost(t, f.harness, reBody)
	if body := readAll(t, r); r.StatusCode != http.StatusOK {
		t.Fatalf("with healthyWorkers 1 after the restart: %d %q, want the worker's 200", r.StatusCode, body)
	}
	if via := f.e.Requests(); len(via) != before+1 || via[len(via)-1].Header.Get("X-Request-Id") == "" {
		t.Errorf("after the restart the worker saw %d new requests, want one through the new Envoy", len(via)-before)
	}
}

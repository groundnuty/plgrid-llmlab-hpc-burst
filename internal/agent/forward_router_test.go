package agent_test

// The serving path in router mode is frpc's proxy, the agent's forwarder, Envoy, then the worker
// (TL1, the science agent's ruling on #429). The forwarder is Go's httputil.ReverseProxy, which S13's
// byte-exactness, measured on Envoy alone, does not cover. So the same request is sent twice from a
// front-like client (#440's frontLikePost): straight to Envoy's listener, and through the agent's own
// forwarder in router mode. The worker must see the same headers both ways, apart from the
// x-request-id value Envoy draws for each request, so no X-Forwarded-For and nothing else is added,
// and must see the body as sent. A mutant forwarder that sets X-Forwarded-For, the header a Unix peer
// would get from SetXForwarded, is the positive control.

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// routerForwardDiff sends the request straight to Envoy and through the socket at via, and returns how
// the worker's view of the second differs from the first.
func routerForwardDiff(t *testing.T, r *router, via string) []string {
	t.Helper()
	if s := frontLikePost(t, r.listener()); s != http.StatusOK {
		t.Fatalf("straight to Envoy: status %d", s)
	}
	if s := frontLikePost(t, via); s != http.StatusOK {
		t.Fatalf("through %s: status %d", filepath.Base(via), s)
	}
	reqs := r.worker.Requests()
	if len(reqs) != 2 {
		t.Fatalf("the worker saw %d requests, want 2", len(reqs))
	}
	problems := headerDiff(reqs[0].Header, reqs[1].Header, "X-Request-Id")
	for i, rq := range reqs {
		if !bytes.Equal(rq.Body, []byte(forwardBody)) {
			problems = append(problems, []string{"straight", "through the forwarder"}[i]+": the body arrived changed")
		}
	}
	return problems
}

func TestTheForwarderAddsNothingOnTheWayToTheRouter(t *testing.T) {
	r := agentRouter(t, workerSpec{})
	f, err := agent.NewForwarder(filepath.Join(r.dir, "serve.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.RouteTo(r.listener())
	for _, p := range routerForwardDiff(t, r, f.Path()) {
		t.Error(p)
	}
}

// The control: a forwarder like the agent's that also sets the X-Forwarded-* headers is seen.
func TestTheRouterForwarderCheckSeesAnAddedHeader(t *testing.T) {
	r := agentRouter(t, workerSpec{})
	sock := filepath.Join(r.dir, "mutant.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	mutant := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: "engine"})
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded() // the mutant: what the agent's forwarder must not do
		},
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", r.listener())
		}, DisableCompression: true},
	}
	srv := &http.Server{Handler: mutant}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	if problems := routerForwardDiff(t, r, sock); !hasProblem(problems, "added X-Forwarded-") {
		t.Errorf("a forwarder that adds X-Forwarded-* passed the check: %q", problems)
	}
}

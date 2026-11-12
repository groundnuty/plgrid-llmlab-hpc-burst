package agent

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Readiness is the agent's per-worker readiness endpoint, which the router health-checks (beta;
// HBEP-5, Worker health; HBEP-7, Several workers). It is one loopback TCP port the kernel chooses, held
// for the Allocation's life, and the worker is named by the check's Host header: each endpoint of
// Envoy's cluster sets health_check_config.hostname to its worker's name (#429's stage 0 measured that
// Envoy 1.40.2 sends it). A TCP port, not a socket: Envoy expects the alternate health-check address
// to be an IP.
//
// GET /ready answers:
//   - 200 for a worker that is verified (the controller's verifiedGeneration equals the worker's
//     generation, and its process runs at that generation) and whose engine's /health passes now;
//   - 503 for a worker it knows that is not, so a restarted worker takes traffic only after its
//     verification, although Envoy's workers are static in its config;
//   - 404 for a name it does not know.
//
// It never answers from the engine's /health alone: a readiness that did would let an unverified
// restart take traffic (#429's acceptance criteria).
type Readiness struct {
	ln  net.Listener
	srv *http.Server

	mu      sync.Mutex
	workers map[string]ReadyWorker
}

// ReadyWorker is what the readiness endpoint knows of one worker.
type ReadyWorker struct {
	// Socket is the engine's Unix socket, whose /health the endpoint probes.
	Socket string
	// Verified says the worker's current process is at the generation the controller verified.
	Verified bool
}

// readinessProbeTimeout bounds the engine probe of one check: below Envoy's 2 s check timeout, so the
// endpoint answers before Envoy gives up.
const readinessProbeTimeout = 1500 * time.Millisecond

// NewReadiness listens on 127.0.0.1 at a port the kernel chooses and serves the endpoint.
func NewReadiness() (*Readiness, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &Readiness{ln: ln, workers: map[string]ReadyWorker{}}
	r.srv = &http.Server{Handler: http.HandlerFunc(r.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = r.srv.Serve(ln) }()
	return r, nil
}

// Port is the endpoint's port on 127.0.0.1.
func (r *Readiness) Port() int { return r.ln.Addr().(*net.TCPAddr).Port }

// Set replaces what the endpoint knows of the workers, by name.
func (r *Readiness) Set(ws map[string]ReadyWorker) {
	cp := make(map[string]ReadyWorker, len(ws))
	for k, v := range ws {
		cp[k] = v
	}
	r.mu.Lock()
	r.workers = cp
	r.mu.Unlock()
}

// Close stops serving.
func (r *Readiness) Close() error { return r.srv.Close() }

func (r *Readiness) serve(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/ready" {
		http.NotFound(w, req)
		return
	}
	name := req.Host
	if h, _, err := net.SplitHostPort(name); err == nil {
		name = h
	}
	r.mu.Lock()
	rw, ok := r.workers[name]
	r.mu.Unlock()
	switch {
	case !ok:
		http.Error(w, "no worker "+strconv.Quote(name), http.StatusNotFound)
	case !rw.Verified:
		http.Error(w, name+" is not verified", http.StatusServiceUnavailable)
	case !engineHealthy(req.Context(), rw.Socket):
		http.Error(w, name+"'s engine does not pass /health", http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// engineHealthy probes the engine's /health on its socket once: vLLM serves it without the key.
func engineHealthy(ctx context.Context, socket string) bool {
	ctx, cancel := context.WithTimeout(ctx, readinessProbeTimeout)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://engine/health", nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

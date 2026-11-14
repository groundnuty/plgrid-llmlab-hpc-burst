package agent_test

// Worker health behind the router, with the real Envoy 1.40.2 (HBEP-5, Worker health; HBEP-7, Several
// workers; #429's PR 3). Envoy health-checks each worker through the agent's readiness endpoint, one
// loopback port with the worker named by Host; outlier detection ejects a worker that answers 5xx; and
// there is no panic mode. Every state below is read from the running Envoy's own /clusters, and every
// failure the tests rely on is made to happen (a stopped worker, an unverified one, 5xx answers), with
// the state before it as the control. The controls that change Envoy's config are the tests' own.

import (
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// readyGet sends GET /ready to the readiness endpoint with the given Host.
func readyGet(t *testing.T, port int, host string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+itoa(port)+"/ready", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /ready (Host %q): %v", host, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// healthServer serves GET /health on a Unix socket in dir with status, until the test ends.
func healthServer(t *testing.T, dir, name string, status int) string {
	t.Helper()
	sock := filepath.Join(dir, name)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// The readiness endpoint answers by Host: 200 for a verified worker whose engine passes /health, 503
// for a worker it knows that is unverified or whose engine fails or is gone, 404 for any other name.
func TestReadinessDispatchesOnHost(t *testing.T) {
	dir := shortDir(t)
	rd, err := agent.NewReadiness()
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	rd.Set(map[string]agent.ReadyWorker{
		"worker-0": {Socket: healthServer(t, dir, "w0.sock", http.StatusOK), Verified: true},
		"worker-1": {Socket: healthServer(t, dir, "w1.sock", http.StatusOK), Verified: false},
		"worker-2": {Socket: healthServer(t, dir, "w2.sock", http.StatusServiceUnavailable), Verified: true},
		"worker-3": {Socket: filepath.Join(dir, "gone.sock"), Verified: true},
	})
	for host, want := range map[string]int{
		"worker-0": 200, "worker-0:80": 200,
		"worker-1": 503, "worker-2": 503, "worker-3": 503,
		"worker-9": 404, "Worker-0": 404, "workers": 404, "": 404,
	} {
		if got := readyGet(t, rd.Port(), host); got != want {
			t.Errorf("Host %q: %d, want %d", host, got, want)
		}
	}
	// Another path is not the endpoint.
	resp, err := http.Get("http://127.0.0.1:" + itoa(rd.Port()) + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /health on the readiness port: %d, want 404", resp.StatusCode)
	}
	// What it knows is replaced, not merged.
	rd.Set(map[string]agent.ReadyWorker{"worker-1": {Socket: filepath.Join(dir, "w1.sock"), Verified: true}})
	if got := readyGet(t, rd.Port(), "worker-0"); got != 404 {
		t.Errorf("worker-0 after a Set without it: %d, want 404", got)
	}
	if got := readyGet(t, rd.Port(), "worker-1"); got != 200 {
		t.Errorf("worker-1 once verified: %d, want 200", got)
	}
}

// hostRecorder stands in for the readiness endpoint to record the Host of every health check, and
// answers 200 for worker-0 and 404 for any other name, as the agent's endpoint does.
type hostRecorder struct {
	ln    net.Listener
	mu    sync.Mutex
	hosts map[string]int
}

func startHostRecorder(t *testing.T) *hostRecorder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &hostRecorder{ln: ln, hosts: map[string]int{}}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.hosts[r.Host]++
		h.mu.Unlock()
		if r.Host != "worker-0" {
			w.WriteHeader(http.StatusNotFound)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return h
}

func (h *hostRecorder) port() int { return h.ln.Addr().(*net.TCPAddr).Port }

func (h *hostRecorder) seen() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]int{}
	for k, v := range h.hosts {
		out[k] = v
	}
	return out
}

// Envoy's checks name the worker by Host, through the agent's config: the recorder sees only
// "worker-0", and Envoy holds worker-0 healthy. Control: the same config with the endpoint's
// health_check_config.hostname deleted sends the cluster's name, which is no worker's, so Envoy holds
// worker-0 failing (#429's stage 0, variant B, made a test).
func TestEnvoySendsEachWorkersNameAsHost(t *testing.T) {
	run := func(t *testing.T, edit func(map[string]any), want string, wantHost string) {
		t.Helper()
		dir := shortDir(t)
		rec := startHostRecorder(t)
		startWorker(t, dir, workerSpec{})
		m := renderMap(t, agent.EnvoyConfig{RunDir: dir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: workerSock(dir)}}, ReadinessPort: rec.port()})
		var r *router
		if edit == nil {
			// The agent's own start, with the recorder as its readiness port.
			p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: testenv.EnvoyBinary(t), Ready: 30 * time.Second,
				Config: agent.EnvoyConfig{RunDir: dir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: workerSock(dir)}}, ReadinessPort: rec.port()}}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Group.Stop(5 * time.Second); <-p.Group.Done(); p.Close() })
			r = &router{dir: dir, proc: p}
		} else {
			edit(m)
			r = &router{dir: dir, raw: startRawEnvoy(t, dir, mustJSON(t, m))}
		}
		r.waitWorker(t, want, 30*time.Second)
		seen := rec.seen()
		if len(seen) != 1 || seen[wantHost] == 0 {
			t.Errorf("the checks' Host headers were %v, want only %q", seen, wantHost)
		}
	}
	t.Run("the agent's config", func(t *testing.T) { run(t, nil, "healthy", "worker-0") })
	t.Run("control: no health_check_config.hostname", func(t *testing.T) {
		run(t, func(m map[string]any) {
			hc := at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config").(map[string]any)
			delete(hc, "hostname")
		}, "failed_active", "workers")
	})
}

// A worker whose engine passes /health but that the controller has not verified gets nothing: the
// readiness endpoint answers 503 and Envoy holds it failing. Envoy never checks the worker's own
// /health: the worker answered none. Once verified, it takes traffic (the control).
func TestEnvoyHoldsAnUnverifiedWorkerUnhealthy(t *testing.T) {
	r := newRouter(t, routerOpts{verified: false, wait: "failed_active"})
	status, body := routerGet(t, r)
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "no healthy upstream") {
		t.Errorf("a request to an unverified worker: %d %q, want Envoy's 503 no healthy upstream", status, body)
	}
	if n := len(r.worker.Requests()); n != 0 {
		t.Errorf("the unverified worker received %d requests, want none", n)
	}
	if n := r.worker.healthHits.Load(); n != 0 {
		t.Errorf("the unverified worker's /health was hit %d times: something checked the engine directly, not the readiness endpoint", n)
	}
	r.ready.Set(map[string]agent.ReadyWorker{"worker-0": {Socket: workerSock(r.dir), Verified: true}})
	r.waitWorker(t, "healthy", 30*time.Second)
	if status, body := routerGet(t, r); status != http.StatusOK || body != workerBody {
		t.Errorf("once verified: %d %q, want the worker's 200", status, body)
	}
	if r.worker.healthHits.Load() == 0 {
		t.Error("once verified, the readiness endpoint never probed the engine's /health")
	}
}

// A worker that stops (its engine gone, as a killed one) is held failing within about 10 s (HBEP-5,
// Test plan, Beta, the router, Worker health): one check interval, 5 s, and the readiness endpoint's
// 503 fails it at once. Control: healthy before the stop.
func TestEnvoyMarksAStoppedWorkerUnhealthy(t *testing.T) {
	r := agentRouter(t, workerSpec{})
	if s := r.workerState(t); s != "healthy" {
		t.Fatalf("before the stop: %s", s)
	}
	r.worker.stop()
	took := r.waitWorker(t, "failed_active", 30*time.Second)
	if took > 12*time.Second {
		t.Errorf("Envoy held the stopped worker failing after %v, want within about 10 s", took)
	}
	t.Logf("held failing %v after the stop", took)
	if status, body := routerGet(t, r); status != http.StatusServiceUnavailable || !strings.Contains(body, "no healthy upstream") {
		t.Errorf("after the stop: %d %q, want Envoy's 503 no healthy upstream", status, body)
	}
}

// No panic mode: with its one worker failing, Envoy answers 503 and routes nothing to it. Control: the
// same with the panic threshold deleted, so Envoy's default 50% applies: with 0% of the workers
// healthy, Envoy panics and routes to the failing worker, which answers.
func TestEnvoyHasNoPanicMode(t *testing.T) {
	r := newRouter(t, routerOpts{verified: false, wait: "failed_active"})
	if status, _ := routerGet(t, r); status != http.StatusServiceUnavailable || len(r.worker.Requests()) != 0 {
		t.Errorf("with no healthy worker: %d and %d requests at the worker, want 503 and none", status, len(r.worker.Requests()))
	}
	c := newRouter(t, routerOpts{verified: false, wait: "failed_active", edit: func(m map[string]any) {
		delete(clusterOf(t, m), "common_lb_config")
	}})
	if status, body := routerGet(t, c); status != http.StatusOK || body != workerBody || len(c.worker.Requests()) != 1 {
		t.Errorf("control, Envoy's default panic threshold: %d %q with %d requests at the worker, want the failing worker's 200", status, body, len(c.worker.Requests()))
	}
}

// Outlier detection on a worker that answers 5xx while its readiness passes. With the agent's config a
// lone worker is never ejected: max_ejection_percent is ⌈100·(n−1)/n⌉, 0 for one worker (the science
// agent's ruling on #565), so the active check alone judges it, and it keeps getting requests. Allowed to
// eject (max_ejection_percent 100, the agent's config otherwise), 3 consecutive 5xx eject the worker, and a
// passing check does not bring it back: it returns only when its ejection, 30 s, ends. Control:
// successful_active_health_check_uneject_host true brings it back at the next passing check, within one
// interval. (With two workers and the agent's config, TestBothWorkersFailingLeaveOneInTheRing shows the
// ejection of one of them.)
func TestEnvoyEjectsAWorkerAfterThree5xx(t *testing.T) {
	fail3 := func(t *testing.T, r *router, status *atomic.Int32) {
		t.Helper()
		status.Store(http.StatusInternalServerError)
		for i := 0; i < 3; i++ {
			if s, _ := routerGet(t, r); s != http.StatusInternalServerError {
				t.Fatalf("5xx %d of 3: Envoy answered %d, want the worker's 500", i+1, s)
			}
		}
	}
	eject := func(t *testing.T, r *router, status *atomic.Int32) time.Duration {
		t.Helper()
		fail3(t, r, status)
		r.waitWorker(t, "ejected", 30*time.Second)
		status.Store(0)
		if s, body := routerGet(t, r); s != http.StatusServiceUnavailable {
			t.Errorf("a request while ejected: %d %q, want Envoy's 503", s, body)
		}
		return r.waitWorker(t, "healthy", 2*time.Minute)
	}
	allowed := func(m map[string]any) {
		clusterOf(t, m)["outlier_detection"].(map[string]any)["max_ejection_percent"] = 100
	}
	t.Run("the agent's config: a lone worker is never ejected", func(t *testing.T) {
		var status atomic.Int32
		r := agentRouter(t, workerSpec{Status: &status})
		fail3(t, r, &status)
		// More 5xx still reach the worker, and Envoy never holds it ejected.
		for i := 0; i < 3; i++ {
			if s, _ := routerGet(t, r); s != http.StatusInternalServerError {
				t.Fatalf("5xx %d after the first 3: Envoy answered %d, want the lone worker's own 500: it was ejected", i+1, s)
			}
		}
		if got := r.workerState(t); got != "healthy" {
			t.Errorf("Envoy holds the lone worker %s after 6 consecutive 5xx, want healthy: max_ejection_percent 0 never ejects it", got)
		}
	})
	t.Run("ejection allowed: out for its 30 s", func(t *testing.T) {
		var status atomic.Int32
		r := controlRouter(t, workerSpec{Status: &status}, allowed)
		back := eject(t, r, &status)
		if back < 20*time.Second {
			t.Errorf("the ejected worker was back after %v, through passing checks (every 5 s): want it out for its 30 s ejection", back)
		}
		t.Logf("back %v after its ejection was seen", back)
	})
	t.Run("control: a passing check unejects", func(t *testing.T) {
		var status atomic.Int32
		r := controlRouter(t, workerSpec{Status: &status}, func(m map[string]any) {
			allowed(m)
			clusterOf(t, m)["outlier_detection"].(map[string]any)["successful_active_health_check_uneject_host"] = true
		})
		back := eject(t, r, &status)
		if back >= 20*time.Second {
			t.Errorf("with uneject on a passing check, the worker was back after %v, want within about one 5 s interval", back)
		}
		t.Logf("back %v after its ejection was seen", back)
	})
}

// routerGet sends GET /v1/models through the router's listener and returns the status and body.
func routerGet(t *testing.T, r *router) (int, string) {
	t.Helper()
	resp, cancel, err := getVia(r.client(t), "/v1/models", 30*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

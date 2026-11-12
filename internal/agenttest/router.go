// Package agenttest runs the allocation agent's serving path in router mode for tests outside
// internal/agent: internal/front's end-to-end router tests (#429). The path is the agent's own pieces,
// wired as its converge loop wires them (internal/agent, router.go): the forwarder on the serving
// socket routed to the Envoy the agent starts from its pin with its own config, in front of a worker
// the test serves on worker-0's Unix socket, health-checked through the agent's readiness endpoint.
//
// It is test support, as net/http/httptest is: it imports testing, and nothing the agent's binary
// builds imports it. It lives outside internal/agent, whose non-test packages may import only the
// standard library (TestAgentImportsOnlyTheStandardLibrary).
package agenttest

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// Router is the agent's serving path in router mode, with its worker served by the test.
type Router struct {
	// Serving is the agent's serving socket, which frpc serves (agent.ServingSocket).
	Serving string
	// Worker is worker-0's socket, which the test serves.
	Worker string
	// RunDir is the job's run directory, where Envoy's sockets, config and log are.
	RunDir string

	bin   string
	ready *agent.Readiness
	fwd   *agent.Forwarder
	mu    sync.Mutex
	envoy *agent.EnvoyProc
}

// StartRouter makes a run directory, has serve serve worker-0 on its socket (serve is called with
// the listener and must not block), starts the agent's readiness endpoint with worker-0 verified, the
// Envoy the agent starts, and the forwarder routed to it, and returns once Envoy holds worker-0
// healthy. Everything is stopped when the test ends.
func StartRouter(t testing.TB, serve func(ln net.Listener)) *Router {
	t.Helper()
	bin := testenv.EnvoyBinary(t)
	dir, err := os.MkdirTemp("", "at")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := &Router{RunDir: dir, Worker: filepath.Join(dir, "worker-0.sock"), Serving: agent.ServingSocket(dir), bin: bin}
	ln, err := net.Listen("unix", r.Worker)
	if err != nil {
		t.Fatal(err)
	}
	serve(ln)
	r.ready, err = agent.NewReadiness()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.ready.Close() })
	r.ready.Set(map[string]agent.ReadyWorker{"worker-0": {Socket: r.Worker, Verified: true}})
	r.fwd, err = agent.NewForwarder(r.Serving)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.fwd.Close() })
	r.fwd.RouteTo("") // router mode from the start: no worker is ever the target
	r.startEnvoy(t)
	t.Cleanup(func() { r.stopEnvoy(syscall.SIGTERM) })
	return r
}

// startEnvoy starts Envoy as the agent does, routes the forwarder to it, and waits until it holds
// worker-0 healthy.
func (r *Router) startEnvoy(t testing.TB) {
	t.Helper()
	p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: r.bin, Ready: 30 * time.Second, Config: agent.EnvoyConfig{
		RunDir: r.RunDir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: r.Worker}}, ReadinessPort: r.ready.Port(),
	}}, time.Now())
	if err != nil {
		t.Fatalf("StartEnvoy: %v", err)
	}
	r.mu.Lock()
	r.envoy = p
	r.mu.Unlock()
	r.fwd.RouteTo(filepath.Join(r.RunDir, agent.EnvoyListenerSocket))
	r.WaitWorker(t, "healthy", 30*time.Second)
}

// stopEnvoy ends the running Envoy with sig (KILL at once, or TERM with a 5 s grace), waits for its
// process group, and leaves the forwarder routed to no router, as the agent's converge loop does
// while its router is not running: the forwarder then answers 503 itself.
func (r *Router) stopEnvoy(sig syscall.Signal) {
	r.mu.Lock()
	p := r.envoy
	r.envoy = nil
	r.mu.Unlock()
	if p == nil {
		return
	}
	if sig == syscall.SIGKILL {
		p.Group.Kill()
	} else {
		p.Group.Stop(5 * time.Second)
	}
	<-p.Group.Done()
	p.Close()
	r.fwd.RouteTo("")
}

// KillEnvoy kills Envoy's process group, as an OOM kill would, and waits for it.
func (r *Router) KillEnvoy(t testing.TB) {
	t.Helper()
	r.stopEnvoy(syscall.SIGKILL)
}

// RestartEnvoy starts Envoy again, as the agent does when the router rung raises the router's
// generation, and returns once the new Envoy holds worker-0 healthy.
func (r *Router) RestartEnvoy(t testing.TB) {
	t.Helper()
	r.startEnvoy(t)
}

// Bridge serves the agent's serving socket on ln, a TCP listener on loopback, byte for byte, until
// the test ends, and returns ln's address: the front's StaticTransports dials TCP, where production
// dials frps, which carries the bytes to frpc and the serving socket unchanged.
func (r *Router) Bridge(t testing.TB, ln net.Listener) string {
	t.Helper()
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				u, err := net.Dial("unix", r.Serving)
				if err != nil {
					return
				}
				defer u.Close()
				go func() { _, _ = io.Copy(u, c); u.(*net.UnixConn).CloseWrite() }()
				select {
				case <-copyDone(c, u):
				case <-done:
				}
			}()
		}
	}()
	t.Cleanup(func() { close(done); ln.Close(); wg.Wait() })
	return ln.Addr().String()
}

// copyDone copies from to to, and closes the returned channel when it ends.
func copyDone(to io.Writer, from io.Reader) <-chan struct{} {
	ch := make(chan struct{})
	go func() { _, _ = io.Copy(to, from); close(ch) }()
	return ch
}

// WaitWorker waits until Envoy's own /clusters?format=json holds worker-0 in state: "healthy",
// "failed_active" (its active check fails, ejected or not), "ejected" (by outlier detection alone) or
// "pending". It re-reads Envoy's state every
// 100 ms, with a deadline, and returns how long it took.
func (r *Router) WaitWorker(t testing.TB, state string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		got, err := r.workerState()
		if err == nil && got == state {
			return time.Since(start)
		}
		if time.Since(start) > within {
			t.Fatalf("Envoy holds worker-0 %q (%v), not %q, after %v", got, err, state, within)
		}
		time.Sleep(100 * time.Millisecond) // re-reads Envoy's state, an observable one, with a deadline
	}
}

func (r *Router) workerState() (string, error) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", filepath.Join(r.RunDir, agent.EnvoyAdminSocket))
	}}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://admin/clusters?format=json")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Health   struct {
					FailedActive  bool `json:"failed_active_health_check"`
					FailedOutlier bool `json:"failed_outlier_check"`
					Pending       bool `json:"pending_active_hc"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", err
	}
	for _, c := range doc.Clusters {
		for _, h := range c.Hosts {
			if c.Name != "workers" || h.Hostname != "worker-0" {
				continue
			}
			switch { // a failing active check first: a worker can be failing and ejected at once
			case h.Health.FailedActive:
				return "failed_active", nil
			case h.Health.FailedOutlier:
				return "ejected", nil
			case h.Health.Pending:
				return "pending", nil
			}
			return "healthy", nil
		}
	}
	return "", io.ErrUnexpectedEOF
}

// Listener is the router's listener, Envoy's Unix socket, for a test that sends to Envoy directly.
func (r *Router) Listener() string { return filepath.Join(r.RunDir, agent.EnvoyListenerSocket) }

package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// What the router's behaviour tests share (envoy_*_test.go): a router is the real Envoy, with a worker
// double behind it, started either by the agent (StartEnvoy, the config as the agent writes it) or,
// for a control, directly from the agent's rendered config with exactly one field changed or deleted.
//
// A control is the TEST'S OWN, never the agent's: the agent's guard (CheckEnvoyConfig) refuses such a
// config, and StartEnvoy would never run it. A control exists to show that the behaviour its test
// checks can fail: the same measurement, made against Envoy with the one setting the agent changes
// from Envoy's default put back, gives the other answer.

// router is a running Envoy with its worker double.
type router struct {
	dir    string
	worker *testWorker
	ready  *agent.Readiness // the readiness endpoint Envoy health-checks the worker through
	proc   *agent.EnvoyProc // the agent's own start; nil for a control
	raw    *rawEnvoy        // a control's process; nil for the agent's own
}

func (r *router) listener() string { return filepath.Join(r.dir, agent.EnvoyListenerSocket) }
func (r *router) admin() string    { return filepath.Join(r.dir, agent.EnvoyAdminSocket) }

// client returns a client of the router's listener, with no timeout of its own: a test bounds a
// request by its context.
func (r *router) client(t *testing.T) *http.Client {
	t.Helper()
	c := unixClient(r.listener())
	t.Cleanup(func() { c.Transport.(*http.Transport).CloseIdleConnections() })
	return c
}

// agentRouter starts Envoy through the agent (StartEnvoy) and a worker
// double with the spec behind it.
func agentRouter(t *testing.T, spec workerSpec) *router {
	t.Helper()
	return newRouter(t, routerOpts{spec: spec, verified: true, wait: "healthy"})
}

// routerOpts shape newRouter.
type routerOpts struct {
	spec     workerSpec
	verified bool                 // the readiness endpoint holds worker-0 verified
	edit     func(map[string]any) // nil: the agent's own start (StartEnvoy); set: a control of the test's own
	wait     string               // the state to wait for in Envoy's /clusters, "" for none
}

// newRouter starts the worker double, the agent's readiness endpoint and Envoy, in that order, so
// Envoy's first check finds the worker, then waits for worker-0's state in Envoy.
func newRouter(t *testing.T, o routerOpts) *router {
	t.Helper()
	dir := shortDir(t)
	r := &router{dir: dir}
	r.worker = startWorker(t, dir, o.spec)
	r.ready = startReadiness(t, dir, o.verified)
	if o.edit == nil {
		r.proc = startTestEnvoyIn(t, dir, r.ready)
		logTailOnFailure(t, filepath.Join(dir, agent.EnvoyLogFile))
	} else {
		m := renderMap(t, agent.EnvoyConfig{
			RunDir:        dir,
			Workers:       []agent.EnvoyWorker{{Name: "worker-0", Socket: workerSock(dir)}},
			ReadinessPort: r.ready.Port(),
		})
		o.edit(m)
		r.raw = startRawEnvoy(t, dir, mustJSON(t, m))
	}
	if o.wait != "" {
		r.waitWorker(t, o.wait, 30*time.Second)
	}
	return r
}

// controlRouter starts Envoy directly from the agent's config for a router in a fresh directory, after
// edit has changed ONE field of it as a decoded map (the helpers routeOf, hcmOf and clusterOf of
// envoyconf_test.go walk to it). The control is the test's own: see the top of this file.
func controlRouter(t *testing.T, spec workerSpec, edit func(m map[string]any)) *router {
	t.Helper()
	return newRouter(t, routerOpts{spec: spec, verified: true, edit: edit, wait: "healthy"})
}

// rawEnvoy is an Envoy the test started itself, from a config the agent's guard would refuse.
type rawEnvoy struct{ exited chan struct{} }

// Exited is closed when the process has exited.
func (e *rawEnvoy) Exited() <-chan struct{} { return e.exited }

// startRawEnvoy writes raw as dir's control.json (mode 0600) and runs the pinned Envoy on it, as the
// agent does but without the guard: -c <file> --disable-hot-restart --concurrency 2 --log-level warn,
// no environment. It returns once dir's listener and admin sockets accept a connection. Cleanup kills the process
// and waits for it, so none is left behind; a config Envoy refuses fails the test at once, with its
// output.
func startRawEnvoy(t *testing.T, dir string, raw []byte) *rawEnvoy {
	t.Helper()
	bin := testenv.EnvoyBinary(t)
	raiseSoftNoFile(t)
	cfg := filepath.Join(dir, "control.json")
	if err := os.WriteFile(cfg, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "control.log")
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", cfg, "--disable-hot-restart", "--concurrency", "2", "--log-level", "warn")
	cmd.Env = []string{}
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		t.Fatal(err)
	}
	e := &rawEnvoy{exited: make(chan struct{})}
	waited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(e.exited); close(waited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-waited
		logf.Close()
	})
	logTailOnFailure(t, logPath)

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if accepts(filepath.Join(dir, agent.EnvoyListenerSocket)) && accepts(filepath.Join(dir, agent.EnvoyAdminSocket)) {
			return e
		}
		select {
		case <-e.exited:
			out, _ := os.ReadFile(logPath)
			t.Fatalf("Envoy exited before it listened (the control's config was refused?):\n%s", out)
		case <-deadline.C:
			t.Fatalf("Envoy did not create its sockets in %s within 30 s", dir)
		case <-tick.C: // the sockets' existence is the observable state; the tick only re-reads it
		}
	}
}

// accepts reports whether a connection to the Unix socket at path is accepted. The socket file exists
// from Envoy's bind, a moment before its listen, and a connect in between is refused (found by a
// -count=50 run under load, twice in 100 starts), so existence alone does not say it is ready.
func accepts(path string) bool {
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// raiseSoftNoFile raises this test process's soft RLIMIT_NOFILE to its hard limit, which the Envoy a
// test starts directly inherits: a test that holds a thousand connections needs two thousand
// descriptors on each side. StartEnvoy does the same for the agent's own start.
func raiseSoftNoFile(t *testing.T) {
	t.Helper()
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r); err != nil {
		t.Fatal(err)
	}
	if r.Cur < r.Max {
		r.Cur = r.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &r); err != nil {
			t.Fatal(err)
		}
	}
}

// logTailOnFailure logs the end of Envoy's log when the test has failed, to say what Envoy did.
func logTailOnFailure(t *testing.T, path string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return
		}
		if len(b) > 4000 {
			b = b[len(b)-4000:]
		}
		t.Logf("the end of %s:\n%s", filepath.Base(path), b)
	})
}

// getVia sends a GET to the router, bounded by timeout.
func getVia(c *http.Client, path string, timeout time.Duration, header http.Header) (*http.Response, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance"+path, nil)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.Do(req)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("GET %s: %w", path, err)
	}
	return resp, cancel, nil
}

// workerState is worker-0's state in Envoy's own /clusters?format=json: "healthy", "failed_active"
// (its active check fails), "ejected" (outlier detection), or "pending" (not yet checked).
func (r *router) workerState(t *testing.T) string {
	t.Helper()
	c := unixClient(r.admin())
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := c.Get("http://admin/clusters?format=json")
	if err != nil {
		t.Fatalf("Envoy's admin: %v", err)
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
		t.Fatalf("Envoy's /clusters?format=json: %v", err)
	}
	for _, c := range doc.Clusters {
		for _, h := range c.Hosts {
			if c.Name != "workers" || h.Hostname != "worker-0" {
				continue
			}
			switch {
			case h.Health.FailedOutlier:
				return "ejected"
			case h.Health.FailedActive:
				return "failed_active"
			case h.Health.Pending:
				return "pending"
			}
			return "healthy"
		}
	}
	t.Fatalf("Envoy's /clusters names no worker-0: %+v", doc)
	return ""
}

// waitWorker waits until Envoy's /clusters holds worker-0 in state, reading it again every 100 ms (the
// state is Envoy's, which tells no one when it changes), and returns how long that took.
func (r *router) waitWorker(t *testing.T, state string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		got := r.workerState(t)
		if got == state {
			return time.Since(start)
		}
		if time.Since(start) > within {
			t.Fatalf("Envoy holds worker-0 %s, not %s, after %v", got, state, within)
		}
		time.Sleep(100 * time.Millisecond) // re-reads Envoy's state, an observable one, with a deadline
	}
}

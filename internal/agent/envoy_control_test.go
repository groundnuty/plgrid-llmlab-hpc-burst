package agent_test

import (
	"context"
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
	p, dir := startTestEnvoy(t)
	r := &router{dir: dir, proc: p}
	logTailOnFailure(t, filepath.Join(dir, agent.EnvoyLogFile))
	r.worker = startWorker(t, dir, spec)
	return r
}

// controlRouter starts Envoy directly from the agent's config for a router in a fresh directory, after
// edit has changed ONE field of it as a decoded map (the helpers routeOf, hcmOf and clusterOf of
// envoyconf_test.go walk to it). The control is the test's own: see the top of this file.
func controlRouter(t *testing.T, spec workerSpec, edit func(m map[string]any)) *router {
	t.Helper()
	dir := shortDir(t)
	m := renderMap(t, agent.EnvoyConfig{
		RunDir:  dir,
		Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: workerSock(dir)}},
	})
	edit(m)
	r := &router{dir: dir}
	r.raw = startRawEnvoy(t, dir, mustJSON(t, m))
	r.worker = startWorker(t, dir, spec)
	return r
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

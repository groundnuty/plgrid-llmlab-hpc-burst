package lifecycle

// The health and metrics endpoints (endpoints.go), on listeners the test holds:
// the paths controller-runtime's own servers answer, and the ownership of the
// listeners NewManager is given. No API server: nothing here reads one.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// serve runs srv on its listener until the test ends and returns the base URL.
func serve(t *testing.T, srv *manager.Server) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the server returned %v after its context ended, want nil", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the server did not stop within 30 s of its context's end")
		}
	})
	return "http://" + srv.Listener.Addr().String()
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func statusOf(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// probeServer answers the paths controller-runtime's health probe server does
// (pkg/manager/internal.go, addHealthProbeServer; pkg/healthz): a probe's own
// path and each check's sub-path, 404 for a check that does not exist, 500 for
// one that fails. The expected statuses are healthz.Handler's documented ones,
// not read back from probeServer.
func TestProbeServerAnswersTheControllerRuntimePaths(t *testing.T) {
	live := &healthz.Handler{Checks: map[string]healthz.Checker{"ping": healthz.Ping}}
	ready := &healthz.Handler{Checks: map[string]healthz.Checker{
		"passes": func(*http.Request) error { return nil },
		"fails":  func(*http.Request) error { return errors.New("example: not ready") },
	}}
	base := serve(t, probeServer(listenLoopback(t), live, ready))
	for path, want := range map[string]int{
		"/healthz":         http.StatusOK,
		"/healthz/":        http.StatusOK,
		"/healthz/ping":    http.StatusOK,
		"/healthz/missing": http.StatusNotFound,
		"/readyz":          http.StatusInternalServerError, // one check fails
		"/readyz/passes":   http.StatusOK,
		"/readyz/fails":    http.StatusInternalServerError,
		"/readyz/missing":  http.StatusNotFound,
		"/metrics":         http.StatusNotFound, // not this server's
		"/":                http.StatusNotFound,
	} {
		if got := statusOf(t, base+path); got != want {
			t.Errorf("GET %s: %d, want %d", path, got, want)
		}
	}
}

// metricsServer serves controller-runtime's registry at /metrics and nothing
// else.
func TestMetricsServerServesTheRegistry(t *testing.T) {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "hpcburst_endpoints_test_gauge", Help: "example"})
	g.Set(7)
	if err := ctrlmetrics.Registry.Register(g); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ctrlmetrics.Registry.Unregister(g) })
	base := serve(t, metricsServer(listenLoopback(t)))
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !containsLine(string(body), "hpcburst_endpoints_test_gauge 7") {
		t.Errorf("GET /metrics: %d %q, want 200 with the registered gauge", resp.StatusCode, body)
	}
	for _, path := range []string{"/", "/healthz", "/metrics/x"} {
		if got := statusOf(t, base+path); got != http.StatusNotFound {
			t.Errorf("GET %s: %d, want 404", path, got)
		}
	}
}

func containsLine(body, line string) bool {
	for start := 0; start < len(body); {
		end := start
		for end < len(body) && body[end] != '\n' {
			end++
		}
		if body[start:end] == line {
			return true
		}
		start = end + 1
	}
	return false
}

// closeRecorder is a listener that records its Close.
type closeRecorder struct {
	net.Listener
	mu     sync.Mutex
	closed bool
}

func (c *closeRecorder) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.Listener.Close()
}

func (c *closeRecorder) wasClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func exampleOptions() Options {
	return Options{Config: &rest.Config{Host: "http://127.0.0.1:1"}, CredentialsDir: "/nonexistent/example", HealthAddr: "127.0.0.1:1111", MetricsAddr: "127.0.0.1:2222"}
}

// NewManager binds the health address, then the metrics address, through
// Options.Listen, and keeps both open for the manager's servers.
func TestNewManagerBindsThroughListen(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	lns := map[string]*closeRecorder{}
	o := exampleOptions()
	o.Listen = func(addr string) (net.Listener, error) {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, addr)
		lns[addr] = &closeRecorder{Listener: listenLoopback(t)}
		return lns[addr], nil
	}
	if _, _, err := NewManager(o); err != nil {
		t.Fatal(err)
	}
	for _, ln := range lns {
		t.Cleanup(func() { ln.Listener.Close() }) // the manager never started, so nothing else does
	}
	if len(asked) != 2 || asked[0] != o.HealthAddr || asked[1] != o.MetricsAddr {
		t.Fatalf("Listen was asked for %v, want the health address %s, then the metrics address %s", asked, o.HealthAddr, o.MetricsAddr)
	}
	for addr, ln := range lns {
		if ln.wasClosed() {
			t.Errorf("the listener for %s was closed by a NewManager that succeeded", addr)
		}
	}
}

// A listener NewManager bound is closed when it then fails: no Start will serve
// it, and a caller that holds the port would otherwise see it accept for good.
func TestNewManagerClosesWhatItBoundWhenItFails(t *testing.T) {
	cause := errors.New("example: the metrics address is taken")
	var health *closeRecorder
	o := exampleOptions()
	o.Listen = func(addr string) (net.Listener, error) {
		if addr == o.MetricsAddr {
			return nil, cause
		}
		health = &closeRecorder{Listener: listenLoopback(t)}
		return health, nil
	}
	_, _, err := NewManager(o)
	if !errors.Is(err, cause) {
		t.Fatalf("NewManager returned %v, want an error wrapping %v", err, cause)
	}
	if health == nil || !health.wasClosed() {
		t.Error("the health listener was not closed after the metrics bind failed")
	}

	// A failure after both binds: controller-runtime refuses the API config.
	var bound []*closeRecorder
	o = exampleOptions()
	o.Config = &rest.Config{Host: "http://[::1"}
	o.Listen = func(string) (net.Listener, error) {
		ln := &closeRecorder{Listener: listenLoopback(t)}
		bound = append(bound, ln)
		return ln, nil
	}
	if _, _, err := NewManager(o); err == nil {
		t.Fatal("NewManager with an unusable API config succeeded")
	}
	if len(bound) != 2 {
		t.Fatalf("NewManager bound %d listeners before the config was refused, want 2 (the test needs a failure after both binds)", len(bound))
	}
	for i, ln := range bound {
		if !ln.wasClosed() {
			t.Errorf("listener %d was not closed after NewManager failed", i)
		}
	}
}

package scenario_test

// The controller double of part3_firstread_test.go (issue #321, PR #334): what
// a serving lifecycle controller exports on /metrics
// (process_start_time_seconds, both hpcburst_collector_success series, the
// raised-events counter's series of part 3's forbidden events, created at 0)
// and frps's proxy list, at the rig's ports, from state a test changes.

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// firstReadPorts: issue #321's block is 29940-29959. The controller double
// binds base+11 (frps's proxy list, 29951) and base+12 (the lifecycle
// controller's /metrics, 29952), where the rig reads them (stack.Ports.URL).
var firstReadPorts = stack.Ports{Base: 29940}

// controllerDouble serves, at the rig's ports, the serving lifecycle
// controller's /metrics and frps's proxy list, from state a test changes: the
// process's start time, its counts of part 3's forbidden events, and whether
// /metrics fails.
type controllerDouble struct {
	mu     sync.Mutex
	start  float64
	counts map[eventcount.Key]float64
	fail   bool
	online []string // the proxies frps lists online, by Instance UID
}

// startControllerDouble serves the double for p3's rig, with each primary's
// proxy online.
func startControllerDouble(t *testing.T, p3 *part3) *controllerDouble {
	t.Helper()
	d := &controllerDouble{start: 1_700_000_000, counts: map[eventcount.Key]float64{}}
	for _, dr := range p3.deps {
		d.online = append(d.online, string(dr.primary.UID))
	}
	state := t.TempDir()
	const pw = "example-web-password"
	if err := os.MkdirAll(filepath.Join(state, "frps"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "frps", "web-password"), []byte(pw+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	serveAt(t, firstReadPorts.Base+11, func(w http.ResponseWriter, req *http.Request) {
		if u, p, ok := req.BasicAuth(); !ok || u != "hpcburst" || p != pw || req.URL.Path != "/api/proxy/tcpmux" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		type proxy struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		}
		var list struct {
			Proxies []proxy `json:"proxies"`
		}
		d.mu.Lock()
		for _, uid := range d.online {
			list.Proxies = append(list.Proxies, proxy{Name: uid, Status: "online"})
		}
		d.mu.Unlock()
		_ = json.NewEncoder(w).Encode(list)
	})
	serveAt(t, firstReadPorts.Base+12, d.metrics)
	p3.ports = firstReadPorts
	p3.h = &stack.Host{State: state}
	return d
}

// serveAt serves h on 127.0.0.1:port until the test ends.
func serveAt(t *testing.T, port int, h http.HandlerFunc) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("listen on issue #321's port %d: %v", port, err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}

// metrics is a serving lifecycle controller's /metrics, in Prometheus's text
// format, or a 503 while the double fails.
func (d *controllerDouble) metrics(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE %s gauge\n%s %g\n", eventcount.ProcessStartName, eventcount.ProcessStartName, d.start)
	fmt.Fprintf(&b, "# TYPE %s gauge\n", eventcount.CollectorSuccessName)
	for _, c := range eventcount.LifecycleCollectors {
		fmt.Fprintf(&b, "%s{collector=%q} 1\n", eventcount.CollectorSuccessName, c)
	}
	fmt.Fprintf(&b, "# TYPE %s counter\n", eventcount.Name)
	for _, k := range part3Forbidden {
		fmt.Fprintf(&b, "%s %g\n", k, d.counts[k])
	}
	_, _ = w.Write([]byte(b.String()))
}

// raise is the serving process handing one event of k to its recorder.
func (d *controllerDouble) raise(k eventcount.Key) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[k]++
}

// restart is the kill and the supervisor's restart: a new process, whose
// series are created at 0. What the old one counted is gone.
func (d *controllerDouble) restart() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.start++
	d.counts = map[eventcount.Key]float64{}
}

func (d *controllerDouble) setFail(fail bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = fail
}

// processStart is the serving process's start time.
func (d *controllerDouble) processStart() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.start
}

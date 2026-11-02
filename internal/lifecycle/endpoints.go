package lifecycle

// The health and metrics endpoints, served on listeners the caller can hand
// over (issue #219).
//
// controller-runtime v0.23.1 binds both of its own endpoints by address: the
// health probes with net.Listen (manager.go, newHealthProbeListener, which is
// unexported) and the metrics server with a net.ListenConfig
// (metrics/server/server.go, createListener). A test that picks a free port,
// releases it and lets the binary bind it later loses the race to any
// process that takes the port in between (TestControllerBinaryStopDuringSubmission,
// 2 Nov 2026). So the manager is built with both of its servers off, and the
// same paths are served here by controller-runtime's own manager.Server, whose
// Listener field exists "when the port reservation needs to happen in advance
// of this runnable starting". Production and tests take this one path; only
// Options.Listen differs.

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// healthzPath and readyzPath are controller-runtime's default probe paths,
	// and metricsPath its metrics path.
	healthzPath = "/healthz"
	readyzPath  = "/readyz"
	metricsPath = "/metrics"

	// metricsShutdownTimeout is controller-runtime's metrics server's.
	metricsShutdownTimeout = time.Minute
)

// listenTCP is Options.Listen's default: the address on TCP, as
// controller-runtime binds its own.
func listenTCP(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }

// listenOptional binds addr with listen. "" and "0" bind nothing and return a
// nil listener, as controller-runtime reads them.
func listenOptional(listen func(string) (net.Listener, error), what, addr string) (net.Listener, error) {
	if addr == "" || addr == "0" {
		return nil, nil
	}
	ln, err := listen(addr)
	if err != nil {
		return nil, fmt.Errorf("error listening on %s for the %s: %w", addr, what, err)
	}
	return ln, nil
}

// closeListener closes ln if there is one; the error of a listener that is
// already closed is not actionable.
func closeListener(ln net.Listener) {
	if ln != nil {
		_ = ln.Close()
	}
}

// newServer is controller-runtime's httpserver.New: an http.Server with its
// defaults, which that package keeps internal.
func newServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		MaxHeaderBytes:    1 << 20,
		IdleTimeout:       90 * time.Second,
		ReadHeaderTimeout: 32 * time.Second,
	}
}

// probeServer serves /healthz and /readyz, and their sub-paths (/readyz/<check>),
// from the two handlers on ln, as controller-runtime's health probe server does
// (pkg/manager/internal.go, addHealthProbeServer). The manager starts it in its
// HTTP servers' group, before any cache, like its own.
func probeServer(ln net.Listener, liveness, readiness *healthz.Handler) *manager.Server {
	mux := http.NewServeMux()
	for path, h := range map[string]*healthz.Handler{readyzPath: readiness, healthzPath: liveness} {
		mux.Handle(path, http.StripPrefix(path, h))
		mux.Handle(path+"/", http.StripPrefix(path, h))
	}
	return &manager.Server{Name: "health probe", Server: newServer(mux), Listener: ln}
}

// metricsServer serves /metrics on ln from controller-runtime's registry, the
// one its own metrics server serves (pkg/metrics/server/server.go, Start).
func metricsServer(ln net.Listener) *manager.Server {
	mux := http.NewServeMux()
	mux.Handle(metricsPath, promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError}))
	timeout := metricsShutdownTimeout
	return &manager.Server{Name: "metrics", Server: newServer(mux), Listener: ln, ShutdownTimeout: &timeout}
}

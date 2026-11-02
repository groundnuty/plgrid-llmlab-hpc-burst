package main

// awaitServing (run_test.go, child_test.go) tells three ends apart: the endpoints
// answered, the process returned, and the time ran out. A timeout read as success
// would let a test pass without having seen the metrics serve (review of PR 233:
// N1), so each end has its test, with the endpoints on ports the test holds.

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// probeFlags are flags whose health address and metrics address are the two
// listeners given, which the caller serves (or not).
func probeFlags(t *testing.T, health, metrics net.Listener) flags {
	t.Helper()
	base := metrics.Addr().(*net.TCPAddr).Port - agentapi.ControllerMetricsPortOffset
	return flags{host: agentapi.Host{StateDir: t.TempDir(), PortBase: base}, healthAddr: health.Addr().String()}
}

func loopbackListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

// serveOK serves 200 on every path of ln until the test ends.
func serveOK(t *testing.T, ln net.Listener) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
}

func TestAwaitServingTellsTheThreeEndsApart(t *testing.T) {
	t.Run("both endpoints answer 200: served", func(t *testing.T) {
		health, metrics := loopbackListener(t), loopbackListener(t)
		serveOK(t, health)
		serveOK(t, metrics)
		b := &binary{f: probeFlags(t, health, metrics), exited: make(chan struct{})}
		if served, err := b.awaitServing(10 * time.Second); !served || err != nil {
			t.Fatalf("awaitServing = %v, %v, want served", served, err)
		}
	})

	t.Run("only the health probes answer: a timeout, not served", func(t *testing.T) {
		// The metrics port accepts into its backlog and never answers, as a stranger's does.
		health, metrics := loopbackListener(t), loopbackListener(t)
		serveOK(t, health)
		t.Cleanup(func() { metrics.Close() })
		b := &binary{f: probeFlags(t, health, metrics), exited: make(chan struct{})}
		served, err := b.awaitServing(500 * time.Millisecond)
		if served || !errors.Is(err, errNotServing) {
			t.Fatalf("awaitServing = %v, %v, want not served and errNotServing", served, err)
		}
	})

	t.Run("the run returned: its error, not served", func(t *testing.T) {
		health, metrics := loopbackListener(t), loopbackListener(t)
		t.Cleanup(func() { health.Close(); metrics.Close() })
		want := errors.New("example: bind: address already in use")
		b := &binary{f: probeFlags(t, health, metrics), exited: make(chan struct{}), err: want}
		close(b.exited)
		served, err := b.awaitServing(10 * time.Second)
		if served || !errors.Is(err, want) {
			t.Fatalf("awaitServing = %v, %v, want not served and the run's error", served, err)
		}
	})

	t.Run("a child: served, exited, and neither", func(t *testing.T) {
		health, metrics := loopbackListener(t), loopbackListener(t)
		serveOK(t, health)
		serveOK(t, metrics)
		c := &child{f: probeFlags(t, health, metrics), exited: make(chan struct{})}
		if served, exited := c.awaitServing(10 * time.Second); !served || exited {
			t.Errorf("a child whose endpoints answer: served %v, exited %v", served, exited)
		}

		silent := loopbackListener(t)
		t.Cleanup(func() { silent.Close() })
		c = &child{f: probeFlags(t, health, silent), exited: make(chan struct{})}
		if served, exited := c.awaitServing(500 * time.Millisecond); served || exited {
			t.Errorf("a child whose metrics never answer: served %v, exited %v, want neither (a timeout)", served, exited)
		}

		c = &child{f: probeFlags(t, health, silent), exited: make(chan struct{})}
		close(c.exited)
		if served, exited := c.awaitServing(10 * time.Second); served || !exited {
			t.Errorf("a child that exited: served %v, exited %v, want exited", served, exited)
		}
	})
}

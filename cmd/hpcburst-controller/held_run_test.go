package main

// Issue #219, deterministically: another process wants a port of the binary's
// between the test's pick and the binary's bind, and the start still succeeds.
//
// The stranger acts at the one moment that matters, inside deps.listen, which
// the binary calls when it is about to bind an address: it tries to bind that
// address first, as a process that took the port between the pick and the bind
// would have, and then the held listener goes to the binary. Waiting for a
// flake to show, or running the old tests more often, would prove nothing: the
// old picks passed whenever no stranger came.

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// stranger is a process that wants every port the binary binds: listen is a
// deps.listen that tries to bind each address first and records what came of it.
type stranger struct {
	mu    sync.Mutex
	tried map[string]error                        // the address -> the stranger's bind error (nil: it got the port)
	next  func(addr string) (net.Listener, error) // what the binary binds after the stranger's try
}

func newStranger(next func(addr string) (net.Listener, error)) *stranger {
	return &stranger{tried: map[string]error{}, next: next}
}

func (s *stranger) listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		_ = ln.Close() // it got the port; the binary's bind below shows what that does
	}
	s.mu.Lock()
	s.tried[addr] = err
	s.mu.Unlock()
	return s.next(addr)
}

func (s *stranger) result(addr string) (tried bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err, tried = s.tried[addr]
	return tried, err
}

func TestControllerBinaryHoldsItsPortsAgainstAStranger(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	if err := api.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}); err != nil {
		t.Fatal(err)
	}

	t.Run("the start succeeds although a stranger tried every port first", func(t *testing.T) {
		f, cert, _ := prepareState(t, api, "direct", exampleFront)
		addrs := binaryAddrs(f)
		for _, addr := range addrs {
			// Between the pick and the bind: the stranger finds each port held.
			ln, err := net.Listen("tcp", addr)
			if err == nil {
				_ = ln.Close()
				t.Fatalf("a stranger bound %s although the test picked it: the pick does not hold the port", addr)
			}
			if !isAddrInUse(err) {
				t.Fatalf("a stranger's bind of the picked %s failed with %v, want address already in use", addr, err)
			}
		}
		st := newStranger(heldAddrs.take)
		b := startBinary(t, f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry(), listen: st.listen})

		// Each endpoint answers from the binary, not from a socket that merely accepts into a
		// backlog: the health probes and the metrics with a status, the plugin with an HTTP
		// response, the agent endpoint with a TLS handshake to the launcher's certificate.
		get := func(url string) (int, bool) {
			resp, err := http.Get(url)
			if err != nil {
				return 0, false
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			return resp.StatusCode, true
		}
		b.eventually(t, "/healthz answers 200 on the health address", 60*time.Second, func() bool {
			code, ok := get("http://" + f.healthAddr + "/healthz")
			return ok && code == http.StatusOK
		})
		b.eventually(t, "/metrics answers 200 on the metrics address", 60*time.Second, func() bool {
			code, ok := get("http://" + f.host.ControllerMetricsAddr() + "/metrics")
			return ok && code == http.StatusOK
		})
		b.eventually(t, "the plugin endpoint answers an HTTP request", 60*time.Second, func() bool {
			_, ok := get("http://" + f.host.FrpsPluginAddr() + agentapi.FrpsPluginPath)
			return ok
		})
		b.eventually(t, "the agent endpoint completes a TLS handshake pinned to the launcher's certificate", 60*time.Second, func() bool {
			c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", f.host.AgentEndpointAddr(), agentapi.ClientTLSConfig(agentapi.PinOf(cert)))
			if err != nil {
				return false
			}
			_ = c.Close()
			return true
		})

		// All four binds went through the hook, each after the stranger's failed try.
		for _, addr := range addrs {
			tried, err := st.result(addr)
			switch {
			case !tried:
				t.Errorf("the binary never asked deps.listen for %s: it bound the address some other way", addr)
			case !isAddrInUse(err):
				t.Errorf("the stranger's bind of %s returned %v, want address already in use", addr, err)
			}
			if !heldAddrs.wasTaken(addr) {
				t.Errorf("the binary did not take the listener held for %s", addr)
			}
		}
		if err := b.stop(t); err != nil {
			t.Fatalf("run returned %v after its context ended, want nil", err)
		}
		for _, addr := range addrs {
			if listening(addr) {
				t.Errorf("%s still accepts after the stop", addr)
			}
		}
	})

	// The control, which shows the test can see the failure this change fixes: a port released
	// before the binary binds it, taken by a stranger meanwhile, is "address already in use", and
	// run returns it. This is the old freePortBase, with the stranger placed deterministically.
	// The plugin's port, because that is the bind that failed in CI.
	t.Run("a port released before the bind and taken by a stranger fails the start", func(t *testing.T) {
		f, _, _ := prepareState(t, api, "direct", exampleFront)
		plugin := f.host.FrpsPluginAddr()
		heldAddrs.drop(plugin) // the old pick: released, with the number handed to the binary
		stolen, err := net.Listen("tcp", plugin)
		if err != nil {
			t.Fatalf("the stranger could not take the released %s: %v", plugin, err)
		}
		t.Cleanup(func() { _ = stolen.Close() })
		st := newStranger(func(addr string) (net.Listener, error) {
			if addr == plugin {
				return net.Listen("tcp", addr) // the binary binds the address, as before
			}
			return heldAddrs.take(addr)
		})
		b := startBinary(t, f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry(), listen: st.listen})
		select {
		case <-b.exited:
		case <-time.After(90 * time.Second):
			t.Fatal("run did not fail within 90 s although the plugin's port was taken")
		}
		if !isAddrInUse(b.err) || !strings.Contains(b.err.Error(), "bind the plugin endpoint "+plugin) {
			t.Fatalf("run returned %v, want its bind of the plugin endpoint %s to fail with address already in use", b.err, plugin)
		}
	})
}

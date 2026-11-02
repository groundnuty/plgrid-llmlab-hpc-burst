package main

// Issue #219, deterministically: another process wants a port of the binary's
// between the test's pick and the binary's bind, and the start still succeeds.
//
// For the ports the binary's own servers bind (the agent endpoint and frps's
// plugin), the stranger acts at the one moment that matters, inside deps.listen,
// which the binary calls when it is about to bind an address: it tries to bind
// that address first, as a process that took the port between the pick and the
// bind would have, and then the held listener goes to the binary. For the metrics
// and the health probes, which controller-runtime binds from an address, the
// stranger takes the port after the test released it and before the run starts
// (beforeLaunch), and the start is repeated on fresh ports. Waiting for a flake to
// show, or running the old tests more often, would prove nothing: the old picks
// passed whenever no stranger came.

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

// stranger is a process that wants every port the binary binds through deps.listen:
// listen tries to bind each address first and records what came of it.
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

// statusOf is the status of a GET, ok false when it got no answer.
func statusOf(url string) (code int, ok bool) {
	resp, err := probeHTTP.Get(url)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, true
}

func TestControllerBinaryHoldsItsPortsAgainstAStranger(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	if err := api.client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}); err != nil {
		t.Fatal(err)
	}

	t.Run("the start succeeds although a stranger tried the agent endpoint and the plugin first", func(t *testing.T) {
		holdsOwnPorts(t, api, false)
	})

	// The same, with an incidental collision on a by-address port at the first start: the start is
	// repeated on fresh ports and the test still passes, because it checks the run that served, on
	// the ports it ended with. A test that required no repeat would fail on exactly the flake this
	// change recovers from.
	t.Run("the same when another process takes the metrics port at the first start", func(t *testing.T) {
		holdsOwnPorts(t, api, true)
	})

	// The metrics and the health probes are bound from an address by controller-runtime. A
	// stranger takes the port in the one window left, after the test released it and before the
	// run starts: the start fails with "address already in use", and is repeated on fresh ports.
	for _, tc := range []struct {
		name string
		addr func(flags) string
	}{
		{"the health port", func(f flags) string { return f.healthAddr }},
		{"the metrics port", func(f flags) string { return f.host.ControllerMetricsAddr() }},
	} {
		t.Run("a stranger takes "+tc.name+" before the first start and the start is repeated", func(t *testing.T) {
			f, _, _ := prepareState(t, api, "direct", exampleFront)
			first := f
			var taken []net.Listener
			beforeLaunch = func(t *testing.T, attempt int, f flags) {
				if attempt != 0 {
					return
				}
				ln, err := net.Listen("tcp", tc.addr(f))
				if err != nil {
					t.Fatalf("the stranger could not take the released %s: %v", tc.addr(f), err)
				}
				taken = append(taken, ln)
			}
			t.Cleanup(func() {
				beforeLaunch = nil
				for _, ln := range taken {
					_ = ln.Close()
				}
			})
			b := startBinary(t, &f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry()})
			if b.retries < 1 {
				t.Fatalf("the injected collision did not repeat the start (run returned %v)", b.err)
			}
			if f.host.PortBase == first.host.PortBase || f.healthAddr == first.healthAddr {
				t.Errorf("the retry kept the ports of the first start (base %d, health %s)", f.host.PortBase, f.healthAddr)
			}
			if code, ok := statusOf("http://" + f.healthAddr + "/healthz"); !ok || code != http.StatusOK {
				t.Errorf("/healthz on the retry's health address %s: %d, %v", f.healthAddr, code, ok)
			}
			if code, ok := statusOf("http://" + f.host.ControllerMetricsAddr() + "/metrics"); !ok || code != http.StatusOK {
				t.Errorf("/metrics on the retry's metrics address %s: %d, %v", f.host.ControllerMetricsAddr(), code, ok)
			}
			if err := b.stop(t); err != nil {
				t.Fatalf("run returned %v after its context ended, want nil", err)
			}
		})
	}

	// The bound: a stranger on the port at every start leaves the test with the binary's own
	// error after maxBindRetries repeats, not a loop.
	t.Run("a stranger on the metrics port at every start ends the repeats at the bound", func(t *testing.T) {
		f, _, _ := prepareState(t, api, "direct", exampleFront)
		var taken []net.Listener
		var attempts []int
		beforeLaunch = func(t *testing.T, attempt int, f flags) {
			ln, err := net.Listen("tcp", f.host.ControllerMetricsAddr())
			if err != nil {
				t.Fatalf("the stranger could not take the released %s: %v", f.host.ControllerMetricsAddr(), err)
			}
			taken = append(taken, ln)
			attempts = append(attempts, attempt)
		}
		t.Cleanup(func() {
			beforeLaunch = nil
			for _, ln := range taken {
				_ = ln.Close()
			}
		})
		b := startBinary(t, &f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry()})
		if b.retries != maxBindRetries || len(attempts) != maxBindRetries+1 {
			t.Fatalf("the start was repeated %d times over attempts %v, want %d repeats (%d starts)", b.retries, attempts, maxBindRetries, maxBindRetries+1)
		}
		select {
		case <-b.exited:
		case <-time.After(30 * time.Second):
			t.Fatal("the last start neither returned nor served")
		}
		if !isAddrInUse(b.err) {
			t.Errorf("the last start returned %v, want its address-already-in-use error", b.err)
		}
	})

	// The control, which shows the test can see the failure this change fixes: a port released
	// before the binary binds it, taken by a stranger meanwhile, is "address already in use", and
	// run returns it. This is the old freePortBase, with the stranger placed deterministically.
	// The plugin's port, because that is the bind that failed in CI. launchBinary does not repeat
	// the start.
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
		b := launchBinary(t, f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry(), listen: st.listen})
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

// holdsOwnPorts is the positive test: a stranger tries the agent endpoint and the plugin at the moment
// the binary is about to bind them, and the start succeeds. With collide, another process also takes
// the metrics port at the first start, which the bounded retry recovers from (startBinary); the checks
// are of the final run, on the final ports, whether or not a collision, the injected one or an
// incidental one, made the start repeat.
func holdsOwnPorts(t *testing.T, api *apiServer, collide bool) {
	f, cert, _ := prepareState(t, api, "direct", exampleFront)
	for _, addr := range ownAddrs(f) {
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
	if collide {
		var taken []net.Listener
		beforeLaunch = func(t *testing.T, attempt int, f flags) {
			if attempt != 0 {
				return
			}
			ln, err := net.Listen("tcp", f.host.ControllerMetricsAddr())
			if err != nil {
				t.Fatalf("the other process could not take the released %s: %v", f.host.ControllerMetricsAddr(), err)
			}
			taken = append(taken, ln)
		}
		t.Cleanup(func() {
			beforeLaunch = nil
			for _, ln := range taken {
				_ = ln.Close()
			}
		})
	}
	st := newStranger(heldAddrs.take)
	b := startBinary(t, &f, deps{providerScheme: providerScheme, registry: prometheus.NewRegistry(), listen: st.listen})
	if collide && b.retries < 1 {
		t.Fatalf("the injected collision did not repeat the start (run returned %v)", b.err)
	}
	if b.retries > 0 {
		t.Logf("the start was repeated %d times; checking the run that served, on its own ports", b.retries)
	}
	// The ports the binary ended with: f was rewritten by each repeat.
	own := ownAddrs(f)

	// Each endpoint answers from the binary, not from a socket that merely accepts into a
	// backlog: the plugin with an HTTP response, the agent endpoint with a TLS handshake to the
	// launcher's certificate (startBinary has seen the health probes and the metrics answer).
	b.eventually(t, "the plugin endpoint answers an HTTP request", 60*time.Second, func() bool {
		_, ok := statusOf("http://" + f.host.FrpsPluginAddr() + agentapi.FrpsPluginPath)
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

	// Both binds of the final run went through the hook, each after the stranger's failed try.
	for _, addr := range own {
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
	for _, addr := range own {
		if listening(addr) {
			t.Errorf("%s still accepts after the stop", addr)
		}
	}
}

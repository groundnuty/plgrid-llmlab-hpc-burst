package main

// The ports the tests pick for a copy of the binary, held until it binds them
// (issue #219).
//
// A test used to take a port from a ":0" listener, close it and hand the number
// to the binary, which bound it later. Between the close and the bind any
// process on the machine could take the port, and the binary's own client
// connections to the API server do: a client socket's local port conflicts with
// a bind of the same number, so the bind failed with "address already in use"
// (TestControllerBinaryStopDuringSubmission, 2 Nov 2026, CI run 41378979337).
// Now the test keeps the listener it picked open and gives that listener to the
// binary: an in-process run through deps.listen (startBinary), a controller
// process through an inherited descriptor (inheritListeners). No port the
// binary binds is ever released before it binds it.
//
// The residual: frps, a real process of the tunnel tests, binds its three
// ports itself and cannot take a listener, so those are held until just before
// it starts (releaseForFrps), and a restart of a copy rebinds the addresses
// its previous run closed.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

type heldPort struct {
	ln    net.Listener
	taken bool // given to a binary, which owns it now
}

type heldPorts struct {
	mu sync.Mutex
	m  map[string]*heldPort
}

// heldAddrs is every port the tests of this package hold, by address. Each address
// is the loopback "host:port" the binary binds it by.
var heldAddrs = &heldPorts{m: map[string]*heldPort{}}

// hold keeps ln, which the test picked, until a binary takes it or the test
// ends, and returns its address. The test's end closes it either way.
func (h *heldPorts) hold(t testing.TB, ln net.Listener) string {
	t.Helper()
	addr := ln.Addr().String()
	h.mu.Lock()
	h.m[addr] = &heldPort{ln: ln}
	h.mu.Unlock()
	t.Cleanup(func() {
		_ = ln.Close()
		h.mu.Lock()
		defer h.mu.Unlock()
		if e := h.m[addr]; e != nil && e.ln == ln {
			delete(h.m, addr)
		}
	})
	return addr
}

// take gives a binary the listener held for addr, once. An address the test
// does not hold, or a second take of one, is an error: it is deps.listen of
// every in-process run, so a bind that did not go through the hook, or one the
// test did not pick, fails the run instead of silently binding fresh.
func (h *heldPorts) take(addr string) (net.Listener, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.m[addr]
	switch {
	case e == nil:
		return nil, fmt.Errorf("the test holds no listener for %s: the binary listens on an address the test did not pick", addr)
	case e.taken:
		return nil, fmt.Errorf("the listener held for %s was taken already", addr)
	}
	e.taken = true
	return e.ln, nil
}

// wasTaken reports whether a binary took the listener held for addr, which is
// to say that it has bound the address.
func (h *heldPorts) wasTaken(addr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.m[addr]
	return e != nil && e.taken
}

// drop closes and forgets the listener held for addr, if there is one.
func (h *heldPorts) drop(addr string) {
	h.mu.Lock()
	e := h.m[addr]
	delete(h.m, addr)
	h.mu.Unlock()
	if e != nil {
		_ = e.ln.Close()
	}
}

// rehold binds addr again, for the next run of a copy whose previous run took
// it and closed it. An address the test holds, or does not know, is left as it
// is.
func (h *heldPorts) rehold(t testing.TB, addr string) {
	t.Helper()
	h.mu.Lock()
	e := h.m[addr]
	h.mu.Unlock()
	if e == nil || !e.taken {
		return
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("bind %s again for the next run of the copy that closed it: %v", addr, err)
	}
	h.hold(t, ln)
}

// holdLoopback binds a port of the kernel's choice on loopback and holds it.
func holdLoopback(t testing.TB) (net.Listener, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln, heldAddrs.hold(t, ln)
}

// binaryAddrs are the four addresses a copy of the binary binds: the agent
// endpoint, frps's plugin, the metrics and the health probes.
func binaryAddrs(f flags) []string {
	return []string{f.host.AgentEndpointAddr(), f.host.FrpsPluginAddr(), f.host.ControllerMetricsAddr(), f.healthAddr}
}

// holdBlock picks a port base whose block is free and holds it: the agent
// endpoint (+3, which the kernel chooses), the offsets the binary binds, and the
// offsets another process of the test binds (frps's). It returns the base. The
// binary's are taken by startBinary or startChild; the others by
// releaseForFrps. A base some offset of which is taken is not used.
func holdBlock(t testing.TB, binary, other []int) int {
	t.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base := ln.Addr().(*net.TCPAddr).Port - agentapi.AgentEndpointPortOffset
		lns := []net.Listener{ln}
		ok := base >= 1024 && base+agentapi.ControllerMetricsPortOffset <= 65535
		for _, off := range append(append([]int{}, binary...), other...) {
			if !ok {
				break
			}
			l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(base+off)))
			if err != nil {
				ok = false
				break
			}
			lns = append(lns, l)
		}
		if !ok {
			for _, l := range lns {
				_ = l.Close()
			}
			continue
		}
		for _, l := range lns {
			heldAddrs.hold(t, l)
		}
		return base
	}
	t.Fatal("found no port base with a free block in 100 tries")
	return 0
}

// dropBlock releases a base holdBlock picked and the test does not use.
func dropBlock(base int, offsets ...int) {
	for _, off := range append([]int{agentapi.AgentEndpointPortOffset}, offsets...) {
		heldAddrs.drop(net.JoinHostPort("127.0.0.1", strconv.Itoa(base+off)))
	}
}

// releaseForFrps releases the three ports frps binds, held for it since
// freeTunnelPortBase, as late as the test can: frps binds them itself.
func releaseForFrps(host agentapi.Host) {
	for _, off := range []int{agentapi.FrpsBindPortOffset, agentapi.FrpsTCPMuxPortOffset, agentapi.FrpsWebPortOffset} {
		heldAddrs.drop(net.JoinHostPort("127.0.0.1", strconv.Itoa(host.PortBase+off)))
	}
}

// childEnvListeners names, for a controller process (child_test.go), the
// listeners it inherits: "addr=fd" pairs, comma-separated. It is read before
// run, and the listeners are what the child's deps.listen answers with.
const childEnvListeners = "HPCBURST_TEST_CONTROLLER_LISTENERS"

// inheritListeners takes the four listeners held for f and returns them as the
// descriptors of ExtraFiles (3, 4, ...) with the value of childEnvListeners,
// and a function that closes the parent's copies, to call once the child has
// started: the child's are the only ones left then, so a stopped child frees
// its ports.
func inheritListeners(t testing.TB, f flags) (files []*os.File, env string, closeParent func()) {
	t.Helper()
	var lns []net.Listener
	var pairs []string
	closeParent = func() {
		for _, file := range files {
			_ = file.Close()
		}
		for _, ln := range lns {
			_ = ln.Close()
		}
	}
	for i, addr := range binaryAddrs(f) {
		ln, err := heldAddrs.take(addr)
		if err != nil {
			closeParent()
			t.Fatal(err)
		}
		lns = append(lns, ln)
		file, err := ln.(*net.TCPListener).File()
		if err != nil {
			closeParent()
			t.Fatalf("the descriptor of the listener held for %s: %v", addr, err)
		}
		files = append(files, file)
		pairs = append(pairs, addr+"="+strconv.Itoa(3+i))
	}
	return files, strings.Join(pairs, ","), closeParent
}

// inheritedListen is the child's deps.listen: the listeners named in the value
// of childEnvListeners, each given once. It returns an error for an address the
// parent did not hand over.
func inheritedListen(env string) (func(addr string) (net.Listener, error), error) {
	m := map[string]net.Listener{}
	for pair := range strings.SplitSeq(env, ",") {
		addr, fdStr, ok := strings.Cut(pair, "=")
		fd, err := strconv.Atoi(fdStr)
		if !ok || err != nil {
			return nil, fmt.Errorf("%s: %q is not addr=fd", childEnvListeners, pair)
		}
		file := os.NewFile(uintptr(fd), addr)
		ln, err := net.FileListener(file)
		_ = file.Close() // FileListener duplicated it
		if err != nil {
			return nil, fmt.Errorf("%s: descriptor %d for %s: %w", childEnvListeners, fd, addr, err)
		}
		m[addr] = ln
	}
	var mu sync.Mutex
	return func(addr string) (net.Listener, error) {
		mu.Lock()
		defer mu.Unlock()
		ln := m[addr]
		if ln == nil {
			return nil, fmt.Errorf("the child inherited no listener for %s (or has bound it already)", addr)
		}
		delete(m, addr)
		return ln, nil
	}, nil
}

// isAddrInUse reports whether err is a bind that found the address taken, as a
// stranger's bind finds a port a test holds.
func isAddrInUse(err error) bool { return errors.Is(err, syscall.EADDRINUSE) }

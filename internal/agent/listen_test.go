package agent

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
)

// TestForwardsStartedTogetherNeverShareAPort: the kernel can offer a port
// again as soon as the probe that got it is closed, before the forward it
// went to has bound it with ssh. Two forwards that ask at the same moment must
// still get different ports (the timing sweep, 1 Nov 2026: tunnel and control
// both got 42079, 1 ms apart, and control moved at once, so its PortChanges
// was 1 before any port was taken from it).
//
// The kernel repeats a port only rarely, so the test forces it: the first two
// requests for port 0 both get the same free port. With ports handed out one
// probe at a time and closed at once, both forwards get it (or the second
// bind fails while the first probe still holds it); with forwardPorts the
// second forward is given another.
func TestForwardsStartedTogetherNeverShareAPort(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	same := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	kernel := probeLoopback
	t.Cleanup(func() { probeLoopback = kernel })
	var mu sync.Mutex
	repeats := 2 // requests for port 0 that get the same port
	probeLoopback = func(port int) (net.Listener, error) {
		mu.Lock()
		repeat := port == 0 && repeats > 0
		if repeat {
			repeats--
		}
		mu.Unlock()
		if repeat {
			return kernel(same)
		}
		return kernel(port)
	}

	fwds := []*SSHForward{{Name: "tunnel"}, {Name: "control"}}
	ports := make([]int, len(fwds))
	errs := make([]error, len(fwds))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, f := range fwds {
		t.Cleanup(f.releasePort)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ports[i], errs[i] = f.localPort()
		}()
	}
	close(start)
	wg.Wait()

	for i, f := range fwds {
		if errs[i] != nil {
			t.Fatalf("forward %s got no port: %v", f.Name, errs[i])
		}
		if st, _ := f.State(); st.Port != ports[i] || st.PortChanges != 0 {
			t.Errorf("forward %s: state %+v, want port %d and no port change", f.Name, st, ports[i])
		}
	}
	if ports[0] == ports[1] {
		t.Fatalf("both forwards got port %d", ports[0])
	}
	mu.Lock()
	defer mu.Unlock()
	if repeats != 0 {
		t.Fatalf("the kernel's repeat was not forced: %d of 2 repeats left", repeats)
	}
}

// TestAFailedMoveKeepsTheForwardsPort (PR #144 review, P1 and N1): a forward
// whose port cannot be bound again, and whose request for a replacement then
// fails, keeps its port. While it does, the kernel offering that port to
// another forward must not hand it out; the first forward then keeps the port
// on its retry; and the other forward's release does not free it. The
// forward gives the port up only at its own release.
func TestAFailedMoveKeepsTheForwardsPort(t *testing.T) {
	kernel := probeLoopback
	t.Cleanup(func() { probeLoopback = kernel })
	var mu sync.Mutex
	var (
		refuse    int // a probe of this port fails, as when another process holds it
		failZero  int // requests for port 0 that fail, as when descriptors run out
		offer     int // the port the kernel offers next for port 0, once
		offerSeen bool
	)
	probeLoopback = func(port int) (net.Listener, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case port != 0 && port == refuse:
			return nil, fmt.Errorf("injected: 127.0.0.1:%d is held by another process", port)
		case port == 0 && failZero > 0:
			failZero--
			return nil, errors.New("injected: too many open files")
		case port == 0 && offer != 0:
			p := offer
			offer, offerSeen = 0, true
			return kernel(p)
		}
		return kernel(port)
	}
	set := func(r, f, o int) {
		mu.Lock()
		defer mu.Unlock()
		refuse, failZero, offer, offerSeen = r, f, o, false
	}
	offered := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return offerSeen
	}
	newFwd := func(name string) *SSHForward {
		f := &SSHForward{Name: name}
		t.Cleanup(f.releasePort)
		return f
	}

	a := newFwd("tunnel")
	p, err := a.localPort()
	if err != nil {
		t.Fatal(err)
	}
	// The port cannot be bound again, and no replacement comes: the move fails.
	set(p, 1, 0)
	if _, err := a.localPort(); err == nil {
		t.Fatal("the move succeeded with no replacement port")
	}
	if st, _ := a.State(); st.Port != p {
		t.Fatalf("after the failed move the forward's port is %d, want its own %d", st.Port, p)
	}
	// The kernel offers that port to another forward: it is still the first's.
	set(0, 0, p)
	b := newFwd("control")
	q, err := b.localPort()
	if err != nil {
		t.Fatal(err)
	}
	if !offered() {
		t.Fatal("the kernel's offer of the first forward's port was not made")
	}
	if q == p {
		t.Fatalf("a second forward was handed port %d, which the first forward still holds", p)
	}
	// The port is free again: the first forward keeps it on its retry.
	set(0, 0, 0)
	if got, err := a.localPort(); err != nil || got != p {
		t.Fatalf("the retry got port %d, %v; want the forward's own %d", got, err, p)
	}
	if st, _ := a.State(); st.PortChanges != 0 {
		t.Fatalf("the forward's port changed %d times, want none", st.PortChanges)
	}
	// The second forward's release leaves the first forward's port held.
	b.releasePort()
	set(0, 0, p)
	c := newFwd("third")
	if r, err := c.localPort(); err != nil || r == p {
		t.Fatalf("after another forward's release a third forward got %d, %v; port %d is still the first forward's", r, err, p)
	}
	// The first forward's own release frees it.
	a.releasePort()
	set(0, 0, p)
	d := newFwd("fourth")
	if r, err := d.localPort(); err != nil || r != p {
		t.Fatalf("after its forward's release port %d was not handed out again: got %d, %v", p, r, err)
	}
}

// closeFails is a listener whose Close reports an error after it closes.
type closeFails struct{ net.Listener }

func (c closeFails) Close() error {
	_ = c.Listener.Close()
	return errors.New("injected: close failed")
}

// TestAMoveAfterACloseErrorGivesTheOldPortUp (PR #144 review, N1): when the
// probe of a forward's own port binds but its close reports an error, the
// forward moves; the port it left must be given up, not held for good.
func TestAMoveAfterACloseErrorGivesTheOldPortUp(t *testing.T) {
	kernel := probeLoopback
	t.Cleanup(func() { probeLoopback = kernel })
	var mu sync.Mutex
	var closeErr, offer int
	probeLoopback = func(port int) (net.Listener, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case port != 0 && port == closeErr:
			ln, err := kernel(port)
			if err != nil {
				return nil, err
			}
			return closeFails{ln}, nil
		case port == 0 && offer != 0:
			p := offer
			offer = 0
			return kernel(p)
		}
		return kernel(port)
	}
	a := &SSHForward{Name: "tunnel"}
	t.Cleanup(a.releasePort)
	p, err := a.localPort()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	closeErr = p
	mu.Unlock()
	q, err := a.localPort()
	if err != nil || q == p {
		t.Fatalf("after the close error the forward has port %d, %v; want it moved from %d", q, err, p)
	}
	mu.Lock()
	closeErr, offer = 0, p
	mu.Unlock()
	b := &SSHForward{Name: "control"}
	t.Cleanup(b.releasePort)
	if r, err := b.localPort(); err != nil || r != p {
		t.Fatalf("port %d, which the first forward left, was not handed out again: got %d, %v", p, r, err)
	}
}

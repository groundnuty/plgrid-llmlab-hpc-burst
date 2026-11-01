package agent

import (
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

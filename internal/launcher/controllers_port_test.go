package launcher

// Issue #384: the fake controller's health port. newCtlFixture used to take it with freePort, which
// releases it, and the fake bound it only once the supervisor had started it, so another process (in
// CI, a parallel package's outbound connection) could take it in the gap and keep it through the
// supervisor's restarts: the fake exited 1 at four starts, and its standard error went to a log the
// test never read. The fixture now holds the port from the moment it chooses it (reservePort) and
// the fake binds it beside that holder (SO_REUSEPORT), and a wait that times out prints the
// supervised processes' logs.

import (
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestAFixturesControllerPortCannotBeTakenBeforeTheFakeBinds: while the fixture holds the port, a
// listener cannot take it, nor can an outbound connection take it as its source port (the two ways
// another process took it), and the fake still binds it, serves /healthz on it and binds it again
// after it exits and the supervisor restarts it.
func TestAFixturesControllerPortCannotBeTakenBeforeTheFakeBinds(t *testing.T) {
	f := newCtlFixture(t, Front{})
	addr := f.ports.ControllerAddr()
	if l, err := net.Listen("tcp", addr); err == nil {
		t.Cleanup(func() { l.Close() })
		t.Errorf("a listener took %s while the fixture holds it", addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Errorf("a listener on %s: %v, want address already in use", addr, err)
	}
	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: f.ports.Controller}, Timeout: 5 * time.Second}
	if c, err := d.Dial("tcp", srv.Addr().String()); err == nil {
		t.Cleanup(func() { c.Close() })
		t.Errorf("an outbound connection took %s as its source port while the fixture holds it", addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Errorf("an outbound connection from %s: %v, want address already in use", addr, err)
	}

	f.supervise(SuperviseOptions{})
	first := f.awaitRunning()
	awaitHealthy(t, addr)
	// It exits, the supervisor starts it again, and it binds the same port again.
	f.steer("exit", "1")
	f.await("a second start", func(e EntryRecord) bool {
		return e.State == entryRunning && e.Proc != nil && e.Proc.PID != first.Proc.PID
	})
	awaitHealthy(t, addr)
	if n := len(f.starts()); n < 2 {
		t.Errorf("the fake started %d times, want at least 2", n)
	}
}

// TestAFakeThatLosesItsPortNamesTheBindErrorInTheTestsOutput: with the port taken by a process the
// fixture does not control, the fake exits 1 at its start, and what it wrote is in childLogs and in
// the message of a wait that times out (withChildLogs), so the failure names itself. The wait here
// is for the log to hold the error, and the message is built, not waited for.
func TestAFakeThatLosesItsPortNamesTheBindErrorInTheTestsOutput(t *testing.T) {
	f := newCtlFixture(t, Front{})
	f.releasePort()
	taken, err := net.Listen("tcp", f.ports.ControllerAddr())
	if err == nil {
		t.Cleanup(func() { taken.Close() })
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatal(err) // another process holding the port serves as well as this listener
	}
	f.supervise(SuperviseOptions{})
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(f.childLogs(), "address already in use") {
		if time.Now().After(deadline) {
			t.Fatalf("the fake's bind error is not in the child logs after 20 s: %q", f.childLogs())
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := f.withChildLogs("timed out waiting for the controller to run")
	for _, want := range []string{"timed out waiting for the controller to run", "controller.log", "fakecontroller:", "address already in use"} {
		if !strings.Contains(got, want) {
			t.Errorf("the timeout's message lacks %q:\n%s", want, got)
		}
	}
	if quiet := newCtlFixture(t, Front{}); quiet.childLogs() != "" || quiet.withChildLogs("m") != "m" {
		t.Errorf("a fixture that ran nothing has logs: %q", quiet.childLogs())
	}
}

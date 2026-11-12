package testenv

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// listenHeld holds a loopback listener on a port of the kernel's choosing for the test, and returns
// its address.
func listenHeld(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// A free address passes, and it is still free afterwards: the check listens and closes, and must
// leave nothing bound.
func TestFreePortsAcceptsAFreeAddressAndLeavesItFree(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	if err := FreePorts(addr); err != nil {
		t.Fatalf("a free address is refused: %v", err)
	}
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the check left the address bound: %v", err)
	}
	again.Close()
}

// Something that holds a port, and answers nothing there, is refused, and the error names its port
// and address; a free address beside it is not named. This is the case /healthz cannot see.
func TestFreePortsNamesTheHeldPortAndOnlyThat(t *testing.T) {
	held := listenHeld(t)
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freeAddr := free.Addr().String()
	free.Close()
	err = FreePorts(freeAddr, held)
	if err == nil {
		t.Fatalf("a held address (%s) was accepted", held)
	}
	_, heldPort, _ := net.SplitHostPort(held)
	_, freePort, _ := net.SplitHostPort(freeAddr)
	if !strings.Contains(err.Error(), held) || !strings.Contains(err.Error(), "port "+heldPort) {
		t.Errorf("the error does not name the held address %s and its port: %v", held, err)
	}
	if strings.Contains(err.Error(), freeAddr) {
		t.Errorf("the error names the free address %s: %v", freeAddr, err)
	}
	if heldPort == freePort {
		t.Fatal("the test's two addresses share a port")
	}
}

// Every held address is named, not only the first.
func TestFreePortsNamesEveryHeldPort(t *testing.T) {
	a, b := listenHeld(t), listenHeld(t)
	err := FreePorts(a, b)
	if err == nil || !strings.Contains(err.Error(), a) || !strings.Contains(err.Error(), b) {
		t.Fatalf("want both %s and %s named, got %v", a, b, err)
	}
}

// startSleeper starts a process that does nothing, only so that the identity code has a process to
// identify, and returns its identity from the kernel's own records. It is a stand-in for the
// identity and wait code and for nothing else: no test here decides that anything leaked by finding
// a process by name.
func startSleeper(t *testing.T) (*exec.Cmd, ProcID) {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	ticks, err := startTicks(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	return cmd, ProcID{PID: cmd.Process.Pid, StartTicks: ticks, BootID: strings.TrimSpace(string(boot))}
}

// A running process opens, does not report an exit, exits when it is killed through the pidfd, and
// is reported gone by the pidfd's readiness: the wait is the kernel's event, and the 20 s only ends a
// hang. Once it is reaped, the identity matches no process.
func TestProcIdentityFollowsAProcessToItsExit(t *testing.T) {
	cmd, id := startSleeper(t)
	p, err := OpenProc(id)
	if err != nil {
		t.Fatalf("a running process with its own identity did not open: %v", err)
	}
	defer p.Close()
	if p.WaitExit(0) {
		t.Fatal("a running process was reported as exited")
	}
	if err := p.Kill(); err != nil {
		t.Fatalf("kill through the pidfd: %v", err)
	}
	if !p.WaitExit(20 * time.Second) {
		t.Fatal("the process did not report an exit after SIGKILL")
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("the killed process reported a clean exit")
	}
	if _, err := OpenProc(id); !errors.Is(err, ErrProcGone) {
		t.Fatalf("after the exit, the identity opened: %v", err)
	}
}

// The identity is the PID with its start time and boot ID, so a PID that another process holds now
// does not open: this is the control for a leak check that would otherwise take any process with the
// recorded PID for the recorded one.
func TestProcIdentityRefusesAReusedPIDAndAnotherBoot(t *testing.T) {
	_, id := startSleeper(t)
	for name, bad := range map[string]ProcID{
		"another start time": {PID: id.PID, StartTicks: id.StartTicks + 1, BootID: id.BootID},
		"another boot":       {PID: id.PID, StartTicks: id.StartTicks, BootID: "00000000-0000-0000-0000-000000000000"},
		"no start time":      {PID: id.PID, BootID: id.BootID},
		"pid 1":              {PID: 1, StartTicks: id.StartTicks, BootID: id.BootID},
	} {
		p, err := OpenProc(bad)
		if err == nil {
			p.Close()
			t.Errorf("%s: the identity opened", name)
		}
	}
}

// statLine builds a /proc/<pid>/stat line from its command name and its start time, with the 17
// fields between the state and the start time as 0, so the expected value is the one written into
// field 22 and not one the parser produced.
func statLine(comm string, start uint64) string {
	f := []string{"123", "(" + comm + ")", "S"}
	for len(f) < 21 {
		f = append(f, "0") // fields 4 to 21
	}
	f = append(f, strconv.FormatUint(start, 10)) // field 22
	return strings.Join(append(f, "0", "0"), " ")
}

// The start time is field 22, counted from the last closing parenthesis, so a command name with
// spaces and parentheses (a process can set any name) does not shift the fields, and a line that
// is cut short is an error, not a zero.
func TestParseStartTicks(t *testing.T) {
	for _, comm := range []string{"sleep", "a b", "we(ird) na)me", "(((", ") 1 2 3 ("} {
		got, err := parseStartTicks(statLine(comm, 424242))
		if err != nil || got != 424242 {
			t.Errorf("comm %q: start time %d, %v; want 424242", comm, got, err)
		}
	}
	for name, line := range map[string]string{
		"no parenthesis": "123 sleep S 1",
		"too few fields": "123 (sleep) S 1 2 3",
		"not a number":   strings.Replace(statLine("x", 1), " 1 ", " one ", 1),
	} {
		if got, err := parseStartTicks(line); err == nil {
			t.Errorf("%s: parsed to %d, want an error", name, got)
		}
	}
	if got, err := startTicks(os.Getpid()); err != nil || got == 0 {
		t.Errorf("this process's own start time: %d, %v", got, err)
	}
	if _, err := startTicks(0x7fffffff); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a PID with no process: %v, want a not-exist error", err)
	}
}

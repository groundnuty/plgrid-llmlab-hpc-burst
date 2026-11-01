package scenario_test

// The rehearsal's own guards, checked without the rehearsal: these run in an
// ordinary go test, with no HPCBURST_REHEARSAL.

import (
	"os/exec"
	"testing"
	"time"
)

// TestRootK3sGuard: a host with no system k3s, such as a CI runner, may run
// the rehearsal unless HPCBURST_TEST_ROOT_K3S=1 says one runs there; a change
// of its listeners across the run fails it, whichever they were.
func TestRootK3sGuard(t *testing.T) {
	for _, c := range []struct {
		before   string
		required bool
		ok       bool
	}{
		{"", false, true},                // a CI runner
		{"6443,6444,10250", false, true}, // this VM, unstated
		{"6443,6444,10250", true, true},
		{"", true, false},
		{"6443,10250", true, false},
	} {
		if err := rootK3sBefore(c.before, c.required); (err == nil) != c.ok {
			t.Errorf("rootK3sBefore(%q, %v) = %v, want ok %v", c.before, c.required, err, c.ok)
		}
	}
	for _, c := range []struct {
		before, after string
		ok            bool
	}{
		{"", "", true},
		{"6443,6444,10250", "6443,6444,10250", true},
		{"6443,6444,10250", "6443,10250", false},
		{"", "6443", false},
	} {
		if err := rootK3sAfter(c.before, c.after); (err == nil) != c.ok {
			t.Errorf("rootK3sAfter(%q, %q) = %v, want ok %v", c.before, c.after, err, c.ok)
		}
	}
}

// TestProcIDGone: step (g)'s check after down tells a live process from one
// that ended, by its PID and start time, on a child this test starts and
// stops by its PID.
func TestProcIDGone(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	start, ok := procStart(pid)
	if !ok {
		t.Fatalf("procStart(%d) of a running child: not found", pid)
	}
	p := procID{name: "sleep", pid: pid, start: start}
	if p.gone() {
		t.Fatalf("a running child %d counts as gone", pid)
	}
	if (procID{name: "sleep", pid: pid, start: start + 1}).gone() == false {
		t.Errorf("a process under the same PID with another start time counts as the same one")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	// A zombie until reaped, which holds nothing: the kill is delivered
	// asynchronously, so this waits on the process's state.
	waitFor(t, 30*time.Second, "the killed, unreaped child to count as gone", p.gone)
	_ = cmd.Wait()
	if !p.gone() {
		t.Errorf("a reaped child %d does not count as gone", pid)
	}
}

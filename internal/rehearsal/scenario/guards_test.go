package scenario_test

// The rehearsal's own guards, checked without the rehearsal: these run in an
// ordinary go test, with no HPCBURST_REHEARSAL.

import (
	"os/exec"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

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
	stack.WaitFor(t, 30*time.Second, "the killed, unreaped child to count as gone", p.gone)
	_ = cmd.Wait()
	if !p.gone() {
		t.Errorf("a reaped child %d does not count as gone", pid)
	}
}

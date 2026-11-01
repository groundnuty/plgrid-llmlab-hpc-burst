package agent_test

import (
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A running worker whose process dies unexpectedly is reported exited, and starts again when the controller
// raises its generation, even when the end of its whole process group is handled by the loop BEFORE the
// leader's exit (#189). The two events are posted by two goroutines, and a process group that dies at once
// closes both channels together, so the order is a matter of scheduling; this test does not wait for it to
// happen. The seam agent.SetExitOrderForTest holds the leader's exit until the group's end has been handled,
// so the order is forced every time, and it counts both hooks to show the seam was in the path. The worker
// is killed with its whole group (SIGKILL to the group), so nothing is left behind and both channels close
// at once.
func TestARunningWorkerKilledWithItsGroupRestartsWhenTheGroupEndIsHandledFirst(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	pid := worker0(h.waitReport("worker-0 running", 30*time.Second, running)).PID

	var held, handled atomic.Int64
	groupHandled := make(chan struct{})
	var once sync.Once
	t.Cleanup(agent.SetExitOrderForTest(
		func(p int) {
			if p != pid {
				return
			}
			held.Add(1)
			select { // the leader's exit is posted only after the group's end was handled
			case <-groupHandled:
			case <-t.Context().Done():
			}
		},
		func(p int) {
			if p == pid {
				handled.Add(1)
				once.Do(func() { close(groupHandled) })
			}
		}))
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}

	r := h.waitReport("worker-0 reported exited", 30*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartExited
	})
	if w := worker0(r); w.PID != pid || w.ExitCode == nil {
		t.Errorf("the exited worker is reported as %+v, want pid %d and an exit code", w, pid)
	}
	if held.Load() != 1 || handled.Load() != 1 {
		t.Fatalf("the seam saw %d leader exits held and %d group ends handled, want one of each: the order was not forced", held.Load(), handled.Load())
	}

	// The controller raises the worker's generation, as it does after it sees the exit: the agent starts a new
	// process, and reports it running with the new generation.
	h.set(h.runState("", 1, 1, nil))
	r = h.waitReport("worker-0 running again", 30*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1 && w.PID != pid
	})
	if w := worker0(r); w.ExitCode != nil {
		t.Errorf("the restarted worker still reports the old exit code %d", *w.ExitCode)
	}
}

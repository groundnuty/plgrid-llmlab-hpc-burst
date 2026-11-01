package agent_test

import (
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
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
	if w := worker0(r); w.PID != pid || !killedBySignal(w) {
		t.Errorf("the exited worker is reported with pid %d and exit code %s, want pid %d and %d (ended by the group's KILL)", w.PID, exitCodeText(w), pid, procgroup.KilledExitCode)
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

// killedBySignal reports whether the worker's exit code is the one of a leader that a signal ended.
func killedBySignal(w *agentapi.WorkerReport) bool {
	return w.ExitCode != nil && *w.ExitCode == procgroup.KilledExitCode
}

func exitCodeText(w *agentapi.WorkerReport) string {
	if w.ExitCode == nil {
		return "none"
	}
	return strconv.Itoa(*w.ExitCode)
}

// A stop that begins after the end of a running worker's group was handled finishes at once (#189, the third
// ordering, found by PR #197's review): the loop handles the group's end of a worker that is still running,
// which changes nothing; the controller then idles the Instance or raises the worker's generation, which
// stops the worker; and only then does the leader's exit reach the loop. The group's end is posted once, so
// without the fix the worker stayed stopping and live for good: an idle Instance showed it stopping for ever,
// and a raised generation never started the new process. The seam holds the leader's exit until the stop has
// been handled, so the order is forced every time; the stop is seen in the report (the worker is stopping or
// stopped, or the new process is there), and both seam hooks are counted.
func TestAStopBegunAfterTheGroupEndWasHandledEndsWithTheWorkerStoppedOrRestarted(t *testing.T) {
	for _, tc := range []struct {
		name string
		// instruct is the controller's instruction that stops the worker.
		instruct func(h *harness) agentapi.DesiredState
		// stopSeen: the report shows the instruction was handled, whether or not the fix is in.
		stopSeen func(w *agentapi.WorkerReport, oldPID int) bool
		// outcome: what the worker is once the leader's exit has been handled too.
		outcome func(w *agentapi.WorkerReport, oldPID int) bool
		what    string
	}{
		{
			name: "an idle instruction",
			instruct: func(h *harness) agentapi.DesiredState {
				return agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: h.runState("", 1, 0, nil).Settings}
			},
			stopSeen: func(w *agentapi.WorkerReport, _ int) bool {
				return w.Phase == agentapi.PartStopping || w.Phase == agentapi.PartStopped
			},
			outcome: func(w *agentapi.WorkerReport, oldPID int) bool {
				return w.Phase == agentapi.PartStopped && w.PID == oldPID && killedBySignal(w)
			},
			what: "worker-0 stopped with the exit code of a killed group",
		},
		{
			name:     "a raised generation",
			instruct: func(h *harness) agentapi.DesiredState { return h.runState("", 1, 1, nil) },
			stopSeen: func(w *agentapi.WorkerReport, oldPID int) bool {
				return w.Phase == agentapi.PartStopping || w.Phase == agentapi.PartStopped || w.PID != oldPID
			},
			outcome: func(w *agentapi.WorkerReport, oldPID int) bool {
				return w.Phase == agentapi.PartRunning && w.Generation == 1 && w.PID != oldPID && w.ExitCode == nil
			},
			what: "worker-0 running again with generation 1 and a new process",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			h.set(h.runState("", 1, 0, nil))
			pid := worker0(h.waitReport("worker-0 running", 30*time.Second, running)).PID

			var held, handled atomic.Int64
			groupHandled, release := make(chan struct{}), make(chan struct{})
			var onceGroup, onceRelease sync.Once
			t.Cleanup(agent.SetExitOrderForTest(
				func(p int) {
					if p != pid {
						return
					}
					held.Add(1)
					select { // the leader's exit is posted only after the stop was handled
					case <-release:
					case <-t.Context().Done():
					}
				},
				func(p int) {
					if p == pid {
						handled.Add(1)
						onceGroup.Do(func() { close(groupHandled) })
					}
				}))
			if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			select { // the loop has handled the end of the group; the worker is still running
			case <-groupHandled:
			case <-time.After(30 * time.Second):
				t.Fatal("the end of the worker's group was not handled")
			}

			h.set(tc.instruct(h))
			h.waitReport("the stop handled", 30*time.Second, func(r agentapi.Report) bool {
				w := worker0(r)
				return w != nil && tc.stopSeen(w, pid)
			})
			onceRelease.Do(func() { close(release) }) // now the leader's exit reaches the loop

			h.waitReport("the outcome: "+tc.what, 30*time.Second, func(r agentapi.Report) bool {
				w := worker0(r)
				return w != nil && tc.outcome(w, pid)
			})
			if held.Load() != 1 || handled.Load() != 1 {
				t.Fatalf("the seam saw %d leader exits held and %d group ends handled, want one of each: the order was not forced", held.Load(), handled.Load())
			}
		})
	}
}

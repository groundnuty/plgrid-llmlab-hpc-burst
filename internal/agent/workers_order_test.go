package agent

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A worker the agent is stopping ends up stopped whichever of the two events
// of its exit is handled first. The leader's exit (leaderExited) and the end of
// its whole group (groupDone) are posted by two goroutines, and a process that
// dies at once on TERM closes both channels together: when the group's end is
// handled first it makes the worker stopped, and the leader's exit that follows
// must not turn it into an unexpected exit, reported exited, with a warning
// (found by PR #169's count-50 run, where an idle instruction after a refused
// verification left worker-0 exited). A worker that was not being stopped is
// still exited when its leader exits, and the same holds in the reverse order, when the group's end of a
// running worker is handled first (#189): it is exited, and not left cleaning a group that is already gone.
func TestAStoppedWorkerIsNotExitedByALateLeaderExit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first string // the event handled first
		// phase of the worker before the events, and what it is after.
		before, want agentapi.PartPhase
		warns        bool
	}{
		{"stopping: the leader's exit first", "leader", agentapi.PartStopping, agentapi.PartStopped, false},
		{"stopping: the group's end first", "group", agentapi.PartStopping, agentapi.PartStopped, false},
		{"running: the leader's exit is unexpected", "leader", agentapi.PartRunning, agentapi.PartExited, true},
		// The reverse case (#189): a running worker dies at once, and the group's end is handled before the
		// leader's exit. The worker is exited all the same, and nothing of its group is left to clean up.
		{"running: the group's end first", "group", agentapi.PartRunning, agentapi.PartExited, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := procgroup.Start(procgroup.Spec{Path: "/bin/sh", Args: []string{"-c", "exit 0"}, Dir: t.TempDir()}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			select { // the process is gone; its channels are closed
			case <-g.Done():
			case <-time.After(30 * time.Second):
				t.Fatal("the process did not end")
			}
			var logs bytes.Buffer
			a := &Agent{log: slog.New(slog.NewTextHandler(&logs, nil)), o: Options{LogDir: t.TempDir()}}
			w := &worker{name: agentapi.WorkerName(1), phase: tc.before, proc: g}
			in := &instance{workers: []*worker{w}}
			a.inst = in
			if tc.first == "leader" {
				a.leaderExited(in, g)
				a.groupDone(in, g)
			} else {
				a.groupDone(in, g)
				a.leaderExited(in, g)
			}
			if w.phase != tc.want {
				t.Errorf("worker is %s, want %s", w.phase, tc.want)
			}
			// Both events are handled and the group is gone: no process of the worker may be counted live. A worker
			// still "cleaning" is live, and no restart ever starts beside it (convergeWorkers waits for !live()).
			if w.cleaning || w.live() {
				t.Errorf("after both events the worker is cleaning=%v live=%v, want neither: it can never be restarted", w.cleaning, w.live())
			}
			if warned := strings.Contains(logs.String(), "without being told to"); warned != tc.warns {
				t.Errorf("warning logged %v, want %v: %s", warned, tc.warns, logs.String())
			}
		})
	}
}

// A stop that begins after the end of the worker's group was handled finishes at once (#189's third
// ordering, found by PR #197's review). A running worker killed with its whole group closes both channels
// together; the loop can handle the group's end first, which changes nothing for a worker that is still
// running, and the controller's next instruction (an idle one, a raised generation) then stops it. The end of
// a group is posted once, and the leader's exit that follows meets a stopping worker and returns, so nothing
// would ever make the worker stopped: it stayed stopping and live for good, and no restart ever started.
// Every order of the stop and the two events ends with the worker stopped, no warning, and nothing live.
func TestAStopBegunAfterTheGroupEndedFinishesAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order []string
	}{
		{"the group's end, the stop, the leader's exit (#189)", []string{"group", "stop", "leader"}},
		{"the stop, the group's end, the leader's exit", []string{"stop", "group", "leader"}},
		{"the stop, the leader's exit, the group's end", []string{"stop", "leader", "group"}},
		{"the leader's exit, the stop, the group's end", []string{"leader", "stop", "group"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := procgroup.Start(procgroup.Spec{Path: "/bin/sh", Args: []string{"-c", "exit 0"}, Dir: t.TempDir()}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			select { // the process is gone; its channels are closed
			case <-g.Done():
			case <-time.After(30 * time.Second):
				t.Fatal("the process did not end")
			}
			var logs bytes.Buffer
			a := &Agent{log: slog.New(slog.NewTextHandler(&logs, nil)), o: Options{LogDir: t.TempDir()}}
			w := &worker{name: agentapi.WorkerName(1), phase: agentapi.PartRunning, proc: g}
			in := &instance{workers: []*worker{w}}
			a.inst = in
			for _, ev := range tc.order {
				switch ev {
				case "group":
					a.groupDone(in, g)
				case "leader":
					a.leaderExited(in, g)
				case "stop":
					a.terminate(w)
				}
			}
			want := agentapi.PartStopped
			if tc.order[0] == "leader" {
				// An unexpected exit comes first: the worker is exited, and a stop that finds it so changes
				// nothing (this is the order that was never in doubt).
				want = agentapi.PartExited
			}
			if w.phase != want {
				t.Errorf("worker is %s, want %s", w.phase, want)
			}
			if w.cleaning || w.live() {
				t.Errorf("after the stop and both events the worker is cleaning=%v live=%v, want neither: it can never be restarted", w.cleaning, w.live())
			}
			if w.exitCode == nil {
				t.Error("the worker has no exit code")
			}
			if warned := strings.Contains(logs.String(), "without being told to"); warned != (want == agentapi.PartExited) {
				t.Errorf("warning logged %v, want %v: %s", warned, want == agentapi.PartExited, logs.String())
			}
		})
	}
}

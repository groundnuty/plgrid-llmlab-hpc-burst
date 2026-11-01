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
// still exited when its leader exits.
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
			if warned := strings.Contains(logs.String(), "without being told to"); warned != tc.warns {
				t.Errorf("warning logged %v, want %v: %s", warned, tc.warns, logs.String())
			}
		})
	}
}

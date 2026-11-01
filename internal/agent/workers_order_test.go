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

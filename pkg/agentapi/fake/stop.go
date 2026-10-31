package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// killedExitCode is the exit code of a process killed by a signal
// (agentapi.WorkerReport.ExitCode).
const killedExitCode = -1

// terminate carries out a stop instruction on one worker: a polite stop, with
// TERM. An engine that honours it exits at once, with code 0. An engine that
// ignores it (Engine.IgnoreTerm) is reported stopping until the grace has run
// out, and is then killed (reap). The deadline is fixed by the first
// instruction: the same instruction repeated at every Step does not move it,
// and the grace is the one in the desired state that carried the instruction.
// A worker that is not starting or running is left as it is.
func (a *Agent) terminate(w *worker, now time.Time) {
	if !w.alive() {
		return
	}
	if a.engine.IgnoreTerm {
		w.phase = agentapi.PartStopping
		w.killAt = now.Add(a.stopGrace)
		return
	}
	code := 0
	w.phase = agentapi.PartStopped
	w.exitCode = &code
}

// reap kills every stopping worker whose deadline has come: phase stopped, exit
// code -1. It runs at the end of each Step, so the report of the Step that
// kills a worker shows it stopped, and a restart's new process starts in the
// next Step.
func (a *Agent) reap(now time.Time) {
	if a.inst == nil {
		return
	}
	for _, w := range a.inst.workers {
		if w.phase == agentapi.PartStopping && !now.Before(w.killAt) {
			code := killedExitCode
			w.phase = agentapi.PartStopped
			w.exitCode = &code
		}
	}
}

// stopWorkers instructs every worker of the current Instance to stop.
func (a *Agent) stopWorkers(now time.Time) {
	if a.inst == nil {
		return
	}
	for _, w := range a.inst.workers {
		a.terminate(w, now)
	}
}

// alive reports whether the worker is starting or running: a stop instruction
// still has something to do.
func (w *worker) alive() bool {
	return w.phase == agentapi.PartStarting || w.phase == agentapi.PartRunning
}

// live reports whether a process of the worker still exists, which includes
// one that is stopping.
func (w *worker) live() bool {
	return w.alive() || w.phase == agentapi.PartStopping
}

// anyLive reports whether any worker still has a process.
func (in *instance) anyLive() bool {
	for _, w := range in.workers {
		if w.live() {
			return true
		}
	}
	return false
}

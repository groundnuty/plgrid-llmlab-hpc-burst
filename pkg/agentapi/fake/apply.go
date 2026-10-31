package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// apply carries out a desired state that checkDesired accepted. now is the
// time of the whole Step.
func (a *Agent) apply(d agentapi.DesiredState, now time.Time) {
	a.stopping = d.Action == agentapi.ActionStop
	switch d.Action {
	case agentapi.ActionRun:
		a.applyRun(*d.Instance, now)
	case agentapi.ActionIdle, agentapi.ActionStop:
		a.stopWorkers()
	}
}

// applyRun converges on the Instance: a new Instance replaces the old one; for
// the same Instance, a raised Instance generation restarts every worker and a
// raised worker generation restarts that worker. A worker the Agent stopped or
// that exited does not start again until a generation rises.
func (a *Agent) applyRun(i agentapi.DesiredInstance, now time.Time) {
	if a.inst == nil || a.inst.uid != i.UID {
		a.stopWorkers()
		a.inst = &instance{uid: i.UID, generation: i.Generation, key: randHex(32)}
	}
	in := a.inst
	in.keyStored = i.KeyStored
	a.promote(now)

	restartAll := i.Generation > in.generation
	if restartAll {
		in.generation = i.Generation
	}
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		switch {
		case w == nil:
			in.workers = append(in.workers, a.startWorker(dw, now))
		case restartAll || dw.Generation > w.generation:
			*w = *a.startWorker(dw, now)
		}
	}
	for _, w := range in.workers {
		if !desires(i.Workers, w.name) {
			w.stop()
		}
	}
	in.applyChecks(a.engine, i)
}

// startWorker starts a process for the desired worker: phase starting, the next
// PID, started now.
func (a *Agent) startWorker(dw agentapi.DesiredWorker, now time.Time) *worker {
	w := &worker{
		name:       dw.Name,
		phase:      agentapi.PartStarting,
		pid:        a.nextPID,
		start:      now,
		generation: dw.Generation,
		socket:     socketDir + "/" + dw.Name + ".sock",
	}
	a.nextPID++
	return w
}

// promote makes every starting worker running whose start delay has passed.
func (a *Agent) promote(now time.Time) {
	if a.inst == nil {
		return
	}
	for _, w := range a.inst.workers {
		if w.phase == agentapi.PartStarting && !now.Before(w.start.Add(a.engine.StartDelay)) {
			w.phase = agentapi.PartRunning
		}
	}
}

// stopWorkers stops, on instruction, every worker that is starting or running.
func (a *Agent) stopWorkers() {
	if a.inst == nil {
		return
	}
	for _, w := range a.inst.workers {
		w.stop()
	}
}

// stop ends a live worker on instruction: phase stopped, and exit code 0 (a
// polite stop that the engine honoured). A worker that already exited keeps its
// phase and its code.
func (w *worker) stop() {
	if !w.alive() {
		return
	}
	code := 0
	w.phase = agentapi.PartStopped
	w.exitCode = &code
}

func (w *worker) alive() bool {
	return w.phase == agentapi.PartStarting || w.phase == agentapi.PartRunning
}

func (in *instance) find(name string) *worker {
	for _, w := range in.workers {
		if w.name == name {
			return w
		}
	}
	return nil
}

func desires(ws []agentapi.DesiredWorker, name string) bool {
	for _, w := range ws {
		if w.Name == name {
			return true
		}
	}
	return false
}

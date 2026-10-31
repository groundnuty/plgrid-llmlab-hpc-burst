package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// apply carries out a desired state that checkDesired accepted. now is the
// time of the whole Step.
func (a *Agent) apply(d agentapi.DesiredState, now time.Time) {
	a.stopping = d.Action == agentapi.ActionStop
	a.stopGrace = max(0, time.Duration(d.Settings.StopGrace))
	a.imageFormat = imageFormatOf(d.Settings.ImageFormat)
	switch d.Action {
	case agentapi.ActionRun:
		a.applyRun(*d.Instance, now)
	case agentapi.ActionIdle, agentapi.ActionStop:
		a.stopWorkers(now)
	}
	a.reap(now)
}

// applyRun converges on the Instance: a new Instance replaces the old one once
// the old one's processes are gone; for the same Instance, a raised Instance
// generation restarts every worker and a raised worker generation restarts that
// worker. A worker the Agent stopped or that exited does not start again until
// a generation rises. No worker starts before the Instance's staging is done
// (staging.go).
func (a *Agent) applyRun(i agentapi.DesiredInstance, now time.Time) {
	if a.inst != nil && a.inst.uid != i.UID {
		a.stopWorkers(now)
		if a.inst.anyLive() {
			// The reports keep showing the old Instance, as the one the Agent
			// last ran, until its last process is gone.
			return
		}
		a.inst = nil
	}
	if a.inst == nil {
		a.inst = &instance{uid: i.UID, generation: i.Generation, key: randHex(32), stageStart: now, stageGen: i.Generation}
	}
	in := a.inst
	in.keyStored = i.KeyStored
	in.restage(a.engine, i.Generation, now)
	a.promote(now)
	// No worker starts, or restarts, before staging is done.
	if in.staging(a.engine, now).Phase == agentapi.StagingDone {
		a.convergeWorkers(i, now)
	}
	in.applyChecks(a.engine, i)
}

// convergeWorkers starts, restarts and stops workers to match the desired
// Instance. A restart stops the old process first; the new one starts once the
// old one is gone, in the same Step when the engine honoured TERM, and in the
// Step after the kill when it did not.
func (a *Agent) convergeWorkers(i agentapi.DesiredInstance, now time.Time) {
	in := a.inst
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		switch {
		case w == nil:
			in.workers = append(in.workers, a.startWorker(dw, i, now))
		case i.Generation > w.instGen || dw.Generation > w.generation:
			a.terminate(w, now)
			if !w.live() {
				*w = *a.startWorker(dw, i, now)
			}
		}
	}
	for _, w := range in.workers {
		if !desires(i.Workers, w.name) {
			a.terminate(w, now)
		}
	}
	// The Instance generation counts as applied once every desired worker runs
	// a process started for it.
	if in.startedFor(i) {
		in.generation = i.Generation
	}
}

// startWorker starts a process for the desired worker: phase starting, the next
// PID, started now. It records the start (Starts).
func (a *Agent) startWorker(dw agentapi.DesiredWorker, i agentapi.DesiredInstance, now time.Time) *worker {
	w := &worker{
		name:       dw.Name,
		phase:      agentapi.PartStarting,
		pid:        a.nextPID,
		start:      now,
		generation: dw.Generation,
		instGen:    i.Generation,
		socket:     socketDir + "/" + dw.Name + ".sock",
	}
	a.nextPID++
	a.starts = append(a.starts, Start{
		Instance:           i.UID,
		Worker:             dw.Name,
		PID:                w.pid,
		Generation:         dw.Generation,
		InstanceGeneration: i.Generation,
		Launch:             cloneLaunch(i.Launch),
		ImageFormat:        a.imageFormat,
		Time:               now,
	})
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

// startedFor reports whether every desired worker has a process started for the
// desired Instance generation or a later one.
func (in *instance) startedFor(i agentapi.DesiredInstance) bool {
	for _, dw := range i.Workers {
		if w := in.find(dw.Name); w == nil || w.instGen < i.Generation {
			return false
		}
	}
	return true
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

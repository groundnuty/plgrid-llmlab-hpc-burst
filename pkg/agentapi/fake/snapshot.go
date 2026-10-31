package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// snapshot returns the full report of the Agent's state as of now. It copies
// everything, so the Agent's later steps never change a report it has sent.
func (a *Agent) snapshot(now time.Time) agentapi.Report {
	r := agentapi.Report{Time: now}
	in := a.inst
	if in == nil {
		return r
	}
	ir := &agentapi.InstanceReport{
		UID:        in.uid,
		Generation: in.generation,
		Staging:    &agentapi.Staging{Phase: agentapi.StagingDone},
	}
	if !in.keyStored {
		ir.Key = in.key
	}
	for _, w := range in.workers {
		ir.Workers = append(ir.Workers, a.workerReport(w, now))
	}
	if w := in.find(agentapi.WorkerName(0)); w != nil && w.phase == agentapi.PartRunning {
		ir.ServingEndpoint = "unix:" + w.socket
	}
	ir.Checks = clonePtr(in.checks, cloneCheckResults)
	r.Instance = ir
	return r
}

func (a *Agent) workerReport(w *worker, now time.Time) agentapi.WorkerReport {
	start := w.start
	wr := agentapi.WorkerReport{
		Name:         w.name,
		Phase:        w.phase,
		PID:          w.pid,
		StartTime:    &start,
		Generation:   w.generation,
		EngineSocket: w.socket,
	}
	if w.exitCode != nil {
		code := *w.exitCode
		wr.ExitCode = &code
	}
	if w.phase == agentapi.PartRunning {
		h := a.engine.health(now, w.name)
		wr.Health = &h
	}
	return wr
}

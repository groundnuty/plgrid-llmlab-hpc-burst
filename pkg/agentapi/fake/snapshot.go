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
	staging := in.staging(a.engine, now)
	ir := &agentapi.InstanceReport{
		UID:        in.uid,
		Generation: in.generation,
		Staging:    &staging,
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
	ir.Checks = clonePtr(in.checks, func(c agentapi.CheckResults) agentapi.CheckResults {
		c = cloneCheckResults(c)
		cutFreeText(&c)
		return c
	})
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
		h := a.engine.health(now, Process{Worker: w.name, PID: w.pid, Start: w.start, Generation: w.generation})
		wr.Health = &h
	}
	return wr
}

// cutFreeText cuts the free text of check results to the protocol's bounds, at
// a rune boundary (agentapi.TruncateUTF8): RequestTiming.Error to
// agentapi.MaxTimingErrorBytes in every round, VerifyResult.Error and
// CheckOutcome.Message to agentapi.MaxMessageBytes. The real agent cuts what it
// reports, so a report of the highest admitted bounds stays under
// agentapi.MaxBodyBytes. It changes c in place, so c must be a copy the caller
// owns (cloneCheckResults).
func cutFreeText(c *agentapi.CheckResults) {
	if v := c.Verify; v != nil {
		v.Error = agentapi.TruncateUTF8(v.Error, agentapi.MaxMessageBytes)
	}
	if q := c.Qualify; q != nil {
		for n := range q.Checks {
			q.Checks[n].Message = agentapi.TruncateUTF8(q.Checks[n].Message, agentapi.MaxMessageBytes)
		}
	}
	if w := c.WarmUp; w != nil {
		for _, round := range w.Rounds {
			for n := range round.Requests {
				round.Requests[n].Error = agentapi.TruncateUTF8(round.Requests[n].Error, agentapi.MaxTimingErrorBytes)
			}
		}
	}
}

package agent

import (
	"encoding/json"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// snapshot returns the full report of the agent's state now (HBEP-7,
// Reports). It copies everything, so later changes never reach a report
// already handed to the reporter. Free text is cut to the protocol's bounds.
func (a *Agent) snapshot() agentapi.Report {
	r := agentapi.Report{Time: a.o.Now()}
	in := a.inst
	if in == nil {
		return r
	}
	st := in.staging
	ir := &agentapi.InstanceReport{
		UID:        in.uid,
		Generation: in.generation,
		Staging:    &st,
	}
	if !in.keyStored {
		ir.Key = in.key
	}
	for _, w := range in.workers {
		wr := agentapi.WorkerReport{
			Name:         w.name,
			Phase:        w.phase,
			Generation:   w.generation,
			EngineSocket: w.socket,
			LogTail:      w.logTail,
		}
		if w.proc != nil {
			start := w.proc.Start()
			wr.PID = w.proc.PID()
			wr.StartTime = &start
		}
		if w.exitCode != nil {
			c := *w.exitCode
			wr.ExitCode = &c
		}
		if w.phase == agentapi.PartRunning && w.health != nil {
			h := cloneHealth(*w.health)
			wr.Health = &h
		}
		ir.Workers = append(ir.Workers, wr)
	}
	if w := in.find(agentapi.WorkerName(0)); w != nil && w.phase == agentapi.PartRunning {
		ir.ServingEndpoint = "unix:" + a.fwd.Path()
	}
	if in.checks != nil {
		c := cloneChecks(*in.checks)
		cutFreeText(&c)
		ir.Checks = &c
	}
	r.Instance = ir
	return r
}

// cloneChecks deep-copies check results through JSON: they hold only plain
// values, slices and maps.
func cloneChecks(c agentapi.CheckResults) agentapi.CheckResults {
	b, err := json.Marshal(c)
	if err != nil {
		return agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	var out agentapi.CheckResults
	if err := json.Unmarshal(b, &out); err != nil {
		return agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	return out
}

// cutFreeText cuts the free text of check results to the protocol's bounds
// (agentapi.TruncateUTF8): RequestTiming.Error to MaxTimingErrorBytes,
// VerifyResult.Error and CheckOutcome.Message to MaxMessageBytes. It changes c,
// which must be a copy.
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

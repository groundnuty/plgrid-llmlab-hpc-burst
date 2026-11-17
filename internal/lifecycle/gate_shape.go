package lifecycle

import (
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The shape a warm-up ran (HBEP-7, The throughput gate: its reference; #616 PR 2).
//
// The gate's key holds the concurrent shape one worker ran, and in this version a miss can release an Allocation,
// so the key is taken from what the warm-up ran, never from the deployment's workload as declared now. A workload
// edit during a warm-up would otherwise key a measurement on a shape it never ran.
//
// What the warm-up ran is read from two places. The typical context and output are the ones the verification
// record fixed: the desired state asks the agent for the warm-up with exactly the record's Shapes (checks,
// workerChecks), so the record is the durable statement of them. The sessions have no durable record: the desired
// state asks for the worker's share of targetSessions as the workload declares it at the time, and the report does
// not say how many it ran. So the settled round itself is the check: its concurrent requests number the key's
// sessions, and each one's output is the key's output. Anything else, a workload edited while the warm-up ran or a
// round that is not the shape that was asked for, is a mismatch, and a mismatch records only.

// gateWorker is what the gate reads of one worker: its measurement, the warm-up result it was taken from, and the
// verification record the warm-up ran under (the Instance's latest, or the worker's own).
type gateWorker struct {
	name   string
	tp     *v1alpha1.Throughput
	warm   *agentapi.WarmUpResult
	record string
}

// ranKey is the key of the Instance's workers' warm-ups: its ID and capacity flags, the typical context and output
// of the verification record, and the sessions of the worker's share of targetSessions as declared now. mismatch is
// empty when every worker's settled round ran that shape, else what differed, and the gate then records only. A
// record that can't be read is an error, never a verdict: RecordOnly is final, and a transient read must not end
// the gate for good.
func (p *pass) ranKey(in *v1alpha1.Instance, sh shapes, ws []gateWorker) (key v1alpha1.ReferenceKey, mismatch string, err error) {
	shape := ranShape{sessions: workerSessions(sh.targetSessions, in)}
	for i, w := range ws {
		rec, err := p.recordedShapes(in, w)
		if err != nil {
			return v1alpha1.ReferenceKey{}, "", err
		}
		if i == 0 {
			shape.context, shape.output = rec.ContextTypical, rec.OutputTypical
			continue
		}
		if rec.ContextTypical != shape.context || rec.OutputTypical != shape.output {
			return instanceKey(in, shape), fmt.Sprintf("%s ran contexts of %d and outputs of %d tokens, %s %d and %d", w.name, rec.ContextTypical, rec.OutputTypical, ws[0].name, shape.context, shape.output), nil
		}
	}
	key = instanceKey(in, shape)
	for _, w := range ws {
		if why := roundMismatch(w, shape); why != "" {
			return key, why, nil
		}
	}
	return key, "", nil
}

// recordedShapes is the resolved shapes of the worker's verification record, the ones its warm-up was asked to run.
func (p *pass) recordedShapes(in *v1alpha1.Instance, w gateWorker) (agentapi.Shapes, error) {
	d, found, err := p.r.readVerification(p.ctx, in.Namespace, w.record)
	switch {
	case err != nil:
		return agentapi.Shapes{}, err
	case !found:
		return agentapi.Shapes{}, fmt.Errorf("the verification record %q that %s's warm-up ran under is not readable yet", w.record, w.name)
	case d.Shapes == nil:
		return agentapi.Shapes{}, fmt.Errorf("the verification record %s holds no resolved shapes", w.record)
	}
	return *d.Shapes, nil
}

// roundMismatch says how a worker's settled round differs from the shape the key names, or "" when it ran it: the
// number of concurrent requests is the key's sessions, and each one produced the key's output.
func roundMismatch(w gateWorker, shape ranShape) string {
	round, ok := settledRound(w.warm)
	if !ok {
		return fmt.Sprintf("%s has no settled warm-up round to read the shape from", w.name)
	}
	var n int32
	for _, q := range round.Requests {
		if q.Shape != agentapi.ShapeConcurrent {
			continue
		}
		n++
		if q.OutputTokens != shape.output {
			return fmt.Sprintf("a concurrent request of %s's settled round produced %d output tokens, not the %d the verification record's shapes ask for", w.name, q.OutputTokens, shape.output)
		}
	}
	if n != shape.sessions {
		return fmt.Sprintf("%s's settled round ran %d concurrent requests, not the %d of the worker's share of targetSessions as declared now", w.name, n, shape.sessions)
	}
	return ""
}

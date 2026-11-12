package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An Instance of several workers (beta; HBEP-7, Several workers; the science agent's rulings on #439).
// Everything here applies only when launch.workers is above 1: an Instance of one worker runs alpha's
// code, unchanged. Each worker is verified, qualified and warmed up on its own socket, through its own
// checks in the desired state (Q6), and its durable state is its entry in status.workers:
//   - its record, ver-<uid>-<worker>-<generation>, the generation counting 1, that worker's restarts
//     and the whole Instance's (Q5), so a record names its worker's current verification exactly;
//   - its verdict: the worker is verified while its entry names its current record and that passed;
//   - whether it warmed up, and the restart generation then verified, which the desired state carries
//     as its verifiedGeneration, so the agent's readiness answers 200 for it only then.
//
// The Instance's own Verified, Qualified and WarmedUp conditions are set True once, when every worker
// first passes; qualification is the Instance's, judged from every worker's results, since a worker's
// entry has no qualification of its own. The Instance enters ready the first time when every worker has
// passed, the router's probe passes and the router holds every worker healthy.

// several reports whether the Instance runs several workers.
func several(in *v1alpha1.Instance) bool { return v1alpha1.LaunchWorkers(in.Spec.Launch) > 1 }

// workerNames returns the Instance's workers, worker-0 to worker-<N-1>.
func workerNames(in *v1alpha1.Instance) []string {
	n := int(v1alpha1.LaunchWorkers(in.Spec.Launch))
	out := make([]string, n)
	for i := range out {
		out[i] = v1alpha1.WorkerName(i)
	}
	return out
}

// workerVerificationGeneration is worker w's verification generation: 1, its own restart generation and
// the whole-Instance restart generation, since a whole-Instance restart verifies every worker again (Q5).
func workerVerificationGeneration(in *v1alpha1.Instance, w string) int64 {
	return 1 + restartGeneration(in, w) + restartGeneration(in, v1alpha1.PartInstance)
}

// workerRecordName is worker w's record at its current verification generation.
func workerRecordName(in *v1alpha1.Instance, w string) (string, error) {
	return v1alpha1.WorkerVerificationRecordName(in.UID, w, workerVerificationGeneration(in, w))
}

// workerEntry is status.workers' entry for w, the zero entry when it has none.
func workerEntry(in *v1alpha1.Instance, w string) v1alpha1.InstanceWorker {
	for _, e := range in.Status.Workers {
		if e.Name == w {
			return e
		}
	}
	return v1alpha1.InstanceWorker{Name: w}
}

// workerVerified reports whether worker w passed its verification at its current generation: its entry
// names its current record, and that record passed.
func workerVerified(in *v1alpha1.Instance, w string) bool {
	name, err := workerRecordName(in, w)
	e := workerEntry(in, w)
	return err == nil && e.Record == name && e.Verdict == string(v1alpha1.VerdictPass)
}

// workerWarmed reports whether worker w is verified and warmed up at its current generation.
func workerWarmed(in *v1alpha1.Instance, w string) bool {
	return workerVerified(in, w) && workerEntry(in, w).WarmedUp
}

// setWorker applies f to status.workers' entry for w, adding the entry when it has none, in the workers'
// order.
func setWorker(s *v1alpha1.InstanceStatus, w string, f func(*v1alpha1.InstanceWorker)) {
	for i := range s.Workers {
		if s.Workers[i].Name == w {
			f(&s.Workers[i])
			return
		}
	}
	e := v1alpha1.InstanceWorker{Name: w}
	f(&e)
	s.Workers = append(s.Workers, e)
	idx := func(name string) int {
		i, _ := workerIndexOf(name)
		return i
	}
	for i := len(s.Workers) - 1; i > 0 && idx(s.Workers[i].Name) < idx(s.Workers[i-1].Name); i-- {
		s.Workers[i], s.Workers[i-1] = s.Workers[i-1], s.Workers[i]
	}
}

// workerIndexOf is i of worker-<i>.
func workerIndexOf(name string) (int, bool) {
	var i int
	if _, err := fmt.Sscanf(name, "worker-%d", &i); err != nil || v1alpha1.WorkerName(i) != name {
		return 0, false
	}
	return i, true
}

// reportOf is the agent's report of worker w.
func reportOf(ir *agentapi.InstanceReport, w string) *agentapi.WorkerReport {
	for i := range ir.Workers {
		if ir.Workers[i].Name == w {
			return &ir.Workers[i]
		}
	}
	return nil
}

// workerAt reports whether the agent runs worker w, in phase, at the current restart generations of the
// worker and the whole Instance.
func workerAt(ir *agentapi.InstanceReport, in *v1alpha1.Instance, w string, phase agentapi.PartPhase) bool {
	r := reportOf(ir, w)
	return r != nil && ir.Generation == restartGeneration(in, v1alpha1.PartInstance) &&
		r.Generation == restartGeneration(in, w) && r.Phase == phase
}

// desiredWorkers is the desired state's workers of an Instance of several workers: each at its own
// restart generation and socket, with its verifiedGeneration once it is verified and warmed up at its
// current generation, and its own checks while it starts, verifies or rejoins.
func (b *Backend) desiredWorkers(ctx context.Context, in *v1alpha1.Instance, md *v1alpha1.ModelDeployment) ([]agentapi.DesiredWorker, error) {
	var out []agentapi.DesiredWorker
	for _, w := range workerNames(in) {
		dw := agentapi.DesiredWorker{Name: w, Generation: restartGeneration(in, w), EngineSocket: agentapi.ContainerEngineSocket(w)}
		if workerWarmed(in, w) {
			g := restartGeneration(in, w)
			dw.VerifiedGeneration = &g
		}
		switch in.Status.Phase {
		case v1alpha1.InstanceStarting, v1alpha1.InstanceVerifying, v1alpha1.InstanceReady:
			c, err := b.workerChecks(ctx, in, md, w)
			if err != nil {
				return nil, err
			}
			dw.Checks = c
		}
		out = append(out, dw)
	}
	return out, nil
}

// workerChecks asks for what worker w's current verification generation still needs: verification
// first, with the identity record's files until the Instance is qualified, then qualification, until the
// Instance is, and warm-up, at the worker's share of the deployment's target sessions,
// ceil(targetSessions / N), with the shapes its record fixed (HBEP-7, Several workers).
func (b *Backend) workerChecks(ctx context.Context, in *v1alpha1.Instance, md *v1alpha1.ModelDeployment, w string) (*agentapi.Checks, error) {
	if workerWarmed(in, w) {
		return nil, nil
	}
	c := &agentapi.Checks{VerificationGeneration: workerVerificationGeneration(in, w)}
	qualified := isTrue(in.Status.Conditions, v1alpha1.ConditionQualified)
	if !workerVerified(in, w) {
		req := &agentapi.VerifyRequest{}
		if !qualified {
			files, err := b.identityFiles(ctx, in)
			if err != nil {
				return nil, err
			}
			req.Files = files
		}
		c.Verify = req
		return c, nil
	}
	if md == nil || md.Spec.Workload == nil {
		return nil, nil
	}
	record := workerEntry(in, w).Record
	var rec appsv1.ControllerRevision
	if err := b.Reader.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: record}, &rec); err != nil {
		return nil, agentapi.Errorf(agentapi.CodeUnavailable, "read verification record %s: %v", record, err)
	}
	var d verificationData
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil || d.Shapes == nil {
		return nil, fmt.Errorf("verification record %s holds no resolved shapes", rec.Name)
	}
	sh := resolveShapes(md)
	if !qualified {
		c.Qualify = &agentapi.QualifyRequest{Shapes: *d.Shapes, ToolCalls: sh.toolCalls}
	}
	c.WarmUp = &agentapi.WarmUpRequest{Shapes: *d.Shapes, ToolCalls: sh.toolCalls, TargetSessions: workerSessions(sh.targetSessions, in)}
	return c, nil
}

// workerSessions is one worker's share of the target sessions: ceil(targetSessions / N), so the
// workers together are checked for at least the deployment's sessions and each report stays within its
// bound (HBEP-7, Several workers).
func workerSessions(target int32, in *v1alpha1.Instance) int32 {
	n := v1alpha1.LaunchWorkers(in.Spec.Launch)
	return (target + n - 1) / n
}

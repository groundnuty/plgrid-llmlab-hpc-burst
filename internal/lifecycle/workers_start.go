package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The start of an Instance of several workers, up to its first entry to ready (workers.go says what is
// kept per worker).

// startingWorkers is starting for an Instance of several workers: each worker's verification, the
// Instance's qualification from every worker's results, each worker's warm-up, then the first entry to
// ready once every worker has passed, the tunnel and the router's probe allow it, and the router holds
// every worker healthy.
func (p *pass) startingWorkers(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) error {
	if w, signal := p.workerStartSignal(in, av, st); signal != "" {
		return p.workerMalfunction(in, alloc, w, signal, st)
	}
	ir := av.report
	if ir == nil || av.unreachable {
		return nil
	}
	if st2 := ir.Staging; st2 != nil && st2.Phase == agentapi.StagingFailed && ir.Generation == restartGeneration(in, v1alpha1.PartInstance) {
		return p.restart(in, alloc, signalStaging, v1alpha1.PartInstance, st, "staging failed: "+st2.Message)
	}
	names := workerNames(in)
	if in.Status.Phase == v1alpha1.InstanceStarting && everyWorker(names, func(w string) bool { return workerAt(ir, in, w, agentapi.PartRunning) }) {
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) { s.Phase = v1alpha1.InstanceVerifying }); err != nil {
			return err
		}
	}
	now := metav1.NewTime(p.now)
	if ok, err := p.verifyWorkers(in, alloc, ir, st); !ok || err != nil {
		return err
	}
	if !everyWorker(names, func(w string) bool { return workerVerified(in, w) }) {
		return nil
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionVerified) {
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionTrue, v1alpha1.ReasonPassed, fmt.Sprintf("each of the %d workers' verification records passed", len(names)), in.Generation, now)
		}); err != nil {
			return err
		}
	}
	sh, ok := p.shapesOf(in)
	if !ok {
		return nil
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionQualified) {
		var msgs []string
		for _, w := range names {
			c := workerResults(ir, in, w)
			if c == nil || c.Qualify == nil || !c.Qualify.Done {
				return nil
			}
			pass, msg := judgeQualify(c.Qualify, sh, p.modelName(in))
			if !pass {
				return p.failCheck(in, alloc, v1alpha1.ConditionQualified, w+": "+msg)
			}
			msgs = append(msgs, w+": "+msg)
		}
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionTrue, v1alpha1.ReasonPassed, strings.Join(msgs, "; "), in.Generation, now)
		}); err != nil {
			return err
		}
	}
	share := workerSessions(sh.targetSessions, in)
	if ok, err := p.warmWorkers(in, alloc, ir, sh); !ok || err != nil {
		return err
	}
	if !everyWorker(names, func(w string) bool { return workerWarmed(in, w) }) {
		return nil
	}
	// Warm-up gates ready: each worker's warm-up is done for its verification generation, the agent having
	// stopped its rounds once the desired state stopped asking.
	var tps []*v1alpha1.Throughput
	for _, w := range names {
		c := workerResults(ir, in, w)
		if c == nil || c.WarmUp == nil || !c.WarmUp.Done {
			return nil
		}
		_, _, tp := judgeWarmUp(c.WarmUp, share)
		tps = append(tps, tp)
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp) {
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionWarmedUp, metav1.ConditionTrue, v1alpha1.ReasonPassed, fmt.Sprintf("each of the %d workers warmed up at %d sessions", len(names), share), in.Generation, now)
			if tp := workersThroughput(tps); tp != nil {
				s.Throughput = tp
			}
		}); err != nil {
			return err
		}
	}
	// The gates of the first entry to ready, as with one worker (starting): the tunnel, then the
	// router's probe, then the router holding every worker healthy.
	if in.Status.ReadyTime == nil && !isTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
		return nil
	}
	if in.Status.ReadyTime == nil && routed(in) && !routerProbePasses(in, av) {
		return nil
	}
	if routed(in) && !routerAdmitsReady(in, av) {
		return nil
	}
	// The router holds every worker healthy: each is admitted, so its checks count from here (Q2).
	if err := p.admitWorkers(in, av); err != nil {
		return err
	}
	return p.becomeReady(in, alloc)
}

// workerResults is worker w's check results at its current verification generation, from a process at
// its current generations.
func workerResults(ir *agentapi.InstanceReport, in *v1alpha1.Instance, w string) *agentapi.CheckResults {
	r := reportOf(ir, w)
	if r == nil || !workerAt(ir, in, w, agentapi.PartRunning) || r.Checks == nil || r.Checks.VerificationGeneration != workerVerificationGeneration(in, w) {
		return nil
	}
	return r.Checks
}

// verifyWorkers judges each verification that a worker not yet verified at its current generation has
// done. ok is false once a failed verdict has acted: the Instance restarted that worker or the whole
// Instance, or ended.
func (p *pass) verifyWorkers(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, ir *agentapi.InstanceReport, st settings) (ok bool, err error) {
	for _, w := range workerNames(in) {
		if workerVerified(in, w) {
			continue
		}
		c := workerResults(ir, in, w)
		if c == nil || c.Verify == nil || !c.Verify.Done {
			continue
		}
		if err := p.workerVerifiedResult(in, alloc, w, c.Verify, st); err != nil {
			return false, err
		}
		if !workerVerified(in, w) {
			return false, nil // it failed: the Instance restarted or ended
		}
	}
	return true, nil
}

// warmWorkers judges the warm-up of each verified worker not yet warmed up, at its share of the target
// sessions: a passed one is warmed up and verified at its restart generation; a failed one fails the
// Instance, and ok is false.
func (p *pass) warmWorkers(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, ir *agentapi.InstanceReport, sh shapes) (ok bool, err error) {
	share := workerSessions(sh.targetSessions, in)
	for _, w := range workerNames(in) {
		if !workerVerified(in, w) || workerWarmed(in, w) {
			continue
		}
		c := workerResults(ir, in, w)
		if c == nil || c.WarmUp == nil {
			continue
		}
		state, msg, _ := judgeWarmUp(c.WarmUp, share)
		switch state {
		case warmUpFailed:
			return false, p.failCheck(in, alloc, v1alpha1.ConditionWarmedUp, w+": "+msg)
		case warmUpRunning:
			continue
		}
		gen := restartGeneration(in, w)
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			setWorker(s, w, func(e *v1alpha1.InstanceWorker) { e.WarmedUp, e.VerifiedGeneration = true, &gen })
		}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func everyWorker(names []string, ok func(string) bool) bool {
	for _, w := range names {
		if !ok(w) {
			return false
		}
	}
	return true
}

// workersThroughput is the Instance's throughput from its workers' warm-ups: the mean of their mean
// times, each worker having run the same number of sessions, measured at the sessions they ran
// together. Nil when a worker's warm-up gave none.
func workersThroughput(tps []*v1alpha1.Throughput) *v1alpha1.Throughput {
	if len(tps) == 0 {
		return nil
	}
	var tpot, ttft int64
	var sessions int32
	for _, tp := range tps {
		if tp == nil {
			return nil
		}
		a, errA := tp.TimePerOutputToken.AsDuration()
		b, errB := tp.TimeToFirstToken.AsDuration()
		if errA != nil || errB != nil {
			return nil
		}
		tpot += int64(a)
		ttft += int64(b)
		sessions += tp.TargetSessions
	}
	n := int64(len(tps))
	return &v1alpha1.Throughput{
		TimePerOutputToken: v1alpha1.Duration(time.Duration(tpot / n).String()),
		TimeToFirstToken:   v1alpha1.Duration(time.Duration(ttft / n).String()),
		TargetSessions:     sessions,
	}
}

// workerStartSignal returns the malfunction of an Instance of several workers that is not ready yet, and
// the worker whose rung it enters: a worker that exited at its current generations (a clean exit too;
// L-2.9), or a start past health.startTimeout from its start instruction, or from its last restart,
// taken by the worker that holds the start back (HBEP-7:469: the timeout judges that worker): the first
// not yet warmed up, else the first the current router report does not hold healthy, else worker-0
// (#498's review).
func (p *pass) workerStartSignal(in *v1alpha1.Instance, av agentView, st settings) (string, string) {
	names := workerNames(in)
	if av.report != nil && !av.unreachable {
		for _, w := range names {
			if workerAt(av.report, in, w, agentapi.PartExited) {
				return w, v1alpha1.ReasonPartExited
			}
		}
	}
	from := in.Status.StartInstructionTime
	if from == nil {
		return "", ""
	}
	start := from.Time
	if r := in.Status.Restarts; len(r) > 0 && r[len(r)-1].Time.After(start) {
		start = r[len(r)-1].Time.Time
	}
	deadline := start.Add(st.startTimeout)
	if p.now.Before(deadline) {
		p.at(deadline)
		return "", ""
	}
	for _, w := range names {
		if !workerWarmed(in, w) {
			return w, v1alpha1.ReasonStartTimeout
		}
	}
	if rr := currentRouter(in, av); rr != nil {
		healthy := map[string]bool{}
		for _, rw := range rr.Workers {
			healthy[rw.Name] = rw.Healthy
		}
		for _, w := range names {
			if !healthy[w] {
				return w, v1alpha1.ReasonStartTimeout
			}
		}
	}
	return names[0], v1alpha1.ReasonStartTimeout
}

// workerMalfunction is malfunction for an Instance of several workers: a start that times out fails an
// Instance that never was ready when nothing has restarted yet or its tunnel is down; anything else
// enters the worker rung of the worker it names.
func (p *pass) workerMalfunction(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, w, signal string, st settings) error {
	if signal == v1alpha1.ReasonStartTimeout && in.Status.ReadyTime == nil && (!anyRestart(in) || tunnelDown(in)) {
		reason, msg := startTimeoutReason(in, st)
		return p.fail(in, alloc, reason, msg)
	}
	return p.restartWorker(in, alloc, signal, w, st)
}

// anyRestart reports whether any worker, or the whole Instance, has restarted: the router's restarts
// verify nothing again, so they do not count.
func anyRestart(in *v1alpha1.Instance) bool {
	if restartGeneration(in, v1alpha1.PartInstance) > 0 {
		return true
	}
	for _, w := range workerNames(in) {
		if restartGeneration(in, w) > 0 {
			return true
		}
	}
	return false
}

// workerVerifiedResult judges worker w's verification and writes its record, as verified does for an
// Instance of one worker, and records it in the worker's entry; the Instance's status names it as the
// latest record. A failing verdict acts as with one worker, but a failed engine read restarts that worker.
func (p *pass) workerVerifiedResult(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, w string, v *agentapi.VerifyResult, st settings) error {
	gen := workerVerificationGeneration(in, w)
	name, err := v1alpha1.WorkerVerificationRecordName(in.UID, w, gen)
	if err != nil {
		return err
	}
	data, found, err := p.r.readVerification(p.ctx, in.Namespace, name)
	if err != nil {
		return err
	}
	if !found {
		sh, ok := p.shapesOf(in)
		if !ok {
			return nil
		}
		var idrec appsv1.ControllerRevision
		if err := p.r.Reader.Get(p.ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Spec.IdentityRecord}, &idrec); err != nil {
			return fmt.Errorf("read identity record %s: %w", in.Spec.IdentityRecord, err)
		}
		d, err := declaredOf(&idrec)
		if err != nil {
			return err
		}
		rehash := !isTrue(in.Status.Conditions, v1alpha1.ConditionQualified)
		data = judgeVerify(d, v, rehash, sh)
		data.Instance, data.DeploymentID, data.Generation = in.Name, string(in.Spec.DeploymentID), gen
		data.Restart = workerRestartKind(in, w)
		data.GPUType = p.gpuType(alloc)
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		rec, err := v1alpha1.NewWorkerVerificationRecord(in.Namespace, p.mdName(in), string(in.Spec.DeploymentID), in.UID, w, gen, raw)
		if err != nil {
			return err
		}
		if err := p.r.createRecord(p.ctx, rec); err != nil {
			return err
		}
	}
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.VerificationRecord, s.Verdict = name, data.Verdict
		setWorker(s, w, func(e *v1alpha1.InstanceWorker) { e.Record, e.Verdict, e.WarmedUp = name, string(data.Verdict), false })
		// A failed verdict sets the Instance's Verified False, as with one worker, before the Instance acts
		// on it (api/v1alpha1/conditions.go): a worker restart has left the condition True (#498's review).
		if data.Verdict != v1alpha1.VerdictPass {
			cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionFalse, v1alpha1.ReasonFailed, w+": "+data.Message, in.Generation, now)
		}
	}); err != nil {
		return err
	}
	if data.Verdict == v1alpha1.VerdictPass {
		return nil
	}
	return p.failedVerification(in, alloc, name, data, st,
		func() (bool, error) { return p.r.earlierWorkerRehashFailure(p.ctx, in, name) },
		func() error { return p.restartWorker(in, alloc, signalVerifyRead, w, st, data.Message) })
}

// earlierWorkerRehashFailure reports whether an earlier verification of the Instance, of any of its
// workers, failed its rehash: the workers share the staged checkpoint.
func (r *Reconciler) earlierWorkerRehashFailure(ctx context.Context, in *v1alpha1.Instance, current string) (bool, error) {
	var list appsv1.ControllerRevisionList
	if err := r.Reader.List(ctx, &list, client.InNamespace(in.Namespace), client.MatchingLabels{
		v1alpha1.LabelRecord:      v1alpha1.RecordVerification,
		v1alpha1.LabelInstanceUID: string(in.UID),
	}); err != nil {
		return false, fmt.Errorf("list verification records: %w", err)
	}
	for i := range list.Items {
		var d verificationData
		if list.Items[i].Name != current && json.Unmarshal(list.Items[i].Data.Raw, &d) == nil && d.Cause == causeRehash {
			return true, nil
		}
	}
	return false, nil
}

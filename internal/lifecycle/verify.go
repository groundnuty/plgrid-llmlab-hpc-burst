package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// starting advances a starting or verifying Instance (HBEP-7, Instance start):
// starting until its worker runs at the current generations, then verifying,
// with verification, qualification and warm-up judged from the agent's
// reports, then ready. A part that exits, or a start that outlasts
// health.startTimeout, is a malfunction.
func (p *pass) starting(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) error {
	if signal := p.startSignal(in, av, st); signal != "" {
		return p.malfunction(in, alloc, signal, st)
	}
	ir := av.report
	if ir == nil || av.unreachable {
		return nil
	}
	if in.Status.Phase == v1alpha1.InstanceStarting && workerRunning(ir, in) {
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) { s.Phase = v1alpha1.InstanceVerifying }); err != nil {
			return err
		}
	}
	c := ir.Checks
	if c == nil || c.VerificationGeneration != verificationGeneration(in) {
		return nil
	}
	now := metav1.NewTime(p.now)
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionVerified) {
		if c.Verify == nil || !c.Verify.Done {
			return nil
		}
		return p.verified(in, alloc, c.Verify, st)
	}
	sh, ok := p.shapesOf(in)
	if !ok {
		return nil
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionQualified) {
		if c.Qualify == nil || !c.Qualify.Done {
			return nil
		}
		pass, msg := judgeQualify(c.Qualify, sh, p.modelName(in))
		if !pass {
			return p.fail(in, alloc, v1alpha1.ReasonQualificationFailed, msg)
		}
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionTrue, v1alpha1.ReasonPassed, msg, in.Generation, now)
		}); err != nil {
			return err
		}
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp) {
		if c.WarmUp == nil {
			return nil
		}
		state, msg, tp := judgeWarmUp(c.WarmUp, sh.targetSessions)
		switch state {
		case warmUpFailed:
			return p.fail(in, alloc, v1alpha1.ReasonQualificationFailed, msg)
		case warmUpRunning:
			return nil
		}
		if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionWarmedUp, metav1.ConditionTrue, v1alpha1.ReasonPassed, msg, in.Generation, now)
			if tp != nil {
				s.Throughput = tp
			}
		}); err != nil {
			return err
		}
	}
	// Warm-up gates ready: the desired state stops asking for rounds once the
	// controller has judged them, and the Instance is ready only when the
	// agent reports the warm-up done for this verification generation (the
	// contract of InstanceStatus.ReadyTime).
	if c.WarmUp == nil || !c.WarmUp.Done {
		return nil
	}
	return p.becomeReady(in, alloc)
}

// startSignal returns the malfunction of an Instance that is not ready yet: a
// part that exited at the current generation (a clean exit too; L-2.9), or a
// start past health.startTimeout from its start instruction, or from its last
// restart.
func (p *pass) startSignal(in *v1alpha1.Instance, av agentView, st settings) string {
	if av.report != nil && !av.unreachable && workerExited(av.report, in) {
		return v1alpha1.ReasonPartExited
	}
	from := in.Status.StartInstructionTime
	if from == nil {
		return ""
	}
	start := from.Time
	if r := in.Status.Restarts; len(r) > 0 && r[len(r)-1].Time.After(start) {
		start = r[len(r)-1].Time.Time
	}
	deadline := start.Add(st.startTimeout)
	if p.now.Before(deadline) {
		p.at(deadline)
		return ""
	}
	return v1alpha1.ReasonStartTimeout
}

// malfunction handles a failure of an Instance that has not become ready yet:
// a first start that times out fails the Instance (HBEP-7, Instance start);
// anything else climbs the restart ladder.
func (p *pass) malfunction(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, signal string, st settings) error {
	if signal == v1alpha1.ReasonStartTimeout && in.Status.ReadyTime == nil && verificationGeneration(in) == 1 {
		return p.fail(in, alloc, v1alpha1.ReasonStartTimeout, fmt.Sprintf("not ready within %s of its start instruction", st.startTimeout))
	}
	return p.restart(in, alloc, signal, "", st)
}

// workerRunning reports whether the agent runs worker-0 at the current
// restart generations of the worker and the whole Instance.
func workerRunning(ir *agentapi.InstanceReport, in *v1alpha1.Instance) bool {
	w := worker0(ir)
	return w != nil && ir.Generation == restartGeneration(in, v1alpha1.PartInstance) &&
		w.Generation == restartGeneration(in, v1alpha1.WorkerName(0)) && w.Phase == agentapi.PartRunning
}

// workerExited reports whether worker-0 exited without being told to, at the
// current generations.
func workerExited(ir *agentapi.InstanceReport, in *v1alpha1.Instance) bool {
	w := worker0(ir)
	return w != nil && ir.Generation == restartGeneration(in, v1alpha1.PartInstance) &&
		w.Generation == restartGeneration(in, v1alpha1.WorkerName(0)) && w.Phase == agentapi.PartExited
}

func worker0(ir *agentapi.InstanceReport) *agentapi.WorkerReport {
	for i := range ir.Workers {
		if ir.Workers[i].Name == agentapi.WorkerName(0) {
			return &ir.Workers[i]
		}
	}
	return nil
}

func (p *pass) modelName(in *v1alpha1.Instance) string {
	if p.md != nil {
		return p.md.Spec.Model
	}
	return ""
}

// shapesOf returns the shapes of the Instance's deployment as declared now.
func (p *pass) shapesOf(in *v1alpha1.Instance) (shapes, bool) {
	if p.md == nil || p.md.Spec.Workload == nil || p.md.Spec.Source.Served == nil {
		return shapes{}, false
	}
	return resolveShapes(p.md), true
}

// verified judges a verification and writes its record (HBEP-7, Instance
// start: one record per verification, written before the Instance can become
// ready; an existing record is read, never rewritten).
func (p *pass) verified(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, v *agentapi.VerifyResult, st settings) error {
	gen := verificationGeneration(in)
	name, err := v1alpha1.VerificationRecordName(in.UID, gen)
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
		rehash := !isTrue(in.Status.Conditions, v1alpha1.ConditionQualified) // a worker restart keeps Qualified and skips the rehash
		data = judgeVerify(d, v, rehash, sh)
		data.Instance, data.DeploymentID, data.Generation = in.Name, string(in.Spec.DeploymentID), gen
		data.Restart = restartKind(in)
		data.GPUType = p.gpuType(alloc)
		if err := p.r.writeVerification(p.ctx, p.mdName(in), in, gen, data); err != nil {
			return err
		}
	}
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.VerificationRecord, s.Verdict = name, data.Verdict
		if data.Verdict == v1alpha1.VerdictPass {
			cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionTrue, v1alpha1.ReasonPassed, "verification record "+name+" passed", in.Generation, now)
		} else {
			cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionFalse, v1alpha1.ReasonFailed, data.Message, in.Generation, now)
		}
	}); err != nil {
		return err
	}
	if data.Verdict == v1alpha1.VerdictPass {
		return nil
	}
	switch data.Cause {
	case causeRehash:
		earlier, err := p.r.earlierRehashFailure(p.ctx, in, gen)
		if err != nil {
			return err
		}
		if !earlier {
			// Move the staged copy aside and restage: the whole-Instance rung
			// (HBEP-7, Instance start).
			return p.restart(in, alloc, signalRehash, v1alpha1.PartInstance, st)
		}
		return p.fail(in, alloc, v1alpha1.ReasonVerificationFailed, "the rehash failed again after restaging: "+data.Message)
	case causeContradiction, causeShape:
		p.r.Recorder.Event(p.mdObject(in), true, eventDegraded, v1alpha1.ReasonVerificationFailed,
			fmt.Sprintf("Instance %s: %s; provisioning in its class stops until the serving spec changes", in.Name, data.Message))
		// Stop the class in this pass too: the record stops it in every later
		// one (classBlocked), and this pass must not provision there either.
		if pv := p.pvOf(in.Labels); pv != nil && pv.hasClass && p.ids[pv.class] != nil && string(p.ids[pv.class].id) == string(in.Spec.DeploymentID) {
			p.ids[pv.class].blocked = "verification record " + name + ": " + data.Message
			p.degrade(v1alpha1.ReasonVerificationFailed, "provisioning in class %s/%s is stopped until the serving spec changes: %s", pv.class.GPUModel, pv.class.CPUArch, data.Message)
		}
	}
	return p.fail(in, alloc, v1alpha1.ReasonVerificationFailed, data.Message)
}

func restartKind(in *v1alpha1.Instance) string {
	switch c := conditionReason(in, v1alpha1.ConditionVerified); c {
	case v1alpha1.ReasonWorkerRestart:
		return "worker"
	case v1alpha1.ReasonInstanceRestart:
		return "instance"
	}
	if verificationGeneration(in) > 1 {
		return "restart"
	}
	return "start"
}

func conditionReason(in *v1alpha1.Instance, typ string) string {
	for _, c := range in.Status.Conditions {
		if c.Type == typ {
			return c.Reason
		}
	}
	return ""
}

func (p *pass) gpuType(alloc *v1alpha1.Allocation) string {
	regs := p.r.Backend.registrations(alloc.UID)
	if len(regs) == 0 || len(regs[len(regs)-1].gpus) == 0 {
		return ""
	}
	return regs[len(regs)-1].gpus[0].Name
}

func (p *pass) mdName(in *v1alpha1.Instance) string { return p.key.Name }

// mdObject is the object an event about the deployment is attached to: the
// ModelDeployment, or the Instance once the ModelDeployment is gone.
func (p *pass) mdObject(in *v1alpha1.Instance) client.Object {
	if p.md != nil {
		return p.md
	}
	return in
}

// becomeReady makes the Instance ready (HBEP-7, Instance start), records its
// first ready time and, for the first Instance of its Allocation, the class's
// start-to-ready time (HBEP-7, Handover).
func (p *pass) becomeReady(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	now := metav1.NewTime(p.now)
	first := in.Status.ReadyTime == nil
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Phase = v1alpha1.InstanceReady
		if s.ReadyTime == nil {
			s.ReadyTime = &now
		}
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionTrue, v1alpha1.ReasonHealthy, "ready", in.Generation, now)
	}); err != nil {
		return err
	}
	if first {
		p.r.Recorder.Event(in, false, eventReady, "Ready", "verified, qualified and warmed up; the front routes to it")
	}
	return nil
}

// firstOnAllocation reports whether the Instance is the first created on its
// Allocation: start-to-ready is measured only for it (HBEP-7, Handover).
func (p *pass) firstOnAllocation(in *v1alpha1.Instance) bool {
	for _, o := range p.insts {
		if o.UID != in.UID && o.Spec.AllocationRef.Name == in.Spec.AllocationRef.Name && o.CreationTimestamp.Before(&in.CreationTimestamp) {
			return false
		}
	}
	return true
}

// readVerification reads a verification record's data, if the record exists.
func (r *Reconciler) readVerification(ctx context.Context, ns, name string) (verificationData, bool, error) {
	var rec appsv1.ControllerRevision
	err := r.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &rec)
	if apierrors.IsNotFound(err) {
		return verificationData{}, false, nil
	}
	if err != nil {
		return verificationData{}, false, fmt.Errorf("read verification record %s: %w", name, err)
	}
	var d verificationData
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		return verificationData{}, false, fmt.Errorf("verification record %s: %w", name, err)
	}
	return d, true, nil
}

// writeVerification creates a verification record. One that already exists is
// left as it is (HBEP-7: read, never rewritten).
func (r *Reconciler) writeVerification(ctx context.Context, md string, in *v1alpha1.Instance, gen int64, d verificationData) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	rec, err := v1alpha1.NewVerificationRecord(in.Namespace, md, string(in.Spec.DeploymentID), in.UID, gen, raw)
	if err != nil {
		return err
	}
	if err := r.Client.Create(ctx, rec, owner); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create verification record %s: %w", rec.Name, err)
	}
	return nil
}

// earlierRehashFailure reports whether an earlier verification of the Instance
// failed its rehash.
func (r *Reconciler) earlierRehashFailure(ctx context.Context, in *v1alpha1.Instance, gen int64) (bool, error) {
	for g := int64(1); g < gen; g++ {
		name, err := v1alpha1.VerificationRecordName(in.UID, g)
		if err != nil {
			return false, err
		}
		d, ok, err := r.readVerification(ctx, in.Namespace, name)
		if err != nil {
			return false, err
		}
		if ok && d.Cause == causeRehash {
			return true, nil
		}
	}
	return false, nil
}

// classBlocked returns why provisioning in a class is stopped: a verification
// of its current ID failed on a settings contradiction or a shape that does not
// fit. The records outlive everything else, so the stop survives a restart,
// and a new serving spec, with a new ID, lifts it.
func (r *Reconciler) classBlocked(ctx context.Context, md *v1alpha1.ModelDeployment, id v1alpha1.FullDeploymentID) (string, error) {
	var list appsv1.ControllerRevisionList
	if err := r.Reader.List(ctx, &list, client.InNamespace(md.Namespace), client.MatchingLabels{
		v1alpha1.LabelRecord:          v1alpha1.RecordVerification,
		v1alpha1.LabelDeploymentID:    id.Display(),
		v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name),
	}); err != nil {
		return "", fmt.Errorf("list verification records: %w", err)
	}
	for i := range list.Items {
		var d verificationData
		if json.Unmarshal(list.Items[i].Data.Raw, &d) != nil || d.DeploymentID != string(id) {
			continue
		}
		if d.Verdict == v1alpha1.VerdictFail && (d.Cause == causeContradiction || d.Cause == causeShape) {
			return fmt.Sprintf("verification record %s: %s", list.Items[i].Name, d.Message), nil
		}
	}
	return "", nil
}

var _ = time.Second

package lifecycle

// A committed target that a FAILURE releases first keeps the failure's own release cause, and the report names the failure
// (HBEP-10, The release cause and The report, as amended by the science agent's ruling on #664, 17 Nov 2026). The gate
// (HBEP-7, The throughput gate) fails an Instance that misses twice and releases its Allocation at once; when the Operation
// committed that Instance (EndStarting) and its Allocation (ReleaseStarted) and cannot act yet (its spec write keeps
// failing), the Allocation is released by the gate's failure, not by the Operation.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// newGateExecWorld is a deployment on A of two Instances on started jobs, the fixture's of the gate (gateMD, an entry of
// 20 ms per output token and 400 ms to first token, so the limits are 40 ms and 800 ms): R, whose engine never comes up, and
// X, newer, whose engine takes eight minutes and then measures what the knob says, twice (the first miss restarts its worker,
// the retry decides). The Allocations are released the moment they are empty (retention), so a Scale to one takes X as
// EndStarting and plans its Allocation as ReleaseStarted.
func newGateExecWorld(t *testing.T, knob *gateKnob) (w *execWorld, r, x string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	started := 0
	h.engine = func(string) afake.Engine {
		started++
		if started == 2 {
			return afake.Engine{StartDelay: 8 * time.Minute, WarmUpRound: gateEngineOf(knob)("").WarmUpRound}
		}
		return afake.Engine{StartDelay: 6 * time.Hour}
	}
	h.batch.Enqueue(pfake.Script{}, pfake.Script{})
	h.Step()
	md := gateMD(func(m *v1alpha1.ModelDeployment) {
		noOverAllocation(m)
		m.Spec.Instances = 1
		m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	})
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	jobStarted := func(in v1alpha1.Instance) bool {
		a, ok := h.allocation(in.Spec.AllocationRef.Name)
		return ok && a.Status.AtProvider.StartTime != nil
	}
	h.until(10*time.Minute, 10*time.Second, "R on a started job", func() bool {
		live := h.liveInstances(hoMD)
		return len(live) == 1 && jobStarted(live[0])
	})
	rIn := h.liveInstances(hoMD)[0]
	r = rIn.Name
	// The entry, before any warm-up has run (R's engine never comes up): both Instances share the deployment's ID.
	id := rIn.Spec.DeploymentID.Display()
	var prof v1alpha1.PlatformProfile
	if err := h.c.Get(h.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = oneEntry()(id)
	if err := h.c.Update(h.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	h.run(time.Minute, 10*time.Second) // X is made later than R, so that a Scale to one takes X
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	h.until(10*time.Minute, 10*time.Second, "X on a started job", func() bool {
		live := h.liveInstances(hoMD)
		return len(live) == 2 && jobStarted(live[0]) && jobStarted(live[1])
	})
	for _, in := range h.liveInstances(hoMD) {
		if in.Name != r {
			x = in.Name
			if got := in.Spec.DeploymentID.Display(); got != id {
				t.Fatalf("X's deployment ID is %q, R's %q: the entry would not match both", got, id)
			}
		}
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, r, x
}

// A Scale to one commits to end X (EndStarting) and release its Allocation (ReleaseStarted); R is kept. The spec write keeps
// failing, so the Operation cannot act. X's engine comes up and misses the throughput gate twice (41 ms per output token
// against a limit of 40 ms): the gate fails it with QualificationFailed and releases its Allocation at once. The Allocation's
// release cause is the gate's, "Instance <X> failed: QualificationFailed", and does NOT parse as an Operation's. The Allocation
// target reports Done, since it is gone, and its outcome names the failure that released it. When the write lands the
// Operation is Done, R untouched.
//
// JobEnded is raised by the Provider's controller (internal/providerctl), which reads the release cause with
// ParseReleaseCauseOperation to put an Operation in `related` (HBEP-8, Allocation events). The rig's Provider is
// pkg/provider/fake, which raises no such event, so the row asserts what the event reads: the gate's cause does not parse as an
// Operation's, so JobEnded's related would be empty and its note would carry the failure's own cause. providerctl/e2e
// (events_delete_test.go) shows the event for a cause that is not the Operation's form, with no related object.
func TestAGateMissReleasesACommittedTargetsAllocationWithTheGatesCauseAndTheTargetIsDoneNamingIt(t *testing.T) {
	ms := time.Millisecond
	knob := &gateKnob{tpot: 41 * ms, ttft: 300 * ms}
	w, r, x := newGateExecWorld(t, knob)
	xAlloc := w.mustInstance(x).Spec.AllocationRef.Name
	stop := w.failSpecWrites()
	planned := w.approveScale("scale", 1, 2)
	pt := planTargets(planned.Status.Plan)
	if pt["Instance "+x] != v1alpha1.PlanEffectEndStarting || pt["Allocation "+xAlloc] != v1alpha1.PlanEffectReleaseStarted {
		t.Fatalf("the plan takes X as %q and its Allocation as %q: %v", pt["Instance "+x], pt["Allocation "+xAlloc], pt)
	}
	if in := w.instance(x); ended(in) || len(in.Status.Restarts) != 0 {
		t.Fatalf("X is %q with restarts %v at the commit: the gate has not judged it yet", in.Status.Phase, in.Status.Restarts)
	}

	// X misses twice: the first miss restarts its worker, the retry fails it, and its Allocation goes.
	w.stepsUntil(40*time.Minute, 10*time.Second, "X failed by the gate", func() bool { return w.instance(x).Status.EndReason == v1alpha1.ReasonQualificationFailed })
	if g := w.instance(x).Status.Gate; g == nil || g.Verdict != v1alpha1.GateMissed || len(g.Misses) != 2 {
		t.Fatalf("X's gate status %+v, want the verdict Missed with both misses", g)
	}
	w.stepsUntil(10*time.Minute, 10*time.Second, "X's Allocation gone", func() bool { _, ok := w.allocation(xAlloc); return !ok })

	cause, deleted := w.log.deletedWith(xAlloc)
	if !deleted || cause != "Instance "+x+" failed: QualificationFailed" {
		t.Errorf("X's Allocation was deleted (%v) with the cause %q, want the gate's: Instance %s failed: QualificationFailed", deleted, cause, x)
	}
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause %q parses as Operation %s/%s %s: the gate released it, not the Operation", cause, ns, name, uid)
	}
	if independentCause.MatchString(cause) {
		t.Errorf("the cause %q reads as an Operation's", cause)
	}

	// While the write still fails the Operation is Executing with both targets Done, the Allocation's naming the failure.
	w.stepsUntil(2*time.Minute, 10*time.Second, "both targets reported Done", func() bool {
		a, okA := outcomeOf(w.getOp("scale"), xAlloc)
		i, okI := outcomeOf(w.getOp("scale"), x)
		return okA && okI && a.State == v1alpha1.TargetStateDone && i.State == v1alpha1.TargetStateDone
	})
	op := w.getOp("scale")
	if o, _ := outcomeOf(op, xAlloc); o.Reason != "" || !strings.HasPrefix(o.Message, "released by "+v1alpha1.ReasonQualificationFailed) || !strings.Contains(o.Message, x) {
		t.Errorf("X's Allocation outcome is %+v, want Done and a message that begins \"released by %s\" and names Instance %s", o, v1alpha1.ReasonQualificationFailed, x)
	}
	if op.Status.Phase != v1alpha1.OperationPhaseExecuting || op.Status.FinishedAt != nil {
		t.Errorf("the Operation is %q with finishedAt %v while its spec write fails, want Executing", op.Status.Phase, op.Status.FinishedAt)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d, want 2 while the write fails", got)
	}

	// The write lands: the Operation is Done, the message stays, R is untouched.
	stop()
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	op = w.getOp("scale")
	if o, _ := outcomeOf(op, xAlloc); o.State != v1alpha1.TargetStateDone || !strings.HasPrefix(o.Message, "released by "+v1alpha1.ReasonQualificationFailed) {
		t.Errorf("after the write X's Allocation outcome is %+v, want Done, still naming the failure", o)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}
	if in := w.instance(r); isDraining(in) || ended(in) {
		t.Errorf("R is %q: the kept Instance is untouched", in.Status.Phase)
	}
	if cause, _ := w.log.deletedWith(xAlloc); cause != "Instance "+x+" failed: QualificationFailed" {
		t.Errorf("X's Allocation's cause is %q at the end", cause)
	}
}

// The edge of the failure's cause: a failure owes an Allocation its release, and the failure's own release write FAULTS in the
// very pass in which the Operations step acts. A Scale's committed EndStarting target X fails at the throughput gate while the
// Operation cannot act (its spec write fails), and its Allocation, ReleaseStarted, is not released: the failure's release write
// faults at every pass, and so does retention's (the same Allocation, empty and idle, is retention's to release at once), so that
// the only release that can succeed is the one under test. Then the spec write is let through and the failure's release faults
// once more, in the pass whose Operations step then acts on the Allocation: it leaves it to the failure (releasePending), and the
// next pass's tombstone releases it with the failure's own cause. Without that, the Operation's act deleted it in that pass with
// the Operation's cause.
func TestAFailureThatOwesAnAllocationItsReleaseKeepsItWhenItsOwnWriteFaultsInThePassTheOperationActs(t *testing.T) {
	ms := time.Millisecond
	knob := &gateKnob{tpot: 41 * ms, ttft: 300 * ms}
	w, r, x := newGateExecWorld(t, knob)
	xAlloc, rAlloc := w.mustInstance(x).Spec.AllocationRef.Name, w.mustInstance(r).Spec.AllocationRef.Name
	specFails, releaseFaults := true, 1<<30
	w.fc.set(func(method string, obj client.Object) error {
		switch o := obj.(type) {
		case *v1alpha1.ModelDeployment:
			if method == "Update" && specFails {
				return errors.New("injected: the write of the spec fails")
			}
		case *v1alpha1.Allocation:
			cause := o.Annotations[v1alpha1.AnnotationReleaseCause]
			if method != "Update" || o.Name != xAlloc || cause == "" {
				return nil
			}
			switch {
			case strings.HasPrefix(cause, "Instance "): // the failure's own release
				if releaseFaults > 0 {
					releaseFaults--
					return errors.New("injected: the write of the failure's release cause fails")
				}
			case strings.HasPrefix(cause, "released when idle"): // retention's, which is held off throughout
				return errors.New("injected: the write of retention's release cause fails")
			}
		}
		return nil
	}, nil)
	planned := w.approveScale("scale", 1, 2)
	if pt := planTargets(planned.Status.Plan); pt["Instance "+x] != v1alpha1.PlanEffectEndStarting || pt["Allocation "+xAlloc] != v1alpha1.PlanEffectReleaseStarted {
		t.Fatalf("the plan takes X as %q and its Allocation as %q, want EndStarting and ReleaseStarted: %v", pt["Instance "+x], pt["Allocation "+xAlloc], pt)
	}

	// X fails at the gate with the Operation unable to act, and its Allocation stays: the failure's release faults at every pass.
	w.stepsUntil(40*time.Minute, 10*time.Second, "X failed by the gate", func() bool { return w.instance(x).Status.EndReason == v1alpha1.ReasonQualificationFailed })
	w.stepsUntil(time.Minute, 10*time.Second, "the failure's release to have faulted", func() bool { return releaseFaults < 1<<30 })
	if a, ok := w.allocation(xAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("X's Allocation is %+v (found %v), want it present and not being deleted: the failure's release write faults", a, ok)
	}
	if _, deleted := w.log.deletedWith(xAlloc); deleted {
		t.Fatal("X's Allocation was deleted while the failure's release write faults")
	}

	// The spec write goes through, and the failure's release faults once more: in the pass whose Operations step acts.
	specFails, releaseFaults = false, 1
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("the pass in which the failure's release faults returned %v, want the injected fault", err)
	}
	if releaseFaults != 0 {
		t.Fatalf("the failure's release did not fault in that pass (%d left)", releaseFaults)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Fatalf("spec.instances is %d after that pass, want 1: the spec write must have landed in it", got)
	}

	// It settles: the next pass's tombstone releases the Allocation with the failure's own cause, never the Operation's.
	w.stepsUntil(10*time.Minute, 10*time.Second, "X's Allocation gone", func() bool { _, ok := w.allocation(xAlloc); return !ok })
	cause, deleted := w.log.deletedWith(xAlloc)
	if !deleted || cause != "Instance "+x+" failed: QualificationFailed" {
		t.Errorf("X's Allocation was deleted (%v) with the cause %q, want the failure's: Instance %s failed: QualificationFailed", deleted, cause, x)
	}
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause %q parses as Operation %s/%s %s: the failure owed the release, not the Operation", cause, ns, name, uid)
	}
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	op := w.getOp("scale")
	if o, ok := outcomeOf(op, xAlloc); !ok || o.State != v1alpha1.TargetStateDone || !strings.HasPrefix(o.Message, "released by "+v1alpha1.ReasonQualificationFailed) {
		t.Errorf("X's Allocation outcome is %+v (%v), want Done naming %s", o, ok, v1alpha1.ReasonQualificationFailed)
	}
	// Nothing else is released.
	if a, ok := w.allocation(rAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Errorf("R's Allocation is %+v (found %v): the kept Instance's Allocation is untouched", a, ok)
	}
	if in := w.instance(r); isDraining(in) || ended(in) {
		t.Errorf("R is %q: the kept Instance is untouched", in.Status.Phase)
	}
	if cause, deleted := w.log.deletedWith(rAlloc); deleted {
		t.Errorf("R's Allocation was deleted with the cause %q", cause)
	}
}

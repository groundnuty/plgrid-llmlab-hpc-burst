package lifecycle

// The Operations step on a fake client (#589, PR 4): the phases it writes, when it writes, the frozen plan,
// the backstop and the count of Approvals. The step runs on a pass whose state the tests set by hand, with
// the times of opfixture_test.go; the Operation objects, the policies and the Approvals are in the client.
// The control plane's own part (admission, the conflict) is in operations_envtest_test.go.

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// opRig runs the Operations step over a fixture.
type opRig struct {
	t   *testing.T
	f   *opFixture
	c   client.Client
	r   *Reconciler
	ctx context.Context
	// stepped is the pass of the last step.
	stepped *pass
}

func newOpRig(t *testing.T, f *opFixture, ops ...*v1alpha1.Operation) *opRig {
	t.Helper()
	scheme, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	b := clientfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.Operation{}, &v1alpha1.ModelDeployment{}, &v1alpha1.Instance{}, &v1alpha1.Allocation{})
	for _, op := range ops {
		b = b.WithObjects(op)
	}
	for i := range f.policies {
		b = b.WithObjects(&f.policies[i])
	}
	for _, a := range f.allocs {
		b = b.WithObjects(a.DeepCopy())
	}
	// What an Operation writes when it executes (the spec, an Instance, an Allocation) is in the client too, and the
	// fixture's objects carry the resourceVersion the client gave them, as objects the pass read do.
	if f.md != nil {
		b = b.WithObjects(f.md.DeepCopy())
	}
	for _, in := range f.insts {
		b = b.WithObjects(in.DeepCopy())
	}
	c := b.Build()
	g := &opRig{t: t, f: f, c: c, r: &Reconciler{Client: c, Reader: c, Clock: SystemClock{}, Backend: &Backend{}, Recorder: &MemoryRecorder{}}, ctx: context.Background()}
	g.syncVersions()
	return g
}

// syncVersions copies the stored resourceVersions into the fixture's objects.
func (g *opRig) syncVersions() {
	g.t.Helper()
	get := func(key types.NamespacedName, obj client.Object) {
		if err := g.c.Get(g.ctx, key, obj); err != nil {
			g.t.Fatal(err)
		}
	}
	if g.f.md != nil {
		var md v1alpha1.ModelDeployment
		get(types.NamespacedName{Namespace: opNS, Name: opMD}, &md)
		g.f.md.ResourceVersion = md.ResourceVersion
	}
	for _, in := range g.f.insts {
		var cur v1alpha1.Instance
		get(types.NamespacedName{Namespace: opNS, Name: in.Name}, &cur)
		in.ResourceVersion = cur.ResourceVersion
	}
	for name, a := range g.f.allocs {
		var cur v1alpha1.Allocation
		get(types.NamespacedName{Namespace: opNS, Name: name}, &cur)
		a.ResourceVersion = cur.ResourceVersion
	}
}

// events are the events the step raised.
func (g *opRig) events() []RecordedEvent { return g.r.Recorder.(*MemoryRecorder).Events() }

// step runs the step once at now, as a pass over the fixture's current state.
func (g *opRig) step(now time.Time) {
	g.t.Helper()
	p := g.f.pass()
	p.r, p.ctx, p.now = g.r, g.ctx, now
	p.mem = g.r.memory()
	if err := p.operations(); err != nil {
		g.t.Fatalf("the Operations step failed: %v", err)
	}
	// The spec the step wrote is the fixture's from here on, as the next pass reads it, and an Allocation that is gone
	// is no longer listed.
	g.f.md = p.md
	g.stepped = p
	for name := range g.f.allocs {
		var cur v1alpha1.Allocation
		if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: name}, &cur); apierrors.IsNotFound(err) {
			delete(g.f.allocs, name)
		}
	}
}

func (g *opRig) op(name string) *v1alpha1.Operation {
	g.t.Helper()
	var op v1alpha1.Operation
	if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: name}, &op); err != nil {
		g.t.Fatal(err)
	}
	return &op
}

func (g *opRig) confirm(name, planID string) {
	g.t.Helper()
	op := g.op(name)
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
	if err := g.c.Update(g.ctx, op); err != nil {
		g.t.Fatal(err)
	}
}

func (g *opRig) approve(user, planID string, opUID types.UID) {
	g.t.Helper()
	ap := approvalBy(user, planID, opUID, opNS)
	if err := g.c.Create(g.ctx, ap); err != nil {
		g.t.Fatal(err)
	}
}

func opNamed(name string, op *v1alpha1.Operation) *v1alpha1.Operation {
	op.Name, op.UID = name, types.UID("uid-"+name)
	return op
}

func condOf(op *v1alpha1.Operation, typ string) *metav1.Condition {
	return apimeta.FindStatusCondition(op.Status.Conditions, typ)
}

// The first pass writes Planned with the plan; the next passes, however much time passes, write nothing,
// because costs are snapshots at the plan's observedAt.
func TestANewOperationIsPlannedAndTimeAloneWritesNothing(t *testing.T) {
	f := idFixture(t)
	g := newOpRig(t, f, opNamed("scale", scaleOp(1)))
	g.step(opAt)
	op := g.op("scale")
	if op.Status.Phase != v1alpha1.OperationPhasePlanned || op.Status.Plan == nil {
		t.Fatalf("phase %q, plan %v: want Planned with a plan", op.Status.Phase, op.Status.Plan)
	}
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonNotConfirmed {
		t.Errorf("Executable %+v, want False/NotConfirmed", c)
	}
	if !op.Status.Plan.ObservedAt.Time.Equal(opAt) || op.Status.ObservedGeneration != op.Generation {
		t.Errorf("observedAt %v, observedGeneration %d", op.Status.Plan.ObservedAt, op.Status.ObservedGeneration)
	}
	rv := op.ResourceVersion
	for _, later := range []time.Duration{time.Minute, 30 * time.Minute, 3 * time.Hour} {
		g.step(opAt.Add(later))
		if got := g.op("scale").ResourceVersion; got != rv {
			t.Fatalf("a pass %s later wrote the Operation (resourceVersion %s -> %s): time passing alone writes nothing", later, rv, got)
		}
	}
	// The stored figure is the snapshot at the plan's time, not recomputed from the clock: the pending Allocation
	// was submitted at 9:10 and observed at 10:00.
	p := op.Status.Plan.Targets[0]
	if p.Cost == nil || p.Cost.QueueAge == nil || p.Cost.QueueAge.Duration.Duration != 50*time.Minute {
		t.Errorf("the queue age is %+v, want the 50m of the snapshot", p.Cost)
	}
}

// While it waits for its confirm, a plan is written again when its ID changes or a cost input changes, and
// not for an input that only ages.
func TestAPlannedOperationIsWrittenOnlyWhenTheIDOrACostInputChanges(t *testing.T) {
	f := idFixture(t)
	g := newOpRig(t, f, opNamed("scale", scaleOp(1)))
	g.step(opAt)
	first := g.op("scale")

	// The Provider's status is read again with no new wait: its read time moves, the figures do not.
	f.pvs["sano"].prov.Status.LastSuccessfulStatusRead = opTime(opAt.Add(time.Minute))
	g.step(opAt.Add(time.Minute))
	if g.op("scale").ResourceVersion != first.ResourceVersion {
		t.Error("a read of the Provider's status with no new wait wrote the plan")
	}
	// The pending Allocation's own start estimate moves with every scheduler pass: no write.
	f.allocs["alloc-p"].Status.AtProvider.StartEstimate = opTime(opAt.Add(time.Hour))
	g.step(opAt.Add(2 * time.Minute))
	if g.op("scale").ResourceVersion != first.ResourceVersion {
		t.Error("a moved start estimate wrote the plan")
	}

	// A new queue wait is a cost input: the plan is written, its ID is the same, and the snapshot is the new time.
	f.pvs["sano"].prov.Status.QueueWaits = waitsOf(5, 9)
	g.step(opAt.Add(3 * time.Minute))
	second := g.op("scale")
	if second.ResourceVersion == first.ResourceVersion {
		t.Fatal("a new queue wait did not write the plan")
	}
	if second.Status.Plan.ID != first.Status.Plan.ID {
		t.Errorf("a cost input moved the ID: %s -> %s", first.Status.Plan.ID, second.Status.Plan.ID)
	}
	if q := second.Status.Plan.Targets[0].Cost.QueueWaits; q.Count != 2 || !second.Status.Plan.ObservedAt.Time.Equal(opAt.Add(3*time.Minute)) {
		t.Errorf("the new plan holds %+v at %v, want two waits at the new time", q, second.Status.Plan.ObservedAt)
	}

	// A pending Allocation starts: the plan's ID changes, and the plan is written again.
	a := f.allocs["alloc-p"]
	a.Status.AtProvider.Phase, a.Status.AtProvider.StartTime, a.Status.AtProvider.HardEnd = v1alpha1.AllocationRunning, opTime(opT0), opTime(opHardEnd)
	g.step(opAt.Add(4 * time.Minute))
	third := g.op("scale")
	if third.Status.Plan.ID == second.Status.Plan.ID || third.ResourceVersion == second.ResourceVersion {
		t.Errorf("a started Allocation did not change the plan: ID %s -> %s", second.Status.Plan.ID, third.Status.Plan.ID)
	}
	if got := third.Status.Plan.Targets[len(third.Status.Plan.Targets)-1].Effect; got != v1alpha1.PlanEffectEndStarting {
		t.Errorf("the started Allocation's Instance is now %s, want EndStarting", got)
	}
}

// A request that cannot be planned is Refused, terminal, and never touched again.
func TestARefusedOperationIsTerminal(t *testing.T) {
	f := idFixture(t)
	g := newOpRig(t, f, opNamed("raise", scaleOp(2)), opNamed("gone", scaleOp(1)))
	g.step(opAt)
	op := g.op("raise")
	if op.Status.Phase != v1alpha1.OperationPhaseRefused || op.Status.FinishedAt == nil {
		t.Fatalf("phase %q, finishedAt %v: want Refused with a finish time", op.Status.Phase, op.Status.FinishedAt)
	}
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonRefused || !strings.Contains(c.Message, "a Scale lowers the count") {
		t.Errorf("Executable %+v, want False/Refused naming the rule", c)
	}
	rv := op.ResourceVersion
	g.step(opAt.Add(time.Hour))
	if g.op("raise").ResourceVersion != rv {
		t.Error("a Refused Operation was written again")
	}
	// A ModelDeployment that is gone refuses its Operations too: the step runs without one.
	f.md = nil
	p := f.pass()
	p.md, p.r, p.ctx, p.now = nil, g.r, g.ctx, opAt.Add(2*time.Hour)
	if err := p.operations(); err != nil {
		t.Fatal(err)
	}
	if got := g.op("gone"); got.Status.Phase != v1alpha1.OperationPhaseRefused || !strings.Contains(condOf(got, v1alpha1.ConditionExecutable).Message, "does not exist") {
		t.Errorf("an Operation on a missing ModelDeployment: %q, %+v", got.Status.Phase, condOf(got, v1alpha1.ConditionExecutable))
	}
}

// Only the Operations that name this ModelDeployment are the pass's.
func TestAPassTouchesOnlyItsOwnDeploymentsOperations(t *testing.T) {
	f := idFixture(t)
	other := opNamed("other", scaleOp(1))
	other.Spec.ModelDeploymentRef.Name = "another-md"
	g := newOpRig(t, f, opNamed("mine", scaleOp(1)), other)
	g.step(opAt)
	if got := g.op("mine").Status.Phase; got != v1alpha1.OperationPhasePlanned {
		t.Errorf("its own Operation is %q", got)
	}
	if got := g.op("other"); got.Status.Phase != "" || got.Status.Plan != nil {
		t.Errorf("another deployment's Operation was touched: %q", got.Status.Phase)
	}
}

// A confirmed Operation's plan is frozen: nothing rewrites status.plan, whatever the state does, while the
// Operation waits for an approval. Here the drains need one: both Allocations are released when idle.
func TestAConfirmedPlanIsFrozenWhileItWaitsForAnApproval(t *testing.T) {
	idle := &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}
	f := newOpFixture(t, 2)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), retention: idle})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute, retention: idle})
	g := newOpRig(t, f, opNamed("scale", scaleOp(1)))
	g.step(opAt)
	planned := g.op("scale")
	if n := countNeeding(planned.Status.Plan); n == 0 {
		t.Fatal("no target needs approval; this fixture's drain of an Allocation released when idle needs one")
	}
	g.confirm("scale", planned.Status.Plan.ID)
	g.step(opAt.Add(time.Minute))
	op := g.op("scale")
	if op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval {
		t.Errorf("phase %q, want AwaitingApproval", op.Status.Phase)
	}
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonAwaitingApproval {
		t.Errorf("Executable %+v, want False/AwaitingApproval", c)
	}
	// The state changes after the confirm: the plan stays as the owner confirmed it.
	f.allocs["alloc-r2"].Status.AtProvider.HardEnd = opTime(opHardEnd.Add(-time.Hour))
	f.insts[1].Status.Phase = v1alpha1.InstanceDraining
	f.md.Generation++
	before := g.op("scale")
	g.step(opAt.Add(2 * time.Minute))
	after := g.op("scale")
	if after.Status.Plan.ID != before.Status.Plan.ID || after.ResourceVersion != before.ResourceVersion {
		t.Errorf("a confirmed Operation was written again: plan %s -> %s, resourceVersion %s -> %s", before.Status.Plan.ID, after.Status.Plan.ID, before.ResourceVersion, after.ResourceVersion)
	}
}

func countNeeding(p *v1alpha1.OperationPlan) int {
	n := 0
	for _, t := range p.Targets {
		if t.NeedsApproval {
			n++
		}
	}
	return n
}

// An Operation that is never confirmed never leaves Planned, however many Approvals, policy changes and
// passes there are.
func TestAnOperationNeverConfirmedNeverLeavesPlanned(t *testing.T) {
	f := idFixture(t)
	g := newOpRig(t, f, opNamed("scale", scaleOp(1)))
	g.step(opAt)
	id := g.op("scale").Status.Plan.ID
	g.approve(userPerson, id, "uid-scale")
	for i := 1; i <= 5; i++ {
		f.policies[0].Generation++
		g.step(opAt.Add(time.Duration(i) * time.Minute))
		if got := g.op("scale").Status.Phase; got != v1alpha1.OperationPhasePlanned {
			t.Fatalf("pass %d: phase %q, want Planned for an Operation never confirmed", i, got)
		}
	}
	if g.op("scale").Spec.Confirm != nil {
		t.Fatal("the controller confirmed an Operation")
	}
}

// The backstop: an Operation found confirmed with no plan, with an ID that is not the plan's, or with a plan
// whose ID this controller did not write, is Refused and never runs.
func TestTheBackstopRefusesAConfirmThatFollowsNoWrittenPlan(t *testing.T) {
	forged := func(id string) *v1alpha1.OperationPlan {
		return &v1alpha1.OperationPlan{
			ID: id, ObservedAt: metav1.NewTime(opT0), ModelDeployment: v1alpha1.PlannedDeployment{Name: opMD, UID: "uid-md", Generation: 3},
			Targets: []v1alpha1.PlanTarget{{Kind: tInst, Name: "p", UID: "uid-p", Effect: v1alpha1.PlanEffectCancelPending, NeedsApproval: false}},
		}
	}
	for _, tc := range []struct {
		name    string
		plan    *v1alpha1.OperationPlan // written to status by the admin; nil for none
		confirm string
	}{
		{"no plan in status", nil, "some-id"},
		{"an ID that is not the plan's", forged("0123456789abcdef"), "fedcba9876543210"},
		// The admin writes status with an ID of its own and the owner confirms that same ID: the two agree, and
		// the ID is still not the hash of the plan it sits on, so it is not one this controller wrote.
		{"an ID this controller never wrote, confirmed as it stands", forged("0123456789abcdef"), "0123456789abcdef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := idFixture(t)
			op := opNamed("scale", scaleOp(1))
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: tc.confirm}
			op.Status.Plan = tc.plan
			if tc.plan != nil {
				op.Status.Phase = v1alpha1.OperationPhasePlanned
			}
			g := newOpRig(t, f, op)
			g.step(opAt)
			got := g.op("scale")
			if got.Status.Phase != v1alpha1.OperationPhaseRefused {
				t.Fatalf("phase %q, want Refused", got.Status.Phase)
			}
			if c := condOf(got, v1alpha1.ConditionExecutable); c == nil || c.Reason != v1alpha1.ReasonRefused || !strings.Contains(c.Message, "confirmed before a plan was written, or with an ID this controller never wrote") {
				t.Errorf("Executable %+v", c)
			}
		})
	}
}

// With a target that needs approval, a confirmed Operation waits in AwaitingApproval, saying which target
// lacks which policy's approval, and counts each Approval as the policies name its approver.
func TestAwaitingApprovalCountsTheApprovals(t *testing.T) {
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}
	f := newOpFixture(t, 2)
	f.policies = []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), globalPolicy(person)}
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	op := opNamed("scale", scaleOp(1)) // owner agent-a requests; the cancel of a pending Allocation needs approval
	g := newOpRig(t, f, op)
	g.step(opAt)
	planned := g.op("scale")
	id := planned.Status.Plan.ID
	g.confirm("scale", id)

	state := func() (v1alpha1.OperationPhase, *metav1.Condition) {
		g.step(opAt.Add(time.Minute))
		got := g.op("scale")
		return got.Status.Phase, condOf(got, v1alpha1.ConditionExecutable)
	}
	phase, c := state()
	if phase != v1alpha1.OperationPhaseAwaitingApproval || c.Reason != v1alpha1.ReasonAwaitingApproval ||
		!strings.Contains(c.Message, "Allocation alloc-p: needs an approval under provider.sano") || !strings.Contains(c.Message, "Instance p: needs an approval under provider.sano") {
		t.Fatalf("phase %q, Executable %+v: want AwaitingApproval naming both targets and the Provider policy", phase, c)
	}
	// B is outside every policy: its Approval, for the right plan, counts for nothing.
	g.approve(userAgentB, id, "uid-scale")
	if phase, c = state(); c.Reason != v1alpha1.ReasonAwaitingApproval {
		t.Fatalf("B's Approval counted: %q %+v", phase, c)
	}
	// An approval of A for ANOTHER plan ID, and one for another Operation's UID, count for nothing either.
	g.approve(userAgentA+"-x", id, "uid-scale") // a different identity of the same shape
	if phase, c = state(); c.Reason != v1alpha1.ReasonAwaitingApproval {
		t.Fatalf("an unnamed approver counted: %q %+v", phase, c)
	}
	wrongPlan := approvalBy(userAgentA, "another-plan", "uid-scale", opNS)
	wrongPlan.Name = "wrong-plan"
	wrongOp := approvalBy(userAgentA, id, "uid-another-op", opNS)
	wrongOp.Name = "wrong-op"
	for _, ap := range []*v1alpha1.Approval{wrongPlan, wrongOp} {
		if err := g.c.Create(g.ctx, ap); err != nil {
			t.Fatal(err)
		}
	}
	if phase, c = state(); c.Reason != v1alpha1.ReasonAwaitingApproval {
		t.Fatalf("an Approval for another plan or Operation counted: %q %+v", phase, c)
	}
	// A's own Approval, under the Provider policy that names A, counts on Sano: every target is on Sano.
	g.approve(userAgentA, id, "uid-scale")
	phase, c = state()
	if phase != v1alpha1.OperationPhaseExecuting || c.Reason != v1alpha1.ReasonExecuting {
		t.Errorf("with A's approval: phase %q, Executable %+v, want every approval in, so committed: Executing", phase, c)
	}
	if got := condOf(g.op("scale"), v1alpha1.ConditionNoApprovalPolicy); got != nil {
		t.Errorf("NoApprovalPolicy %+v is set although policies apply", got)
	}
}

// No policy at all: a target that needs approval waits, with the condition NoApprovalPolicy. Two policies of
// one scope: unapprovable, naming both.
func TestNoPolicyAndDuplicatePolicies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policies []v1alpha1.ApprovalPolicy
		reason   string
		message  string
	}{
		{"no policy at all", nil, v1alpha1.ReasonNoApprovalPolicy, "no ApprovalPolicy applies"},
		{"two policies of one scope", []v1alpha1.ApprovalPolicy{namespacePolicy(opNS, person), func() v1alpha1.ApprovalPolicy {
			d := namespacePolicy(opNS, person)
			d.Name = "a-hand-made-name"
			return d
		}()}, v1alpha1.ReasonDuplicateApprovalPolicy, "ApprovalPolicies a-hand-made-name and namespace.ns name one scope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := idFixture(t)
			f.policies = tc.policies
			g := newOpRig(t, f, opNamed("scale", scaleOp(1)))
			g.step(opAt)
			planned := g.op("scale")
			if c := condOf(planned, v1alpha1.ConditionNoApprovalPolicy); c == nil || c.Status != metav1.ConditionTrue || c.Reason != tc.reason || !strings.Contains(c.Message, tc.message) {
				t.Fatalf("while planned: NoApprovalPolicy %+v, want True/%s naming %q", c, tc.reason, tc.message)
			}
			g.confirm("scale", planned.Status.Plan.ID)
			g.approve(userPerson, planned.Status.Plan.ID, "uid-scale")
			g.step(opAt.Add(time.Minute))
			got := g.op("scale")
			if got.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || condOf(got, v1alpha1.ConditionExecutable).Reason != v1alpha1.ReasonAwaitingApproval {
				t.Errorf("phase %q, Executable %+v: the person's Approval cannot satisfy a target that no policy covers", got.Status.Phase, condOf(got, v1alpha1.ConditionExecutable))
			}
			if c := condOf(got, v1alpha1.ConditionNoApprovalPolicy); c == nil || c.Reason != tc.reason {
				t.Errorf("NoApprovalPolicy %+v, want %s", c, tc.reason)
			}
		})
	}
}

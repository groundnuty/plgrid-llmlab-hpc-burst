package lifecycle

// The executor's own rules on the fixture pass over a fake client (#589, PR 5): which committed targets are done with no
// act, which cause a release carries, that only the committed list is acted on, and when the spec is written. The
// scenario rig's tests (opexec_rig_test.go) run the whole controller; these pin each rule where a fixture can set up
// exactly its case.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The ONE function that says when a target with no act of its own is done (the science agent's ruling on #589, 16 Nov
// 2026): Keep is done at the commit, SetEndTime once the spec write lands or the deployment is gone, DeleteDeployment once the
// deployment is gone. Every effect with an act is never done by this function. The table is written out by hand.
func TestNoActDoneTable(t *testing.T) {
	for _, tc := range []struct {
		effect                  v1alpha1.PlanEffect
		specWritten, deployGone bool
		want                    bool
	}{
		{v1alpha1.PlanEffectKeep, false, false, true},
		{v1alpha1.PlanEffectKeep, true, true, true},
		{v1alpha1.PlanEffectSetEndTime, false, false, false},
		{v1alpha1.PlanEffectSetEndTime, true, false, true},
		{v1alpha1.PlanEffectSetEndTime, false, true, true},
		{v1alpha1.PlanEffectDeleteDeployment, false, false, false},
		{v1alpha1.PlanEffectDeleteDeployment, true, false, false}, // a write that landed is not a deletion
		{v1alpha1.PlanEffectDeleteDeployment, false, true, true},
		{v1alpha1.PlanEffectCancelPending, true, true, false},
		{v1alpha1.PlanEffectDrainInstance, true, true, false},
		{v1alpha1.PlanEffectDrainLastReady, true, true, false},
		{v1alpha1.PlanEffectEndStarting, true, true, false},
		{v1alpha1.PlanEffectReleaseKept, true, true, false},
		{v1alpha1.PlanEffectReleaseStarted, true, true, false},
	} {
		if got := noActDone(tc.effect, tc.specWritten, tc.deployGone); got != tc.want {
			t.Errorf("noActDone(%s, specWritten %v, deploymentGone %v) = %v, want %v", tc.effect, tc.specWritten, tc.deployGone, got, tc.want)
		}
	}
}

// The release cause goes through lifecycle's releaseCause, which cuts it at 256 bytes: a namespace of 63 characters with
// a name of 145 characters and a UID of 36 is exactly 256 bytes with its "operation " and its two separators, and parses
// back; a longer name is cut, and the parser refuses the cut cause, so the event then names no Operation (the Ruling:
// "names over 145 characters lose the UID and the event goes out without related").
func TestAnOperationsCauseRoundTripsThroughReleaseCause(t *testing.T) {
	ns := strings.Repeat("n", 63)
	uid := types.UID("0123abcd-0123-4abc-8def-0123456789ab")
	name := strings.Repeat("o", 145)
	cause := releaseCause(v1alpha1.ReleaseCauseOperation(ns, name, uid))
	if len(cause) > maxReleaseCause {
		t.Fatalf("the cause is %d bytes, over the %d the annotation holds", len(cause), maxReleaseCause)
	}
	gotNS, gotName, gotUID, ok := v1alpha1.ParseReleaseCauseOperation(cause)
	if !ok || gotNS != ns || gotName != name || gotUID != uid {
		t.Errorf("a 63-character namespace with a 145-character name parsed back as %q/%q %q (%v)", gotNS, gotName, gotUID, ok)
	}
	long := releaseCause(v1alpha1.ReleaseCauseOperation(ns, strings.Repeat("o", 146), uid))
	if len(long) > maxReleaseCause || !strings.HasSuffix(long, "…") {
		t.Errorf("a 146-character name: the cause is %d bytes %q, want cut to the bound and marked", len(long), long)
	}
	if _, _, _, ok := v1alpha1.ParseReleaseCauseOperation(long); ok {
		t.Error("the parser read a cut cause: a longer name must be refused cleanly")
	}
}

// executingOp is an Operation of the action that is Executing, as the controller left it: the committed targets are
// the list it acts on, and the plan lists the targets it was made of.
func executingOp(name string, op *v1alpha1.Operation, committed []v1alpha1.CommittedTarget, planned ...v1alpha1.PlanTarget) *v1alpha1.Operation {
	op = opNamed(name, op)
	op.Status.Phase, op.Status.Committed = v1alpha1.OperationPhaseExecuting, committed
	op.Status.Plan = &v1alpha1.OperationPlan{
		ID: "0123456789abcdef", ObservedAt: metav1.NewTime(opAt),
		ModelDeployment: v1alpha1.PlannedDeployment{Name: opMD, UID: "uid-md", Generation: 3},
		Targets:         planned,
	}
	return op
}

func committedOf(kind v1alpha1.TargetKind, name string, effect v1alpha1.PlanEffect) v1alpha1.CommittedTarget {
	return v1alpha1.CommittedTarget{Kind: kind, Name: name, UID: types.UID("uid-" + name), Generation: 1, Effect: effect}
}

// getInstance and getAlloc read what the fixture's client holds.
func (g *opRig) getInstance(name string) (*v1alpha1.Instance, bool) {
	var in v1alpha1.Instance
	err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: name}, &in)
	return &in, err == nil
}

func (g *opRig) getAlloc(name string) (*v1alpha1.Allocation, bool) {
	var a v1alpha1.Allocation
	err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: name}, &a)
	return &a, err == nil
}

// Only the committed list is acted on. The plan lists two pending Instances and their Allocations, and the committed
// list only the first pair: the second is not touched, however the controller's own selection or the plan would read.
func TestOnlyTheCommittedTargetsAreActedOn(t *testing.T) {
	f := newOpFixture(t, 3)
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	f.addInstance(instOpts{name: "p2", phase: v1alpha1.InstancePending, created: 2 * time.Minute, submit: tp(opT0.Add(15 * time.Minute))})
	planned := func(kind v1alpha1.TargetKind, name string) v1alpha1.PlanTarget {
		return v1alpha1.PlanTarget{Kind: kind, Name: name, UID: types.UID("uid-" + name), Effect: v1alpha1.PlanEffectCancelPending, NeedsApproval: true}
	}
	op := executingOp("scale", scaleOp(1),
		[]v1alpha1.CommittedTarget{
			committedOf(v1alpha1.TargetKindInstance, "p", v1alpha1.PlanEffectCancelPending),
			committedOf(v1alpha1.TargetKindAllocation, "alloc-p", v1alpha1.PlanEffectCancelPending),
		},
		planned(v1alpha1.TargetKindInstance, "p"), planned(v1alpha1.TargetKindAllocation, "alloc-p"),
		planned(v1alpha1.TargetKindInstance, "p2"), planned(v1alpha1.TargetKindAllocation, "alloc-p2"))
	g := newOpRig(t, f, op)
	g.step(opAt)

	if in, ok := g.getInstance("p"); !ok || !ended(in) {
		t.Errorf("the committed Instance p is %q (found %v), want ended", in.Status.Phase, ok)
	}
	if _, ok := g.getAlloc("alloc-p"); ok {
		t.Error("the committed Allocation alloc-p still exists")
	}
	if in, ok := g.getInstance("p2"); !ok || ended(in) || in.Status.Phase != v1alpha1.InstancePending {
		t.Errorf("Instance p2, in the plan and not committed, is %q (found %v): it must not be touched", in.Status.Phase, ok)
	}
	if a, ok := g.getAlloc("alloc-p2"); !ok || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
		t.Errorf("Allocation alloc-p2, in the plan and not committed, was touched: %+v (found %v)", a, ok)
	}
	if in, ok := g.getInstance("r"); !ok || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("Instance r is %q", in.Status.Phase)
	}
}

// A target already done is skipped, and one whose UID is gone counts as done: an ended Instance is not ended again, and an
// Instance and an Allocation that no longer exist are done without any write. The Operation's report says done for each.
func TestADoneTargetIsSkippedAndAGoneUIDCountsAsDone(t *testing.T) {
	f := newOpFixture(t, 1)
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "done", phase: v1alpha1.InstanceEnded, created: time.Minute})
	op := executingOp("scale", scaleOp(1), []v1alpha1.CommittedTarget{
		committedOf(v1alpha1.TargetKindInstance, "done", v1alpha1.PlanEffectCancelPending),
		committedOf(v1alpha1.TargetKindInstance, "vanished", v1alpha1.PlanEffectDrainInstance),
		committedOf(v1alpha1.TargetKindAllocation, "alloc-vanished", v1alpha1.PlanEffectReleaseKept),
		committedOf(v1alpha1.TargetKindAllocation, "alloc-r", v1alpha1.PlanEffectKeep),
	})
	g := newOpRig(t, f, op)
	// Whatever is written to an Instance or an Allocation is seen through its resourceVersion.
	versions := map[string]string{}
	for _, n := range []string{"r", "done"} {
		in, _ := g.getInstance(n)
		versions["Instance "+n] = in.ResourceVersion
	}
	a, _ := g.getAlloc("alloc-r")
	versions["Allocation alloc-r"] = a.ResourceVersion
	g.step(opAt)

	for _, n := range []string{"r", "done"} {
		if in, _ := g.getInstance(n); in.ResourceVersion != versions["Instance "+n] {
			t.Errorf("Instance %s was written: nothing was left to do", n)
		}
	}
	if a, _ := g.getAlloc("alloc-r"); a.ResourceVersion != versions["Allocation alloc-r"] {
		t.Error("Allocation alloc-r was written: a Keep has no act")
	}
	got := g.op("scale")
	if got.Status.Phase != v1alpha1.OperationPhaseDone || len(got.Status.Targets) != 4 {
		t.Fatalf("phase %q with %d outcomes, want Done with four", got.Status.Phase, len(got.Status.Targets))
	}
	for _, o := range got.Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("%s %s is %s, want Done", o.Kind, o.Name, o.State)
		}
	}
}

// An Allocation that an Executing Operation committed to release is released for it, whichever path deletes it; a Keep,
// which retention releases later, keeps retention's own cause; and an Operation that is not Executing counts for nothing.
func TestAReleaseCauseIsTheOperationsOnlyForAReleasingCommittedTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		effect    v1alpha1.PlanEffect
		phase     v1alpha1.OperationPhase
		queued    bool // the Allocation's job has not started
		finished  bool
		wantOwnOp bool
	}{
		{"a committed ReleaseStarted", v1alpha1.PlanEffectReleaseStarted, v1alpha1.OperationPhaseExecuting, false, false, true},
		{"a committed CancelPending of a job still queued", v1alpha1.PlanEffectCancelPending, v1alpha1.OperationPhasePartlyFailed, true, false, true},
		{"a committed CancelPending whose job started: it is not the Operation's release", v1alpha1.PlanEffectCancelPending, v1alpha1.OperationPhaseExecuting, false, false, false},
		{"a committed ReleaseKept", v1alpha1.PlanEffectReleaseKept, v1alpha1.OperationPhaseExecuting, false, false, true},
		{"a committed Keep keeps retention's cause", v1alpha1.PlanEffectKeep, v1alpha1.OperationPhaseExecuting, false, false, false},
		{"an Operation that is Done counts for nothing", v1alpha1.PlanEffectReleaseStarted, v1alpha1.OperationPhaseDone, false, false, false},
		{"a PartlyFailed Operation with a finish time counts for nothing", v1alpha1.PlanEffectReleaseStarted, v1alpha1.OperationPhasePartlyFailed, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			a := f.emptyAllocation("alloc-k", nil)
			if tc.queued {
				a.Status.AtProvider.StartTime = nil
			}
			op := executingOp("rel", releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"alloc-k"}}),
				[]v1alpha1.CommittedTarget{committedOf(v1alpha1.TargetKindAllocation, "alloc-k", tc.effect)})
			op.Status.Phase = tc.phase
			if tc.finished {
				op.Status.FinishedAt = opTime(opAt)
			}
			g := newOpRig(t, f, op)
			g.syncVersions()
			p := f.pass()
			p.r, p.ctx, p.now, p.mem = g.r, g.ctx, opAt, g.r.memory()
			p.ops = []*v1alpha1.Operation{g.op("rel")}
			if err := p.deleteAllocation(a, "its job is Ended"); err != nil {
				t.Fatal(err)
			}
			stored, _ := g.getAlloc("alloc-k")
			// The cause the store held: gone with the delete in the fake client, so read it from the object deleteAllocation wrote.
			got := a.Annotations[v1alpha1.AnnotationReleaseCause]
			_ = stored
			want := "its job is Ended"
			if tc.wantOwnOp {
				want = "operation ns/rel uid-rel"
			}
			if got != want {
				t.Errorf("the release cause is %q, want %q", got, want)
			}
		})
	}
}

// The spec is written once, before any act, and by the Operation's own count: a Scale that is Executing with the spec
// not yet written writes spec.instances = N; once SpecWritten is recorded a person's later edit is never written over.
func TestTheSpecIsWrittenOnceAndAPersonsLaterEditIsKept(t *testing.T) {
	f := newOpFixture(t, 3)
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	op := executingOp("scale", scaleOp(1), nil)
	g := newOpRig(t, f, op)
	g.step(opAt)
	var md v1alpha1.ModelDeployment
	if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: opMD}, &md); err != nil {
		t.Fatal(err)
	}
	if md.Spec.Instances != 1 {
		t.Fatalf("spec.instances is %d, want 1: the resumed Operation writes the spec", md.Spec.Instances)
	}
	if got := g.op("scale"); got.Status.Phase != v1alpha1.OperationPhaseDone || !isTrue(got.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Fatalf("phase %q, SpecWritten %v", got.Status.Phase, isTrue(got.Status.Conditions, v1alpha1.ConditionSpecWritten))
	}
	// A person raises the count afterwards, and a Scale Operation that were still Executing would not write it down again.
	stillExecuting := g.op("scale")
	stillExecuting.Status.Phase = v1alpha1.OperationPhaseExecuting
	stillExecuting.Status.Committed = []v1alpha1.CommittedTarget{committedOf(v1alpha1.TargetKindInstance, "never-gone", v1alpha1.PlanEffectDrainInstance)}
	stillExecuting.Status.Targets = nil
	if err := g.c.Status().Update(g.ctx, stillExecuting); err != nil {
		t.Fatal(err)
	}
	md.Spec.Instances = 5
	if err := g.c.Update(g.ctx, &md); err != nil {
		t.Fatal(err)
	}
	g.f.md = md.DeepCopy()
	g.step(opAt.Add(time.Minute))
	var again v1alpha1.ModelDeployment
	if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: opMD}, &again); err != nil || again.Spec.Instances != 5 {
		t.Errorf("spec.instances is %d (%v), want the person's 5 kept", again.Spec.Instances, err)
	}
}

// A target that is no longer a deployment of this UID is never written: an End whose deployment is another of the same
// name now writes nothing to it, and its target counts as done.
func TestASpecIsNeverWrittenToAnotherDeploymentOfTheSameName(t *testing.T) {
	f := newOpFixture(t, 1)
	op := executingOp("end", endOp(false), []v1alpha1.CommittedTarget{{Kind: v1alpha1.TargetKindModelDeployment, Name: opMD, UID: "uid-the-old-one", Generation: 3, Effect: v1alpha1.PlanEffectSetEndTime}})
	op.Status.Plan.ModelDeployment.UID = "uid-the-old-one"
	g := newOpRig(t, f, op)
	g.step(opAt)
	var md v1alpha1.ModelDeployment
	if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: opMD}, &md); err != nil {
		t.Fatal(err)
	}
	if md.Spec.EndTime != nil {
		t.Errorf("the end time of the deployment that is there now was set to %v", md.Spec.EndTime)
	}
	if got := g.op("end"); got.Status.Phase != v1alpha1.OperationPhaseDone {
		t.Errorf("phase %q: the target whose UID is gone counts as done", got.Status.Phase)
	}
}

// The spec is written only while the deployment's generation is the plan's: a generation past it means the spec changed
// since the plan, by this Operation's own write whose record did not land or by someone else, and neither is written over.
// The cases are written out by hand: the plan's generation is 3, and a change of the spec moves it by one.
func TestTheSpecIsWrittenOnlyWhileTheGenerationIsThePlans(t *testing.T) {
	later := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		op            *v1alpha1.Operation
		generation    int64
		instances     int32
		endTime       *time.Time
		wantInstances int32
		wantEnd       *time.Time
	}{
		{"Scale, nobody has touched the spec: written", scaleOp(1), 3, 3, nil, 1, nil},
		{"Scale, this Operation's own write landed (generation 4, count 1): nothing to write", scaleOp(1), 4, 1, nil, 1, nil},
		{"Scale, a person raised it after the plan (generation 4, count 2): not written over", scaleOp(1), 4, 2, nil, 2, nil},
		{"Scale, our write landed and a person raised it (generation 5): not written over", scaleOp(1), 5, 2, nil, 2, nil},
		{"Scale, a person edited something else after the plan (generation 4, count 3): not written", scaleOp(1), 4, 3, nil, 3, nil},
		{"End, nobody has touched the spec: the end time is now", endOp(false), 3, 1, nil, 1, tp(opAt)},
		{"End, our write landed (generation 4, end time now): nothing to write", endOp(false), 4, 1, tp(opAt), 1, tp(opAt)},
		{"End, a person moved the end time later (generation 4): not written over", endOp(false), 4, 1, &later, 1, &later},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, tc.instances)
			f.md.Generation, f.md.Spec.EndTime = tc.generation, nil
			if tc.endTime != nil {
				f.md.Spec.EndTime = opTime(*tc.endTime)
			}
			op := executingOp("op", tc.op, nil)
			g := newOpRig(t, f, op)
			g.step(opAt)
			var md v1alpha1.ModelDeployment
			if err := g.c.Get(g.ctx, types.NamespacedName{Namespace: opNS, Name: opMD}, &md); err != nil {
				t.Fatal(err)
			}
			if md.Spec.Instances != tc.wantInstances {
				t.Errorf("spec.instances is %d, want %d", md.Spec.Instances, tc.wantInstances)
			}
			switch {
			case tc.wantEnd == nil && md.Spec.EndTime != nil:
				t.Errorf("the end time is %v, want none", md.Spec.EndTime)
			case tc.wantEnd != nil && (md.Spec.EndTime == nil || !md.Spec.EndTime.Time.Equal(*tc.wantEnd)):
				t.Errorf("the end time is %v, want %v", md.Spec.EndTime, tc.wantEnd)
			}
			if got := g.op("op"); !isTrue(got.Status.Conditions, v1alpha1.ConditionSpecWritten) {
				t.Errorf("SpecWritten is not recorded: %+v", got.Status.Conditions)
			}
		})
	}
}

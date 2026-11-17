package lifecycle

// The seven rows of #664's last review of bd3ea3b2 (a Carbon review's seven claims), each asserting the CORRECT behaviour (HBEP-10
// as amended, and the rulings). Every one FAILED on bd3ea3b2, which showed its claim real, and passes on the head that fixes it.
// Nothing here is released, drained, deleted or committed that a plan did not approve for what its target is NOW.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// approveEnd plans, confirms and approves an End, and returns once it has committed to want targets.
func (w *execWorld) approveEnd(name string, del bool, want int) *v1alpha1.Operation {
	w.t.Helper()
	planned := w.plan(name, endSpec(del))
	w.confirm(name)
	w.untilPhase(name, v1alpha1.OperationPhaseAwaitingApproval)
	w.approve(name, "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp(name).Status.Committed) == want })
	return planned
}

// scancelOf counts the fake Slurm's scancel calls that name the job.
func scancelOf(w *execWorld, job string) int {
	n := 0
	for _, c := range w.batch.Calls() {
		if c.Op == "scancel" && strings.Contains(strings.Join(c.Args, " "), job) {
			n++
		}
	}
	return n
}

// 1. After LastReadySinceCommit the Operation finishes PartlyFailed: the controller's own step 7 must not then end or drain the
// kept R, which the approved plan never named.
func TestAfterALastReadySinceCommitNothingOutsideTheCommittedTargetsIsEverTaken(t *testing.T) {
	w, fleet, _ := newStartingFleetWorld(t, []time.Duration{20 * time.Minute, 6 * time.Minute}, false)
	r, x := fleet[0], fleet[1]
	rAlloc, xAlloc := w.mustInstance(r).Spec.AllocationRef.Name, w.mustInstance(x).Spec.AllocationRef.Name
	rJob := w.allocOrNil(rAlloc).Annotations[v1alpha1.AnnotationJobID]
	stop := w.failSpecWrites()
	planned := w.approveScale("scale", 1, 2)
	if pt := planTargets(planned.Status.Plan); pt["Instance "+x] != v1alpha1.PlanEffectEndStarting || pt["Allocation "+xAlloc] != v1alpha1.PlanEffectReleaseStarted {
		t.Fatalf("the plan is %v", pt)
	}
	w.stepsUntil(30*time.Minute, 30*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(r).Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("R is ready: X is not the last ready one")
	}
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass in which the spec write lands: %v", err)
	}
	w.stepsUntil(2*time.Minute, 10*time.Second, "the Operation finished (the count is given back first)", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	op := w.getOp("scale")
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Fatalf("the Operation is %q finishedAt %v, want PartlyFailed and finished: the scenario did not arise", op.Status.Phase, op.Status.FinishedAt)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d after the Operation finished, want 2: its untaken unit gave its count back (#698)", got)
	}
	check := func(when string) bool {
		bad := false
		if in := w.instance(r); ended(in) || isDraining(in) {
			t.Errorf("%s: R is %q (end reason %q): the kept Instance the plan never named was ended or drained", when, in.Status.Phase, in.Status.EndReason)
			bad = true
		}
		if a, ok := w.allocation(rAlloc); !ok || !a.DeletionTimestamp.IsZero() {
			t.Errorf("%s: R's Allocation %s is %+v (found %v): deleted, though the plan never named it", when, rAlloc, a, ok)
			bad = true
		}
		if n := scancelOf(w, rJob); n != 0 {
			t.Errorf("%s: the fake Slurm saw %d scancel calls on R's job", when, n)
			bad = true
		}
		return bad
	}
	if check("in the pass in which the spec write landed") {
		return
	}
	readyFor := 0
	w.stepsUntil(40*time.Minute, 30*time.Second, "R ready and six passes after", func() bool {
		if check("after the Operation finished") {
			return true
		}
		if w.instance(r).Status.Phase == v1alpha1.InstanceReady {
			readyFor++
		}
		return readyFor >= 6
	})
}

// 2. A DrainInstance target (no approval) that becomes the last ready Instance is not drained.
func TestACommittedDrainInstanceThatBecomesTheLastReadyOneIsNotDrained(t *testing.T) {
	w, insts, _ := newTwoWorld(t)
	stop := w.failSpecWrites()
	planned := w.plan("scale", scaleTo(1))
	var victim, survivor string
	for _, pt := range planned.Status.Plan.Targets {
		t.Logf("plan target %s %s %s needsApproval=%v", pt.Kind, pt.Name, pt.Effect, pt.NeedsApproval)
		if pt.Kind == v1alpha1.TargetKindInstance && pt.Effect == v1alpha1.PlanEffectDrainInstance {
			victim = pt.Name
		}
	}
	if victim == "" {
		t.Fatalf("the plan has no DrainInstance target: %v", planTargets(planned.Status.Plan))
	}
	for _, n := range insts {
		if n != victim {
			survivor = n
		}
	}
	w.confirm("scale")
	// With no approval needed the Operation commits on its own; the spec write fails, so nothing is acted on.
	w.stepsUntil(2*time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	// The survivor FAILS before the next pass.
	in := w.mustInstance(survivor)
	now := metaTime(w.clock.Now())
	in.Status.Phase, in.Status.EndReason, in.Status.EndTime = v1alpha1.InstanceEnded, v1alpha1.ReasonQualificationFailed, &now
	if err := w.c.Status().Update(w.ctx, in); err != nil {
		t.Fatal(err)
	}
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the survivor failed: %v", err)
	}
	if v := w.instance(victim); isDraining(v) || ended(v) {
		t.Errorf("the victim %s is %q: the last ready Instance was drained under a DrainInstance approval that needed none", victim, v.Status.Phase)
	}
	op := w.getOp("scale")
	if o, ok := outcomeOf(op, victim); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonLastReadySinceCommit {
		t.Errorf("the victim's outcome is %+v (%v), want Failed with %s", o, ok, reasonLastReadySinceCommit)
	}
}

// 3. An End whose CancelPending Allocation's job started after the commit and before the spec write landed.
func TestAnEndNeverReleasesAJobThatStartedAfterItsCancelWasApprovedAsQueued(t *testing.T) {
	for _, del := range []bool{false, true} {
		name := "End without delete"
		if del {
			name = "End with delete"
		}
		t.Run(name, func(t *testing.T) {
			w := newExecWorld(t, time.Hour)
			pAlloc := w.pendingAlloc
			// The spec write fails, and so does the Delete: an End with delete writes no spec, and its Delete must not go through
			// before P's job has started, whichever head runs this.
			failing := true
			w.fc.set(func(method string, obj client.Object) error {
				if _, ok := obj.(*v1alpha1.ModelDeployment); ok && (method == "Update" || method == "Delete") && failing {
					return errors.New("injected: the write of the spec fails")
				}
				return nil
			}, nil)
			stop := func() { failing = false }
			planned := w.plan("end", endSpec(del))
			if pt := planTargets(planned.Status.Plan); pt["Allocation "+pAlloc] != v1alpha1.PlanEffectCancelPending {
				t.Fatalf("the plan takes P's Allocation as %q: %v", pt["Allocation "+pAlloc], pt)
			}
			w.confirm("end")
			w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("end", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
			w.stepsUntil(90*time.Minute, 2*time.Minute, "P's job started", func() bool {
				a := w.allocOrNil(pAlloc)
				return a != nil && a.Status.AtProvider.StartTime != nil
			})
			stop()
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("the pass in which the spec write lands: %v", err)
			}
			cause, deleted := w.log.deletedWith(pAlloc)
			t.Logf("OBSERVED: in the pass in which the spec landed, P's Allocation deleted=%v cause=%q", deleted, cause)
			if deleted {
				t.Errorf("P's started Allocation was released IN THE SAME PASS as the spec write (cause %q): the approval was to cancel a queued job", cause)
			}
			for i := 0; i < 400 && !deleted; i++ {
				w.clock.Advance(30 * time.Second)
				if err := w.stepKeepingErrors(); err != nil {
					t.Fatalf("pass %d: %v", i, err)
				}
				if cause, deleted = w.log.deletedWith(pAlloc); deleted {
					t.Logf("OBSERVED: P's Allocation was released %s after the spec landed, with the cause %q", (time.Duration(i+1) * 30 * time.Second), cause)
				}
			}
			op := w.getOp("end")
			if o, ok := outcomeOf(op, pAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit {
				t.Errorf("P's Allocation outcome is %+v (%v), want Failed with %s", o, ok, reasonJobStartedSinceCommit)
			}
			if deleted {
				t.Errorf("P's Allocation was released (cause %q): a started-since-commit Allocation is kept warm (#698)", cause)
			} else {
				t.Logf("OBSERVED: P's Allocation was NOT released within 200 minutes: kept warm")
			}
		})
	}
}

// 4. A warm, kept Allocation whose job dies after the plan and before the Release acts: the failure's cause.
func TestADeadEmptyAllocationIsReleasedWithTheFailuresCauseAndItsTargetNamesIt(t *testing.T) {
	for _, mode := range []string{"Failed", "Lost"} {
		t.Run(mode, func(t *testing.T) {
			w, kA, _ := newKeptTwoProvidersWorld(t)
			job := w.allocOrNil(kA).Annotations[v1alpha1.AnnotationJobID]
			w.plan("release", releaseSpec(kA))
			w.confirm("release")
			w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("release", "person", userPerson)
			// The job dies before the pass that commits the Release, which retention would then have released itself. The Provider
			// marks a vanished job Lost only after its grace, so its passes run alone until the Allocation reads Lost.
			if mode == "Lost" {
				w.batch.Vanish(job)
				for i := 0; i < 60 && w.allocOrNil(kA).Status.AtProvider.Phase != v1alpha1.AllocationLost; i++ {
					w.clock.Advance(time.Minute)
					if err := w.prov.Sync(w.ctx); err != nil {
						t.Fatalf("provider sync: %v", err)
					}
				}
				if got := w.allocOrNil(kA).Status.AtProvider.Phase; got != v1alpha1.AllocationLost {
					t.Fatalf("kA is %s after the Provider's passes, want Lost: the scenario did not arise", got)
				}
			} else {
				w.batch.Requeue(job)
			}
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("the pass: %v", err)
			}
			a := w.allocOrNil(kA)
			if a != nil {
				t.Logf("OBSERVED: kA phase %s after the pass", a.Status.AtProvider.Phase)
			}
			w.stepsUntil(5*time.Minute, 10*time.Second, "kA gone", func() bool { _, ok := w.allocation(kA); return !ok })
			cause, deleted := w.log.deletedWith(kA)
			if !deleted {
				t.Fatal("kA was never deleted")
			}
			if ns, nm, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
				t.Errorf("kA's cause is the Operation's, %q (%s/%s %s): a dead Allocation's release cause is the failure's, \"its job is %s\"", cause, ns, nm, uid, mode)
			}
			if !strings.Contains(cause, "its job is "+mode) {
				t.Errorf("kA's cause is %q, want retention's dead-Allocation reason \"its job is %s\"", cause, mode)
			}
			w.untilPhase("release", v1alpha1.OperationPhaseDone)
			if o, ok := outcomeOf(w.getOp("release"), kA); !ok || o.State != v1alpha1.TargetStateDone || !strings.HasPrefix(o.Message, "released by ") {
				t.Errorf("kA's outcome is %+v (%v), want Done and a message naming the failure (\"released by ...\")", o, ok)
			}
		})
	}
}

// 5. An End with delete races a person's edit between the pass's read of the deployment and the Delete: it must not go through.
func TestAnEndWithDeleteDoesNotDeleteADeploymentAPersonEditedAfterThePassReadIt(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	armed, edited := false, false
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" && armed && !edited {
			edited = true
			// A person raises the count, after the pass read the deployment (mutateMD moves the generation, as the API server does).
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 })
			return nil
		}
		return nil
	}, nil)
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	armed = true
	for i := 0; i < 6; i++ {
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err != nil && !strings.Contains(err.Error(), "conflict") && !strings.Contains(err.Error(), "modified") {
			t.Logf("pass %d error: %v", i, err)
		}
	}
	if !edited {
		t.Fatal("the Delete never ran: the scenario did not arise")
	}
	var md v1alpha1.ModelDeployment
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: hoMD}, &md); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatalf("the deployment was DELETED although a person edited it after the pass read it (spec.instances 5): %v", err)
		}
		t.Fatal(err)
	}
	if !md.DeletionTimestamp.IsZero() {
		t.Fatal("the deployment is being deleted")
	}
	op := w.getOp("end")
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Errorf("the Operation is %q finishedAt %v, want PartlyFailed and finished (SpecChangedSinceCommit)", op.Status.Phase, op.Status.FinishedAt)
	}
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateFailed || o.Reason != reasonSpecChangedSinceCommit {
			t.Errorf("target %s %s is %s (%s), want Failed with %s", o.Kind, o.Name, o.State, o.Reason, reasonSpecChangedSinceCommit)
		}
	}
	if n := w.batch.Count("scancel"); n != 0 {
		t.Errorf("%d scancel calls: nothing is released", n)
	}
}

// 6. A Scale that commits NO target, stopped by a person's spec change, is not Done.
func TestAScaleOfNoTargetThatASpecChangeStoppedIsPartlyFailedNotDone(t *testing.T) {
	f := newOpFixture(t, 3)
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: time.Minute})
	// A person raised spec.instances to 4 after the plan: the generation moved and the effect (at most 2) no longer holds.
	f.md.Spec.Instances, f.md.Generation = 4, 4
	op := executingOp("scale", scaleOp(2), nil)
	g := newOpRig(t, f, op)
	g.step(opAt)
	got := g.op("scale")
	t.Logf("OBSERVED: phase %q finishedAt %v committed %d targets %d", got.Status.Phase, got.Status.FinishedAt, len(got.Status.Committed), len(got.Status.Targets))
	for _, c := range got.Status.Conditions {
		t.Logf("  condition %s=%s (%s): %s", c.Type, c.Status, c.Reason, c.Message)
	}
	if got.Status.Phase != v1alpha1.OperationPhasePartlyFailed {
		t.Errorf("the Operation is %q: a Scale a person's edit stopped, with no target to carry the failure, must not be Done (HBEP-10: finally failed, PartlyFailed)", got.Status.Phase)
	}
}

// 7. op-1's commit write errors because another controller copy committed AND finished it, and changed the deployment: op-2
// must not commit in that pass.
func TestACommitErrorOnOneOperationBlocksEveryOtherCommitInThatPass(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("op-1", scaleTo(1))
	w.plan("op-2", scaleTo(1))
	w.confirm("op-1")
	w.confirm("op-2")
	w.untilPhase("op-1", v1alpha1.OperationPhaseAwaitingApproval)
	w.untilPhase("op-2", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("op-1", "person", userPerson)
	w.approve("op-2", "person", userPerson)
	fired := false
	w.fc.set(func(method string, obj client.Object) error {
		op, ok := obj.(*v1alpha1.Operation)
		if !ok || method != "Status.Update" || op.Name != "op-1" || op.Status.Phase != v1alpha1.OperationPhaseExecuting || fired {
			return nil
		}
		fired = true
		// Another controller copy committed and finished op-1, and changed the deployment: it ended the pending Instance.
		var cur v1alpha1.Operation
		if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: "op-1"}, &cur); err != nil {
			return err
		}
		done := metaTime(w.clock.Now())
		cur.Status.Phase, cur.Status.FinishedAt = v1alpha1.OperationPhaseDone, &done
		if err := w.c.Status().Update(w.ctx, &cur); err != nil {
			return err
		}
		in := w.mustInstance(w.pending)
		in.Status.Phase, in.Status.EndReason, in.Status.EndTime = v1alpha1.InstanceEnded, v1alpha1.ReasonDrained, &done
		if err := w.c.Status().Update(w.ctx, in); err != nil {
			return err
		}
		return errors.New("injected: the commit write of op-1 fails")
	}, nil)
	w.clock.Advance(10 * time.Second)
	_ = w.stepKeepingErrors()
	if !fired {
		t.Fatal("op-1's commit write never ran: the scenario did not arise")
	}
	op2 := w.getOp("op-2")
	t.Logf("OBSERVED: op-2 phase %q committed %d targets", op2.Status.Phase, len(op2.Status.Committed))
	if len(op2.Status.Committed) != 0 || op2.Status.Phase == v1alpha1.OperationPhaseExecuting {
		t.Errorf("op-2 COMMITTED in the pass in which op-1's commit conflicted (phase %q, %d targets): its stale check ran against the pass's pre-op-1 state", op2.Status.Phase, len(op2.Status.Committed))
	}
	_ = pfake.Script{}
}

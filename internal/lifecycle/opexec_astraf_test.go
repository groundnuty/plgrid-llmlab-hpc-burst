package lifecycle

// Rows for the findings of the focused review of #664 at 77479164 (Codex gpt-6.2-astra). Each asserts the CORRECT behaviour; a row
// that passes drops its item.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// 1. An End commits while a pending Instance waits for its Allocation, which cannot be created yet (the create is held back, as a
// full submit-slot limit holds it): the Instance exists with NO Allocation. The create is then let through, before the spec write
// lands (it keeps failing). Step 6 must not create the Allocation in that window: from the commit until the spec write lands the
// controller creates no Allocation, so no job of it can be cancelled by the End's shutdown, and no unmarked job can start.
func TestAnEndCommittedWhileAnInstanceWaitsForItsAllocationCreatesNoAllocationBeforeItsSpecWrite(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	holdCreate, failSpec := true, false
	w.fc.set(func(method string, obj client.Object) error {
		switch obj.(type) {
		case *v1alpha1.Allocation:
			if method == "Create" && holdCreate {
				return errors.New("injected: the Allocation's create is held back")
			}
		case *v1alpha1.ModelDeployment:
			if method == "Update" && failSpec {
				return errors.New("injected: the write of the spec fails")
			}
		}
		return nil
	}, nil)
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	var waiting string
	w.stepsUntil(5*time.Minute, 10*time.Second, "an Instance with no Allocation", func() bool {
		for _, in := range w.instances(hoMD) {
			if w.allocOrNil(in.Spec.AllocationRef.Name) == nil {
				waiting = in.Name
				return true
			}
		}
		return false
	})
	op := opRigOp("end", endSpec(false))
	w.create(op)
	w.stepsUntil(time.Minute, 10*time.Second, "end planned", func() bool { return w.getOp("end").Status.Phase == v1alpha1.OperationPhasePlanned })
	w.confirm("end")
	w.stepsUntil(time.Minute, 10*time.Second, "end awaiting approval", func() bool { return w.getOp("end").Status.Phase == v1alpha1.OperationPhaseAwaitingApproval })
	failSpec = true
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	before := w.objectNames()
	holdCreate = false // the slot opens
	w.passes(60, nil)
	if o := w.getOp("end"); apimeta.IsStatusConditionTrue(o.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Fatal("the spec write landed: the case did not arise")
	}
	for name := range w.objectNames() {
		if !before[name] {
			t.Errorf("%s was created between the commit and the spec write (Instance %s was waiting for it)", name, waiting)
		}
	}
}

// 2. An End commits a QUEUED Instance; its mark write keeps failing. The job starts and the Instance ends CLEANLY by a path that is
// not the Operation's (a person deletes it, as a hedge's loss would end it). Retention is zero-idle. The running Allocation must NOT
// be deleted while its mark has not landed.
func TestARunningAllocationWhoseMarkCouldNotBeWrittenIsNotReleasedByRetentionWhenItsInstanceEndsCleanly(t *testing.T) {
	w := newExecWorldWith(t, time.Hour, func(m *v1alpha1.ModelDeployment) {
		m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	})
	pAlloc := w.pendingAlloc
	w.fc.set(func(method string, obj client.Object) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && a.Name == pAlloc && method == "Update" && a.Annotations[v1alpha1.AnnotationKeptWarm] != "" {
			return errors.New("injected: the write of the kept-warm mark fails")
		}
		return nil
	}, nil)
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P's job started", func() bool {
		a := w.allocOrNil(pAlloc)
		return a != nil && a.Status.AtProvider.StartTime != nil
	})
	if a := w.allocOrNil(pAlloc); a == nil || keptWarm(a) {
		t.Fatalf("P's Allocation is %+v, want it started and unmarked: the case did not arise", a)
	}
	in := w.mustInstance(w.pending)
	if err := w.c.Delete(w.ctx, in); err != nil { // the Instance ends cleanly, by a path that is not the Operation's
		t.Fatal(err)
	}
	released := false
	w.passes(60, func(i int) bool {
		if cause, deleted := w.log.deletedWith(pAlloc); deleted {
			released = true
			t.Errorf("pass %d: P's running Allocation was RELEASED (cause %q) while its mark had not landed", i, cause)
			return true
		}
		return false
	})
	if !released && !w.endedOrGone(w.pending) {
		t.Fatalf("P's Instance is %s: it never ended, so the case did not arise", w.instanceState(w.pending))
	}
}

// 3. After CancelPending commits, the Provider reports the job's phase Running with NO StartTime. A Running job is started: under a
// Scale to one its pair is NOT taken (its Instance is not ended as pending, its Allocation is not deleted), and under an End its
// Allocation is not deleted.
func TestAJobReportedRunningWithNoStartTimeIsNotCancelledAsPending(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spec  v1alpha1.OperationSpec
		pairs bool // the Instance is not taken either
	}{
		{"a Scale to one", scaleTo(1), true},
		{"an End", endSpec(false), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newExecWorld(t, time.Hour)
			pAlloc := w.pendingAlloc
			stop := w.failSpecWrites()
			w.plan("op", tc.spec)
			w.confirm("op")
			w.untilPhase("op", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("op", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("op").Status.Committed) > 0 })
			// The Provider reports Running with no start time: the observation is made directly, and the Provider's own passes are not
			// run, so the fake Slurm (whose job is still queued) does not overwrite it.
			a := w.allocOrNil(pAlloc)
			a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
			a.Status.AtProvider.StartTime = nil
			if err := w.c.Status().Update(w.ctx, a); err != nil {
				t.Fatal(err)
			}
			stop()
			for i := 0; i < 8; i++ {
				w.clock.Advance(10 * time.Second)
				if _, err := w.r.Reconcile(w.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: rigNS, Name: hoMD}}); err != nil {
					t.Logf("pass %d: %v", i, err)
				}
			}
			if cause, deleted := w.log.deletedWith(pAlloc); deleted {
				t.Errorf("P's Allocation was DELETED (cause %q) though the Provider reported its job Running", cause)
			}
			if in := w.instanceOrNil(w.pending); tc.pairs && in != nil && ended(in) {
				t.Errorf("P's Instance was ended as pending though the Provider reported its job Running")
			}
		})
	}
}

// 4. A Scale to one leaves two started pairs untaken (their jobs started since the commit) while a third target drains. A person
// raises the count to two while the drain is outstanding. The give-back must be RECORDED (countGivenBack set, so spec.instances
// accounts for the untaken units), and step 7 must trim NO serving Instance.
func TestACountRaiseWhileADrainIsOutstandingStillRecordsTheGiveBackAndTrimsNoServingInstance(t *testing.T) {
	w, pairs := fourInstanceWorld(t)
	stop := w.failSpecWrites()
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	// The ninety minutes the queued jobs wait would bring the ready Instances' leases to their handover, whose successor is a creation
	// of its own: the leases are moved out, so that nothing but the Operation's committed state is under test.
	for _, in := range w.liveInstances(hoMD) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			w.moveLeaseEnd(in.Spec.AllocationRef.Name)
		}
	}
	w.stepsUntil(90*time.Minute, 2*time.Minute, "both queued jobs started", func() bool {
		for _, n := range pairs {
			if a := w.allocOrNil(n); a == nil || a.Status.AtProvider.StartTime == nil {
				return false
			}
		}
		return true
	})
	stop()
	w.clock.Advance(10 * time.Second)
	_ = w.stepKeepingErrors() // the act pass: the older ready Instance starts draining; the started pairs are not taken
	draining := 0
	for _, in := range w.liveInstances(hoMD) {
		if isDraining(&in) {
			draining++
		}
	}
	if draining == 0 {
		t.Fatal("no Instance is draining after the act pass: the case did not arise")
	}
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	w.passes(40, nil)
	op := w.getOp("scale")
	if rec := op.Status.CountGivenBack; rec == nil {
		t.Errorf("the give-back was not recorded: the Operation is %q finished %v with %d outcomes", op.Status.Phase, op.Status.FinishedAt, len(op.Status.Targets))
	} else if rec.Units != 2 || rec.Instances == nil || *rec.Instances != 3 {
		t.Errorf("the record is %+v, want 2 units and the larger of the raised 2 and the plan's 1 plus them, 3", rec)
	}
	if got := w.md(hoMD).Spec.Instances; got != 3 {
		t.Errorf("spec.instances is %d, want 3: max(2, 1+2)", got)
	}
	live := 0
	for _, in := range w.liveInstances(hoMD) {
		if !isDraining(&in) && !ended(&in) {
			live++
		}
	}
	if live != 3 {
		t.Errorf("%d serving or starting Instances after the raise, want the 3 that survive: step 7 trimmed a serving Instance", live)
	}
}

// fourInstanceWorld is a deployment of two ready Instances (R1, R2) and two whose jobs are queued behind an hour (P1, P2); it returns
// the Allocations' names of the queued two.
func fourInstanceWorld(t *testing.T) (*execWorld, []string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{}, pfake.Script{}, pfake.Script{QueueWait: time.Hour}, pfake.Script{QueueWait: time.Hour})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	for n := int32(1); n <= 4; n++ {
		if n > 1 {
			h.run(5*time.Minute, 30*time.Second)
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = n })
		}
		want, ready := int(n), int(n)
		if n > 2 {
			ready = 2
		}
		h.until(10*time.Minute, 10*time.Second, "the Instances", func() bool { return len(h.liveInstances(hoMD)) == want && h.readyCount(hoMD) == ready })
	}
	w := &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	var queued []string
	for _, in := range h.liveInstances(hoMD) {
		if in.Status.Phase != v1alpha1.InstanceReady {
			queued = append(queued, in.Spec.AllocationRef.Name)
		}
	}
	if len(queued) != 2 {
		t.Fatalf("%d queued Instances, want 2", len(queued))
	}
	return w, queued
}

// 5. A person switches a deployment to a managed source while its deletion is held by an empty, marked, started job (an End with delete
// left it). A Release of that job must be PLANNABLE and, approved, free the hold and complete the delete.
func TestAReleaseOfAMarkedStartedJobIsPlannableOnADeploymentSwitchedToManagedAndCompletesTheDelete(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, true)
	stop()
	w.passes(60, nil)
	if md, ok := w.deployment(); !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v), want it deleting and held: the case did not arise", md, ok)
	}
	if a, ok := w.allocation(pAlloc); !ok || !keptWarm(a) || a.Status.AtProvider.StartTime == nil {
		t.Fatalf("P's Allocation is %+v (found %v), want it marked and started: the case did not arise", a, ok)
	}
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
		m.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}}
	})
	rel := opRigOp("rel", releaseSpec(pAlloc))
	w.create(rel)
	w.stepsUntil(time.Minute, 10*time.Second, "the Release planned or refused", func() bool {
		ph := w.getOp("rel").Status.Phase
		return ph == v1alpha1.OperationPhasePlanned || ph == v1alpha1.OperationPhaseRefused
	})
	if op := w.getOp("rel"); op.Status.Phase != v1alpha1.OperationPhasePlanned {
		c := apimeta.FindStatusCondition(op.Status.Conditions, v1alpha1.ConditionExecutable)
		msg := ""
		if c != nil {
			msg = c.Message
		}
		t.Fatalf("the Release is %q (%s): it must be plannable on a managed deployment held by a kept-warm job", op.Status.Phase, msg)
	}
	w.confirm("rel")
	w.untilPhase("rel", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("rel", "person", userPerson)
	w.releaseDone("rel")
	w.stepsUntil(5*time.Minute, 10*time.Second, "the deployment gone", func() bool { _, ok := w.deployment(); return !ok })
}

// 6. Deleting a FINISHED Operation never clears the mark (HBEP-10: "removing a finished Operation changes nothing"). A Scale to one
// leaves P's started pair serving; the Operation cannot give its count back (the spec write keeps failing), so it is still unfinished
// when the deployment's end time comes and the shutdown starts draining P. P's drain does not end (its end write is held back). The
// give-back then lands, the Operation finishes, and a person deletes it. The mark on P's Allocation STAYS, and when P's drain ends the
// Allocation is not released at the end time.
func TestDeletingAFinishedOperationWhileTheInstanceDrainsDoesNotClearTheMark(t *testing.T) {
	w := newExecWorldWith(t, time.Hour, func(m *v1alpha1.ModelDeployment) {
		m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	})
	pAlloc := w.pendingAlloc
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
		end := metav1.NewTime(w.clock.Now().Add(2 * time.Hour))
		m.Spec.EndTime = &end
	})
	failSpec, holdEnd := true, false
	w.fc.set(func(method string, obj client.Object) error {
		switch o := obj.(type) {
		case *v1alpha1.ModelDeployment:
			if method == "Update" && failSpec {
				return errors.New("injected: the write of the spec fails")
			}
		case *v1alpha1.Instance:
			if method == "Status.Update" && holdEnd && o.Status.Phase == v1alpha1.InstanceEnded {
				return errors.New("injected: the Instance's end is held back")
			}
		}
		return nil
	}, nil)
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	failSpec = false
	w.clock.Advance(10 * time.Second)
	_ = w.stepKeepingErrors() // the act pass: the spec lands, the pair is not taken, its final outcomes are recorded
	failSpec = true           // the give-back's write cannot land: the Operation stays unfinished
	holdEnd = true
	w.stepsUntil(3*time.Hour, 5*time.Minute, "the shutdown draining P", func() bool { return isDraining(w.instance(w.pending)) })
	failSpec = false
	w.stepsUntil(5*time.Minute, 10*time.Second, "the Scale finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	if in := w.instance(w.pending); !isDraining(in) || ended(in) {
		t.Fatalf("P is %q: it must still be draining: the case did not arise", in.Status.Phase)
	}
	if err := w.c.Delete(w.ctx, w.getOp("scale")); err != nil { // a person deletes the finished Operation
		t.Fatal(err)
	}
	w.passes(6, nil)
	if a, ok := w.allocation(pAlloc); !ok || !keptWarm(a) {
		t.Errorf("P's Allocation is %+v (found %v) after the finished Operation was deleted, want its mark to STAY: removing a finished Operation changes nothing", a, ok)
	}
	holdEnd = false
	released := false
	w.passes(40, func(i int) bool {
		if cause, deleted := w.log.deletedWith(pAlloc); deleted {
			released = true
			t.Errorf("pass %d after P's drain ended: P's started Allocation was RELEASED (cause %q) at the deployment's end time", i, cause)
			return true
		}
		return false
	})
	_ = released
}

// 7. An Operation whose approved plan releases a marked, started Allocation (kept warm by an earlier End) passes the mark where it
// blocks the release, once its spec write has landed, and never clears the mark. An End with delete commits to release P's Allocation.
//   - With a person's edit making its Delete conflict, the spec write does not land: P's Allocation is not released in any pass, and
//     when the End aborts SpecChangedSinceCommit its mark is still there.
//   - With no conflict, P's Allocation is released under the End, with the End's cause.
func TestAnApprovedReleaseOfAMarkedStartedAllocationPassesTheMarkOnlyOnceTheSpecWriteLandsAndNeverClearsIt(t *testing.T) {
	for _, conflict := range []bool{true, false} {
		name := "no conflict: released under the End"
		if conflict {
			name = "a person's edit makes the Delete conflict: aborted, still marked"
		}
		t.Run(name, func(t *testing.T) {
			w, pAlloc, stop := keptWarmWorld(t, false)
			stop()
			w.passes(12, nil)
			if a, ok := w.allocation(pAlloc); !ok || !keptWarm(a) || a.Status.AtProvider.StartTime == nil {
				t.Fatalf("P's Allocation is %+v (found %v), want it marked and started: the case did not arise", a, ok)
			}
			armed, edited := false, false
			w.fc.set(func(method string, obj client.Object) error {
				if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" && armed && !edited && conflict {
					edited = true
					w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 }) // a person's edit: the Delete conflicts
				}
				return nil
			}, nil)
			w.plan("end2", endSpec(true))
			w.confirm("end2")
			w.untilPhase("end2", v1alpha1.OperationPhaseAwaitingApproval)
			armed = true
			w.approve("end2", "person", userPerson)
			if conflict {
				released := false
				w.passes(30, func(i int) bool {
					if cause, deleted := w.log.deletedWith(pAlloc); deleted {
						released = true
						t.Errorf("pass %d: P's Allocation was RELEASED (cause %q) though the End that would have released it was undone by a person's edit", i, cause)
						return true
					}
					return false
				})
				if !edited {
					t.Fatal("the Delete never ran: the scenario did not arise")
				}
				if op := w.getOp("end2"); op.Status.FinishedAt == nil {
					t.Fatalf("the End is %q, not finished: the scenario did not arise", op.Status.Phase)
				}
				if a, ok := w.allocation(pAlloc); !released && (!ok || !keptWarm(a)) {
					t.Errorf("P's Allocation is %+v (found %v), want its mark still there: the End that would have released it was aborted", a, ok)
				}
				return
			}
			w.stepsUntil(5*time.Minute, 10*time.Second, "P's Allocation released", func() bool { _, ok := w.allocation(pAlloc); return !ok })
			w.wantCause(pAlloc, w.getOp("end2"))
		})
	}
}

// 8. While a deployment's delete waits on held-by-kept-warm, its status names the marked, started Allocations that hold it, with each
// one's hard end, and says an approved Release frees it. After a Release frees it, the delete completes and the reason is gone.
func TestWhileADeleteWaitsOnKeptWarmTheDeploymentsStatusNamesTheAllocationsThatHoldIt(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, true)
	stop()
	w.passes(60, nil)
	md, ok := w.deployment()
	if !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v), want it deleting and held: the case did not arise", md, ok)
	}
	c := apimeta.FindStatusCondition(md.Status.Conditions, v1alpha1.ConditionProgressing)
	a := w.allocOrNil(pAlloc)
	if c == nil || c.Reason != v1alpha1.ReasonHeldByKeptWarm || !strings.Contains(c.Message, pAlloc) || a == nil || a.Status.AtProvider.HardEnd == nil ||
		!strings.Contains(c.Message, a.Status.AtProvider.HardEnd.UTC().Format(time.RFC3339)) || !strings.Contains(c.Message, "Release") {
		t.Errorf("the deployment's Progressing condition is %+v, want reason HeldByKeptWarm naming %s with its hard end and an approved Release", c, pAlloc)
	}
	w.plan("rel", releaseSpec(pAlloc))
	w.confirm("rel")
	w.untilPhase("rel", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("rel", "person", userPerson)
	w.releaseDone("rel")
	w.stepsUntil(5*time.Minute, 10*time.Second, "the deployment gone", func() bool { _, ok := w.deployment(); return !ok })
}

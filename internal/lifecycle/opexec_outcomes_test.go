package lifecycle

// Rows that assert OUTCOMES, not a mechanism, so they stay valid whatever design the kept-warm question settles on (#707's round-2
// findings):
//   - an approved End completes: a started, healthy Allocation the plan releases (approved) is released by the End itself, and the
//     End finishes, rather than stalling behind a hold (Astra);
//   - a started job is never released when its target goes final while its Instance still lives, with and without a failed mark write
//     (Fable);
//   - a started job whose pending Instance an Operation ended, and whose Operation then finished SpecChangedSinceCommit across
//     restarts while the job was still pending, is not released by retention when the job starts (Astra).

import (
	"errors"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// An approved End completes. Two started, healthy Allocations under the default Keep retention and no profile maxIdle; the End's plan
// releases them (approved). The End commits, sets the end time to now and ends its Instances; the Allocations are released with the
// Operation's cause, the End finishes Done within a bounded number of passes, and nothing stays Executing, so the next Operation on the
// deployment can commit.
func TestAnApprovedEndThatReleasesStartedHealthyAllocationsCompletesAndReleasesThem(t *testing.T) {
	w := newExecWorld(t, 0) // both jobs started at once, both Instances ready
	allocs := []string{w.readyAlloc, w.pendingAlloc}
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.passes(180, func(int) bool { return w.getOp("end").Status.Phase == v1alpha1.OperationPhaseDone }) // up to half an hour
	op := w.getOp("end")
	if op.Status.Phase != v1alpha1.OperationPhaseDone {
		t.Errorf("the approved End is %q half an hour after its approval, want Done: it must complete, not stall until the hard end", op.Status.Phase)
	}
	for _, name := range allocs {
		if a, ok := w.allocation(name); ok && a.DeletionTimestamp.IsZero() {
			t.Errorf("Allocation %s is still there after the End: the plan approved releasing it", name)
		}
	}
	if op.Status.Phase == v1alpha1.OperationPhaseDone {
		for _, name := range allocs {
			w.wantCause(name, op)
		}
	}
}

// A started job is never released when its target goes final while its Instance still lives. Under an End, and under a Scale to zero,
// a pending pair I and A is committed and A is marked before any act; a delay lets A's job start and I turn Ready; the acting pass
// drains I, and A, which hosts I until the drain ends, is recorded final (JobStartedSinceCommit) and is then empty. A's started job is
// never released, except by maxIdle, its hard end or a failure release (the world has none), and the report agrees with the cluster.
func TestAStartedJobWhoseTargetWentFinalWhileItsInstanceLivedIsNeverReleased(t *testing.T) {
	for _, action := range []struct {
		name string
		spec v1alpha1.OperationSpec
	}{
		{"an End", endSpec(false)},
		{"a Scale to zero", scaleTo(0)},
	} {
		t.Run(action.name, func(t *testing.T) {
			w := newExecWorld(t, time.Hour)
			pAlloc := w.pendingAlloc
			stop := w.failSpecWrites()
			w.plan("op", action.spec)
			w.confirm("op")
			w.untilPhase("op", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("op", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("op").Status.Committed) > 0 })
			// A's job starts and I turns Ready while no act is made.
			w.stepsUntil(90*time.Minute, 2*time.Minute, "I ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
			stop()
			released := false
			w.passes(120, func(i int) bool {
				if cause, deleted := w.log.deletedWith(pAlloc); deleted {
					released = true
					t.Errorf("pass %d: A's started Allocation was RELEASED (cause %q): the approval was to cancel a queued job", i, cause)
					return true
				}
				return false
			})
			if released {
				return
			}
			if a := w.allocOrNil(pAlloc); a == nil || a.Status.AtProvider.StartTime == nil {
				t.Fatalf("A's Allocation is %+v: its job never started, the scenario did not arise", a)
			}
			if !w.endedOrGone(w.pending) {
				t.Fatalf("I is %s after the passes: its drain never ended, so the case did not arise", w.instanceState(w.pending))
			}
			w.reportAgreesWithTheCluster("op")
		})
	}
}

// The multi-fault release. A Scale from three to one ends the PENDING Instance A; A's Allocation cancel FAILS and so does the drain of
// the Operation's other target, so two Instances survive; the controller restarts and the Operation stays Executing; a person raises the
// count to two, matching the survivors. While A's job is still pending the value check finals the rest SpecChangedSinceCommit and the
// Operation finishes. The controller restarts after that final write, and A's job STARTS during the downtime. On resumption A is
// empty, unmarked and not held by an unfinished Operation, and the desired count is satisfied, so nothing places on it: retention
// (release-when-idle) must not release A's started job, except by maxIdle, its hard end or a failure release.
func TestAStartedJobWhoseOperationFinishedSpecChangedWhileItWasPendingIsNotReleasedWhenItStarts(t *testing.T) {
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{}, pfake.Script{}, pfake.Script{QueueWait: time.Hour})
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	for n := int32(1); n <= 3; n++ {
		if n > 1 {
			h.run(5*time.Minute, 30*time.Second)
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = n })
		}
		want := int(n)
		if n == 3 {
			want = 2 // the third job is queued: its Instance exists, and is not ready
		}
		h.until(10*time.Minute, 10*time.Second, "the Instances", func() bool { return h.readyCount(hoMD) == want && len(h.liveInstances(hoMD)) == int(n) })
	}
	w := &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	var aName, aAlloc string
	for _, in := range h.liveInstances(hoMD) {
		if in.Status.Phase != v1alpha1.InstanceReady {
			aName, aAlloc = in.Name, in.Spec.AllocationRef.Name
		}
	}
	if aName == "" {
		t.Fatal("the world has no pending Instance")
	}
	cancelAndDrainFail := true
	install := func() {
		w.fc.set(func(method string, obj client.Object) error {
			if !cancelAndDrainFail {
				return nil
			}
			switch o := obj.(type) {
			case *v1alpha1.Allocation:
				if o.Name == aAlloc && method == "Update" && o.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
					return errors.New("injected: the cancel of A's Allocation fails")
				}
			case *v1alpha1.Instance:
				if method == "Status.Update" && o.Name != aName && o.Status.Phase == v1alpha1.InstanceDraining {
					return errors.New("injected: the drain does not start")
				}
			}
			return nil
		}, nil)
	}
	install()
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })

	w.clock.Advance(10 * time.Second)
	_ = w.stepKeepingErrors() // A's Instance ends; the cancel and the drain fail
	if in := w.instanceOrNil(aName); in != nil && !ended(in) {
		t.Fatalf("A's Instance is %q after the pass, want it ended: the scenario did not arise", in.Status.Phase)
	}
	w.restart(false)
	install()
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 }) // a person raises the count, matching the survivors
	w.clock.Advance(10 * time.Second)
	_ = w.stepKeepingErrors() // the value check finals the rest SpecChangedSinceCommit
	if op := w.getOp("scale"); op.Status.FinishedAt == nil {
		t.Fatalf("the Scale is %q, not finished: the scenario did not arise", op.Status.Phase)
	}
	if a := w.allocOrNil(aAlloc); a == nil || a.Status.AtProvider.StartTime != nil {
		t.Fatalf("A's Allocation is %+v while its job should still be pending: the scenario did not arise", a)
	}

	// The controller restarts after that final write, and A's job starts during the downtime.
	cancelAndDrainFail = false
	w.restart(false)
	w.clock.Advance(90 * time.Minute)
	if err := w.prov.Sync(w.ctx); err != nil {
		t.Fatalf("provider sync: %v", err)
	}
	released := false
	w.passes(60, func(i int) bool {
		if cause, deleted := w.log.deletedWith(aAlloc); deleted {
			released = true
			t.Errorf("pass %d after the resumption: A's started Allocation was RELEASED (cause %q) by retention: only its queued cancellation was approved", i+1, cause)
			return true
		}
		return false
	})
	if a := w.allocOrNil(aAlloc); !released && (a == nil || a.Status.AtProvider.StartTime == nil) {
		t.Fatalf("A's Allocation is %+v: its job never started, the scenario did not arise", a)
	}
	w.reportAgreesWithTheCluster("scale")
}

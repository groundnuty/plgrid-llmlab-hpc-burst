package lifecycle

// The delta reviews of #664 (Codex Astra, Opus, Fable, 17 Nov 2026) found failure paths of the kept-warm design that no row covered.
// Under the owner's "protect first" design (HBEP-10, The report, Kept warm) each row asserts the outcome that design promises:
//   1. a mark write that fails before acting blocks the spec write and every act in that pass, the Operation stays Executing, and the
//      next pass retries it;
//   2. the finalizer stays while the End is unfinished, whatever write fails, and a restart between its add and the Delete call changes
//      nothing, since no path reads a "delete sent" record;
//   3. an End with delete that finishes without deleting leaves no finalizer, and a person's later delete is not held.

import (
	"errors"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// passes runs n passes of ten seconds each, calling check after every one with its index; the error of a pass is logged, since the
// rows inject failures.
func (w *execWorld) passes(n int, check func(i int) (stop bool)) {
	w.t.Helper()
	for i := 0; i < n; i++ {
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			w.t.Logf("pass %d: %v", i, err)
		}
		if check != nil && check(i) {
			return
		}
	}
}

// 1. The write of P's kept-warm mark FAILS from the commit on. Nothing else happens in those passes: the spec is not written (the end
// time is not set, the deployment is not deleted and carries no finalizer), no Instance is touched, nothing is released, no target is
// recorded, and the Operation stays Executing, not PartlyFailed. The first subtest leaves P queued; the second lets P's job start and
// its Instance turn Ready meanwhile. When the mark can be written, it lands, and the End proceeds with P's started job kept.
func TestAFailedKeptWarmMarkBlocksTheSpecWriteAndEveryActAndTheNextPassRetriesIt(t *testing.T) {
	for _, del := range []bool{false, true} {
		for _, started := range []bool{false, true} {
			name := "End without delete"
			if del {
				name = "End with delete"
			}
			if started {
				name += ", P's job starts and its Instance turns Ready meanwhile"
			} else {
				name += ", P stays queued"
			}
			t.Run(name, func(t *testing.T) {
				w := newExecWorld(t, time.Hour)
				pAlloc := w.pendingAlloc
				markFails := true
				w.fc.set(func(method string, obj client.Object) error {
					if a, ok := obj.(*v1alpha1.Allocation); ok && a.Name == pAlloc && method == "Update" && markFails && a.Annotations[v1alpha1.AnnotationKeptWarm] != "" {
						return errors.New("injected: the write of the kept-warm mark fails")
					}
					return nil
				}, nil)
				w.plan("end", endSpec(del))
				w.confirm("end")
				w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
				w.approve("end", "person", userPerson)
				w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
				blocked := func(where string) {
					t.Helper()
					if op := w.getOp("end"); op.Status.Phase != v1alpha1.OperationPhaseExecuting || len(op.Status.Targets) != 0 || apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
						t.Errorf("%s: the End is %q with %d outcomes, SpecWritten %v, want Executing with no outcome and no spec write: nothing happens until every mark has landed",
							where, op.Status.Phase, len(op.Status.Targets), apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten))
					}
					md, ok := w.deployment()
					if !ok || !md.DeletionTimestamp.IsZero() || md.Spec.EndTime != nil || controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
						t.Errorf("%s: the deployment is %+v (found %v), want it untouched: no end time, no finalizer, no delete", where, md, ok)
					}
					if in := w.instanceOrNil(w.pending); in == nil || ended(in) || isDraining(in) {
						t.Errorf("%s: P's Instance was acted on (%v)", where, in)
					}
					if cause, deleted := w.log.deletedWith(pAlloc); deleted {
						t.Errorf("%s: P's Allocation was RELEASED (cause %q) while its mark could not be written", where, cause)
					}
					if n := w.batch.Count("scancel"); n != 0 {
						t.Errorf("%s: %d scancel calls", where, n)
					}
				}
				if started {
					w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
				}
				w.passes(12, func(i int) bool { blocked("pass " + string(rune('A'+i))); return false })

				// The mark can be written now: it lands, and the End proceeds, with P's started job kept.
				markFails = false
				w.passes(40, nil)
				if a, ok := w.allocation(pAlloc); started && (!ok || !a.DeletionTimestamp.IsZero() || !keptWarm(a)) {
					t.Errorf("after the mark could be written P's Allocation is %+v (found %v), want it marked and kept", a, ok)
				}
				if op := w.getOp("end"); !apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
					t.Errorf("the End's spec write did not land after the mark did: %q", op.Status.Phase)
				}
			})
		}
	}
}

// 2. An End with delete deletes, and then FAILS to record SpecWritten, so no target is acted on. The finalizer must stay, since the End
// is committed and not finished; P's Allocation was marked at the commit, so once its job has started no rule releases it, and the
// deployment is still there when the record lands.
func TestTheKeptWarmFinalizerStaysUntilTheOperationsRecordAndTheMarksHaveLanded(t *testing.T) {
	w, pAlloc, _ := keptWarmWorld(t, true)
	recordFails := true
	w.fc.set(func(method string, obj client.Object) error {
		if op, ok := obj.(*v1alpha1.Operation); ok && method == "Status.Update" && recordFails && apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
			return errors.New("injected: the write that records SpecWritten fails")
		}
		return nil
	}, nil) // the finalizer's update and the Delete are let through
	gone := false
	w.passes(4, func(i int) bool {
		md, ok := w.deployment()
		if !ok {
			gone = true
			t.Errorf("pass %d: the deployment is GONE although the Operation's record (SpecWritten) has not landed and no target was acted on: the kept-warm finalizer was removed too early", i)
			return true
		}
		if md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
			t.Errorf("pass %d: the deployment is %+v, want it deleting and holding the kept-warm finalizer", i, md.ObjectMeta)
			return true
		}
		return false
	})
	// The record can land now: the next passes mark P and report.
	recordFails = false
	w.passes(20, nil)
	if cause, deleted := w.log.deletedWith(pAlloc); deleted {
		t.Errorf("P's started Allocation was RELEASED (cause %q, deployment gone %v): the approval was to cancel a queued job, and it is kept warm", cause, gone)
	}
	a, ok := w.allocation(pAlloc)
	if !ok || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationKeptWarm] == "" {
		t.Errorf("P's Allocation is %+v (found %v), want it marked and kept", a, ok)
	}
	if md, ok := w.deployment(); !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Errorf("the deployment is %+v (found %v), want it deleting and held by the finalizer while P is kept warm", md, ok)
	}
}

// 3. A kept-warm Allocation exists (a first End left P's job marked). A second End, with delete, adds its finalizer, and its Delete
// conflicts with a person's edit; the Operation finishes SpecChangedSinceCommit on a LIVE deployment. An End that finishes
// without deleting removes its finalizer as part of finishing. A person's later direct delete is then a person's: it is not held, and
// P's Allocation is released with retention's cause, not an Operation's.
func TestAnEndWithDeleteThatFinishesWithoutDeletingRemovesItsFinalizerAndAPersonsLaterDeleteIsNotHeld(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, false) // the first End: P is marked
	stop()
	w.passes(12, nil)
	if a, ok := w.allocation(pAlloc); !ok || a.Annotations[v1alpha1.AnnotationKeptWarm] == "" || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("P's Allocation is %+v (found %v) after the first End, want it marked and kept: the case is not exercised", a, ok)
	}

	armed, edited := false, false
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" && armed && !edited {
			edited = true
			// A person's edit after the pass read the deployment and added the finalizer: the Delete's resourceVersion conflicts.
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 })
		}
		return nil
	}, nil)
	w.plan("end2", endSpec(true))
	w.confirm("end2")
	w.untilPhase("end2", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end2", "person", userPerson)
	armed = true
	w.passes(8, func(int) bool { return w.getOp("end2").Status.FinishedAt != nil })
	if !edited {
		t.Fatal("the second End's Delete never ran: the scenario did not arise")
	}
	op := w.getOp("end2")
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Fatalf("the second End is %q finishedAt %v, want PartlyFailed and finished (SpecChangedSinceCommit): the case is not exercised", op.Status.Phase, op.Status.FinishedAt)
	}
	md, ok := w.deployment()
	if !ok || !md.DeletionTimestamp.IsZero() {
		t.Fatalf("the deployment is %+v (found %v), want it live: the Delete conflicted", md, ok)
	}
	if controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Errorf("the finished End left the kept-warm finalizer on a LIVE deployment: %v", md.Finalizers)
	}

	// The second End aborted before its Delete, so P's Allocation is still marked and nothing holds the deployment: a person's delete of
	// it completes at once (no finalizer holds, so the deletion rule releases P), and is not held by an orphan the aborted End would have
	// left.
	if err := w.c.Delete(w.ctx, md); err != nil { // a person's delete
		t.Fatal(err)
	}
	w.passes(20, func(int) bool { _, ok := w.deployment(); return !ok })
	if m, ok := w.deployment(); ok {
		t.Errorf("a person's delete of the deployment is HELD: it is %+v with finalizers %v", m.DeletionTimestamp, m.Finalizers)
	}
}

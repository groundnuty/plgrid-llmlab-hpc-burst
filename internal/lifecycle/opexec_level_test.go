package lifecycle

// Rows of the delta reviews of #664 and #707 about the ORDER of writes across passes, which the owner's "protect first" design closes
// by marking every committed CancelPending Allocation before any act (HBEP-10, The report, Kept warm). Each asserts what must be true
// of the cluster:
//   - a started job whose Operation left it with no live Instance is never released by retention or any rule except maxIdle, its
//     hard end and a failure release, and the report matches the cluster (Fable's pass-1 and pass-2 case);
//   - a postponed end that finishes the Operation SpecChangedSinceCommit does not let retention release the started job (Astra);
//   - the failure releases pass the mark; the finalizer is a level rule that no restart or failed write upsets, and needs no record of a
//     delete that was sent; the mark clears when an Instance is placed on the Allocation and when a pair serves on.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Fable's case. A Scale above zero, and an End, commit a CancelPending Instance whose job is pending. The Allocation was marked
// before the first act. In pass 1 the Instance is ended and the Allocation's cancel, its delete, FAILS (injected: the release cause's
// write, the first of a release). Before pass 2 the fake Slurm starts the job. Pass 2 sees a started job with no live Instance. The
// started Allocation is never released by retention or by any rule except maxIdle, its hard end or a failure release, and the
// report matches the cluster.
func TestAStartedJobWhoseCancelFailedAfterItsInstanceEndedIsNeverReleasedAndTheReportAgrees(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec v1alpha1.OperationSpec
		want int // the committed targets
	}{
		{"a Scale above zero", scaleTo(1), 2},
		{"an End", endSpec(false), 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newExecWorld(t, time.Hour)
			pAlloc := w.pendingAlloc
			cancelFails, attempts := true, 0
			w.fc.set(func(method string, obj client.Object) error {
				if a, ok := obj.(*v1alpha1.Allocation); ok && a.Name == pAlloc && method == "Update" && cancelFails && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
					attempts++
					return errors.New("injected: the write of the release cause fails, so the cancel is not made")
				}
				return nil
			}, nil)
			w.plan("op", tc.spec)
			w.confirm("op")
			w.untilPhase("op", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("op", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("op").Status.Committed) > 0 })
			if a := w.allocOrNil(pAlloc); a == nil || a.Status.AtProvider.StartTime != nil {
				t.Fatalf("P's Allocation is %+v at the commit, want it queued", a)
			}

			// Pass 1: the Instance is ended; the cancel fails.
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); err == nil {
				t.Log("pass 1 reported no error")
			}
			if attempts == 0 {
				t.Fatal("the Allocation's cancel was never attempted in pass 1: the scenario did not arise")
			}
			if in := w.instanceOrNil(w.pending); in != nil && !ended(in) {
				t.Fatalf("P's Instance is %q after pass 1, want it ended: the scenario did not arise", in.Status.Phase)
			}

			// Before pass 2 the job starts; pass 2 and the ones after it make no failing write. R's lease is moved out first, so that the
			// ninety minutes do not make the controller hand R over to a successor placed on the warm Allocation.
			cancelFails = false
			w.moveLeaseEnd(w.readyAlloc)
			w.clock.Advance(90 * time.Minute)
			released := false
			w.passes(60, func(i int) bool {
				if cause, deleted := w.log.deletedWith(pAlloc); deleted {
					released = true
					t.Errorf("pass %d after the start: P's started Allocation was RELEASED (cause %q): the approval was to cancel a queued job, and no rule but maxIdle, its hard end or an unconditional release releases it", i+2, cause)
					return true
				}
				return false
			})
			if a := w.allocOrNil(pAlloc); !released && (a == nil || a.Status.AtProvider.StartTime == nil) {
				t.Fatalf("P's Allocation is %+v: its job never started, the scenario did not arise", a)
			}
			w.reportAgreesWithTheCluster("op")
		})
	}
}

// Astra's case. An End empties a started-since-commit Allocation (its mark was written before the first act); then a person (or an
// agent: postponing the end is an allowed agent edit) POSTPONES the end. The next pass's value check fails every unfinished target
// SpecChangedSinceCommit and finishes the Operation. The empty Allocation then follows retention (release-when-idle here), and it must
// NOT be released, except by maxIdle, its hard end or a failure release. The second subtest puts a controller restart between the pass
// that landed the spec and the postponement.
func TestAPostponedEndThatFinishesTheOperationDoesNotLetRetentionReleaseTheStartedJob(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "no restart"
		if restart {
			name = "a controller restart between the spec's landing and the postponement"
		}
		t.Run(name, func(t *testing.T) {
			w := newExecWorldWith(t, time.Hour, func(m *v1alpha1.ModelDeployment) {
				m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
			})
			pAlloc := w.pendingAlloc
			stop := w.failSpecWrites()
			w.plan("end", endSpec(false))
			w.confirm("end")
			w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("end", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
			if a := w.allocOrNil(pAlloc); a == nil || !keptWarm(a) {
				t.Fatalf("P's Allocation is %+v after the commit, want it marked before any act", a)
			}
			w.stepsUntil(90*time.Minute, 2*time.Minute, "P's job started", func() bool {
				a := w.allocOrNil(pAlloc)
				return a != nil && a.Status.AtProvider.StartTime != nil
			})
			stop()

			// The pass that lands the spec: P's Instance is drained, and its Allocation is left to retention.
			w.clock.Advance(10 * time.Second)
			_ = w.stepKeepingErrors()
			if restart {
				w.restart(false)
			}
			// The end is postponed before the next pass: the effect the End committed to no longer holds.
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
				later := metav1.NewTime(w.clock.Now().Add(time.Hour))
				m.Spec.EndTime = &later
			})
			released := false
			w.passes(60, func(i int) bool {
				if cause, deleted := w.log.deletedWith(pAlloc); deleted {
					released = true
					t.Errorf("pass %d after the postponement: P's started Allocation was RELEASED (cause %q): the approval was to cancel a queued job", i+2, cause)
					return true
				}
				return false
			})
			if !released && !w.endedOrGone(w.pending) {
				t.Fatalf("P's Instance is %s: its drain never ended, so the case did not arise", w.instanceState(w.pending))
			}
			// The postponed End goes on as a deployment of two: its count then places an Instance on the warm Allocation, which clears the
			// mark. Either way the job is kept: marked, or hosting an Instance.
			if a, ok := w.allocation(pAlloc); !released && (!ok || !a.DeletionTimestamp.IsZero() || (!keptWarm(a) && len(w.liveOnAllocation(pAlloc)) == 0)) {
				t.Errorf("P's Allocation is %+v (found %v), want it kept: marked, or hosting an Instance placed on it", a, ok)
			}
			w.reportAgreesWithTheCluster("end")
		})
	}
}

// The failure releases pass the mark (HBEP-7, Reconcile loop, step 5, and Release): a marked, started Allocation whose Instance fails
// while its job runs, whose job was submitted as another, or whose job is lost, is released AT ONCE with the failure's own cause. The
// first two have the Operation's spec write failing, so the Operation has not acted; the third has its End landed and the Allocation
// empty.
func TestAFailureReleaseOfAMarkedStartedAllocationIsMadeAtOnceWithTheFailuresCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(w *execWorld, alloc string)
		want string // a word of the cause
	}{
		{"an Instance that failed while its job runs", func(w *execWorld, alloc string) {
			w.batch.Requeue(w.allocOrNil(alloc).Annotations[v1alpha1.AnnotationJobID])
		}, ""},
		{"an Allocation submitted as another", func(w *execWorld, alloc string) { readBackMark(w.t, w.rig, alloc) }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, pAlloc, _ := keptWarmWorld(t, false) // the spec write keeps failing: P is marked, started and Ready, and nothing is acted on
			w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
			if a := w.allocOrNil(pAlloc); a == nil || !keptWarm(a) {
				t.Fatalf("P's Allocation is %+v, want it marked", a)
			}
			tc.fail(w, pAlloc)
			released := false
			w.passes(30, func(int) bool { _, released = w.log.deletedWith(pAlloc); return released }) // five minutes
			if !released {
				t.Fatalf("P's marked Allocation was NOT released within five minutes of its failure: it is %+v", w.allocOrNil(pAlloc))
			}
			cause, _ := w.log.deletedWith(pAlloc)
			if ns, nm, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
				t.Errorf("the cause is the Operation's, %q (%s/%s %s): a failure's release carries the failure's own cause", cause, ns, nm, uid)
			}
		})
	}
	t.Run("a job that is lost, the Allocation empty", func(t *testing.T) {
		w, pAlloc, stop := keptWarmWorld(t, false)
		stop()
		w.passes(60, nil) // the End lands, P's Instance drains and ends: P's Allocation is empty, marked and kept
		a := w.allocOrNil(pAlloc)
		if a == nil || !keptWarm(a) || len(w.liveInstances(hoMD)) > 1 {
			t.Fatalf("P's Allocation is %+v, want it marked and empty: the case did not arise", a)
		}
		job := a.Annotations[v1alpha1.AnnotationJobID]
		w.batch.Vanish(job)
		for i := 0; i < 60 && w.allocOrNil(pAlloc) != nil && w.allocOrNil(pAlloc).Status.AtProvider.Phase != v1alpha1.AllocationLost; i++ {
			w.clock.Advance(time.Minute)
			if err := w.prov.Sync(w.ctx); err != nil {
				t.Fatalf("provider sync: %v", err)
			}
		}
		w.passes(20, nil)
		cause, released := w.log.deletedWith(pAlloc)
		if !released {
			t.Fatalf("P's marked Allocation was NOT released after its job was lost: it is %+v", w.allocOrNil(pAlloc))
		}
		if !strings.Contains(cause, "its job is Lost") {
			t.Errorf("the cause is %q, want retention's \"its job is Lost\"", cause)
		}
	})
}

// The finalizer is a level rule. An End with delete that finishes WITHOUT deleting (a person's edit made the Delete conflict, so it
// ends SpecChangedSinceCommit), whose finalizer REMOVAL write fails once, or with a controller restart between the finish and the
// removal: a later pass still removes the finalizer from the LIVE deployment, so a person's later delete is not held.
func TestAnEndWithDeleteThatFinishedWithoutDeletingStillHasItsFinalizerRemovedByALaterPass(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "the removal fails once"
		if restart {
			name = "a controller restart between the finish and the removal"
		}
		t.Run(name, func(t *testing.T) {
			w := newExecWorld(t, 10*time.Hour)
			armed, edited, removalFailed := false, false, false
			install := func() {
				w.fc.set(func(method string, obj client.Object) error {
					md, ok := obj.(*v1alpha1.ModelDeployment)
					if !ok {
						return nil
					}
					switch {
					case method == "Delete" && armed && !edited:
						edited = true
						w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 }) // a person's edit: the Delete conflicts
					case method == "Update" && !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) && edited && !removalFailed && !restart:
						removalFailed = true
						return errors.New("injected: the finalizer's removal fails")
					}
					return nil
				}, nil)
			}
			install()
			w.plan("end", endSpec(true))
			w.confirm("end")
			w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("end", "person", userPerson)
			armed = true
			w.passes(4, func(int) bool { return w.getOp("end").Status.FinishedAt != nil })
			if !edited {
				t.Fatal("the Delete never ran: the scenario did not arise")
			}
			if op := w.getOp("end"); op.Status.FinishedAt == nil {
				t.Fatalf("the End is %q, not finished: the scenario did not arise", op.Status.Phase)
			}
			if md, ok := w.deployment(); !ok || !md.DeletionTimestamp.IsZero() {
				t.Fatalf("the deployment is %+v (found %v), want it live", md, ok)
			}
			if restart {
				w.restart(false) // between the finish and the removal: the new controller's client makes no failing write
				w.fc.set(nil, nil)
			}
			w.passes(10, nil)
			md, ok := w.deployment()
			if !ok || controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
				t.Errorf("the kept-warm finalizer is still on the LIVE deployment ten passes after the End finished (found %v, restart %v, removal failed once %v): a person's later delete is held", ok, restart, removalFailed)
			}
		})
	}
}

// No path needs a record that the delete was SENT. The End is committed, its finalizer is added, and the controller restarts before the
// Delete call is sent (the call fails once, as a crash between the two would leave it). The finalizer stays while the End is
// unfinished, the Delete is then sent and the End proceeds, and the finalizer does not stay for ever: without a marked, started
// Allocation it comes off once the End finishes and the deployment goes; with one, it stays until the Allocation is released (here by
// the profile's maxIdle), and then comes off.
func TestARestartBetweenTheFinalizersAddAndTheDeleteKeepsTheFinalizerAndTheDeleteIsThenSentAndTheFinalizerDoesNotStayForever(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "no marked, started Allocation remains"
		if started {
			name = "a marked, started Allocation remains until maxIdle releases it"
		}
		t.Run(name, func(t *testing.T) {
			w := newExecWorld(t, time.Hour, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
			pAlloc := w.pendingAlloc
			deleteFails, deleteCalls := true, 0
			install := func() {
				w.fc.set(func(method string, obj client.Object) error {
					if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" {
						deleteCalls++
						if deleteFails {
							return errors.New("injected: the controller died before the Delete was sent")
						}
					}
					return nil
				}, nil)
			}
			install()
			w.plan("end", endSpec(true))
			w.confirm("end")
			w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
			w.approve("end", "person", userPerson)
			w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
			held := func(where string) {
				t.Helper()
				if md, ok := w.deployment(); !ok || !md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
					t.Errorf("%s: the deployment is %+v (found %v), want it live and holding the finalizer: the End is committed and unfinished", where, md, ok)
				}
			}
			held("after the commit pass")
			if deleteCalls == 0 {
				t.Fatal("the Delete was never attempted: the scenario did not arise")
			}
			if started {
				w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
			}
			w.restart(false)
			install()
			w.passes(6, func(i int) bool { held("after the restart"); return false })

			deleteFails = false
			w.passes(30, nil)
			if op := w.getOp("end"); op.Status.FinishedAt == nil {
				t.Fatalf("the End is %q, not finished after its Delete was sent", op.Status.Phase)
			}
			md, ok := w.deployment()
			if started {
				if !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
					t.Fatalf("the deployment is %+v (found %v), want it deleting and held while P's marked, started Allocation remains", md, ok)
				}
				if a, ok := w.allocation(pAlloc); !ok || !a.DeletionTimestamp.IsZero() || !keptWarm(a) {
					t.Fatalf("P's Allocation is %+v (found %v), want it marked and kept", a, ok)
				}
				w.stepsUntil(2*time.Hour, 30*time.Second, "maxIdle releases the Allocation and the deployment goes", func() bool { _, ok := w.deployment(); return !ok })
				if cause, _ := w.log.deletedWith(pAlloc); !strings.Contains(cause, "maxIdle") {
					t.Errorf("P's Allocation was released with the cause %q, want maxIdle's", cause)
				}
			} else if ok {
				t.Errorf("the deployment is still there after the End finished: %+v: the finalizer stays for ever", md.Finalizers)
			}
		})
	}
}

// An End with delete that FINISHES WITHOUT SENDING its delete leaves NO finalizer: the Delete call fails in the commit pass, and a
// person's edit then moves the spec before the next pass, so the End ends SpecChangedSinceCommit before any Delete went out.
func TestAnEndWithDeleteThatFinishesWithoutSendingItsDeleteLeavesNoFinalizer(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	deleteCalls := 0
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" {
			deleteCalls++
			return errors.New("injected: the Delete is not sent")
		}
		return nil
	}, nil)
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	if md, ok := w.deployment(); !ok || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v) after the commit pass, want it holding the finalizer: the scenario did not arise", md, ok)
	}
	attempts := deleteCalls
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 5 }) // a person's edit before the next pass
	w.passes(6, func(int) bool { return w.getOp("end").Status.FinishedAt != nil })
	if op := w.getOp("end"); op.Status.FinishedAt == nil || op.Status.Phase != v1alpha1.OperationPhasePartlyFailed {
		t.Fatalf("the End is %q finished %v, want PartlyFailed and finished (SpecChangedSinceCommit)", op.Status.Phase, op.Status.FinishedAt)
	}
	if deleteCalls != attempts {
		t.Errorf("%d Delete calls after the edit, want none: the End ended before sending its delete", deleteCalls-attempts)
	}
	w.passes(4, nil)
	if md, ok := w.deployment(); !ok || !md.DeletionTimestamp.IsZero() || controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Errorf("the deployment is %+v (found %v), want it live with NO finalizer: the End finished without sending its delete", md, ok)
	}
}

// A person's delete WHILE the finalizer holds waits (HBEP-10, The report, Kept warm): an End with delete is committed, its Delete not
// yet sent, and P's job has started, so P's Allocation is marked and started. A person deletes the deployment: it is deleting, held by
// the finalizer, and P's Allocation is NOT released by the deletion rule; it waits for the profile's maxIdle, and then the deployment
// goes.
func TestAPersonsDeleteWhileTheKeptWarmFinalizerHoldsWaitsForTheMarkedStartedAllocation(t *testing.T) {
	w := newExecWorld(t, time.Hour, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
	pAlloc := w.pendingAlloc
	deleteFails := true
	w.fc.set(func(method string, obj client.Object) error {
		if md, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" && deleteFails && md.Name == hoMD {
			return errors.New("injected: the End's Delete is not sent")
		}
		return nil
	}, nil)
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P's job started", func() bool {
		a := w.allocOrNil(pAlloc)
		return a != nil && a.Status.AtProvider.StartTime != nil
	})
	md, ok := w.deployment()
	if !ok || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v), want it holding the finalizer: the scenario did not arise", md, ok)
	}
	if err := w.c.Delete(w.ctx, md); err != nil { // a person's delete, while the finalizer holds
		t.Fatal(err)
	}
	released := false
	w.passes(60, func(i int) bool {
		if cause, deleted := w.log.deletedWith(pAlloc); deleted {
			released = true
			t.Errorf("pass %d: P's marked, started Allocation was RELEASED (cause %q) by a person's delete made while the finalizer holds: it waits", i, cause)
			return true
		}
		return false
	})
	if released {
		return
	}
	if md, ok := w.deployment(); !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Errorf("the deployment is %+v (found %v), want it deleting and held", md, ok)
	}
	deleteFails = false
	w.stepsUntil(2*time.Hour, 30*time.Second, "maxIdle releases P and the deployment goes", func() bool { _, ok := w.deployment(); return !ok })
	if cause, _ := w.log.deletedWith(pAlloc); !strings.Contains(cause, "maxIdle") {
		t.Errorf("P's Allocation was released with the cause %q, want maxIdle's", cause)
	}
}

// Cleared (a). A Scale to zero ends P's pair though P's job started since the commit: P's Instance drains, and its Allocation is kept
// marked, empty. After the Operation has finished a person raises the count to two, and the controller places an Instance on each warm
// Allocation. The mark on P's Allocation is cleared, since an Instance was placed on it, and the report still says what the Operation
// did: P's target failed JobStartedSinceCommit, with the words kept warm. A later clearing does not change it.
func TestTheMarkIsClearedWhenAnInstanceIsPlacedOnTheMarkedAllocationAndTheReportStillSaysKeptWarm(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	pAlloc := w.pendingAlloc
	stop := w.failSpecWrites()
	w.plan("scale", scaleTo(0))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	if a := w.allocOrNil(pAlloc); a == nil || !keptWarm(a) {
		t.Fatalf("P's Allocation is %+v after the commit, want it marked before any act", a)
	}
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.stepsUntil(10*time.Minute, 10*time.Second, "the Scale finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	w.passes(60, nil)
	if a, ok := w.allocation(pAlloc); !ok || !keptWarm(a) || len(w.liveOnAllocation(pAlloc)) != 0 {
		t.Fatalf("P's Allocation is %+v (found %v) with %d live Instances, want it marked, kept and empty: the case did not arise", a, ok, len(w.liveOnAllocation(pAlloc)))
	}
	o, ok := outcomeOf(w.getOp("scale"), pAlloc)
	if !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit || !strings.Contains(o.Message, "kept warm") {
		t.Fatalf("P's outcome is %+v (%v), want Failed with %s and the words kept warm", o, ok, reasonJobStartedSinceCommit)
	}
	reported := o

	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 }) // a person raises the count: Instances are placed
	w.stepsUntil(30*time.Minute, 10*time.Second, "an Instance placed on P's Allocation", func() bool { return len(w.liveOnAllocation(pAlloc)) > 0 })
	w.passes(6, nil)
	if a, ok := w.allocation(pAlloc); !ok || keptWarm(a) {
		t.Errorf("P's Allocation is %+v (found %v) with an Instance placed on it, want its mark cleared", a, ok)
	}
	if o, ok := outcomeOf(w.getOp("scale"), pAlloc); !ok || o.State != reported.State || o.Reason != reported.Reason || o.Message != reported.Message {
		t.Errorf("P's outcome is now %+v (%v), want what it was, %+v: the report is historical", o, ok, reported)
	}
}

// liveOnAllocation lists the live Instances of the deployment placed on the Allocation.
func (w *execWorld) liveOnAllocation(alloc string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range w.liveInstances(hoMD) {
		if in.Spec.AllocationRef.Name == alloc {
			out = append(out, in)
		}
	}
	return out
}

// Cleared, directly. An Allocation that hosts a live Instance and carries a mark of an Operation that is gone has its mark cleared in
// the next pass: an Instance is placed on it.
func TestTheMarkIsClearedWhenAnInstanceIsPlacedOnAMarkedAllocation(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	a := w.allocOrNil(w.readyAlloc)
	if a == nil || len(w.liveInstances(hoMD)) == 0 {
		t.Fatal("the world has no live Instance on R's Allocation")
	}
	next := a.DeepCopy()
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	next.Annotations[v1alpha1.AnnotationKeptWarm] = v1alpha1.ReleaseCauseOperation("example-ns", "earlier", "uid-earlier")
	if err := w.c.Update(w.ctx, next); err != nil {
		t.Fatal(err)
	}
	w.passes(3, nil)
	if got := w.allocOrNil(w.readyAlloc); got == nil || keptWarm(got) {
		t.Errorf("R's Allocation is %+v, want it present with its mark cleared: an Instance is placed on it", got)
	}
}

var _ = apimeta.IsStatusConditionTrue

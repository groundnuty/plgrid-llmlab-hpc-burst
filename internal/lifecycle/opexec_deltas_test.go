package lifecycle

// The final deltas of #707 (HBEP-10, The report, Kept warm; HBEP-7, steps 7 and 8), each asserted as an outcome:
//   - the guards key on "committed and not finished", PartlyFailed while a target is retried included: step 7's scale-down takes the
//     committed targets, and the by-value check runs;
//   - no Allocation and no Instance is created between the commit and the spec write;
//   - an Operation whose approved plan releases a marked, started Allocation clears its mark at its commit, and the release happens;
//   - a committed Operation is kept until it finishes by a finalizer: a person's delete of it waits;
//   - the report names a marked, started Allocation with its hard end where the profile sets no maxIdle.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A Release whose delete cannot be made is PartlyFailed and not finished, a failure its retry can fix. While it is, a person lowers the
// count: step 7's scale-down takes the Operation's committed targets, not a victim of its own, exactly as for an Executing one.
func TestAPartlyFailedReleaseThatIsRetriedStillHoldsBackTheControllersOwnScaleDown(t *testing.T) {
	w, serving, kept := newKeptThreeWorld(t)
	failing := true
	w.failAllocationWrites(kept, &failing)
	w.plan("release", releaseSpec(kept))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("release", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("release").Status.Committed) == 1 })
	w.passes(3, nil)
	if op := w.getOp("release"); op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt != nil {
		t.Fatalf("the Release is %q finished %v, want PartlyFailed and not finished: the case did not arise", op.Status.Phase, op.Status.FinishedAt)
	}
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	w.passes(12, nil)
	for _, n := range serving {
		if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("Instance %s is %q beside a PartlyFailed Release that is retried: step 7's own selection must take no victim", n, in.Status.Phase)
		}
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls beside the retried Release", got)
	}
}

// The by-value check runs on every pass of a PartlyFailed, unfinished Operation. A Scale to one whose queued cancel is REFUSED is
// PartlyFailed and retried; a person then raises the count above the plan's: the next pass fails every target not yet done with
// SpecChangedSinceCommit and finishes the Operation, rather than retrying a release the raise undid.
func TestTheValueCheckRunsOnAPartlyFailedOperationThatIsRetried(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	w.playCrossplane()
	w.batch.FailCancels(errors.New("scancel: slurm_kill_job: Socket timed out on send/recv operation"))
	w.approveScale("scale", 1, 2)
	w.stepsUntil(5*time.Minute, 10*time.Second, "the Scale partly failed and retried", func() bool {
		op := w.getOp("scale")
		return op.Status.Phase == v1alpha1.OperationPhasePartlyFailed && op.Status.FinishedAt == nil
	})
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	w.passes(8, func(int) bool { return w.getOp("scale").Status.FinishedAt != nil })
	op := w.getOp("scale")
	if op.Status.FinishedAt == nil {
		t.Fatalf("the Scale is %q, not finished: the by-value check did not run on a PartlyFailed Operation", op.Status.Phase)
	}
	found := false
	for _, o := range op.Status.Targets {
		if o.State == v1alpha1.TargetStateFailed && o.Reason == reasonSpecChangedSinceCommit {
			found = true
		}
	}
	if !found {
		t.Errorf("no target failed with %s: %+v", reasonSpecChangedSinceCommit, op.Status.Targets)
	}
}

// No Allocation and no Instance is created between an Operation's commit and its spec write. The spec write keeps failing, and R's job
// is requeued, so R's Instance fails and the deployment is short of a primary: the controller creates nothing for it until the spec has
// landed.
func TestNoAllocationOrInstanceIsCreatedBetweenTheCommitAndTheSpecWrite(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	w.failSpecWrites()
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	before := w.objectNames()
	w.batch.Requeue(w.allocOrNil(w.readyAlloc).Annotations[v1alpha1.AnnotationJobID]) // R's Instance fails
	w.passes(40, nil)
	if op := w.getOp("end"); op.Status.Phase != v1alpha1.OperationPhaseExecuting && op.Status.Phase != v1alpha1.OperationPhasePartlyFailed {
		t.Fatalf("the End is %q: the case did not arise", op.Status.Phase)
	}
	for name := range w.objectNames() {
		if !before[name] {
			t.Errorf("%s was created between the commit and the spec write", name)
		}
	}
}

// An Operation whose approved plan RELEASES a marked, started Allocation clears its mark at its commit, since that release is now priced
// and approved, and the release happens. An End left P's started Allocation marked; a Release of it is approved. The Allocation's own
// release is made to stall (its release cause's write fails), so the commit's clearing is seen on its own; once allowed, the Allocation
// is released with the Release's cause.
func TestAPlanThatReleasesAMarkedStartedAllocationClearsItsMarkAtItsCommitAndTheReleaseHappens(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, false)
	stop()
	w.passes(60, nil)
	if a, ok := w.allocation(pAlloc); !ok || !keptWarm(a) || a.Status.AtProvider.StartTime == nil {
		t.Fatalf("P's Allocation is %+v (found %v), want it marked and started: the case did not arise", a, ok)
	}
	causeFails := true
	w.fc.set(func(method string, obj client.Object) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && a.Name == pAlloc && method == "Update" && causeFails && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
			return errors.New("injected: the write of the release cause fails")
		}
		return nil
	}, nil)
	w.plan("rel", releaseSpec(pAlloc))
	w.confirm("rel")
	w.untilPhase("rel", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("rel", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("rel").Status.Committed) > 0 })
	w.passes(3, nil)
	if a, ok := w.allocation(pAlloc); !ok || !a.DeletionTimestamp.IsZero() || keptWarm(a) {
		t.Fatalf("P's Allocation is %+v (found %v) after the Release committed, want it present with its mark CLEARED at the commit", a, ok)
	}
	causeFails = false
	w.releaseDone("rel")
	w.wantCause(pAlloc, w.getOp("rel"))
}

// A committed Operation is kept until it finishes. Its finalizer is added at the commit; a person's delete of it while it is unfinished
// WAITS, the Operation staying with its committed targets; when it finishes the finalizer is removed and the delete completes.
func TestACommittedOperationIsKeptUntilItFinishesAndAPersonsDeleteOfItWaits(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	stop := w.failSpecWrites()
	w.approveScale("scale", 1, 2)
	op := w.getOp("scale")
	if !controllerutil.ContainsFinalizer(op, v1alpha1.FinalizerOperationUnfinished) {
		t.Fatalf("the committed Operation carries %v, want the %s finalizer added at its commit", op.Finalizers, v1alpha1.FinalizerOperationUnfinished)
	}
	committed := len(op.Status.Committed)
	if err := w.c.Delete(w.ctx, op); err != nil { // a person's delete of the unfinished Operation
		t.Fatal(err)
	}
	w.passes(10, nil)
	cur := w.getOp("scale")
	if cur.DeletionTimestamp.IsZero() || len(cur.Status.Committed) != committed || cur.Status.FinishedAt != nil {
		t.Fatalf("the Operation is %+v after a person's delete, want it deleting, unfinished and with its %d committed targets: the delete waits", cur.ObjectMeta, committed)
	}
	stop()
	var gone bool
	w.passes(40, func(int) bool {
		var o v1alpha1.Operation
		gone = apierrors.IsNotFound(w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: "scale"}, &o))
		return gone
	})
	if !gone {
		t.Errorf("the Operation is still there after it finished: %+v", w.getOp("scale").ObjectMeta)
	}
}

// The report names a marked, started Allocation with its hard end where the profile sets no maxIdle, so its requester can repurpose it
// or plan a Release; where the profile sets one, it names that.
func TestTheReportNamesAMarkedStartedAllocationWithItsHardEndWhereTheProfileSetsNoMaxIdle(t *testing.T) {
	for _, maxIdle := range []bool{false, true} {
		name := "no maxIdle"
		var opts []rigOption
		if maxIdle {
			name = "a maxIdle of 30 minutes"
			opts = append(opts, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
		}
		t.Run(name, func(t *testing.T) {
			w, pAlloc, stop := keptWarmWorld(t, false, opts...)
			stop()
			w.passes(30, nil)
			a, ok := w.allocation(pAlloc)
			if !ok || a.Status.AtProvider.HardEnd == nil {
				t.Fatalf("P's Allocation is %+v (found %v), want it started with a hard end", a, ok)
			}
			o, ok := outcomeOf(w.getOp("end"), pAlloc)
			if !ok || o.Reason != reasonJobStartedSinceCommit {
				t.Fatalf("P's outcome is %+v (%v), want Failed with %s", o, ok, reasonJobStartedSinceCommit)
			}
			hardEnd := a.Status.AtProvider.HardEnd.UTC().Format(time.RFC3339)
			switch {
			case maxIdle && (!strings.Contains(o.Message, "maxIdle, 30m0s") || strings.Contains(o.Message, "sets no maxIdle")):
				t.Errorf("the report is %q, want it to name the profile's maxIdle of 30m0s", o.Message)
			case !maxIdle && (!strings.Contains(o.Message, "sets no maxIdle") || !strings.Contains(o.Message, hardEnd)):
				t.Errorf("the report is %q, want it to say the profile sets no maxIdle and name the hard end %s", o.Message, hardEnd)
			}
		})
	}
}

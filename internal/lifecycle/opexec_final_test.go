package lifecycle

// A recorded final failure and the last-ready rule (#589, PR 5, the last review round; HBEP-10, The report, as amended by
// #665, #666 and #668). A target the report records as failed for good is never acted on again by any later pass, and no
// later release of its Allocation carries the Operation's cause; a final failure is recorded only by a pass that acts,
// never by one whose spec write failed or did not land; and a Scale to zero is the shutdown, so it is exempt from the
// last-ready rule as an End is. Each test opens the window between the commit and the act with the controller's own spec
// write failing (failSpecWrites), so every check waits on observable state, and a check that something did NOT happen
// waits on a sentinel Operation (passRanAfterNow).

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// newStartingFleetWorld is a deployment on A whose Instances are made one after the other, each on a job that has started
// (retention releases an empty Allocation at once, so a Scale plans the Allocation of an Instance it ends as ReleaseStarted):
// the i-th Instance's engine takes delays[i] to be ready, and with pending one more, Y, pends behind a queue that outlasts the
// test. insts are the started ones in the order they were made, the oldest first; a Scale takes the newest first.
func newStartingFleetWorld(t *testing.T, delays []time.Duration, pending bool) (w *execWorld, insts []string, y string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	started := 0
	h.engine = func(string) afake.Engine {
		started++
		return afake.Engine{StartDelay: delays[(started-1)%len(delays)]}
	}
	scripts := make([]pfake.Script, len(delays))
	if pending {
		scripts = append(scripts, pfake.Script{QueueWait: 10 * time.Hour})
	}
	h.batch.Enqueue(scripts...)
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	known := map[string]bool{}
	next := func(want int, startedJobs bool) string {
		h.until(10*time.Minute, 10*time.Second, fmt.Sprintf("%d Instances made", want), func() bool {
			live := h.liveInstances(hoMD)
			if len(live) != want {
				return false
			}
			for _, in := range live {
				if a, ok := h.allocation(in.Spec.AllocationRef.Name); startedJobs && (!ok || a.Status.AtProvider.StartTime == nil) {
					return false
				}
			}
			return true
		})
		for _, in := range h.liveInstances(hoMD) {
			if !known[in.Name] {
				known[in.Name] = true
				return in.Name
			}
		}
		t.Fatalf("no new Instance among %d", want)
		return ""
	}
	insts = append(insts, next(1, true))
	for n := 2; n <= len(delays); n++ {
		h.run(time.Minute, 10*time.Second) // each is made later than the one before, so that a Scale takes the newer
		h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = int32(n) })
		insts = append(insts, next(n, true))
	}
	if pending {
		h.run(time.Minute, 10*time.Second)
		h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = int32(len(delays) + 1) })
		y = next(len(delays)+1, false)
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, insts, y
}

// approveScale plans, confirms and approves a Scale to n, and returns once the Operation has committed to want targets.
func (w *execWorld) approveScale(name string, n int32, want int) *v1alpha1.Operation {
	w.t.Helper()
	planned := w.plan(name, scaleTo(n))
	w.confirm(name)
	w.untilPhase(name, v1alpha1.OperationPhaseAwaitingApproval)
	w.approve(name, "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp(name).Status.Committed) == want })
	return planned
}

// A Scale from four to two commits to end X and release its Allocation, and to cancel Y's pending job; R1 and R2 are kept and
// still starting. Y's cancel is REFUSED, so the Operation stays open and every pass acts again. The spec write fails first; X
// turns Ready while R1 and R2 do not, so when the write lands X is the last ready Instance, and X and its Allocation are
// recorded LastReadySinceCommit. Then a kept Instance turns Ready, and X is no longer the last ready one. No later pass acts
// on X: it is not drained, its Allocation is not deleted and its job is not cancelled, and the report still says what it
// recorded, while Y's target keeps being retried.
func TestARecordedFinalFailureIsNeverActedOnAgainWhenAKeptInstanceTurnsReady(t *testing.T) {
	w, fleet, y := newStartingFleetWorld(t, []time.Duration{20 * time.Minute, 20 * time.Minute, 6 * time.Minute}, true)
	r1, r2, x := fleet[0], fleet[1], fleet[2]
	w.playCrossplane()
	const slurmError = "scancel: slurm_kill_job: Socket timed out on send/recv operation"
	w.batch.FailCancels(errors.New(slurmError))
	stop := w.failSpecWrites()
	xAlloc, yAlloc := w.mustInstance(x).Spec.AllocationRef.Name, w.mustInstance(y).Spec.AllocationRef.Name
	xJob := w.allocOrNil(xAlloc).Annotations[v1alpha1.AnnotationJobID]
	if xJob == "" {
		t.Fatalf("X's Allocation %s names no job", xAlloc)
	}
	t.Cleanup(func() {
		if t.Failed() {
			for _, in := range w.instances(hoMD) {
				t.Logf("Instance %s: phase %q end reason %q", in.Name, in.Status.Phase, in.Status.EndReason)
			}
		}
	})

	planned := w.approveScale("scale", 2, 4)
	pt := planTargets(planned.Status.Plan)
	for target, want := range map[string]v1alpha1.PlanEffect{
		"Instance " + x: v1alpha1.PlanEffectEndStarting, "Allocation " + xAlloc: v1alpha1.PlanEffectReleaseStarted,
		"Instance " + y: v1alpha1.PlanEffectCancelPending, "Allocation " + yAlloc: v1alpha1.PlanEffectCancelPending,
	} {
		if pt[target] != want {
			t.Fatalf("the plan takes %s as %q, want %q: %v", target, pt[target], want, pt)
		}
	}
	// While the write fails nothing is acted on, and X turns Ready while the kept Instances do not.
	w.stepsUntil(30*time.Minute, 30*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(r1).Status.Phase == v1alpha1.InstanceReady || w.instance(r2).Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("a kept Instance is ready: X would not be the last ready one")
	}
	for _, o := range w.getOp("scale").Status.Targets {
		if o.State == v1alpha1.TargetStateFailed && failureIsFinal(o.Reason) {
			t.Fatalf("%+v: a pass whose spec write failed recorded a final failure", o)
		}
	}
	stop()
	w.stepsUntil(time.Minute, 10*time.Second, "X and its Allocation recorded as the last ready one", func() bool {
		a, okA := outcomeOf(w.getOp("scale"), x)
		b, okB := outcomeOf(w.getOp("scale"), xAlloc)
		return okA && okB && a.Reason == reasonLastReadySinceCommit && b.Reason == reasonLastReadySinceCommit
	})
	w.stepsUntil(time.Minute, 10*time.Second, "Y's cancel reported failing", func() bool {
		o, ok := outcomeOf(w.getOp("scale"), yAlloc)
		return ok && o.State == v1alpha1.TargetStateFailed && strings.Contains(o.Message, slurmError)
	})
	if op := w.getOp("scale"); op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt != nil {
		t.Fatalf("the Operation is %q with finishedAt %v, want PartlyFailed and open: Y's cancel is retried", op.Status.Phase, op.Status.FinishedAt)
	}

	// A kept Instance turns Ready: X is no longer the last ready one, which changes nothing the report recorded.
	w.stepsUntil(60*time.Minute, 2*time.Minute, "a kept Instance ready", func() bool {
		return w.instance(r1).Status.Phase == v1alpha1.InstanceReady || w.instance(r2).Status.Phase == v1alpha1.InstanceReady
	})
	w.passRanAfterNow(hoMD, "sentinel")
	if in := w.instance(x); isDraining(in) || ended(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("X is %q: its failure for good is recorded, so no later pass drains it", in.Status.Phase)
	}
	if a, ok := w.allocation(xAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Errorf("X's Allocation is %+v (found %v): it is left alone", a, ok)
	}
	if cause, deleted := w.log.deletedWith(xAlloc); deleted {
		t.Errorf("X's Allocation was deleted with the cause %q", cause)
	}
	for _, c := range w.batch.Calls() {
		if c.Op == "scancel" && strings.Contains(strings.Join(c.Args, " "), xJob) {
			t.Errorf("the fake Slurm saw %s %v: X's job is not cancelled", c.Op, c.Args)
		}
	}
	op := w.getOp("scale")
	for _, name := range []string{x, xAlloc} {
		if o, ok := outcomeOf(op, name); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonLastReadySinceCommit {
			t.Errorf("%s's outcome is %+v (%v), want it still Failed with %s", name, o, ok, reasonLastReadySinceCommit)
		}
	}
	if o, ok := outcomeOf(op, yAlloc); !ok || o.State != v1alpha1.TargetStateFailed || failureIsFinal(o.Reason) {
		t.Errorf("Y's Allocation is %+v (%v), want Failed and retried: its cancel is refused", o, ok)
	}
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt != nil {
		t.Errorf("the Operation is %q with finishedAt %v, want PartlyFailed and open while Y is retried", op.Status.Phase, op.Status.FinishedAt)
	}
}

// A Scale to one commits to end X, and R is kept. The spec write fails: X turns Ready, and then R turns Ready too, all before
// the write lands. A pass that could not act records nothing as final, so on the pass that does act X is not the last ready
// Instance: it is NOT recorded LastReadySinceCommit, it is drained as planned, and the Operation is Done.
func TestAPassThatCouldNotActRecordsNoLastReadyAndTheInstanceIsDrainedAsPlanned(t *testing.T) {
	w, fleet, _ := newStartingFleetWorld(t, []time.Duration{12 * time.Minute, 5 * time.Minute}, false)
	r, x := fleet[0], fleet[1]
	stop := w.failSpecWrites()
	xAlloc := w.mustInstance(x).Spec.AllocationRef.Name
	planned := w.approveScale("scale", 1, 2)
	if got := planTargets(planned.Status.Plan)["Instance "+x]; got != v1alpha1.PlanEffectEndStarting {
		t.Fatalf("the plan takes X as %q: %v", got, planTargets(planned.Status.Plan))
	}
	w.stepsUntil(30*time.Minute, 30*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(r).Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("R is ready already: X must be the last ready one for a while")
	}
	// Passes run while X is the last ready Instance and the write keeps failing.
	w.passRanAfterNow(hoMD, "sentinel")
	w.stepsUntil(30*time.Minute, 30*time.Second, "R ready", func() bool { return w.instance(r).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	op := w.getOp("scale")
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateDone || o.Reason != "" {
			t.Errorf("%s %s is %s (%s): X is drained as planned, and nothing failed", o.Kind, o.Name, o.State, o.Reason)
		}
	}
	if in := w.instanceOrNil(x); in != nil && !ended(in) && !isDraining(in) {
		t.Errorf("X is %q, want drained and ended", in.Status.Phase)
	}
	w.wantCause(xAlloc, op)
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}
}

// A Scale to ZERO is the shutdown, as an End is, so the last-ready rule does not apply (HBEP-10, The report, as amended by
// #668). Both Instances are starting, so both are committed as EndStarting with their Allocations; X turns Ready while the
// write fails. When it lands X is DRAINED under the Operation's cause, R is ended, and the Operation is Done with no failure.
func TestAScaleToZeroDrainsAnEndStartingInstanceThatTurnedReadyAndIsDone(t *testing.T) {
	w, r, x := newTwoStartingWorld(t)
	stop := w.failSpecWrites()
	xAlloc, rAlloc := w.mustInstance(x).Spec.AllocationRef.Name, w.mustInstance(r).Spec.AllocationRef.Name
	planned := w.approveScale("scale", 0, 4)
	pt := planTargets(planned.Status.Plan)
	if pt["Instance "+x] != v1alpha1.PlanEffectEndStarting || pt["Allocation "+xAlloc] != v1alpha1.PlanEffectReleaseStarted {
		t.Fatalf("the plan takes X as %q and its Allocation as %q: %v", pt["Instance "+x], pt["Allocation "+xAlloc], pt)
	}
	w.stepsUntil(15*time.Minute, 10*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	op := w.getOp("scale")
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateDone || o.Reason != "" {
			t.Errorf("%s %s is %s (%s), want Done: a Scale to zero drains every Instance", o.Kind, o.Name, o.State, o.Reason)
		}
	}
	w.wantCause(xAlloc, op)
	w.wantCause(rAlloc, op)
	if got := w.md(hoMD).Spec.Instances; got != 0 {
		t.Errorf("spec.instances is %d, want 0", got)
	}
	if evs := w.partlyFailedEvents("scale"); len(evs) != 0 {
		t.Errorf("%d OperationPartlyFailed events for an Operation that is Done: %+v", len(evs), evs)
	}
}

// A Scale to ZERO commits to cancel P's pending job. The write fails; P's job starts and P turns Ready. When the write lands
// P's target fails for good with its own reason, JobStartedSinceCommit (the approval was for a queued job), P's Instance is
// DRAINED, since the spec says zero, and P's Allocation follows RETENTION, with retention's own cause: not the Operation's.
func TestAScaleToZeroDrainsTheInstanceWhoseJobStartedAndItsAllocationFollowsRetention(t *testing.T) {
	// The Allocations are released the moment they are empty (retention), so P's is released by retention once its Instance ends.
	w := newExecWorldWith(t, time.Hour, func(m *v1alpha1.ModelDeployment) {
		m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(0)}
	}) // R drains from 1 h 50 min into its 4 h lease: it is still ready when P turns ready
	stop := w.failSpecWrites()
	w.approveScale("scale", 0, 4)
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	if a := w.allocOrNil(w.pendingAlloc); a == nil || a.Status.AtProvider.StartTime == nil || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("P's Allocation is %+v: its job should have started, and nothing should have released it", a)
	}
	stop()
	w.until(30*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	op := w.getOp("scale")
	if o, ok := outcomeOf(op, w.pendingAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit {
		t.Errorf("P's Allocation outcome is %+v (%v), want Failed with %s", o, ok, reasonJobStartedSinceCommit)
	}
	if o, ok := outcomeOf(op, w.pending); !ok || o.State != v1alpha1.TargetStateDone {
		t.Errorf("P's Instance outcome is %+v (%v), want Done: a Scale to zero drains it, and it has ended", o, ok)
	}
	for _, o := range op.Status.Targets {
		if o.Reason == reasonLastReadySinceCommit {
			t.Errorf("%s %s is Failed with %s: the rule does not apply to a Scale to zero", o.Kind, o.Name, o.Reason)
		}
	}
	if in := w.instanceOrNil(w.pending); in != nil && !ended(in) && !isDraining(in) {
		t.Errorf("P is %q, want drained and ended", in.Status.Phase)
	}
	w.until(30*time.Minute, 10*time.Second, "P's Allocation released by retention", func() bool {
		_, deleted := w.log.deletedWith(w.pendingAlloc)
		return deleted
	})
	cause, _ := w.log.deletedWith(w.pendingAlloc)
	if _, _, _, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok || cause == "" {
		t.Errorf("P's Allocation was released with the cause %q, want retention's own: the Operation did not release it", cause)
	}
}

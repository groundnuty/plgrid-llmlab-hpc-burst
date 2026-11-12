package alpha

// Row B's live flow (issue #376; nownooverlap.go): alpha-athena-now-nooverlap, driving the stack liveRun started (in its
// world) and returning what it saw. Nothing here judges: Run does (VerifyNowNoOverlap).

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// NowNoOverlap is alpha-athena-now-nooverlap: the predecessor ready and its warm start read; a stream held across its
// drain start; its job gone from the slot (its Instance drained and ended, then released after the idle limit); the slot
// read between the jobs (Q7); the successor's job, within its start wait from its Allocation's creation; the successor
// ready, its warm start read; then the deployment ended, and its jobs seen ended.
func (r *liveRun) NowNoOverlap(ctx context.Context, p Plan) (NowNoOverlapObs, error) {
	r.start(p)
	pre, err := r.firstReady(p)
	if err != nil {
		return NowNoOverlapObs{}, err
	}
	o := NowNoOverlapObs{Deployment: r.observedDeployment(), PredecessorTag: r.tagOf(pre)}
	end := r.nowNoOverlapSteps(ctx, p, pre, &o)
	if stopsTheRun(end) {
		return NowNoOverlapObs{}, end
	}
	// The order, the mark and the refusal are read from the whole of the watch, once it has caught up: a version it
	// delivered late still counts.
	if err := r.syncWatch(ctx, stack.KindInstances|stack.KindAllocations|stack.KindDeployments, "row B's result"); err != nil {
		return NowNoOverlapObs{}, notKnownBeside(err, end)
	}
	r.nowNoOverlapFromWatch(pre, &o)
	tags, err := r.runTags()
	if err != nil {
		return NowNoOverlapObs{}, err
	}
	o.Tags = tags
	return o, end
}

// nowNoOverlapSteps is row B's steps from the predecessor ready to the deployment's end. It returns ErrSlotTaken, a
// SecondNotStartedError, an error that stops the run, or nil.
func (r *liveRun) nowNoOverlapSteps(ctx context.Context, p Plan, pre v1alpha1.Instance, o *NowNoOverlapObs) error {
	if pre.Status.ReadyTime != nil {
		o.PredecessorReady = pre.Status.ReadyTime.Time
	}
	// The warm start, from the agent's own log of the job, read while the job runs: its release removes the files.
	_, files := r.identityOf(ctx, pre)
	o.WarmPredecessor = r.warmOf(ctx, pre, files)
	ds, ok := DrainStartOf(&pre, r.dw)
	if !ok {
		r.t.Fatal("the predecessor has no lease end")
	}
	o.DrainStart = ds
	key := r.fc.KeyFor(&pre)
	rate := r.rate(key)
	lead, err := r.streamLead(rate)
	if err != nil {
		return err
	}
	n, err := r.tokensFor(2*lead, rate)
	if err != nil {
		return err
	}
	r.untilBefore(ds, lead, "the stream across the drain start")
	s := r.openStream(n, key, "")
	<-s.first
	pr := r.startProber(ds)
	o.Streams = []Stream{r.finish(s)}
	// The predecessor's job leaves the slot: its Instance ends once the stream has, and ReleaseWhenIdle releases its
	// Allocation the idle limit later; by its lease end at the latest, and EndMargin for the controller to see it.
	predAlloc := pre.Spec.AllocationRef.Name
	r.nowPredecessorLeft(predAlloc, pre.Status.LeaseEnd.Time)
	succAlloc, outcome := r.nowSuccessorStart(ctx, p, predAlloc, &o.Slot)
	switch {
	case outcome.taken:
		o.NoReadyTarget = pr.end()
		r.stopGuard()
		p.Logf("%s: a job that is not the run's holds the one-job slot: the run ends its deployment and leaves that job alone", p.Row.Name)
		r.endDeployment(ctx, p)
		o.DeploymentEnded = true
		return ErrSlotTaken
	case outcome.notStarted != nil:
		o.NoReadyTarget = pr.end()
		return outcome.notStarted
	case outcome.failed != "":
		// A failure the verdict names from what the observation lacks: the run ends its deployment at once, so that the
		// controller makes no further job in the owner's slot.
		o.NoReadyTarget = pr.end()
		p.Logf("%s: %s; the run ends its deployment", p.Row.Name, outcome.failed)
		r.stopGuard()
		r.endDeployment(ctx, p)
		o.DeploymentEnded = true
		return nil
	}
	// Ready within health.startTimeout and its margin from the job's start. A sign that the controller would make a third
	// job (the successor's Instance ended unready, or a third Allocation of the run) ends the wait at once.
	succ, ready := r.successorOf(pre.Name, r.readyWait(), r.nowThirdJobSign(predAlloc, succAlloc))
	o.NoReadyTarget = pr.end()
	p.Logf("%s: the gap prober's replies by status and code: %v", p.Row.Name, pr.replies)
	if ready {
		if succ.Status.ReadyTime != nil {
			o.SuccessorReady = succ.Status.ReadyTime.Time
		}
		_, sfiles := r.identityOf(ctx, succ)
		o.WarmSuccessor = r.warmOf(ctx, succ, sfiles)
		// The guard has set the deployment's end at the successor's drain start; read it back before the run ends it.
		if err := r.guardResult(); err != nil && !errors.Is(err, ErrSecondNotStarted) {
			return err
		}
	} else {
		p.Logf("%s: the successor was not ready in time; the run ends its deployment", p.Row.Name)
		r.stopGuard()
	}
	r.endDeployment(ctx, p)
	o.DeploymentEnded = true
	return nil
}

// nowPredecessorLeft waits until the watch shows the predecessor's job gone from the slot as the controller counts it
// (LeftTheSlot), by its lease end and EndMargin.
func (r *liveRun) nowPredecessorLeft(predAlloc string, leaseEnd time.Time) {
	r.waitFor(time.Until(leaseEnd)+EndMargin, "the predecessor's job to leave the one-job slot", func() bool {
		for _, a := range r.watch.Allocations() {
			if a.Name == predAlloc && LeftTheSlot(a) {
				return true
			}
		}
		return false
	})
}

// successorOutcome is how the wait for the successor's job ended: started (all fields empty), the slot taken by a job
// that is not the run's, the start wait past with the job still waiting (a NOT RUN), or a failure the verdict names.
type successorOutcome struct {
	taken      bool
	notStarted error
	failed     string
}

// nowSuccessorStart is row B's wait for its successor's job once the predecessor's has left the slot (Q7 and the start
// wait). At the guard's cadence (r.every(), GuardEvery: a read of squeue --me on the shared login node no more often than
// the guard's own) it reads the watch for the successor's Allocation and a submission of it refused for the submit
// limit, then a fresh listing of the user's queue (squeue --me) for a job that is not the run's in the slot
// (ForeignInSlot), and for the successor's job:
//   - a job that is not the run's in the slot ends the wait at once: the slot is taken. The run never waits on that job
//     or touches it; only reads of the queue name it, and the run keeps a count;
//   - the successor's Allocation must come within SuccessorAllocationWait of the predecessor's leaving;
//   - its job must start within the row's start wait from the Allocation's creation, on this host's clock, as row A's
//     start wait runs from the deployment's (the API server's clock is this host's). Past it a fresh look decides, as at
//     the first clock: a job the scheduler shows waiting is ErrSecondNotStarted (NOT RUN, exit 3); a job never accepted
//     is a failure, as is a refusal for the submit limit with no job outside the run seen (slotChecks).
func (r *liveRun) nowSuccessorStart(ctx context.Context, p Plan, predAlloc string, so *SlotObs) (string, successorOutcome) {
	deadline, _ := stack.BoundedDeadline(r.t, SuccessorAllocationWait+p.Row.StartWait)
	left := time.Now()
	owned := RunOwned(r.prefix, r.tags)
	succAlloc, succTag := "", ""
	var created time.Time
	for {
		r.checkGuard()
		for _, a := range r.watch.Allocations() {
			if a.Name == predAlloc || a.Allocation == nil {
				continue
			}
			if succAlloc == "" {
				succAlloc, created = a.Name, a.Allocation.CreationTimestamp.Time
				succTag = v1alpha1.JobTag(a.Allocation.Namespace, a.Allocation.Name, a.Allocation.UID)
			}
			if a.Name == succAlloc && RefusedForTheSubmitLimit(a.Allocation) {
				so.Refused = true
			}
		}
		qctx, cancel := context.WithTimeout(ctx, time.Minute)
		js, err := r.batch.Queue(qctx)
		cancel()
		so.Reads++
		if err != nil {
			so.Failed++
		} else {
			if ForeignInSlot(p.Row, js, owned) > 0 {
				so.ForeignSeen = true
				if r.slotTakenFn != nil {
					r.slotTakenFn()
				}
				return succAlloc, successorOutcome{taken: true}
			}
			for _, j := range js {
				if succTag != "" && j.Name == succTag {
					so.Listed = true
					if jobStarted(j) {
						return succAlloc, successorOutcome{}
					}
				}
			}
		}
		now := time.Now()
		switch {
		case succAlloc == "" && now.Sub(left) > SuccessorAllocationWait:
			return "", successorOutcome{failed: fmt.Sprintf("the successor's Allocation was not created within %s of the predecessor's job leaving the slot", SuccessorAllocationWait)}
		case succAlloc != "" && !now.Before(created.Add(p.Row.StartWait)), !now.Before(deadline):
			return succAlloc, r.nowSuccessorPastStartWait(p, succTag)
		}
		// The poll only waits: the gap is the controller's own times (the successor's ReadyTime, the predecessor's recorded
		// lease end), and the start wait's end is decided by a fresh look (nowSuccessorPastStartWait), so a look up to one
		// interval late changes no verdict.
		time.Sleep(r.every())
	}
}

// nowSuccessorPastStartWait is the read at the successor's start wait's end: a fresh look, in which the successor's job is
// listed and waiting to start, is the NOT RUN of a second job that never started (the cancel by tag follows in Close,
// and noStartThroughTheCancel checks it); anything else is a failure that names it.
func (r *liveRun) nowSuccessorPastStartWait(p Plan, succTag string) successorOutcome {
	l, err := r.lookNow()
	if err != nil {
		return successorOutcome{failed: fmt.Sprintf("the successor's start wait ran out, and the queue could not be read: %v", err)}
	}
	for _, j := range l.Jobs {
		if j.Name != succTag {
			continue
		}
		if !WaitingToStart(j) {
			return successorOutcome{failed: fmt.Sprintf("the successor's start wait ran out with its job %s (reason %q), so it cannot be called one that never started", j.Phase, j.Reason)}
		}
		r.claimed = true
		if r.guard != nil {
			r.claimedPred = r.guard.pred
		}
		p.Logf("%s: the successor's job was still waiting %s after its Allocation's creation; the run ends and its jobs are cancelled", p.Row.Name, p.Row.StartWait)
		return successorOutcome{notStarted: &SecondNotStartedError{ReplacementLate: r.replacementLate(), StartWait: p.Row.StartWait}}
	}
	return successorOutcome{failed: "the successor's job was not accepted by the scheduler within its start wait"}
}

// nowThirdJobSign is successorOf's stop for row B: true once the successor's Instance has ended before it was ready (a
// start that failed: the Instance's phase is ended, whatever its reason), or the watch shows an
// Allocation of the run that is neither the predecessor's nor the successor's. A failed Instance's replacement is a new
// Allocation (hbep/0003-resource-model-and-naming/README.md:358), a third job in the owner's slot, so the run ends its
// deployment at once rather than waiting out the ready wait.
func (r *liveRun) nowThirdJobSign(predAlloc, succAlloc string) func() bool {
	return func() bool {
		for _, a := range r.watch.Allocations() {
			if a.Name != predAlloc && a.Name != succAlloc {
				return true
			}
		}
		for _, x := range r.instances() {
			if x.Spec.AllocationRef.Name == succAlloc && x.Status.Phase == v1alpha1.InstanceEnded {
				return true
			}
		}
		return false
	}
}

// nowNoOverlapFromWatch fills what the watch shows once it has caught up: NoOverlapSlot, the one-slot order on the
// controller's side (OneSlotOrder), the queued-successor mark, and a submission refused for the submit limit.
func (r *liveRun) nowNoOverlapFromWatch(pre v1alpha1.Instance, o *NowNoOverlapObs) {
	o.NoOverlapSlotSeen = SlotSeen(r.watch.Deployments())
	allocs := r.watch.Allocations()
	predAlloc, succAlloc := pre.Spec.AllocationRef.Name, ""
	for _, a := range allocs {
		if a.Name != predAlloc {
			succAlloc = a.Name
			break
		}
	}
	if succAlloc == "" {
		return
	}
	leftAt, createdAt, ok := OneSlotOrder(allocs, predAlloc, succAlloc)
	o.OrderSeen, o.CreatedAfterLeft = ok, ok && leftAt < createdAt
	for _, a := range allocs {
		if a.Name != succAlloc || a.Allocation == nil {
			continue
		}
		if _, marked := a.Allocation.Annotations[v1alpha1.AnnotationQueuedSuccessor]; marked {
			o.SuccessorMarked = true
		}
		if RefusedForTheSubmitLimit(a.Allocation) {
			o.Slot.Refused = true
		}
	}
}

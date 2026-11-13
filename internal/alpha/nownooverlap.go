package alpha

// Row B of issue #376, alpha-athena-now-nooverlap: HBEP-7's no-overlap mode on the one-slot path, on Athena's plgrid-now
// under profiles/athena-now.yaml (the owner's decisions of 11 Nov 2026, and the science agent's rulings Q3 and Q7).
//
//	hbep/0007-lifecycle-controller/README.md:482: "when the effective submit limit is 1, so no slot is free, the successor is not submitted early. Step 7 creates its Allocation once the predecessor's job has left the slot: its phase is `Ended` with `jobCompleting` false, or its deletion has completed"
//	hbep/0007-lifecycle-controller/README.md:484: "the gap is the time from the primary's drain start until its successor is `ready`. ... On the one-slot path the successor's whole queue wait is added."
//	hbep/0007-lifecycle-controller/README.md:639: "on Athena `plgrid-now` under `profiles/athena-now.yaml` ..., one deployment with `ReleaseWhenIdle`, a short `idleLimit` and a stream in flight at the drain start goes through no-overlap mode on the one-slot path end to end through the front, with no request cut. The successor's Allocation is created only after the predecessor's job has left the slot, and the gap from the drain start to the successor's `ready`, its queue wait included, is measured and recorded. A run with no stream in flight at the drain start proves nothing here;"
//
// Two jobs in turn on the owner's one-job slot, each of a 21-minute lease, both starting warm from the pre-staging row's
// staging (the ruling 1(b)). The run reads the slot between them (Q7): a job that is not the run's in it is the owner's
// own work, which the run never waits on or touches; it ends the deployment, and the row is NOT RUN.

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The row's figures.
const (
	// NowNoOverlapLease is each job's leaseLimit: athena-now.yaml's lead of 20 minutes plus one, the shortest lease the
	// controller places (time limits are whole minutes, and must be longer than the lead).
	NowNoOverlapLease = 21 * time.Minute
	// NowNoOverlapBudget is the row's run budget, both start waits included, and the script's timeout for it (the shape's
	// timeout column, as for the other rows with a start wait): above the row's worst case with its factor
	// (TestEachStackRowsWorstCaseIsItsWaitsAddedUp; hack/livechecks, TestEveryStackRowsTimeoutIsAboveItsWorstCase).
	NowNoOverlapBudget = 115 * time.Minute
	// SuccessorAllocationWait bounds the wait, once the predecessor's job has left the slot, for the controller to create
	// the successor's Allocation: one pass of the lifecycle controller.
	SuccessorAllocationWait = stack.WaitDeadline
	// nowEndAfterIdle is what the end of the run takes beyond the idle limit, once the deployment is deleted: the drain
	// ends on the agent's next report with no request running (internal/lifecycle, drainStep), and the cancel of the job
	// once released. The release itself waits the idle limit even then: ReleaseWhenIdle holds after the deployment's
	// deletion (internal/lifecycle, retention.go; keepUntil applies to Keep alone).
	nowEndAfterIdle = time.Minute
)

// NowNoOverlapRow is row B: two jobs of one GPU in turn on plgrid-now, no QOS (plgrid-now's own applies, as on row A),
// ReleaseWhenIdle with the live idle limit, the owner's go of plgrid-now (Q6), and a slot hold of the bounds its run keeps
// on a pass (Q3): the first start wait, the predecessor's lease, the successor's Allocation, its own start wait, its start
// to ready, and the end (the idle limit and a minute): 10 + 21 + 3 + 10 + 12 + 3 = 59 minutes, inside the hour
// (TestRowBsSlotHoldIsAtLeastItsOwnBounds; the Fable review of PR #406).
func NowNoOverlapRow() Row {
	return Row{Name: "alpha-athena-now-nooverlap", Cluster: "athena", Profile: "athena-now", Partition: "plgrid-now", GPUs: 1,
		Lease: NowNoOverlapLease, Budget: NowNoOverlapBudget,
		SlotHold:  NowStartWait + NowNoOverlapLease + SuccessorAllocationWait + NowStartWait + NowStartTimeout + IdleLimit + nowEndAfterIdle,
		StartWait: NowStartWait, MostJobs: 2, PostsTo: "#145 and #376", Timing: AthenaNowTiming(), OwnerGo: OwnerGoAthenaNow,
		ReleaseWhenIdle: true}
}

// ErrSlotTaken is what row B's scenario returns, beside what it saw, when a job that is not the run's held the one-job
// slot between the row's two jobs (the science agent's ruling Q7 on #376): the owner's own work. The run ended its
// deployment and never touched that job; the one-slot handover was never put to the test. Run judges the predecessor's
// part, and a failure there wins; with none, the run is NOT RUN (NotRun), its record and text to post written.
var ErrSlotTaken = errors.New("a job that is not the run's held the one-job slot between the row's two jobs")

// slotTakenWords say what happened, in the marker, which hack/live-checks.sh reads for row B, and in the text to post.
const slotTakenWords = "a job that is not the run's held the one-job slot between the row's two jobs: the owner's own work, left untouched; the run ended its deployment"

// HoldsSlot reports whether a job of the user's queue listing holds the row's one-job slot, as the script's slot guard
// counts it (hack/live-checks.sh, slot_jobs: a job of the slot's partition or QOS). provider.Job carries no QOS, so the
// partition decides: on plgrid-now the slot is the partition. A job that has ended holds it no more, unless it is still
// completing, which the scheduler and the controller's slot count both still count (HBEP-4, Phases, A completing job);
// a listing keeps an ended job for MinJobAge.
func HoldsSlot(row Row, j provider.Job) bool {
	if j.Partition != row.Partition {
		return false
	}
	switch j.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed:
		return j.Completing
	}
	return true
}

// ForeignInSlot counts the jobs of a listing that hold the row's slot and are not the run's own (Owned): the owner's
// own work. It names none: the count is all the run keeps of them.
func ForeignInSlot(row Row, jobs []provider.Job, owned Owned) int {
	n := 0
	for _, j := range jobs {
		if HoldsSlot(row, j) && !owned(j.Name) {
			n++
		}
	}
	return n
}

// SlotObs is what row B read of its slot between its jobs (Q7), from the moment the predecessor's job left the slot as
// the controller counts it, until the successor's job was listed in the queue.
type SlotObs struct {
	// Reads counts the reads of the slot the run made; Failed those that could not be read.
	Reads  int `json:"reads"`
	Failed int `json:"failed"`
	// ForeignSeen is true when a read listed a job that is not the run's in the slot: the owner's own work, by count
	// alone, never its ID or name.
	ForeignSeen bool `json:"foreignSeen"`
	// Refused is true when the successor's Allocation recorded a submission the scheduler refused for the submit
	// limit (SubmissionRecord.Code, ReasonSubmitLimitReached); the run then read the slot again at once.
	Refused bool `json:"refused"`
	// Listed is true when the successor's job was seen in the queue: from then on the slot was the run's.
	Listed bool `json:"listed"`
}

// slotChecks: a successor refused for the submit limit while no job outside the run was ever seen in the slot is a
// failure (Q7: "anything else is a FAIL"), since the run's own count let it be refused. With a foreign job seen, the
// refusal is the owner's slot use, ErrSlotTaken's NOT RUN, and no failure here.
func slotChecks(s SlotObs) []string {
	if s.Refused && !s.ForeignSeen {
		return []string{"the successor's submission was refused for the submit limit while no job outside the run was seen in the slot"}
	}
	return nil
}

// RefusedForTheSubmitLimit reports whether an Allocation's submission record is a refusal for a full submit limit: the
// state Rejected with Code ReasonSubmitLimitReached, which the Provider's controller writes from the scheduler's reason
// name (api/v1alpha1, SubmissionRecord.Code).
func RefusedForTheSubmitLimit(a *v1alpha1.Allocation) bool {
	s := a.Status.AtProvider.Submission
	return s != nil && s.State == v1alpha1.SubmissionRejected && s.Code == v1alpha1.ReasonSubmitLimitReached
}

// LeftTheSlot reports whether an Allocation version shows its job gone from the slot as the lifecycle controller counts
// it (hbep/0007-lifecycle-controller/README.md:482): its phase Ended with jobCompleting false, or its deletion complete.
func LeftTheSlot(o stack.AllocObservation) bool {
	if o.Deleted {
		return true
	}
	return o.Phase == v1alpha1.AllocationEnded && o.Allocation != nil && !o.Allocation.Status.AtProvider.JobCompleting
}

// OneSlotOrder is the controller's side of the one-slot order, from the watch's Allocation versions in the order it
// delivered them (one watch, so the API's order): the index of the first version of the predecessor that shows it left
// the slot (LeftTheSlot), and of the first version of the successor. ok is false when either is missing.
func OneSlotOrder(allocs []stack.AllocObservation, pred, succ string) (left, created int, ok bool) {
	left, created = -1, -1
	for i, o := range allocs {
		if o.Name == pred && left < 0 && LeftTheSlot(o) {
			left = i
		}
		if o.Name == succ && created < 0 {
			created = i
		}
	}
	return left, created, left >= 0 && created >= 0
}

// NowNoOverlapObs is a run of row B.
type NowNoOverlapObs struct {
	Tags []string `json:"tags"`
	// PredecessorTag is the predecessor's job tag among Tags; the other is the successor's.
	PredecessorTag string `json:"predecessorTag"`
	// Deployment is what the run's ModelDeployment held: athena's Provider alone, ReleaseWhenIdle and the lease.
	Deployment DeploymentSpec `json:"deployment"`
	// Streams are the streams held across the predecessor's drain start.
	Streams []Stream `json:"streams"`
	// PredecessorReady and SuccessorReady are when each Instance became ready; DrainStart the predecessor's drain start,
	// from its recorded lease end. SuccessorReady less DrainStart is the gap.
	PredecessorReady time.Time `json:"predecessorReady"`
	DrainStart       time.Time `json:"drainStart"`
	SuccessorReady   time.Time `json:"successorReady"`
	// NoOverlapSlotSeen is true when the deployment carried the NoOverlapSlot reason (HBEP-7, No-overlap mode).
	NoOverlapSlotSeen bool `json:"noOverlapSlotSeen"`
	// NoReadyTarget counts the new requests the front answered with 503 no_ready_target during the gap: recorded, not
	// judged.
	NoReadyTarget int `json:"noReadyTarget"`
	// OrderSeen is true when the watch showed both the predecessor's leaving the slot and the successor's Allocation,
	// and CreatedAfterLeft when the successor's first version came after the predecessor's leaving (OneSlotOrder).
	OrderSeen        bool `json:"orderSeen"`
	CreatedAfterLeft bool `json:"createdAfterLeft"`
	// SuccessorMarked is true when the successor's Allocation carries the queued-successor mark, which the one-slot path
	// never sets (hbep/0007-lifecycle-controller/README.md:478, "The cases").
	SuccessorMarked bool `json:"successorMarked"`
	// Slot is what the run read of the slot between the jobs (Q7).
	Slot SlotObs `json:"slot"`
	// WarmPredecessor and WarmSuccessor are each job's warm start as its agent's own log showed it (the ruling 1(b)).
	WarmPredecessor WarmObs `json:"warmPredecessor"`
	WarmSuccessor   WarmObs `json:"warmSuccessor"`
	// DeploymentEnded is true when the run ended its deployment at the row's end and saw every job of the run end.
	DeploymentEnded bool `json:"deploymentEnded"`
}

// Gap is the time from the predecessor's drain start until its successor is ready; zero when either end was not seen.
func (o NowNoOverlapObs) Gap() time.Duration {
	if o.DrainStart.IsZero() || o.SuccessorReady.IsZero() {
		return 0
	}
	return o.SuccessorReady.Sub(o.DrainStart)
}

// VerifyNowNoOverlap judges alpha-athena-now-nooverlap: the predecessor's part (nowNoOverlapFirstJobChecks), two
// Allocations, the one-slot order on both sides, the gap, the slot reads, and the deployment ended.
func VerifyNowNoOverlap(row Row, o NowNoOverlapObs, jobs []provider.Job) []string {
	f := nowNoOverlapFirstJobChecks(row, o, jobs)
	f = append(f, tagCountChecks(row, o.Tags, "a no-overlap run makes exactly two Allocations: a predecessor and a successor")...)
	if o.SuccessorReady.IsZero() {
		f = append(f, "the successor was not seen ready")
	}
	if o.Gap() <= 0 {
		f = append(f, "the gap was not measured: the drain start and the successor's ready are not both seen, in that order")
	}
	switch {
	case !o.OrderSeen:
		f = append(f, "the watch did not show both the predecessor's leaving the slot and the successor's Allocation")
	case !o.CreatedAfterLeft:
		f = append(f, "the successor's Allocation was created before the predecessor's job had left the slot")
	}
	if o.SuccessorMarked {
		f = append(f, "the successor's Allocation carries the queued-successor mark, which the one-slot path never sets")
	}
	lf, by := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	if sucTag := o.successorTag(); sucTag != "" {
		pre, okP := by[o.PredecessorTag]
		suc, okS := by[sucTag]
		if okP && okS {
			f = append(f, oneSlotChecks(pre, suc)...)
		}
	}
	if !o.DeploymentEnded {
		f = append(f, "the deployment was not seen ended, with every job of the run, at the row's end")
	}
	return dedupe(f)
}

// nowNoOverlapFirstJobChecks are row B's checks that hold whether or not the successor ever ran: the deployment, the
// predecessor ready, the stream across its drain start, NoOverlapSlot (set from the drain start on, before any
// successor), the cluster's record of the jobs, and the slot reads. The one-slot order and the gap were never put to the
// test when the successor never started, or the owner took the slot.
func nowNoOverlapFirstJobChecks(row Row, o NowNoOverlapObs, jobs []provider.Job) []string {
	f := deploymentChecks(row, o.Deployment)
	if o.PredecessorReady.IsZero() {
		f = append(f, "the predecessor was not seen ready")
	}
	// The predecessor serves a stream that started before its drain start and ends after it, with no cut
	// (hbep/0007-lifecycle-controller/README.md:622: "A run with no stream in flight at the drain start proves nothing here").
	f = append(f, streamChecks(o.Streams, o.DrainStart)...)
	if !o.NoOverlapSlotSeen {
		f = append(f, "the deployment never showed NoOverlapSlot")
	}
	// The cluster's record of the jobs that were made: the predecessor's always, and a successor's whose submission was
	// refused, or never sent, has none to judge (the owner took the slot). The full verdict judges every tag.
	if o.PredecessorTag == "" {
		f = append(f, "the predecessor's job tag was not read")
	}
	by := jobsByTag(jobs)
	var made []string
	for _, t := range o.Tags {
		if _, ok := by[t]; ok || t == o.PredecessorTag {
			made = append(made, t)
		}
	}
	lf, _ := ledgerChecks(row, made, jobs)
	f = append(f, lf...)
	return append(f, slotChecks(o.Slot)...)
}

// successorTag is the tag of Tags that is not the predecessor's, when there are exactly two.
func (o NowNoOverlapObs) successorTag() string {
	if len(o.Tags) != 2 {
		return ""
	}
	for _, t := range o.Tags {
		if t != o.PredecessorTag {
			return t
		}
	}
	return ""
}

// oneSlotChecks judges, from the cluster's own times alone (one clock), that the successor was submitted only after the
// predecessor's job had ended, and so never queued or ran beside it: the one-slot path's order, the opposite of the
// queued path's (queuedAheadChecks).
func oneSlotChecks(pre, suc provider.Job) []string {
	if suc.SubmitTime.IsZero() || pre.EndTime == nil || suc.StartTime == nil {
		return []string{"the cluster's times for the two jobs are incomplete"}
	}
	var f []string
	if suc.SubmitTime.Before(*pre.EndTime) {
		f = append(f, "the successor was submitted before the predecessor's job ended")
	}
	if suc.StartTime.Before(*pre.EndTime) {
		f = append(f, "the successor started before the predecessor's job ended")
	}
	return f
}

// OneSlotQueueWait is the successor's queue wait on the one-slot path, its start less its submission, and the time from
// the predecessor's end to the successor's submission, both from the cluster's own record; ok is false when the record
// lacks a time.
func OneSlotQueueWait(pre, suc provider.Job) (queue, toSubmit time.Duration, ok bool) {
	if pre.EndTime == nil || suc.StartTime == nil || suc.SubmitTime.IsZero() {
		return 0, 0, false
	}
	return suc.StartTime.Sub(suc.SubmitTime), suc.SubmitTime.Sub(*pre.EndTime), true
}

// nowNoOverlapNotRun is what ends a run of row B NOT RUN when nothing failed: the owner's job in the slot (the scenario's
// ErrSlotTaken), and a warm start an agent's own log did not show, each job's on its own; all that hold. A job that
// never ran shows no warm start to judge: the predecessor's is judged once it was ready, the successor's once it was.
func nowNoOverlapNotRun(o NowNoOverlapObs, scenarioErr error) error {
	var errs []error
	if errors.Is(scenarioErr, ErrSlotTaken) {
		errs = append(errs, ErrSlotTaken)
	}
	if !o.PredecessorReady.IsZero() {
		if w := warmStartShownByTheAgentsOwnLog(o.WarmPredecessor); w != nil {
			w.Job = "the predecessor's job"
			errs = append(errs, w)
		}
	}
	if !o.SuccessorReady.IsZero() {
		if w := warmStartShownByTheAgentsOwnLog(o.WarmSuccessor); w != nil {
			w.Job = "the successor's job"
			errs = append(errs, w)
		}
	}
	return errors.Join(errs...)
}

// nowNoOverlapPost is row B's text to post: counts, durations and fixed words, no job ID, name or path.
func nowNoOverlapPost(row Row, o NowNoOverlapObs, failures []string, jobs []provider.Job, notRun error) string {
	var b strings.Builder
	head := verdictLine(row, failures)
	if len(failures) == 0 && notRun != nil {
		head = fmt.Sprintf("%s: NOT RUN (posts to %s): %s", row.Name, row.PostsTo, notRunWhy(notRun))
	}
	n, cut := inFlight(o.Streams, o.DrainStart)
	fmt.Fprintf(&b, "%s\n%s; ReleaseWhenIdle %t, idleLimit %s\n- gap from the drain start to the successor ready, its queue wait included: %s\n- streams in flight at the drain start: %d of %d; cut: %d; NoOverlapSlot seen: %t; new requests answered 503 no_ready_target: %d\n",
		head, deploymentLine(o.Deployment), o.Deployment.ReleaseWhenIdle, o.Deployment.IdleLimit, o.Gap().Round(time.Second), n, len(o.Streams), cut, o.NoOverlapSlotSeen, o.NoReadyTarget)
	fmt.Fprintf(&b, "- the successor's Allocation created after the predecessor's job left the slot: %t (seen: %t); the queued-successor mark: %t\n", o.CreatedAfterLeft, o.OrderSeen, o.SuccessorMarked)
	if sucTag := o.successorTag(); sucTag != "" {
		by := jobsByTag(jobs)
		if q, s, ok := OneSlotQueueWait(by[o.PredecessorTag], by[sucTag]); ok {
			fmt.Fprintf(&b, "- from the predecessor's job end to the successor's submission: %s; the successor's queue wait: %s\n", s.Round(time.Second), q.Round(time.Second))
		}
	}
	fmt.Fprintf(&b, "- the slot between the jobs: %d reads (%d failed); a job outside the run seen in it: %t; the successor refused for the submit limit: %t\n",
		o.Slot.Reads, o.Slot.Failed, o.Slot.ForeignSeen, o.Slot.Refused)
	b.WriteString(warmLine("the predecessor's job", o.WarmPredecessor, !o.PredecessorReady.IsZero()))
	b.WriteString(warmLine("the successor's job", o.WarmSuccessor, !o.SuccessorReady.IsZero()))
	fmt.Fprintf(&b, "- the deployment ended at the row's end, every job of the run seen ended: %t\n", o.DeploymentEnded)
	return b.String()
}

// warmLine is one job's warm start in the text to post.
func warmLine(job string, w WarmObs, ready bool) string {
	if !ready {
		return fmt.Sprintf("- the warm start of %s: not judged, its Instance was not seen ready\n", job)
	}
	if e := warmStartShownByTheAgentsOwnLog(w); e != nil {
		return fmt.Sprintf("- the warm start of %s, from its agent's own log: NOT shown (%s): NOT RUN, never a pass\n", job, e.counts())
	}
	return fmt.Sprintf("- the warm start of %s, from its agent's own log: the image staged already, and all %d checkpoint files %s\n", job, len(w.Files), StagedPresentMatching)
}

// notRunWhy is the fixed words of a NOT RUN that did start: the slot taken, a warm start unshown (which job, and its
// counts).
func notRunWhy(err error) string {
	var why []string
	if errors.Is(err, ErrSlotTaken) {
		why = append(why, slotTakenWords)
	}
	for _, w := range warmUnshownOf(err) {
		why = append(why, fmt.Sprintf("%s (%s)", warmUnshownWords, w.counts()))
	}
	return strings.Join(why, "; ")
}

// warmUnshownOf is every WarmUnshownError err holds, a joined error's included, in order.
func warmUnshownOf(err error) []*WarmUnshownError {
	var out []*WarmUnshownError
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if w, ok := e.(*WarmUnshownError); ok {
			out = append(out, w)
			return
		}
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			for _, x := range u.Unwrap() {
				walk(x)
			}
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
	return out
}

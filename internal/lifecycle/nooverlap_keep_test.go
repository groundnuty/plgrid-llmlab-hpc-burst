package lifecycle

// No-overlap mode under Keep retention (issue #173; HBEP-7, Handover,
// No-overlap mode). nooverlap_test.go runs the mode with ReleaseWhenIdle, where
// the predecessor's job is cancelled a short idle limit after its Instance ends.
// Under Keep, the default, the Allocation is held warm until its hard end, so
// its job keeps the one submit slot, and the successor can be submitted only
// after that end. The lines this file rests on, from
// hbep/0007-lifecycle-controller/README.md at the head of #172 (0f4e7b7):
//   - line 461: "from its drain start until its successor is `ready`, the
//     deployment has `Degraded` with reason `NoOverlapSlot` (proposed), whose
//     message names the Provider and the limit, and no `ReplacementLate`."
//   - line 464: "when the effective submit limit is 1, so no slot is free, the
//     successor is not submitted early. Step 7 creates its Allocation once the
//     predecessor's job has left the slot: its phase is `Ended`, or its
//     deletion has completed".
//   - line 465: "Under the default `Keep` its job holds the GPU, or the slot,
//     until its hard end, so the gap takes in the rest of the drain window. [...]
//     The controller does not release a kept Allocation early by itself, since
//     that needs approval (open question 8)".
//   - line 592: "The successor's Allocation is created only after the
//     predecessor's job is `Ended` or deleted, never while it is held, pending
//     or running".
//   - line 314: "a successor that is held back or refused adds
//     `SubmitLimitReached` to the message" (the held-back successor, step 7).
//
// What the test reads, it reads from the fake Slurm (pkg/provider/fake): its
// call log of sbatch calls, with the time of each, and its job records, with
// each job's submit, start, hard end and end. The controller's own intent (its
// Allocations, Instances and annotations) is read for the status it shows and
// for the check that no successor Allocation exists while the slot is held.
//
// The timeline is nooverlap_test.go's: the primary's job starts at 08:00:20 with
// a 4 h limit, so its hard end is 12:00:20 and its drain start 09:50:20. Its
// engine reports one request in flight from 09:45:00 until 09:56:00, so the
// primary drains at 09:50:20, serves the request to its end, and ends after
// 09:56:00. The Allocation stays, kept, and its job keeps running until 12:00:20.

import (
	"sort"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// slurmView reads the jobs the fake Slurm holds, found from its own sbatch call
// log (the tag of each call), not from the controller's Allocations. The call
// log lists refused calls too, so a submission into an occupied slot shows up
// as a call with no job.
type slurmView struct {
	rg   *rig
	seen int
	ids  []string
}

// sbatchCalls returns the fake's sbatch calls, oldest first.
func (v *slurmView) sbatchCalls() []pfake.Call {
	var out []pfake.Call
	for _, c := range v.rg.batch.Calls() {
		if c.Op == "sbatch" {
			out = append(out, c)
		}
	}
	return out
}

// jobs returns the fake's current record of every job it holds, by submit time.
func (v *slurmView) jobs() []provider.Job {
	v.rg.t.Helper()
	if n := v.rg.submissions(); n != v.seen {
		tags := map[string]bool{}
		for _, c := range v.sbatchCalls() {
			tags[c.Args[0]] = true
		}
		v.ids = nil
		for tag := range tags {
			found, err := v.rg.batch.FindByTag(v.rg.ctx, tag, time.Time{})
			if err != nil {
				v.rg.t.Fatalf("find jobs tagged %s: %v", tag, err)
			}
			for _, j := range found {
				v.ids = append(v.ids, j.ID)
			}
		}
		v.seen = n
	}
	var out []provider.Job
	for _, id := range v.ids {
		j, ok := v.rg.batch.Job(id)
		if !ok {
			v.rg.t.Fatalf("the fake Slurm has no job %s", id)
		}
		out = append(out, j)
	}
	sort.SliceStable(out, func(i, k int) bool { return out[i].SubmitTime.Before(out[k].SubmitTime) })
	return out
}

// live counts the jobs with no end: held, pending or running, which is what
// the fake counts against maxSubmitPerUser.
func (v *slurmView) live() int {
	n := 0
	for _, j := range v.jobs() {
		if j.EndTime == nil {
			n++
		}
	}
	return n
}

// keepStart creates the deployment with the retention policy given (nil: the
// default) and runs it to the primary's readiness, as noOverlapStart does.
func keepStart(rg *rig, pol *v1alpha1.RetentionPolicy) *v1alpha1.Instance {
	rg.t.Helper()
	md := mdSpec(hoMD)
	md.Spec.RetentionPolicy = pol
	rg.create(md)
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	p := rg.instances(hoMD)[0]
	if p.Status.ReadyTime == nil || !p.Status.ReadyTime.Time.Equal(hoT(8, 1, 10)) || p.Status.LeaseEnd == nil || !p.Status.LeaseEnd.Time.Equal(hoT(12, 0, 20)) {
		rg.t.Fatalf("the primary should be ready at 08:01:10 with leaseEnd 12:00:20, got %v and %v", p.Status.ReadyTime, p.Status.LeaseEnd)
	}
	return &p
}

// TestNoOverlapKeepHoldsTheSlotToTheHardEnd runs the one-slot path (HBEP-7:
// maxSubmitPerUser 1 and maxGpusPerUser 1, no partition QOS, so the effective
// submit limit is 1) with Keep retention and a request in flight across the
// primary's drain start, through the primary's job end:
//   - the fake Slurm gets one sbatch call before the end and no other until
//     the old job has ended; its second call is accepted, after that end, and
//     at no time are two jobs live;
//   - the old job runs to its hard end, 12:00:20, with its Instance long gone
//     and its Allocation kept: Keep releases nothing early;
//   - from the drain start to the successor's readiness the deployment has
//     Degraded NoOverlapSlot naming the Provider and the limit, with
//     SubmitLimitReached added once the successor is held back, and no
//     ReplacementLate;
//   - the controller restarts mid-hold and the gap goes on;
//   - once the successor is ready the gap is over.
func TestNoOverlapKeepHoldsTheSlotToTheHardEnd(t *testing.T) {
	rg := noOverlapRig(t, oneSlotOpts(withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := keepStart(rg, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep})
	pAlloc := p.Spec.AllocationRef.Name
	slurm := &slurmView{rg: rg}
	drainStart, jobEnd := hoT(9, 50, 20), hoT(12, 0, 20)

	// Keep, not ReleaseWhenIdle: the deployment's status and the Allocation
	// both say so, so a run that passes cannot be a ReleaseWhenIdle run.
	if m := rg.md(hoMD).Status.Retention; m == nil || m.EffectiveMode != v1alpha1.RetentionKeep {
		t.Fatalf("effective retention %+v, want Keep", m)
	}
	if a, ok := rg.allocation(pAlloc); !ok || a.Spec.RetentionPolicy.EffectiveMode() != v1alpha1.RetentionKeep {
		t.Fatalf("the primary's Allocation %+v, want retention Keep", a)
	}
	jobs := slurm.jobs()
	if len(jobs) != 1 || jobs[0].EndTime != nil || jobs[0].HardEnd == nil || !jobs[0].HardEnd.Equal(jobEnd) {
		t.Fatalf("the fake Slurm holds %+v, want the primary's one job, live, with hard end 12:00:20", jobs)
	}
	oldJob := jobs[0].ID

	// invariants run after every step of the run. They read the fake Slurm.
	var (
		primaryEnded, restarted bool
		keptIdleSteps           int  // steps with the primary's Instance ended and its job still live
		sawSubmitLimit          bool // the message added SubmitLimitReached during the hold
		holdSteps               int  // steps in that hold with no successor Allocation yet
		heldAfterRestart        int
	)
	invariants := func() {
		now := rg.clock.Now()
		at := now.Format(time.TimeOnly)
		if n := slurm.live(); n > 1 {
			t.Fatalf("at %s the fake Slurm holds %d live jobs on a one-job QOS: %+v", at, n, slurm.jobs())
		}
		old, _ := rg.batch.Job(oldJob)
		oldLive := old.EndTime == nil
		calls := slurm.sbatchCalls()
		if oldLive && len(calls) != 1 {
			t.Fatalf("at %s the old job is live and the fake Slurm has had %d sbatch calls, want only the primary's: %+v", at, len(calls), calls)
		}
		for _, c := range calls[min(1, len(calls)):] {
			if old.EndTime == nil || c.Time.Before(*old.EndTime) {
				t.Fatalf("at %s an sbatch call was made at %s, before the old job ended (%v)", at, c.Time.Format(time.TimeOnly), old.EndTime)
			}
		}
		noReplacementLate(t, rg)
		streamNotCut(t, rg, p.Name)
		pi := rg.instancesByName()[p.Name]
		if pi != nil && pi.Status.Phase == v1alpha1.InstanceEnded {
			primaryEnded = true
		}

		// Before the drain start: the primary serves, nothing is wrong and
		// nothing else is submitted.
		if now.Before(drainStart) {
			if n := len(rg.allAllocations()); n != 1 {
				t.Fatalf("at %s %d Allocations, want only the primary's before its drain start", at, n)
			}
			if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonAsDesired {
				t.Fatalf("at %s Degraded = %s/%s %q before the drain start, want False/AsDesired", at, c.Status, c.Reason, c.Message)
			}
			return
		}

		// From the drain start until a successor is ready.
		if rg.readyCount(hoMD) == 0 {
			c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonNoOverlapSlot ||
				!strings.Contains(c.Message, "Provider "+rigProvider) || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
				t.Fatalf("at %s in the gap Degraded = %s/%s %q, want NoOverlapSlot naming Provider %s and its maxSubmitPerUser 1", at, c.Status, c.Reason, c.Message, rigProvider)
			}
			if primaryEnded && oldLive && strings.Contains(c.Message, v1alpha1.ReasonSubmitLimitReached+": ") {
				sawSubmitLimit = true
			}
		}

		// Keep releases nothing early: once the Instance has ended, the old
		// job lives on, as the fake lists it, until its hard end.
		if primaryEnded && !oldLive && now.Before(jobEnd) {
			t.Fatalf("at %s the old job has ended (%s, %v), before its hard end %s: the kept Allocation was released early", at, old.Phase, old.EndTime, jobEnd.Format(time.TimeOnly))
		}

		// The hold: the primary's Instance has ended, its job has not.
		if primaryEnded && oldLive {
			keptIdleSteps++
			a, ok := rg.allocation(pAlloc)
			if !ok || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
				t.Fatalf("at %s the primary's Instance has ended and its job is live, but its Allocation is %+v: Keep releases nothing early", at, a)
			}
			if old.Phase != v1alpha1.AllocationRunning {
				t.Fatalf("at %s the old job is %s, want running to its hard end", at, old.Phase)
			}
			if n := len(rg.allAllocations()); n != 1 {
				t.Fatalf("at %s %d Allocations while the old job holds the slot, want only the old one", at, n)
			}
			holdSteps++
			if restarted {
				heldAfterRestart++
			}
		}
	}

	// Before the drain start: coarse steps, then 10 s ones to the boundary.
	hoStepTo(rg, drainStart.Add(-time.Minute), 2*time.Minute, invariants)
	hoStepTo(rg, drainStart, 10*time.Second, invariants)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the primary is %s, want draining", ph)
	}
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonCapacityDropped)); n == 0 {
		t.Fatal("no CapacityDropped event at the drain start")
	}
	invariants()

	// The drain, with the request in flight, until the Instance has ended.
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return primaryEnded }, invariants)
	pi := rg.instancesByName()[p.Name]
	if pi == nil || pi.Status.EndTime == nil || pi.Status.EndTime.Time.Before(noStreamTo) {
		t.Fatalf("the primary %+v ended before its request did, at 09:56:00", pi)
	}

	// The hold, in coarse steps up to the minute before the job's end, with a
	// controller restart in the middle of it: the gap is recorded on the
	// Instances, not held in memory. Then in 10 s steps until the old job has
	// ended, read from the fake. The hold lasts about two hours, so coarse
	// steps are the only way to reach its end in a test.
	hardEnd := *jobs[0].HardEnd
	for rg.clock.Now().Before(hardEnd.Add(-time.Minute)) {
		step := min(2*time.Minute, hardEnd.Add(-time.Minute).Sub(rg.clock.Now()))
		rg.clock.Advance(step)
		rg.Step()
		invariants()
		if !restarted && !rg.clock.Now().Before(hoT(11, 0, 0)) {
			restarted = true
			rg.restartController()
		}
	}
	if !restarted {
		t.Fatal("the controller was not restarted in the hold")
	}
	if got := rg.submissions(); got != 1 {
		t.Fatalf("%d sbatch calls a minute before the old job's hard end, want only the primary's", got)
	}
	oldEnded := func() bool { j, _ := rg.batch.Job(oldJob); return j.EndTime != nil }
	hoStepUntil(rg, hardEnd.Add(10*time.Minute), 10*time.Second, oldEnded, invariants)

	// The old job ended at its hard end: Keep held the slot to then, so the
	// controller released nothing early.
	old, _ := rg.batch.Job(oldJob)
	if old.EndTime == nil || !old.EndTime.Equal(hardEnd) || old.StartTime == nil || old.EndTime.Sub(*old.StartTime) != old.TimeLimit {
		t.Fatalf("the old job %+v, want it to have run its whole time limit to 12:00:20", old)
	}
	if len(old.State) == 0 || old.State[0] != "TIMEOUT" {
		t.Fatalf("the old job ended in state %v, want TIMEOUT: Keep did not cancel it", old.State)
	}

	// Then the successor, submitted after the end, and its readiness.
	hoStepUntil(rg, hardEnd.Add(10*time.Minute), 10*time.Second, func() bool { return successorReady(rg) }, invariants)
	if !restarted || heldAfterRestart == 0 || keptIdleSteps < 20 {
		t.Fatalf("restarted %v; %d hold steps after it, %d in all, want a restart mid-hold with the hold checked after it", restarted, heldAfterRestart, keptIdleSteps)
	}
	if !sawSubmitLimit {
		t.Fatal("the held-back successor never added SubmitLimitReached to the message during the hold")
	}

	calls := slurm.sbatchCalls()
	jobs = slurm.jobs()
	if len(calls) != 2 || len(jobs) != 2 {
		t.Fatalf("%d sbatch calls and %d jobs, want the primary's and one successor's, with no call refused: calls %+v, jobs %+v", len(calls), len(jobs), calls, jobs)
	}
	succ := jobs[1]
	if calls[1].Time.Before(*old.EndTime) || succ.SubmitTime.Before(*old.EndTime) {
		t.Fatalf("the successor's sbatch call was at %s and its job submitted at %s, before the old job ended at %s", calls[1].Time.Format(time.TimeOnly), succ.SubmitTime.Format(time.TimeOnly), old.EndTime.Format(time.TimeOnly))
	}
	if succ.StartTime == nil || succ.StartTime.Before(*old.EndTime) {
		t.Fatalf("the successor's job started at %v, before the old job ended", succ.StartTime)
	}
	if jobs[0].ID != oldJob || succ.ID == oldJob {
		t.Fatalf("jobs %+v, want the old job first and a new one second", jobs)
	}

	// The successor is the primary's, marked as a no-overlap successor, and
	// ready; the gap is over.
	var si *v1alpha1.Instance
	for _, in := range rg.instances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name {
			si = &in
		}
	}
	if si == nil || si.Annotations[annotationNoOverlapSuccessor] == "" || si.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("successor %+v, want the primary's, marked, and ready", si)
	}
	if si.Status.ReadyTime.Time.Before(*old.EndTime) {
		t.Fatalf("the successor was ready at %s, before the old job ended", si.Status.ReadyTime.Time.Format(time.TimeOnly))
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("Degraded = %s/%s %q once the successor is ready, want False", c.Status, c.Reason, c.Message)
	}
	t.Logf("gap: drain start 09:50:20, old Instance ended %s, old job ended %s, successor submitted %s, ready %s",
		pi.Status.EndTime.Time.Format(time.TimeOnly), old.EndTime.Format(time.TimeOnly), succ.SubmitTime.Format(time.TimeOnly), si.Status.ReadyTime.Time.Format(time.TimeOnly))
}

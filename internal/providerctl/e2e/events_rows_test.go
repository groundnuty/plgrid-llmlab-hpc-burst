package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// HBEP-8's beta rows for the Provider controller's typed Allocation events
// (Test plan, Beta: Allocation events on the fake Slurm; Allocation failures; A
// failed status write; Repeating Allocation conditions; A moved hard end). Each
// reads the Events the API server holds, for at least one of each, and the
// counter for counts and absence. The expectations are the HBEP's own words:
// the types, reasons and ids are written out here, not taken from the code.

// A held job is submitted, its hold lifted, it starts and runs to its time
// limit, with the fake clock passing an hour before its hard end: the API holds
// started JobSubmitted, started HoldLifted, ready JobStarted, ending-soon
// HardEndNear and ended JobEnded, each regarding the Allocation, reported by
// hpcburst-provider, with no note naming the job ID, account or grant.
func TestAllocationEventsOnTheFakeSlurm(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	r.newAlloc("a", "3h")
	a := r.until("a", "Pending", 12, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationPending })
	id := r.jobID(a)
	jobStart := r.clock.Now() // the fake Slurm stamps the job's start with this
	if err := r.sl.Start(id); err != nil {
		t.Fatal(err)
	}
	a = r.until("a", "Running", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning })
	hardEnd := a.Status.AtProvider.HardEnd.Time
	if d := hardEnd.Sub(r.clock.Now()); d < 3*time.Hour-10*time.Second || d > 3*time.Hour {
		t.Fatalf("hardEnd %s is %s ahead, want about 3 h after the start", hardEnd, d)
	}
	// One second before hardEnd-1h: nothing yet. At it: HardEndNear.
	r.clock.Set(hardEnd.Add(-time.Hour - 2*time.Second))
	_ = r.pass("a")
	if n := r.count(r.key("ending-soon", "HardEndNear", "Normal")); n != 0 {
		t.Fatalf("HardEndNear raised %v times before hardEnd-1h", n)
	}
	r.clock.Set(hardEnd.Add(-time.Hour - time.Second))
	_ = r.pass("a")
	// The job runs to its limit: Slurm ends it TIMEOUT at its hard end.
	r.clock.Set(hardEnd)
	if err := r.sl.End(id, "TIMEOUT"); err != nil {
		t.Fatal(err)
	}
	r.until("a", "Ended", 6, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded })

	evs := r.waitEvents(a,
		want{"started", "JobSubmitted"}, want{"started", "HoldLifted"}, want{"ready", "JobStarted"},
		want{"ending-soon", "HardEndNear"}, want{"ended", "JobEnded"})
	for _, e := range evs {
		if e.ReportingController != v1alpha1.FieldManagerProvider {
			t.Errorf("%s reported by %q, want %q", e.Reason, e.ReportingController, v1alpha1.FieldManagerProvider)
		}
		wantType := provider.EventKindOrdinary
		if e.Action == "failing" || e.Action == "degraded" {
			wantType = provider.EventKindWarning
		}
		if e.Type != wantType || len(e.Note) > 1024 {
			t.Errorf("%s: type %s note %d bytes, want %s and at most 1,024", e.Reason, e.Type, len(e.Note), wantType)
		}
	}
	if got := notesOf(evs, "JobSubmitted"); len(got) == 0 || !strings.Contains(got[0], "attempt 1") || !strings.Contains(got[0], rowPart) {
		t.Errorf("JobSubmitted notes %q, want the attempt and the partition", got)
	}
	r.checkNotes(evs, id)

	// The log's half (HBEP-8, Test plan, Beta: Allocation events on the fake Slurm), read through a fresh client: at
	// least one of each, on hb.ev.allocation.<uid>, with the ids written out here from the fixture's own times and
	// counts, each fact being the one HBEP-8's list in The event's id names: the attempt's number for JobSubmitted, the
	// restart count for HoldLifted and JobStarted, the hard end for HardEndNear and the end time for JobEnded. The job
	// started at jobStart, with a 3 h limit, and the test ended it at its hard end.
	wantEnd := rfc(jobStart.Add(3 * time.Hour))
	if got := rfc(hardEnd); got != wantEnd {
		t.Fatalf("the Allocation's hard end is %s, the fixture's (start %s + 3 h) is %s", got, rfc(jobStart), wantEnd)
	}
	r.idle()
	msgs := r.wantLog(a,
		eid(a, "JobSubmitted", "1"), eid(a, "HoldLifted", "0"), eid(a, "JobStarted", "0"),
		eid(a, "HardEndNear", wantEnd), eid(a, "JobEnded", wantEnd))
	r.checkLogNotes(msgs, id)
	r.matchAPI(msgs, evs, "JobSubmitted", "HoldLifted", "JobStarted", "HardEndNear", "JobEnded")
	if got, _ := r.counter.Unlogged(eventcount.ControllerProvider(r.prov)); got != 0 {
		t.Errorf("hpcburst_events_unlogged_total reads %v with the log running throughout", got)
	}
	// It ended at its planned end, never by a failure.
	for _, reason := range []string{"AllocationEndedEarly", "AllocationFailed", "JobLost"} {
		if n := r.count(r.key("failing", reason, "Warning")); n != 0 {
			t.Errorf("%s raised %v times for a job that ran to its time limit", reason, n)
		}
	}
	// The counter counts each hand-over exactly: one of each.
	for _, w := range []struct{ action, reason, typ string }{
		{"started", "JobSubmitted", "Normal"}, {"started", "HoldLifted", "Normal"}, {"ready", "JobStarted", "Normal"},
		{"ending-soon", "HardEndNear", "Normal"}, {"ended", "JobEnded", "Normal"},
	} {
		if n := r.count(r.key(w.action, w.reason, w.typ)); n != 1 {
			t.Errorf("%s/%s handed over %v times, want 1", w.action, w.reason, n)
		}
	}
}

// running makes a rig whose Allocation "a" runs, started now with a 3 h limit,
// and returns the Allocation and its job ID.
func running(t *testing.T) (*evRig, *v1alpha1.Allocation, int) {
	t.Helper()
	r := newEvRig(t, rigOpts{})
	r.newAlloc("a", "3h")
	a := r.until("a", "Pending", 12, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationPending })
	id := r.jobID(a)
	if err := r.sl.Start(id); err != nil {
		t.Fatal(err)
	}
	a = r.until("a", "Running", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning })
	return r, a, id
}

// Allocation failures: each of the HBEP's cases on its own Allocation, with the
// API holding the failing event, and none of the event of another case.
func TestAllocationFailures(t *testing.T) {
	t.Run("a job that fails gives AllocationFailed", func(t *testing.T) {
		r, a, id := running(t)
		endT := r.clock.Now() // the fake Slurm stamps the job's end with this
		if err := r.sl.End(id, "FAILED"); err != nil {
			t.Fatal(err)
		}
		r.until("a", "Failed", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationFailed })
		evs := r.waitEvents(a, want{"failing", "AllocationFailed"})
		r.checkNotes(evs, id)
		// The log's id: the end time (HBEP-8, The event's id).
		r.idle()
		msgs := r.wantLog(a, eid(a, "AllocationFailed", rfc(endT)))
		r.checkLogNotes(msgs, id)
		r.matchAPI(msgs, evs, "AllocationFailed")
		if n := r.count(r.key("failing", "Requeued", "Warning")); n != 0 {
			t.Errorf("Requeued raised %v times for a failed job", n)
		}
	})
	t.Run("a requeue gives Requeued", func(t *testing.T) {
		r, a, id := running(t)
		if err := r.sl.Requeue(id); err != nil {
			t.Fatal(err)
		}
		a = r.until("a", "requeued", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Requeued })
		evs := r.waitEvents(a, want{"failing", "Requeued"})
		r.checkNotes(evs, id)
		// The log's id: the restart count, which the fake Slurm's one Requeue made 1.
		r.idle()
		r.checkLogNotes(r.wantLog(a, eid(a, "Requeued", "1")), id)
		if n := r.count(r.key("failing", "AllocationFailed", "Warning")); n != 0 {
			t.Errorf("AllocationFailed raised %v times for a requeue", n)
		}
		if a.Status.AtProvider.RestartCount != 1 {
			t.Errorf("restart count %d, want 1", a.Status.AtProvider.RestartCount)
		}
	})
	t.Run("absent past lostGrace gives JobLost, and its return JobReappeared", func(t *testing.T) {
		r, a, id := running(t)
		// The job ends and leaves the queue listing while accounting does not show
		// it either: absent from both reads, with the controller having seen it Running.
		if err := r.sl.End(id, "COMPLETED"); err != nil {
			t.Fatal(err)
		}
		if err := r.sl.AcctHide(id); err != nil {
			t.Fatal(err)
		}
		r.clock.Advance(time.Hour) // past the queue's minimum age for an ended job
		_ = r.pass("a")
		absentSince := r.clock.Now() // the first pass that found the job absent: the id's fact
		if n := r.count(r.key("failing", "JobLost", "Warning")); n != 0 {
			t.Fatalf("JobLost raised %v times on the first absence, before lostGrace", n)
		}
		r.clock.Advance(3 * time.Minute) // lostGrace is 2 minutes
		r.until("a", "Lost", 3, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationLost })
		r.waitEvents(a, want{"failing", "JobLost"})
		if err := r.sl.AcctShow(id); err != nil {
			t.Fatal(err)
		}
		r.until("a", "not Lost", 3, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase != v1alpha1.AllocationLost })
		evs := r.waitEvents(a, want{"ready", "JobReappeared"})
		r.checkNotes(evs, id)
		// The log's ids: JobLost and JobReappeared share absentSince, and so are two events of their own.
		r.idle()
		r.checkLogNotes(r.wantLog(a, eid(a, "JobLost", rfc(absentSince)), eid(a, "JobReappeared", rfc(absentSince))), id)
	})
	t.Run("cancelled from outside before the hard end gives AllocationEndedEarly", func(t *testing.T) {
		r, a, id := running(t)
		r.clock.Advance(2*time.Hour + 30*time.Minute) // hardEnd - 30 min
		endT := r.clock.Now()
		if err := r.sl.End(id, "CANCELLED"); err != nil {
			t.Fatal(err)
		}
		r.until("a", "Ended", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded })
		evs := r.waitEvents(a, want{"failing", "AllocationEndedEarly"})
		r.checkNotes(evs, id)
		r.idle()
		r.checkLogNotes(r.wantLog(a, eid(a, "AllocationEndedEarly", rfc(endT))), id)
		if n := r.count(r.key("ended", "JobEnded", "Normal")); n != 0 {
			t.Errorf("JobEnded raised %v times for an end from outside before the hard end, with no Instance draining", n)
		}
	})
	t.Run("COMPLETED before the hard end while its Instance drains gives JobEnded", func(t *testing.T) {
		r, a, id := running(t)
		// The Instance is in the API, in phase Draining, and names the
		// Allocation. A fixture whose Instance never drains proves nothing: the
		// control above ends the same way with none, and raises the failing event.
		in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "drain-i", Namespace: r.ns},
			Spec: v1alpha1.InstanceSpec{
				DeploymentID: rowDeploymentID, IdentityRecord: "id-" + rowDeploymentID[:12],
				AllocationRef: v1alpha1.ObjectReference{Name: "a"}, Launch: rowLaunch(),
			}}
		if err := c.Create(r.ctx, in); err != nil {
			t.Fatal(err)
		}
		in.Status.Phase = v1alpha1.InstanceDraining
		if err := c.Status().Update(r.ctx, in); err != nil {
			t.Fatal(err)
		}
		if got := (&v1alpha1.Instance{}); c.Get(r.ctx, client.ObjectKeyFromObject(in), got) != nil || got.Status.Phase != v1alpha1.InstanceDraining {
			t.Fatalf("the Instance in the API is %q, not Draining", got.Status.Phase)
		}
		r.clock.Advance(2*time.Hour + 50*time.Minute) // hardEnd - 10 min
		endT := r.clock.Now()
		if err := r.sl.End(id, "COMPLETED"); err != nil {
			t.Fatal(err)
		}
		r.until("a", "Ended", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded })
		evs := r.waitEvents(a, want{"ended", "JobEnded"})
		r.checkNotes(evs, id)
		r.idle()
		r.checkLogNotes(r.wantLog(a, eid(a, "JobEnded", rfc(endT))), id)
		if n := r.count(r.key("failing", "AllocationEndedEarly", "Warning")); n != 0 {
			t.Errorf("AllocationEndedEarly raised %v times for an end while the Instance drains", n)
		}
	})
	t.Run("CANCELLED with its end time at the hard end gives JobEnded", func(t *testing.T) {
		r, a, id := running(t)
		hardEnd := a.Status.AtProvider.HardEnd.Time
		r.clock.Set(hardEnd)
		if err := r.sl.End(id, "CANCELLED"); err != nil {
			t.Fatal(err)
		}
		r.until("a", "Ended", 5, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded })
		evs := r.waitEvents(a, want{"ended", "JobEnded"})
		r.checkNotes(evs, id)
		r.idle()
		r.checkLogNotes(r.wantLog(a, eid(a, "JobEnded", rfc(hardEnd))), id)
		if n := r.count(r.key("failing", "AllocationEndedEarly", "Warning")); n != 0 {
			t.Errorf("AllocationEndedEarly raised %v times for a job cancelled at its hard end", n)
		}
	})
}

// A failed status write: the interceptor fails the status write of the pass that
// computes JobStarted, and the next pass raises it again. The counter holds the
// two hand-overs and the API holds at least one event. The id is
// <UID>/JobStarted/<restart count>, so it is the same exactly when its inputs
// are: the failed write carried the same restart count that the next pass
// stores, and the UID and the annotations (the job ID, the submission record)
// are unchanged between the two passes. The id is asserted on those inputs, not
// on the event, whose id the API does not hold.
func TestAFailedStatusWriteRaisesJobStartedAgainUnderTheSameID(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	a := r.newAlloc("a", "3h")
	a = r.until("a", "Pending", 12, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationPending })
	id := r.jobID(a)
	if err := r.sl.Start(id); err != nil {
		t.Fatal(err)
	}
	key := r.key("ready", "JobStarted", "Normal")
	if n := r.count(key); n != 0 {
		t.Fatalf("JobStarted raised %v times before the job started", n)
	}
	before, _ := r.get("a")
	r.failStatus.Store(true)
	if err := r.pass("a"); err == nil {
		t.Fatal("the pass whose status write was failed returned no error")
	}
	if r.failStatus.Load() {
		t.Fatal("the interceptor was not reached: no status write was attempted")
	}
	if n := r.count(key); n != 1 {
		t.Fatalf("after the pass whose write failed, JobStarted handed over %v times, want 1", n)
	}
	stored, _ := r.get("a")
	if stored.Status.AtProvider.Phase == v1alpha1.AllocationRunning {
		t.Fatalf("the stored phase is Running after a failed write: the pass that computed JobStarted must not be recorded")
	}
	r.failedMu.Lock()
	failed := r.failedStatus
	r.failedMu.Unlock()
	if failed == nil || failed.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the failed write carried %+v, want the Running status the pass computed", failed)
	}
	if err := r.pass("a"); err != nil {
		t.Logf("the next pass returned %v", err)
	}
	if n := r.count(key); n != 2 {
		t.Fatalf("after the next pass, JobStarted handed over %v times, want 2 (raised again)", n)
	}
	after, _ := r.get("a")
	if after.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the next pass stored phase %s, want Running", after.Status.AtProvider.Phase)
	}
	// The id's inputs: the UID, the reason and the fact, the restart count.
	if before.UID != after.UID || failed.UID != after.UID {
		t.Errorf("UIDs %s, %s, %s differ", before.UID, failed.UID, after.UID)
	}
	if failed.Status.AtProvider.RestartCount != after.Status.AtProvider.RestartCount {
		t.Errorf("the failed write carried restart count %d, the next pass stored %d: the id would differ",
			failed.Status.AtProvider.RestartCount, after.Status.AtProvider.RestartCount)
	}
	for _, k := range []string{v1alpha1.AnnotationJobID, v1alpha1.AnnotationSubmission} {
		if before.Annotations[k] != after.Annotations[k] || before.Annotations[k] == "" {
			t.Errorf("annotation %s changed between the passes (%q, then %q)", k, before.Annotations[k], after.Annotations[k])
		}
	}
	if got, want := fmt.Sprintf("%s/JobStarted/%d", after.UID, after.Status.AtProvider.RestartCount), string(after.UID)+"/JobStarted/0"; got != want {
		t.Errorf("the id's inputs give %s, want %s", got, want)
	}
	evs := r.waitEvents(after, want{"ready", "JobStarted"})
	r.checkNotes(evs, id)

	// The log's half (HBEP-8, Test plan, Beta: A failed status write): the event was raised twice, and the log holds
	// exactly one copy of its id, <uid>/JobStarted/0, since the second publish, within the duplicate window, was
	// answered as a duplicate of the first: neither was lost (nothing is counted unlogged), and the stream, read through a
	// fresh client, holds one. The API holds at least one (above).
	r.idle()
	msgs := r.wantLog(after, eid(after, "JobStarted", "0"))
	if got := loggedOf(msgs, "JobStarted"); len(got) != 1 {
		t.Errorf("the log holds %d JobStarted for the Allocation (%s), want exactly one under the id %s", len(got), joinIDs(got), eid(after, "JobStarted", "0"))
	}
	if got, _ := r.counter.Unlogged(eventcount.ControllerProvider(r.prov)); got != 0 {
		t.Errorf("hpcburst_events_unlogged_total reads %v: a publish was lost, so the one copy proves nothing about the duplicate", got)
	}
}

// Repeating conditions, read every minute on the fake clock: an admin hold kept
// for 3 h gives at most 4 AdminHold; COMPLETING held for 15 min gives one
// JobCompleting, at 10 min; held for 5 s, none.
func TestRepeatingAllocationConditions(t *testing.T) {
	t.Run("an admin hold for three hours", func(t *testing.T) {
		r := newEvRig(t, rigOpts{})
		r.newAlloc("a", "6h")
		a := r.until("a", "Pending", 12, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationPending })
		id := r.jobID(a)
		if err := r.sl.HoldAdmin(id); err != nil {
			t.Fatal(err)
		}
		a = r.until("a", "Held", 4, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.AllocationHeld })
		key := r.key("degraded", "AdminHold", "Warning")
		for i := 0; i < 180; i++ {
			r.clock.Advance(time.Minute)
			_ = r.pass("a")
		}
		n := r.count(key)
		if n < 3 || n > 4 {
			t.Fatalf("AdminHold handed over %v times in 3 h read every minute, want 3 or 4 (at 0, 1 h, 2 h, 3 h)", n)
		}
		evs := r.waitEvents(a, want{"degraded", "AdminHold"})
		r.checkNotes(evs, id)
		// The log: each emission of a repeating condition is an event of its own, so each has a fresh UUID
		// for its id, and no two are the same (HBEP-8, The event's id).
		r.idle()
		holds := loggedOf(r.logAbout(a.UID), "AdminHold")
		if float64(len(holds)) != n {
			t.Fatalf("the log holds %d AdminHold events (%s), want the %v handed over", len(holds), joinIDs(holds), n)
		}
		seen := map[string]bool{}
		for _, h := range holds {
			if !uuidRE.MatchString(h.ID) || seen[h.ID] {
				t.Errorf("AdminHold's id %q is not a fresh UUID of its own", h.ID)
			}
			seen[h.ID] = true
		}
	})
	for _, c := range []struct {
		name string
		hold time.Duration
		want float64
	}{{"COMPLETING for 15 minutes", 15 * time.Minute, 1}, {"COMPLETING for 5 seconds", 5 * time.Second, 0}} {
		t.Run(c.name, func(t *testing.T) {
			r, a, id := running(t)
			// The job ends and carries COMPLETING until told otherwise.
			if err := r.sl.End(id, "COMPLETED"); err != nil {
				t.Fatal(err)
			}
			if err := r.sl.HoldCompleting(id, true); err != nil {
				t.Fatal(err)
			}
			key := r.key("degraded", "JobCompleting", "Warning")
			step := time.Minute
			if c.hold < time.Minute {
				step = time.Second
			}
			var held time.Duration
			for ; held < c.hold; held += step {
				_ = r.pass("a")
				r.clock.Advance(step - time.Second) // pass() moves one second itself
			}
			if err := r.sl.HoldCompleting(id, false); err != nil {
				t.Fatal(err)
			}
			r.passes("a", 3)
			if n := r.count(key); n != c.want {
				t.Fatalf("JobCompleting handed over %v times for COMPLETING held %s, want %v", n, c.hold, c.want)
			}
			if c.want > 0 {
				evs := r.waitEvents(a, want{"degraded", "JobCompleting"})
				r.checkNotes(evs, id)
			}
		})
	}
}

// A moved hard end: HardEndNear at hardEnd-1h, then the hard end moves 2 h later
// (an administrator raises the limit), and a second one comes an hour before the
// new hard end.
func TestAMovedHardEndRaisesHardEndNearAgain(t *testing.T) {
	r, a, id := running(t)
	hardEnd := a.Status.AtProvider.HardEnd.Time
	key := r.key("ending-soon", "HardEndNear", "Normal")
	r.clock.Set(hardEnd.Add(-time.Hour))
	r.passes("a", 2)
	if n := r.count(key); n != 1 {
		t.Fatalf("HardEndNear handed over %v times at hardEnd-1h, want 1 (once, though two passes ran)", n)
	}
	if err := r.sl.SetTimeLimit(id, 5*60); err != nil { // minutes
		t.Fatal(err)
	}
	r.passes("a", 2)
	moved := r.until("a", "the hard end moved", 3, func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.HardEnd.Time.After(hardEnd) }).Status.AtProvider.HardEnd.Time
	if want := hardEnd.Add(2 * time.Hour); moved.Sub(want) > 5*time.Second || want.Sub(moved) > 5*time.Second {
		t.Fatalf("the hard end moved to %s, want %s", moved, want)
	}
	if n := r.count(key); n != 1 {
		t.Fatalf("HardEndNear handed over %v times after the hard end moved 2 h later but before the new hardEnd-1h, want still 1", n)
	}
	r.clock.Set(moved.Add(-time.Hour))
	r.passes("a", 2)
	if n := r.count(key); n != 2 {
		t.Fatalf("HardEndNear handed over %v times an hour before the new hard end, want 2", n)
	}
	// The API holds both: the first names the old hard end, the second the moved
	// one. The status write between the two changed the Allocation, so the
	// recorder does not merge them.
	evs := r.waitEvents(a, want{"ending-soon", "HardEndNear"})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && len(notesOf(evs, "HardEndNear")) < 2 {
		time.Sleep(100 * time.Millisecond)
		evs = r.apiEvents(a)
	}
	notes := strings.Join(notesOf(evs, "HardEndNear"), "\n")
	for _, end := range []time.Time{hardEnd, moved} {
		if want := end.UTC().Truncate(time.Second).Format(time.RFC3339); !strings.Contains(notes, want) {
			t.Errorf("the API's HardEndNear notes %q do not name the hard end %s", notes, want)
		}
	}
	// The log: two events of the same reason with different facts, the moved hard end's two, have two ids
	// (HBEP-8, The event's id: the hard end for HardEndNear). The first hard end is the fixture's start of the
	// job plus 3 h, the second 2 h later.
	r.idle()
	j, ok := r.sl.Job(id)
	if !ok {
		t.Fatal("the fake Slurm has no such job")
	}
	wantFirst := j.Start.Add(3 * time.Hour) // the fake's own record of the start, and the limit the Allocation asked for
	wantMoved := wantFirst.Add(2 * time.Hour)
	if rfc(hardEnd) != rfc(wantFirst) || rfc(moved) != rfc(wantMoved) {
		t.Fatalf("the hard ends are %s and %s, the fixture's are %s and %s", rfc(hardEnd), rfc(moved), rfc(wantFirst), rfc(wantMoved))
	}
	first, second := eid(a, "HardEndNear", rfc(wantFirst)), eid(a, "HardEndNear", rfc(wantMoved))
	if first == second {
		t.Fatal("the two hard ends have one id")
	}
	if got := loggedOf(r.wantLog(a, first, second), "HardEndNear"); len(got) != 2 {
		t.Errorf("the log holds %d HardEndNear (%s), want the two", len(got), joinIDs(got))
	}
}

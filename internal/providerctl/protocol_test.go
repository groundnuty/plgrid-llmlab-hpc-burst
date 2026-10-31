package providerctl

// The submission protocol of HBEP-7, open question 11, ported from spike S4's
// scenarios 1 to 3 (spikes/s4/README.md, the table), plus the interrupted
// create. Expected values come from HBEP-4, HBEP-7 open question 11, the S4
// table and the doc comment of pkg/provider/fake/controller.go, never from the
// code under test. Submissions and lifts are counted from the fake's call log.

import (
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The fake numbers its jobs from 998 (fake.NewBatch).
const ptFirstJob = "998"

// The harness stamps CreationTimestamp from the manual clock; the age of an
// attempt without a time, and the accounting window, depend on it.
func TestProtocolHarnessKeepsTheCreationTimestamp(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	if got := e.get("a").CreationTimestamp.Time; !got.Equal(at(12, 0, 0)) {
		t.Fatalf("creation timestamp %s, want the manual clock's 12:00:00", got)
	}
}

// S4 scenario 1 (README table; HBEP-4, The Slurm dialect, Accounting): a job
// ends and leaves the queue listing after MinJobAge. It is reported Ended from
// accounting, never missing, and nothing is submitted again: 1 sbatch, and in
// hold mode 1 lift.
func TestS4Scenario1EndedJobPurgedFromTheQueueIsEndedNotMissing(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.batch.Enqueue(ptEndsAfter(5 * time.Minute))
			a := e.newAlloc("a")
			e.run("a", 4)
			id := ptJobID(e, "a")
			if id != ptFirstJob || ptPhase(e, "a") != v1alpha1.AllocationRunning {
				t.Fatalf("precondition: job %q, phase %s; want %s Running", id, ptPhase(e, "a"), ptFirstJob)
			}
			// Past the end (5 min after the start) plus MinJobAge (10 s), in one
			// step, so the next read finds the job in accounting only.
			e.clock.Advance(5*time.Minute + time.Minute)
			queued, err := e.platform.ListByTag(e.ctx, tagOf(a))
			if err != nil || len(queued) != 0 {
				t.Fatalf("precondition: the queue listing still shows %v (err %v)", queued, err)
			}
			mark := ptMark(e)
			e.run("a", 3)

			got := e.get("a")
			if p := got.Status.AtProvider.Phase; p != v1alpha1.AllocationEnded {
				t.Errorf("phase %s (reason %q), want Ended from accounting", p, got.Status.AtProvider.Reason)
			}
			if got.Status.AtProvider.EndTime == nil {
				t.Error("no end time in the status")
			}
			if ptCallsWithArg(e, mark, "sacct", id) == 0 {
				t.Error("no accounting read of the job: Ended did not come from sacct")
			}
			if ptJobID(e, "a") != id || got.Status.AtProvider.JobID != id {
				t.Errorf("job ID changed: annotation %q, status %q, want %q", ptJobID(e, "a"), got.Status.AtProvider.JobID, id)
			}
			if e.rec.has("JobLost") {
				t.Error("a JobLost event for a job that ended")
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
			wantLifts := 0
			if m.hold {
				wantLifts = 1
			}
			if n := e.lifts(); n != wantLifts {
				t.Errorf("%d lifts, want %d", n, wantLifts)
			}
		})
	}
}

// S4 scenario 2, "killed after sbatch returned" (README table; HBEP-7 open
// question 11, Adoption): the record write that carries the job ID fails once,
// as it would if the process died there. A fresh controller finds the job by
// tag and adopts it: 1 sbatch, and in hold mode 1 lift.
func TestS4Scenario2KilledAfterSbatchReturned(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			raw := e.c
			e.c, _ = ptFailJobIDWrite(raw)
			e.build()
			e.newAlloc("a")
			e.run("a", 1)

			// The kill landed after the sbatch and before the job ID was stored.
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("precondition: %d sbatch calls, want 1", n)
			}
			if _, ok := e.batch.Job(ptFirstJob); !ok {
				t.Fatal("precondition: the sbatch registered no job")
			}
			if id := ptJobID(e, "a"); id != "" {
				t.Fatalf("precondition: job ID %q already recorded", id)
			}
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionInFlight || r.Attempt != 1 {
				t.Fatalf("precondition: record %+v, want attempt 1 InFlight", r)
			}
			if n := e.count("scontrol-release"); n != 0 {
				t.Fatalf("precondition: %d lift calls before the restart", n)
			}

			e.c = raw
			e.build() // the restart: nothing in memory
			e.run("a", 4)
			if id := ptJobID(e, "a"); id != ptFirstJob {
				t.Errorf("job ID %q after the restart, want %s adopted by tag", id, ptFirstJob)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
			wantLifts := 0
			if m.hold {
				wantLifts = 1
			}
			if n := e.lifts(); n != wantLifts {
				t.Errorf("%d lifts, want %d", n, wantLifts)
			}
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
				t.Errorf("phase %s, want Running", p)
			}
			if !e.rec.has("JobAdopted") {
				t.Error("no JobAdopted event")
			}
			// The adopted claimant's attempt is resolved by the job once its
			// window has passed (adoption marks it Done only then).
			e.clock.Advance(ptWindow + time.Second)
			e.run("a", 2)
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionDone {
				t.Errorf("record %+v after the window, want Done", r)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls after the window, want 1", n)
			}
		})
	}
}

// S4 scenario 2, "killed while sbatch blocked, reply lost": the scheduler
// registered the job but the reply never came (Unknown). The tag finds it:
// 1 sbatch, and in hold mode 1 lift (HBEP-7: "Unknown is never Failed").
func TestS4Scenario2KilledWhileSbatchBlockedReplyLost(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.batch.Enqueue(fake.Script{Outcome: provider.Unknown, Registered: true})
			e.newAlloc("a")
			e.run("a", 1)
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionUnknown {
				t.Fatalf("precondition: record %+v, want Unknown", r)
			}
			if id := ptJobID(e, "a"); id != "" {
				t.Fatalf("precondition: job ID %q recorded from an Unknown outcome", id)
			}
			e.build() // the restart
			e.run("a", 4)
			if id := ptJobID(e, "a"); id != ptFirstJob {
				t.Errorf("job ID %q, want %s adopted by tag", id, ptFirstJob)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
			wantLifts := 0
			if m.hold {
				wantLifts = 1
			}
			if n := e.lifts(); n != wantLifts {
				t.Errorf("%d lifts, want %d", n, wantLifts)
			}
			// Adopting an Unknown attempt marks it Done (HBEP-7, Adoption).
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionDone {
				t.Errorf("record %+v, want Done", r)
			}
		})
	}
}

// S4 scenario 2, "killed after the claim, before sbatch", hold mode (README
// table; HBEP-7, Hold mode; fake.Controller doc: the window is passed when the
// age is strictly above it): nothing is submitted inside the window; a second
// after it the attempt is dropped and attempt 2 submits exactly once.
func TestS4Scenario2KilledBeforeSbatchHold(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 1))
	e.run("a", 3)
	if n := e.count("sbatch"); n != 0 {
		t.Fatalf("%d sbatch calls inside the window, want 0", n)
	}
	if p := ptPhase(e, "a"); p != v1alpha1.AllocationSubmitting {
		t.Fatalf("phase %s inside the window, want Submitting", p)
	}
	e.clock.Advance(ptWindow) // exactly the window: still waiting
	e.run("a", 2)
	if n := e.count("sbatch"); n != 0 {
		t.Fatalf("%d sbatch calls at exactly the window, want 0", n)
	}
	e.clock.Advance(time.Second)
	e.run("a", 4)
	if n := e.count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls after the window, want 1", n)
	}
	if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 2 {
		t.Errorf("record %+v, want attempt 2 (the dropped attempt 1 replaced)", r)
	}
	if n := e.lifts(); n != 1 {
		t.Errorf("%d lifts, want 1", n)
	}
}

// S4 scenario 2, "killed after the claim, before sbatch", fallback mode
// (HBEP-7: Fallback mode, The operator condition; fake.Controller doc): no
// sbatch; Unknown after the window; SubmissionResolved False, NeedsOperator
// after unknownBound; a wrong abandon-attempt is ignored; abandon-attempt equal
// to the attempt number allows exactly 1 sbatch.
func TestS4Scenario2KilledBeforeSbatchFallback(t *testing.T) {
	e := newEnv(t, fallback)
	e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 1)) // attempt time 12:00:00
	e.run("a", 3)
	if n := e.count("sbatch"); n != 0 || ptPhase(e, "a") != v1alpha1.AllocationSubmitting {
		t.Fatalf("inside the window: %d sbatch, phase %s; want 0 and Submitting", n, ptPhase(e, "a"))
	}
	e.clock.Set(at(12, 5, 1))
	e.run("a", 2)
	a := e.get("a")
	if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionUnknown {
		t.Fatalf("record %+v after the window, want Unknown", r)
	}
	if p := a.Status.AtProvider.Phase; p != v1alpha1.AllocationSubmissionUnknown {
		t.Fatalf("phase %s after the window, want SubmissionUnknown", p)
	}
	if n := e.count("sbatch"); n != 0 {
		t.Fatalf("%d sbatch calls after the window, want 0: elapsed time never permits a retry", n)
	}

	e.clock.Set(at(12, 10, 0)) // exactly unknownBound after the attempt
	e.run("a", 1)
	if s := ptCondStatus(e.get("a"), v1alpha1.ConditionSubmissionResolved); s == corev1.ConditionFalse {
		t.Fatal("NeedsOperator raised at exactly unknownBound, want it only after")
	}
	e.clock.Set(at(12, 10, 1))
	e.run("a", 1)
	c := e.get("a").GetCondition(v1alpha1.ConditionSubmissionResolved)
	if c.Status != corev1.ConditionFalse || c.Reason != v1alpha1.ReasonNeedsOperator {
		t.Fatalf("SubmissionResolved = %s/%s, want False/NeedsOperator", c.Status, c.Reason)
	}
	if !e.rec.has("SubmissionUnresolved") {
		t.Error("no SubmissionUnresolved warning event")
	}

	ptSetAnnotation(e, "a", v1alpha1.AnnotationAbandonAttempt, "2") // not the attempt's number
	e.run("a", 2)
	if n := e.count("sbatch"); n != 0 {
		t.Fatalf("%d sbatch calls after a wrong abandon-attempt, want 0", n)
	}
	ptSetAnnotation(e, "a", v1alpha1.AnnotationAbandonAttempt, "1")
	e.run("a", 1)
	if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionAbandoned {
		t.Fatalf("record %+v after the abandon, want Abandoned", r)
	}
	c = e.get("a").GetCondition(v1alpha1.ConditionSubmissionResolved)
	if c.Status != corev1.ConditionTrue || c.Reason != v1alpha1.ReasonResolved {
		t.Errorf("SubmissionResolved = %s/%s after the abandon, want True/Resolved", c.Status, c.Reason)
	}
	e.run("a", 3)
	if n := e.count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls after the abandon, want exactly 1", n)
	}
	if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 2 {
		t.Errorf("record %+v, want attempt 2", r)
	}
}

// S4 scenario 3 (README table; HBEP-4, Phases): a requeue under --no-requeue is
// Failed with reason Requeued, and the verdict is kept when the job is gone
// (the Requeued field is kept once seen; lost after lostGrace, and the real
// phase returns with the job). A limit of this test: the fake cannot erase its
// own record of the requeue, so "no evidence left" is covered by the job being
// absent, not by a job without the flag.
func TestS4Scenario3RequeueIsFailedAndKept(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.newAlloc("a")
			e.run("a", 4)
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
				t.Fatalf("precondition: phase %s, want Running", p)
			}
			e.batch.Requeue(ptFirstJob)
			e.run("a", 2)
			wantFailed := func(step string) {
				t.Helper()
				o := e.get("a").Status.AtProvider
				if o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued || !o.Requeued {
					t.Errorf("%s: phase %s reason %q requeued %v, want Failed, Requeued, true", step, o.Phase, o.Reason, o.Requeued)
				}
			}
			wantFailed("after the requeue")
			if rc := e.get("a").Status.AtProvider.RestartCount; rc != 1 {
				t.Errorf("restart count %d, want 1", rc)
			}

			e.batch.Vanish(ptFirstJob) // the job is gone from both reads
			e.run("a", 1)
			wantFailed("job gone, inside lostGrace")
			e.clock.Advance(ptLostGrace + time.Second)
			e.run("a", 1)
			if o := e.get("a").Status.AtProvider; o.Phase != v1alpha1.AllocationLost || !o.Requeued {
				t.Errorf("after lostGrace: phase %s requeued %v, want Lost with Requeued kept", o.Phase, o.Requeued)
			}
			e.batch.Reappear(ptFirstJob)
			e.run("a", 1)
			wantFailed("job back")

			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1: a requeued job is not resubmitted", n)
			}
			if m.hold {
				if n := e.lifts(); n != 1 {
					t.Errorf("%d lifts, want 1: the requeue gets no second lift", n)
				}
			}
		})
	}
}

// S4 scenario 3, "requeue leaves the job held" (README table: 1 release in
// total; HBEP-7 Settle: lift only with restart count 0 and no hold-lifted
// marker). A job that comes back held with restart count 1 is never lifted. The
// adoption path has no hold-lifted marker, so the restart count is the only
// guard in this setup. The control, without the requeue, gets its 1 lift.
func TestS4Scenario3RequeuedHeldJobIsNeverLiftedAgain(t *testing.T) {
	setup := func(t *testing.T, requeue bool) (*env, string) {
		e := newEnv(t)
		a := e.newAlloc("a")
		id := ptSubmit(e, a, true)
		if requeue {
			e.batch.Requeue(id)
			if j, _ := e.batch.Job(id); j.Hold != provider.HoldUser || j.RestartCount != 1 {
				t.Fatalf("precondition: job %+v, want held by user with restart count 1", j)
			}
		}
		return e, id
	}

	t.Run("requeued", func(t *testing.T) {
		e, id := setup(t, true)
		mark := ptMark(e)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != id {
			t.Fatalf("job ID %q, want %s adopted", got, id)
		}
		if n := ptCountSince(e, mark, "scontrol-release"); n != 0 {
			t.Errorf("%d lift calls for a requeued held job, want 0", n)
		}
		if o := e.get("a").Status.AtProvider; o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued {
			t.Errorf("phase %s reason %q, want Failed, Requeued", o.Phase, o.Reason)
		}
	})
	t.Run("control: not requeued", func(t *testing.T) {
		e, id := setup(t, false)
		mark := ptMark(e)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != id {
			t.Fatalf("job ID %q, want %s adopted", got, id)
		}
		if n := ptCountSince(e, mark, "scontrol-release"); n != 1 {
			t.Errorf("%d lift calls for the same job without the requeue, want 1", n)
		}
	})
	t.Run("after its one lift", func(t *testing.T) {
		e := newEnv(t)
		e.newAlloc("a")
		e.run("a", 4)
		if n := e.lifts(); n != 1 {
			t.Fatalf("precondition: %d lifts, want 1", n)
		}
		e.batch.Requeue(ptFirstJob)
		e.run("a", 4)
		if n := ptLiftAttempts(e); n != 1 {
			t.Errorf("%d lift calls in all, want 1", n)
		}
	})
}

// ptInterrupt leaves a's Allocation as a crash inside Create does: crossplane's
// create-pending annotation with no outcome, the claim InFlight, and the job
// the sbatch registered. It returns the job's ID.
func ptInterrupt(e *env, name string, hold bool) string {
	e.t.Helper()
	a := e.get(name)
	id := ptSubmit(e, a, hold)
	meta.SetExternalName(a, tagOf(a))
	meta.SetExternalCreatePending(a, time.Now())
	if err := e.c.Update(e.ctx, a); err != nil {
		e.t.Fatal(err)
	}
	ptPlantRecord(e, name, ptInFlight(e, 1))
	if !meta.ExternalCreateIncomplete(e.get(name)) {
		e.t.Fatal("precondition: the create is not incomplete")
	}
	return id
}

// An interrupted create with the deterministic external name (HBEP-7 open
// question 11: the tag is the external name; crossplane-runtime
// WithDeterministicExternalName): the reconciler goes on to Observe and finds
// the job by tag. The control builds the same reconciler with the option off:
// it stops at "cannot determine creation result" and calls the platform not at
// all, which shows that the option is what lets the tag lookup run.
func TestInterruptedCreateGoesOnToObserveWithTheDeterministicName(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			t.Run("deterministic", func(t *testing.T) {
				e := newEnv(t, m.mods...)
				e.newAlloc("a")
				id := ptInterrupt(e, "a", m.hold)
				mark := ptMark(e)
				e.run("a", 4)
				if ptCountSince(e, mark, "squeue") == 0 {
					t.Error("the platform was not read: the reconciler did not reach Observe")
				}
				if got := ptJobID(e, "a"); got != id {
					t.Errorf("job ID %q, want the interrupted sbatch's job %s found by tag", got, id)
				}
				if n := e.count("sbatch"); n != 1 {
					t.Errorf("%d sbatch calls in all (the interrupted one included), want 1", n)
				}
				wantLifts := 0
				if m.hold {
					wantLifts = 1
				}
				if n := e.lifts(); n != wantLifts {
					t.Errorf("%d lifts, want %d", n, wantLifts)
				}
			})
			t.Run("control: not deterministic", func(t *testing.T) {
				e := newEnv(t, m.mods...)
				e.newAlloc("a")
				ptInterrupt(e, "a", m.hold)
				e.ctl = ptControllerOver(e, e.platform, managed.WithDeterministicExternalName(false))
				mark := ptMark(e)
				e.run("a", 4)
				if got := len(e.batch.Calls()) - mark; got != 0 {
					t.Errorf("%d platform calls from a reconciler that cannot determine the creation result, want 0", got)
				}
				if got := ptJobID(e, "a"); got != "" {
					t.Errorf("job ID %q recorded, want none: the reconciler stalls", got)
				}
				c := e.get("a").GetCondition(xpv2.TypeSynced)
				if c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "cannot determine creation result") {
					t.Errorf("Synced = %s %q, want False with the create-incomplete message", c.Status, c.Message)
				}
			})
		})
	}
}

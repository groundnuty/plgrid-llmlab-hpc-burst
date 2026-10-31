package providerctl

// Observe: adoption, duplicates, failed reads, Lost and its grace, the
// read-back, an admin hold and a lift that fails. Expected values come from
// HBEP-7 open question 11 (Adoption, Settle, Lost and revisable, Fallback
// mode), HBEP-4 (Read-back, Phases) and the doc comment of
// pkg/provider/fake/controller.go.

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// ptEnded reports whether the fake's own record of the job has ended in state.
func ptEnded(t *testing.T, e *env, id, state string) bool {
	t.Helper()
	j, ok := e.batch.Job(id)
	if !ok {
		t.Fatalf("the fake holds no job %s", id)
	}
	return j.EndTime != nil && len(j.State) > 0 && j.State[0] == state
}

// Adoption with no recorded job ID (HBEP-7 open question 11, Adoption; the
// fake.Controller doc defers to HBEP-7 here): a live job that is not held, then
// a job that ran, then the lowest job ID. In hold mode the other held jobs are
// cancelled while held, and only the adopted job's hold is ever lifted.
func TestAdoptionOrder(t *testing.T) {
	t.Run("a live unheld job beats a held job with a lower ID", func(t *testing.T) {
		e := newEnv(t)
		a := e.newAlloc("a")
		held := ptSubmit(e, a, true)    // 998, held
		unheld := ptSubmit(e, a, false) // 999, live and not held
		mark := ptMark(e)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != unheld {
			t.Errorf("adopted %q, want the live unheld job %s (not the lower ID %s)", got, unheld, held)
		}
		if !ptEnded(t, e, held, "CANCELLED") {
			t.Errorf("held job %s not cancelled", held)
		}
		if ptEnded(t, e, unheld, "CANCELLED") {
			t.Errorf("the adopted job %s was cancelled", unheld)
		}
		if n := ptCallsWithArg(e, mark, "scontrol-release", held); n != 0 {
			t.Errorf("%d lift calls for the held extra, want 0: it is cancelled while held", n)
		}
	})

	t.Run("else a job that ran", func(t *testing.T) {
		e := newEnv(t)
		a := e.newAlloc("a")
		e.batch.Enqueue(fake.Script{}, ptEndsAfter(time.Minute))
		held := ptSubmit(e, a, true)     // 998, held, never ran
		ran := ptSubmit(e, a, false)     // 999, runs one minute
		e.clock.Advance(2 * time.Minute) // 999 has ended, and left the queue
		mark := ptMark(e)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != ran {
			t.Errorf("adopted %q, want the job that ran, %s (not the lower ID %s)", got, ran, held)
		}
		if !ptEnded(t, e, held, "CANCELLED") {
			t.Errorf("held job %s not cancelled", held)
		}
		if n := ptCountSince(e, mark, "scontrol-release"); n != 0 {
			t.Errorf("%d lift calls, want 0: the adopted job already ran", n)
		}
	})

	t.Run("else the lowest ID, numerically", func(t *testing.T) {
		e := newEnv(t)
		a := e.newAlloc("a")
		// 998, 999 and 1000: the lowest as text is "1000".
		ids := []string{ptSubmit(e, a, true), ptSubmit(e, a, true), ptSubmit(e, a, true)}
		if ids[2] != "1000" {
			t.Fatalf("precondition: IDs %v", ids)
		}
		mark := ptMark(e)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != ids[0] {
			t.Errorf("adopted %q, want the lowest ID %s", got, ids[0])
		}
		for _, id := range ids[1:] {
			if !ptEnded(t, e, id, "CANCELLED") {
				t.Errorf("held job %s not cancelled", id)
			}
			if n := ptCallsWithArg(e, mark, "scontrol-release", id); n != 0 {
				t.Errorf("%d lift calls for %s, want 0", n, id)
			}
		}
		if n := e.lifts(); n != 1 {
			t.Errorf("%d lifts, want 1: only the adopted job's hold", n)
		}
		if ptEnded(t, e, ids[0], "CANCELLED") {
			t.Errorf("the adopted job %s was cancelled", ids[0])
		}
	})

	t.Run("fallback: the lowest live unheld job; the others are reported, not cancelled", func(t *testing.T) {
		e := newEnv(t, fallback)
		a := e.newAlloc("a")
		first, second := ptSubmit(e, a, false), ptSubmit(e, a, false)
		e.run("a", 4)
		if got := ptJobID(e, "a"); got != first {
			t.Errorf("adopted %q, want %s", got, first)
		}
		if d := e.get("a").Status.AtProvider.Duplicates; !slices.Equal(d, []string{second}) {
			t.Errorf("duplicates %v, want [%s]", d, second)
		}
		if n := e.count("scancel"); n != 0 {
			t.Errorf("%d scancel calls, want 0: fallback reports a duplicate and cancels it on deletion", n)
		}
		if !e.rec.has("DuplicateJob") {
			t.Error("no DuplicateJob event")
		}
	})
}

// S4 Astra B5 (README table; HBEP-7 Fallback mode: "extra jobs that are not
// held go into a sticky duplicates list"): when the original completes and is
// purged, the recorded ID is kept and the duplicate stays reported. Two
// orders: the duplicate was noticed first and then both ended, and the
// duplicate was first seen in the reconcile after the original had ended.
func TestDuplicateKeptAfterTheOriginalCompletesAndIsPurged(t *testing.T) {
	t.Run("duplicate noticed, then both end", func(t *testing.T) {
		e := newEnv(t, fallback)
		e.batch.Enqueue(fake.Script{}, ptEndsAfter(2*time.Minute)) // the original, then the duplicate
		a := e.newAlloc("a")
		e.run("a", 4)
		dup := ptSubmit(e, a, false)
		e.run("a", 1)
		want := []string{dup}
		if d := e.get("a").Status.AtProvider.Duplicates; !slices.Equal(d, want) {
			t.Fatalf("duplicates %v, want %v", d, want)
		}
		// The duplicate ends and is purged while the original still runs.
		ptStep(e, "a", 3*time.Minute)
		if d := e.get("a").Status.AtProvider.Duplicates; !slices.Equal(d, want) {
			t.Errorf("duplicates %v after the duplicate was purged, want the sticky %v", d, want)
		}
		// Then the original ends (30 m) and is purged.
		e.clock.Advance(30 * time.Minute)
		e.run("a", 3)
		got := e.get("a")
		if ptJobID(e, "a") != ptFirstJob || got.Status.AtProvider.JobID != ptFirstJob {
			t.Errorf("recorded job %q / %q, want %s kept", ptJobID(e, "a"), got.Status.AtProvider.JobID, ptFirstJob)
		}
		if got.Status.AtProvider.Phase != v1alpha1.AllocationEnded {
			t.Errorf("phase %s, want Ended", got.Status.AtProvider.Phase)
		}
		if d := got.Status.AtProvider.Duplicates; !slices.Equal(d, want) {
			t.Errorf("duplicates %v after the original ended, want %v", d, want)
		}
		if n := e.count("sbatch"); n != 2 {
			t.Errorf("%d sbatch calls (the original and the test's duplicate), want 2", n)
		}
	})

	t.Run("duplicate first seen after the original ended", func(t *testing.T) {
		e := newEnv(t, fallback)
		a := e.newAlloc("a")
		e.run("a", 4)
		e.clock.Advance(20 * time.Minute)
		dup := ptSubmit(e, a, false) // ends at 12:50, after the original's 12:30
		e.clock.Advance(15 * time.Minute)
		if j, _ := e.batch.Job(ptFirstJob); j.EndTime == nil {
			t.Fatal("precondition: the original has not ended")
		}
		e.run("a", 3)
		got := e.get("a")
		if ptJobID(e, "a") != ptFirstJob {
			t.Errorf("recorded job %q, want %s kept (not the live duplicate %s)", ptJobID(e, "a"), ptFirstJob, dup)
		}
		if got.Status.AtProvider.Phase != v1alpha1.AllocationEnded {
			t.Errorf("phase %s, want Ended", got.Status.AtProvider.Phase)
		}
		if d := got.Status.AtProvider.Duplicates; !slices.Equal(d, []string{dup}) {
			t.Errorf("duplicates %v, want [%s]", d, dup)
		}
	})
}

// Failed reads are not absence (provider package doc: "an error from a read
// means the plugin cannot tell ... and it never counts towards lostGrace";
// HBEP-7: Lost counts only reads that succeeded). The clock runs an hour with
// every read failing: the phase never becomes Lost, nothing is submitted, and
// the real phase returns when reads do. The control makes the job vanish
// instead, which is Lost.
func TestFakeFailedReadsAreNotAbsence(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.newAlloc("a")
			e.run("a", 4)
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
				t.Fatalf("precondition: phase %s, want Running", p)
			}
			e.batch.FailReads(errors.New("scripted: the platform is unreachable"))
			mark := ptMark(e)
			for i := 0; i < 12; i++ {
				a := ptStep(e, "a", 5*time.Minute)
				if p := a.Status.AtProvider.Phase; p == v1alpha1.AllocationLost || a.Status.AtProvider.AbsentSince != nil {
					t.Fatalf("after %d min of failed reads: phase %s, absentSince %v; want neither Lost nor absent", 5*(i+1), p, a.Status.AtProvider.AbsentSince)
				}
			}
			if e.rec.has("JobLost") {
				t.Error("a JobLost event during failed reads")
			}
			if n := ptCountSince(e, mark, "sbatch"); n != 0 {
				t.Errorf("%d sbatch calls during failed reads, want 0", n)
			}

			e.batch.FailReads(nil)
			e.run("a", 2)
			want, _ := e.batch.Job(ptFirstJob)
			if p := ptPhase(e, "a"); p != want.Phase || p != v1alpha1.AllocationEnded {
				t.Errorf("phase %s after the reads recovered, want the job's real phase %s (Ended)", p, want.Phase)
			}
			if n := ptCountSince(e, mark, "sbatch"); n != 0 {
				t.Errorf("%d sbatch calls after recovery, want 0", n)
			}
		})
	}

	t.Run("control: a vanished job is Lost", func(t *testing.T) {
		e := newEnv(t)
		e.newAlloc("a")
		e.run("a", 4)
		e.batch.Vanish(ptFirstJob)
		e.run("a", 1)
		for i := 0; i < 3; i++ {
			ptStep(e, "a", time.Minute)
		}
		if p := ptPhase(e, "a"); p != v1alpha1.AllocationLost {
			t.Errorf("phase %s three minutes after the job vanished, want Lost", p)
		}
	})
}

// Lost, and revisable (HBEP-7 open question 11; fake.Controller doc; lostGrace
// 2 min): after the job vanishes it is not Lost within 2 min and is Lost after,
// with a Warning event; when it reappears the real phase returns, with an event.
// It is never resubmitted.
func TestLostGraceAndReappearance(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.newAlloc("a")
			e.run("a", 4)
			e.batch.Vanish(ptFirstJob)
			e.run("a", 1) // absent since now
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
				t.Fatalf("phase %s on first absence, want the last known, Running", p)
			}
			ptStep(e, "a", ptLostGrace-time.Second)
			if p := ptPhase(e, "a"); p == v1alpha1.AllocationLost {
				t.Fatalf("phase Lost after %s, before lostGrace", ptLostGrace-time.Second)
			}
			ptStep(e, "a", time.Second) // exactly lostGrace
			if p := ptPhase(e, "a"); p == v1alpha1.AllocationLost {
				t.Fatalf("phase Lost at exactly lostGrace, want it only after")
			}
			if e.rec.has("JobLost") {
				t.Fatal("JobLost event before lostGrace")
			}
			ptStep(e, "a", time.Second)
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationLost {
				t.Fatalf("phase %s after lostGrace, want Lost", p)
			}
			if !e.rec.has("JobLost") {
				t.Error("no JobLost event")
			}

			e.batch.Reappear(ptFirstJob)
			e.run("a", 1)
			a := e.get("a")
			if p := a.Status.AtProvider.Phase; p != v1alpha1.AllocationRunning || a.Status.AtProvider.AbsentSince != nil {
				t.Errorf("phase %s absentSince %v after reappearance, want Running and none", p, a.Status.AtProvider.AbsentSince)
			}
			if !e.rec.has("JobReappeared") {
				t.Error("no JobReappeared event")
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1: a Lost job is never resubmitted", n)
			}
		})
	}
}

// Read-back, a different time limit (HBEP-4, Read-back; HBEP-7 Settle: lift
// only if the job passed the read-back): the job is established with 10 m, not
// the requested 30 m. SubmittedAsOther is True with a Warning event, and the
// hold is not lifted. The control, established as requested, gets its lift.
func TestReadBackDifferentTimeLimitIsNotLifted(t *testing.T) {
	t.Run("differs", func(t *testing.T) {
		e := newEnv(t)
		e.batch.Enqueue(fake.Script{AsTimeLimit: 10 * time.Minute})
		e.newAlloc("a")
		e.run("a", 4)
		a := e.get("a")
		c := a.GetCondition(v1alpha1.ConditionSubmittedAsOther)
		// api/v1alpha1: reason ReasonReadBackMismatch, a message naming each
		// field with both values.
		if c.Status != corev1.ConditionTrue || c.Reason != v1alpha1.ReasonReadBackMismatch || !strings.Contains(c.Message, "timeLimit: requested 30m0s, got 10m0s") {
			t.Errorf("SubmittedAsOther = %s %s %q, want True, ReadBackMismatch, naming the time limit with both values", c.Status, c.Reason, c.Message)
		}
		if n := ptLiftAttempts(e); n != 0 {
			t.Errorf("%d lift calls for a job established as other, want 0", n)
		}
		if p := a.Status.AtProvider.Phase; p != v1alpha1.AllocationHeld {
			t.Errorf("phase %s, want Held", p)
		}
		if !e.rec.has("SubmittedAsOther") {
			t.Error("no SubmittedAsOther event")
		}
	})
	t.Run("control: as requested", func(t *testing.T) {
		e := newEnv(t)
		e.newAlloc("a")
		e.run("a", 4)
		a := e.get("a")
		// A matching read-back sets no condition (fake.Controller's doc comment).
		if s := ptCondStatus(a, v1alpha1.ConditionSubmittedAsOther); s == corev1.ConditionTrue {
			t.Errorf("SubmittedAsOther = %s, want none", s)
		}
		if n := e.lifts(); n != 1 {
			t.Errorf("%d lifts, want 1", n)
		}
	})
	t.Run("fallback: the condition is set; nothing was held", func(t *testing.T) {
		e := newEnv(t, fallback)
		e.batch.Enqueue(fake.Script{AsTimeLimit: 10 * time.Minute})
		e.newAlloc("a")
		e.run("a", 4)
		if s := ptCondStatus(e.get("a"), v1alpha1.ConditionSubmittedAsOther); s != corev1.ConditionTrue {
			t.Errorf("SubmittedAsOther = %s, want True", s)
		}
	})
}

// Read-back, a rewritten name (HBEP-4, Read-back: "a name that differs refuses
// the Allocation and cancels the job"; fake.Script.Rename): the job's name came
// back shorter than the tag. The Allocation is Refused, the job is cancelled
// once, nothing is lifted or resubmitted, and Refused stays. The renamed job is
// invisible to the tag lookup, so the refusal comes from the recorded job.
func TestReadBackRenamedJobIsRefusedAndStaysRefused(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.batch.Enqueue(fake.Script{Rename: "hb-x"})
			a := e.newAlloc("a")
			if len("hb-x") >= len(tagOf(a)) {
				t.Fatal("precondition: the new name is not shorter than the tag")
			}
			e.run("a", 4)
			check := func(step string) {
				t.Helper()
				o := e.get("a").Status.AtProvider
				if o.Phase != v1alpha1.AllocationRefused || o.Reason != v1alpha1.ReasonJobRenamed {
					t.Errorf("%s: phase %s reason %q, want Refused for the rewritten name", step, o.Phase, o.Reason)
				}
			}
			check("after the first reconciles")
			if !ptEnded(t, e, ptFirstJob, "CANCELLED") {
				t.Error("the renamed job was not cancelled")
			}
			e.clock.Advance(ptWindow + time.Minute)
			e.run("a", 4)
			e.clock.Advance(time.Hour)
			e.run("a", 4)
			check("an hour later")
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1: a refused Allocation is not resubmitted", n)
			}
			if n := e.count("scancel"); n != 1 {
				t.Errorf("%d scancel calls, want 1", n)
			}
			if n := ptLiftAttempts(e); n != 0 {
				t.Errorf("%d lift calls for a renamed job, want 0", n)
			}
			if !e.rec.has("JobNameRewritten") {
				t.Error("no JobNameRewritten event")
			}
		})
	}
}

// An administrator's hold (HBEP-7 Settle: "an admin hold is reported as a
// condition and never retried"; fake.Controller doc: no lift call): the
// AdminHold condition is True, the phase Held, and no lift is attempted, however
// often it is observed. The control, a user hold, gets its lift call.
func TestAdminHoldIsReportedAndNeverLifted(t *testing.T) {
	t.Run("admin hold", func(t *testing.T) {
		e := newEnv(t)
		e.batch.Enqueue(fake.Script{AdminHold: true})
		e.newAlloc("a")
		e.run("a", 4)
		e.clock.Advance(time.Hour)
		e.run("a", 4)
		a := e.get("a")
		if s := ptCondStatus(a, v1alpha1.ConditionAdminHold); s != corev1.ConditionTrue {
			t.Errorf("AdminHold = %s, want True", s)
		}
		if p := a.Status.AtProvider.Phase; p != v1alpha1.AllocationHeld {
			t.Errorf("phase %s, want Held", p)
		}
		if n := ptLiftAttempts(e); n != 0 {
			t.Errorf("%d lift calls for an admin hold, want 0", n)
		}
	})
	t.Run("control: user hold", func(t *testing.T) {
		e := newEnv(t)
		e.newAlloc("a")
		e.run("a", 4)
		if n := ptLiftAttempts(e); n != 1 {
			t.Errorf("%d lift calls for a user hold, want 1", n)
		}
		if s := ptCondStatus(e.get("a"), v1alpha1.ConditionAdminHold); s == corev1.ConditionTrue {
			t.Error("AdminHold True for a user hold")
		}
	})
}

// A lift that fails with an unknown error (HBEP-7 Settle; fake.Controller doc):
// the job stays held and there is no hold-lifted annotation; the next
// reconcile lifts, and the annotation is written after that success. Two paths:
// the lift in Create's settle, and the lift in Observe's settle after an
// adoption.
func TestLiftFailureIsRetriedAndTheMarkerFollowsTheSuccess(t *testing.T) {
	check := func(t *testing.T, e *env, id string) {
		t.Helper()
		if got := e.get("a").Annotations[v1alpha1.AnnotationHoldLifted]; got != "" {
			t.Fatalf("hold-lifted %q written although the lift failed", got)
		}
		if j, _ := e.batch.Job(id); j.Hold != provider.HoldUser {
			t.Fatalf("job %s hold %v after the failed lift, want it still held", id, j.Hold)
		}
		if n, l := ptLiftAttempts(e), e.lifts(); n != 1 || l != 0 {
			t.Fatalf("%d lift calls, %d took effect; want 1 and 0", n, l)
		}
		e.run("a", 1)
		if got := e.get("a").Annotations[v1alpha1.AnnotationHoldLifted]; got != id {
			t.Errorf("hold-lifted %q after the retry, want %s", got, id)
		}
		if n, l := ptLiftAttempts(e), e.lifts(); n != 2 || l != 1 {
			t.Errorf("%d lift calls, %d took effect; want 2 and 1", n, l)
		}
		e.run("a", 3)
		if n := ptLiftAttempts(e); n != 2 {
			t.Errorf("%d lift calls after more reconciles, want 2: the hold is lifted once", n)
		}
		if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
			t.Errorf("phase %s, want Running", p)
		}
	}
	t.Run("in Create", func(t *testing.T) {
		e := newEnv(t)
		e.batch.FailNextLiftHold(errors.New("scripted: scontrol timed out"))
		e.newAlloc("a")
		e.run("a", 1)
		check(t, e, ptFirstJob)
	})
	t.Run("in Observe after an adoption", func(t *testing.T) {
		e := newEnv(t)
		a := e.newAlloc("a")
		id := ptSubmit(e, a, true)
		e.batch.FailNextLiftHold(errors.New("scripted: scontrol timed out"))
		e.run("a", 1)
		if got := ptJobID(e, "a"); got != id {
			t.Fatalf("job ID %q, want %s adopted", got, id)
		}
		check(t, e, id)
	})
}

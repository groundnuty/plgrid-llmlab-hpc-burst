package fake

import (
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The fallback-mode tests (HBEP-7, open question 11: Fallback mode and The
// operator condition). The default Controller runs fallback mode. Submissions
// are counted from the call log. Times are written out by hand.

// wantResolved checks the Allocation's SubmissionResolved condition.
func (e *env) wantResolved(name string, status corev1.ConditionStatus, reason xpv2.ConditionReason, message string) xpv2.Condition {
	e.t.Helper()
	c := e.alloc(name).GetCondition(v1alpha1.ConditionSubmissionResolved)
	if c.Status != status || c.Reason != reason || !strings.Contains(c.Message, message) {
		e.t.Fatalf("at %s SubmissionResolved = %+v, want %s %s with %q in its message", e.clock.Now().Format("15:04:05"), c, status, reason, message)
	}
	return c
}

func (e *env) wantNoConditions(name string) {
	e.t.Helper()
	if cs := e.alloc(name).Status.Conditions; len(cs) != 0 {
		e.t.Fatalf("at %s Allocation %s has conditions %+v, want none", e.clock.Now().Format("15:04:05"), name, cs)
	}
}

func TestFallbackInFlightClaimBecomesUnknownAfterTheWindow(t *testing.T) {
	for _, tc := range []struct {
		name          string
		window        time.Duration
		before, after time.Time
	}{
		{"the default window is 5m", 0, at(12, 5, 0), at(12, 5, 1)},
		{"a window of 90s", 90 * time.Second, at(12, 1, 30), at(12, 1, 31)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.ctl.InflightWindow = tc.window
			e.newAlloc("a1", testProvider)
			e.setRecordJSON("a1", `{"attempt":1,"state":"InFlight","writer":"fake","time":"2026-10-30T12:00:00Z"}`)
			e.syncAt(at(12, 0, 0))
			e.wantPhase("a1", v1alpha1.AllocationSubmitting)
			e.syncAt(tc.before)
			e.wantPhase("a1", v1alpha1.AllocationSubmitting)
			if rec := submission(t, e.alloc("a1")); rec.State != v1alpha1.SubmissionInFlight {
				t.Fatalf("record = %+v at the window, want it still InFlight", rec)
			}

			e.syncAt(tc.after)
			o := e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
			rec := submission(t, e.alloc("a1"))
			if rec.State != v1alpha1.SubmissionUnknown || rec.Attempt != 1 {
				t.Fatalf("record = %+v, want attempt 1 Unknown", rec)
			}
			wantMT(t, "record time", rec.Time, at(12, 0, 0)) // the attempt's time, not the conversion's
			if o.Submission == nil || o.Submission.State != v1alpha1.SubmissionUnknown {
				t.Fatalf("submission mirror = %+v", o.Submission)
			}
			e.syncAt(at(14, 0, 0)) // Unknown is never retried by time
			e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
			if n := e.batch.Count("sbatch"); n != 0 {
				t.Fatalf("sbatch count = %d, want 0: only a job found by tag, or an operator, resolves it", n)
			}
		})
	}
}

func TestFallbackOperatorConditionAfterTheUnknownBound(t *testing.T) {
	for _, tc := range []struct {
		name          string
		bound         time.Duration
		before, after time.Time
	}{
		{"the default bound is 10m", 0, at(12, 10, 0), at(12, 10, 1)},
		{"a bound of 3m", 3 * time.Minute, at(12, 3, 0), at(12, 3, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.ctl.UnknownBound = tc.bound
			e.batch.Enqueue(Script{Outcome: provider.Unknown})
			e.newAlloc("a1", testProvider)
			e.sync() // 12:00:00: Unknown, nothing found
			e.syncAt(tc.before)
			e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
			e.wantNoConditions("a1")

			e.syncAt(tc.after)
			e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
			c := e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, v1alpha1.AnnotationAbandonAttempt)
			wantMT(t, "condition time", &c.LastTransitionTime, tc.after)

			// The condition does not churn: later passes leave it as it was.
			before := e.alloc("a1").ResourceVersion
			e.syncAt(tc.after.Add(20 * time.Minute))
			e.syncAt(tc.after.Add(2 * time.Hour))
			if e.alloc("a1").ResourceVersion != before {
				t.Fatal("the unchanged condition was written again")
			}
			c = e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1")
			wantMT(t, "condition time", &c.LastTransitionTime, tc.after)
			if n := e.batch.Count("sbatch"); n != 1 {
				t.Fatalf("sbatch count = %d, want 1: no second job until an operator abandons the attempt", n)
			}
		})
	}
}

func TestFallbackBoundCountsFromTheAttemptNotFromTheConversion(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.setRecordJSON("a1", `{"attempt":1,"state":"InFlight","writer":"fake","time":"2026-10-30T12:00:00Z"}`)
	// The first pass is 20m after the claim: the window (5m) and the bound
	// (10m) have both passed, so the claim is Unknown and an operator is needed.
	e.syncAt(at(12, 20, 0))
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	rec := submission(t, e.alloc("a1"))
	wantMT(t, "record time", rec.Time, at(12, 0, 0))
	e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1")
}

func TestFallbackAbandonAllowsTheNextAttempt(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown}, Script{Outcome: provider.Unknown}) // then accepted
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: attempt 1 Unknown
	e.syncAt(at(12, 10, 1))
	e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1")

	// A value that is not the attempt's number in decimal does nothing.
	for _, wrong := range []string{"2", "0", "01", "one", ""} {
		e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, wrong)
		e.syncAt(at(12, 10, 30))
		e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
		e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1")
		if rec := submission(t, e.alloc("a1")); rec.State != v1alpha1.SubmissionUnknown {
			t.Fatalf("abandon-attempt %q: record = %+v, want it still Unknown", wrong, rec)
		}
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d before the abandon, want 1", n)
	}

	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "1")
	e.syncAt(at(12, 11, 0))
	rec := submission(t, e.alloc("a1"))
	if rec.State != v1alpha1.SubmissionAbandoned || rec.Attempt != 1 {
		t.Fatalf("record = %+v, want attempt 1 Abandoned", rec)
	}
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "abandoned")
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d in the abandoning pass, want 1: the next pass submits", n)
	}

	e.syncAt(at(12, 11, 0)) // the next pass: attempt 2, which is Unknown again
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}
	rec = submission(t, e.alloc("a1"))
	if rec.State != v1alpha1.SubmissionUnknown || rec.Attempt != 2 {
		t.Fatalf("record = %+v, want attempt 2 Unknown", rec)
	}
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)

	// The annotation still says 1: it never abandons attempt 2. Attempt 2 has
	// its own bound, from 12:11:00.
	e.syncAt(at(12, 21, 0))
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "abandoned")
	e.syncAt(at(12, 21, 1))
	e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 2")
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2: abandon-attempt 1 does not abandon attempt 2", n)
	}

	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "2")
	e.syncAt(at(12, 22, 0))
	e.syncAt(at(12, 22, 0)) // attempt 3 is accepted: job 998, as nothing registered before
	a := e.alloc("a1")
	if rec = submission(t, a); rec.State != v1alpha1.SubmissionDone || rec.Attempt != 3 {
		t.Fatalf("record = %+v, want attempt 3 Done", rec)
	}
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" || e.batch.Count("sbatch") != 3 {
		t.Fatalf("job-id %q, sbatch %d, want 998 and 3", a.Annotations[v1alpha1.AnnotationJobID], e.batch.Count("sbatch"))
	}
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "attempt 2 abandoned")
	e.wantPhase("a1", v1alpha1.AllocationRunning)
}

func TestFallbackJobFoundByTagWinsOverAnAbandon(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown, Registered: true})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: Unknown, and the scheduler holds job 998
	e.batch.Vanish("998")
	e.syncAt(at(12, 11, 0)) // past the bound, nothing found
	e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1")

	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "1")
	e.batch.Reappear("998") // the tag turns up before the operator's mark is read
	e.syncAt(at(12, 12, 0))
	a := e.alloc("a1")
	if rec := submission(t, a); rec.State != v1alpha1.SubmissionDone || rec.Attempt != 1 {
		t.Fatalf("record = %+v, want attempt 1 Done: a job found by tag wins", rec)
	}
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" {
		t.Fatalf("job-id = %q, want the adopted 998", a.Annotations[v1alpha1.AnnotationJobID])
	}
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "resolved by job 998")
	e.syncAt(at(12, 30, 0))
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "resolved by job 998") // it stays
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1", n)
	}
}

func TestFallbackResolutionBeforeTheBoundWritesNoCondition(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown, Registered: true})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	e.sync() // the tag is found
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.wantNoConditions("a1")
}

func TestHoldModeIgnoresAbandonAndRaisesNoOperatorCondition(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{Outcome: provider.Unknown}) // then accepted
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: Unknown, nothing found
	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "1")
	e.syncAt(at(12, 3, 0)) // inside the window
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	if rec := submission(t, e.alloc("a1")); rec.State != v1alpha1.SubmissionUnknown {
		t.Fatalf("record = %+v, want it still Unknown: hold mode ignores abandon-attempt", rec)
	}
	e.syncAt(at(12, 5, 1)) // past the window: dropped by time, never Abandoned
	if rec := submission(t, e.alloc("a1")); rec.Attempt != 2 || rec.State != v1alpha1.SubmissionDone {
		t.Fatalf("record = %+v, want attempt 2 Done", rec)
	}
	e.wantNoConditions("a1")
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}
}

func TestDeleteInFallbackModeReleasesAnAbandonedAttempt(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: Unknown, nothing found
	e.del("a1")
	e.syncAt(at(12, 30, 0))
	if e.gone("a1") {
		t.Fatal("the finalizer went with an Unknown attempt and no abandon")
	}
	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "2") // not this attempt
	e.syncAt(at(12, 31, 0))
	if e.gone("a1") {
		t.Fatal("the finalizer went for an abandon of another attempt")
	}
	e.annotate("a1", v1alpha1.AnnotationAbandonAttempt, "1")
	e.syncAt(at(12, 32, 0))
	if !e.gone("a1") {
		t.Fatal("the finalizer stayed after the operator abandoned the attempt")
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: a deleting Allocation submits nothing", n)
	}
}

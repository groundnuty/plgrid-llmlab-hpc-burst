package fake

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The read-back (HBEP-4, The Slurm dialect) and the retention mirror. Every
// count of a cancel comes from the call log. Expected messages and times are
// written out by hand. A job submitted at 12:00:00 with QueueWait 5m starts at
// 12:05:00.

func (e *env) mutateSpec(name string, f func(*v1alpha1.AllocationSpec)) {
	e.t.Helper()
	a := e.alloc(name)
	f(&a.Spec)
	if err := e.c.Update(context.Background(), a); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) setObservation(name string, f func(*v1alpha1.AllocationObservation)) {
	e.t.Helper()
	a := e.alloc(name)
	f(&a.Status.AtProvider)
	if err := e.c.Status().Update(context.Background(), a); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) wantCondition(name string, typ xpv2.ConditionType, status corev1.ConditionStatus, reason xpv2.ConditionReason, message string) {
	e.t.Helper()
	c := e.alloc(name).GetCondition(typ)
	if c.Status != status || c.Reason != reason || c.Message != message {
		e.t.Fatalf("at %s condition %s = %+v, want %s %s %q", e.clock.Now().Format("15:04:05"), typ, c, status, reason, message)
	}
}

func scancels(b *Batch) (ids []string) {
	for _, c := range b.Calls() {
		if c.Op == "scancel" {
			ids = append(ids, c.Args...)
		}
	}
	return ids
}

func TestARenamedJobIsRefusedAndCancelledOnce(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, Rename: "example-shortened-name"})
	e.newAlloc("a1", testProvider)
	tag := v1alpha1.JobTag(testNS, "a1", "uid-a1")
	wantMessage := `the scheduler stored job 998 under the name "example-shortened-name", not the tag "` + tag + `"; the job is cancelled`

	e.sync() // 12:00:00
	o := e.wantPhase("a1", v1alpha1.AllocationRefused)
	if o.Reason != "JobRenamed" {
		t.Fatalf("reason = %q, want JobRenamed", o.Reason)
	}
	e.wantCondition("a1", xpv2.TypeSynced, corev1.ConditionFalse, "JobRenamed", wantMessage)
	if got := scancels(e.batch); len(got) != 1 || got[0] != "998" {
		t.Fatalf("scancel calls after the first Sync = %v, want [998]", got)
	}
	e.wantJobState("998", "CANCELLED")

	for _, when := range []time.Time{at(12, 1, 0), at(12, 6, 0), at(13, 0, 0)} {
		e.syncAt(when)
		o = e.wantPhase("a1", v1alpha1.AllocationRefused) // the ended job does not overwrite it
		if o.Reason != "JobRenamed" {
			t.Fatalf("at %s reason = %q, want JobRenamed kept", when.Format("15:04:05"), o.Reason)
		}
	}
	e.wantCondition("a1", xpv2.TypeSynced, corev1.ConditionFalse, "JobRenamed", wantMessage)
	if got := scancels(e.batch); len(got) != 1 {
		t.Fatalf("scancel calls after several Syncs = %v, want exactly one", got)
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: a refused Allocation is not resubmitted", n)
	}
}

func TestARenamedHeldJobInHoldModeIsRefusedAndCancelled(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{Rename: "example-shortened-name"})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationRefused)
	e.wantJobState("998", "CANCELLED")
	e.syncAt(at(12, 1, 0))
	if got := scancels(e.batch); len(got) != 1 || got[0] != "998" {
		t.Fatalf("scancel calls = %v, want [998] once", got)
	}
	e.wantLifts(0, 0) // the renamed job is not found by tag, so settle never lifts it
}

func TestAHandSetRefusedSurvivesASync(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.setObservation("a1", func(o *v1alpha1.AllocationObservation) {
		o.Phase, o.Reason = v1alpha1.AllocationRefused, "example: set by hand"
	})
	e.syncAt(at(12, 6, 0)) // the job runs
	o := e.wantPhase("a1", v1alpha1.AllocationRefused)
	if o.Reason != "example: set by hand" {
		t.Fatalf("reason = %q, want it kept", o.Reason)
	}
	wantMT(t, "startTime", o.StartTime, at(12, 5, 0)) // the rest of the observation still follows the job
	if n := len(scancels(e.batch)); n != 0 {
		t.Fatalf("%d scancel calls, want 0: the job's name is the tag", n)
	}
}

func TestATimeLimitReadBackMismatchSetsSubmittedAsOther(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, AsTimeLimit: 20 * time.Minute}) // requested 30m
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 6, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationRunning) // the phase still mirrors the job
	wantMT(t, "hardEnd", o.HardEnd, at(12, 25, 0))
	const wantMessage = "job 998 was established with other settings than requested: timeLimit: requested 30m0s, got 20m0s"
	e.wantCondition("a1", v1alpha1.ConditionSubmittedAsOther, corev1.ConditionTrue, "ReadBackMismatch", wantMessage)
	if n := len(scancels(e.batch)); n != 0 {
		t.Fatalf("%d scancel calls, want 0: a mismatch does not cancel", n)
	}

	// An unchanged Sync writes nothing, and the condition is never cleared,
	// even when the job later shows the requested limit.
	before := e.alloc("a1").ResourceVersion
	e.sync()
	if after := e.alloc("a1").ResourceVersion; after != before {
		t.Fatalf("resourceVersion %s -> %s: an unchanged Allocation was written", before, after)
	}
	e.batch.SetTimeLimit("998", 30*time.Minute)
	e.syncAt(at(12, 7, 0))
	e.wantCondition("a1", v1alpha1.ConditionSubmittedAsOther, corev1.ConditionTrue, "ReadBackMismatch", wantMessage)
	e.wantPhase("a1", v1alpha1.AllocationRunning)
}

func TestAJobAsRequestedSetsNoSubmittedAsOther(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 6, 0))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.wantNoConditions("a1")
}

func TestReadBackNamesEachDifferingFieldWithBothValues(t *testing.T) {
	const prefix = "job 998 was established with other settings than requested: "
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.BatchParameters)
		want string
	}{
		{"partition", func(b *v1alpha1.BatchParameters) { b.Partition = "example-other" },
			`partition: requested "example-other", got "example-gpu"`},
		{"gpus", func(b *v1alpha1.BatchParameters) { b.GPUs = 2 },
			"gpus: requested 2, got 1"},
		{"time limit", func(b *v1alpha1.BatchParameters) { b.TimeLimit = "20m" },
			"timeLimit: requested 20m0s, got 30m0s"},
		{"all three", func(b *v1alpha1.BatchParameters) { b.Partition, b.GPUs, b.TimeLimit = "example-other", 2, "20m" },
			`partition: requested "example-other", got "example-gpu"; timeLimit: requested 20m0s, got 30m0s; gpus: requested 2, got 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.newAlloc("a1", testProvider)
			e.sync() // job 998 is established as requested: partition example-gpu, 30m, 1 GPU
			e.wantNoConditions("a1")
			// The spec is edited after the fact (the real CRD forbids it; the
			// fake client does not), which is a job that differs from the request.
			e.mutateSpec("a1", func(s *v1alpha1.AllocationSpec) { tc.edit(s.ForProvider.Batch) })
			e.syncAt(at(12, 1, 0))
			e.wantCondition("a1", v1alpha1.ConditionSubmittedAsOther, corev1.ConditionTrue, "ReadBackMismatch", prefix+tc.want)
			e.wantPhase("a1", v1alpha1.AllocationRunning)
		})
	}
}

func TestReadBackMismatchAndSubmissionResolvedCoexist(t *testing.T) {
	// A fallback-mode attempt that was Unknown raises NeedsOperator; then a job
	// with the tag and another limit shows up. The pass that adopts it sets
	// SubmissionResolved True and the read-back's condition together.
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown}) // the reply is lost and no job is registered
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 11, 0)) // past the 10m bound: NeedsOperator
	e.wantResolved("a1", corev1.ConditionFalse, v1alpha1.ReasonNeedsOperator, "attempt 1 is still unknown")
	e.batch.Enqueue(Script{AsTimeLimit: 20 * time.Minute})
	if id := e.submitFor("a1", false); id != "998" {
		t.Fatalf("setup: the late job is %s, want 998", id)
	}
	e.syncAt(at(12, 12, 0)) // found by tag, adopted, read back
	e.wantResolved("a1", corev1.ConditionTrue, v1alpha1.ReasonResolved, "attempt 1 resolved by job 998")
	e.wantCondition("a1", v1alpha1.ConditionSubmittedAsOther, corev1.ConditionTrue, "ReadBackMismatch",
		"job 998 was established with other settings than requested: timeLimit: requested 30m0s, got 20m0s")
}

func TestRetentionModeIsMirroredFromTheSpec(t *testing.T) {
	idle := v1alpha1.Duration("1h")
	for _, tc := range []struct {
		name   string
		policy *v1alpha1.RetentionPolicy
		want   v1alpha1.RetentionMode
	}{
		{"no policy", nil, "Keep"},
		{"a policy with no mode", &v1alpha1.RetentionPolicy{}, "Keep"},
		{"Keep", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}, "Keep"},
		{"ReleaseWhenIdle", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &idle}, "ReleaseWhenIdle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.newAlloc("a1", testProvider)
			e.mutateSpec("a1", func(s *v1alpha1.AllocationSpec) { s.RetentionPolicy = tc.policy })
			e.sync()
			e.wantRetention("a1", tc.want)
		})
	}
}

func (e *env) wantRetention(name string, want v1alpha1.RetentionMode) {
	e.t.Helper()
	r := e.alloc(name).Status.Retention
	if r == nil || r.EffectiveMode != want {
		e.t.Fatalf("at %s Allocation %s status.retention = %+v, want effectiveMode %s", e.clock.Now().Format("15:04:05"), name, r, want)
	}
}

func TestRetentionIsMirroredOnEveryStatusWrite(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Rejected})
	e.newAlloc("a1", testProvider)
	e.sync() // the submission is refused: the status written has phase Submitting, not a job's
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	e.wantRetention("a1", "Keep")

	// A change of the policy shows at the next Sync even though nothing else
	// changed.
	idle := v1alpha1.Duration("1h")
	e.mutateSpec("a1", func(s *v1alpha1.AllocationSpec) {
		s.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &idle}
	})
	e.sync() // the same instant: in the reject backoff
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	e.wantRetention("a1", "ReleaseWhenIdle")
	e.mutateSpec("a1", func(s *v1alpha1.AllocationSpec) { s.RetentionPolicy = nil })
	e.syncAt(at(12, 2, 0)) // the next attempt is accepted
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.wantRetention("a1", "Keep")
}

// Refused is kept on every path, not only when a job is observed.
func TestARefusedAllocationWithNoJobSubmitsNothingAndStaysRefused(t *testing.T) {
	for _, script := range []Script{{Outcome: provider.Rejected}, {}} { // a refusal and an acceptance
		e := newEnv(t)
		e.batch.Enqueue(script)
		e.newAlloc("a1", testProvider)
		e.setObservation("a1", func(o *v1alpha1.AllocationObservation) {
			o.Phase, o.Reason = v1alpha1.AllocationRefused, "example: invalid profile"
		})
		e.sync()
		e.syncAt(at(12, 5, 0))
		o := e.wantPhase("a1", v1alpha1.AllocationRefused)
		if o.Reason != "example: invalid profile" {
			t.Fatalf("reason = %q, want it kept", o.Reason)
		}
		if n := e.batch.Count("sbatch"); n != 0 {
			t.Fatalf("sbatch logged %d times, want 0: a refused Allocation submits nothing", n)
		}
		e.wantRetention("a1", "Keep") // its status is still written
	}
}

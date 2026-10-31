package fake

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func (e *env) stageFile(path, content string) {
	e.t.Helper()
	if err := e.batch.EnsureFile(context.Background(), file(path, content, 0o600)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) del(name string) {
	e.t.Helper()
	if err := e.c.Delete(context.Background(), e.alloc(name)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) gone(name string) bool {
	e.t.Helper()
	var a v1alpha1.Allocation
	err := e.c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: name}, &a)
	if apierrors.IsNotFound(err) {
		return true
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return false
}

func TestDeleteCancelsLiveJobsThenRemovesFinalizer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script Script
		at     time.Time // when the Allocation is deleted
	}{
		{"pending", Script{QueueWait: 10 * time.Minute}, at(12, 1, 0)},
		{"running", Script{QueueWait: 5 * time.Minute}, at(12, 10, 0)},
		{"held by an administrator", Script{AdminHold: true}, at(12, 1, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.batch.Enqueue(tc.script)
			e.newAlloc("a1", testProvider)
			e.sync()
			tag := v1alpha1.JobTag(testNS, "a1", "uid-a1")
			e.stageFile("alloc/"+tag+"/credential", "example-credential")
			e.stageFile("alloc/hb-another-0123456789ab/credential", "example-credential")
			e.syncAt(tc.at)
			if j, _ := e.batch.Job("998"); j.EndTime != nil {
				t.Fatalf("setup: job 998 has ended: %v", j.State)
			}
			e.del("a1")

			e.sync()
			if !e.gone("a1") {
				t.Fatalf("Allocation survived its deletion: %+v", e.alloc("a1").ObjectMeta)
			}
			j, _ := e.batch.Job("998")
			if j.State[0] != "CANCELLED" {
				t.Fatalf("job 998 state = %v, want CANCELLED", j.State)
			}
			wantTime(t, "EndTime", j.EndTime, tc.at)
			if n := e.batch.Count("scancel"); n != 1 {
				t.Fatalf("scancel logged %d times, want 1", n)
			}
			if _, ok := e.batch.Files()["alloc/"+tag+"/credential"]; ok {
				t.Fatal("the Allocation's credential file is still staged")
			}
			if _, ok := e.batch.Files()["alloc/hb-another-0123456789ab/credential"]; !ok {
				t.Fatal("another Allocation's file was removed")
			}
			if n := e.batch.Count("remove-allocation-files"); n != 1 {
				t.Fatalf("remove-allocation-files logged %d times, want 1", n)
			}
		})
	}
}

func TestDeleteCancelsEveryJobWithTheTag(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync() // job 998 recorded
	tag := v1alpha1.JobTag(testNS, "a1", "uid-a1")
	// A duplicate with the tag, as a late registration would leave.
	dup, outcome, err := e.batch.Submit(context.Background(), provider.SubmitRequest{Tag: tag, Hold: true, Params: *e.alloc("a1").Spec.ForProvider.Batch})
	if outcome != provider.Accepted || err != nil {
		t.Fatalf("setup Submit = %s %v", outcome, err)
	}
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	for _, id := range []string{"998", dup} {
		if j, _ := e.batch.Job(id); j.State[0] != "CANCELLED" {
			t.Fatalf("job %s state = %v, want CANCELLED", id, j.State)
		}
	}
}

func TestDeleteCancelsARequeuedJobThatStillHoldsItsAllocation(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.Requeue("998") // Failed by phase, but not ended
	e.syncAt(at(12, 1, 0))
	e.wantPhase("a1", v1alpha1.AllocationFailed)
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	if j, _ := e.batch.Job("998"); j.EndTime == nil || j.State[0] != "CANCELLED" {
		t.Fatalf("job 998 = %v end %v, want it cancelled", j.State, j.EndTime)
	}
}

func TestDeleteOfEndedJobCancelsNothing(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(13, 0, 0)) // ended 12:30:00
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	if n := e.batch.Count("scancel"); n != 0 {
		t.Fatalf("scancel logged %d times for an ended job, want 0", n)
	}
}

func TestDeleteWaitsOutFailedReads(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	e.del("a1")
	e.batch.FailReads(errors.New("example transport failure"))
	e.sync()
	if e.gone("a1") {
		t.Fatal("the finalizer was removed while the jobs were unknown")
	}
	if e.batch.Count("scancel") != 0 {
		t.Fatal("jobs were cancelled without being read")
	}
	e.batch.FailReads(nil)
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived once reads worked")
	}
}

func TestDeleteWithUnknownAttemptAndNoJobKeepsTheFinalizer(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.del("a1")
	for i := 1; i <= 3; i++ {
		e.syncAt(at(12, 0, 0).Add(time.Duration(i) * 10 * time.Minute))
		if e.gone("a1") {
			t.Fatal("the finalizer was removed with an Unknown attempt and no job found (HBEP-7, open question 11)")
		}
	}
	if e.batch.Count("sbatch") != 1 {
		t.Fatalf("sbatch count = %d, want 1: a deleting Allocation submits nothing", e.batch.Count("sbatch"))
	}
}

func TestDeleteWithUnknownAttemptWhoseJobIsFoundCancelsIt(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown, Registered: true})
	e.newAlloc("a1", testProvider)
	e.sync() // Unknown; the job exists
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived: the Unknown attempt's job was found and cancelled")
	}
	if j, _ := e.batch.Job("998"); j.State[0] != "CANCELLED" {
		t.Fatalf("job 998 state = %v, want CANCELLED", j.State)
	}
	if n := e.batch.Count("scontrol-release"); n != 0 {
		t.Fatalf("a deleting Allocation lifted %d holds, want 0", n)
	}
}

func TestHoldReleaseLiftsTheHoldOnce(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)

	e.sync()
	a := e.alloc("a1")
	if a.Annotations[v1alpha1.AnnotationHoldLifted] != "998" {
		t.Fatalf("hold-lifted annotation = %q, want 998 (a lift of an unheld job would set none)", a.Annotations[v1alpha1.AnnotationHoldLifted])
	}
	o := e.wantPhase("a1", v1alpha1.AllocationPending)
	if o.Held {
		t.Fatal("the observation still shows a hold")
	}
	e.syncAt(at(12, 5, 0))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.syncAt(at(12, 6, 0))
	if n := e.batch.Count("scontrol-release"); n != 1 {
		t.Fatalf("scontrol-release logged %d times, want 1", n)
	}
	// The queue wait counts from the lift, which was immediate: 5m from submit.
	if w := e.provider().Status.QueueWaits; len(w) != 1 || w[0].Wait() != 5*time.Minute {
		t.Fatalf("queue waits = %+v, want one of 5m", w)
	}
}

func TestWithoutHoldReleaseNothingIsHeldOrLifted(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; ok {
		t.Fatal("a hold-lifted annotation without hold mode")
	}
	if e.batch.Count("scontrol-release") != 0 {
		t.Fatal("a hold was lifted without hold mode")
	}
}

func TestHoldReleaseAdminHoldStaysHeldAndIsNotRetried(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{AdminHold: true})
	e.newAlloc("a1", testProvider)
	e.sync()
	o := e.wantPhase("a1", v1alpha1.AllocationHeld)
	if !o.Held || o.Reason != "JobHeldAdmin" {
		t.Fatalf("observation = held %v reason %q, want held by JobHeldAdmin", o.Held, o.Reason)
	}
	if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; ok {
		t.Fatal("hold-lifted set for an admin hold")
	}
	e.syncAt(at(12, 10, 0))
	e.syncAt(at(12, 20, 0))
	e.wantPhase("a1", v1alpha1.AllocationHeld)
	// HBEP-7, Settle: a hold is lifted only for reason JobHeldUser, so an
	// admin-held job is never offered to scontrol at all.
	if n := e.batch.Count("scontrol-release"); n != 0 {
		t.Fatalf("scontrol-release logged %d times, want 0: an admin hold is never lifted or retried", n)
	}
}

func TestHoldReleaseAdoptedJobIsLiftedOnce(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{Outcome: provider.Unknown, Registered: true})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	if n := e.batch.Count("scontrol-release"); n != 0 {
		t.Fatalf("scontrol-release logged %d times before the adoption, want 0", n)
	}
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	if got := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; got != "998" {
		t.Fatalf("hold-lifted annotation = %q, want 998", got)
	}
	e.syncAt(at(12, 1, 0))
	if e.batch.Count("scontrol-release") != 1 || e.batch.Count("sbatch") != 1 {
		t.Fatalf("scontrol-release %d, sbatch %d, want 1 and 1", e.batch.Count("scontrol-release"), e.batch.Count("sbatch"))
	}
}

func TestQueueWaitCapDropsTheOldestAndNeverChurns(t *testing.T) {
	e := newEnv(t)
	// 21 jobs, each submitted a minute after the last and started at once.
	for i := range 21 {
		e.clock.Set(at(12, 0, 0).Add(time.Duration(i) * time.Minute))
		e.newAlloc(fmt.Sprintf("a%02d", i), testProvider)
		e.sync()
	}
	var want []string
	for id := 999; id <= 1018; id++ { // job 998 is the oldest and went
		want = append(want, strconv.Itoa(id))
	}
	waits := e.provider().Status.QueueWaits
	if got := waitIDs(e.provider()); !reflect.DeepEqual(got, want) {
		t.Fatalf("queue wait job IDs = %v, want %v", got, want)
	}
	if len(waits) != v1alpha1.MaxQueueWaits {
		t.Fatalf("%d queue waits, want %d", len(waits), v1alpha1.MaxQueueWaits)
	}
	wantMT(t, "newest wait start", &waits[19].StartTime, at(12, 20, 0))

	// The dropped job (998, Allocation a00) is observed again. Its wait must not
	// come back and push out another. Re-observing every job in start order
	// would hide a bug, because the sweep rewrites the window; so the newer
	// Allocations are deleted first, and a00 alone is observed.
	for i := 1; i < 21; i++ {
		e.del(fmt.Sprintf("a%02d", i))
	}
	e.syncAt(at(12, 21, 0))
	for i := 1; i < 21; i++ {
		if !e.gone(fmt.Sprintf("a%02d", i)) {
			t.Fatalf("Allocation a%02d survived its deletion", i)
		}
	}
	e.wantPhase("a00", v1alpha1.AllocationRunning)
	if got := waitIDs(e.provider()); !reflect.DeepEqual(got, want) {
		t.Fatalf("after job 998 was observed again the waits changed to %v", got)
	}
	// A restarted controller reads the same persisted status and does the same.
	e.ctl = &Controller{Client: e.c, Provider: testProvider, Batch: e.batch, Clock: e.clock}
	e.syncAt(at(12, 23, 0))
	if got := waitIDs(e.provider()); !reflect.DeepEqual(got, want) {
		t.Fatalf("after a controller restart the waits changed to %v", got)
	}
}

func TestQueueWaitsInOnePassGoOldestStartFirst(t *testing.T) {
	e := newEnv(t)
	// Names sort a, b, c; their jobs start 20m, 5m and 10m after submission.
	e.batch.Enqueue(Script{QueueWait: 20 * time.Minute}, Script{QueueWait: 5 * time.Minute}, Script{QueueWait: 10 * time.Minute})
	for _, n := range []string{"a", "b", "c"} {
		e.newAlloc(n, testProvider)
	}
	e.sync() // 12:00:00: all three submitted, pending: jobs 998, 999, 1000
	e.syncAt(at(12, 25, 0))
	if got, want := waitIDs(e.provider()), []string{"999", "1000", "998"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue wait job IDs = %v, want %v (by start time, not by name)", got, want)
	}
}

func TestSyncWritesNothingWhenNothingChanged(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	before := e.alloc("a1").ResourceVersion
	e.sync() // the same instant
	if after := e.alloc("a1").ResourceVersion; after != before {
		t.Fatalf("resourceVersion %s -> %s: an unchanged Allocation was written", before, after)
	}
}

package fake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// As in batch_test.go, expected times are literals worked out from the script.
// Every time is a whole second, because metav1.Time keeps no more.

const (
	testProvider = "example-provider"
	testNS       = "example-ns"
)

type env struct {
	t     *testing.T
	clock *Clock
	batch *Batch
	c     client.Client
	ctl   *Controller
}

// withManagedFields makes the fake client record managedFields, so a test sees
// which field manager wrote what.
func withManagedFields(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
	return b.WithReturnManagedFields()
}

func newEnv(t *testing.T, opts ...func(*clientfake.ClientBuilder) *clientfake.ClientBuilder) *env {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// A deleting Allocation gets its accounting record, a ControllerRevision.
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clock := NewClock(at(12, 0, 0))
	batch := NewBatch(clock, Config{Partitions: []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
	}})
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: testProvider, Generation: 1}}
	builder := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(prov).
		WithStatusSubresource(&v1alpha1.Allocation{}, &v1alpha1.Provider{})
	for _, opt := range opts {
		builder = opt(builder)
	}
	c := builder.Build()
	return &env{t: t, clock: clock, batch: batch, c: c,
		ctl: &Controller{Client: c, Provider: testProvider, Batch: batch, Clock: clock}}
}

func (e *env) newAlloc(name, providerName string) *v1alpha1.Allocation {
	e.t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: testNS, UID: types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(e.clock.Now()),
		},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: providerName},
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
				Partition: gpuPart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8,
				Memory: resource.MustParse("16Gi"), AgentDigest: strings.Repeat("a", 64),
			}},
		},
	}
	if err := e.c.Create(context.Background(), a); err != nil {
		e.t.Fatal(err)
	}
	return a
}

func (e *env) alloc(name string) *v1alpha1.Allocation {
	e.t.Helper()
	var a v1alpha1.Allocation
	if err := e.c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: name}, &a); err != nil {
		e.t.Fatalf("get Allocation %s: %v", name, err)
	}
	return &a
}

func (e *env) obs(name string) v1alpha1.AllocationObservation { return e.alloc(name).Status.AtProvider }

func (e *env) provider() *v1alpha1.Provider {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testProvider}, &p); err != nil {
		e.t.Fatal(err)
	}
	return &p
}

func (e *env) sync() {
	e.t.Helper()
	if err := e.ctl.Sync(context.Background()); err != nil {
		e.t.Fatalf("Sync: %v", err)
	}
}

func (e *env) syncAt(t time.Time) {
	e.t.Helper()
	e.clock.Set(t)
	e.sync()
}

func (e *env) wantPhase(name string, want v1alpha1.AllocationPhase) v1alpha1.AllocationObservation {
	e.t.Helper()
	o := e.obs(name)
	if o.Phase != want {
		e.t.Fatalf("at %s Allocation %s phase = %q (reason %q), want %q", e.clock.Now().Format("15:04:05"), name, o.Phase, o.Reason, want)
	}
	return o
}

func wantMT(t *testing.T, name string, got *metav1.Time, want time.Time) {
	t.Helper()
	if got == nil || !got.Time.Equal(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func wantNilMT(t *testing.T, name string, got *metav1.Time) {
	t.Helper()
	if got != nil {
		t.Fatalf("%s = %v, want nil", name, got)
	}
}

func submission(t *testing.T, a *v1alpha1.Allocation) v1alpha1.SubmissionRecord {
	t.Helper()
	raw, ok := a.Annotations[v1alpha1.AnnotationSubmission]
	if !ok {
		t.Fatalf("Allocation %s has no submission record", a.Name)
	}
	var r v1alpha1.SubmissionRecord
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func waitIDs(p *v1alpha1.Provider) []string {
	var out []string
	for _, w := range p.Status.QueueWaits {
		out = append(out, w.JobID)
	}
	return out
}

func TestSubmitThenPendingThenRunning(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)

	e.sync() // 12:00:00
	a := e.alloc("a1")
	wantTag := v1alpha1.JobTag(testNS, "a1", "uid-a1")
	if a.Annotations[v1alpha1.AnnotationExternalName] != wantTag {
		t.Fatalf("external name = %q, want %q", a.Annotations[v1alpha1.AnnotationExternalName], wantTag)
	}
	if len(a.Finalizers) != 1 || a.Finalizers[0] != v1alpha1.FinalizerCancelAll {
		t.Fatalf("finalizers = %v, want the cancel-all finalizer", a.Finalizers)
	}
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" {
		t.Fatalf("job-id annotation = %q, want 998", a.Annotations[v1alpha1.AnnotationJobID])
	}
	rec := submission(t, a)
	if rec.Attempt != 1 || rec.State != v1alpha1.SubmissionDone || rec.Writer != "fake" {
		t.Fatalf("record = %+v, want attempt 1 Done by fake", rec)
	}
	wantMT(t, "record time", rec.Time, at(12, 0, 0))

	o := e.wantPhase("a1", v1alpha1.AllocationPending)
	if o.Tag != wantTag || o.JobID != "998" || o.Held || o.Requeued || o.RestartCount != 0 {
		t.Fatalf("observation = %+v", o)
	}
	wantMT(t, "submitTime", o.SubmitTime, at(12, 0, 0))
	wantNilMT(t, "startTime", o.StartTime)
	wantNilMT(t, "hardEnd", o.HardEnd)
	wantNilMT(t, "endTime", o.EndTime)
	if o.Submission == nil || o.Submission.State != v1alpha1.SubmissionDone {
		t.Fatalf("submission mirror = %+v", o.Submission)
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1", n)
	}
	if len(e.provider().Status.QueueWaits) != 0 {
		t.Fatalf("queue waits before any start: %v", e.provider().Status.QueueWaits)
	}

	e.syncAt(at(12, 4, 59))
	e.wantPhase("a1", v1alpha1.AllocationPending)
	if len(e.provider().Status.QueueWaits) != 0 {
		t.Fatal("a pending job added a queue wait")
	}

	e.syncAt(at(12, 5, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "startTime", o.StartTime, at(12, 5, 0))
	wantMT(t, "hardEnd", o.HardEnd, at(12, 35, 0))
	wantNilMT(t, "startEstimate", o.StartEstimate)

	prov := e.provider()
	if len(prov.Status.QueueWaits) != 1 {
		t.Fatalf("queue waits = %+v, want exactly one", prov.Status.QueueWaits)
	}
	w := prov.Status.QueueWaits[0]
	wantMT(t, "wait submit", &w.SubmitTime, at(12, 0, 0))
	wantMT(t, "wait start", &w.StartTime, at(12, 5, 0))
	if w.JobID != "998" || w.Wait() != 5*time.Minute {
		t.Fatalf("queue wait = %+v, want job 998 and 5m", w)
	}
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 5, 0))
	if n := len(prov.Status.Conditions); n != 1 {
		t.Fatalf("provider conditions = %+v, want one", prov.Status.Conditions)
	}
	cond := prov.Status.Conditions[0]
	if cond.Type != "Ready" || cond.Status != metav1.ConditionTrue || cond.Reason != "Polled" {
		t.Fatalf("condition = %+v, want Ready True Polled", cond)
	}

	e.syncAt(at(12, 6, 0)) // a repeated observe adds nothing
	e.syncAt(at(12, 7, 0))
	if n := len(e.provider().Status.QueueWaits); n != 1 {
		t.Fatalf("after more Syncs: %d queue waits, want 1", n)
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d after more Syncs, want 1", n)
	}
}

func TestAllocationOfAnotherProviderIsLeftAlone(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("other", "example-other-provider")
	e.sync()
	a := e.alloc("other")
	if len(a.Finalizers) != 0 || len(a.Annotations) != 0 || a.Status.AtProvider.Phase != "" {
		t.Fatalf("another Provider's Allocation was touched: %+v", a.ObjectMeta)
	}
	if e.batch.Count("sbatch") != 0 {
		t.Fatal("a job was submitted for another Provider's Allocation")
	}
}

func TestLateStartStaysPending(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 10 * time.Minute, StartEstimate: 3 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	o := e.wantPhase("a1", v1alpha1.AllocationPending)
	wantMT(t, "startEstimate", o.StartEstimate, at(12, 3, 0))

	e.syncAt(at(12, 9, 59)) // the estimate is long past, the start is not
	o = e.wantPhase("a1", v1alpha1.AllocationPending)
	wantMT(t, "startEstimate", o.StartEstimate, at(12, 3, 0))
	wantNilMT(t, "startTime", o.StartTime)

	e.syncAt(at(12, 10, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantNilMT(t, "startEstimate", o.StartEstimate)
}

func TestFailedStartIsFailed(t *testing.T) {
	e := newEnv(t)
	zero := time.Duration(0)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &zero, FailState: "BOOT_FAIL"})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 4, 59))
	e.wantPhase("a1", v1alpha1.AllocationPending)
	e.syncAt(at(12, 5, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationFailed)
	wantMT(t, "endTime", o.EndTime, at(12, 5, 0))
	if o.Reason != "BootFail" {
		t.Fatalf("reason = %q, want the scheduler's BootFail", o.Reason)
	}
}

func TestHardEndIsEnded(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 34, 59))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.syncAt(at(12, 35, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationEnded)
	wantMT(t, "endTime", o.EndTime, at(12, 35, 0))
	wantMT(t, "hardEnd", o.HardEnd, at(12, 35, 0))

	// Purged from the queue listing 10 s later: still Ended, never Lost.
	e.syncAt(at(12, 35, 10))
	e.syncAt(at(14, 0, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationEnded)
	wantNilMT(t, "absentSince", o.AbsentSince)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: nothing is submitted again for an ended job", n)
	}
}

func TestLostAfterGraceAndRevisable(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync() // submitted and running at 12:00:00
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.batch.Vanish("998")

	e.syncAt(at(12, 1, 0)) // first read that does not show the job
	o := e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "absentSince", o.AbsentSince, at(12, 1, 0))
	e.syncAt(at(12, 3, 0)) // exactly LostGrace (2m) after absentSince: not yet
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "absentSince", o.AbsentSince, at(12, 1, 0))
	e.syncAt(at(12, 3, 1)) // one second more
	o = e.wantPhase("a1", v1alpha1.AllocationLost)
	if o.JobID != "998" {
		t.Fatalf("jobID = %q, want 998 kept", o.JobID)
	}

	e.batch.Reappear("998")
	e.syncAt(at(12, 4, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantNilMT(t, "absentSince", o.AbsentSince)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: a Lost job is never resubmitted", n)
	}
}

func TestLostGraceFromTheController(t *testing.T) {
	e := newEnv(t)
	e.ctl.LostGrace = 10 * time.Second
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.Vanish("998")
	e.syncAt(at(12, 1, 0))
	e.syncAt(at(12, 1, 10))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.syncAt(at(12, 1, 11))
	e.wantPhase("a1", v1alpha1.AllocationLost)
}

func TestFailedReadsAreNeverAbsence(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync() // running at 12:00:00, last good read 12:00:00
	boom := errors.New("example transport failure")
	e.batch.FailReads(boom)

	// Far longer than LostGrace (2m) of failing reads, each pass without an error.
	for _, when := range []time.Time{at(12, 1, 0), at(12, 5, 0), at(12, 29, 0), at(12, 29, 59)} {
		e.syncAt(when)
		o := e.wantPhase("a1", v1alpha1.AllocationRunning)
		wantNilMT(t, "absentSince", o.AbsentSince)
		wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 0, 0))
	}

	e.batch.FailReads(nil)
	e.syncAt(at(13, 1, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationEnded) // the 30m lease ended meanwhile
	wantNilMT(t, "absentSince", o.AbsentSince)
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(13, 1, 0))
}

func TestUnknownRegisteredIsAdoptedOnTheNextSync(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown, Registered: true, Reason: "example: reply lost"})
	e.newAlloc("a1", testProvider)

	e.sync()
	o := e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	a := e.alloc("a1")
	if _, ok := a.Annotations[v1alpha1.AnnotationJobID]; ok {
		t.Fatal("a job ID was recorded for an Unknown attempt")
	}
	rec := submission(t, a)
	if rec.State != v1alpha1.SubmissionUnknown || rec.Attempt != 1 || rec.Reason != "example: reply lost" {
		t.Fatalf("record = %+v, want attempt 1 Unknown with the reason", rec)
	}
	if o.Submission == nil || o.Submission.State != v1alpha1.SubmissionUnknown {
		t.Fatalf("submission mirror = %+v", o.Submission)
	}

	e.sync() // the same instant: the tag is found
	a = e.alloc("a1")
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" {
		t.Fatalf("job-id annotation = %q, want the adopted 998", a.Annotations[v1alpha1.AnnotationJobID])
	}
	if rec = submission(t, a); rec.State != v1alpha1.SubmissionDone || rec.Attempt != 1 {
		t.Fatalf("record = %+v, want attempt 1 Done", rec)
	}
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.syncAt(at(12, 10, 0))
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want exactly 1", n)
	}
}

func TestUnknownNotRegisteredNeverSubmitsAgain(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Unknown})
	e.newAlloc("a1", testProvider)
	e.sync()
	// Each step is longer than LostGrace and RejectBackoff together: elapsed
	// time never permits a retry.
	for i := 1; i <= 6; i++ {
		e.syncAt(at(12, 0, 0).Add(time.Duration(i) * 10 * time.Minute))
		e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
		if n := e.batch.Count("sbatch"); n != 1 {
			t.Fatalf("after %d Syncs sbatch count = %d, want 1", i, n)
		}
	}
	a := e.alloc("a1")
	if _, ok := a.Annotations[v1alpha1.AnnotationJobID]; ok {
		t.Fatal("a job ID appeared with no job")
	}
	if rec := submission(t, a); rec.State != v1alpha1.SubmissionUnknown || rec.Attempt != 1 {
		t.Fatalf("record = %+v, want attempt 1 still Unknown", rec)
	}
}

func TestInFlightRecordWithoutJobIsResolvedByTag(t *testing.T) {
	e := newEnv(t)
	a := e.newAlloc("a1", testProvider)
	tag := v1alpha1.JobTag(testNS, "a1", "uid-a1")
	// A controller died after its claim and after its sbatch registered a job.
	a.Annotations = map[string]string{
		v1alpha1.AnnotationSubmission: `{"attempt":1,"state":"InFlight","writer":"fake"}`,
	}
	if err := e.c.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, outcome, err := e.batch.Submit(context.Background(), provider.SubmitRequest{
		Tag: tag, Params: *a.Spec.ForProvider.Batch,
	}); outcome != provider.Accepted || err != nil {
		t.Fatalf("setup Submit = %s %v", outcome, err)
	}
	e.sync()
	a = e.alloc("a1")
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" || submission(t, a).State != v1alpha1.SubmissionDone {
		t.Fatalf("annotations = %v, want the job adopted", a.Annotations)
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1 (the setup's): an InFlight record is never answered with a new sbatch", n)
	}
}

func TestRejectedWaitsForBackoffThenResubmits(t *testing.T) {
	e := newEnv(t)
	const reason = "Batch job submission failed: example reason"
	e.batch.Enqueue(
		Script{Outcome: provider.Rejected, Reason: reason},
		Script{Outcome: provider.Rejected, Reason: reason + " again"},
	)
	e.newAlloc("a1", testProvider)

	e.sync() // 12:00:00, attempt 1
	o := e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	rec := submission(t, e.alloc("a1"))
	if rec.State != v1alpha1.SubmissionRejected || rec.Attempt != 1 || rec.Reason != reason {
		t.Fatalf("record = %+v, want attempt 1 Rejected with the scheduler's reason", rec)
	}
	wantMT(t, "record time", rec.Time, at(12, 0, 0))
	if o.Submission == nil || o.Submission.Reason != reason {
		t.Fatalf("submission mirror = %+v", o.Submission)
	}
	if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; ok {
		t.Fatal("a job ID was recorded for a rejection")
	}

	e.syncAt(at(12, 0, 59)) // RejectBackoff is 1m: not yet
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d before the backoff passed, want 1", n)
	}

	e.syncAt(at(12, 1, 0)) // attempt 2, rejected again
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	rec = submission(t, e.alloc("a1"))
	if rec.Attempt != 2 || rec.State != v1alpha1.SubmissionRejected || rec.Reason != reason+" again" {
		t.Fatalf("record = %+v, want attempt 2 Rejected", rec)
	}
	wantMT(t, "record time", rec.Time, at(12, 1, 0))
	e.syncAt(at(12, 1, 30))
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}

	e.syncAt(at(12, 2, 0)) // attempt 3 takes the default script: accepted
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	rec = submission(t, e.alloc("a1"))
	if rec.Attempt != 3 || rec.State != v1alpha1.SubmissionDone {
		t.Fatalf("record = %+v, want attempt 3 Done", rec)
	}
	if n := e.batch.Count("sbatch"); n != 3 {
		t.Fatalf("sbatch count = %d, want 3", n)
	}
}

func TestRejectBackoffFromTheController(t *testing.T) {
	e := newEnv(t)
	e.ctl.RejectBackoff = 10 * time.Second
	e.batch.Enqueue(Script{Outcome: provider.Rejected, Reason: "example"})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 0, 9))
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1", n)
	}
	e.syncAt(at(12, 0, 10))
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}
}

func TestPreCallRefusalIsRejected(t *testing.T) {
	e := newEnv(t)
	a := e.newAlloc("a1", testProvider)
	a.Spec.ForProvider.Batch.GPUs = 9 // above gpusPerNode
	if err := e.c.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	rec := submission(t, e.alloc("a1"))
	if rec.State != v1alpha1.SubmissionRejected || !strings.Contains(rec.Reason, provider.ErrInvalidRequest.Error()) {
		t.Fatalf("record = %+v, want Rejected with the refusal", rec)
	}
	if n := e.batch.Count("sbatch"); n != 0 {
		t.Fatalf("sbatch count = %d, want 0: the request never reached the scheduler", n)
	}
}

func TestRequeueIsFailedWithReasonRequeued(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 10, 0))
	e.wantPhase("a1", v1alpha1.AllocationRunning)

	e.batch.Requeue("998")
	e.syncAt(at(12, 11, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationFailed)
	if o.Reason != v1alpha1.ReasonRequeued || !o.Requeued || o.RestartCount != 1 {
		t.Fatalf("observation = %+v, want reason Requeued, requeued, restart count 1", o)
	}
	e.syncAt(at(12, 12, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationFailed)
	if o.Reason != v1alpha1.ReasonRequeued || !o.Requeued {
		t.Fatalf("a later Sync lost the requeue: %+v", o)
	}
}

func TestRequeuedIsKeptOnceSeen(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync() // running, never requeued
	// An earlier pass (or an earlier controller) saw a requeue; the job does not
	// show one now. "Requeued" and its reason are kept once set (HBEP-4, Phases).
	a := e.alloc("a1")
	a.Status.AtProvider.Requeued = true
	if err := e.c.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	e.syncAt(at(12, 1, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationRunning)
	if !o.Requeued || o.Reason != v1alpha1.ReasonRequeued {
		t.Fatalf("observation = requeued %v reason %q, want both kept", o.Requeued, o.Reason)
	}
}

func TestExternalCancelIsEnded(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 10, 0))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	e.batch.CancelExternally("998")
	e.syncAt(at(12, 11, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationEnded)
	wantMT(t, "endTime", o.EndTime, at(12, 10, 0))
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d: an externally cancelled job is not resubmitted", n)
	}
}

func TestOneBadAllocationDoesNotStopTheOthers(t *testing.T) {
	e := newEnv(t)
	bad := e.newAlloc("a-bad", testProvider)
	bad.Annotations = map[string]string{v1alpha1.AnnotationSubmission: "{not json"}
	if err := e.c.Update(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	e.newAlloc("b-good", testProvider)
	err := e.ctl.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "a-bad") {
		t.Fatalf("Sync = %v, want an error naming a-bad", err)
	}
	e.wantPhase("b-good", v1alpha1.AllocationRunning)
	if e.provider().Status.LastSuccessfulStatusRead == nil {
		t.Fatal("the Provider's status was not written after one Allocation failed")
	}
}

// A refusal for a full submit limit is recorded Rejected with the code
// SubmitLimitReached in the same record, and waits for the backoff like any
// Rejected attempt; a policy refusal for another limit, and a refusal with no
// reason line, carry no code (HBEP-4, QOS limits).
func TestSubmitLimitRefusalIsRecordedWithItsCode(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(
		Script{Outcome: provider.Rejected, Policy: "QOSMaxSubmitJobPerUserLimit"},
		Script{Outcome: provider.Rejected, Policy: "QOSMaxWallDurationPerJobLimit"},
		Script{Outcome: provider.Rejected, Reason: "Batch job submission failed: Invalid account"},
	)
	e.newAlloc("a1", testProvider)
	for i, tc := range []struct {
		at       time.Time
		wantCode string
		wantText string
	}{
		{at(12, 0, 0), v1alpha1.ReasonSubmitLimitReached, "QOSMaxSubmitJobPerUserLimit"},
		{at(12, 1, 0), "", "QOSMaxWallDurationPerJobLimit"},
		{at(12, 2, 0), "", "Invalid account"},
	} {
		e.syncAt(tc.at)
		o := e.wantPhase("a1", v1alpha1.AllocationSubmitting)
		rec := submission(t, e.alloc("a1"))
		if rec.Attempt != int32(i+1) || rec.State != v1alpha1.SubmissionRejected || rec.Code != tc.wantCode || !strings.Contains(rec.Reason, tc.wantText) {
			t.Fatalf("attempt %d: record = %+v, want Rejected with code %q and a reason naming %s", i+1, rec, tc.wantCode, tc.wantText)
		}
		if o.Submission == nil || o.Submission.Code != tc.wantCode {
			t.Fatalf("attempt %d: submission mirror = %+v, want code %q", i+1, o.Submission, tc.wantCode)
		}
	}
	if n := e.batch.Count("sbatch"); n != 3 {
		t.Fatalf("sbatch count = %d, want 3: each refusal waits for the backoff, then the next attempt runs", n)
	}
}

// Each full-slot reason name, and another policy reason, run through the
// submission (Create) and then the passes that observe the backoff (Observe):
// the Allocation settles at once and stays the same on every pass, with no
// write between them, so nothing reading it can see it flap. A full slot shows
// atProvider.reason SubmitLimitReached and the record's code; the other reason
// shows neither (`@hpc-burst-science-agent`, 1 Nov 2026, on the refusal
// contract).
func TestPolicyRefusalSettlesWithoutFlapping(t *testing.T) {
	for _, tc := range []struct {
		policy string
		code   string
	}{
		{"QOSMaxSubmitJobPerUserLimit", v1alpha1.ReasonSubmitLimitReached},
		{"AssocMaxSubmitJobLimit", v1alpha1.ReasonSubmitLimitReached},
		{"MaxSubmitJobsPerAccount", v1alpha1.ReasonSubmitLimitReached},
		{"QOSMaxWallDurationPerJobLimit", ""},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			e := newEnv(t)
			e.batch.Enqueue(Script{Outcome: provider.Rejected, Policy: tc.policy})
			e.newAlloc("a1", testProvider)
			e.syncAt(at(12, 0, 0)) // the submission, refused
			first := e.alloc("a1")
			check := func(a *v1alpha1.Allocation, when string) {
				t.Helper()
				o := a.Status.AtProvider
				rec := submission(t, a)
				if o.Phase != v1alpha1.AllocationSubmitting || o.Reason != tc.code || rec.State != v1alpha1.SubmissionRejected ||
					rec.Code != tc.code || rec.Reason != "sbatch: error: "+tc.policy {
					t.Fatalf("%s: phase %q, reason %q, record %+v; want Submitting, reason %q, a Rejected record with code %q and the reason line",
						when, o.Phase, o.Reason, rec, tc.code, tc.code)
				}
				if o.Submission == nil || o.Submission.Code != tc.code {
					t.Fatalf("%s: submission mirror %+v, want code %q", when, o.Submission, tc.code)
				}
			}
			check(first, "after the submission")
			// Passes inside the 1-minute backoff observe and submit nothing.
			for _, s := range []int{10, 20, 30, 59} {
				e.syncAt(at(12, 0, s))
				a := e.alloc("a1")
				check(a, fmt.Sprintf("pass at 12:00:%02d", s))
				if a.ResourceVersion != first.ResourceVersion {
					t.Fatalf("pass at 12:00:%02d wrote the Allocation (resourceVersion %s, then %s): its state changed between passes",
						s, first.ResourceVersion, a.ResourceVersion)
				}
			}
			if n := e.batch.Count("sbatch"); n != 1 {
				t.Fatalf("sbatch count = %d during the backoff, want 1", n)
			}
		})
	}
}

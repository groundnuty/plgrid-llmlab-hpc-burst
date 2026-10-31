package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Accounting in the fake Controller (HBEP-8, Provider accounting): the live
// status, the one accounting read, and the kept record. A job submitted at
// 12:00:00 with Script.QueueWait 5m starts at 12:05:00. The Allocation asks for
// 1 GPU and 8 CPUs (newAlloc), so a job that has run for s seconds has 8 CPUs, 1
// GPU and s elapsed seconds. Every expected number below is written out from
// those times, never derived from the fields the code reads.

func i32(v int32) *int32 { return &v }
func i64(v int64) *int64 { return &v }

func tp(t time.Time) *time.Time { return &t }

// show prints what p points at, or "unknown" for nil.
func show[T any](p *T) string {
	if p == nil {
		return "unknown"
	}
	return fmt.Sprint(*p)
}

// wantLive checks status.atProvider.accounting of an Allocation whose counts
// and elapsed time are all known.
func (e *env) wantLive(name string, cpus, gpus int32, elapsed int64, read time.Time) {
	e.t.Helper()
	a := e.obs(name).Accounting
	if a == nil {
		e.t.Fatalf("at %s Allocation %s has no accounting, want %d CPUs, %d GPUs, %d s", e.clock.Now().Format("15:04:05"), name, cpus, gpus, elapsed)
	}
	wantCount(e.t, "allocatedCPUs", a.AllocatedCPUs, cpus)
	wantCount(e.t, "allocatedGPUs", a.AllocatedGPUs, gpus)
	wantSecs(e.t, "elapsedSeconds", a.ElapsedSeconds, elapsed)
	wantMT(e.t, "readTime", &a.ReadTime, read)
}

func wantSecs(t *testing.T, what string, got *int64, want int64) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("%s = %s, want %d", what, show(got), want)
	}
}

// recWant is what an accounting record must hold. A nil count, elapsed time or
// time is unknown, and unknown must name exactly those fields, in the order of
// the record's Unknown list.
type recWant struct {
	cpus, gpus *int32
	secs       *int64
	start, end *time.Time
	unknown    []string
	jobIDs     []string
	md         string // the ModelDeployment, "" when the Allocation has none
}

// acct returns the Allocation's accounting record: acct-<UID>, and the test's
// UIDs are "uid-<name>".
func (e *env) acct(name string) (*appsv1.ControllerRevision, bool) {
	e.t.Helper()
	var cr appsv1.ControllerRevision
	err := e.c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: "acct-uid-" + name}, &cr)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return &cr, true
}

func (e *env) wantNoRecord(name string) {
	e.t.Helper()
	if cr, ok := e.acct(name); ok {
		e.t.Fatalf("at %s the accounting record of %s exists: %s", e.clock.Now().Format("15:04:05"), name, cr.Data.Raw)
	}
}

func (e *env) wantRecord(name string, w recWant) {
	e.t.Helper()
	cr, ok := e.acct(name)
	if !ok {
		e.t.Fatalf("at %s there is no accounting record for %s", e.clock.Now().Format("15:04:05"), name)
	}
	if len(cr.OwnerReferences) != 0 {
		e.t.Errorf("the record has owner references %v: garbage collection would remove it", cr.OwnerReferences)
	}
	if cr.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordAccounting || cr.Labels[v1alpha1.LabelProvider] != testProvider {
		e.t.Errorf("record labels = %v, want record=accounting and provider=%s", cr.Labels, testProvider)
	}
	if got := cr.Labels[v1alpha1.LabelModelDeployment]; got != w.md {
		e.t.Errorf("record model-deployment label = %q, want %q", got, w.md)
	}
	if got := cr.Annotations[v1alpha1.AnnotationModelDeployment]; got != w.md {
		e.t.Errorf("record model-deployment annotation = %q, want %q", got, w.md)
	}
	var d v1alpha1.AccountingRecordData
	if err := json.Unmarshal(cr.Data.Raw, &d); err != nil {
		e.t.Fatalf("record data %s: %v", cr.Data.Raw, err)
	}
	if d.Provider != testProvider || d.Partition != gpuPart || d.Basis != "allocated" || d.Tag != v1alpha1.JobTag(testNS, name, types.UID("uid-"+name)) {
		e.t.Errorf("record = provider %q partition %q tag %q basis %q, want %q, %q, the tag of %s, allocated", d.Provider, d.Partition, d.Tag, d.Basis, testProvider, gpuPart, name)
	}
	if !reflect.DeepEqual(d.JobIDs, w.jobIDs) {
		e.t.Errorf("record jobIDs = %v, want %v", d.JobIDs, w.jobIDs)
	}
	if !reflect.DeepEqual(d.Unknown, w.unknown) {
		e.t.Errorf("record unknown = %v, want %v", d.Unknown, w.unknown)
	}
	for _, c := range []struct {
		what      string
		got, want *int32
	}{{"allocatedCPUs", d.AllocatedCPUs, w.cpus}, {"allocatedGPUs", d.AllocatedGPUs, w.gpus}} {
		if (c.got == nil) != (c.want == nil) || (c.got != nil && *c.got != *c.want) {
			e.t.Errorf("record %s = %s, want %s", c.what, show(c.got), show(c.want))
		}
	}
	if (d.ElapsedSeconds == nil) != (w.secs == nil) || (w.secs != nil && *d.ElapsedSeconds != *w.secs) {
		e.t.Errorf("record elapsedSeconds = %s, want %s", show(d.ElapsedSeconds), show(w.secs))
	}
	wantRecTime(e.t, "startTime", d.StartTime, w.start)
	wantRecTime(e.t, "endTime", d.EndTime, w.end)
}

func wantRecTime(t *testing.T, what string, got *metav1.Time, want *time.Time) {
	t.Helper()
	if want == nil {
		wantNilMT(t, "record "+what, got)
		return
	}
	wantMT(t, "record "+what, got, *want)
}

func TestAccountingOfACancelledRunningJob(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: pending, so its hours are unknown, never 0
	if o := e.wantPhase("a1", v1alpha1.AllocationPending); o.Accounting != nil {
		t.Fatalf("a pending Allocation has accounting %+v, want none", o.Accounting)
	}
	e.syncAt(at(12, 3, 0))
	if o := e.obs("a1"); o.Accounting != nil {
		t.Fatalf("a pending Allocation has accounting %+v, want none", o.Accounting)
	}

	e.syncAt(at(12, 5, 0)) // started this second
	e.wantLive("a1", 8, 1, 0, at(12, 5, 0))
	e.syncAt(at(12, 7, 30))
	e.wantLive("a1", 8, 1, 150, at(12, 7, 30))
	e.syncAt(at(12, 10, 0))
	e.wantLive("a1", 8, 1, 300, at(12, 10, 0))

	e.del("a1")
	e.sync() // cancelled at 12:10:00
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(300), start: tp(at(12, 5, 0)), end: tp(at(12, 10, 0)), jobIDs: []string{"998"}})
	if n := e.batch.Count("scancel"); n != 1 {
		t.Errorf("scancel logged %d times, want 1", n)
	}
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Errorf("sacct-job logged %d times, want 1: the record uses the reads the deleting pass made", n)
	}
}

// A job that ends on its own: its final numbers rest on the source of its final
// state. Past the queue listing (MinJobAge 10 s) they come from accounting's
// elapsed time; inside it, from the listing's end minus start. With the counts
// never read (the Allocation was not observed running), accounting still gives
// them when it is the source, and the listing does not.
func TestRecordOfAJobThatEndsOnItsOwn(t *testing.T) {
	failAfter := 12 * time.Minute
	for _, tc := range []struct {
		name     string
		script   Script
		syncs    []time.Time
		deleteAt time.Time
		cpus     *int32
		gpus     *int32
		secs     int64
		end      time.Time
		unknown  []string
		live     int64 // the elapsed seconds of the last live status
	}{
		{"fails after 12m, read after the purge", Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter},
			[]time.Time{at(12, 0, 0), at(12, 5, 0), at(12, 10, 0), at(12, 20, 0)}, at(12, 20, 0),
			i32(8), i32(1), 720, at(12, 17, 0), nil, 720},
		{"time limit, read after the purge", Script{QueueWait: 5 * time.Minute},
			[]time.Time{at(12, 0, 0), at(12, 5, 0), at(12, 36, 0)}, at(12, 36, 0),
			i32(8), i32(1), 1800, at(12, 35, 0), nil, 1800},
		{"fails after 12m, final state from the queue listing", Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter},
			[]time.Time{at(12, 0, 0), at(12, 5, 0), at(12, 17, 5)}, at(12, 17, 5),
			i32(8), i32(1), 720, at(12, 17, 0), nil, 720},
		{"time limit, never seen running, accounting is the source", Script{QueueWait: 5 * time.Minute},
			[]time.Time{at(12, 0, 0)}, at(12, 40, 0),
			i32(8), i32(1), 1800, at(12, 35, 0), nil, -1},
		{"time limit, never seen running, the queue listing is the source", Script{QueueWait: 5 * time.Minute},
			[]time.Time{at(12, 0, 0)}, at(12, 35, 5),
			nil, nil, 1800, at(12, 35, 0), []string{"allocatedCPUs", "allocatedGPUs"}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.batch.Enqueue(tc.script)
			e.newAlloc("a1", testProvider)
			for _, when := range tc.syncs {
				e.syncAt(when)
			}
			if tc.live >= 0 {
				e.wantLive("a1", 8, 1, tc.live, tc.syncs[len(tc.syncs)-1])
			}
			e.clock.Set(tc.deleteAt)
			e.del("a1")
			e.sync()
			if !e.gone("a1") {
				t.Fatal("Allocation survived its deletion")
			}
			e.wantRecord("a1", recWant{cpus: tc.cpus, gpus: tc.gpus, secs: i64(tc.secs), start: tp(at(12, 5, 0)), end: tp(tc.end),
				unknown: tc.unknown, jobIDs: []string{"998"}})
			if n := e.batch.Count("scancel"); n != 0 {
				t.Errorf("scancel logged %d times for an ended job, want 0", n)
			}
		})
	}
}

// A job deleted before it started has no accounting: every hours field of the
// record is unknown and named so, the end time is the cancel's, and the record
// is written at once.
func TestRecordOfAJobThatNeverStarted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script Script
		phase  v1alpha1.AllocationPhase
	}{
		{"pending", Script{QueueWait: 10 * time.Minute}, v1alpha1.AllocationPending},
		{"held", Script{AdminHold: true}, v1alpha1.AllocationHeld},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.batch.Enqueue(tc.script)
			e.newAlloc("a1", testProvider)
			e.sync()
			e.syncAt(at(12, 3, 0))
			if o := e.wantPhase("a1", tc.phase); o.Accounting != nil {
				t.Fatalf("a %s Allocation has accounting %+v, want none", tc.name, o.Accounting)
			}
			e.del("a1")
			e.sync() // one pass: nothing to wait for
			if !e.gone("a1") {
				t.Fatal("Allocation survived its deletion")
			}
			e.wantRecord("a1", recWant{end: tp(at(12, 3, 0)), jobIDs: []string{"998"},
				unknown: []string{"allocatedCPUs", "allocatedGPUs", "elapsedSeconds", "startTime"}})
			if n := e.batch.Count("sacct-job"); n != 0 {
				t.Errorf("sacct-job logged %d times for a job that never started, want 0", n)
			}
		})
	}
}

// A requeue clears the start (HBEP-4, Phases), so the job is pending and has no
// accounting until it runs again; a requeued job read from the queue listing
// has its earlier executions unknown, so its elapsed time is unknown too.
func TestLiveAccountingOfARequeuedJob(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.wantLive("a1", 8, 1, 300, at(12, 10, 0))

	e.batch.Requeue("998") // eligible again at 12:10:00, starts at 12:15:00
	e.syncAt(at(12, 11, 0))
	if o := e.wantPhase("a1", v1alpha1.AllocationFailed); o.Accounting != nil {
		t.Fatalf("a requeued, pending job has accounting %+v, want none", o.Accounting)
	}
	e.syncAt(at(12, 17, 0)) // running again since 12:15:00; the phase of a requeued job stays Failed
	wantMT(t, "startTime", e.wantPhase("a1", v1alpha1.AllocationFailed).StartTime, at(12, 15, 0))
	a := e.obs("a1").Accounting
	if a == nil || a.ElapsedSeconds != nil {
		t.Fatalf("a requeued running job has accounting %+v, want its elapsed time unknown", a)
	}
	wantMT(t, "readTime", &a.ReadTime, at(12, 17, 0))
	// The second execution is read once more for its own allocation (the
	// counts were dropped with the first execution's accounting while it
	// waited), as the real controller does.
	wantCount(t, "allocatedCPUs", a.AllocatedCPUs, 8)
	wantCount(t, "allocatedGPUs", a.AllocatedGPUs, 1)
	if n := e.batch.Count("sacct-job"); n != 2 {
		t.Errorf("%d accounting reads, want 2: one per execution", n)
	}
}

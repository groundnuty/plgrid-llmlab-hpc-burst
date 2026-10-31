package providerctl

// The accounting record, acct-<Allocation UID> (HBEP-8, Provider accounting;
// HBEP-4, Two controllers, Delete): written before the finalizer goes, once,
// from the reads the deleting pass already made, and kept after the Allocation
// is gone. Over the real Batch and the fake Slurm. The record's data is
// compared with golden strings written by hand from the times given to the fake;
// a test never builds its expectation with the code that writes the record.

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// uidOf is the UID newRealEnv gives the Allocation.
func (e *realEnv) uidOf(name string) string { return "uid-" + e.provider + "-" + name }

func (e *realEnv) record(name string) (*appsv1.ControllerRevision, bool) {
	e.t.Helper()
	var r appsv1.ControllerRevision
	err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: "acct-" + e.uidOf(name)}, &r)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return &r, true
}

// wantRecord checks the record's data against a golden string whose tag and job
// ID are filled in, and the shape every record has: the labels, no owner
// reference, the namespace.
func (e *realEnv) wantRecord(name, wantData string) *appsv1.ControllerRevision {
	e.t.Helper()
	r, ok := e.record(name)
	if !ok {
		e.t.Fatalf("no record acct-%s", e.uidOf(name))
	}
	if got := string(r.Data.Raw); got != wantData {
		e.t.Errorf("record data\n got %s\nwant %s", got, wantData)
	}
	if len(r.OwnerReferences) != 0 {
		e.t.Errorf("owner references %v: the record outlives the Allocation, so it has none", r.OwnerReferences)
	}
	wantLabels := map[string]string{"hpcburst.groundnuty.github.io/record": "accounting", "hpcburst.groundnuty.github.io/provider": e.provider}
	if len(r.Labels) != 2 || r.Labels["hpcburst.groundnuty.github.io/record"] != "accounting" || r.Labels["hpcburst.groundnuty.github.io/provider"] != e.provider {
		e.t.Errorf("labels %v, want %v", r.Labels, wantLabels)
	}
	return r
}

func jobTag(e *realEnv, name string) string {
	a := e.get(name)
	return v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
}

func head(e *realEnv, name string, ids ...string) string {
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = `"` + id + `"`
	}
	return fmt.Sprintf(`{"provider":"%s","partition":"%s","tag":"%s","jobIDs":[%s]`, e.provider, gpuPart, jobTag(e, name), strings.Join(q, ","))
}

// A job that ends on its own, then the Allocation is deleted: the record holds
// the final numbers (8 CPUs, 1 GPU, 120 s from 12:01:00 to 12:03:00), and it is
// there after the Allocation is gone.
func TestAccountingRecordOfAJobThatEndedOnItsOwn(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1)
	if _, ok := e.record("alloc-a"); ok {
		t.Fatal("the record exists before the Allocation is deleted")
	}
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":120,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:03:00Z","basis":"allocated"}`)
}

// A job cancelled while running, with the final state read from accounting
// (Helios: the listing drops an ended job at once) and from the queue listing
// (Athena: it lists the ended job until MinJobAge). Started 12:01:00, cancelled
// by the delete at 12:02:30: 90 s either way.
func TestAccountingRecordOfAJobCancelledWhileRunning(t *testing.T) {
	for name, cfg := range map[string]fakeslurm.Config{
		"final state from accounting":  {NotListed: fakeslurm.NotListedHelios},
		"final state from the listing": {NotListed: fakeslurm.NotListedAthena, MinJobAge: 10 * time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, cfg)
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 2)
			id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
			clock.Advance(60 * time.Second)
			e.start("alloc-a") // 12:01:00
			e.poll("alloc-a", 1)
			clock.Set(at(12, 2, 30))
			e.del("alloc-a")
			e.run("alloc-a", 1) // Delete cancels the job at 12:02:30
			if j := slurmJob(t, sl, id); j.State != fakeslurm.Cancelled {
				t.Fatalf("precondition: job %s is %s, want CANCELLED", id, j.State)
			}
			if _, ok := e.record("alloc-a"); ok {
				t.Fatal("the record was written in the pass that cancelled the job")
			}
			head := head(e, "alloc-a", "1001")
			deleteUntilGone(e, sl, "alloc-a", id)
			e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":90,`+
				`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:02:30Z","basis":"allocated"}`)
		})
	}
}

// A job that never started (cancelled while queued): its hours are unknown,
// each field named, and the record is written at once, not after lostGrace.
func TestAccountingRecordOfAJobThatNeverStarted(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(30 * time.Second)
	e.run("alloc-a", 1)
	if a := e.accStatus("alloc-a"); a != nil {
		t.Fatalf("a queued job has accounting %+v, want none", a)
	}
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	e.run("alloc-a", 1) // cancelled at 12:00:30
	clock.Advance(10 * time.Second)
	e.run("alloc-a", 1)
	if !e.gone("alloc-a") {
		t.Fatalf("the Allocation is still there; its job is %s", slurmJob(t, sl, id).State)
	}
	e.wantRecord("alloc-a", head+`,"endTime":"2026-10-30T12:00:30Z","basis":"allocated",`+
		`"unknown":["allocatedCPUs","allocatedGPUs","elapsedSeconds","startTime"]}`)
}

// Accounting unavailable during the delete: the job ended and neither the
// listing nor accounting shows it. The finalizer waits for the final read at
// most lostGrace (the profile's default, 2 minutes), counted from the first pass
// that did not see the job; a controller restarted in between keeps the wait;
// then the record is written with the elapsed and end times unknown, and the
// Allocation goes. What was known before stays: 8 CPUs, 1 GPU, the start.
func TestAccountingRecordWaitsLostGraceForTheFinalRead(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed)
	if err := sl.AcctHide(jobNumber(t, id)); err != nil {
		t.Fatal(err)
	}
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	e.run("alloc-a", 1) // 12:03:00: the first pass that cannot see the job
	since := e.get("alloc-a").Status.AtProvider.AbsentSince
	if since == nil || !since.Time.Equal(at(12, 3, 0)) {
		t.Fatalf("absentSince %v, want 12:03:00", since)
	}
	waiting := func(when time.Time) {
		t.Helper()
		clock.Set(when)
		e.run("alloc-a", 1)
		if e.gone("alloc-a") {
			t.Fatalf("the Allocation is gone at %s, before lostGrace has passed", when.Format("15:04:05"))
		}
		if _, ok := e.record("alloc-a"); ok {
			t.Fatalf("the record is written at %s, before lostGrace has passed", when.Format("15:04:05"))
		}
	}
	waiting(at(12, 4, 0))
	ctl, err := New(e.c, e.c, e.c.Scheme(), e.opts) // a restarted controller, nothing in memory
	if err != nil {
		t.Fatal(err)
	}
	e.ctl = ctl
	waiting(at(12, 5, 0)) // exactly lostGrace after 12:03:00: not yet past it
	clock.Set(at(12, 5, 1))
	e.run("alloc-a", 1)
	if !e.gone("alloc-a") {
		t.Fatal("the Allocation is still there after lostGrace")
	}
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"startTime":"2026-10-30T12:01:00Z","basis":"allocated",`+
		`"unknown":["elapsedSeconds","endTime"]}`)
}

// The record is created if absent and never rewritten: one that exists already
// (a controller's earlier pass whose later step failed, or anything else under
// that name) keeps its data and its resourceVersion.
func TestAccountingRecordIsNeverRewritten(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a")
	e.poll("alloc-a", 2)
	existing := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "acct-" + e.uidOf("alloc-a"), Namespace: testNS},
		Data:       runtime.RawExtension{Raw: []byte(`{"basis":"allocated","marker":"written earlier"}`)},
		Revision:   1,
	}
	if err := e.c.Create(e.ctx, existing); err != nil {
		t.Fatal(err)
	}
	before, _ := e.record("alloc-a")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	after, ok := e.record("alloc-a")
	if !ok {
		t.Fatal("the record is gone")
	}
	if string(after.Data.Raw) != `{"basis":"allocated","marker":"written earlier"}` || after.ResourceVersion != before.ResourceVersion {
		t.Errorf("the record was rewritten: data %s, resourceVersion %s -> %s", after.Data.Raw, before.ResourceVersion, after.ResourceVersion)
	}
}

// An Allocation that never had a job has nothing to account for: no record.
func TestNoAccountingRecordWithoutAJob(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	// No credential, so nothing is submitted (the prerequisite of HBEP-4).
	if err := removeCredential(e, a); err != nil {
		t.Fatal(err)
	}
	e.run("alloc-a", 2)
	if id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]; id != "" {
		t.Fatalf("precondition: job %s was submitted", id)
	}
	e.del("alloc-a")
	e.run("alloc-a", 3)
	if !e.gone("alloc-a") {
		t.Fatal("the Allocation is still there")
	}
	if r, ok := e.record("alloc-a"); ok {
		t.Errorf("an Allocation with no job wrote a record: %s", r.Data.Raw)
	}
}

// Deleting the Allocation also drops the in-memory record of the job's reads.
func TestAccountingReadsAreForgottenWithTheAllocation(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	n := 0
	e.ctl.shared.acctRead.Range(func(_, _ any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("precondition: %d accounting reads remembered, want 1", n)
	}
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
	n = 0
	e.ctl.shared.acctRead.Range(func(_, _ any) bool { n++; return true })
	if n != 0 {
		t.Errorf("%d accounting reads still remembered after the Allocation went", n)
	}
}

// removeCredential deletes the Allocation's credential file.
func removeCredential(e *realEnv, a *v1alpha1.Allocation) error {
	return os.Remove(agentapi.AllocationCredentialPath(e.credDir, string(a.UID)))
}

// A scheme that cannot write the accounting record would fail the finalizer on
// every pass and make the Allocation undeletable: New refuses it at once.
func TestNewRefusesASchemeWithoutControllerRevisions(t *testing.T) {
	e := newEnv(t)
	bare := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(bare); err != nil {
		t.Fatal(err)
	}
	_, err := New(e.c, e.c, bare, e.opts)
	if err == nil || !strings.Contains(err.Error(), "ControllerRevision") {
		t.Errorf("New with a scheme lacking apps/v1 = %v, want an error naming ControllerRevision", err)
	}
}

package providerctl

// The queued-successor mark against the Provider's own controller (issue #195; HBEP-4, Provider
// status, README line 479; HBEP-7, Handover, No-overlap mode, lines 464 to 466 and Test plan
// line 600): a started job whose Allocation carries the mark adds no wait and makes no Provider
// status write; its queueWaitRecorded is set, and once the job ends its observation is final, so
// the job is not read again. The test runs over the real slurm.Batch and the fake Slurm, the
// path of a real Provider, and not over the stand-in controller of the lifecycle tests. The
// marked Allocation's first two submissions are refused for the submit limit, to show that the
// mark records nothing whatever the refusal history; the unmarked control has none. A marked job
// that never starts would prove nothing here, so both start and end.

import (
	"strconv"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestAMarkedJobWithARefusalHistoryAddsNoWaitAndBecomesFinal(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	sl.Script(fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"))
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")

	// The marked successor: two refused attempts, then accepted at 12:02:02.
	e.newAlloc("alloc-marked", 1, markedQueuedSuccessor)
	e.run("alloc-marked", 2) // attempt 1: refused
	e.attempt("alloc-marked")
	e.attempt("alloc-marked") // attempt 3: accepted
	if r, _ := readRecord(e.get("alloc-marked")); r.sub == nil || r.sub.Attempt != 3 || e.get("alloc-marked").Annotations[v1alpha1.AnnotationJobID] == "" {
		t.Fatalf("setup: record %+v, want attempt 3 accepted with a job", r.sub)
	}
	// The unmarked control, with no refusal, is accepted at the same moment.
	e.newAlloc("alloc-control", 1)
	e.run("alloc-control", 2)
	ids := map[string]int{}
	for _, n := range []string{"alloc-marked", "alloc-control"} {
		id, err := strconv.Atoi(e.get(n).Annotations[v1alpha1.AnnotationJobID])
		if err != nil {
			t.Fatalf("setup: %s has no job: %v", n, err)
		}
		ids[n] = id
	}

	// Both pend 43 min and start at 12:45:04; both end at 13:10:00.
	clock.Advance(43 * time.Minute)
	for _, n := range []string{"alloc-marked", "alloc-control"} {
		e.start(n)
		e.poll(n, 1)
	}
	for _, n := range []string{"alloc-marked", "alloc-control"} {
		if err := sl.End(ids[n], fakeslurm.Completed); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"alloc-marked", "alloc-control"} {
		e.poll(n, 3)
		if o := e.get(n).Status.AtProvider; o.Phase != v1alpha1.AllocationEnded || !o.QueueWaitRecorded {
			t.Fatalf("%s: phase %s, queue wait recorded %v; want Ended with the wait settled", n, o.Phase, o.QueueWaitRecorded)
		}
	}

	// The control records its wait, from the fake Slurm's own record of its job; the marked job,
	// which started all the same, has none.
	var control fakeslurm.Job
	for _, j := range sl.Jobs() {
		if j.ID == ids["alloc-control"] {
			control = j
		}
	}
	if control.Start.IsZero() || control.Start.Sub(control.Submit) < 40*time.Minute {
		t.Fatalf("setup: the control's job %+v, want a pend of over 40 min", control)
	}
	waits := e.provStatus().QueueWaits
	if len(waits) != 1 || waits[0].JobID != strconv.Itoa(ids["alloc-control"]) ||
		!waits[0].SubmitTime.Time.Equal(control.Submit) || !waits[0].StartTime.Time.Equal(control.Start) {
		t.Fatalf("the Provider's waits %+v, want the control's job %d alone (submitted %s, started %s) and none for the marked job %d", waits, ids["alloc-control"], control.Submit.Format(time.TimeOnly), control.Start.Format(time.TimeOnly), ids["alloc-marked"])
	}

	// Final: once ended, neither job is read again.
	reads := map[string]int{}
	for n, id := range ids {
		reads[n] = jobReads(sl, strconv.Itoa(id))
	}
	for _, n := range []string{"alloc-marked", "alloc-control"} {
		e.poll(n, 5)
	}
	for n, id := range ids {
		if got := jobReads(sl, strconv.Itoa(id)); got != reads[n] {
			t.Errorf("%s: %d more accounting reads of the ended job %d over 5 polls, want none: its observation is final", n, got-reads[n], id)
		}
	}
}

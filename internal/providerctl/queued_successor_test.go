package providerctl

// A queued-path no-overlap successor records no queue wait (issue #195; HBEP-7,
// Handover, No-overlap mode, README lines 463 to 466 and Test plan lines 599 to 605;
// HBEP-4, Provider status, line 479). Its Allocation carries
// v1alpha1.AnnotationQueuedSuccessor from its creation, because the job pends behind
// its own predecessor's per-user cap and not in the scheduler's queue: a wait recorded
// for it would inflate the Provider's waits for every plan that reads them. The
// Provider's controller (observe.go) skips a marked Allocation. An unmarked Allocation
// on the same Provider (line 604: the primary) records its wait as ever.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func markedQueuedSuccessor(a *v1alpha1.Allocation) {
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[v1alpha1.AnnotationQueuedSuccessor] = "Provider example-provider: maxGpusPerUser 1 of QOS example-now"
}

// Three jobs are submitted at 12:00:00, in this order, with these scripted
// waits: the primary's 3 min (starts 12:03:00), a marked successor's 40 min
// (12:40:00), and an unmarked job's 40 min (12:40:00). The Provider's waits are
// the primary's and the unmarked job's, with the times of the fake Slurm's
// records; the marked successor's job started, and has no entry. The unmarked
// job with the same pend is the control: without it the test would not show
// that the mark is why the successor has no entry.
func TestAMarkedQueuedSuccessorRecordsNoQueueWaitAndAnUnmarkedJobDoes(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{QueueWait: 3 * time.Minute}, fake.Script{QueueWait: 40 * time.Minute}, fake.Script{QueueWait: 40 * time.Minute})
	e.newAlloc("a-primary")
	e.run("a-primary", 4) // submitted and lifted at 12:00:00
	e.newAlloc("b-successor", markedQueuedSuccessor)
	e.run("b-successor", 4)
	e.newAlloc("c-control")
	e.run("c-control", 4)
	if got := e.get("b-successor").Annotations[v1alpha1.AnnotationQueuedSuccessor]; got == "" {
		t.Fatal("setup: the successor was created without its mark")
	}

	// 12:04:00: the primary runs (its 30 min limit ends at 12:33:00); the others pend.
	e.clock.Advance(4 * time.Minute)
	for _, n := range []string{"a-primary", "b-successor", "c-control"} {
		e.run(n, 1)
	}
	if ptPhase(e, "a-primary") != v1alpha1.AllocationRunning || ptPhase(e, "b-successor") != v1alpha1.AllocationPending {
		t.Fatalf("setup: at 12:04:00 the primary is %s and the successor %s, want Running and Pending", ptPhase(e, "a-primary"), ptPhase(e, "b-successor"))
	}
	// 12:41:00: the successor and the unmarked job have started.
	e.clock.Advance(37 * time.Minute)
	for _, n := range []string{"a-primary", "b-successor", "c-control"} {
		e.run(n, 1)
	}
	for n, want := range map[string]v1alpha1.AllocationPhase{"a-primary": v1alpha1.AllocationEnded, "b-successor": v1alpha1.AllocationRunning, "c-control": v1alpha1.AllocationRunning} {
		if ptPhase(e, n) != want {
			t.Fatalf("setup: Allocation %s is %s at 12:41:00, want %s", n, ptPhase(e, n), want)
		}
	}
	jobs := map[string]string{}
	for _, n := range []string{"a-primary", "b-successor", "c-control"} {
		jobs[n] = e.get(n).Status.AtProvider.JobID
	}
	succ, _ := e.batch.Job(jobs["b-successor"])
	if succ.StartTime == nil || !succ.StartTime.Equal(at(12, 40, 0)) || !succ.SubmitTime.Equal(at(12, 0, 0)) {
		t.Fatalf("setup: the successor's job %+v, want submitted 12:00:00 and started 12:40:00 (the 40 min pend)", succ)
	}

	p := ptSync(e)
	got := map[string]v1alpha1.QueueWait{}
	for _, w := range p.Status.QueueWaits {
		got[w.JobID] = w
	}
	if len(got) != 2 || got[jobs["a-primary"]].JobID == "" || got[jobs["c-control"]].JobID == "" {
		t.Fatalf("waits %v, want those of the primary %s and the unmarked job %s only", ptWaitIDs(p), jobs["a-primary"], jobs["c-control"])
	}
	if _, in := got[jobs["b-successor"]]; in {
		t.Errorf("the marked successor's job %s has a recorded wait: %+v", jobs["b-successor"], got[jobs["b-successor"]])
	}
	// The times are the fake's records of those jobs, worked out from the scripts.
	for _, tc := range []struct {
		name  string
		start time.Time
		wait  time.Duration
	}{{"a-primary", at(12, 3, 0), 3 * time.Minute}, {"c-control", at(12, 40, 0), 40 * time.Minute}} {
		w := got[jobs[tc.name]]
		if !w.SubmitTime.Time.Equal(at(12, 0, 0)) || !w.StartTime.Time.Equal(tc.start) || w.Wait() != tc.wait {
			t.Errorf("%s: wait %+v, want 12:00:00 to %s (%v)", tc.name, w, tc.start.Format(time.TimeOnly), tc.wait)
		}
	}
	// The marked job counts as settled, like a recorded one, so it can be final.
	for _, n := range []string{"a-primary", "b-successor", "c-control"} {
		if !e.get(n).Status.AtProvider.QueueWaitRecorded {
			t.Errorf("Allocation %s: queueWaitRecorded is false after its job started", n)
		}
	}

	// Seen again, after a restart and with the jobs ended, the marked job adds nothing.
	e.build()
	e.clock.Advance(2 * time.Hour)
	for _, n := range []string{"a-primary", "b-successor", "c-control"} {
		e.run(n, 3)
	}
	if got := ptWaitIDs(ptSync(e)); len(got) != 2 {
		t.Errorf("waits %v after a restart and the jobs' ends, want still the two", got)
	}
}

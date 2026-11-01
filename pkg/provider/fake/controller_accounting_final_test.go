package fake

import (
	"testing"
	"time"
)

// The kept record's rules, in the order the real controller applies them
// (HBEP-8, Provider accounting): a requeued job, a read with the elapsed time,
// a known final in the stored status, then the wait for the final read. A job
// submitted at 12:00:00 with QueueWait 5m starts at 12:05:00, and with
// FailAfter 10m fails at 12:15:00, after 600 s.

// A requeued Allocation's record has the counts and the elapsed time unknown
// and is written at once, with no wait for a read that could not change that.
func TestRecordOfARequeuedAllocationHasNoHoursAndIsWrittenAtOnce(t *testing.T) {
	// The second execution starts at 12:15:00 (eligible again at the requeue,
	// 12:10:00, plus the 5m wait).
	t.Run("the job is read", func(t *testing.T) {
		e := newEnv(t)
		e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
		e.newAlloc("a1", testProvider)
		e.sync()
		e.syncAt(at(12, 5, 0))
		e.syncAt(at(12, 10, 0))
		e.batch.Requeue("998")
		e.syncAt(at(12, 17, 0)) // running again, the stored status records the requeue
		e.del("a1")
		e.syncAt(at(12, 20, 0)) // cancelled: the job ends at 12:20:00
		if !e.gone("a1") {
			t.Fatal("the Allocation of a requeued job waited for a final read")
		}
		e.wantRecord("a1", recWant{start: tp(at(12, 15, 0)), end: tp(at(12, 20, 0)), jobIDs: []string{"998"},
			unknown: []string{"allocatedCPUs", "allocatedGPUs", "elapsedSeconds"}})
	})
	// The read at delete no longer shows the requeue (one execution, restart
	// count 0), but the stored status recorded it: still no hours.
	t.Run("the read no longer shows the requeue", func(t *testing.T) {
		e := newEnv(t)
		e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
		e.newAlloc("a1", testProvider)
		e.sync()
		e.syncAt(at(12, 5, 0))
		e.syncAt(at(12, 10, 0))
		e.batch.Requeue("998")
		e.syncAt(at(12, 17, 0))
		if !e.obs("a1").Requeued {
			t.Fatal("precondition: the stored status does not record the requeue")
		}
		e.batch.ForgetRequeue("998")
		e.del("a1")
		e.syncAt(at(12, 20, 0))
		if !e.gone("a1") {
			t.Fatal("the Allocation of a requeued job waited for a final read")
		}
		e.wantRecord("a1", recWant{start: tp(at(12, 15, 0)), end: tp(at(12, 20, 0)), jobIDs: []string{"998"},
			unknown: []string{"allocatedCPUs", "allocatedGPUs", "elapsedSeconds"}})
	})
	// The job is in neither source: the start is the stored status's.
	t.Run("no source shows the job", func(t *testing.T) {
		e := newEnv(t)
		e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
		e.newAlloc("a1", testProvider)
		e.sync()
		e.syncAt(at(12, 5, 0))
		e.syncAt(at(12, 10, 0))
		e.batch.Requeue("998")
		e.syncAt(at(12, 17, 0))
		e.batch.Vanish("998")
		e.del("a1")
		e.syncAt(at(12, 20, 0))
		if !e.gone("a1") {
			t.Fatal("the Allocation of a requeued job, read by neither source, waited for a final read")
		}
		e.wantRecord("a1", recWant{start: tp(at(12, 15, 0)), jobIDs: []string{"998"},
			unknown: []string{"allocatedCPUs", "allocatedGPUs", "elapsedSeconds", "endTime"}})
	})
}

// A known final observation in the stored status (the end time and the elapsed
// time, read while the job was still in accounting) is written at once even
// when neither source shows the job at delete: a later absence never erases it.
func TestRecordFromAKnownFinalStatusWhenNeitherSourceShowsTheJob(t *testing.T) {
	e := newEnv(t)
	failAfter := 10 * time.Minute
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.syncAt(at(12, 16, 0)) // ended at 12:15:00, now in accounting alone: 600 s
	e.wantLive("a1", 8, 1, 600, at(12, 16, 0))
	wantMT(t, "endTime", e.obs("a1").EndTime, at(12, 15, 0))

	e.batch.Vanish("998")
	e.del("a1")
	e.syncAt(at(12, 20, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation waited for a final read although its status holds the final")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(600), start: tp(at(12, 5, 0)), end: tp(at(12, 15, 0)), jobIDs: []string{"998"}})
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Errorf("sacct-job logged %d times, want the 1 of the live pass", n)
	}
}

// incompleteEnv returns an Allocation whose job ran 12:05:00 to 12:15:00 and
// whose accounting record lacks time.elapsed, deleted after its last live pass
// at 12:10:00 (300 s, 8 CPUs, 1 GPU stored, no end time). The first deleting
// pass is at 12:20:00, where the job is read from accounting without the elapsed
// time.
func incompleteEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	failAfter := 10 * time.Minute
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.batch.OmitElapsed("998", true)
	e.del("a1")
	return e
}

// A read of the recorded job without the elapsed time is incomplete: the
// Allocation waits LostGrace from the first such pass, writes no record meanwhile,
// and then writes what the read has, with the elapsed time unknown.
func TestRecordWaitsLostGraceForAnIncompleteReadThenIsWrittenUnknown(t *testing.T) {
	e := incompleteEnv(t)
	for _, c := range []struct {
		when   time.Time
		anchor time.Time
	}{{at(12, 20, 0), at(12, 20, 0)}, {at(12, 20, 5), at(12, 20, 0)}, {at(12, 22, 0), at(12, 20, 0)}} { // 12:22:00 is exactly LostGrace on
		e.syncAt(c.when)
		if e.gone("a1") {
			t.Fatalf("at %s the Allocation is gone: it should wait for the final read", c.when.Format("15:04:05"))
		}
		e.wantNoRecord("a1")
		wantMT(t, "absentSince", e.obs("a1").AbsentSince, c.anchor)
		if n := e.batch.Count("remove-allocation-files"); n != 0 {
			t.Fatalf("remove-allocation-files logged %d times while waiting, want 0", n)
		}
	}
	e.restart() // the wait lives in the stored status
	e.syncAt(at(12, 22, 1))
	if !e.gone("a1") {
		t.Fatal("the Allocation survived the grace")
	}
	// The counts and the times are the read's: it shows the end at 12:15:00,
	// which the stored status never had.
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), start: tp(at(12, 5, 0)), end: tp(at(12, 15, 0)),
		jobIDs: []string{"998"}, unknown: []string{"elapsedSeconds"}})
	if n := e.batch.Count("remove-allocation-files"); n != 1 {
		t.Errorf("remove-allocation-files logged %d times, want 1", n)
	}
}

// The complete read comes within the grace: the record has the real elapsed time.
func TestRecordUsesTheCompleteReadWhenItReplacesAnIncompleteOne(t *testing.T) {
	e := incompleteEnv(t)
	e.syncAt(at(12, 20, 0))
	if e.gone("a1") {
		t.Fatal("the Allocation is gone after an incomplete read")
	}
	e.batch.OmitElapsed("998", false) // accounting has the whole record: 600 s
	e.syncAt(at(12, 21, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation survived although the complete read came")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(600), start: tp(at(12, 5, 0)), end: tp(at(12, 15, 0)), jobIDs: []string{"998"}})
}

package fake

import (
	"context"
	"strings"
	"testing"
	"time"
)

// When the accounting read is made (HBEP-8, Provider accounting): judged from
// the stored status alone, never from the Controller's memory. A job submitted
// at 12:00:00 with Script.QueueWait 5m starts at 12:05:00. See
// controller_accounting_test.go for the numbers: 8 CPUs, 1 GPU.

// The status write of the pass that made the read fails, so the store has no
// counts: the next pass reads again, and has them. The read cannot be lost to
// memory of having made it.
func TestAStatusWriteThatFailsAfterTheAccountingReadCostsOneMoreRead(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same Controller"
		if restart {
			name = "restarted Controller"
		}
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			arm := e.failStatusUpdateOnce("*v1alpha1.Allocation")
			e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
			e.newAlloc("a1", testProvider)
			e.sync() // 12:00:00: pending

			arm()
			e.clock.Set(at(12, 5, 0)) // started
			err := e.ctl.Sync(context.Background())
			if err == nil || !strings.Contains(err.Error(), "update status") {
				t.Fatalf("Sync error = %v, want the failed Allocation status write", err)
			}
			if n := e.batch.Count("sacct-job"); n != 1 {
				t.Fatalf("sacct-job logged %d times, want the 1 read of the failed pass", n)
			}
			if a := e.obs("a1").Accounting; a != nil {
				t.Fatalf("setup: accounting %+v reached the store although the write failed", a)
			}

			if restart {
				e.restart()
			}
			e.syncAt(at(12, 5, 30))
			if n := e.batch.Count("sacct-job"); n != 2 {
				t.Fatalf("sacct-job logged %d times, want 2: the store had no counts, so the next pass reads again", n)
			}
			e.wantLive("a1", 8, 1, 30, at(12, 5, 30))
			e.syncAt(at(12, 6, 30))
			e.syncAt(at(12, 7, 30))
			if n := e.batch.Count("sacct-job"); n != 2 {
				t.Fatalf("sacct-job logged %d times, want 2: the counts are stored, no more reads", n)
			}
		})
	}
}

// A read that finds nothing (accounting lags the queue listing) is retried
// once a minute, counted from the stored readTime that every read moves, until
// the first usable one; then never. A restarted Controller keeps the spacing,
// for it lives in the stored status.
func TestAnEmptyAccountingReadIsRetriedOnceAMinuteUntilUsable(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.LagAccounting("998", true)
	reads := func(when time.Time, want int) {
		t.Helper()
		e.syncAt(when)
		if n := e.batch.Count("sacct-job"); n != want {
			t.Fatalf("at %s sacct-job logged %d times, want %d", when.Format("15:04:05"), n, want)
		}
	}

	reads(at(12, 5, 0), 1) // started: the first read finds nothing
	if a := e.obs("a1").Accounting; a == nil || a.AllocatedCPUs != nil || a.AllocatedGPUs != nil {
		t.Fatalf("accounting %+v after an empty read, want the block with the counts unknown", a)
	}
	wantMT(t, "readTime", &e.obs("a1").Accounting.ReadTime, at(12, 5, 0))
	reads(at(12, 5, 30), 1) // 30 s after the stored readTime: not due
	e.restart()
	reads(at(12, 5, 45), 1) // a new Controller value judges from the same status
	reads(at(12, 6, 0), 2)  // 60 s after 12:05:00: due, and the retry moves readTime
	wantMT(t, "readTime", &e.obs("a1").Accounting.ReadTime, at(12, 6, 0))
	reads(at(12, 6, 59), 2)
	reads(at(12, 7, 0), 3)

	e.batch.LagAccounting("998", false) // accounting has caught up
	reads(at(12, 7, 30), 3)             // 30 s after the last empty read
	reads(at(12, 8, 0), 4)              // 60 s after it: usable
	e.wantLive("a1", 8, 1, 180, at(12, 8, 0))

	e.restart()
	reads(at(12, 9, 0), 4)
	reads(at(12, 10, 0), 4)
	reads(at(12, 20, 0), 4)
	e.wantLive("a1", 8, 1, 900, at(12, 20, 0))
}

// A job that accounting already gave (the queue listing no longer showed it) is
// not read again for the counts: the record the pass has carries them.
func TestNoAccountingReadForAJobAccountingAlreadyGave(t *testing.T) {
	e := newEnv(t)
	failAfter := 10 * time.Minute
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 20, 0)) // the job started at 12:05:00 and failed at 12:15:00, unseen
	if n := e.batch.Count("sacct-job"); n != 0 {
		t.Fatalf("sacct-job logged %d times, want 0: accounting's own record has the counts", n)
	}
	e.wantLive("a1", 8, 1, 600, at(12, 20, 0))
}

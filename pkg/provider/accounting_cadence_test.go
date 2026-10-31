package provider

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The status-write cadence (science agent, 31 Oct 2026, on the review of
// PR 118): a running job's stored block is rewritten when a count changes, when
// the job ends, or once it is a minute old; otherwise it is kept, readTime
// included. The job starts at 12:00:00.

func secs(v int64) *int64 { return &v }

// stored is a block read at h:m:s with the given counts (negative: unknown)
// and elapsed seconds.
func stored(cpus, gpus int32, elapsed int64, h, m, s int) *v1alpha1.AllocationAccounting {
	b := &v1alpha1.AllocationAccounting{ElapsedSeconds: secs(elapsed), ReadTime: metav1.NewTime(*ti(h, m, s))}
	if cpus >= 0 {
		b.AllocatedCPUs = n32(cpus)
	}
	if gpus >= 0 {
		b.AllocatedGPUs = n32(gpus)
	}
	return b
}

func readTimeIs(t *testing.T, what string, got *v1alpha1.AllocationAccounting, h, m, s int) {
	t.Helper()
	if got == nil || !got.ReadTime.Time.Equal(*ti(h, m, s)) {
		t.Errorf("%s: block %+v, want readTime %02d:%02d:%02d", what, got, h, m, s)
	}
}

func TestMirrorAccountingKeepsARunningBlockForAMinute(t *testing.T) {
	running := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue}
	prior := &v1alpha1.AllocationObservation{StartTime: &metav1.Time{Time: *ti(12, 0, 0)}, Accounting: stored(4, 1, 40, 12, 0, 40)}

	// 59 s after readTime, same counts: kept as it is.
	got := MirrorAccounting(running, 1, prior, *ti(12, 1, 39))
	readTimeIs(t, "59 s on", got, 12, 0, 40)
	elapsedIs(t, got, 40)

	// 60 s after readTime: written, 100 s at 12:01:40.
	got = MirrorAccounting(running, 1, prior, *ti(12, 1, 40))
	readTimeIs(t, "60 s on", got, 12, 1, 40)
	elapsedIs(t, got, 100)
}

func TestMirrorAccountingWritesACountChangeAtOnce(t *testing.T) {
	// The stored block has no counts (the first accounting read found
	// nothing); 10 s later a read gives 4 CPUs and 1 GPU: written at once.
	prior := &v1alpha1.AllocationObservation{StartTime: &metav1.Time{Time: *ti(12, 0, 0)}, Accounting: stored(-1, -1, 40, 12, 0, 40)}
	j := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue,
		Accounting: &JobAccounting{AllocatedCPUs: n32(4), AllocatedGPUs: n32(1), Executions: 1}}
	got := MirrorAccounting(j, 1, prior, *ti(12, 0, 50))
	readTimeIs(t, "counts known 10 s on", got, 12, 0, 50)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)
	elapsedIs(t, got, 50)

	// Stored 4 CPUs and 1 GPU; 10 s later accounting gives 8 CPUs: written.
	prior.Accounting = stored(4, 1, 40, 12, 0, 40)
	j.Accounting = &JobAccounting{AllocatedCPUs: n32(8), AllocatedGPUs: n32(1), Executions: 1}
	got = MirrorAccounting(j, 1, prior, *ti(12, 0, 50))
	readTimeIs(t, "CPU count changed 10 s on", got, 12, 0, 50)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 8)
}

func TestMirrorAccountingWritesTheEndAtOnce(t *testing.T) {
	prior := &v1alpha1.AllocationObservation{StartTime: &metav1.Time{Time: *ti(12, 0, 0)}, Accounting: stored(4, 1, 40, 12, 0, 40)}
	ended := Job{State: []string{"COMPLETED"}, StartTime: ti(12, 0, 0), EndTime: ti(12, 0, 45), Source: SourceQueue}
	got := MirrorAccounting(ended, 1, prior, *ti(12, 0, 50))
	readTimeIs(t, "ended 10 s on", got, 12, 0, 50)
	elapsedIs(t, got, 45)

	// Read again ten minutes later, nothing changed but the clock: kept.
	prior = &v1alpha1.AllocationObservation{StartTime: prior.StartTime, EndTime: &metav1.Time{Time: *ti(12, 0, 45)}, Accounting: got}
	got = MirrorAccounting(ended, 1, prior, *ti(12, 10, 50))
	readTimeIs(t, "ended, read again", got, 12, 0, 50)
	elapsedIs(t, got, 45)
}

// The kept block is a copy: the caller's stored status is not shared.
func TestMirrorAccountingDoesNotAliasTheStoredBlock(t *testing.T) {
	prior := &v1alpha1.AllocationObservation{StartTime: &metav1.Time{Time: *ti(12, 0, 0)}, Accounting: stored(4, 1, 40, 12, 0, 40)}
	running := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue}
	got := MirrorAccounting(running, 1, prior, *ti(12, 0, 50))
	if got == prior.Accounting {
		t.Fatal("the kept block is the stored one, not a copy")
	}
	*got.ElapsedSeconds = 999
	if *prior.Accounting.ElapsedSeconds != 40 {
		t.Error("changing the kept block changed the stored one")
	}
}

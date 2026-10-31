package provider

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Every expected number is written out by hand from the times below: the job
// starts at 12:00:00 and the clocks are whole seconds, so 12:01:30 is 90 s.

func ti(h, m, s int) *time.Time {
	t := time.Date(2026, 10, 31, h, m, s, 0, time.UTC)
	return &t
}

func n32(v int32) *int32 { return &v }

func dur(s int) *time.Duration { d := time.Duration(s) * time.Second; return &d }

// countIs compares an optional count with a literal; want < 0 means unknown.
func countIs(t *testing.T, what string, got *int32, want int32) {
	t.Helper()
	switch {
	case want < 0 && got != nil:
		t.Errorf("%s = %d, want unknown", what, *got)
	case want >= 0 && (got == nil || *got != want):
		t.Errorf("%s = %v, want %d", what, got, want)
	}
}

func elapsedIs(t *testing.T, got *v1alpha1.AllocationAccounting, want int64) {
	t.Helper()
	switch {
	case want < 0 && got.ElapsedSeconds != nil:
		t.Errorf("elapsedSeconds = %d, want unknown", *got.ElapsedSeconds)
	case want >= 0 && (got.ElapsedSeconds == nil || *got.ElapsedSeconds != want):
		t.Errorf("elapsedSeconds = %v, want %d", got.ElapsedSeconds, want)
	}
}

func TestAccountingOfNotStarted(t *testing.T) {
	// A pending or held job, and one cancelled before it started (it has an end
	// time and no start): unknown, never 0.
	for name, j := range map[string]Job{
		"pending":                {State: []string{"PENDING"}},
		"held":                   {State: []string{"PENDING"}, Hold: HoldUser},
		"cancelled while queued": {State: []string{"CANCELLED"}, EndTime: ti(12, 0, 40), Source: SourceAccounting, Accounting: &JobAccounting{Elapsed: dur(0), Executions: 1}},
	} {
		if got := AccountingOf(j, 1, nil, *ti(12, 1, 0)); got != nil {
			t.Errorf("%s: accounting %+v, want none", name, got)
		}
	}
}

func TestAccountingOfRunning(t *testing.T) {
	j := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue}
	now := time.Date(2026, 10, 31, 12, 1, 30, 700_000_000, time.UTC) // the sub-second part is dropped
	got := AccountingOf(j, 1, nil, now)
	elapsedIs(t, got, 90)
	if !got.ReadTime.Equal(&metav1.Time{Time: *ti(12, 1, 30)}) {
		t.Errorf("readTime %v, want 12:01:30", got.ReadTime)
	}
	// The listing gives no allocation: the counts are unknown until accounting
	// is read once.
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, -1)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, -1)

	// After the one accounting read: 4 CPUs and 1 GPU, as Athena's capture shows.
	j.Accounting = &JobAccounting{AllocatedCPUs: n32(4), AllocatedGPUs: n32(1), Elapsed: dur(5000), Executions: 1}
	got = AccountingOf(j, 1, nil, now)
	elapsedIs(t, got, 90) // a running job's elapsed time runs to readTime, not accounting's
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)

	// Later passes read only the listing: the stored counts stay.
	prior := got
	j.Accounting = nil
	got = AccountingOf(j, 1, prior, now.Add(30*time.Second))
	elapsedIs(t, got, 120)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)
	if prior.ElapsedSeconds == nil || *prior.ElapsedSeconds != 90 {
		t.Error("the stored accounting was changed in place")
	}
}

// A start time after the controller's clock (the two clocks differ) is 0
// seconds, never negative.
func TestAccountingOfClockSkew(t *testing.T) {
	j := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 5), Source: SourceQueue}
	elapsedIs(t, AccountingOf(j, 1, nil, *ti(12, 0, 0)), 0)
}

func TestAccountingOfEndedFromQueue(t *testing.T) {
	// Athena lists an ended job for a while (the real capture: start 1793452614,
	// end 1793452704, 90 s). Here: 12:00:00 to 12:01:30.
	j := Job{State: []string{"COMPLETED"}, StartTime: ti(12, 0, 0), EndTime: ti(12, 1, 30), Source: SourceQueue}
	prior := &v1alpha1.AllocationAccounting{AllocatedCPUs: n32(4), AllocatedGPUs: n32(1), ElapsedSeconds: func() *int64 { v := int64(60); return &v }()}
	got := AccountingOf(j, 1, prior, *ti(12, 5, 0))
	elapsedIs(t, got, 90) // end minus start, not the read time and not the stored value
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)

	// A requeued job whose final state came from the listing: its earlier
	// executions are unknown, so the elapsed time is.
	j.Requeued, j.RestartCount = true, 1
	elapsedIs(t, AccountingOf(j, 1, prior, *ti(12, 5, 0)), -1)

	// An end before the start is not a duration.
	bad := Job{State: []string{"COMPLETED"}, StartTime: ti(12, 1, 30), EndTime: ti(12, 0, 0), Source: SourceQueue}
	elapsedIs(t, AccountingOf(bad, 1, nil, *ti(12, 5, 0)), -1)
}

func TestAccountingOfEndedFromAccounting(t *testing.T) {
	// Accounting's time.elapsed is used, not end minus start: here 91 s against
	// 92 s of wall clock between the two times.
	j := Job{State: []string{"COMPLETED"}, StartTime: ti(12, 0, 0), EndTime: ti(12, 1, 32), Source: SourceAccounting,
		Accounting: &JobAccounting{AllocatedCPUs: n32(4), AllocatedGPUs: n32(1), Elapsed: dur(91), Executions: 1}}
	got := AccountingOf(j, 1, nil, *ti(12, 5, 0))
	elapsedIs(t, got, 91)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)

	// A requeued job: the sum of the two executions, 60 s and 70 s.
	j.Requeued, j.Accounting.Elapsed, j.Accounting.Executions = true, dur(130), 2
	elapsedIs(t, AccountingOf(j, 1, nil, *ti(12, 5, 0)), 130)

	// A record that has no elapsed time, or whose executions are not all there:
	// unknown, and the counts stay known.
	j.Accounting.Elapsed = nil
	got = AccountingOf(j, 1, nil, *ti(12, 5, 0))
	elapsedIs(t, got, -1)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)

	// Accounting read for a running job: the elapsed time runs to the read.
	run := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceAccounting, Accounting: &JobAccounting{Elapsed: dur(30), Executions: 1}}
	elapsedIs(t, AccountingOf(run, 1, nil, *ti(12, 1, 30)), 90)
}

// The GPU count is 0 only for a job that asked for none and whose allocation
// list was read; a list that shows no GPU for a job that asked for one leaves
// the count unknown, so a change in accounting's shape is not 0 GPU-hours.
func TestAccountingOfMissingGPUEntry(t *testing.T) {
	cpuOnly := &JobAccounting{AllocatedCPUs: n32(4), Elapsed: dur(227), Executions: 1} // S7's real CPU job: 4 CPUs, 227 s
	j := Job{State: []string{"COMPLETED"}, StartTime: ti(12, 0, 0), EndTime: ti(12, 3, 47), Source: SourceAccounting, Accounting: cpuOnly}

	got := AccountingOf(j, 0, nil, *ti(13, 0, 0))
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs (none asked for)", got.AllocatedGPUs, 0)
	elapsedIs(t, got, 227)

	got = AccountingOf(j, 1, nil, *ti(13, 0, 0))
	countIs(t, "allocatedGPUs (one asked for, none listed)", got.AllocatedGPUs, -1)
	if _, ok := (*got).GPUHours(); ok {
		t.Error("GPU-hours are known although the GPU count is not")
	}
	if h, ok := (*got).CPUHours(); !ok || h < 0.25222 || h > 0.25223 {
		t.Errorf("CPU-hours = %v, %v; want 4 x 227 / 3600 = 0.252222", h, ok)
	}

	// No accounting read at all, and none asked for: nothing is read, so 0 is
	// not claimed.
	q := Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue}
	got = AccountingOf(q, 0, nil, *ti(12, 1, 0))
	countIs(t, "allocatedGPUs (no read)", got.AllocatedGPUs, -1)

	// Astra B8: a GPU entry is listed but its count cannot be read (null,
	// missing, a negative sentinel): unknown, even for a job that asked for
	// none, never a known 0.
	j.Accounting = &JobAccounting{AllocatedCPUs: n32(4), GPUsListed: true, Elapsed: dur(227), Executions: 1}
	got = AccountingOf(j, 0, nil, *ti(13, 0, 0))
	countIs(t, "allocatedGPUs (listed, unreadable)", got.AllocatedGPUs, -1)
}

// A job whose final read never came keeps the stored counts and has no elapsed
// time; one never seen started has no accounting at all.
func TestAccountingLost(t *testing.T) {
	prior := &v1alpha1.AllocationAccounting{AllocatedCPUs: n32(4), AllocatedGPUs: n32(1), ElapsedSeconds: func() *int64 { v := int64(60); return &v }()}
	got := AccountingLost(ti(12, 0, 0), 1, prior, *ti(12, 9, 0))
	elapsedIs(t, got, -1)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 4)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 1)
	got = AccountingLost(ti(12, 0, 0), 1, nil, *ti(12, 9, 0))
	elapsedIs(t, got, -1)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, -1)
	if AccountingLost(nil, 1, prior, *ti(12, 9, 0)) != nil {
		t.Error("a job never seen started has accounting")
	}
}

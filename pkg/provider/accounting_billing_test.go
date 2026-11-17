package provider

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The scheduler's billing count in the accounting block (beta; HBEP-8, Provider
// accounting, Billing). It is read and kept as the CPU count is, and a change in
// it is a count change for the status-write cadence. The job starts at 12:00:00;
// every expected number is a literal.

func runningJob(a *JobAccounting) Job {
	return Job{State: []string{"RUNNING"}, StartTime: ti(12, 0, 0), Source: SourceQueue, Accounting: a}
}

// A read's billing count reaches the block; a read without one leaves it
// unknown while the CPU and GPU counts stay known.
func TestAccountingOfBillingCount(t *testing.T) {
	got := AccountingOf(runningJob(&JobAccounting{AllocatedCPUs: n32(16), AllocatedGPUs: n32(2), AllocatedBilling: n32(3), Executions: 1}), 2, nil, *ti(12, 30, 0))
	countIs(t, "allocatedBilling", got.AllocatedBilling, 3)
	countIs(t, "allocatedCPUs", got.AllocatedCPUs, 16)
	countIs(t, "allocatedGPUs", got.AllocatedGPUs, 2)

	got = AccountingOf(runningJob(&JobAccounting{AllocatedCPUs: n32(16), AllocatedGPUs: n32(2), Executions: 1}), 2, nil, *ti(12, 30, 0))
	countIs(t, "allocatedBilling of a reply with none", got.AllocatedBilling, -1)
	countIs(t, "allocatedGPUs beside it", got.AllocatedGPUs, 2)

	// A known 0 is a count, not unknown.
	got = AccountingOf(runningJob(&JobAccounting{AllocatedCPUs: n32(1), AllocatedBilling: n32(0), Executions: 1}), 0, nil, *ti(12, 30, 0))
	countIs(t, "allocatedBilling 0", got.AllocatedBilling, 0)
}

// A later read that lacks the entry keeps the stored count, as for the CPUs;
// a read that has one replaces it.
func TestAccountingOfBillingKeepsThePriorCount(t *testing.T) {
	prior := stored(16, 2, 40, 12, 0, 40)
	prior.AllocatedBilling = n32(3)
	got := AccountingOf(runningJob(&JobAccounting{Executions: 1}), 2, prior, *ti(12, 5, 0))
	countIs(t, "allocatedBilling kept", got.AllocatedBilling, 3)
	got = AccountingOf(runningJob(&JobAccounting{AllocatedBilling: n32(5), Executions: 1}), 2, prior, *ti(12, 5, 0))
	countIs(t, "allocatedBilling replaced", got.AllocatedBilling, 5)
}

// A requeued job's billing count is unknown with its other counts, whatever the
// read and the stored block show.
func TestAccountingOfRequeuedJobHasNoBillingCount(t *testing.T) {
	prior := stored(16, 2, 40, 12, 0, 40)
	prior.AllocatedBilling = n32(3)
	for name, j := range map[string]Job{
		"requeue seen":    {Requeued: true},
		"restart count 1": {RestartCount: 1},
		"two executions":  {},
	} {
		j.State, j.StartTime, j.Source = []string{"RUNNING"}, ti(12, 0, 0), SourceAccounting
		j.Accounting = &JobAccounting{AllocatedCPUs: n32(16), AllocatedBilling: n32(3), Executions: 1}
		if name == "two executions" {
			j.Accounting.Executions = 2
		}
		got := AccountingOf(j, 2, prior, *ti(12, 5, 0))
		countIs(t, name+": allocatedBilling", got.AllocatedBilling, -1)
	}
}

// A billing count that changes (or first arrives) inside the minute is written
// at once; an unchanged one keeps the stored block, readTime included.
func TestMirrorAccountingWritesABillingChangeAtOnce(t *testing.T) {
	prior := &v1alpha1.AllocationObservation{StartTime: &metav1.Time{Time: *ti(12, 0, 0)}, Accounting: stored(16, 2, 40, 12, 0, 40)}
	prior.Accounting.AllocatedBilling = n32(3)
	same := runningJob(&JobAccounting{AllocatedCPUs: n32(16), AllocatedGPUs: n32(2), AllocatedBilling: n32(3), Executions: 1})
	got := MirrorAccounting(same, 2, prior, *ti(12, 0, 50), false)
	readTimeIs(t, "billing unchanged, 10 s on", got, 12, 0, 40)

	changed := runningJob(&JobAccounting{AllocatedCPUs: n32(16), AllocatedGPUs: n32(2), AllocatedBilling: n32(4), Executions: 1})
	got = MirrorAccounting(changed, 2, prior, *ti(12, 0, 50), false)
	readTimeIs(t, "billing 3 to 4, 10 s on", got, 12, 0, 50)
	countIs(t, "allocatedBilling", got.AllocatedBilling, 4)

	// The stored block had no billing count and a later read gives one.
	prior.Accounting.AllocatedBilling = nil
	got = MirrorAccounting(same, 2, prior, *ti(12, 0, 50), false)
	readTimeIs(t, "billing first known, 10 s on", got, 12, 0, 50)
	countIs(t, "allocatedBilling", got.AllocatedBilling, 3)
}

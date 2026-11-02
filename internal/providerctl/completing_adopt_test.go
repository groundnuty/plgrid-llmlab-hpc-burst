package providerctl

// Adoption and a completing job (#198's final rule: "Adoption ranks a
// completing job as 'a job that ran'"; HBEP-7 open question 11, Adoption: a
// live job that is not held, then a job that ran, then the lowest job ID). A
// requeued job that is still completing is ["PENDING","COMPLETING"] with a
// restart count of 1 and no end time, so it looks live and unheld; it must not
// rank in the first class, or a lower ID beats a job that is running (Astra B3
// on PR #217). Every case names its candidates by hand.

import (
	"strconv"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// choose over hand-built jobs, sorted by ID as FindByTag returns them. A
// requeued completing job has no end time and no start time: the listing gives
// neither for a PENDING job.
func TestChooseRanksACompletingJobAsOneThatRan(t *testing.T) {
	start, end := at(12, 1, 0), at(12, 2, 30)
	requeuedCG := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"PENDING", "COMPLETING"}, Completing: true, Requeued: true, RestartCount: 1}
	}
	running := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"RUNNING"}, StartTime: &start}
	}
	pending := func(id string) provider.Job { return provider.Job{ID: id, State: []string{"PENDING"}} }
	held := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"PENDING"}, Hold: provider.HoldUser}
	}
	endedCG := func(id string) provider.Job {
		return provider.Job{ID: id, State: []string{"CANCELLED", "COMPLETING"}, Completing: true, StartTime: &start, EndTime: &end}
	}
	for _, tc := range []struct {
		name string
		jobs []provider.Job
		want string
	}{
		{"a running job beats a requeued completing one with a lower ID", []provider.Job{requeuedCG("1001"), running("1002")}, "1002"},
		{"a pending unheld job beats a requeued completing one with a lower ID", []provider.Job{requeuedCG("1001"), pending("1002")}, "1002"},
		{"a requeued completing job is one that ran: it beats a held job with a lower ID", []provider.Job{held("1001"), requeuedCG("1002")}, "1002"},
		{"an ended completing job beats a held job with a lower ID", []provider.Job{held("1001"), endedCG("1002")}, "1002"},
		{"a running job beats an ended completing one with a lower ID", []provider.Job{endedCG("1001"), running("1002")}, "1002"},
		{"two completing jobs: the lowest ID", []provider.Job{endedCG("1001"), requeuedCG("1002")}, "1001"},
		{"a requeued completing job alone is adopted", []provider.Job{requeuedCG("1001")}, "1001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := choose(tc.jobs).ID; got != tc.want {
				t.Errorf("choose picked %s, want %s", got, tc.want)
			}
		})
	}
}

// The same ranking through the controller over the fake Slurm: a lost record,
// a requeued completing job with the lowest ID (the flag held, so its listing
// is ["PENDING","COMPLETING"]) and a running job beside it. The running job is
// adopted, and the completing one is not.
func TestAdoptionPrefersARunningJobToARequeuedCompletingOne(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	tag := tagOf(a)
	low := sl.OutsideJob("user-a", tag, gpuPart, "", 1)
	high := sl.OutsideJob("user-a", tag, gpuPart, "", 1)
	if low != 1001 || high != 1002 {
		t.Fatalf("precondition: job IDs %d and %d, want 1001 and 1002", low, high)
	}
	if err := sl.Start(low); err != nil {
		t.Fatal(err)
	}
	if err := sl.Requeue(low); err != nil {
		t.Fatal(err)
	}
	if err := sl.HoldCompleting(low, true); err != nil {
		t.Fatal(err)
	}
	if err := sl.Start(high); err != nil {
		t.Fatal(err)
	}
	// The precondition: the low job is PENDING with the flag after a requeue,
	// the case under test, and the high job is running.
	if j := slurmJob(t, sl, "1001"); j.State != fakeslurm.Pending || !j.Completing || j.RestartCount != 1 {
		t.Fatalf("precondition: the low job is %s, completing %v, restarts %d; want PENDING, true, 1", j.State, j.Completing, j.RestartCount)
	}
	if j := slurmJob(t, sl, "1002"); j.State != fakeslurm.Running {
		t.Fatalf("precondition: the high job is %s, want RUNNING", j.State)
	}
	clock.Advance(10 * time.Second)
	e.run("alloc-a", 3)
	if got := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]; got != strconv.Itoa(high) {
		t.Errorf("adopted job %q, want the running job %d, not the lower ID %d that is completing", got, high, low)
	}
}

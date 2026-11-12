package fake_test

// Several workers' checks (HBEP-7, Several workers; Q6): each worker's DesiredWorker.Checks run against
// that worker's own process, gated on it alone, and land in its own WorkerReport; the Instance's own
// checks stay absent when none are asked. One worker's restart drops only its own results.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// workerReportOf is the named worker's report in the last Instance report.
func workerReportOf(t *testing.T, r *rig, name string) agentapi.WorkerReport {
	t.Helper()
	for _, w := range r.lastInstance().Workers {
		if w.Name == name {
			return w
		}
	}
	t.Fatalf("no report of %s", name)
	return agentapi.WorkerReport{}
}

func twoWorkersVerified(gen0, gen1 int64) func(*agentapi.DesiredInstance) {
	return func(i *agentapi.DesiredInstance) {
		withWorkers(gen0, gen1)(i)
		for n := range i.Workers {
			i.Workers[n].Checks = &agentapi.Checks{VerificationGeneration: 1 + i.Workers[n].Generation, Verify: &agentapi.VerifyRequest{}}
		}
	}
}

func TestEachWorkersChecksAreInItsOwnReport(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(twoWorkersVerified(0, 0))))
	r.steps(2) // the workers start, then run and are checked
	for _, name := range []string{"worker-0", "worker-1"} {
		w := workerReportOf(t, r, name)
		if w.Checks == nil || w.Checks.Verify == nil || !w.Checks.Verify.Done || w.Checks.VerificationGeneration != 1 {
			t.Errorf("%s's checks: %+v, want its verification of generation 1", name, w.Checks)
		}
	}
	if c := r.lastInstance().Checks; c != nil {
		t.Errorf("the Instance's own checks: %+v, want none: only the workers' were asked", c)
	}

	// worker-1 restarts: its new process has no result until checked anew at its new generation, and
	// worker-0's result stays.
	r.set(run(instance(twoWorkersVerified(0, 1))))
	r.steps(1)
	if w := workerReportOf(t, r, "worker-0"); w.Checks == nil || w.Checks.Verify == nil {
		t.Errorf("worker-0's checks after worker-1's restart: %+v, want its verification kept", w.Checks)
	}
	r.steps(2)
	if w := workerReportOf(t, r, "worker-1"); w.Generation != 1 || w.Checks == nil || w.Checks.VerificationGeneration != 2 || w.Checks.Verify == nil {
		t.Errorf("worker-1 after its restart: generation %d, checks %+v; want generation 1 checked at verification generation 2", w.Generation, w.Checks)
	}
}

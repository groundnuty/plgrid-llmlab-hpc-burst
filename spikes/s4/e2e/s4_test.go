package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// sbatchDelay keeps each sbatch in flight long enough for the other
// reconciler to observe in the meantime. Without an overlap the scenario
// proves nothing.
const sbatchDelay = "FAKE_SLURM_SBATCH_DELAY_BEFORE=2"

// twoReconcilers starts r1 and r2 on one namespace with the same flags,
// waits until both run workers, and creates one Allocation.
func twoReconcilers(t *testing.T, args []string) (*fakeSlurm, [2]*proc, string) {
	t.Helper()
	ns := newNamespace(t)
	f := newFakeSlurm(t, sbatchDelay)
	var ps [2]*proc
	for i, name := range []string{"r1", "r2"} {
		ps[i] = startProvider(t, ns, f, provCfg{name: name, args: args})
	}
	for _, p := range ps {
		p.waitLog(t, "Starting workers", 20*time.Second)
	}
	createAllocation(t, ns, "a1")
	return f, ps, ns
}

// overlap checks that the reconciler which did not submit made a squeue call
// while the other's sbatch was in flight.
func overlap(t *testing.T, f *fakeSlurm) (submitter, other string, n int) {
	t.Helper()
	starts := f.sbatch(t, "sbatch_start", "")
	subs := f.sbatch(t, "sbatch_submitted", "")
	if len(starts) == 0 || len(subs) == 0 {
		t.Fatalf("no sbatch in the log")
	}
	submitter = starts[0].Caller
	other = map[string]string{"r1": "r2", "r2": "r1"}[submitter]
	for _, e := range f.entries(t) {
		if e.Event == "squeue" && e.Caller == other && e.TS.After(starts[0].TS) && e.TS.Before(subs[0].TS) {
			n++
		}
	}
	return submitter, other, n
}

// Scenario 4 (design): two reconcilers on one Allocation, no leader
// election. The in-flight guard keeps the second from submitting while the
// first's sbatch runs.
func TestS4TwoReconcilersInflightGuard(t *testing.T) {
	f, _, _ := twoReconcilers(t, nil)
	eventually(t, 20*time.Second, func() (bool, string) {
		return len(f.sbatch(t, "sbatch_submitted", "")) >= 1, "no submission"
	})
	time.Sleep(6 * time.Second)
	assertSubmissions(t, f, "", 1)
	sub, other, n := overlap(t, f)
	if n == 0 {
		t.Errorf("%s made no squeue call while %s's sbatch was in flight: the race was not exercised", other, sub)
	}
	t.Logf("%s submitted; %s observed %d times during that sbatch and did not submit", sub, other, n)
}

// Scenario 4, crossplane-runtime's own guard: with
// WithDeterministicExternalName(false) the second reconciler sees an
// incomplete create and stops before Observe. One submission, at the cost of
// the scenario-2 stall.
func TestS4TwoReconcilersCrossplaneDefault(t *testing.T) {
	f, ps, _ := twoReconcilers(t, []string{"--deterministic-external-name=false", "--guard-inflight=false"})
	eventually(t, 20*time.Second, func() (bool, string) {
		return len(f.sbatch(t, "sbatch_submitted", "")) >= 1, "no submission"
	})
	time.Sleep(6 * time.Second)
	assertSubmissions(t, f, "", 1)
	sub := f.sbatch(t, "sbatch_start", "")[0].Caller
	for _, p := range ps {
		if p.name != sub && !p.logContains(t, "cannot determine creation result") {
			t.Errorf("%s never hit the create-incomplete guard: the race was not exercised", p.name)
		}
	}
}

// Control for scenario 4: deterministic external name without the in-flight
// guard. The second reconciler observes during the first's sbatch, finds
// nothing, wins its own create-pending update and submits again.
func TestS4ControlNoGuardDuplicates(t *testing.T) {
	f, _, _ := twoReconcilers(t, []string{"--guard-inflight=false"})
	eventually(t, 20*time.Second, func() (bool, string) {
		n := len(f.sbatch(t, "sbatch_submitted", ""))
		return n >= 2, fmt.Sprintf("%d submissions", n)
	})
	callers := map[string]bool{}
	for _, e := range f.sbatch(t, "sbatch_start", "") {
		callers[e.Caller] = true
	}
	t.Logf("without the guard: %d submissions from %v", len(f.sbatch(t, "sbatch_submitted", "")), callers)
}

// Scenario 4 with the provider's Lease (HBEP-7 open question 11): only the
// leader reconciles. The leader is killed mid-submit (after sbatch returned,
// before anything was persisted); the follower takes the Lease after the
// lease duration, finds the job by tag and does not submit.
func TestS4LeaderElection(t *testing.T) {
	args := []string{"--leader-elect", "--lease-duration=6s", "--renew-deadline=4s", "--retry-period=1s"}
	ns := newNamespace(t)
	f := newFakeSlurm(t, sbatchDelay)
	mpath := filepath.Join(f.dir, "after-sbatch.marker")
	r1 := startProvider(t, ns, f, provCfg{name: "r1", args: args, env: []string{"S4_FAULT_AFTER_SBATCH=" + mpath}})
	r1.waitLog(t, "Successfully acquired lease", 20*time.Second)
	r2 := startProvider(t, ns, f, provCfg{name: "r2", args: args})
	time.Sleep(3 * time.Second) // r2 is up and waiting for the lease
	key := createAllocation(t, ns, "a1")
	m := waitMarker(t, mpath, 20*time.Second)
	if n := f.calls(t, "squeue", "r2", time.Time{}) + f.calls(t, "sbatch_start", "r2", time.Time{}); n != 0 {
		t.Errorf("follower r2 made %d Slurm calls while r1 led", n)
	}
	if r2.logContains(t, "Successfully acquired lease") {
		t.Fatalf("r2 acquired the lease while r1 was alive")
	}
	r1.kill(t)
	killed := time.Now()
	assertKilledBeforePersist(t, key, m.JobID)
	r2.waitLog(t, "Successfully acquired lease", 20*time.Second)
	waitAlloc(t, key, 15*time.Second, "new leader found the job", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == m.JobID })
	took := time.Since(killed)
	time.Sleep(4 * time.Second)
	assertSubmissions(t, f, "", 1)
	t.Logf("r2 took over and found job %d %s after r1 was killed mid-submit; one submission", m.JobID, took.Round(time.Millisecond))
}

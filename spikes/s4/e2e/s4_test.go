package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// sbatchDelay keeps each sbatch in flight long enough for the other
// reconciler to observe in the meantime (crossplane-default and Lease tests).
const sbatchDelay = "FAKE_SLURM_SBATCH_DELAY_BEFORE=2"

// twoGated starts r1 and r2 with the same flags against a fake whose sbatch
// waits at a gate before registering, creates one Allocation, and returns
// once the first sbatch is held at the gate.
func twoGated(t *testing.T, args []string) (*fakeSlurm, gate, map[string]*proc, client.ObjectKey) {
	t.Helper()
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	g := newGate(f)
	f.env = append(f.env, g.env())
	ps := map[string]*proc{}
	for _, name := range []string{"r1", "r2"} {
		ps[name] = startProvider(t, ns, f, provCfg{name: name, args: args})
	}
	for _, p := range ps {
		p.waitLog(t, "Starting workers", 20*time.Second)
	}
	key := createAllocation(t, ns, "a1")
	f.waitEvents(t, "sbatch_gate_wait", 1, 20*time.Second)
	return f, g, ps, key
}

var peer = map[string]string{"r1": "r2", "r2": "r1"}

// Scenario 4 (design): two reconcilers on one Allocation, no leader
// election. The submitter is held at the gate before its job registers. The
// other must read the persisted claim and take the in-flight branch (its log
// names the attempt and the claimant), without spinning, and never submit.
func TestS4TwoReconcilersInflightGuard(t *testing.T) {
	f, g, ps, key := twoGated(t, gatedArgs)
	sub := f.events(t, "sbatch_gate_wait")[0].Caller
	other := peer[sub]
	guard := fmt.Sprintf("in-flight guard: submission attempt 1 by %s is unresolved", sub)
	ps[other].waitLog(t, guard, 10*time.Second)
	from := time.Now()
	time.Sleep(5 * time.Second)
	to := time.Now()
	sq, sa := f.callRate(t, "squeue", other, from, to), f.callRate(t, "sacct", other, from, to)
	if sq > 2 || sa > 2 {
		t.Errorf("%s polled squeue %.1f/s and sacct %.1f/s while waiting; want at most 2/s each (no hot loop)", other, sq, sa)
	}
	if n := len(f.events(t, "sbatch_start")); n != 1 {
		t.Errorf("%d sbatch calls while %s's sbatch was held at the gate; want 1", n, sub)
	}
	g.open(t)
	f.waitEvents(t, "sbatch_submitted", 1, 10*time.Second)
	a := waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, "", 1)
	assertRunsOnce(t, f, externalName(a), 2*time.Second)
	t.Logf("%s submitted; %s logged %q and polled squeue %.1f/s, sacct %.1f/s while waiting", sub, other, guard, sq, sa)
}

// Scenario 4, crossplane-runtime's own guard: with
// WithDeterministicExternalName(false) the second reconciler sees an
// incomplete create and stops before Observe. One submission, at the cost of
// the scenario-2 stall.
func TestS4TwoReconcilersCrossplaneDefault(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t, sbatchDelay)
	var ps []*proc
	for _, name := range []string{"r1", "r2"} {
		ps = append(ps, startProvider(t, ns, f, provCfg{name: name, args: []string{"--deterministic-external-name=false", "--guard-inflight=false"}}))
	}
	for _, p := range ps {
		p.waitLog(t, "Starting workers", 20*time.Second)
	}
	createAllocation(t, ns, "a1")
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

// Control for scenario 4: the in-flight guard off, under the same gate. Both
// reconcilers submit (two distinct callers). In hold mode only one job is
// ever released and the other is cancelled while held: the hold layer alone
// keeps one job running. In fallback mode both jobs are live: the duplicate
// the guard prevents, found by tag and reported.
func TestS4ControlNoGuard(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			f, key, tag := noGuardDuplicate(t, m.args)
			callers := map[string]bool{}
			for _, e := range f.events(t, "sbatch_start") {
				callers[e.Caller] = true
			}
			if len(callers) != 2 {
				t.Errorf("submitting callers %v, want r1 and r2", callers)
			}
			subs := f.events(t, "sbatch_submitted")
			older, newer := min(subs[0].JobID, subs[1].JobID), max(subs[0].JobID, subs[1].JobID)
			if m.name == "hold" {
				assertRunsOnce(t, f, tag, 0)
				return
			}
			waitAlloc(t, key, 10*time.Second, "duplicate reported", func(a *v1alpha1.Allocation) bool {
				o := a.Status.AtProvider
				return o.JobID == older && len(o.Duplicates) == 1 && o.Duplicates[0] == newer
			})
			if live := f.liveJobs(t, tag); len(live) != 2 {
				t.Errorf("fallback without the guard: %d live jobs, want the 2 the control produces", len(live))
			}
		})
	}
}

// noGuardDuplicate makes both reconcilers submit (in-flight guard off): the
// first sbatch waits at the gate until the second has started too.
func noGuardDuplicate(t *testing.T, mode []string) (*fakeSlurm, client.ObjectKey, string) {
	t.Helper()
	f, g, _, key := twoGated(t, withArgs(gatedArgs, append([]string{"--guard-inflight=false"}, mode...)...))
	f.waitEvents(t, "sbatch_gate_wait", 2, 15*time.Second)
	g.open(t)
	f.waitEvents(t, "sbatch_submitted", 2, 10*time.Second)
	a := waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	time.Sleep(3 * time.Second)
	return f, key, externalName(a)
}

// Scenario 4 with the provider's Lease (HBEP-7 open question 11): only the
// leader reconciles. The leader is killed mid-submit (after sbatch returned,
// before anything was persisted); the follower takes the Lease after the
// lease duration, finds the held job by tag, releases it and does not submit.
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
	assertRunsOnce(t, f, externalName(getAlloc(t, key)), 0)
	t.Logf("r2 took over and found job %d %s after r1 was killed mid-submit; one submission", m.JobID, took.Round(time.Millisecond))
}

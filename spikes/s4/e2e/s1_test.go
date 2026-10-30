package e2e

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// runToPurge submits one Allocation, runs its job to COMPLETED in the fake,
// and waits until the fake's squeue has forgotten it. It returns the
// Allocation key, its tag, its job ID and the purge time.
func runToPurge(t *testing.T, ns string, f *fakeSlurm) (client.ObjectKey, string, int64, time.Time) {
	t.Helper()
	key := createAllocation(t, ns, "a1")
	eventually(t, 20*time.Second, func() (bool, string) { return len(f.sbatch(t, "sbatch_submitted", "")) == 1, "no submission" })
	id := f.sbatch(t, "sbatch_submitted", "")[0].JobID // expected job ID from the fake, not the provider
	a := waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == id })
	tag := externalName(a)
	waitReleased(t, f, id)
	f.ctl(t, "start", fmt.Sprint(id))
	waitAlloc(t, key, 10*time.Second, "running", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseRunning })
	f.ctl(t, "end", fmt.Sprint(id), "COMPLETED")
	ended := time.Now()
	// Precondition: the fake forgets the job from squeue after MinJobAge,
	// not before. A fake that never forgets cannot produce the bug.
	if ids := f.squeueIDs(t, tag); len(ids) != 1 {
		t.Fatalf("precondition: squeue should still list the ended job, got %v", ids)
	}
	eventually(t, minJobAge+5*time.Second, func() (bool, string) {
		return !slices.Contains(f.squeueIDs(t, tag), id), "squeue still lists the job"
	})
	purged := time.Now()
	if purged.Sub(ended) < minJobAge-time.Second {
		t.Fatalf("precondition: purged after %s, before MinJobAge %s", purged.Sub(ended), minJobAge)
	}
	t.Logf("job %d tag %s ended, purged from squeue after %s", id, tag, purged.Sub(ended).Round(time.Millisecond))
	return key, tag, id, purged
}

// Scenario 1: a job runs, ends, and stays gone from squeue past MinJobAge,
// with no resubmission. Observe falls back to sacct and reports the ended job
// as existing and terminal.
func TestS1EndedJobNotResubmitted(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1"})
	key, tag, id, purged := runToPurge(t, ns, f)

	time.Sleep(8 * time.Second) // eight poll intervals past the purge
	a := getAlloc(t, key)
	assertSubmissions(t, f, "", 1)
	assertRunsOnce(t, f, tag, 2*time.Second)
	if got := a.Status.AtProvider; got.JobID != id || got.Phase != v1alpha1.PhaseSucceeded || got.Source != "sacct" {
		t.Errorf("status after purge: got job %d phase %q source %q, want job %d phase Succeeded source sacct", got.JobID, got.Phase, got.Source, id)
	}
	// Observe really ran in the dangerous window: squeue (empty) and sacct
	// calls after the purge.
	sq, sa := f.calls(t, "squeue", "r1", purged), f.calls(t, "sacct", "r1", purged)
	if sq < 3 || sa < 3 {
		t.Errorf("observe calls after purge: squeue %d, sacct %d, want >= 3 each", sq, sa)
	}
	t.Logf("after purge: %d squeue and %d sacct calls, %d sbatch (tag %s)", sq, sa, len(f.sbatch(t, "sbatch_start", "")), tag)
}

// Control for scenario 1: an Observe that reads only squeue resubmits the
// ended job once MinJobAge has passed. This shows the fake can produce the
// bug the issue describes.
func TestS1ControlSqueueOnlyResubmits(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1", args: []string{"--observe=squeue-only", "--guard-succeeded=false", "--guard-inflight=false", "--hold-release=false"}})
	runToPurge(t, ns, f)
	eventually(t, 15*time.Second, func() (bool, string) {
		n := len(f.sbatch(t, "sbatch_submitted", ""))
		return n >= 2, fmt.Sprintf("%d submissions", n)
	})
	t.Logf("squeue-only Observe without guards resubmitted: %d submissions", len(f.sbatch(t, "sbatch_submitted", "")))
}

// One-rule control for scenario 1: squeue-only Observe, guards on. The
// succeeded guard alone prevents the resubmission, but the Allocation
// reports Lost (after lost_grace) instead of the job's real end state:
// sacct is what tells.
func TestS1SqueueOnlyWithGuardsReportsLost(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1", args: []string{"--observe=squeue-only"}})
	key, _, id, _ := runToPurge(t, ns, f)
	a := waitAlloc(t, key, lostGrace+10*time.Second, "reported Lost", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseLost })
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, "", 1)
	t.Logf("job %d (COMPLETED in the fake) reported as %s; one submission", id, a.Status.AtProvider.Phase)
}

// waitReleased waits until the fake shows the job not held (released by the
// provider in hold mode; never held in fallback mode). A held job cannot be
// started.
func waitReleased(t *testing.T, f *fakeSlurm, id int64) {
	t.Helper()
	eventually(t, 10*time.Second, func() (bool, string) {
		for _, j := range f.jobs(t, "") {
			if j.ID == id {
				return !j.Held, fmt.Sprintf("job %d still held", id)
			}
		}
		return false, fmt.Sprintf("job %d not in the fake", id)
	})
}

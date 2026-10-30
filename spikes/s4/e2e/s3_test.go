package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

// Scenario 3: the submit uses --no-requeue, and a job the fake requeues
// anyway is reported as failed. The verdict survives the job's purge from
// squeue, including on data_parser v0.0.40, whose sacct output has no
// restart_cnt.
func TestS3RequeueReportedFailed(t *testing.T) {
	for _, parser := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(parser, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t, "FAKE_SLURM_DATA_PARSER="+parser)
			startProvider(t, ns, f, provCfg{name: "r1"})
			key := createAllocation(t, ns, "a1")
			eventually(t, 20*time.Second, func() (bool, string) { return len(f.sbatch(t, "sbatch_submitted", "")) == 1, "no submission" })
			id := f.sbatch(t, "sbatch_submitted", "")[0].JobID
			a := waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == id })
			tag := externalName(a)

			starts := f.sbatch(t, "sbatch_start", "")
			if len(starts) != 1 || !slices.Contains(starts[0].Argv, "--no-requeue") {
				t.Fatalf("want one sbatch with --no-requeue, got %d calls, argv %v", len(starts), starts)
			}
			f.ctl(t, "start", fmt.Sprint(id))
			waitAlloc(t, key, 10*time.Second, "running", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseRunning })
			f.ctl(t, "requeue", fmt.Sprint(id))
			waitAlloc(t, key, 10*time.Second, "requeue reported as failed", func(a *v1alpha1.Allocation) bool {
				o := a.Status.AtProvider
				return o.Phase == v1alpha1.PhaseFailed && o.Requeued && strings.HasPrefix(o.Reason, "Requeued")
			})

			// The requeued execution runs and ends; after the purge only sacct
			// knows the job.
			f.ctl(t, "start", fmt.Sprint(id))
			f.ctl(t, "end", fmt.Sprint(id), "COMPLETED")
			eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
			time.Sleep(4 * time.Second)
			a = getAlloc(t, key)
			if o := a.Status.AtProvider; o.Phase != v1alpha1.PhaseFailed || !o.Requeued || o.Source != "sacct" || o.JobID != id {
				t.Errorf("after purge: got phase %q requeued %v source %q job %d; want Failed, true, sacct, %d", o.Phase, o.Requeued, o.Source, o.JobID, id)
			}
			assertSubmissions(t, f, "", 1)

			// Deleting the Allocation of an ended job releases its finalizer
			// without scancel.
			if err := h.k8s.Delete(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			eventually(t, 15*time.Second, func() (bool, string) { return allocGone(t, key), "Allocation still present" })
			if n := f.calls(t, "scancel", "", time.Time{}); n != 0 {
				t.Errorf("scancel calls for an ended job: %d, want 0", n)
			}
		})
	}
}

// Deleting the Allocation of a pending job cancels it once, then releases
// the finalizer (HBEP-11: the provider job is cancelled before the object
// disappears).
func TestDeletePendingCancels(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1"})
	key := createAllocation(t, ns, "a1")
	a := waitAlloc(t, key, 20*time.Second, "submitted", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	if err := h.k8s.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, func() (bool, string) { return allocGone(t, key), "Allocation still present" })
	if n := f.calls(t, "scancel", "", time.Time{}); n != 1 {
		t.Errorf("scancel calls: %d, want 1", n)
	}
	assertSubmissions(t, f, "", 1)
}

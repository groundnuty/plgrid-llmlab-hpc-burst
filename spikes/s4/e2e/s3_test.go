package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
)

var parsers = []string{"v0.0.42", "v0.0.40"}

func requeuedFailed(a *v1alpha1.Allocation) bool {
	o := a.Status.AtProvider
	return o.Phase == v1alpha1.PhaseFailed && o.Requeued && strings.HasPrefix(o.Reason, "Requeued")
}

// submitRunning creates an Allocation, waits for its job to be submitted,
// released and observed, and starts it in the fake.
func submitRunning(t *testing.T, ns string, f *fakeSlurm) (key2 string, id int64, tag string) {
	t.Helper()
	key := createAllocation(t, ns, "a1")
	eventually(t, 20*time.Second, func() (bool, string) { return len(f.sbatch(t, "sbatch_submitted", "")) == 1, "no submission" })
	id = f.sbatch(t, "sbatch_submitted", "")[0].JobID
	a := waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == id })
	waitReleased(t, f, id)
	f.ctl(t, "start", fmt.Sprint(id))
	waitAlloc(t, key, 10*time.Second, "running", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseRunning })
	return key.Name, id, externalName(a)
}

// squeueJob decodes the fake's squeue entry for one job, independently of
// the provider's parser.
func (f *fakeSlurm) squeueJob(t *testing.T, tag string, id int64) (state []string, restarts int) {
	t.Helper()
	cmd := exec.Command(filepath.Join(h.slurmBin, "squeue"), "--me", "--json", "--name="+tag)
	cmd.Env = f.cmdEnv("test")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Jobs []struct {
			ID       int64    `json:"job_id"`
			State    []string `json:"job_state"`
			Restarts int      `json:"restart_cnt"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatal(err)
	}
	for _, j := range r.Jobs {
		if j.ID == id {
			return j.State, j.Restarts
		}
	}
	t.Fatalf("job %d not in squeue", id)
	return nil, 0
}

// sacctEvidence decodes the fake's sacct records for one job: the number of
// records, whether any carries REQUEUED, and the highest restart_cnt.
func (f *fakeSlurm) sacctEvidence(t *testing.T, tag string, id int64) (records int, flagged bool, restarts int) {
	t.Helper()
	cmd := exec.Command(filepath.Join(h.slurmBin, "sacct"), "--json", "-X", "-D", "--name="+tag, "-S", "now-3600", "-E", "now")
	cmd.Env = f.cmdEnv("test")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Jobs []struct {
			ID    int64 `json:"job_id"`
			State struct {
				Current []string `json:"current"`
			} `json:"state"`
			Restarts *int `json:"restart_cnt"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatal(err)
	}
	for _, j := range r.Jobs {
		if j.ID != id {
			continue
		}
		records++
		flagged = flagged || slices.Contains(j.State.Current, "REQUEUED")
		if j.Restarts != nil && *j.Restarts > restarts {
			restarts = *j.Restarts
		}
	}
	return records, flagged, restarts
}

// Scenario 3: the submit uses --no-requeue, and a job the fake requeues
// anyway is reported as failed. Real Slurm shows a requeued job in squeue as
// PENDING with restart_cnt raised and no REQUEUED flag, so detection here is
// count-only. The verdict survives the purge from squeue, and it survives an
// accounting purge that leaves no requeue evidence at all (sticky status).
func TestS3RequeueReportedFailed(t *testing.T) {
	for _, parser := range parsers {
		t.Run(parser, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t, "FAKE_SLURM_DATA_PARSER="+parser)
			startProvider(t, ns, f, provCfg{name: "r1"})
			name, id, tag := submitRunning(t, ns, f)
			key := keyOf(ns, name)
			starts := f.sbatch(t, "sbatch_start", "")
			if len(starts) != 1 || !slices.Contains(starts[0].Argv, "--no-requeue") {
				t.Fatalf("want one sbatch with --no-requeue, got %d calls, argv %v", len(starts), starts)
			}
			f.ctl(t, "requeue", fmt.Sprint(id))
			if st, n := f.squeueJob(t, tag, id); !slices.Equal(st, []string{"PENDING"}) || n != 1 {
				t.Fatalf("precondition: squeue after requeue shows %v restart_cnt %d; want [PENDING] and 1 (count-only evidence)", st, n)
			}
			waitAlloc(t, key, 10*time.Second, "requeue reported as failed (count-only)", requeuedFailed)

			// The requeued execution runs and ends; after the purge only sacct
			// knows the job.
			f.ctl(t, "start", fmt.Sprint(id))
			f.ctl(t, "end", fmt.Sprint(id), "COMPLETED")
			eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
			time.Sleep(4 * time.Second)
			a := getAlloc(t, key)
			if o := a.Status.AtProvider; !requeuedFailed(a) || o.Source != "sacct" || o.JobID != id {
				t.Errorf("after purge: got phase %q requeued %v source %q job %d; want Failed, true, sacct, %d", o.Phase, o.Requeued, o.Source, o.JobID, id)
			}

			// Accounting purges the earlier records: no evidence is left.
			f.ctl(t, "forget-requeue", fmt.Sprint(id))
			if recs, flagged, n := f.sacctEvidence(t, tag, id); recs != 1 || flagged || n != 0 {
				t.Fatalf("precondition: sacct still shows evidence: %d records, flag %v, restart_cnt %d", recs, flagged, n)
			}
			time.Sleep(4 * time.Second)
			if a = getAlloc(t, key); !requeuedFailed(a) {
				t.Errorf("sticky verdict lost once the evidence was gone: phase %q requeued %v reason %q", a.Status.AtProvider.Phase, a.Status.AtProvider.Requeued, a.Status.AtProvider.Reason)
			}
			assertSubmissions(t, f, "", 1)
			if r := f.releasedIDs(t); len(r) != 1 {
				t.Errorf("released %v, want only the first execution's release", r)
			}

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

// Scenario 3 from accounting alone: the requeue, the rerun, the end and the
// purge all happen while no reconciler runs. On v0.0.42 sacct carries
// restart_cnt; on v0.0.40 only the earlier record's REQUEUED flag and the
// record count show it (flag-only evidence).
func TestS3RequeueWhileDown(t *testing.T) {
	for _, parser := range parsers {
		t.Run(parser, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t, "FAKE_SLURM_DATA_PARSER="+parser)
			r1 := startProvider(t, ns, f, provCfg{name: "r1"})
			name, id, tag := submitRunning(t, ns, f)
			r1.kill(t)
			f.ctl(t, "requeue", fmt.Sprint(id))
			f.ctl(t, "start", fmt.Sprint(id))
			f.ctl(t, "end", fmt.Sprint(id), "COMPLETED")
			eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
			recs, flagged, n := f.sacctEvidence(t, tag, id)
			t.Logf("sacct evidence: %d records, REQUEUED flag %v, restart_cnt %d", recs, flagged, n)
			if parser == "v0.0.40" && (n != 0 || !flagged) {
				t.Fatalf("precondition: v0.0.40 should show the flag and no restart_cnt")
			}
			startProvider(t, ns, f, provCfg{name: "r2"})
			waitAlloc(t, keyOf(ns, name), 15*time.Second, "requeue found in accounting alone", func(a *v1alpha1.Allocation) bool {
				return requeuedFailed(a) && a.Status.AtProvider.Source == "sacct"
			})
			assertSubmissions(t, f, "", 1)
		})
	}
}

// A requeue can leave the job held as JobHeldUser (requeue-hold, or a
// launch failure; job_mgr.c 24.11:17613-17628), which the owner could
// release. The provider releases a job only once, so the requeued job stays
// held and the Allocation reports Failed.
func TestS3RequeueHoldNeverReleased(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1"})
	name, id, _ := submitRunning(t, ns, f)
	f.ctl(t, "requeue-hold", fmt.Sprint(id))
	waitAlloc(t, keyOf(ns, name), 10*time.Second, "requeue reported as failed", requeuedFailed)
	time.Sleep(5 * time.Second)
	n := 0
	for _, e := range f.events(t, "scontrol_release") {
		if e.Exit == 0 && e.Note == "" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d releases, want 1 (the first execution only)", n)
	}
	for _, j := range f.jobs(t, "") {
		if !j.Held {
			t.Errorf("job %d is no longer held after its requeue-hold", j.ID)
		}
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

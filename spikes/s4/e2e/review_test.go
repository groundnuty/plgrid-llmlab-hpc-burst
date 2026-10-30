package e2e

// The Opus review's three reproductions (they failed at 9b3d066; logs in the
// review), adapted to the two-phase protocol, plus the late-registration
// case they imply. Expected counts come from issue #24: one job per
// Allocation, never two able to run.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/controller"
)

// lateSbatch puts a wrapper sbatch first on the provider's PATH: its first
// call runs the fake in the background with a registration delay and exits
// when the provider's reconcile timeout kills it, as when a remote sbatch
// outlives its ssh channel or slurmctld handles the RPC after the client
// gave up. Later calls go straight to the fake.
func lateSbatch(t *testing.T, f *fakeSlurm, delay time.Duration) string {
	t.Helper()
	wrap := filepath.Join(f.dir, "wrap")
	if err := os.MkdirAll(wrap, 0o755); err != nil {
		t.Fatal(err)
	}
	once := filepath.Join(f.dir, "first-sbatch")
	real := filepath.Join(h.slurmBin, "sbatch")
	script := fmt.Sprintf("#!/bin/sh\nif [ ! -e %[1]q ]; then\n  : > %[1]q\n  FAKE_SLURM_SBATCH_DELAY_BEFORE=%[3]d %[2]q \"$@\" >/dev/null 2>&1 &\n  wait $!\n  exit $?\nfi\nexec %[2]q \"$@\"\n", once, real, int(delay/time.Second))
	if err := os.WriteFile(filepath.Join(wrap, "sbatch"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return "PATH=" + wrap + ":" + h.slurmBin + ":" + os.Getenv("PATH")
}

// Opus repro 1: an ambiguous sbatch failure. The first sbatch's client is
// killed at the reconcile timeout (5 s); its job registers 7 s after the
// call. At 9b3d066 Create returned an error, crossplane wrote create-failed,
// the guard switched off and a second job was submitted. Now the outcome is
// Unknown, the guard holds, and the late job is adopted: 1 job, released.
func TestReviewAmbiguousSbatchFailure(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1", env: []string{lateSbatch(t, f, 7*time.Second)}})
	key := createAllocation(t, ns, "a1")
	time.Sleep(25 * time.Second)
	a := getAlloc(t, key)
	for _, e := range f.events(t, "sbatch_submitted") {
		t.Logf("sbatch_submitted job %d at %s by %s", e.JobID, e.TS.Format(time.RFC3339Nano), e.Caller)
	}
	t.Logf("annotations %v", a.GetAnnotations())
	assertSubmissions(t, f, "", 1)
	assertRunsOnce(t, f, externalName(a), 0)
	if a.Status.AtProvider.JobID == 0 {
		t.Errorf("late job not adopted: status %+v", a.Status.AtProvider)
	}
}

// The same ambiguous failure, with the job registering only after the
// in-flight window (16 s). Hold mode retries after the window; the late job
// lands held and is cancelled, so exactly one job is ever released. Fallback
// mode never retries on elapsed time: it raises the operator condition at
// the bound, then adopts the late job when it appears.
func TestAmbiguousLateRegistrationAfterWindow(t *testing.T) {
	for _, md := range modes {
		t.Run(md.name, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t)
			startProvider(t, ns, f, provCfg{name: "r1", args: md.args, env: []string{lateSbatch(t, f, inflightWindow+4*time.Second)}})
			key := createAllocation(t, ns, "a1")
			if md.name == "hold" {
				f.waitEvents(t, "sbatch_submitted", 2, inflightWindow+20*time.Second)
				time.Sleep(4 * time.Second)
				a := getAlloc(t, key)
				id := assertRunsOnce(t, f, externalName(a), 2*time.Second)
				if a.Status.AtProvider.JobID != id {
					t.Errorf("status job %d, want the released job %d", a.Status.AtProvider.JobID, id)
				}
				return
			}
			waitAlloc(t, key, unknownBound+10*time.Second, "operator condition", func(a *v1alpha1.Allocation) bool {
				return a.GetCondition(controller.TypeSubmissionResolved).Reason == controller.ReasonNeedsOperator
			})
			a := waitAlloc(t, key, 20*time.Second, "late job adopted", func(a *v1alpha1.Allocation) bool {
				return a.Status.AtProvider.JobID != 0 && a.GetCondition(controller.TypeSubmissionResolved).Status == corev1.ConditionTrue
			})
			time.Sleep(3 * time.Second)
			assertSubmissions(t, f, "", 1)
			if live := f.liveJobs(t, externalName(a)); len(live) != 1 {
				t.Errorf("%d live jobs, want 1", len(live))
			}
		})
	}
}

// Opus repro 2: accounting lags past the in-flight window after a kill
// (slurmctld purges a finished job without its accounting record:
// job_mgr.c 23.11:11519-11528, 24.11:9841-9921). Hold mode: the killed
// reconciler's job is still held (a held job never runs), so it stays in
// squeue and is adopted. Fallback mode: the job ran, ended and left squeue
// while sacct lags; the provider never resubmits and asks an operator.
func TestReviewAcctLagAfterKill(t *testing.T) {
	for _, md := range modes {
		t.Run(md.name, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t, "FAKE_SLURM_ACCT_DELAY=60")
			key, m := killAfterSbatch(t, ns, f, md.args)
			a := getAlloc(t, key)
			pending, tag := meta.GetExternalCreatePending(a), externalName(a)
			if md.name == "fallback" {
				f.ctl(t, "start", fmt.Sprint(m.JobID))
				f.ctl(t, "end", fmt.Sprint(m.JobID), "FAILED")
				eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
			}
			if d := time.Until(pending.Add(inflightWindow + 2*time.Second)); d > 0 {
				time.Sleep(d)
			}
			startProvider(t, ns, f, provCfg{name: "r2", args: md.args})
			if md.name == "hold" {
				waitAlloc(t, key, 10*time.Second, "held job adopted", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == m.JobID })
				time.Sleep(3 * time.Second)
				assertSubmissions(t, f, "", 1)
				assertRunsOnce(t, f, tag, 0)
				return
			}
			waitAlloc(t, key, unknownBound+10*time.Second, "operator condition", func(a *v1alpha1.Allocation) bool {
				return a.GetCondition(controller.TypeSubmissionResolved).Reason == controller.ReasonNeedsOperator
			})
			time.Sleep(3 * time.Second)
			assertSubmissions(t, f, "", 1)
		})
	}
}

// Opus repro 3: delete an Allocation that has a live duplicate. Every job
// with the tag must be cancelled before the finalizer goes (HBEP-11). The
// duplicate comes from the fallback no-guard control (two live jobs).
func TestReviewDeleteWithDuplicate(t *testing.T) {
	f, key, tag := noGuardDuplicate(t, []string{"--hold-release=false"})
	live := f.liveJobs(t, tag)
	if len(live) != 2 {
		t.Fatalf("precondition: %d live jobs with the tag, want 2", len(live))
	}
	if err := h.k8s.Delete(context.Background(), getAlloc(t, key)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, func() (bool, string) { return allocGone(t, key), "Allocation still present" })
	for _, j := range live {
		ok := false
		for _, e := range f.events(t, "scancel") {
			ok = ok || (e.JobID == j.ID && e.Exit == 0)
		}
		if !ok {
			t.Errorf("job %d was never cancelled", j.ID)
		}
	}
	if left := f.liveJobs(t, tag); len(left) != 0 {
		t.Errorf("Allocation deleted, %d job(s) with its tag still live", len(left))
	}
}

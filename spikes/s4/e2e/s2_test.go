package e2e

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/controller"
)

// killAfterSbatch starts reconciler r1 with the after-sbatch fault hook,
// creates an Allocation, waits until sbatch has returned inside Create, and
// SIGKILLs r1 there. It checks that the kill landed before anything recording
// the submission was persisted.
func killAfterSbatch(t *testing.T, ns string, f *fakeSlurm, args []string) (client.ObjectKey, marker) {
	t.Helper()
	mpath := filepath.Join(f.dir, "after-sbatch.marker")
	r1 := startProvider(t, ns, f, provCfg{name: "r1", args: args, env: []string{"S4_FAULT_AFTER_SBATCH=" + mpath}})
	key := createAllocation(t, ns, "a1")
	m := waitMarker(t, mpath, 20*time.Second)
	r1.kill(t)
	assertKilledBeforePersist(t, key, m.JobID)
	return key, m
}

func assertKilledBeforePersist(t *testing.T, key client.ObjectKey, jobID int64) {
	t.Helper()
	a := getAlloc(t, key)
	if !hasAnno(a, meta.AnnotationKeyExternalCreatePending) {
		t.Fatalf("precondition: no create-pending annotation: %v", a.GetAnnotations())
	}
	if hasAnno(a, meta.AnnotationKeyExternalCreateSucceeded) || hasAnno(a, meta.AnnotationKeyExternalCreateFailed) {
		t.Fatalf("precondition: create result already persisted: %v", a.GetAnnotations())
	}
	if a.Status.AtProvider.JobID != 0 || externalName(a) == fmt.Sprint(jobID) {
		t.Fatalf("precondition: job ID %d already persisted (status %d, external name %q)", jobID, a.Status.AtProvider.JobID, externalName(a))
	}
	t.Logf("killed with job %d registered in Slurm: create-pending set, no result, no job ID stored; external name %q", jobID, externalName(a))
}

func assertSubmissions(t *testing.T, f *fakeSlurm, tag string, want int) {
	t.Helper()
	starts, subs := len(f.sbatch(t, "sbatch_start", tag)), len(f.sbatch(t, "sbatch_submitted", tag))
	if starts != want || subs != want {
		t.Errorf("sbatch for %s: %d calls, %d jobs submitted; want %d", tag, starts, subs, want)
	}
}

// Scenario 2 (design): killed after sbatch returned, before the result was
// stored. On restart Observe finds the job by tag; no second sbatch.
func TestS2KillAfterSbatchReturned(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	key, m := killAfterSbatch(t, ns, f, nil)
	tag := externalName(getAlloc(t, key))
	startProvider(t, ns, f, provCfg{name: "r2"})
	a := waitAlloc(t, key, 15*time.Second, "job found after restart", func(a *v1alpha1.Allocation) bool {
		return a.Status.AtProvider.JobID == m.JobID
	})
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, tag, 1)
	a = getAlloc(t, key)
	if !hasAnno(a, meta.AnnotationKeyExternalCreateSucceeded) {
		t.Errorf("create-succeeded not recorded after recovery: %v", a.GetAnnotations())
	}
	t.Logf("recovered job %d phase %s source %s", a.Status.AtProvider.JobID, a.Status.AtProvider.Phase, a.Status.AtProvider.Source)
}

// Scenario 2 (design), recovery through sacct: the job ends and is purged
// from squeue while no reconciler runs.
func TestS2KillAfterSbatchJobEndsWhileDown(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	key, m := killAfterSbatch(t, ns, f, nil)
	tag := externalName(getAlloc(t, key))
	f.ctl(t, "start", fmt.Sprint(m.JobID))
	f.ctl(t, "end", fmt.Sprint(m.JobID), "COMPLETED")
	eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
	startProvider(t, ns, f, provCfg{name: "r2"})
	waitAlloc(t, key, 15*time.Second, "ended job found in sacct", func(a *v1alpha1.Allocation) bool {
		o := a.Status.AtProvider
		return o.JobID == m.JobID && o.Phase == v1alpha1.PhaseSucceeded && o.Source == "sacct"
	})
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, tag, 1)
}

// Scenario 2 (design), lost reply: sbatch registered the job but the
// reconciler died before sbatch replied.
func TestS2KillWhileSbatchBlocked(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	bpath := filepath.Join(f.dir, "sbatch-block.marker")
	r1 := startProvider(t, ns, f, provCfg{name: "r1", env: []string{"FAKE_SLURM_SBATCH_BLOCK_MARKER=" + bpath}})
	key := createAllocation(t, ns, "a1")
	m := waitMarker(t, bpath, 20*time.Second)
	r1.kill(t)
	if err := syscall.Kill(m.PID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("precondition: blocked sbatch pid %d still alive (%v); the reply was not lost", m.PID, err)
	}
	assertKilledBeforePersist(t, key, m.JobID)
	tag := externalName(getAlloc(t, key))
	startProvider(t, ns, f, provCfg{name: "r2"})
	waitAlloc(t, key, 15*time.Second, "job found after restart", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == m.JobID })
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, tag, 1)
}

// Scenario 2 (design), killed after create-pending was written but before
// sbatch ran. The restarted reconciler waits out the in-flight window, then
// submits exactly once.
func TestS2KillBeforeSbatch(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	mpath := filepath.Join(f.dir, "before-sbatch.marker")
	r1 := startProvider(t, ns, f, provCfg{name: "r1", env: []string{"S4_FAULT_BEFORE_SBATCH=" + mpath}})
	key := createAllocation(t, ns, "a1")
	waitMarker(t, mpath, 20*time.Second)
	r1.kill(t)
	a := getAlloc(t, key)
	tag := externalName(a)
	pending := meta.GetExternalCreatePending(a)
	if pending.IsZero() || len(f.sbatch(t, "sbatch_start", tag)) != 0 {
		t.Fatalf("precondition: want create-pending set and no sbatch; pending %v", pending)
	}
	startProvider(t, ns, f, provCfg{name: "r2"})
	eventually(t, inflightWindow+15*time.Second, func() (bool, string) {
		return len(f.sbatch(t, "sbatch_submitted", tag)) == 1, "no submission yet"
	})
	first := f.sbatch(t, "sbatch_start", tag)[0].TS
	if wait := first.Sub(pending); wait < inflightWindow-time.Second {
		t.Errorf("submitted %s after create-pending; the in-flight guard should hold for %s", wait, inflightWindow)
	}
	waitAlloc(t, key, 10*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	time.Sleep(5 * time.Second)
	assertSubmissions(t, f, tag, 1)
	t.Logf("submitted %s after the dead reconciler's create-pending", first.Sub(pending).Round(time.Millisecond))
}

// Scenario 2, annotations alone: crossplane's classic pattern (external name
// = job ID set by Create) with WithDeterministicExternalName(false), the
// default. The restarted reconciler refuses to go on: no duplicate, but the
// Allocation is stuck and never observes its job.
func TestS2AnnotationsOnlyStalls(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	args := []string{"--observe=" + controller.ModeJobID, "--deterministic-external-name=false"}
	key, m := killAfterSbatch(t, ns, f, args)
	restart := time.Now()
	startProvider(t, ns, f, provCfg{name: "r2", args: args})
	a := waitAlloc(t, key, 15*time.Second, "stalled with a create-incomplete error", func(a *v1alpha1.Allocation) bool {
		c := a.GetCondition(xpv2.TypeSynced)
		return c.Reason == xpv2.ReasonReconcileError && strings.Contains(c.Message, "cannot determine creation result")
	})
	time.Sleep(8 * time.Second)
	a = getAlloc(t, key)
	assertSubmissions(t, f, "", 1)
	if a.Status.AtProvider.JobID != 0 {
		t.Errorf("stalled Allocation observed job %d; expected it never to observe", a.Status.AtProvider.JobID)
	}
	if n := f.calls(t, "squeue", "r2", restart) + f.calls(t, "sacct", "r2", restart); n != 0 {
		t.Errorf("restarted reconciler made %d squeue/sacct calls; expected none (it stops before Observe)", n)
	}
	t.Logf("stuck: job %d runs unobserved; Synced=%s %q", m.JobID, a.GetCondition(xpv2.TypeSynced).Status, a.GetCondition(xpv2.TypeSynced).Message)
}

// Control for scenario 2: the classic pattern with
// WithDeterministicExternalName(true) and no tag lookup resubmits.
func TestS2ControlNoTagLookupDuplicates(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	args := []string{"--observe=" + controller.ModeJobID, "--deterministic-external-name=true", "--guard-inflight=false", "--guard-succeeded=false"}
	killAfterSbatch(t, ns, f, args)
	startProvider(t, ns, f, provCfg{name: "r2", args: args})
	eventually(t, 15*time.Second, func() (bool, string) {
		n := len(f.sbatch(t, "sbatch_submitted", ""))
		return n == 2, fmt.Sprintf("%d submissions", n)
	})
}

package e2e

// Review fixes beyond the races: identity (Astra B4), duplicates and output
// order (Astra B5), the Lost grace period (science agent), and spec
// immutability (Opus nit).

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/controller"
)

func createAllocationNamed(t *testing.T, ns, name, externalName string) client.ObjectKey {
	t.Helper()
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: map[string]string{meta.AnnotationKeyExternalName: externalName}},
		Spec:       v1alpha1.AllocationSpec{ForProvider: v1alpha1.AllocationParameters{Partition: "gpu", TimeLimit: "01:00:00", Wrap: "serve-model"}},
	}
	if err := h.k8s.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return client.ObjectKeyFromObject(a)
}

// B4: a prepopulated external name that is not the canonical tag. With no
// job under it, the provider refuses to submit and says why. With a job
// under it (a restored object), the provider observes that job, submits
// nothing, and cancels it on deletion: one name for observation, deletion
// and status.
func TestPrepopulatedExternalName(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	restored := "hb-restored-0000aaaa0000"
	jobX := f.submitAs(t, "test", "--job-name="+restored, "--no-requeue", "--wrap=serve-model")
	startProvider(t, ns, f, provCfg{name: "r1"})
	foreign := "hb-foreign-000000000000"
	k1 := createAllocationNamed(t, ns, "a1", foreign)
	k2 := createAllocationNamed(t, ns, "a2", restored)

	a1 := waitAlloc(t, k1, 15*time.Second, "refused", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseRefused })
	if r := a1.Status.AtProvider.Reason; !strings.Contains(r, foreign) || !strings.Contains(r, controller.Tag(a1)) {
		t.Errorf("refusal reason %q should name %q and the canonical tag %q", r, foreign, controller.Tag(a1))
	}
	a2 := waitAlloc(t, k2, 15*time.Second, "restored job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID == jobX })
	if a2.Status.AtProvider.Tag != restored {
		t.Errorf("status tag %q, want the persisted name %q", a2.Status.AtProvider.Tag, restored)
	}
	time.Sleep(4 * time.Second)
	if n := len(f.sbatch(t, "sbatch_start", "")) - 1; n != 0 {
		t.Errorf("provider ran sbatch %d times; want 0 (the only job is the test's)", n)
	}
	for _, k := range []client.ObjectKey{k1, k2} {
		if err := h.k8s.Delete(context.Background(), getAlloc(t, k)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, func() (bool, string) { return allocGone(t, k1) && allocGone(t, k2), "Allocations still present" })
	if live := f.liveJobs(t, restored); len(live) != 0 {
		t.Errorf("job %d under the restored name still live after deletion", jobX)
	}
}

// B5: a duplicate appears, then the established job completes and is purged
// from squeue while the duplicate stays pending, with squeue printing jobs in
// descending order. The established job ID is kept (from sacct), and the
// duplicate stays reported.
func TestDuplicateKeptAfterOriginalPurged(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1", env: []string{"FAKE_SLURM_SQUEUE_ORDER=desc"}})
	key := createAllocation(t, ns, "a1")
	a := waitAlloc(t, key, 20*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	idA, tag := a.Status.AtProvider.JobID, externalName(a)
	waitReleased(t, f, idA)
	idB := f.submitAs(t, "test", "--job-name="+tag, "--no-requeue", "--wrap=serve-model")
	waitAlloc(t, key, 10*time.Second, "duplicate reported", func(a *v1alpha1.Allocation) bool {
		o := a.Status.AtProvider
		return o.JobID == idA && len(o.Duplicates) == 1 && o.Duplicates[0] == idB
	})
	f.ctl(t, "start", fmt.Sprint(idA))
	f.ctl(t, "end", fmt.Sprint(idA), "COMPLETED")
	eventually(t, minJobAge+5*time.Second, func() (bool, string) {
		ids := f.squeueIDs(t, tag)
		return len(ids) == 1 && ids[0] == idB, fmt.Sprintf("squeue lists %v", ids)
	})
	time.Sleep(4 * time.Second)
	o := getAlloc(t, key).Status.AtProvider
	if o.JobID != idA || o.Phase != v1alpha1.PhaseSucceeded || len(o.Duplicates) != 1 || o.Duplicates[0] != idB {
		t.Errorf("after the purge: job %d phase %s duplicates %v; want job %d Succeeded, duplicates [%d]", o.JobID, o.Phase, o.Duplicates, idA, idB)
	}
	if len(k8sEvents(t, ns, "DuplicateJob")) == 0 {
		t.Errorf("no DuplicateJob event")
	}
}

// Adoption with reversed squeue output: a reconciler dies after sbatch
// returned (job held, nothing recorded) and a second held job with the tag
// exists. The restarted reconciler adopts the lowest job ID whatever the
// output order, cancels the other while held, and releases only the adopted
// job.
func TestAdoptLowestWithReversedOrder(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	key, m := killAfterSbatch(t, ns, f, nil)
	tag := externalName(getAlloc(t, key))
	second := f.submitAs(t, "test", "--hold", "--job-name="+tag, "--no-requeue", "--wrap=serve-model")
	if second <= m.JobID {
		t.Fatalf("precondition: second job %d should have a higher ID than %d", second, m.JobID)
	}
	startProvider(t, ns, f, provCfg{name: "r2", env: []string{"FAKE_SLURM_SQUEUE_ORDER=desc"}})
	waitAlloc(t, key, 15*time.Second, "lowest adopted", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	time.Sleep(3 * time.Second)
	a := getAlloc(t, key)
	if got := a.GetAnnotations()[controller.AnnoJobID]; got != fmt.Sprint(m.JobID) {
		t.Errorf("adopted job %s, want the lowest ID %d", got, m.JobID)
	}
	if id := assertRunsOnce(t, f, tag, 0); id != m.JobID {
		t.Errorf("released job %d, want %d", id, m.JobID)
	}
}

// Lost needs a grace period and is revisable (science agent's change): a job
// absent from both squeue and sacct is not Lost before lost_grace, is Lost
// after it, and returns to its real state when accounting catches up.
func TestLostGraceAndReappearance(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	startProvider(t, ns, f, provCfg{name: "r1"})
	key := createAllocation(t, ns, "a1")
	a := waitAlloc(t, key, 20*time.Second, "job observed", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
	id, tag := a.Status.AtProvider.JobID, externalName(a)
	waitReleased(t, f, id)
	f.ctl(t, "start", fmt.Sprint(id))
	f.ctl(t, "end", fmt.Sprint(id), "COMPLETED")
	f.ctl(t, "acct-hide", fmt.Sprint(id)) // slurmdbd has no record yet
	eventually(t, minJobAge+5*time.Second, func() (bool, string) { return len(f.squeueIDs(t, tag)) == 0, "not purged" })
	purged := time.Now()
	for time.Since(purged) < lostGrace-2*time.Second {
		if o := getAlloc(t, key).Status.AtProvider; o.Phase == v1alpha1.PhaseLost {
			t.Fatalf("reported Lost %s after the job left both squeue and sacct; lost_grace is %s", time.Since(purged).Round(time.Millisecond), lostGrace)
		}
		time.Sleep(500 * time.Millisecond)
	}
	waitAlloc(t, key, 10*time.Second, "Lost after lost_grace", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.Phase == v1alpha1.PhaseLost })
	lostAfter := time.Since(purged)
	if len(k8sEvents(t, ns, "JobLost")) == 0 {
		t.Errorf("no JobLost event")
	}
	f.ctl(t, "acct-show", fmt.Sprint(id)) // accounting catches up
	waitAlloc(t, key, 10*time.Second, "back to the real state", func(a *v1alpha1.Allocation) bool {
		o := a.Status.AtProvider
		return o.Phase == v1alpha1.PhaseSucceeded && o.Source == "sacct" && o.AbsentSince == nil
	})
	if len(k8sEvents(t, ns, "JobReappeared")) == 0 {
		t.Errorf("no JobReappeared event")
	}
	assertSubmissions(t, f, "", 1)
	t.Logf("Lost %s after the purge (lost_grace %s); back to Succeeded once sacct had the record", lostAfter.Round(time.Millisecond), lostGrace)
}

// forProvider is immutable (a CEL rule in the CRD), so an edit is refused
// visibly instead of being ignored by an Observe that always reports up to
// date.
func TestSpecImmutable(t *testing.T) {
	ns := newNamespace(t)
	key := createAllocation(t, ns, "a1")
	a := getAlloc(t, key)
	a.Spec.ForProvider.Wrap = "something-else"
	err := h.k8s.Update(context.Background(), a)
	if err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Errorf("editing forProvider: got %v, want a refusal naming immutability", err)
	}
}

// B3 in fallback mode: the late job is not held, so it would run; it must
// still be cancelled before the finalizer goes.
func TestB3FallbackDeleteWhileSubmissionInFlight(t *testing.T) {
	b3Run(t, withArgs(gatedArgs, "--hold-release=false"))
}

// Fallback mode keeps the finalizer while an attempt is unresolved, past the
// in-flight window, until positive resolution or an operator's abandon: a
// late registration there would run unheld. The claimant is killed before
// sbatch, so nothing ever registers; after the abandon the finalizer goes
// with no submission at all.
func TestFallbackDeleteWaitsForUnresolvedAttempt(t *testing.T) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	fallback := []string{"--hold-release=false"}
	mpath := filepath.Join(f.dir, "before-sbatch.marker")
	r1 := startProvider(t, ns, f, provCfg{name: "r1", args: fallback, env: []string{"S4_FAULT_BEFORE_SBATCH=" + mpath}})
	key := createAllocation(t, ns, "a1")
	waitMarker(t, mpath, 20*time.Second)
	r1.kill(t)
	claimed := meta.GetExternalCreatePending(getAlloc(t, key))
	if err := h.k8s.Delete(context.Background(), getAlloc(t, key)); err != nil {
		t.Fatal(err)
	}
	startProvider(t, ns, f, provCfg{name: "r2", args: fallback})
	a := waitAlloc(t, key, unknownBound+15*time.Second, "operator condition during deletion", func(a *v1alpha1.Allocation) bool {
		return a.GetCondition(controller.TypeSubmissionResolved).Reason == controller.ReasonNeedsOperator
	})
	if age := time.Since(claimed); age < inflightWindow+2*time.Second {
		t.Fatalf("precondition: only %s since the claim; the window is %s", age, inflightWindow)
	}
	if allocGone(t, key) {
		t.Fatalf("finalizer released while the attempt was unresolved")
	}
	annotate(t, key, controller.AnnoAbandon, fmt.Sprint(a.Status.AtProvider.Attempt))
	eventually(t, 15*time.Second, func() (bool, string) { return allocGone(t, key), "Allocation still present after the abandon" })
	if n := len(f.sbatch(t, "sbatch_start", "")); n != 0 {
		t.Errorf("%d sbatch calls; want 0", n)
	}
}

// A reply lost after slurmctld accepted the job ("Socket timed out", exit
// 1) is Unknown, not Rejected: the job is found by tag and adopted, never
// submitted again.
func TestLostReplyIsUnknown(t *testing.T) {
	for _, md := range modes {
		t.Run(md.name, func(t *testing.T) {
			ns := newNamespace(t)
			f := newFakeSlurm(t)
			f.env = append(f.env, "FAKE_SLURM_SBATCH_LOST_REPLY_ONCE="+filepath.Join(f.dir, "lost-reply.marker"))
			r1 := startProvider(t, ns, f, provCfg{name: "r1", args: md.args})
			key := createAllocation(t, ns, "a1")
			a := waitAlloc(t, key, 20*time.Second, "job adopted", func(a *v1alpha1.Allocation) bool { return a.Status.AtProvider.JobID != 0 })
			time.Sleep(4 * time.Second)
			assertSubmissions(t, f, "", 1)
			if !r1.logContains(t, "sbatch outcome unknown") || len(k8sEvents(t, ns, "SubmissionUnknown")) == 0 {
				t.Errorf("the lost reply was not treated as Unknown")
			}
			if md.name == "hold" {
				assertRunsOnce(t, f, externalName(a), 0)
			} else if live := f.liveJobs(t, externalName(a)); len(live) != 1 {
				t.Errorf("%d live jobs, want 1", len(live))
			}
		})
	}
}

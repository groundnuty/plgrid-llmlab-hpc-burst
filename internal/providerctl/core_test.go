package providerctl

import (
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A fresh Allocation in hold mode: the tag becomes the external name, the
// finalizer is added, the three files are staged, one held sbatch runs and its
// hold is lifted once, and the job is observed Running.
func TestHoldModeSubmitsHeldAndLiftsOnce(t *testing.T) {
	e := newEnv(t)
	a := e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a = e.get("alloc-a")
	tag := tagOf(a)
	if got := meta.GetExternalName(a); got != tag {
		t.Errorf("external name %q, want the tag %q", got, tag)
	}
	if !sets.New(a.Finalizers...).Has(v1alpha1.FinalizerCancelAll) {
		t.Errorf("finalizers %v lack %s", a.Finalizers, v1alpha1.FinalizerCancelAll)
	}
	if n := e.count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1", n)
	}
	if n := e.lifts(); n != 1 {
		t.Errorf("%d lifts took effect, want 1", n)
	}
	if got := a.Annotations[v1alpha1.AnnotationHoldLifted]; got == "" || got != a.Annotations[v1alpha1.AnnotationJobID] {
		t.Errorf("hold-lifted %q, job-id %q", got, a.Annotations[v1alpha1.AnnotationJobID])
	}
	if p := a.Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
		t.Errorf("phase %s, want Running (reason %q)", p, a.Status.AtProvider.Reason)
	}
	files := e.batch.Files()
	for _, p := range []string{agentapi.StagedAgentPath(e.digest), agentapi.StagedCredentialPath(tag), agentapi.StagedConfigPath(tag)} {
		if _, ok := files[p]; !ok {
			t.Errorf("file %s was not staged", p)
		}
	}
	for _, c := range e.batch.Calls() {
		for _, arg := range c.Args {
			if arg == string(files[agentapi.StagedCredentialPath(tag)].Content) {
				t.Fatal("the credential reached the call log")
			}
		}
	}
	// More reconciles change nothing.
	e.run("alloc-a", 3)
	if n, l := e.count("sbatch"), e.lifts(); n != 1 || l != 1 {
		t.Errorf("after more reconciles: %d sbatch, %d lifts; want 1 and 1", n, l)
	}
}

// Deleting cancels every live job with the tag, removes the files, then the
// finalizer goes.
func TestDeleteCancelsAndReleasesTheFinalizer(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag := tagOf(a)
	if err := e.c.Delete(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	e.run("alloc-a", 4)
	if n := e.count("scancel"); n != 1 {
		t.Errorf("%d scancel calls, want 1", n)
	}
	if live := e.live(tag); len(live) != 0 {
		t.Errorf("live jobs with the tag after deletion: %v", live)
	}
	if n := e.count("remove-allocation-files"); n != 1 {
		t.Errorf("%d file removals, want 1", n)
	}
	got := &v1alpha1.Allocation{}
	if err := e.c.Get(e.ctx, clientKey("alloc-a"), got); err == nil {
		t.Errorf("the Allocation still exists with finalizers %v", got.Finalizers)
	}
}

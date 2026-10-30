package e2e

// Races from the Astra review (B2, B3), driven by barriers in the fake
// rather than by timing. These tests use only flags and helpers that also
// exist at 9b3d066, so the same file runs against that commit's provider
// (README, "Evidence against the old logic"). Expected counts come from
// issue #24 (one job per Allocation) and HBEP-11 (a provider job is
// cancelled before its object disappears), never from the code.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
)

// Longer timeouts than the defaults, so a gated sbatch is not killed by the
// reconcile timeout while the test holds the gate.
var gatedArgs = []string{"--timeout=15s", "--inflight-window=20s"}

// B2: a definite sbatch failure and its retry fall in the same second.
// crossplane's create annotations have one-second resolution, and
// ExternalCreateIncomplete wants create-pending strictly newer than
// create-failed, so a tie switches a timestamp-based in-flight guard off
// while the retry's sbatch runs, and the second reconciler submits too.
// Expected: 2 sbatch calls (the rejected one and one retry), 1 job accepted,
// at most 1 job able to run.
func TestB2SameSecondFailureRetryRace(t *testing.T) {
	for run := 1; run <= 3; run++ {
		tie := false
		t.Run(fmt.Sprintf("run%d", run), func(t *testing.T) { tie = b2Run(t) })
		if tie {
			return
		}
	}
	t.Errorf("precondition never held in 3 runs: create-pending of the retry never fell in the same second as create-failed")
}

func b2Run(t *testing.T) bool {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	g := newGate(f)
	f.env = append(f.env, g.env(), "FAKE_SLURM_SBATCH_FAIL_ONCE="+f.dir+"/fail-once.marker", "FAKE_SLURM_SBATCH_FAIL_ALIGN=1")
	for _, name := range []string{"r1", "r2"} {
		startProvider(t, ns, f, provCfg{name: name, args: gatedArgs}).waitLog(t, "Starting workers", 20*time.Second)
	}
	key := createAllocation(t, ns, "a1")
	f.waitEvents(t, "sbatch_failed", 1, 20*time.Second)
	waits := f.waitEvents(t, "sbatch_gate_wait", 1, 10*time.Second)
	a := getAlloc(t, key)
	pending, failed := a.GetAnnotations()[meta.AnnotationKeyExternalCreatePending], a.GetAnnotations()[meta.AnnotationKeyExternalCreateFailed]
	if pending == "" || pending != failed {
		g.open(t)
		t.Skipf("precondition not met (create-pending %q, create-failed %q): no same-second tie in this run", pending, failed)
	}
	t.Logf("tie: create-failed and the retry's create-pending are both %s; %s's retry is held at the gate", failed, waits[0].Caller)
	time.Sleep(4 * time.Second) // the other reconciler reads the tied version meanwhile
	g.open(t)
	time.Sleep(8 * time.Second)

	starts, fails, subs := len(f.events(t, "sbatch_start")), len(f.events(t, "sbatch_failed")), len(f.events(t, "sbatch_submitted"))
	if subs != 1 || starts != 2 || fails != 1 {
		t.Errorf("B2: %d sbatch calls, %d rejected, %d jobs accepted for one Allocation; want 2, 1, 1", starts, fails, subs)
	}
	runnable := 0
	for _, j := range f.liveJobs(t, externalName(a)) {
		if !j.Held {
			runnable++
		}
	}
	if r := len(f.releasedIDs(t)); runnable > 1 || r > 1 {
		t.Errorf("B2: %d live jobs able to run, %d released; want at most 1", runnable, r)
	}
	t.Logf("%d sbatch calls (%d rejected), %d accepted, %d live able to run", starts, fails, subs, runnable)
	return true
}

// B3: delete the Allocation while a reconciler's sbatch is in flight. The
// finalizer must stay until the submission resolves, and the job that lands
// afterwards must be cancelled before the object disappears (HBEP-11).
func TestB3DeleteWhileSubmissionInFlight(t *testing.T) { b3Run(t, gatedArgs) }

func b3Run(t *testing.T, args []string) {
	ns := newNamespace(t)
	f := newFakeSlurm(t)
	g := newGate(f)
	f.env = append(f.env, g.env())
	for _, name := range []string{"r1", "r2"} {
		startProvider(t, ns, f, provCfg{name: name, args: args}).waitLog(t, "Starting workers", 20*time.Second)
	}
	key := createAllocation(t, ns, "a1")
	submitter := f.waitEvents(t, "sbatch_gate_wait", 1, 20*time.Second)[0].Caller
	a := getAlloc(t, key)
	tag := externalName(a)
	if err := h.k8s.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // the other reconciler handles the deletion meanwhile
	if allocGone(t, key) {
		t.Errorf("B3: the finalizer was released while %s's sbatch was in flight", submitter)
	}
	g.open(t)
	id := f.waitEvents(t, "sbatch_submitted", 1, 10*time.Second)[0].JobID
	var gone time.Time
	eventually(t, 30*time.Second, func() (bool, string) {
		if allocGone(t, key) {
			gone = time.Now()
			return true, ""
		}
		return false, "Allocation still present"
	})
	cancelled := false
	for _, e := range f.events(t, "scancel") {
		if e.JobID == id && e.Exit == 0 && e.TS.Before(gone) {
			cancelled = true
		}
	}
	if !cancelled {
		t.Errorf("B3: job %d, which landed after the delete, was not cancelled before the Allocation disappeared", id)
	}
	if live := f.liveJobs(t, tag); len(live) != 0 {
		t.Errorf("B3: Allocation deleted, %d job(s) with its tag still live: %+v", len(live), live)
	}
	for _, j := range f.jobs(t, tag) {
		t.Logf("job %d: %s, started %v", j.ID, j.state(), j.started())
	}
}

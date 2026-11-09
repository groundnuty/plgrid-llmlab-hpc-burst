package conformance

// The accounting row on Helios under QOS now has the start-wait guard of
// conformance-helios-gh200 (issue #194; the science agent, 2 Nov 2026): when
// the owner's other job holds the one GPU, this run's job pends, and at the
// bound of 10 minutes it is cancelled by its tag and the run is NOT RUN, with
// no kept accounting record. The row's Athena twin has no slot: its first clock is the wait in the queue
// (HPCBURST_CONF_PENDING_MAX_SECONDS), and past that its job is cancelled by its tag and the run is NOT RUN too
// (the science agent's ruling of 8 Nov 2026, #272: a queue wait tests nothing, so it is never a FAIL).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// Both rows, one bound each: the slot row's start wait of 10 minutes, and the no-slot row's wait in the queue, set
// to a minute by the dry world. Expected values are written out, not read from the rows. Neither run may be
// reported as a failure, and a job that never started is cancelled by its tag with the owner's job left alone.
func TestTheAccountingRowIsNotRunWhenTheOwnersJobHoldsTheSlot(t *testing.T) {
	for _, c := range []struct {
		row, fault, wantSkip string
		bound                time.Duration
	}{
		{"gpu-accounting-helios", "slot-held", "NOT RUN: the job: a job did not start within 10m0s of its acceptance", 10 * time.Minute},
		{"gpu-accounting-athena", "queue-held", "NOT RUN: the job: a job was still pending after 1m0s in the queue", time.Minute},
	} {
		t.Run(c.row, func(t *testing.T) { accountingNotRun(t, c.row, c.fault, c.wantSkip, c.bound) })
	}
}

func accountingNotRun(t *testing.T, row, fault, wantSkip string, bound time.Duration) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	w, plat := dryAccountingWorld(t, row, fault, out)
	rec := &stopRecorder{TB: t}
	w.t = rec
	began := w.at()
	ends(func() { gpuAccountingBody(rec, w, row) })

	if rec.failed != "" {
		t.Fatalf("the run failed: %s", rec.failed)
	}
	if !strings.HasPrefix(rec.skipped, wantSkip) {
		t.Fatalf("skipped = %q, want a NOT RUN beginning %q", rec.skipped, wantSkip)
	}
	// The bound, on the fake clock: as long as the bound and no more than the cancel's few seconds.
	if waited := w.at().Sub(began); waited < bound || waited > bound+time.Minute {
		t.Errorf("the wait lasted %s, want the bound of %s", waited, bound)
	}
	// This run's job was cancelled and never started; the owner's was not touched.
	var ours, owners int
	for _, j := range plat.Jobs() {
		switch {
		case strings.HasPrefix(j.Name, "hb-"):
			ours++
			if j.State != fakeslurm.Cancelled || !j.Start.IsZero() {
				t.Errorf("this run's job is %s (started %v), want cancelled and never started", j.State, !j.Start.IsZero())
			}
		default:
			owners++
			if j.State != fakeslurm.Running {
				t.Errorf("the owner's job is %s, want it left running", j.State)
			}
		}
	}
	if ours != 1 || owners != 1 {
		t.Errorf("%d jobs of this run and %d of the owner's, want 1 and 1", ours, owners)
	}
	// The marker is there, with no job ID or tag; and no accounting record, no posts and no record file were written.
	raw, err := os.ReadFile(filepath.Join(out, notRunFile))
	if err != nil {
		t.Fatalf("no marker: %v", err)
	}
	if strings.Contains(string(raw), "hb-") {
		t.Errorf("the marker names a tag: %s", raw)
	}
	for _, f := range []string{"posts.md", "record.json"} {
		if _, err := os.Stat(filepath.Join(out, f)); err == nil {
			t.Errorf("a NOT RUN run wrote %s", f)
		}
	}
	var revs appsv1.ControllerRevisionList
	if err := w.c.List(t.Context(), &revs, client.InNamespace(testNS)); err != nil {
		t.Fatal(err)
	}
	for _, r := range revs.Items {
		if strings.HasPrefix(r.Name, "acct-") {
			t.Errorf("a NOT RUN run left the accounting record %s", r.Name)
		}
	}
}

// The start wait ends when the job runs or has run, never earlier and never for a job that failed: a job that
// started inside the bound is not the bound's to cancel, however long it then takes.
func TestTheAccountingStartWaitEndsOnAJobThatRunsOrHasRun(t *testing.T) {
	for _, c := range []struct {
		phase v1alpha1.AllocationPhase
		want  bool
	}{
		{v1alpha1.AllocationPending, false},
		{v1alpha1.AllocationRunning, true},
		{v1alpha1.AllocationEnded, true},
	} {
		a := &v1alpha1.Allocation{Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: c.phase}}}
		if got := accountingStarted(t, a); got != c.want {
			t.Errorf("phase %s: started %t, want %t", c.phase, got, c.want)
		}
	}
	for _, ph := range []v1alpha1.AllocationPhase{v1alpha1.AllocationFailed, v1alpha1.AllocationRefused, v1alpha1.AllocationLost} {
		a := &v1alpha1.Allocation{Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: ph}}}
		rec := &stopRecorder{TB: t}
		ends(func() { accountingStarted(rec, a) })
		if !strings.Contains(rec.failed, "not a job that ended") {
			t.Errorf("phase %s: failed = %q, want the run to end", ph, rec.failed)
		}
	}
}

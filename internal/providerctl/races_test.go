package providerctl

// Races between two reconcilers on one Allocation (HBEP-7 open question 11,
// Claim then submit; S4 scenario 4 and Astra's B2, spikes/s4/README.md). The
// gate (ptGate) holds one Submit before it reaches the fake, so nothing is
// registered while it waits; the second reconciler is a second controller from
// a second New call over the same client. Counts come from the fake's call log.

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// S4 scenario 4 (README table: "1 sbatch; the other logs the guard naming
// attempt 1 and the claimant"): while the first reconciler is inside its sbatch
// the second waits behind the claim and submits nothing. After the release
// there is one job and, in hold mode, one lift.
func TestTwoReconcilersOnOneAllocationSubmitOnce(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			gate := ptViaGate(e)
			second := ptControllerOver(e, e.platform)
			e.newAlloc("a")

			gate.arm()
			first := ptReconcileAsync(e, e.ctl, "a")
			ptWaitEntered(t, gate)

			// The claim is on the object; the first sbatch is held at the gate.
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionInFlight || r.Attempt != 1 {
				t.Fatalf("record %+v while the first sbatch is held, want attempt 1 InFlight", r)
			}
			for i := 0; i < 3; i++ {
				_ = ptReconcileWith(e, second, "a")
			}
			if n := e.count("sbatch"); n != 0 {
				t.Errorf("%d sbatch calls while the first is held at the gate, want 0", n)
			}
			if n := gate.submitCalls(); n != 1 {
				t.Errorf("%d Submit calls reached the first reconciler's platform, want 1", n)
			}
			if p := ptPhase(e, "a"); p != v1alpha1.AllocationSubmitting {
				t.Errorf("phase %s while an attempt is in flight, want Submitting", p)
			}

			close(gate.release)
			ptWaitDone(t, first)
			for i := 0; i < 3; i++ {
				_ = ptReconcileWith(e, second, "a")
				e.run("a", 1)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want exactly 1", n)
			}
			if got := ptJobID(e, "a"); got != ptFirstJob {
				t.Errorf("job ID %q, want %s", got, ptFirstJob)
			}
			wantLifts := 0
			if m.hold {
				wantLifts = 1
			}
			if n, all := e.lifts(), ptLiftAttempts(e); n != wantLifts || all != wantLifts {
				t.Errorf("%d lifts applied, %d release calls in all, want %d of each", n, all, wantLifts)
			}
		})
	}
}

// Whatever the interleaving (HBEP-7: "at most one job per Allocation running,
// whatever the timing"): two reconcilers run at once, one of them delayed in
// its sbatch, and the second races the release. The counts are checked from the
// call log: 1 sbatch, and in hold mode at most 1 scontrol release call of any
// result, then exactly 1 (Astra B8: a second release that found the job no
// longer held would not show in the applied lifts).
func TestTwoReconcilersRacingNeverSubmitOrLiftTwice(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			for i := 0; i < 15; i++ {
				e := newEnv(t, m.mods...)
				gate := ptViaGate(e)
				second := ptControllerOver(e, e.platform)
				e.newAlloc("a")

				gate.arm()
				first := ptReconcileAsync(e, e.ctl, "a")
				ptWaitEntered(t, gate)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for k := 0; k < 3; k++ {
						_ = ptReconcileWith(e, second, "a")
					}
				}()
				close(gate.release) // the second may be anywhere in its reconciles
				ptWaitDone(t, first)
				wg.Wait()

				if n := e.count("sbatch"); n != 1 {
					t.Fatalf("run %d: %d sbatch calls, want 1", i, n)
				}
				if n := ptLiftAttempts(e); n > 1 {
					t.Fatalf("run %d: %d release calls in the race, want at most 1 (Astra B8: count every call, not only the applied ones)", i, n)
				}
				e.run("a", 4)
				if n := e.count("sbatch"); n != 1 {
					t.Fatalf("run %d: %d sbatch calls after settling, want 1", i, n)
				}
				wantLifts := 0
				if m.hold {
					wantLifts = 1
				}
				if n, all := e.lifts(), ptLiftAttempts(e); n != wantLifts || all != wantLifts {
					t.Fatalf("run %d: %d lifts applied, %d release calls in all after settling, want %d of each", i, n, all, wantLifts)
				}
				if n := e.count("scancel"); n != 0 {
					t.Fatalf("run %d: %d scancel calls, want 0: there was no extra job", i, n)
				}
			}
		})
	}
}

// A claimant that read the Allocation before another claimed (a stale copy)
// loses the compare-and-swap and submits nothing (HBEP-7: "A lost claim means
// no sbatch"). Create is called directly with the stale copy. There is no
// control that switches the compare-and-swap off without editing the code under
// test.
func TestLostClaimSubmitsNothing(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			e.newAlloc("a")
			// The tag initializer's write, as a reconcile does it.
			a := e.get("a")
			meta.SetExternalName(a, tagOf(a))
			if err := e.c.Update(e.ctx, a); err != nil {
				t.Fatal(err)
			}
			stale := e.get("a") // read before anyone claimed

			e.run("a", 1) // another claimant claims attempt 1 and submits
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("precondition: %d sbatch calls, want 1", n)
			}

			ext, err := (&connector{s: e.ctl.shared}).Connect(e.ctx, stale)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ext.Create(e.ctx, stale)
			if err == nil || !strings.Contains(err.Error(), "could not claim submission attempt") || !kerrors.IsConflict(err) {
				t.Fatalf("Create from a stale copy returned %v, want a lost claim (a conflict)", err)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls after the lost claim, want still 1", n)
			}
			if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 1 {
				t.Errorf("record %+v, want the winner's attempt 1 untouched", r)
			}
		})
	}
}

// S4 Astra B2 (README table: "2 calls, 1 rejected, 1 accepted"; HBEP-7 open
// question 11): a Rejected outcome and its retry. Crossplane's create
// annotations have one-second resolution on the wall clock, so a failure and
// its retry can fall in the same second and the annotations then say nothing
// about whether a create is in flight. The test forces that tie
// (create-failed not older than the retry's create-pending, so
// ExternalCreateIncomplete is false) and asserts it holds. While the retry's
// sbatch is held at the gate a second reconciler runs; it must not submit. In
// all: 2 sbatch calls, 1 job.
func TestSameSecondFailureAndRetryRace(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			gate := ptViaGate(e)
			second := ptControllerOver(e, e.platform)
			e.batch.Enqueue(fake.Script{Outcome: provider.Rejected, Reason: "Batch job submission failed: example refusal"})
			e.newAlloc("a")
			e.run("a", 2)
			if r := ptRecordOf(e, "a"); r == nil || r.State != v1alpha1.SubmissionRejected {
				t.Fatalf("precondition: record %+v, want Rejected", r)
			}

			// The tie: create-failed is later than any create-pending the retry
			// will write, as when both fall in one second.
			a := e.get("a")
			meta.SetExternalCreateFailed(a, time.Now().Add(time.Hour))
			if err := e.c.Update(e.ctx, a); err != nil {
				t.Fatal(err)
			}

			e.clock.Advance(ptBackoff)
			gate.arm()
			retry := ptReconcileAsync(e, e.ctl, "a")
			ptWaitEntered(t, gate)
			if got := e.get("a"); meta.ExternalCreateIncomplete(got) {
				t.Fatal("precondition: the annotations still say the create is incomplete: no tie")
			} else if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 2 || r.State != v1alpha1.SubmissionInFlight {
				t.Fatalf("precondition: record %+v, want attempt 2 InFlight", r)
			}
			for i := 0; i < 3; i++ {
				_ = ptReconcileWith(e, second, "a")
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls while the retry is held and the second reconciler ran, want 1 (the rejected one)", n)
			}

			close(gate.release)
			ptWaitDone(t, retry)
			for i := 0; i < 3; i++ {
				_ = ptReconcileWith(e, second, "a")
				e.run("a", 1)
			}
			if n := e.count("sbatch"); n != 2 {
				t.Errorf("%d sbatch calls, want 2: one rejected, one accepted", n)
			}
			if _, ok := e.batch.Job(ptFirstJob); !ok {
				t.Error("no job registered")
			}
			if _, ok := e.batch.Job("999"); ok {
				t.Error("a second job was registered")
			}
			if got := ptJobID(e, "a"); got != ptFirstJob {
				t.Errorf("job ID %q, want %s", got, ptFirstJob)
			}
			if r := ptRecordOf(e, "a"); r == nil || r.Attempt != 2 || r.State != v1alpha1.SubmissionDone {
				t.Errorf("record %+v, want attempt 2 Done", r)
			}
			wantLifts := 0
			if m.hold {
				wantLifts = 1
			}
			if n, all := e.lifts(), ptLiftAttempts(e); n != wantLifts || all != wantLifts {
				t.Errorf("%d lifts applied, %d release calls in all, want %d of each", n, all, wantLifts)
			}
		})
	}
}

// liftGate wraps a Platform so that the first LiftHold blocks, before it
// reaches the fake, until the test releases it. Later calls pass.
type liftGate struct {
	*fakePlatform
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (g *liftGate) LiftHold(ctx context.Context, id string) error {
	first := false
	g.once.Do(func() { first = true })
	if first {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.fakePlatform.LiftHold(ctx, id)
}

// Astra B8 with a deterministic barrier: two controllers on one Allocation, as
// across a manager handover in one process. The first is inside its release of
// the recorded job's hold when the second observes the job, still held, with
// no hold-lifted marker. The second must not release too: it waits for the
// first's lift and re-reads the marker and the job. One release call in all,
// counted from the call log whatever its result.
func TestTwoLiftersAcrossAHandoverReleaseOnce(t *testing.T) {
	e := newEnv(t)
	g := &liftGate{fakePlatform: e.platform, entered: make(chan struct{}), release: make(chan struct{})}
	e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) { return g, io.NopCloser(nil), nil }
	e.build()
	second := ptControllerOver(e, g)
	e.newAlloc("a")

	first := ptReconcileAsync(e, e.ctl, "a") // Create, sbatch, record, settle: the lift blocks
	select {
	case <-g.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the first reconciler's lift never reached the gate")
	}
	if ptJobID(e, "a") == "" {
		t.Fatal("precondition: no job recorded while the first lift is held")
	}
	other := ptReconcileAsync(e, second, "a")
	otherDone := false
	select {
	case <-other: // without a fence it releases on its own and returns
		otherDone = true
	case <-time.After(2 * time.Second): // with one it waits for the first
	}
	close(g.release)
	ptWaitDone(t, first)
	if !otherDone {
		ptWaitDone(t, other)
	}
	e.run("a", 2)
	if n, all := e.lifts(), ptLiftAttempts(e); n != 1 || all != 1 {
		t.Errorf("%d lifts applied, %d release calls in all, want 1 of each", n, all)
	}
	if got := e.get("a").Annotations[v1alpha1.AnnotationHoldLifted]; got != ptJobID(e, "a") {
		t.Errorf("hold-lifted %q, want the recorded job %s", got, ptJobID(e, "a"))
	}
}

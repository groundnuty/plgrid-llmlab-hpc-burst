package providerctl

// The accepted residual of the hold lift (HBEP-4, Test plan, "The hold lift's
// re-read"; issue 93). liftRecorded re-reads the job under the per-job lock
// and releases only a liftable job, but the re-read and the release are two
// calls. Across two processes nothing fences the gap, so a second leader can
// release the job in it, and an administrator's requeue-with-hold can follow.

import (
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// releaseStall is a Platform whose first LiftHold stops before it reaches the
// platform, until the test lets it go. It is installed after liftRecorded's
// re-read, which runs before the LiftHold call, so the stall is between the
// re-read and the release.
type releaseStall struct {
	Platform
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *releaseStall) LiftHold(ctx context.Context, id string) error {
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Platform.LiftHold(ctx, id)
}

// withReleaseStall puts the stall around the platform the env already makes.
func withReleaseStall(s *releaseStall) realOption {
	return func(e *realEnv) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			p, c, err := inner(v)
			s.Platform = p
			return s, c, err
		}
	}
}

// The known risk's interleaving, which HBEP-4 accepts and the test plan says
// to pin "so a change in either direction shows":
//
//  1. leader A re-reads the job, finds it liftable, and stalls before its release;
//  2. leader B releases the job;
//  3. the job starts;
//  4. the fake requeues it with a hold, as an administrator would;
//  5. A resumes and releases that later hold.
//
// A's release is the one extra: the fake login node's call log shows exactly
// two "scontrol release" calls for the job, B's and A's, and the job ends
// without the hold. A guard that stopped A (a second re-read after the stall)
// would leave 1; a lift that repeated would leave more.
//
// B is a direct release on the platform, not a second reconcile: both
// leaders would take the in-process liftLocks entry for the job, and this
// residual is the cross-process case, where no lock is shared. B's marker
// write is left out for the same reason it cannot help: A re-read the
// Allocation, and found no marker, before B released.
func TestResidualInterleavingGivesOneExtraRelease(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	stall := &releaseStall{entered: make(chan struct{}), release: make(chan struct{})}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withReleaseStall(stall))
	e.newAlloc("alloc-a", 1)

	// 1. A: the reconcile that creates the job, re-reads it and stalls.
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		e.run("alloc-a", 1)
	}()
	select {
	case <-stall.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("leader A never reached its release")
	}
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatalf("precondition: job ID %q: %v", id, err)
	}
	if j := slurmJob(t, sl, id); !j.Held || j.RestartCount != 0 || releaseCalls(sl, id) != 0 {
		t.Fatalf("precondition: job held %v, restart count %d, %d releases; want held, 0, 0 while A stalls",
			j.Held, j.RestartCount, releaseCalls(sl, id))
	}

	// 2. B releases the job; 3. it starts; 4. it is requeued with a hold.
	if err := e.batch.LiftHold(e.ctx, id); err != nil {
		t.Fatalf("leader B's release: %v", err)
	}
	if got := releaseCalls(sl, id); got != 1 {
		t.Fatalf("precondition: %d releases after B's, want 1", got)
	}
	if err := sl.Start(n); err != nil {
		t.Fatalf("the released job did not start: %v", err)
	}
	if err := sl.RequeueHold(n); err != nil {
		t.Fatal(err)
	}
	if j := slurmJob(t, sl, id); !j.Held || j.RestartCount != 1 || j.State != fakeslurm.Pending {
		t.Fatalf("precondition: after the requeue the job is %s, held %v, restart count %d; want pending, held, 1", j.State, j.Held, j.RestartCount)
	}

	// 5. A resumes.
	close(stall.release)
	select {
	case <-aDone:
	case <-time.After(20 * time.Second):
		t.Fatal("leader A's reconcile did not return")
	}
	// The extra release is A's, one in all after B's, and it lifted the later hold.
	if got := releaseCalls(sl, id); got != 2 {
		t.Errorf("%d \"scontrol release\" calls reached the login node, want 2: B's and exactly one extra", got)
	}
	if j := slurmJob(t, sl, id); j.Held || j.RestartCount != 1 {
		t.Errorf("job held %v, restart count %d after A's release; want the later hold released, restart count 1", j.Held, j.RestartCount)
	}
	if got := e.get("alloc-a").Annotations[v1alpha1.AnnotationHoldLifted]; got != id {
		t.Errorf("hold-lifted marker %q, want %s (A's release writes it)", got, id)
	}
	// Later reconciles release nothing more: the residual is one release.
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 3)
	if got := releaseCalls(sl, id); got != 2 {
		t.Errorf("%d release calls after three more reconciles, want still 2", got)
	}
}

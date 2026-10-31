package managed

import (
	"context"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The rate gate: HBEP-4's "at most once a minute". Reads are counted at the
// gateway double (env.gets), never inside the controller.

func TestSyncReadsAtMostOncePerCatalogInterval(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	steps := []struct {
		at       time.Time
		wantWait time.Duration
		wantGets int
	}{
		{at(12, 0, 0), time.Minute, 1},                                   // the first pass reads
		{at(12, 0, 10), 50 * time.Second, 1},                             // 10 s later: 50 s left
		{at(12, 0, 30), 30 * time.Second, 1},                             //
		{at(12, 0, 59).Add(999 * time.Millisecond), time.Millisecond, 1}, // 1 ms short of a minute
		{at(12, 1, 0), time.Minute, 2},                                   // exactly a minute: reads again
		{at(12, 1, 59).Add(999 * time.Millisecond), time.Millisecond, 2},
		{at(12, 2, 0), time.Minute, 3},
	}
	prevGets := 0
	for _, st := range steps {
		before := e.provider()
		wait := e.syncAt(st.at)
		if wait != st.wantWait {
			t.Fatalf("Sync at %s returned a wait of %v, want %v", st.at.Format("15:04:05.000"), wait, st.wantWait)
		}
		if got := e.gets(); got != st.wantGets {
			t.Fatalf("after Sync at %s the gateway logged %d reads, want %d", st.at.Format("15:04:05.000"), got, st.wantGets)
		}
		if st.wantGets == prevGets && e.provider().ResourceVersion != before.ResourceVersion {
			t.Fatalf("Sync at %s read nothing and still wrote the Provider", st.at.Format("15:04:05.000"))
		}
		prevGets = st.wantGets
	}
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 2, 0))
}

func TestStartReadsOnceAMinuteAndStopsWithItsContext(t *testing.T) {
	fc := clocktesting.NewFakeClock(at(12, 0, 0))
	e := newEnv(t, withClock(fc), withModels(model("example-a")))
	if !e.ctl.NeedLeaderElection() {
		t.Fatal("NeedLeaderElection() = false, want true: only the holder of the Provider's Lease reads")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.ctl.Start(ctx) }()

	waitFor(t, "the first pass", fc.HasWaiters)
	if got := e.gets(); got != 1 {
		t.Fatalf("the gateway logged %d reads after Start's first pass, want 1", got)
	}
	// Ten seconds at a time over five minutes: a read at 12:00:00 and then at
	// each full minute, six in all.
	for i := 1; i <= 30; i++ {
		fc.Step(10 * time.Second)
		waitFor(t, "Start to wait again", fc.HasWaiters) // a pass ends before its wait begins
		wantGets := 1 + i*10/60
		if got := e.gets(); got != wantGets {
			t.Fatalf("after %d s the gateway logged %d reads, want %d", i*10, got, wantGets)
		}
		wantRead := at(12, 0, 0).Add(time.Duration(i*10/60) * time.Minute)
		wantMT(t, "lastCatalogRead after "+(time.Duration(i*10)*time.Second).String(), e.provider().Status.LastCatalogRead, wantRead)
	}
	if got := e.gets(); got != 6 {
		t.Fatalf("the gateway logged %d reads in five minutes, want exactly 6", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v after its context ended, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after its context ended")
	}
}

// A process that restarts must not read sooner than a minute after the last
// read or pass the Provider's stored status records. Stored times are whole
// seconds, so the floor is the stored time plus one second plus a minute.
func TestStoredStatusFloorsTheFirstReadOfANewController(t *testing.T) {
	for _, tc := range []struct {
		name            string
		reconcile, read *time.Time
		floor           time.Time
	}{
		{"lastReconcileTime 30 s ago", ptr(at(11, 59, 30)), nil, at(12, 0, 31)},
		{"lastCatalogRead 20 s ago", nil, ptr(at(11, 59, 40)), at(12, 0, 41)},
		{"both: the later one counts", ptr(at(11, 59, 50)), ptr(at(11, 59, 20)), at(12, 0, 51)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, withModels(model("example-a")))
			e.storeStatus(func(s *v1alpha1.ProviderStatus) {
				s.LastReconcileTime, s.LastCatalogRead = mtOrNil(tc.reconcile), mtOrNil(tc.read)
			})
			stored := e.provider().Status
			if (tc.reconcile != nil) != (stored.LastReconcileTime != nil) || (tc.read != nil) != (stored.LastCatalogRead != nil) {
				t.Fatalf("the stored status is %+v, the test did not set up what it meant to", stored)
			}

			before := e.provider()
			if wait := e.syncAt(at(12, 0, 0)); wait != tc.floor.Sub(at(12, 0, 0)) {
				t.Fatalf("Sync at 12:00:00 waited %v, want %v: the floor is %s", wait, tc.floor.Sub(at(12, 0, 0)), tc.floor.Format("15:04:05"))
			}
			if wait := e.syncAt(tc.floor.Add(-time.Second)); wait != time.Second {
				t.Fatalf("Sync a second before the floor waited %v, want 1s", wait)
			}
			if got := e.gets(); got != 0 {
				t.Fatalf("the gateway logged %d reads before the floor, want 0", got)
			}
			if e.provider().ResourceVersion != before.ResourceVersion {
				t.Fatal("a pass before the floor wrote the Provider")
			}
			e.syncAt(tc.floor)
			if got := e.gets(); got != 1 {
				t.Fatalf("the gateway logged %d reads at the floor, want 1", got)
			}
			wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, tc.floor)
		})
	}
}

// The restart as it happens: a second Controller on a Provider the first one
// has just read.
func TestSecondControllerWaitsOutTheFirstOnesRead(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")))
	e.syncAt(at(12, 0, 0))

	second, err := New(Options{Client: e.c, Provider: testProvider, Clock: e.clock})
	if err != nil {
		t.Fatal(err)
	}
	e.clock.Set(at(12, 0, 30))
	wait, err := second.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if wait != 31*time.Second || e.gets() != 1 {
		t.Fatalf("the second Controller waited %v with %d reads logged, want 31s and 1: stored lastReconcileTime 12:00:00 plus 61 s", wait, e.gets())
	}
	e.clock.Set(at(12, 1, 0))
	if _, err := second.Sync(context.Background()); err != nil || e.gets() != 1 {
		t.Fatalf("at 12:01:00 the second Controller read (reads %d, err %v), want it to wait for 12:01:01", e.gets(), err)
	}
	e.clock.Set(at(12, 1, 1))
	if _, err := second.Sync(context.Background()); err != nil || e.gets() != 2 {
		t.Fatalf("at 12:01:01 the second Controller did not read (reads %d, err %v)", e.gets(), err)
	}
}

// Start sleeps as long as the gated pass says, not a whole interval.
func TestStartSleepsOutTheStoredFloor(t *testing.T) {
	fc := clocktesting.NewFakeClock(at(12, 0, 0))
	e := newEnv(t, withClock(fc), withModels(model("example-a")))
	e.storeStatus(func(s *v1alpha1.ProviderStatus) { s.LastReconcileTime = mtOrNil(ptr(at(11, 59, 30))) }) // floor 12:00:31
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.ctl.Start(ctx) }()

	waitFor(t, "the gated first pass", fc.HasWaiters)
	if got := e.gets(); got != 0 {
		t.Fatalf("the gateway logged %d reads on a gated first pass, want 0", got)
	}
	fc.Step(30 * time.Second)
	if !fc.HasWaiters() || e.gets() != 0 {
		t.Fatalf("at 12:00:30 Start stopped waiting or read (reads %d), want it to wait for 12:00:31", e.gets())
	}
	fc.Step(time.Second)
	waitFor(t, "the read at the floor", func() bool { return e.gets() == 1 })
	waitFor(t, "Start to wait again", fc.HasWaiters)
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 0, 31))

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestNewNeedsAClientAndAProviderName(t *testing.T) {
	e := newEnv(t)
	if _, err := New(Options{Provider: testProvider}); err == nil {
		t.Fatal("New without a client = nil error")
	}
	if _, err := New(Options{Client: e.c}); err == nil {
		t.Fatal("New without a Provider name = nil error")
	}
}

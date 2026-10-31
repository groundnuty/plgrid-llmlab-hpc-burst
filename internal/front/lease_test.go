package front

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// HBEP-8, Observers and Gaps after a restart: the front renews a heartbeat
// Lease, "front" in hpcburst-system, every 10 s with leaseDurationSeconds 30;
// at start it reads the Lease's previous renewTime before taking it, and once
// it holds it raises one degraded event, reason ObservationGap, regarding the
// Lease and naming that renewTime. A first start, with no Lease, raises none.
// These tests run against client-go's fake clientset (not envtest), on the
// fake clock. The Lease's name, namespace, period and duration are literals
// from the HBEP.

const (
	leaseNS   = "hpcburst-system"
	leaseObj  = "front"
	leaseDesc = "*v1.Lease hpcburst-system/front" // how MemoryRecorder names the subject
)

// lkFix is a keeper on a fake clientset and a fake clock, running.
type lkFix struct {
	t      *testing.T
	cs     *kubefake.Clientset
	clk    *clock.Fake
	rec    *MemoryRecorder
	ref    *leaseRef
	logs   *syncBuffer
	cancel context.CancelFunc
	done   chan struct{}
}

func newLkFix(t *testing.T, prior *coordinationv1.Lease, reactors ...func(*kubefake.Clientset)) *lkFix {
	t.Helper()
	cs := kubefake.NewSimpleClientset()
	if prior != nil {
		if _, err := cs.CoordinationV1().Leases(leaseNS).Create(context.Background(), prior, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range reactors {
		r(cs)
	}
	f := &lkFix{t: t, cs: cs, clk: clock.NewFake(t0), rec: &MemoryRecorder{}, ref: &leaseRef{}, logs: &syncBuffer{}, done: make(chan struct{})}
	return f
}

func (f *lkFix) start() {
	f.t.Helper()
	log := slog.New(slog.NewTextHandler(f.logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	k := newLeaseKeeper(f.cs, f.clk, f.rec, f.ref, log)
	go func() { defer close(f.done); k.run(ctx) }()
	f.t.Cleanup(f.stop)
}

func (f *lkFix) stop() {
	f.cancel()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		f.t.Error("the keeper did not stop when its context ended")
	}
}

func (f *lkFix) lease() (*coordinationv1.Lease, error) {
	return f.cs.CoordinationV1().Leases(leaseNS).Get(context.Background(), leaseObj, metav1.GetOptions{})
}

// waitRenewTime waits until the Lease's renewTime is want.
func (f *lkFix) waitRenewTime(want time.Time) *coordinationv1.Lease {
	f.t.Helper()
	var got *coordinationv1.Lease
	errWaitFor(f.t, "the Lease's renewTime to be "+want.Format(time.RFC3339), func() bool {
		l, err := f.lease()
		got = l
		return err == nil && l.Spec.RenewTime != nil && l.Spec.RenewTime.Time.Equal(want)
	})
	return got
}

// beat advances the fake clock by one renewal period, once the keeper has
// armed its timer.
func (f *lkFix) beat() {
	f.t.Helper()
	errWaitFor(f.t, "the keeper to wait for its next beat", func() bool { return f.clk.Pending() == 1 })
	f.clk.Advance(10 * time.Second)
}

func gaps(rec *MemoryRecorder) []RecordedEvent { return rec.Find("degraded", "ObservationGap") }

// TestLeaseFirstStartCreatesItAndRenewsEvery10Seconds: with no Lease, the
// front creates "front" in hpcburst-system with leaseDurationSeconds 30 and
// renewTime now, and raises no ObservationGap; every 10 s on its clock it
// moves renewTime on.
func TestLeaseFirstStartCreatesItAndRenewsEvery10Seconds(t *testing.T) {
	f := newLkFix(t, nil)
	f.start()
	l := f.waitRenewTime(t0)
	if l.Namespace != "hpcburst-system" || l.Name != "front" {
		t.Errorf("Lease %s/%s, want hpcburst-system/front", l.Namespace, l.Name)
	}
	if l.Spec.LeaseDurationSeconds == nil || *l.Spec.LeaseDurationSeconds != 30 {
		t.Errorf("leaseDurationSeconds = %v, want 30", l.Spec.LeaseDurationSeconds)
	}
	if l.Spec.HolderIdentity == nil || !strings.HasPrefix(*l.Spec.HolderIdentity, "hpcburst-front") {
		t.Errorf("holderIdentity = %v, want the front's", l.Spec.HolderIdentity)
	}
	errWaitFor(t, "the front to hold the Lease", f.ref.held)
	f.beat()
	f.waitRenewTime(t0.Add(10 * time.Second))
	f.beat()
	f.waitRenewTime(t0.Add(20 * time.Second))
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("a first start raised %d ObservationGap events, want none: %+v", len(g), g)
	}
}

// TestLeaseRestartRaisesOneObservationGapNamingTheLastRenewTime: a Lease from
// an earlier run, last renewed at 11:55:00, is taken, and one degraded
// ObservationGap regards it, naming 11:55:00.000Z, the gap's start. The
// renewals that follow raise no more.
func TestLeaseRestartRaisesOneObservationGapNamingTheLastRenewTime(t *testing.T) {
	renewed := metav1.NewMicroTime(time.Date(2026, 10, 31, 11, 55, 0, 0, time.UTC))
	f := newLkFix(t, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: leaseNS, Name: leaseObj},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: strp("an-earlier-front"), RenewTime: &renewed},
	})
	f.start()
	f.waitRenewTime(t0)
	errWaitFor(t, "the ObservationGap event", func() bool { return len(gaps(f.rec)) > 0 })
	f.beat()
	f.waitRenewTime(t0.Add(10 * time.Second))
	f.beat()
	f.waitRenewTime(t0.Add(20 * time.Second))
	g := gaps(f.rec)
	if len(g) != 1 {
		t.Fatalf("%d ObservationGap events after two renewals, want 1: %+v", len(g), g)
	}
	if !g[0].Warning || g[0].Object != leaseDesc || !strings.Contains(g[0].Message, "2026-10-31T11:55:00.000Z") {
		t.Errorf("event %+v: want a warning regarding %s naming 2026-10-31T11:55:00.000Z", g[0], leaseDesc)
	}
}

// TestLeaseWithNoRenewTimeRaisesNoGap: a Lease that was never renewed names
// no gap start, so there is nothing to say.
func TestLeaseWithNoRenewTimeRaisesNoGap(t *testing.T) {
	f := newLkFix(t, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: leaseNS, Name: leaseObj}})
	f.start()
	f.waitRenewTime(t0)
	f.beat()
	f.waitRenewTime(t0.Add(10 * time.Second))
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("a Lease with no renewTime raised %d events, want none: %+v", len(g), g)
	}
}

// TestLeaseFailuresAreNotFatalAndAreRetried: the Lease cannot be created
// (the launcher has not made the namespace yet, say). The keeper does not
// stop; the failure is logged once for a run of the same error, not on every
// beat; the next beat after the obstacle is gone takes the Lease.
func TestLeaseFailuresAreNotFatalAndAreRetried(t *testing.T) {
	var mu sync.Mutex
	failing := true
	f := newLkFix(t, nil, func(cs *kubefake.Clientset) {
		cs.PrependReactor("create", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, leaseNS)
			}
			return false, nil, nil
		})
	})
	f.start()
	for range 3 {
		errWaitFor(t, "the keeper to wait for its next beat", func() bool { return f.clk.Pending() == 1 })
		f.clk.Advance(10 * time.Second)
	}
	if f.ref.held() {
		t.Error("the front reports holding a Lease it could not create")
	}
	if _, err := f.lease(); !apierrors.IsNotFound(err) {
		t.Errorf("Lease Get = %v, want not found", err)
	}
	if n := strings.Count(f.logs.String(), "the front's Lease is not held"); n != 1 {
		t.Errorf("the same failure was logged %d times in 4 beats, want once:\n%s", n, f.logs.String())
	}
	mu.Lock()
	failing = false
	mu.Unlock()
	f.beat()
	errWaitFor(t, "the Lease to be created once the obstacle is gone", func() bool { _, err := f.lease(); return err == nil })
	errWaitFor(t, "the front to hold the Lease", f.ref.held)
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("a Lease created late raised %d ObservationGap events, want none: %+v", len(g), g)
	}
}

// TestLeaseDeletedWhileRunningIsTakenAgainWithNoGap: someone deletes the
// Lease. The next renewal fails, and the beat after takes it again. That is
// not a restart: no ObservationGap.
func TestLeaseDeletedWhileRunningIsTakenAgainWithNoGap(t *testing.T) {
	f := newLkFix(t, nil)
	f.start()
	f.waitRenewTime(t0)
	if err := f.cs.CoordinationV1().Leases(leaseNS).Delete(context.Background(), leaseObj, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	f.beat() // the renewal finds it gone
	errWaitFor(t, "the front to see it does not hold the Lease", func() bool { return !f.ref.held() })
	f.beat() // takes it again
	f.waitRenewTime(t0.Add(20 * time.Second))
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("taking a deleted Lease again raised %d ObservationGap events, want none: %+v", len(g), g)
	}
}

// TestLeaseConflictOnRenewalIsTakenAgainWithNoGap: a renewal that meets a
// conflict (the Lease changed under the front) is read and taken again at the
// next beat. That Lease holds the front's own last renewTime, and it is no
// restart: no ObservationGap.
func TestLeaseConflictOnRenewalIsTakenAgainWithNoGap(t *testing.T) {
	var mu sync.Mutex
	conflicts := 0
	f := newLkFix(t, nil, func(cs *kubefake.Clientset) {
		cs.PrependReactor("update", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if conflicts > 0 {
				conflicts--
				return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, leaseObj, errors.New("the object has been modified"))
			}
			return false, nil, nil
		})
	})
	f.start()
	f.waitRenewTime(t0)
	mu.Lock()
	conflicts = 1
	mu.Unlock()
	f.beat() // the renewal conflicts
	errWaitFor(t, "the front to see it does not hold the Lease", func() bool { return !f.ref.held() })
	f.beat() // reads the Lease again and takes it
	f.waitRenewTime(t0.Add(20 * time.Second))
	errWaitFor(t, "the front to hold the Lease again", f.ref.held)
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("taking the Lease again after a conflict raised %d ObservationGap events, want none: %+v", len(g), g)
	}
}

// TestUsageWriteFailedRegardsTheLeaseOnceHeld: before the front holds its
// Lease the event regards the failed request's ModelDeployment; once it
// holds it, the event regards the Lease (HBEP-8, Events).
func TestUsageWriteFailedRegardsTheLeaseOnceHeld(t *testing.T) {
	h := newScFix(t, scOpts{})
	boom := errors.New("write: no space left on device")
	h.router.usageFailed(scDep, "r1", boom)
	first := h.rec.Find(eventFailing, reasonUsageWriteFailed)
	if len(first) != 1 || !strings.Contains(first[0].Object, "ns/md") {
		t.Fatalf("before the Lease: %+v, want one event regarding ns/md", first)
	}

	cs := kubefake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	k := newLeaseKeeper(cs, h.clk, h.rec, h.router.lease, discardLog())
	go func() { defer close(done); k.run(ctx) }()
	defer func() { cancel(); <-done }()
	errWaitFor(t, "the front to hold its Lease", h.router.lease.held)

	h.clk.Advance(time.Hour) // the event repeats hourly
	h.router.usageFailed(scDep, "r2", boom)
	all := h.rec.Find(eventFailing, reasonUsageWriteFailed)
	if len(all) != 2 || all[1].Object != leaseDesc || !all[1].Warning {
		t.Fatalf("after the Lease: %+v, want a second event, a warning regarding %s", all, leaseDesc)
	}
}

// kernelListener is a loopback listener on a port the kernel chooses.
func kernelListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// TestRunTakesTheLeaseAndRaisesTheGapAtStart: Run, given an API client, takes
// the front's Lease at start and raises the ObservationGap of a restart,
// naming the Lease's previous renewTime; it reports that on the event
// recorder it was given and leaves the keeper stopped when it returns.
func TestRunTakesTheLeaseAndRaisesTheGapAtStart(t *testing.T) {
	renewed := metav1.NewMicroTime(time.Date(2026, 10, 31, 11, 55, 0, 0, time.UTC))
	cs := kubefake.NewSimpleClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: leaseNS, Name: leaseObj},
		Spec:       coordinationv1.LeaseSpec{RenewTime: &renewed},
	})
	rec := &MemoryRecorder{}
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
	logs := &syncBuffer{}
	o := Options{
		Config:           Config{ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage")}.WithDefaults(),
		Dynamic:          fakeWatchClient(),
		Kube:             cs,
		Recorder:         rec,
		Log:              slog.New(slog.NewTextHandler(logs, nil)),
		ClientListener:   kernelListener(t),
		DialHomeListener: kernelListener(t),
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- Run(ctx, o) }()
	errWaitFor(t, "the ObservationGap event", func() bool { return len(gaps(rec)) > 0 })
	g := gaps(rec)[0]
	if g.Object != leaseDesc || !strings.Contains(g.Message, "2026-10-31T11:55:00.000Z") {
		t.Errorf("event %+v: want one regarding %s naming 2026-10-31T11:55:00.000Z", g, leaseDesc)
	}
	l, err := cs.CoordinationV1().Leases(leaseNS).Get(context.Background(), leaseObj, metav1.GetOptions{})
	if err != nil || l.Spec.RenewTime == nil || !l.Spec.RenewTime.Time.After(renewed.Time) {
		t.Errorf("Lease after start = %+v (%v), want renewTime after the previous one", l, err)
	}
	cancel()
	select {
	case err := <-ran:
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
}

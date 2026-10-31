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

// leasesGVR is the resource the fake clientset's tracker stores Leases under.
var leasesGVR = schema.GroupVersionResource{Group: "coordination.k8s.io", Version: "v1", Resource: "leases"}

// commitThenFail makes the first matching write ("create" or "update" of a
// Lease) reach the store and then return an error, as a write does whose
// response is lost: the Lease is changed and the caller cannot know.
func commitThenFail(verb string, armed *bool, mu *sync.Mutex) func(*kubefake.Clientset) {
	return func(cs *kubefake.Clientset) {
		cs.PrependReactor(verb, "leases", func(a clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if !*armed {
				return false, nil, nil
			}
			*armed = false
			obj := a.(clienttesting.CreateAction).GetObject()
			var err error
			if verb == "create" {
				err = cs.Tracker().Create(leasesGVR, obj, leaseNS)
			} else {
				err = cs.Tracker().Update(leasesGVR, obj, leaseNS)
			}
			if err != nil {
				return true, nil, err
			}
			return true, nil, errors.New("connection reset by peer: the response was lost")
		})
	}
}

// TestObservationGapKeepsTheStartsObservationWhenATakeoverIsUncertain (PR 117
// review, Astra B3): the Lease was last renewed at 11:00. The front's takeover
// update at 12:00 commits, but its response is lost. The retry reads the
// front's own 12:00 heartbeat; the gap still starts at 11:00, the observation
// made before the first write, not at 12:00, which would hide the outage.
func TestObservationGapKeepsTheStartsObservationWhenATakeoverIsUncertain(t *testing.T) {
	var mu sync.Mutex
	armed := true
	renewed := metav1.NewMicroTime(time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC))
	f := newLkFix(t, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: leaseNS, Name: leaseObj},
		Spec:       coordinationv1.LeaseSpec{RenewTime: &renewed},
	}, commitThenFail("update", &armed, &mu))
	f.start()
	f.waitRenewTime(t0) // the takeover committed
	errWaitFor(t, "the lost response to be logged", func() bool { return strings.Contains(f.logs.String(), "response was lost") })
	if g := gaps(f.rec); len(g) != 0 {
		t.Fatalf("an uncertain takeover raised %d events before the retry: %+v", len(g), g)
	}
	f.beat() // the retry reads the front's own 12:00 heartbeat, and takes the Lease
	f.waitRenewTime(t0.Add(10 * time.Second))
	errWaitFor(t, "the ObservationGap event", func() bool { return len(gaps(f.rec)) > 0 })
	g := gaps(f.rec)
	if len(g) != 1 || !strings.Contains(g[0].Message, "2026-10-31T11:00:00.000Z") || strings.Contains(g[0].Message, "2026-10-31T12:00:00.000Z") {
		t.Errorf("events %+v: want exactly one naming 2026-10-31T11:00:00.000Z, the observation before the first write", g)
	}
}

// TestFirstStartRaisesNoGapWhenTheCreateIsUncertain (PR 117 review, Astra
// B3): there was no Lease. The create commits, but its response is lost; the
// retry finds the front's own Lease. That is still a first start: no gap.
func TestFirstStartRaisesNoGapWhenTheCreateIsUncertain(t *testing.T) {
	var mu sync.Mutex
	armed := true
	f := newLkFix(t, nil, commitThenFail("create", &armed, &mu))
	f.start()
	f.waitRenewTime(t0) // the create committed
	errWaitFor(t, "the lost response to be logged", func() bool { return strings.Contains(f.logs.String(), "response was lost") })
	f.beat() // the retry finds its own Lease and takes it
	f.waitRenewTime(t0.Add(10 * time.Second))
	errWaitFor(t, "the front to hold the Lease", f.ref.held)
	f.beat()
	f.waitRenewTime(t0.Add(20 * time.Second))
	if g := gaps(f.rec); len(g) != 0 {
		t.Errorf("an uncertain create raised %d ObservationGap events, want none: %+v", len(g), g)
	}
}

// usageKeeper runs a keeper on its own fake clock over the harness's router,
// as Run wires it: the router's Lease reference, and its event flush after each
// beat that holds the Lease.
func usageKeeper(t *testing.T, h *scFix, reactors ...func(*kubefake.Clientset)) (*clock.Fake, func()) {
	t.Helper()
	cs := kubefake.NewSimpleClientset()
	for _, r := range reactors {
		r(cs)
	}
	clk := clock.NewFake(t0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	k := newLeaseKeeper(cs, clk, h.rec, h.router.lease, discardLog())
	k.onHeld = h.router.flushUsageEvent
	go func() { defer close(done); k.run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return clk, func() {
		errWaitFor(t, "the keeper to wait for its next beat", func() bool { return clk.Pending() == 1 })
		clk.Advance(10 * time.Second)
	}
}

func usageEvents(h *scFix) []RecordedEvent { return h.rec.Find(eventFailing, reasonUsageWriteFailed) }

// TestUsageWriteFailedWaitsForTheLeaseAndFiresOnce (@hpc-burst-science-agent,
// 31 Oct 2026): usage fails before the front holds its Lease. There is no
// fallback to a ModelDeployment: the failure is only counted, logged and shown
// in /healthz, and no event goes out. When the Lease is taken, exactly one
// UsageWriteFailed event follows, regarding the Lease; more beats raise no
// more; an hour on, with no new request, the failure that lasts raises its
// next; a write that succeeds ends the failure, so the next one raises its
// event at once.
func TestUsageWriteFailedWaitsForTheLeaseAndFiresOnce(t *testing.T) {
	h := newScFix(t, scOpts{})
	boom := errors.New("write: no space left on device")
	h.router.usageFailed("r1", boom)
	h.router.usageFailed("r2", boom)
	if ev := usageEvents(h); len(ev) != 0 {
		t.Fatalf("before the Lease, %d UsageWriteFailed events: %+v; want none, and no ModelDeployment fallback", len(ev), ev)
	}

	_, beat := usageKeeper(t, h)
	errWaitFor(t, "the one event once the Lease is held", func() bool { return len(usageEvents(h)) == 1 })
	ev := usageEvents(h)[0]
	if ev.Object != leaseDesc || !ev.Warning || !strings.Contains(ev.Message, "no space left") || !strings.Contains(ev.Message, "r2") {
		t.Errorf("event %+v: want a warning regarding %s, naming the latest failure (request r2)", ev, leaseDesc)
	}
	beat()
	beat()
	if n := len(usageEvents(h)); n != 1 {
		t.Fatalf("%d events after two more beats, want still 1: the hourly gate", n)
	}

	h.clk.Advance(time.Hour) // the harness's clock is the router's
	beat()
	errWaitFor(t, "the hourly repeat", func() bool { return len(usageEvents(h)) == 2 })

	h.router.usageWritten()
	h.router.usageFailed("r3", boom)
	if n := len(usageEvents(h)); n != 3 {
		t.Errorf("%d events after a new failure that follows a success, want 3: it raises its event at once", n)
	}
	for i, e := range usageEvents(h) {
		if e.Object != leaseDesc {
			t.Errorf("event %d regards %q, want %s", i, e.Object, leaseDesc)
		}
	}
}

// TestRenewalAfterATransientFailureHoldsTheLeaseAgain (PR 117 review, Astra
// B1): a renewal fails with an error that leaves the front's copy of the
// Lease intact, so the next beat renews it, not takes it. That success must
// restore "held": a UsageWriteFailed raised while the renewals failed goes out
// regarding the Lease as soon as one succeeds, even though no request has a
// ModelDeployment to fall back on.
func TestRenewalAfterATransientFailureHoldsTheLeaseAgain(t *testing.T) {
	h := newScFix(t, scOpts{})
	var mu sync.Mutex
	failing := false
	_, beat := usageKeeper(t, h, func(cs *kubefake.Clientset) {
		cs.PrependReactor("update", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return true, nil, errors.New("etcdserver: request timed out")
			}
			return false, nil, nil
		})
	})
	errWaitFor(t, "the front to hold the Lease", h.router.lease.held)

	mu.Lock()
	failing = true
	mu.Unlock()
	beat() // the renewal fails; the front's copy of the Lease is intact
	errWaitFor(t, "the front to see it does not hold the Lease", func() bool { return !h.router.lease.held() })
	h.router.usageFailed("r1", errors.New("write: no space left on device"))
	if n := len(usageEvents(h)); n != 0 {
		t.Fatalf("%d events while the Lease is not held, want none", n)
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	beat() // renews, which holds the Lease again
	errWaitFor(t, "the front to hold the Lease again", h.router.lease.held)
	errWaitFor(t, "the event regarding the Lease", func() bool { return len(usageEvents(h)) == 1 })
	if ev := usageEvents(h)[0]; ev.Object != leaseDesc {
		t.Errorf("event regards %q, want %s", ev.Object, leaseDesc)
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

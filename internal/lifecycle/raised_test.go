package lifecycle

// HBEP-8's raised-events counter in the lifecycle controller (Metrics,
// Controller metrics, Raised events; Test plan, Raised events). Every rig of
// this package raises its events through the production chain, KubeRecorder
// over eventcount.Events, into a recorder that keeps them for the tests
// (memoryEvents); each rig checks at its end that every event it raised was
// counted once, with its controller, action and type, and was counted before
// the recorder received it. The absence tests use the hooks of the loops that
// have no pass marker: the observer and the gap emitter.

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/leaderelection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// memoryEvents stands for client-go's events.k8s.io recorder under the
// counting wrapper: it hands each event to a MemoryRecorder, and notes how
// many events of each series it has received and whether the count had risen
// to include each one when it arrived.
type memoryEvents struct {
	m *MemoryRecorder
	c *eventcount.Counter

	mu     sync.Mutex
	handed map[eventcount.Key]float64
	early  []string // events that arrived before the count included them
}

func newMemoryEvents(m *MemoryRecorder, c *eventcount.Counter) *memoryEvents {
	return &memoryEvents{m: m, c: c, handed: map[eventcount.Key]float64{}}
}

// Eventf implements events.EventRecorder.
func (e *memoryEvents) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reason, Action: action, Type: eventtype}
	e.mu.Lock()
	e.handed[k]++
	n := e.handed[k]
	e.mu.Unlock()
	// Events of one series may arrive concurrently, so the count is at least
	// the number received so far, this one included.
	if got, _ := raisedCount(e.c, k); got < n {
		e.mu.Lock()
		e.early = append(e.early, fmt.Sprintf("%s arrived with the count at %g, below the %g received", k, got, n))
		e.mu.Unlock()
	}
	var rel client.Object
	if r, ok := related.(client.Object); ok {
		rel = r
	}
	e.m.Event(regarding.(client.Object), rel, action, reason, fmt.Sprintf(note, args...))
}

// check fails unless every event received was counted exactly once with its
// labels, nothing else was counted, and no event arrived before its count.
func (e *memoryEvents) check(t *testing.T) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.early {
		t.Errorf("raised events: %s", s)
	}
	counted := allRaised(t, e.c)
	for k, n := range e.handed {
		if counted[k] != n {
			t.Errorf("raised events: %s was handed to the recorder %g times and counted %g times", k, n, counted[k])
		}
	}
	for k, n := range counted {
		if n != 0 && e.handed[k] == 0 {
			t.Errorf("raised events: %s counted %g times, never handed to the recorder", k, n)
		}
	}
}

// raisedCount reads one series of the counter through a registry, as a scrape
// would: reading it through the counter would create it.
func raisedCount(c *eventcount.Counter, k eventcount.Key) (float64, bool) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		return 0, false
	}
	fams, err := reg.Gather()
	if err != nil {
		return 0, false
	}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			if labelsOf(m.GetLabel()) == k {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func labelsOf(ls []*dto.LabelPair) eventcount.Key {
	var k eventcount.Key
	for _, l := range ls {
		switch l.GetName() {
		case eventcount.LabelController:
			k.Controller = l.GetValue()
		case eventcount.LabelReason:
			k.Reason = l.GetValue()
		case eventcount.LabelAction:
			k.Action = l.GetValue()
		case eventcount.LabelType:
			k.Type = l.GetValue()
		}
	}
	return k
}

// allRaised returns every series of the counter with its count.
func allRaised(t *testing.T, c *eventcount.Counter) map[eventcount.Key]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[eventcount.Key]float64{}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			out[labelsOf(m.GetLabel())] = m.GetCounter().GetValue()
		}
	}
	return out
}

// TestARigCountsWhatItRaises: a rig whose pass raises events counts each one,
// under the lifecycle controller, with the action and type it carries; the
// rig's end checks the totals. A Provider read once, then silent past
// ProviderReadyStaleness, raises failing/HeartbeatMissed, a Warning.
func TestARigCountsWhatItRaises(t *testing.T) {
	rg := newRig(t)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reasonHeartbeatMissed, Action: eventFailing, Type: provider.EventKindWarning}
	if n, ok := raisedCount(rg.events, k); ok || n != 0 {
		t.Fatalf("%s reads %g (present %v) before any event; the rig creates no series at 0", k, n, ok)
	}
	rg.clock.Advance(6 * time.Minute)
	rg.checkHeartbeats()
	if n, _ := raisedCount(rg.events, k); n != 1 || len(rg.rec.Find(eventFailing, reasonHeartbeatMissed)) != 1 {
		t.Fatalf("%s = %g with %d events recorded, want 1 and 1", k, n, len(rg.rec.Find(eventFailing, reasonHeartbeatMissed)))
	}
}

// metricsRead reads a registry's /metrics the way a scraper does, and parses
// it for one series of the counter.
func metricsRead(t *testing.T, reg *prometheus.Registry, k eventcount.Key) eventcount.Read {
	t.Helper()
	sent := time.Now()
	w := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	answered := time.Now()
	r, err := eventcount.ParseRead(w.Body.Bytes(), k)
	if err != nil {
		t.Fatal(err)
	}
	r.Sent, r.Answered = sent, answered
	return r
}

// observedPass is one observer pass its hook reported: when it began, when
// the hook signalled, and the pass's outcome.
type observedPass struct {
	began, ended time.Time
	err          error
}

// passLog records the passes a hook reports, and waits for them.
type passLog struct {
	mu     sync.Mutex
	passes []observedPass
	more   chan struct{}
}

func newPassLog() *passLog { return &passLog{more: make(chan struct{}, 1)} }

func (l *passLog) add(began time.Time, err error) {
	l.mu.Lock()
	l.passes = append(l.passes, observedPass{began: began, ended: time.Now(), err: err})
	l.mu.Unlock()
	select {
	case l.more <- struct{}{}:
	default:
	}
}

// barrierAfter waits for the passes that began at or after at, and returns the
// first of them that read what it judges (a nil error) as the barrier. A pass
// whose read failed judged nothing, so it is no barrier (HBEP-8, Test plan,
// Raised events: a barrier that shows only that the emitting read completed
// proves nothing). When the first n such passes all failed, it returns a
// barrier with no pass, which makes the check inconclusive: it waits on the
// count of passes, never on a window.
func (l *passLog) barrierAfter(t *testing.T, at time.Time, what string, n int) eventcount.Barrier {
	t.Helper()
	deadline := time.After(time.Minute)
	for {
		l.mu.Lock()
		var failed []error
		for _, p := range l.passes {
			if p.began.Before(at) {
				continue
			}
			if p.err == nil {
				l.mu.Unlock()
				return eventcount.Barrier{What: what, PassStart: p.began, Observed: p.ended}
			}
			failed = append(failed, p.err)
		}
		seen := len(l.passes)
		l.mu.Unlock()
		if len(failed) >= n {
			return eventcount.Barrier{What: fmt.Sprintf("%s: none of the %d passes since %s read what it judges (first: %v)", what, len(failed), at.Format(time.RFC3339Nano), failed[0])}
		}
		select {
		case <-l.more:
		case <-deadline:
			t.Fatalf("no %s began after %s within a minute (%d passes seen, %d failed)", what, at.Format(time.RFC3339Nano), seen, len(failed))
		}
	}
}

// barrierPasses is how many failed passes in a row make an observer barrier
// inconclusive.
const barrierPasses = 5

// TestObserverAbsenceOfHeartbeatMissed proves, by the four conditions, that
// the observer raised no HeartbeatMissed for a Provider whose read is still
// within ProviderReadyStaleness, and then that it raised one once the read is
// past it. The reads go through the serving collector, as the copy's /metrics
// would; the barrier is the observer's hook, after a pass that began after the
// change (HBEP-8, Raised events, condition 3).
func TestObserverAbsenceOfHeartbeatMissed(t *testing.T) {
	rg := newRig(t)
	rg.events.Zero(eventcount.ControllerLifecycle, RaisedEvents())
	reg := prometheus.NewRegistry()
	reg.MustRegister(newStateCollector(rg.c, rg.backend, rg.clock, func() time.Time { return rigStart }, func() bool { return true }, rg.events),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reasonHeartbeatMissed, Action: eventFailing, Type: provider.EventKindWarning}
	quiet := "the rig runs no reconcile pass, has no Instance and no front Lease, so the observer's Provider judgement is the one source of HeartbeatMissed"
	first := metricsRead(t, reg, k)

	// The change: the Provider's read ages to 4 min, within the 5 min rule.
	rg.clock.Advance(4 * time.Minute)
	change := time.Now()
	log := newPassLog()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = (&observer{r: rg.r, every: 20 * time.Millisecond, onPass: log.add}).Start(ctx)
		close(done)
	}()
	defer func() { cancel(); <-done }()
	b := log.barrierAfter(t, change, "an observer pass over the Provider", barrierPasses)
	b.Due = change
	second := metricsRead(t, reg, k)
	a := eventcount.Absence{Key: k, First: first, Second: second, Change: change, Collectors: eventcount.LifecycleCollectors,
		Barriers: []eventcount.Barrier{b}, Quiet: quiet}
	if err := a.Require(); err != nil {
		t.Fatal(err)
	}

	// The control: past 5 min the same pass raises it, and the same check says so.
	rg.clock.Advance(2 * time.Minute)
	change = time.Now()
	b = log.barrierAfter(t, change, "an observer pass over the stale Provider", barrierPasses)
	b.Due = change
	third := metricsRead(t, reg, k)
	a = eventcount.Absence{Key: k, First: second, Second: third, Change: change, Collectors: eventcount.LifecycleCollectors,
		Barriers: []eventcount.Barrier{b}, Quiet: quiet}
	if v, why := a.Judge(); v != eventcount.Raised {
		t.Fatalf("a stale Provider: %s (%s), want raised", v, why)
	}
}

// TestObserverAbsenceNeedsASuccessfulRead: an observer pass whose Provider
// list fails judged no Provider, so it is no barrier, and the absence check
// across such passes is inconclusive, never "not raised", although the count
// did not rise. Here the Provider is stale through the failing passes, so
// HeartbeatMissed is due and a check that took a failed pass as its barrier
// would pass on an event that was never judged. Once the list reads again, the
// next pass raises it, and the same check says raised. The counter is read
// with the process collector, registered on their own: no serving predicate
// is injected.
func TestObserverAbsenceNeedsASuccessfulRead(t *testing.T) {
	var failing atomic.Bool
	rg := newRig(t, withInterceptors(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1alpha1.ProviderList); ok && failing.Load() {
				return errors.New("the Provider list fails (injected)")
			}
			return c.List(ctx, list, opts...)
		},
	}))
	rg.events.Zero(eventcount.ControllerLifecycle, RaisedEvents())
	reg := prometheus.NewRegistry()
	reg.MustRegister(rg.events, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reasonHeartbeatMissed, Action: eventFailing, Type: provider.EventKindWarning}
	quiet := "the rig runs no reconcile pass, has no Instance and no front Lease, so the observer's Provider judgement is the one source of HeartbeatMissed"
	first := metricsRead(t, reg, k)

	// The change: the Provider's read ages past the 5 min rule while its
	// list fails.
	failing.Store(true)
	rg.clock.Advance(6 * time.Minute)
	change := time.Now()
	log := newPassLog()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = (&observer{r: rg.r, every: 20 * time.Millisecond, onPass: log.add}).Start(ctx)
		close(done)
	}()
	defer func() { cancel(); <-done }()
	b := log.barrierAfter(t, change, "an observer pass over the Provider", barrierPasses)
	b.Due = change
	second := metricsRead(t, reg, k)
	if second.Count != first.Count {
		t.Fatalf("%s rose from %g to %g while the Provider list failed", k, first.Count, second.Count)
	}
	a := eventcount.Absence{Key: k, First: first, Second: second, Change: change, Barriers: []eventcount.Barrier{b}, Quiet: quiet}
	if v, why := a.Judge(); v != eventcount.Inconclusive {
		t.Fatalf("passes whose Provider list failed: %s (%s), want inconclusive", v, why)
	}

	// The control: the list reads again, and the next pass raises it.
	failing.Store(false)
	change = time.Now()
	b = log.barrierAfter(t, change, "an observer pass over the stale Provider", barrierPasses)
	b.Due = change
	third := metricsRead(t, reg, k)
	a = eventcount.Absence{Key: k, First: second, Second: third, Change: change, Barriers: []eventcount.Barrier{b}, Quiet: quiet}
	if v, why := a.Judge(); v != eventcount.Raised {
		t.Fatalf("a stale Provider read again: %s (%s), want raised", v, why)
	}
}

// leadOnce runs client-go's elector over the lock until it leads, starting the
// gap emitter in OnStartedLeading as controller-runtime starts its leader
// runnables, and returns once the emitter's hook has signalled.
func leadOnce(t *testing.T, mem *memLock, em *gapEmitter, signalled <-chan time.Time) time.Time {
	t.Helper()
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: em.lock, LeaseDuration: 600 * time.Millisecond, RenewDeadline: 400 * time.Millisecond, RetryPeriod: 100 * time.Millisecond,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) { _ = em.Start(ctx) },
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { le.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case at := <-signalled:
		return at
	case <-time.After(time.Minute):
		t.Fatalf("the gap emitter did not signal within a minute; lock calls %+v", mem.opsCopy())
		return time.Time{}
	}
}

// TestGapEmitterAbsenceOfObservationGap proves, by the four conditions, that a
// first start, with no previous Lease, raised no ObservationGap, and that a
// start after a previous holder's Lease raised one. The emitter has no pass
// marker: its hook signals once its decision is made and any event handed
// over. The counter is read with the process collector, from this process.
func TestGapEmitterAbsenceOfObservationGap(t *testing.T) {
	k := eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: reasonObservationGap, Action: eventDegraded, Type: provider.EventKindWarning}
	quiet := "only the gap emitter raises ObservationGap, and this test runs no other"
	for _, tc := range []struct {
		name     string
		previous bool
		want     eventcount.Verdict
	}{{"a first start", false, eventcount.NotRaised}, {"a start after another holder", true, eventcount.Raised}} {
		t.Run(tc.name, func(t *testing.T) {
			c := eventcount.New()
			c.Zero(eventcount.ControllerLifecycle, RaisedEvents())
			reg := prometheus.NewRegistry()
			reg.MustRegister(c, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
			mem := &memLock{id: "lifecycle-new", seq: &Sequence{}}
			if tc.previous {
				mem.write("lifecycle-old", time.Now().Add(-time.Minute))
			}
			signalled := make(chan time.Time, 1)
			rec := KubeRecorder{R: eventcount.Events(newMemoryEvents(&MemoryRecorder{}, c), c, eventcount.ControllerLifecycle)}
			em := &gapEmitter{lock: &gapLock{Interface: mem}, rec: rec, onDone: func() { signalled <- time.Now() }}
			first := metricsRead(t, reg, k)
			change := time.Now() // the elector, and so the emitter's start, begins after it
			observed := leadOnce(t, mem, em, signalled)
			second := metricsRead(t, reg, k)
			a := eventcount.Absence{Key: k, First: first, Second: second, Change: change,
				Barriers: []eventcount.Barrier{{What: "the gap emitter's start", PassStart: change, Observed: observed}}, Quiet: quiet}
			if v, why := a.Judge(); v != tc.want {
				t.Fatalf("%s (%s), want %s", v, why, tc.want)
			}
		})
	}
}

// TestRaisedEventsTypesFollowTheAction: each series the lifecycle controller
// creates at 0 has the Kubernetes type KubeRecorder gives its action, and no
// series is listed twice.
func TestRaisedEventsTypesFollowTheAction(t *testing.T) {
	seen := map[string]bool{}
	var keys []string
	for _, s := range RaisedEvents() {
		id := s.Action + "/" + s.Reason
		if seen[id] {
			t.Errorf("%s is listed twice", id)
		}
		seen[id] = true
		keys = append(keys, id)
		want := provider.EventKindOrdinary
		if s.Action == eventDegraded || s.Action == eventFailing {
			want = provider.EventKindWarning
		}
		if s.Type != want {
			t.Errorf("%s has type %s, want %s", id, s.Type, want)
		}
	}
	sort.Strings(keys)
	t.Logf("%d series: %v", len(keys), keys)
}

package lifecycle

// The KV-skew signal's question-free parts (#518): the skew read from one report's snapshot of every
// worker, only on fresh gauges and only against a sibling the router routes to; the per-process clock,
// advanced once per report and stopped by a report that is not skewed; and the signal, due
// kvSkewTimeout after the clock's start, floored at the controller's start. The thresholds await the
// science agent's ruling (kvSkewFullAt), and the signal is not yet asked by any worker's rung.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// skewGauges is one worker's gauges in a test report; a nil field is unknown.
type skewGauges struct {
	running, waiting *int64
	kv               *float64
	stale            bool // its metrics were last read three probe intervals before the report
}

func gauges(running, waiting int64, kv float64) skewGauges {
	return skewGauges{running: agentapi.Int64(running), waiting: agentapi.Int64(waiting), kv: agentapi.Float64(kv)}
}

const skewProbe = 10 * time.Second

// skewInstance is an Instance of three workers, each warmed up and router-admitted at generation 0, but
// those named in unadmitted.
func skewInstance(unadmitted ...string) *v1alpha1.Instance {
	in := &v1alpha1.Instance{}
	in.UID = "11111111-2222-3333-4444-555555555555"
	three := int32(3)
	in.Spec.Launch.Workers = &three
	g := int64(0)
	for _, w := range []string{"worker-0", "worker-1", "worker-2"} {
		rec, _ := workerRecordName(in, w)
		e := v1alpha1.InstanceWorker{Name: w, Record: rec, Verdict: string(v1alpha1.VerdictPass), WarmedUp: true, VerifiedGeneration: &g, RouterAdmittedGeneration: &g}
		for _, u := range unadmitted {
			if u == w {
				e.RouterAdmittedGeneration = nil
			}
		}
		in.Status.Workers = append(in.Status.Workers, e)
	}
	return in
}

// skewView is the agentView of a report at now with each worker's gauges and the router's view: routed
// names the workers its running router holds healthy; a worker absent from it is held unhealthy.
func skewView(now time.Time, ws map[string]skewGauges, routed map[string]bool, routerRunning bool) agentView {
	ir := &agentapi.InstanceReport{}
	rr := &agentapi.RouterReport{Phase: agentapi.PartExited}
	if routerRunning {
		rr.Phase = agentapi.PartRunning
	}
	for _, name := range []string{"worker-0", "worker-1", "worker-2"} {
		g, ok := ws[name]
		if !ok {
			continue
		}
		read := now
		if g.stale {
			read = now.Add(-3 * skewProbe)
		}
		ir.Workers = append(ir.Workers, agentapi.WorkerReport{Name: name, PID: 100 + len(ir.Workers), Phase: agentapi.PartRunning,
			Health: &agentapi.Health{Time: now, ProbeOK: true, MetricsReadAt: &read, RunningRequests: g.running, WaitingRequests: g.waiting, KVCacheUsage: g.kv}})
		rr.Workers = append(rr.Workers, agentapi.RouterWorker{Name: name, Healthy: routed[name]})
	}
	ir.Router = rr
	return agentView{report: ir, reportAt: now, reportTime: now, now: now, probeInterval: skewProbe}
}

func TestWhenAWorkerIsKVSkewed(t *testing.T) {
	now := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	full, idle, busy := gauges(8, 3, 0.97), gauges(0, 0, 0.05), gauges(4, 0, 0.6)
	both := map[string]bool{"worker-0": true, "worker-1": true, "worker-2": true}
	for _, c := range []struct {
		name   string
		ws     map[string]skewGauges
		routed map[string]bool
		router bool
		want   bool
	}{
		{"worker-1 full with requests waiting, worker-0 idle and routed", map[string]skewGauges{"worker-0": idle, "worker-1": full}, both, true, true},
		{"the sibling busy", map[string]skewGauges{"worker-0": busy, "worker-1": full}, both, true, false},
		{"the sibling with one waiting", map[string]skewGauges{"worker-0": gauges(0, 1, 0.1), "worker-1": full}, both, true, false},
		{"both full", map[string]skewGauges{"worker-0": full, "worker-1": full}, both, true, false},
		{"one worker", map[string]skewGauges{"worker-1": full}, both, true, false},
		{"no request waiting", map[string]skewGauges{"worker-0": idle, "worker-1": gauges(8, 0, 0.99)}, both, true, false},
		{"the KV cache not full", map[string]skewGauges{"worker-0": idle, "worker-1": gauges(8, 3, 0.5)}, both, true, false},
		{"the idle sibling held unhealthy by the router (rejoining)", map[string]skewGauges{"worker-0": idle, "worker-1": full}, map[string]bool{"worker-1": true}, true, false},
		{"the router not running", map[string]skewGauges{"worker-0": idle, "worker-1": full}, both, false, false},
		{"the idle sibling's gauges stale", map[string]skewGauges{"worker-0": {running: idle.running, waiting: idle.waiting, kv: idle.kv, stale: true}, "worker-1": full}, both, true, false},
		{"the full worker's gauges stale", map[string]skewGauges{"worker-0": idle, "worker-1": {running: full.running, waiting: full.waiting, kv: full.kv, stale: true}}, both, true, false},
		{"the sibling's running count unknown", map[string]skewGauges{"worker-0": {waiting: agentapi.Int64(0), kv: agentapi.Float64(0)}, "worker-1": full}, both, true, false},
		{"the full worker's KV use unknown", map[string]skewGauges{"worker-0": idle, "worker-1": {running: full.running, waiting: full.waiting}}, both, true, false},
		{"three workers, one idle and routed", map[string]skewGauges{"worker-0": full, "worker-1": full, "worker-2": idle}, both, true, true},
	} {
		if got := workerSkewed(skewInstance(), skewView(now, c.ws, c.routed, c.router), "worker-1"); got != c.want {
			t.Errorf("%s: skewed %v, want %v", c.name, got, c.want)
		}
	}
	av := skewView(now, map[string]skewGauges{"worker-0": idle, "worker-1": full}, both, true)
	if workerSkewed(skewInstance("worker-0"), av, "worker-1") {
		t.Error("the idle sibling not router-admitted (rejoining) counts as routable")
	}
	av.report.Router.Workers[0].Ejected = true
	if workerSkewed(skewInstance(), av, "worker-1") {
		t.Error("the idle sibling ejected by outlier detection counts as routed")
	}
	av.unreachable = true
	av.report.Router.Workers[0].Ejected = false
	if workerSkewed(skewInstance(), av, "worker-1") {
		t.Error("an unreachable agent's last report counts")
	}
}

// skewBackend is a Backend holding a tracker for worker-1's process of the report skewView builds.
func skewBackend(av agentView) (*Backend, types.UID, string, agentapi.WorkerReport) {
	alloc, inst := types.UID("alloc-1"), "11111111-2222-3333-4444-555555555555"
	w := *reportOf(av.report, "worker-1")
	tokens := int64(1000)
	b := &Backend{agents: map[string]*agentState{string(alloc): {health: map[string]*tracker{trackerKey(inst, w): {tokens: &tokens, tokensSince: av.now.Add(-time.Hour)}}}}}
	return b, alloc, inst, w
}

func TestTheSkewClockIsAdvancedOncePerReport(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	av := skewView(t0, map[string]skewGauges{"worker-0": gauges(0, 0, 0), "worker-1": gauges(8, 3, 0.97)}, map[string]bool{"worker-0": true, "worker-1": true}, true)
	b, alloc, inst, w := skewBackend(av)
	if got := b.skewClock(alloc, inst, w, t0, true); !got.Equal(t0) {
		t.Fatalf("a first skewed report starts the clock at %v, want %v", got, t0)
	}
	if got := b.skewClock(alloc, inst, w, t0.Add(10*time.Second), true); !got.Equal(t0) {
		t.Errorf("a later skewed report: since %v, want %v kept", got, t0)
	}
	// The same report judged again (another pass on it) changes nothing, a different verdict included.
	if got := b.skewClock(alloc, inst, w, t0.Add(10*time.Second), false); !got.Equal(t0) {
		t.Errorf("the same report judged again: since %v, want %v kept", got, t0)
	}
	if got := b.skewClock(alloc, inst, w, t0.Add(20*time.Second), false); !got.IsZero() {
		t.Errorf("a report that is not skewed: since %v, want the clock stopped", got)
	}
	if got := b.skewClock(alloc, inst, w, t0.Add(30*time.Second), true); !got.Equal(t0.Add(30 * time.Second)) {
		t.Errorf("a skew after a clear: since %v, want it started again at %v", got, t0.Add(30*time.Second))
	}
	other := w
	other.PID++
	if got := b.skewClock(alloc, inst, other, t0.Add(40*time.Second), true); !got.IsZero() {
		t.Errorf("a process with no tracker (no metrics read yet): since %v, want none", got)
	}
}

// The signal is due kvSkewTimeout after the clock's start; a report that is not skewed in between, a
// stale gauge among them, starts it again; a restarted controller starts it again from its own start.
func TestTheKVSkewSignalIsDueAfterKVSkewTimeout(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	routed := map[string]bool{"worker-0": true, "worker-1": true}
	skewed := func(at time.Time) agentView {
		return skewView(at, map[string]skewGauges{"worker-0": gauges(0, 0, 0), "worker-1": gauges(8, 3, 0.97)}, routed, true)
	}
	staleSibling := func(at time.Time) agentView {
		return skewView(at, map[string]skewGauges{"worker-0": {running: agentapi.Int64(0), waiting: agentapi.Int64(0), kv: agentapi.Float64(0), stale: true}, "worker-1": gauges(8, 3, 0.97)}, routed, true)
	}
	st := settings{kvSkewTimeout: 2 * time.Minute}
	in := skewInstance()
	alloc := &v1alpha1.Allocation{}
	alloc.UID = "alloc-1"
	b, _, _, _ := skewBackend(skewed(t0))
	started := t0.Add(-time.Hour)
	signal := func(r *Reconciler, mem *memory, av agentView) (string, time.Time) {
		p := &pass{r: r, now: av.now, mem: mem}
		w := reportOf(av.report, "worker-1")
		sig, _ := p.kvSkewSignal(in, alloc, av, st, w)
		return sig, p.next
	}
	r, mem := &Reconciler{Backend: b}, newMemory(started)
	for at := t0; at.Before(t0.Add(2 * time.Minute)); at = at.Add(10 * time.Second) {
		if sig, next := signal(r, mem, skewed(at)); sig != "" || !next.Equal(t0.Add(2*time.Minute)) {
			t.Fatalf("at %s: signal %q, next pass %v; want none yet, and a pass asked for at %v", at.Sub(t0), sig, next, t0.Add(2*time.Minute))
		}
	}
	if sig, _ := signal(r, mem, skewed(t0.Add(2*time.Minute))); sig != v1alpha1.ReasonKVSkew {
		t.Fatalf("at kvSkewTimeout: signal %q, want KVSkew", sig)
	}

	// A stale sibling's sample in between is no skew: the clock starts again.
	b2, _, _, _ := skewBackend(skewed(t0))
	r2, mem2 := &Reconciler{Backend: b2}, newMemory(started)
	signal(r2, mem2, skewed(t0))
	signal(r2, mem2, staleSibling(t0.Add(90*time.Second)))
	if sig, _ := signal(r2, mem2, skewed(t0.Add(100*time.Second))); sig != "" {
		t.Fatalf("after a stale sibling's report: signal %q, want none", sig)
	}
	if sig, _ := signal(r2, mem2, skewed(t0.Add(2*time.Minute+10*time.Second))); sig != "" {
		t.Errorf("130 s after the first skew but 30 s after the clock's restart: signal %q, want none", sig)
	}
	if sig, _ := signal(r2, mem2, skewed(t0.Add(100*time.Second+2*time.Minute))); sig != v1alpha1.ReasonKVSkew {
		t.Errorf("kvSkewTimeout after the clock's restart: signal %q, want KVSkew", sig)
	}

	// A controller restarted mid-skew: a new Backend and memory. The old clock is gone, and even a clock
	// that ran before this controller's start is floored at it.
	b3, _, _, _ := skewBackend(skewed(t0))
	r3, mem3 := &Reconciler{Backend: b3}, newMemory(t0.Add(time.Minute))
	signal(r3, mem3, skewed(t0)) // the clock starts at t0, before this controller started
	if sig, next := signal(r3, mem3, skewed(t0.Add(2*time.Minute))); sig != "" || !next.Equal(t0.Add(3*time.Minute)) {
		t.Errorf("kvSkewTimeout after the skew began but 1 min after the controller started: signal %q, next %v; want none, and %v", sig, next, t0.Add(3*time.Minute))
	}
	if sig, _ := signal(r3, mem3, skewed(t0.Add(3*time.Minute))); sig != v1alpha1.ReasonKVSkew {
		t.Errorf("kvSkewTimeout after the controller's start: signal %q, want KVSkew", sig)
	}
}

// Not draining (HBEP-7, by #520): the clock holds only while the worker's generated-token counter is known
// and unchanged since it started. A counter that moves starts it again at that report; an unknown one
// stops it.
func TestTheSkewClockNeedsAFlatTokenCounter(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	av := skewView(t0, map[string]skewGauges{"worker-0": gauges(0, 0, 0), "worker-1": gauges(8, 3, 0.97)}, map[string]bool{"worker-0": true, "worker-1": true}, true)
	b, alloc, inst, w := skewBackend(av)
	tr := b.agents[string(alloc)].health[trackerKey(inst, w)]
	if got := b.skewClock(alloc, inst, w, t0, true); !got.Equal(t0) {
		t.Fatalf("a flat counter: the clock starts at %v, want %v", got, t0)
	}
	tr.tokensSince = t0.Add(15 * time.Second) // the counter moved, taken in with the report at t0+15s
	if got := b.skewClock(alloc, inst, w, t0.Add(20*time.Second), true); !got.Equal(t0.Add(20 * time.Second)) {
		t.Errorf("after the counter moved: the clock's start is %v, want it started again at %v", got, t0.Add(20*time.Second))
	}
	if got := b.skewClock(alloc, inst, w, t0.Add(30*time.Second), true); !got.Equal(t0.Add(20 * time.Second)) {
		t.Errorf("the counter flat again: start %v, want %v kept", got, t0.Add(20*time.Second))
	}
	tr.tokens = nil
	if got := b.skewClock(alloc, inst, w, t0.Add(40*time.Second), true); !got.IsZero() {
		t.Errorf("an unknown counter: start %v, want the clock stopped", got)
	}
}

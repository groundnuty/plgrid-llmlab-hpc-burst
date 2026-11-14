package lifecycle

// The crash-correlation event (HBEP-7, Health and restarts: Crash correlation; #539) on hand-built
// exits and reports, with every time chosen by the test: the pair, and the non-pairs.

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

var crashT0 = time.Date(2026, 11, 13, 22, 0, 0, 0, time.UTC)

// crashInstance is a ready Instance of one worker, verified and warmed up: its worker serving (#550).
func crashInstance(name string) *v1alpha1.Instance {
	return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID("uid-" + name)},
		Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady, Conditions: []metav1.Condition{
			{Type: v1alpha1.ConditionVerified, Status: metav1.ConditionTrue},
			{Type: v1alpha1.ConditionWarmedUp, Status: metav1.ConditionTrue}}}}
}

// exitOf is an exit of worker w of Instance in, at at, of process pid.
func exitOf(in *v1alpha1.Instance, w string, pid int, at time.Time) *exitSeen {
	return &exitSeen{key: fmt.Sprintf("%s/%s/%d", in.UID, w, pid), inst: in, worker: w, at: at, seen: at}
}

func TestRecordExitPairsExitsOfTwoInstancesWithin30s(t *testing.T) {
	a, b, c := crashInstance("a"), crashInstance("b"), crashInstance("c")
	type pairOut struct{ later, earlier string }
	for _, tc := range []struct {
		name  string
		md    []types.UID // each exit's ModelDeployment; "md" when nil
		exits []*exitSeen
		want  []pairOut // per exit, in order: the pair it raised, or {} for none
	}{
		{name: "10 s apart", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(10*time.Second))},
			want: []pairOut{{}, {"b", "a"}}},
		{name: "exactly 30 s apart", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(30*time.Second))},
			want: []pairOut{{}, {"b", "a"}}},
		{name: "31 s apart", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(31*time.Second))},
			want: []pairOut{{}, {}}},
		{name: "one Instance's crash loop", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(a, "worker-0", 3, crashT0.Add(5*time.Second)), exitOf(a, "worker-0", 4, crashT0.Add(10*time.Second))},
			want: []pairOut{{}, {}, {}}},
		{name: "two workers of one Instance", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(a, "worker-1", 2, crashT0.Add(time.Second))},
			want: []pairOut{{}, {}}},
		{name: "two deployments", md: []types.UID{"md", "md-2"}, exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(time.Second))},
			want: []pairOut{{}, {}}},
		{name: "the later exit seen first", exits: []*exitSeen{exitOf(b, "worker-0", 2, crashT0.Add(10*time.Second)), exitOf(a, "worker-0", 1, crashT0)},
			want: []pairOut{{}, {"b", "a"}}},
		{name: "one exit seen in every report", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(5*time.Second)), exitOf(b, "worker-0", 2, crashT0.Add(5*time.Second))},
			want: []pairOut{{}, {"b", "a"}, {}}},
		{name: "three in a row", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(10*time.Second)), exitOf(c, "worker-0", 3, crashT0.Add(20*time.Second))},
			want: []pairOut{{}, {"b", "a"}, {"c", "b"}}},
		// The nearest exit to C's (B's, 2 s after it) has paired already; A's, 8 s before it, still pairs.
		{name: "a late exit whose nearest has paired", exits: []*exitSeen{exitOf(a, "worker-0", 1, crashT0), exitOf(b, "worker-0", 2, crashT0.Add(10*time.Second)), exitOf(c, "worker-0", 3, crashT0.Add(8*time.Second))},
			want: []pairOut{{}, {"b", "a"}, {"c", "a"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMemory(crashT0)
			for i, e := range tc.exits {
				md := types.UID("md")
				if tc.md != nil {
					md = tc.md[i]
				}
				later, earlier := m.recordExit(md, e)
				var got pairOut
				if later != nil {
					got = pairOut{later.inst.Name, earlier.inst.Name}
				}
				if got != tc.want[i] {
					t.Errorf("exit %d (%s %s at +%s): pair %+v, want %+v", i, e.inst.Name, e.worker, e.at.Sub(crashT0), got, tc.want[i])
				}
			}
		})
	}
}

// crashPass is a pass over ModelDeployment md that records events.
func crashPass(now time.Time, mem *memory) (*pass, *MemoryRecorder) {
	rec := &MemoryRecorder{}
	md := mdSpec("md")
	md.UID = "md-uid"
	return &pass{r: &Reconciler{Recorder: rec}, now: now, mem: mem, md: md}, rec
}

// reportView is an agent view whose report has the given workers and router, received at at.
func reportView(at time.Time, workers []agentapi.WorkerReport, router *agentapi.RouterReport) agentView {
	return agentView{report: &agentapi.InstanceReport{Workers: workers, Router: router}, reportAt: at, now: at}
}

func exitedWorker(name string, pid int, exitedAt *time.Time) agentapi.WorkerReport {
	start := crashT0.Add(-time.Hour)
	return agentapi.WorkerReport{Name: name, Phase: agentapi.PartExited, PID: pid, StartTime: &start, ExitCode: ptrTo(1), ExitedAt: exitedAt}
}

// The pass reads exits from the agents' reports: an exited worker's ExitedAt decides the window, and
// its first sight when absent; a stopped worker (a requested stop) and the router never count. The
// event regards the Instance whose exit made the pair, related to the other.
func TestCorrelateExitsReadsTheReports(t *testing.T) {
	a, b := crashInstance("a"), crashInstance("b")
	at := func(d time.Duration) *time.Time { v := crashT0.Add(d); return &v }
	t.Run("ExitedAt decides the window, not when the reports came", func(t *testing.T) {
		mem := newMemory(crashT0)
		// A exited at +0 and B at +31 s, but the reports came 20 s apart: no pair.
		p, rec := crashPass(crashT0.Add(40*time.Second), mem)
		p.correlateExits(a, reportView(crashT0.Add(40*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 1, at(0))}, nil))
		p.now = crashT0.Add(60 * time.Second)
		p.correlateExits(b, reportView(crashT0.Add(60*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 2, at(31*time.Second))}, nil))
		if evs := rec.Find(eventFailing, v1alpha1.ReasonCrashCorrelated); len(evs) != 0 {
			t.Errorf("events %+v, want none: the exits were 31 s apart", evs)
		}
		// Exits 10 s apart, seen 50 s apart: a pair.
		mem2 := newMemory(crashT0)
		p2, rec2 := crashPass(crashT0, mem2)
		p2.correlateExits(a, reportView(crashT0, []agentapi.WorkerReport{exitedWorker("worker-0", 1, at(0))}, nil))
		p2.now = crashT0.Add(50 * time.Second)
		p2.correlateExits(b, reportView(crashT0.Add(50*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 2, at(10*time.Second))}, nil))
		evs := rec2.Find(eventFailing, v1alpha1.ReasonCrashCorrelated)
		if len(evs) != 1 || evs[0].Name != "b" || evs[0].RelatedName != "a" || evs[0].RelatedKind != "*v1alpha1.Instance" || !evs[0].Warning {
			t.Fatalf("events %+v, want one failing Warning CrashCorrelated regarding b, related to Instance a", evs)
		}
	})
	t.Run("first sight when ExitedAt is absent", func(t *testing.T) {
		p, rec := crashPass(crashT0, newMemory(crashT0))
		p.correlateExits(a, reportView(crashT0, []agentapi.WorkerReport{exitedWorker("worker-0", 1, nil)}, nil))
		p.correlateExits(b, reportView(crashT0.Add(20*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 2, nil)}, nil))
		if evs := rec.Find(eventFailing, v1alpha1.ReasonCrashCorrelated); len(evs) != 1 {
			t.Errorf("events %+v, want one: first seen 20 s apart", evs)
		}
	})
	t.Run("a requested stop and the router never count", func(t *testing.T) {
		p, rec := crashPass(crashT0, newMemory(crashT0))
		stopped := exitedWorker("worker-0", 1, at(0))
		stopped.Phase, stopped.ExitedAt = agentapi.PartStopped, nil
		p.correlateExits(a, reportView(crashT0, []agentapi.WorkerReport{stopped}, &agentapi.RouterReport{Phase: agentapi.PartExited, ExitCode: ptrTo(1)}))
		p.correlateExits(b, reportView(crashT0.Add(5*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 2, at(5*time.Second))}, nil))
		if evs := rec.Find(eventFailing, v1alpha1.ReasonCrashCorrelated); len(evs) != 0 {
			t.Errorf("events %+v, want none: a's worker was stopped as asked, and its router is not a worker", evs)
		}
	})
}

// A deleted deployment's exits are gone once crashKeep has passed, whatever deployment the next exit is
// of: the tracker is bounded by time across every deployment.
func TestRecordExitPrunesEveryDeployment(t *testing.T) {
	m := newMemory(crashT0)
	a, b, c := crashInstance("a"), crashInstance("b"), crashInstance("c")
	m.recordExit("md-gone", exitOf(a, "worker-0", 1, crashT0))
	m.recordExit("md-gone", exitOf(b, "worker-0", 2, crashT0.Add(time.Minute)))
	live := exitOf(c, "worker-0", 3, crashT0.Add(crashKeep+2*time.Minute))
	m.recordExit("md-live", live)
	if xs, ok := m.exits["md-gone"]; ok {
		t.Errorf("the deleted deployment keeps %d exits past crashKeep", len(xs))
	}
	for k := range m.exitKeys {
		if k != live.key {
			t.Errorf("exit key %q kept past crashKeep", k)
		}
	}
	if len(m.exits["md-live"]) != 1 {
		t.Errorf("the live deployment's exits: %d, want its one", len(m.exits["md-live"]))
	}
}

// severalInstance is a ready Instance of two workers, worker-1 verified and, when warmed, warmed up at its
// current generation.
func severalInstance(t *testing.T, name string, warmed bool) *v1alpha1.Instance {
	t.Helper()
	in := crashInstance(name)
	in.UID = newUID() // a verification record's name needs a UUID
	two := int32(2)
	in.Spec.Launch.Workers = &two
	rec, err := v1alpha1.WorkerVerificationRecordName(in.UID, "worker-1", workerVerificationGeneration(in, "worker-1"))
	if err != nil {
		t.Fatal(err)
	}
	in.Status.Workers = []v1alpha1.InstanceWorker{{Name: "worker-1", Record: rec, Verdict: string(v1alpha1.VerdictPass), WarmedUp: warmed}}
	return in
}

// Only a serving worker's exit counts (#550): its Instance ready or draining, and it verified and warmed up
// since its last start. A failed start, an Instance verifying, a worker not warmed since a restart, the
// drain's own stop, and a stale Instance of an earlier same-named deployment, ready or draining, never
// count. Each case pairs a candidate exit with a serving one on
// another Instance 5 s later.
func TestCorrelateExitsCountsOnlyServingWorkers(t *testing.T) {
	at := func(d time.Duration) *time.Time { v := crashT0.Add(d); return &v }
	verifying := crashInstance("v")
	verifying.Status.Phase = v1alpha1.InstanceVerifying
	starting := crashInstance("s")
	starting.Status.Phase = v1alpha1.InstanceStarting
	draining := crashInstance("d")
	draining.Status.Phase = v1alpha1.InstanceDraining
	// A draining Instance whose worker restarted: WarmedUp Unknown until it warms up again.
	drainingRestarted := crashInstance("dr")
	drainingRestarted.Status.Phase = v1alpha1.InstanceDraining
	drainingRestarted.Status.Conditions[1].Status = metav1.ConditionUnknown
	staleDraining := crashInstance("od")
	staleDraining.Status.Phase = v1alpha1.InstanceDraining
	failedStart := agentapi.WorkerReport{Name: "worker-0", Phase: agentapi.PartExited, ExitCode: ptrTo(127), ExitedAt: at(0)}
	for _, c := range []struct {
		name   string
		in     *v1alpha1.Instance
		worker agentapi.WorkerReport
		stale  bool
		want   int
	}{
		{"a serving worker", crashInstance("r"), exitedWorker("worker-0", 1, at(0)), false, 1},
		{"an Instance verifying", verifying, exitedWorker("worker-0", 1, at(0)), false, 0},
		{"a failed start", starting, failedStart, false, 0},
		{"several workers, warmed", severalInstance(t, "w", true), exitedWorker("worker-1", 1, at(0)), false, 1},
		{"several workers, not warmed", severalInstance(t, "n", false), exitedWorker("worker-1", 1, at(0)), false, 0},
		{"a stale Instance, ready", crashInstance("o"), exitedWorker("worker-0", 1, at(0)), true, 0},
		{"a draining Instance, warmed", draining, exitedWorker("worker-0", 1, at(0)), false, 1},
		{"a draining Instance, not warmed since a restart", drainingRestarted, exitedWorker("worker-0", 1, at(0)), false, 0},
		{"a stale Instance, draining", staleDraining, exitedWorker("worker-0", 1, at(0)), true, 0},
		{"a draining Instance's worker stopped by its drain", draining, agentapi.WorkerReport{Name: "worker-0", Phase: agentapi.PartStopped, PID: 1, ExitCode: ptrTo(0)}, false, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, rec := crashPass(crashT0, newMemory(crashT0))
			p.stale = map[types.UID]bool{}
			if c.stale {
				p.stale[c.in.UID] = true
			}
			p.correlateExits(c.in, reportView(crashT0, []agentapi.WorkerReport{c.worker}, nil))
			p.correlateExits(crashInstance("b"), reportView(crashT0.Add(5*time.Second), []agentapi.WorkerReport{exitedWorker("worker-0", 2, at(5*time.Second))}, nil))
			if got := len(rec.Find(eventFailing, v1alpha1.ReasonCrashCorrelated)); got != c.want {
				t.Errorf("%d CrashCorrelated events, want %d", got, c.want)
			}
		})
	}
}

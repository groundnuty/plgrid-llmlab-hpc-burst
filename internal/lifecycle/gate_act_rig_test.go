package lifecycle

// The throughput gate acts (HBEP-7, Over-allocation and qualification (beta), The throughput gate; Test plan, The
// gate, The forced rung, Several workers; #616, PR 2). A first miss restarts the worker once, at rung 1, forced; the
// retry decides: a pass is Passed, a second miss fails the Instance with QualificationFailed and releases its
// Allocation. The fixture is gate_rig_test.go's: an entry of 20 ms per output token and 400 ms to first token, the
// default tolerance, dated 2026-10-23, so the limits are 40 ms and max(800 ms, 500 ms) = 800 ms. Every value below is
// a literal worked out by hand; the entry and the engine's measurements are separate literals in each row, and no
// expected limit or verdict comes from the gate's own helpers.

import (
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// gateKnob is what the fake engines' warm-ups measure now: a test turns it between the first measurement and the
// retry, in the same goroutine that steps the rig.
type gateKnob struct{ tpot, ttft time.Duration }

// gateEngineOf is an engine per Allocation whose warm-up rounds measure what the knob says when each is asked for.
func gateEngineOf(k *gateKnob) func(string) afake.Engine {
	return func(string) afake.Engine {
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			return gateRound(k.tpot, k.ttft)(n, req)
		}}
	}
}

// gateSequence is gateEngineOf for a measurement that does not change.
func gateSequence(tpot time.Duration) func(string) afake.Engine {
	return gateEngineOf(&gateKnob{tpot: tpot, ttft: 300 * time.Millisecond})
}

// gateRestarts is the failing events of an Instance, by code, oldest first.
func gateRestarts(rg *rig, name string) []string { return healthFailing(rg, name) }

// gateRestartSignals is the signals of the Instance's recorded restarts.
func gateRestartSignals(in *v1alpha1.Instance) []string {
	var out []string
	for _, r := range in.Status.Restarts {
		out = append(out, r.Part+"/"+r.Signal)
	}
	return out
}

// A miss is measured twice. The first restarts the worker at rung 1, its event first and the miss recorded with the
// restart; the retry then passes, or it misses too and fails the Instance with QualificationFailed, which names both
// measurements, the limits and the entry's date, and the Allocation is deleted after its release cause. Each limit
// is tried on the side that misses; its pass is in gate_rig_test.go (39, 790, 140 and 30 ms).
func TestGateAMissIsMeasuredTwice(t *testing.T) {
	ms := time.Millisecond
	tol50 := []func(*v1alpha1.ExpectedPerformance){func(e *v1alpha1.ExpectedPerformance) { e.TolerancePercent = ptrTo(int32(50)) }}
	ttft50 := []func(*v1alpha1.ExpectedPerformance){func(e *v1alpha1.ExpectedPerformance) { e.TimeToFirstToken = "50ms" }}
	for _, tc := range []struct {
		name         string
		edit         []func(*v1alpha1.ExpectedPerformance)
		first, retry gateKnob
		firstText    string // the first miss's measurement, as the restart event names it
		wants        []string
	}{
		{"41 ms, then 30 ms", nil, gateKnob{41 * ms, 300 * ms}, gateKnob{30 * ms, 300 * ms}, "time per output token 41ms (limit 40ms)", nil},
		{"41 ms twice", nil, gateKnob{41 * ms, 300 * ms}, gateKnob{41 * ms, 300 * ms}, "time per output token 41ms (limit 40ms)",
			[]string{"the first measurement was time per output token 41ms and time to first token 300ms", "the retry after a worker restart time per output token 41ms and time to first token 300ms",
				"the limits of the entry dated 2026-10-23 are time per output token 40ms and time to first token 800ms"}},
		{"810 ms to first token, then 300 ms", nil, gateKnob{20 * ms, 810 * ms}, gateKnob{20 * ms, 300 * ms}, "time to first token 810ms (limit 800ms)", nil},
		{"810 ms to first token twice", nil, gateKnob{20 * ms, 810 * ms}, gateKnob{20 * ms, 810 * ms}, "time to first token 810ms (limit 800ms)",
			[]string{"the first measurement was time per output token 20ms and time to first token 810ms", "the retry after a worker restart time per output token 20ms and time to first token 810ms",
				"time per output token 40ms and time to first token 800ms"}},
		{"entry ttft 50 ms: 160 ms, then 140 ms", ttft50, gateKnob{20 * ms, 160 * ms}, gateKnob{20 * ms, 140 * ms}, "time to first token 160ms (limit 150ms)", nil},
		{"entry ttft 50 ms: 160 ms twice", ttft50, gateKnob{20 * ms, 160 * ms}, gateKnob{20 * ms, 160 * ms}, "time to first token 160ms (limit 150ms)",
			[]string{"the first measurement was time per output token 20ms and time to first token 160ms", "time per output token 40ms and time to first token 150ms"}},
		{"tolerance 50: 31 ms, then 30 ms", tol50, gateKnob{31 * ms, 300 * ms}, gateKnob{30 * ms, 300 * ms}, "time per output token 31ms (limit 30ms)", nil},
		{"tolerance 50: 31 ms twice", tol50, gateKnob{31 * ms, 300 * ms}, gateKnob{31 * ms, 300 * ms}, "time per output token 31ms (limit 30ms)",
			[]string{"the first measurement was time per output token 31ms and time to first token 300ms", "time per output token 30ms and time to first token 600ms"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := newCauseLog()
			rg := newRig(t, withInterceptors(log.funcs()))
			k := tc.first
			rg.engine = gateEngineOf(&k)
			name, _ := gateEntries(rg, gateMD(), oneEntry(tc.edit...))
			alloc := rg.instance(name).Spec.AllocationRef.Name

			startUntil(rg, "md", 10*time.Minute, "the first miss's restart", func() bool { return len(gateRestarts(rg, name)) > 0 })
			in := rg.instance(name)
			if got := gateRestarts(rg, name); len(got) != 1 || got[0] != "WorkerRestart" {
				t.Fatalf("failing events %v after the first miss, want the one rung-1 restart, WorkerRestart", got)
			}
			if got := gateRestartSignals(in); len(got) != 1 || got[0] != "worker-0/ThroughputMissed" {
				t.Errorf("recorded restarts %v, want worker-0/ThroughputMissed", got)
			}
			if evs := rg.rec.Find("failing", "WorkerRestart"); len(evs) != 1 || !strings.Contains(evs[0].Message, tc.firstText) || !strings.Contains(evs[0].Message, "ThroughputMissed") {
				t.Errorf("the restart's event %+v, want it to name the signal and %q", evs, tc.firstText)
			}
			g := in.Status.Gate
			if g == nil || g.Verdict != "" || len(g.Misses) != 1 || g.Misses[0].Worker != "worker-0" || g.ReferenceDate != "2026-10-23" {
				t.Fatalf("status.gate %+v after the first miss, want one miss of worker-0 against 2026-10-23 and no verdict", g)
			}
			if in.Status.Phase == v1alpha1.InstanceReady || ended(in) {
				t.Fatalf("the Instance is %s after a first miss, want it measuring again", in.Status.Phase)
			}
			k = tc.retry // what the restarted worker's warm-up measures

			if len(tc.wants) == 0 {
				startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
				got := rg.instance(name)
				if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || g.ReferenceDate != "2026-10-23" || len(g.Misses) != 1 {
					t.Errorf("status.gate %+v, want Passed against 2026-10-23 with the first miss kept", g)
				}
				if ev := gateRestarts(rg, name); len(ev) != 1 || len(got.Status.Restarts) != 1 || got.Status.EndReason != "" {
					t.Errorf("failing events %v, restarts %v, end reason %q, want the one restart and no end", ev, gateRestartSignals(got), got.Status.EndReason)
				}
				return
			}
			startUntil(rg, "md", 10*time.Minute, "the Instance failed", func() bool { return ended(rg.instance(name)) })
			got := rg.instance(name)
			if got.Status.EndReason != v1alpha1.ReasonQualificationFailed || got.Status.ReadyTime != nil {
				t.Fatalf("end reason %q, ready %v, want QualificationFailed and never ready", got.Status.EndReason, got.Status.ReadyTime)
			}
			msg := conditionMessage(got, v1alpha1.ConditionQualified)
			for _, want := range append([]string{"the throughput gate:", "missed twice"}, tc.wants...) {
				if !strings.Contains(msg, want) {
					t.Errorf("Qualified's message %q does not hold %q", msg, want)
				}
			}
			if c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionQualified); c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonFailed {
				t.Errorf("Qualified is %+v, want False/Failed", c)
			}
			if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GateMissed || g.ReferenceDate != "2026-10-23" || len(g.Misses) != 2 {
				t.Errorf("status.gate %+v, want Missed against 2026-10-23 with both misses", g)
			}
			if ev := gateRestarts(rg, name); len(ev) != 2 || ev[0] != "WorkerRestart" || ev[1] != v1alpha1.ReasonQualificationFailed {
				t.Errorf("failing events %v, want the restart, then QualificationFailed, and no RestartLoop", ev)
			}
			startUntil(rg, "md", 5*time.Minute, "the Allocation deleted", func() bool { _, ok := log.deletedWith(alloc); return ok })
			if cause, _ := log.deletedWith(alloc); cause != "Instance "+name+" failed: QualificationFailed" {
				t.Errorf("the Allocation was deleted with the cause %q in the store, want it written first: Instance %s failed: QualificationFailed", cause, name)
			}
		})
	}
}

// crashDuringStart exits the worker of the Instance on alloc as soon as its k-th process (counting from 1) runs, and
// steps until the controller has restarted it: one more failing event on the Instance than before.
func crashDuringStart(rg *rig, name, alloc string, k int) {
	rg.t.Helper()
	have := len(gateRestarts(rg, name))
	startUntil(rg, "md", 10*time.Minute, "the worker's process running", func() bool {
		ra, ok := rg.agents[alloc]
		return ok && len(ra.agent.Starts()) >= k
	})
	if in := rg.instance(name); in.Status.ReadyTime != nil || isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp) {
		rg.t.Fatalf("process %d runs on a warmed up Instance: the crash would not be during its start", k)
	}
	rg.agentOf(alloc).agent.CrashWorker(agentapi.WorkerName(0), 1)
	startUntil(rg, "md", 10*time.Minute, "the restart of the exit", func() bool { return len(gateRestarts(rg, name)) > have })
}

// The forced rung (HBEP-7, The gate's record; plan Q2): an Instance whose worker exited twice during its start has
// spent the worker rung's two attempts within restart.window (30 min by default), so a third failure would climb to
// the whole-Instance rung. The gate's first miss does not: it is one more rung-1 restart, recorded with its signal,
// never rung 2 and never RestartLoop, and its retry decides, at 30 ms by a pass, at 41 ms by a QualificationFailed.
func TestGateTheFirstMissTakesTheWorkerRungWhateverItHasSpent(t *testing.T) {
	ms := time.Millisecond
	for _, retry := range []time.Duration{30 * ms, 41 * ms} {
		t.Run(retry.String(), func(t *testing.T) {
			rg := newRig(t)
			k := &gateKnob{tpot: 41 * ms, ttft: 300 * ms}
			rg.engine = gateEngineOf(k)
			name, _ := gateEntries(rg, gateMD(), oneEntry())
			alloc := rg.instance(name).Spec.AllocationRef.Name
			crashDuringStart(rg, name, alloc, 1)
			crashDuringStart(rg, name, alloc, 2)
			if got := gateRestarts(rg, name); len(got) != 2 {
				t.Fatalf("failing events %v after two exits, want two rung-1 restarts", got)
			}
			startUntil(rg, "md", 10*time.Minute, "the gate's restart", func() bool { return len(gateRestarts(rg, name)) > 2 })
			got := gateRestarts(rg, name)
			if want := []string{"WorkerRestart", "WorkerRestart", "WorkerRestart"}; len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
				t.Fatalf("failing events %v, want a third WorkerRestart: never InstanceRestart, never RestartLoop", got)
			}
			in := rg.instance(name)
			if sig := gateRestartSignals(in); len(sig) != 3 || sig[0] != "worker-0/PartExited" || sig[1] != "worker-0/PartExited" || sig[2] != "worker-0/ThroughputMissed" {
				t.Fatalf("recorded restarts %v, want two worker-0/PartExited and the gate's worker-0/ThroughputMissed", sig)
			}
			if in.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance)] != "" || in.Status.Phase == v1alpha1.InstanceEnded {
				t.Errorf("annotations %v, phase %s: the whole Instance restarted or the Instance ended", in.Annotations, in.Status.Phase)
			}
			k.tpot = retry
			if retry == 30*ms {
				startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
				if g := rg.instance(name).Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || len(g.Misses) != 1 {
					t.Errorf("status.gate %+v, want the retry to decide: Passed", g)
				}
				return
			}
			startUntil(rg, "md", 10*time.Minute, "the Instance failed", func() bool { return ended(rg.instance(name)) })
			if end := rg.instance(name).Status.EndReason; end != v1alpha1.ReasonQualificationFailed {
				t.Errorf("end reason %q, want QualificationFailed from the retry, not RestartLoop", end)
			}
			if ev := gateRestarts(rg, name); len(ev) != 4 || ev[3] != v1alpha1.ReasonQualificationFailed {
				t.Errorf("failing events %v, want the three restarts and QualificationFailed", ev)
			}
		})
	}
}

// The restart is still spaced by restart.backoff, and no miss is recorded while it waits. With a backoff of 5 min, a
// worker that exited during its start (its restart recorded at T) is followed by a warm-up that settles about a minute
// later, whose miss the gate holds back until T + 5 min: nothing is recorded in status.gate until the restart is
// made, in the same write.
func TestGateTheFirstMissIsSpacedByTheBackoff(t *testing.T) {
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{Backoff: dur(5 * time.Minute)}
	}))
	rg.engine = gateSequence(41 * time.Millisecond)
	name, _ := gateEntries(rg, gateMD(), oneEntry())
	alloc := rg.instance(name).Spec.AllocationRef.Name
	crashDuringStart(rg, name, alloc, 1)
	exit := rg.instance(name).Status.Restarts[0].Time.Time
	startUntil(rg, "md", 20*time.Minute, "the gate's restart", func() bool {
		in := rg.instance(name)
		if len(in.Status.Restarts) == 1 && in.Status.Gate != nil && len(in.Status.Gate.Misses) != 0 {
			t.Fatalf("a miss is recorded at %s, before the restart that spaces it", rg.clock.Now().Format(time.TimeOnly))
		}
		return len(in.Status.Restarts) == 2
	})
	in := rg.instance(name)
	if in.Status.Restarts[1].Signal != "ThroughputMissed" || in.Status.Gate == nil || len(in.Status.Gate.Misses) != 1 {
		t.Fatalf("restarts %v, gate %+v, want the exit's, then the gate's with its miss", gateRestartSignals(in), in.Status.Gate)
	}
	if gap := in.Status.Restarts[1].Time.Sub(exit); gap < 5*time.Minute || gap > 5*time.Minute+30*time.Second {
		t.Errorf("the gate's restart came %s after the exit's, want it at restart.backoff, 5 min, and one pass later at most", gap)
	}
}

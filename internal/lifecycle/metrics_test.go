package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The tests in this file cover contracts PR 92 (agentapi.Health): a nil gauge
// is unknown, never zero; no gauge-based action is taken while the metrics are
// not fresh (Health.MetricsFresh); counters are compared only when
// MetricsReadAt advanced. Each "never" case has a control that fires in the
// same rig, so a test that sees no event is not passing for want of a signal.

// TestMetricsRigSamplesAreFresh guards every gauge test in the package: the
// rig's default engine double, reported at the rig's pace, is fresh by the
// controller's judgement. If it were not, every "does not fire" test would pass
// for free.
func TestMetricsRigSamplesAreFresh(t *testing.T) {
	rg, _, allocName := healthRig(t, func() afake.Engine { return afake.Engine{} })
	startStep(rg, "md", startTick)
	alloc, ok := rg.allocation(allocName)
	if !ok {
		t.Fatalf("Allocation %s not found", allocName)
	}
	rep, at, _ := rg.backend.latest(alloc.UID)
	if rep == nil || rep.Instance == nil {
		t.Fatal("no Instance report reached the controller")
	}
	w := worker0(rep.Instance)
	av := agentView{report: rep.Instance, reportAt: at, reportTime: rep.Time, now: rg.clock.Now(), probeInterval: defaultProbeInterval}
	if !av.metricsFresh(w) {
		t.Fatalf("the rig's default sample is not fresh (report %s, received %s, now %s, health %+v)", rep.Time, at, rg.clock.Now(), w.Health)
	}
}

// stalledWithoutCounter is HealthStalled with a fresh read that holds no
// generated-token counter: eight requests run and the counter is unknown.
func stalledWithoutCounter(now time.Time, p afake.Process) agentapi.Health {
	h := afake.HealthStalled(now, p)
	h.GeneratedTokens = nil
	return h
}

// TestMetricsNoProgressNeedsFreshKnownCounters: the no-progress signal fires
// on a stall the controller can see (fresh reads, read age at or under two
// probe intervals) and never on one it cannot: a read older than two probe
// intervals, a cached sample re-sent in new Reports, an unreadable /metrics,
// or a counter the read did not hold.
func TestMetricsNoProgressNeedsFreshKnownCounters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		h     func() func(time.Time, afake.Process) agentapi.Health
		fires bool
	}{
		{"fresh-stall-fires", func() func(time.Time, afake.Process) agentapi.Health { return afake.HealthStalled }, true},
		{"read-15s-old-fires", func() func(time.Time, afake.Process) agentapi.Health {
			return afake.HealthMetricsStale(15 * time.Second)
		}, true},
		{"read-25s-old-never", func() func(time.Time, afake.Process) agentapi.Health {
			return afake.HealthMetricsStale(25 * time.Second)
		}, false},
		{"cached-sample-never", afake.HealthCached, false},
		{"unreadable-never", func() func(time.Time, afake.Process) agentapi.Health { return afake.HealthMetricsUnreadable }, false},
		{"counter-unknown-never", func() func(time.Time, afake.Process) agentapi.Health { return stalledWithoutCounter }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: tc.h()} })
			if tc.fires {
				healthUntilEvent(rg, in.Name, 0, 45*time.Minute, startTick)
				if got := healthFailing(rg, in.Name); len(got) != 1 || got[0] != "WorkerRestart" {
					t.Fatalf("failing events %v, want one WorkerRestart", got)
				}
				return
			}
			rg.run(45*time.Minute, startTick)
			if got := healthFailing(rg, in.Name); len(got) != 0 {
				t.Fatalf("failing events %v over 45 min; the controller cannot see this stall and must not act on it", got)
			}
			if ph := rg.instance(in.Name).Status.Phase; ph != v1alpha1.InstanceReady {
				t.Fatalf("phase %s, want still ready", ph)
			}
		})
	}
}

// errorsAfter returns a double that answers with phase1 for its responses by
// status until *switchAt, then with {"5xx": 10} at every sample: ten server
// errors in all, and no new answer after them.
func errorsAfter(switchAt *time.Time, phase1 map[string]int64) func(time.Time, afake.Process) agentapi.Health {
	return func(now time.Time, p afake.Process) agentapi.Health {
		h := afake.DefaultHealth(now, p)
		if switchAt.IsZero() || now.Before(*switchAt) {
			h.ResponsesByStatus = phase1
			return h
		}
		h.ResponsesByStatus = map[string]int64{"5xx": 10}
		return h
	}
}

// TestMetricsErrorsOnlyNeedsKnownAnswers: the only-errors signal compares two
// known counts of answers. Ten server errors after a known count of none fire
// it (control); the same ten errors after reads that held no responses by
// status (nil, unknown) never do, since nothing says when they came; an
// errors-only engine whose read is older than two probe intervals never does.
func TestMetricsErrorsOnlyNeedsKnownAnswers(t *testing.T) {
	t.Run("errors-only-fires", func(t *testing.T) {
		rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: afake.HealthErrorsOnly()} })
		healthUntilEvent(rg, in.Name, 0, 10*time.Minute, startTick)
	})
	t.Run("errors-only-stale-never", func(t *testing.T) {
		errs := afake.HealthErrorsOnly()
		rg, in, _ := healthRig(t, func() afake.Engine {
			return afake.Engine{Health: func(now time.Time, p afake.Process) agentapi.Health {
				h := errs(now, p)
				read := now.Add(-25 * time.Second)
				h.MetricsReadAt = &read
				return h
			}}
		})
		rg.run(10*time.Minute, startTick)
		if got := healthFailing(rg, in.Name); len(got) != 0 {
			t.Fatalf("failing events %v; stale metrics are unknown", got)
		}
	})
	for _, tc := range []struct {
		name   string
		phase1 map[string]int64
		fires  bool
	}{
		{"known-none-then-errors-fires", map[string]int64{}, true},
		{"unknown-then-errors-never", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var switchAt time.Time
			rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: errorsAfter(&switchAt, tc.phase1)} })
			rg.run(2*time.Minute, startTick) // phase 1 for over the 1 min window
			switchAt = rg.clock.Now().Add(startTick)
			rg.run(5*time.Minute, startTick)
			got := healthFailing(rg, in.Name)
			if tc.fires && len(got) == 0 {
				t.Fatal("no failing event: ten server errors after a known count of none are only errors")
			}
			if !tc.fires && len(got) != 0 {
				t.Fatalf("failing events %v: a nil ResponsesByStatus is unknown, never a count of zero", got)
			}
		})
	}
}

// drainRead is the default idle engine until *from, then an idle engine as
// mode describes it.
func drainRead(from *time.Time, mode string) func(time.Time, afake.Process) agentapi.Health {
	return func(now time.Time, p afake.Process) agentapi.Health {
		h := afake.DefaultHealth(now, p)
		if from.IsZero() || now.Before(*from) {
			return h
		}
		switch mode {
		case "read-lags-15s": // fresh (15 s <= 2 x 10 s), but read 15 s before the sample
			read := now.Add(-15 * time.Second)
			h.MetricsReadAt = &read
		case "read-25s-old": // stale
			read := now.Add(-25 * time.Second)
			h.MetricsReadAt = &read
		case "unreadable":
			return afake.HealthMetricsUnreadable(now, p)
		case "running-unknown": // a fresh read without the running gauge
			h.RunningRequests = nil
		}
		return h
	}
}

// TestMetricsDrainEndsOnAKnownZeroReadAfterTheDrainBegan: a draining primary
// ends Drained only on a known zero of running requests from fresh metrics
// read after the drain began (09:50:20). The control, fresh reads at each
// sample, ends at 09:50:30, the first report after the drain began. A read
// that lags its sample by 15 s is fresh, but at 09:50:30 it was taken at
// 09:50:15, before the drain began, so the primary ends only at 09:50:40.
// Stale, unreadable and gauge-less reads never end it before its hard end.
func TestMetricsDrainEndsOnAKnownZeroReadAfterTheDrainBegan(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		endedAt time.Time // zero: still draining at 09:55:20
	}{
		{"fresh", hoT(9, 50, 30)},
		{"read-lags-15s", hoT(9, 50, 40)},
		{"read-25s-old", time.Time{}},
		{"unreadable", time.Time{}},
		{"running-unknown", time.Time{}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			var from time.Time
			rg.engine = func(string) afake.Engine { return afake.Engine{Health: drainRead(&from, tc.mode)} }
			p := hoStart(rg, nil)
			hoStepTo(rg, hoT(9, 50, 0), time.Minute, nil)
			from = hoT(9, 50, 10)
			var ended time.Time
			for rg.clock.Now().Before(hoT(9, 55, 20)) && ended.IsZero() {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
				if got := rg.instance(p.Name); got.Status.Phase == v1alpha1.InstanceEnded {
					if got.Status.EndReason != v1alpha1.ReasonDrained {
						t.Fatalf("ended with %s, want Drained", got.Status.EndReason)
					}
					ended = rg.clock.Now()
				}
			}
			if _, ok := drainBegan(rg.instance(p.Name)); !ok {
				t.Fatal("the primary never began draining; the test proves nothing")
			}
			if !ended.Equal(tc.endedAt) {
				t.Fatalf("ended at %s, want %s (zero: still draining)", ended.Format(time.TimeOnly), tc.endedAt.Format(time.TimeOnly))
			}
		})
	}
}

// TestQualifyReasoningBudgetExhaustedIsAnOrdinaryFailure (science-agent
// ruling, 31 Oct 2026): a behaviour check that fails with
// ReasoningBudgetExhausted fails the Instance like any failed check, the
// reason code is recorded, and its class is not stopped: the controller
// provisions again. The control, a max-model-len contradiction in the same
// rig, does stop the class (one submission, Degraded VerificationFailed), so
// the observation can see a stop.
func TestQualifyReasoningBudgetExhaustedIsAnOrdinaryFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		e     afake.Engine
		stops bool
	}{
		{"reasoning-budget", afake.Engine{Qualify: afake.QualifyReasoningBudgetExhausted}, false},
		{"contradiction-control", afake.Engine{Verify: afake.VerifyMaxModelLen("16384")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := verifRig(t, func() afake.Engine { return tc.e })
			st, _ := verifEnded(rg, in.Name)
			rg.run(10*time.Minute, startTick)
			degraded := rg.condition("md", v1alpha1.ConditionDegraded)
			if tc.stops {
				if rg.submissions() != 1 || degraded.Reason != v1alpha1.ReasonVerificationFailed {
					t.Fatalf("submissions %d, Degraded %s/%s; the control must stop the class", rg.submissions(), degraded.Status, degraded.Reason)
				}
				return
			}
			if rg.submissions() < 2 || degraded.Reason == v1alpha1.ReasonVerificationFailed {
				t.Fatalf("submissions %d, Degraded %s/%s; a ReasoningBudgetExhausted failure must not stop the class", rg.submissions(), degraded.Status, degraded.Reason)
			}
			if st.EndReason != v1alpha1.ReasonQualificationFailed {
				t.Errorf("end reason %q, want %s", st.EndReason, v1alpha1.ReasonQualificationFailed)
			}
			var msg string
			for _, c := range st.Conditions {
				if c.Type == v1alpha1.ConditionHealthy {
					msg = c.Message
				}
			}
			if !strings.Contains(msg, agentapi.ReasonReasoningBudgetExhausted) {
				t.Errorf("Healthy message %q does not record the reason code %s", msg, agentapi.ReasonReasoningBudgetExhausted)
			}
		})
	}
}

// TestQualifyRecordsTheBehaviourContext: the behaviour check's ContextTokens,
// the prompt length it ran at, is recorded in the Qualified condition (the
// only place v1alpha1 has for it).
func TestQualifyRecordsTheBehaviourContext(t *testing.T) {
	rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Qualify: afake.QualifyBehaviourContext(12345)} })
	var msg string
	for _, c := range rg.instance(in.Name).Status.Conditions {
		if c.Type == v1alpha1.ConditionQualified {
			msg = c.Message
		}
	}
	if !strings.Contains(msg, "12345 context tokens") {
		t.Fatalf("Qualified message %q does not record the behaviour check's 12345 context tokens", msg)
	}
}

// TestTrackerComparesOnlyAdvancedReads: a sample whose MetricsReadAt did not
// advance repeats the last read's values (agentapi.Health), so the tracker
// takes nothing from it: no answer sample, and no move of the no-progress
// clock, even when it repeats an idle engine's zero running requests.
func TestTrackerComparesOnlyAdvancedReads(t *testing.T) {
	s := &agentState{health: map[string]*tracker{}}
	w := agentapi.WorkerReport{Name: "worker-0", PID: 4000}
	t0 := rigStart
	sample := func(at, read time.Time) {
		w.Health = &agentapi.Health{Time: at, ProbeOK: true, MetricsReadAt: &read,
			RunningRequests: agentapi.Int64(0), GeneratedTokens: agentapi.Int64(5), ResponsesByStatus: map[string]int64{"2xx": 3}}
		s.track("uid", w, at)
	}
	sample(t0, t0)
	sample(t0.Add(10*time.Second), t0) // the read did not advance
	tr := s.health[trackerKey("uid", w)]
	if len(tr.samples) != 1 || !tr.tokensSince.Equal(t0) {
		t.Fatalf("after a repeated read: %d samples, tokensSince %s; want 1 and %s", len(tr.samples), tr.tokensSince.Format(time.TimeOnly), t0.Format(time.TimeOnly))
	}
	sample(t0.Add(20*time.Second), t0.Add(20*time.Second)) // control: an advanced read counts
	if len(tr.samples) != 2 || !tr.tokensSince.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("after an advanced read: %d samples, tokensSince %s; want 2 and +20s", len(tr.samples), tr.tokensSince.Format(time.TimeOnly))
	}
}

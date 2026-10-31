package lifecycle

// Scenario tests for health, restarts and the ladder (HBEP-7, Test plan: Health,
// "A part that exits cleanly", "Each restart's event is emitted before the agent
// sees the raised generation", "A restart runs the Instance's original launch").

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

const healthWorker = "worker-0"

// healthSampled wraps a health double so a test learns when the first sample of
// a worker was taken: the no-progress clock starts at the first report in which
// worker-0 runs (it comes during verifying, not at ready). The time is the
// agent double's, not the controller's.
type healthSampled struct {
	first time.Time
	h     func(time.Time, afake.Process) agentapi.Health
}

func (s *healthSampled) sample(now time.Time, w afake.Process) agentapi.Health {
	if s.first.IsZero() {
		s.first = now
	}
	return s.h(now, w)
}

// healthFailing returns the codes of the failing events raised on an Instance.
func healthFailing(rg *rig, inst string) []string {
	var out []string
	for _, e := range rg.rec.Find("failing", "") {
		if e.Name == inst {
			out = append(out, e.Code)
		}
	}
	return out
}

// healthUntilEvent steps (with the ready check) until the Instance has more
// failing events than n, and returns the clock at that Step.
func healthUntilEvent(rg *rig, inst string, n int, limit time.Duration, tick time.Duration) time.Time {
	rg.t.Helper()
	for end := rg.clock.Now().Add(limit); len(healthFailing(rg, inst)) <= n; startStep(rg, "md", tick) {
		if !rg.clock.Now().Before(end) {
			rg.t.Fatalf("no failing event on %s within %s (now %s); events %v", inst, limit, rg.clock.Now().Format(time.RFC3339), healthFailing(rg, inst))
		}
	}
	return rg.clock.Now()
}

// healthRig brings one Instance of "md" to ready with an engine double per agent.
func healthRig(t *testing.T, mk func() afake.Engine, opts ...rigOption) (*rig, v1alpha1.Instance, string) {
	rg := newRig(t, opts...)
	rg.engine = func(string) afake.Engine { return mk() }
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	return rg, in, alloc
}

// TestHealthNoTokenProgressRestartsWorker covers "a stalled Instance with
// requests in flight is restarted at the worker rung within noProgressTimeout
// (30 min) plus one tick, not before", for two doubles: eight requests running
// with a frozen counter (HealthStalled), and one request in a prefill that never
// produces a token (HealthSlowPrefill, "one with no token for 30 min is"
// restarted). The clock starts at the first sample, so the restart is due at
// first sample + 30 min; no failing event may come earlier.
func TestHealthNoTokenProgressRestartsWorker(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    func(time.Time, afake.Process) agentapi.Health
	}{{"stalled", afake.HealthStalled}, {"slow-prefill-never-ends", afake.HealthSlowPrefill}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &healthSampled{h: tc.h}
			rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: s.sample} })
			at := healthUntilEvent(rg, in.Name, 0, 45*time.Minute, startTick)
			want := s.first.Add(30 * time.Minute)
			if at.Before(want) || at.After(want.Add(startTick)) {
				t.Errorf("restart at %s, want within [%s, +%s] (first sample %s + 30m)", at.Format(time.TimeOnly), want.Format(time.TimeOnly), startTick, s.first.Format(time.TimeOnly))
			}
			if got := healthFailing(rg, in.Name); !slices.Equal(got, []string{"WorkerRestart"}) {
				t.Errorf("failing events = %v, want [WorkerRestart]", got)
			}
			if a := rg.instance(in.Name).Annotations; a[v1alpha1.RestartGenerationAnnotation(healthWorker)] != "1" || a[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance)] != "" {
				t.Errorf("annotations = %v, want restart-worker-0 1 only", a)
			}
		})
	}
}

// TestHealthPlainLoadAndShortPrefillRestartNothing covers "Plain load with long
// queues restarts nothing" over an hour (256 running, 1000 waiting, a full KV
// cache, tokens rising) and "an engine double in prefill for 1,100 s ... is not
// restarted": one request in prefill for 1,100 s (the double's sample), then
// tokens flow. The stall tests above show the same rules do fire.
func TestHealthPlainLoadAndShortPrefillRestartNothing(t *testing.T) {
	prefill := func() afake.Engine {
		s := &healthSampled{}
		var tokens int64
		s.h = func(now time.Time, w afake.Process) agentapi.Health {
			if now.Sub(s.first) < 1100*time.Second {
				return afake.HealthSlowPrefill(now, w)
			}
			tokens += 10
			return agentapi.Health{Time: now, ProbeOK: true, RunningRequests: 1, GeneratedTokens: tokens, KVCacheUsage: 0.05}
		}
		return afake.Engine{Health: s.sample}
	}
	for _, tc := range []struct {
		name string
		mk   func() afake.Engine
		run  time.Duration
	}{
		{"plain-load-one-hour", func() afake.Engine { return afake.Engine{Health: afake.HealthPlainLoad()} }, time.Hour},
		{"prefill-1100s-then-tokens", prefill, 40 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in, alloc := healthRig(t, tc.mk)
			for end := rg.clock.Now().Add(tc.run); rg.clock.Now().Before(end); {
				startStep(rg, "md", startTick)
				if st := rg.instance(in.Name); st.Status.Phase != v1alpha1.InstanceReady || len(rg.rec.Find("failing", "")) != 0 || len(st.Annotations) != 0 {
					t.Fatalf("%s: phase %s, failing events %v, annotations %v; nothing may restart", rg.clock.Now().Format(time.TimeOnly), st.Status.Phase, rg.rec.Find("failing", ""), st.Annotations)
				}
			}
			rep, _ := rg.agentOf(alloc).agent.LastReport()
			h := rep.Instance.Workers[0].Health
			if h == nil || h.RunningRequests == 0 {
				t.Errorf("the engine reported no requests in flight (%+v); the test would prove nothing", h)
			}
		})
	}
}

// TestHealthRepeatedFailuresClimbLadder covers "repeated failures climb the
// ladder in order: worker, worker, instance, instance, then the Instance fails
// and a new Allocation is created". Each crash comes as soon as the new process
// is live, so the controller must wait out restart.backoff (30 s): consecutive
// restarts are 30 s apart, never closer (a ladder that ignored the backoff
// would restart 10 s apart).
func TestHealthRepeatedFailuresClimbLadder(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
	ag := rg.agentOf(alloc).agent
	var times []time.Time
	for k := 0; k < 5; k++ {
		starts := len(ag.Starts())
		ag.CrashWorker(healthWorker, 1)
		times = append(times, healthUntilEvent(rg, in.Name, k, 3*time.Minute, startTick))
		if k < 4 {
			for len(ag.Starts()) == starts {
				startStep(rg, "md", startTick)
			}
		}
	}
	want := []string{"WorkerRestart", "WorkerRestart", "InstanceRestart", "InstanceRestart", v1alpha1.ReasonRestartLoop}
	if got := healthFailing(rg, in.Name); !slices.Equal(got, want) {
		t.Fatalf("failing events = %v, want %v", got, want)
	}
	for k := 1; k < len(times); k++ {
		if gap := times[k].Sub(times[k-1]); gap < 30*time.Second || gap > 30*time.Second+startTick {
			t.Errorf("event %d came %s after event %d, want 30 s (restart.backoff) to 40 s", k+1, gap, k)
		}
	}
	got := rg.instance(in.Name)
	if got.Status.Phase != v1alpha1.InstanceEnded || got.Status.EndReason != v1alpha1.ReasonRestartLoop ||
		got.Annotations[v1alpha1.RestartGenerationAnnotation(healthWorker)] != "2" || got.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartInstance)] != "2" {
		t.Errorf("Instance %s (%s), annotations %v; want ended/RestartLoop with worker-0 2 and instance 2", got.Status.Phase, got.Status.EndReason, got.Annotations)
	}
	startUntil(rg, "md", 3*time.Minute, "the failed Allocation released and a new one provisioned", func() bool {
		_, alive := rg.allocation(alloc)
		return !alive && len(rg.allocations("md")) == 1 && rg.submissions() == 2
	})
}

// TestHealthIgnoredTermIsKilledAfterStopGrace covers "A process that ignores TERM
// is killed after stopGrace and the restart completes". The profile sets
// restart.stopGrace 45 s (the default is 10 s) so the desired state can carry it
// only from the profile. The restart is triggered while the worker lives (a
// stall), since a crashed worker is never sent TERM. Expected, with 5 s Steps:
// the agent sees the raised generation at I, reports the worker stopping until
// I+45 s, when it is killed (exit -1), and starts the new process at I+50 s.
func TestHealthIgnoredTermIsKilledAfterStopGrace(t *testing.T) {
	const tick = 5 * time.Second
	s := &healthSampled{h: afake.HealthStalled}
	rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{StopGrace: dur(45 * time.Second)}
	}))
	var instruction time.Time
	rg.onAgent = func(ra *rigAgent) {
		ra.agent.OnDesired = func(d agentapi.DesiredState) {
			if d.Instance != nil && d.Instance.Workers[0].Generation == 1 && instruction.IsZero() {
				instruction = rg.clock.Now()
			}
		}
	}
	rg.engine = func(string) afake.Engine { return afake.Engine{IgnoreTerm: true, Health: s.sample} }
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	ag := rg.agentOf(alloc).agent
	var stopping, killed bool
	for len(ag.Starts()) < 2 {
		startStep(rg, "md", tick)
		if rep, ok := ag.LastReport(); ok && rep.Instance != nil && len(rep.Instance.Workers) > 0 {
			w := rep.Instance.Workers[0]
			stopping = stopping || w.Phase == agentapi.PartStopping
			killed = killed || (w.Phase == agentapi.PartStopped && w.ExitCode != nil && *w.ExitCode == -1)
		}
		if len(ag.Starts()) == 1 && rg.clock.Now().After(s.first.Add(40*time.Minute)) {
			t.Fatal("no restart within 40 minutes of the first sample")
		}
	}
	started := ag.Starts()[1].Time
	if instruction.IsZero() || started.Sub(instruction) < 45*time.Second || started.Sub(instruction) > 45*time.Second+2*tick {
		t.Errorf("new process started %s after the instruction %s, want 45 s (stopGrace) to 55 s", started.Sub(instruction), instruction.Format(time.TimeOnly))
	}
	if !stopping || !killed {
		t.Errorf("worker reported stopping=%v and killed-with-exit--1=%v; the TERM-ignoring process must be stopping, then killed", stopping, killed)
	}
	var grace time.Duration
	for _, d := range ag.Seen() {
		grace = time.Duration(d.Settings.StopGrace)
	}
	if grace != 45*time.Second {
		t.Errorf("desired state carries stopGrace %s, want the profile's 45s", grace)
	}
	startUntil(rg, "md", 10*time.Minute, "the restart completes", func() bool {
		st := rg.instance(in.Name).Status
		return st.Phase == v1alpha1.InstanceReady && st.VerificationRecord == verifRecordName(in, 2)
	})
}

// TestHealthCleanExitCountsAsFailed covers "A part that exits cleanly without
// being told to counts as failed": exit code 0 raises a worker restart.
func TestHealthCleanExitCountsAsFailed(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
	ag := rg.agentOf(alloc).agent
	ag.CrashWorker(healthWorker, 0)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	rep, _ := ag.LastReport()
	if w := rep.Instance.Workers[0]; w.ExitCode == nil || *w.ExitCode != 0 {
		t.Fatalf("worker report %+v, want a clean exit with code 0", w)
	}
	if got := healthFailing(rg, in.Name); !slices.Equal(got, []string{"WorkerRestart"}) {
		t.Errorf("failing events = %v, want [WorkerRestart]", got)
	}
	startUntil(rg, "md", 10*time.Minute, "ready again", func() bool { return rg.readyCount("md") == 1 && len(ag.Starts()) == 2 })
}

// TestHealthRestartEventPrecedesGeneration covers "Each restart's event is
// emitted before the agent sees the raised generation", twice: the agent's
// OnDesired hook finds the event already recorded when a generation rises in the
// desired state, and an interceptor on the Instance's update finds it recorded
// when the generation is written. Non-vacuity: both fire for a worker and for an
// Instance restart (attemptsPerRung 1).
func TestHealthRestartEventPrecedesGeneration(t *testing.T) {
	var rg *rig
	writes := map[string]int{}
	check := func(part, code string, gen int64, where string) {
		if n := int64(len(rg.rec.Find("failing", code))); n < gen {
			t.Errorf("%s: %s generation %d is raised, but only %d failing/%s events exist", where, part, gen, n, code)
		}
	}
	codes := map[string]string{healthWorker: "WorkerRestart", v1alpha1.PartInstance: "InstanceRestart"}
	rg = newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1))}
	}), withInterceptors(interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if in, ok := obj.(*v1alpha1.Instance); ok {
			var cur v1alpha1.Instance
			if c.Get(ctx, client.ObjectKeyFromObject(in), &cur) == nil {
				for part, code := range codes {
					key := v1alpha1.RestartGenerationAnnotation(part)
					if in.Annotations[key] != cur.Annotations[key] {
						var gen int64
						_, _ = fmt.Sscan(in.Annotations[key], &gen)
						check(part, code, gen, "Update")
						writes[part]++
					}
				}
			}
		}
		return c.Update(ctx, obj, opts...)
	}}))
	seen := map[string]int{}
	rg.onAgent = func(ra *rigAgent) {
		last := map[string]int64{}
		ra.agent.OnDesired = func(d agentapi.DesiredState) {
			if d.Instance == nil {
				return
			}
			for part, gen := range map[string]int64{healthWorker: d.Instance.Workers[0].Generation, v1alpha1.PartInstance: d.Instance.Generation} {
				if gen > last[part] {
					check(part, codes[part], gen, "OnDesired")
					seen[part]++
				}
				last[part] = gen
			}
		}
	}
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	for k, code := range []string{"WorkerRestart", "InstanceRestart"} {
		rg.agentOf(alloc).agent.CrashWorker(healthWorker, 1)
		healthUntilEvent(rg, in.Name, k, 3*time.Minute, startTick)
		startUntil(rg, "md", 10*time.Minute, "ready after "+code, func() bool { return rg.readyCount("md") == 1 })
	}
	for _, part := range []string{healthWorker, v1alpha1.PartInstance} {
		if writes[part] != 1 || seen[part] != 1 {
			t.Errorf("%s: %d generation writes and %d generation rises seen by the agent, want 1 and 1", part, writes[part], seen[part])
		}
	}
}

// TestHealthRestartRunsOriginalLaunch covers "A restart runs the Instance's
// original launch, even after the ModelDeployment's serving spec has changed":
// the spec gains max-num-seqs 128 (capacity, not an ID input), the worker is
// restarted, and the new process starts from the launch of the first start, with
// --max-num-seqs 256.
func TestHealthRestartRunsOriginalLaunch(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
	ag := rg.agentOf(alloc).agent
	rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) {
		provisionFlags(md, "max-num-seqs", "128")
	})
	if cmd := rg.md("md").Spec.Source.Served.Flags; !slices.Contains(cmd, v1alpha1.EngineFlag{Name: "max-num-seqs", Value: "128"}) {
		t.Fatalf("the spec was not changed: %v", cmd)
	}
	ag.CrashWorker(healthWorker, 1)
	startUntil(rg, "md", 10*time.Minute, "restarted and ready", func() bool { return len(ag.Starts()) == 2 && rg.readyCount("md") == 1 })
	st := ag.Starts()
	want := specLaunchArgv("--max-model-len=32768", "--max-num-seqs=256")
	if !reflect.DeepEqual(st[0].Launch, st[1].Launch) || !slices.Equal(st[1].Launch.Command, want) {
		t.Errorf("restart launch %q differs from the first %q, or from the original argv %q (no 128)", st[1].Launch.Command, st[0].Launch.Command, want)
	}
	if !reflect.DeepEqual(st[1].Launch.Command, in.Spec.Launch.Command) {
		t.Errorf("restart launch %v is not the Instance's spec.launch %v", st[1].Launch.Command, in.Spec.Launch.Command)
	}
}

// TestHealthMissedDesiredStateCorrectedByNextPoll covers "A missed desired-state
// message is corrected by the next poll": the controller raises the worker
// generation while the agent is silent for one Step; the agent has not applied
// it then, applies it at its next poll, and nothing else restarts.
func TestHealthMissedDesiredStateCorrectedByNextPoll(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	_, alloc := startReady(rg)
	ag := rg.agentOf(alloc).agent
	ag.CrashWorker(agentapi.WorkerName(0), 1)
	startStep(rg, "md", startTick) // the controller hears the exit and raises the generation
	if n := len(rg.rec.Find("failing", "WorkerRestart")); n != 1 {
		t.Fatalf("worker restarts = %d after the crash, want 1", n)
	}
	before, _ := ag.LastReport()
	ag.Silence(true)
	startStep(rg, "md", startTick) // the agent misses the raised generation
	if after, _ := ag.LastReport(); !after.Time.Equal(before.Time) || len(ag.Starts()) != 1 {
		t.Fatalf("the silent agent acted: last report %s -> %s, %d starts", before.Time, after.Time, len(ag.Starts()))
	}
	ag.Silence(false)
	startStep(rg, "md", startTick)
	st := ag.Starts()
	if len(st) != 2 || st[1].Generation != 1 {
		t.Fatalf("starts after the next poll = %+v, want a second process for worker generation 1", st)
	}
	startUntil(rg, "md", 5*time.Minute, "ready again", func() bool { return rg.readyCount("md") == 1 })
	if n := len(rg.rec.Find("failing", "")); n != 1 {
		t.Errorf("failing events = %d, want only the one restart", n)
	}
}

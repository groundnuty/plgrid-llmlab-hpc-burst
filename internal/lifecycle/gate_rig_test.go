package lifecycle

// The throughput gate through the pass (HBEP-7, Test plan, Over-allocation and
// the throughput gate, the gate's rows). This file holds the rows where the gate
// passes or records only; gate_act_rig_test.go holds the rows where it acts. The
// fixture is the HBEP's: one entry for the Instance's ID, with max-num-seqs
// 16, 8 sessions, and contexts of 1,024 and outputs of 128 tokens, at 20 ms per
// output token and 400 ms to first token, the default tolerance, dated 10 days
// before the rig clock (2026-11-02 08:00 UTC, so 2026-10-23). Every measurement
// has 300 ms to first token unless a row says otherwise. The entry and the fake
// agent's timings are separate literals in each row: no entry is written from an
// Instance's status.throughput or from the agent's timings, and no expected
// limit or verdict comes from the gate's helpers.

import (
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// gateMD is the fixture deployment: max-num-seqs 16, targetSessions 8, typical
// contexts of 1,024 and outputs of 128 tokens.
func gateMD(edit ...func(*v1alpha1.ModelDeployment)) *v1alpha1.ModelDeployment {
	md := mdSpec("md")
	s := md.Spec.Source.Served
	s.Flags = append(s.Flags, v1alpha1.EngineFlag{Name: "max-num-seqs", Value: "16"})
	md.Spec.Workload = &v1alpha1.DeploymentWorkload{
		ContextTokens: v1alpha1.ContextTokens{Typical: 1024}, OutputTokens: v1alpha1.OutputTokens{Typical: 128}, TargetSessions: 8,
	}
	for _, e := range edit {
		e(md)
	}
	return md
}

// gateFlag sets an engine flag of the fixture deployment, adding it when absent.
func gateFlag(name, value string) func(*v1alpha1.ModelDeployment) {
	return func(md *v1alpha1.ModelDeployment) {
		s := md.Spec.Source.Served
		for i := range s.Flags {
			if s.Flags[i].Name == name {
				s.Flags[i].Value = value
				return
			}
		}
		s.Flags = append(s.Flags, v1alpha1.EngineFlag{Name: name, Value: value})
	}
}

// gateEntryFor is the fixture's entry for an Instance with deployment ID id.
func gateEntryFor(id string, edit ...func(*v1alpha1.ExpectedPerformance)) v1alpha1.ExpectedPerformance {
	e := gateFixtureEntry(gateKey())
	e.DeploymentID = id
	for _, f := range edit {
		f(&e)
	}
	return e
}

// gateRound is an engine's warm-up round with the given concurrent timings: its
// total is the first token's time plus the output's other 127 tokens at tpot,
// so the controller measures exactly tpot per output token. The other shapes
// are DefaultWarmUpRound's, the same in every round, so it settles at round 2.
func gateRound(tpot, ttft time.Duration) func(int32, agentapi.WarmUpRequest) agentapi.WarmUpRound {
	return func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
		round := afake.DefaultWarmUpRound(n, req)
		for i := range round.Requests {
			if r := &round.Requests[i]; r.Shape == agentapi.ShapeConcurrent {
				r.TimeToFirstToken = agentapi.Duration(ttft)
				r.Total = agentapi.Duration(ttft + tpot*time.Duration(r.OutputTokens-1))
			}
		}
		return round
	}
}

// gateEntries installs the profile's entries once the deployment's Instance
// exists, so its ID is known, and before any warm-up has run: the pass reads the
// profile afresh every time. It returns the Instance's name and ID.
func gateEntries(rg *rig, md *v1alpha1.ModelDeployment, entries func(id string) []v1alpha1.ExpectedPerformance) (name, id string) {
	rg.t.Helper()
	rg.create(md)
	startUntil(rg, "md", 2*time.Minute, "an Instance", func() bool { return len(rg.instances("md")) == 1 })
	in := rg.instances("md")[0]
	if isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp) || in.Status.Phase == v1alpha1.InstanceReady {
		rg.t.Fatalf("the Instance is already warmed up (%s): the entries come too late for the fixture", in.Status.Phase)
	}
	id = in.Spec.DeploymentID.Display()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = entries(id)
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
	return in.Name, id
}

// gateReady runs one Instance with timings tpot and 300 ms to first token (the
// row's ttft when given) to ready, and returns it and the rig.
func gateReady(t *testing.T, tpot time.Duration, md *v1alpha1.ModelDeployment, entries func(id string) []v1alpha1.ExpectedPerformance, ttft ...time.Duration) (*rig, v1alpha1.Instance) {
	t.Helper()
	first := 300 * time.Millisecond
	if len(ttft) > 0 {
		first = ttft[0]
	}
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{WarmUpRound: gateRound(tpot, first)} }
	gateEntries(rg, md, entries)
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	return rg, rg.instances("md")[0]
}

func oneEntry(edit ...func(*v1alpha1.ExpectedPerformance)) func(string) []v1alpha1.ExpectedPerformance {
	return func(id string) []v1alpha1.ExpectedPerformance {
		return []v1alpha1.ExpectedPerformance{gateEntryFor(id, edit...)}
	}
}

// A pass and a record-only verdict leave the Instance ready as it was before the
// gate judged: no restart, no end, and status.gate keeps no misses.
func gateRecordsOnly(t *testing.T, in v1alpha1.Instance, rg *rig) {
	t.Helper()
	if in.Status.Phase != v1alpha1.InstanceReady || len(in.Status.Restarts) != 0 || in.Status.EndReason != "" {
		t.Errorf("phase %s, restarts %+v, end reason %q: the gate acted", in.Status.Phase, in.Status.Restarts, in.Status.EndReason)
	}
	if got := rg.rec.Find("failing", "WorkerRestart"); len(got) != 0 {
		t.Errorf("a restart event was recorded: %v", got)
	}
	if g := in.Status.Gate; g != nil && len(g.Misses) != 0 {
		t.Errorf("status.gate holds misses %+v: nothing missed", g.Misses)
	}
}

// The limits (HBEP-7, Test plan, The gate): 40 ms and max(800, 500) = 800 ms.
// Each limit has its pass here, just inside it, naming both limits in
// Qualified's message and leaving the Instance ready with no restart; its miss,
// just over it, is a row of gate_act_rig_test.go.
func TestGatePassesWithinEachLimit(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		name  string
		tpot  time.Duration
		ttft  time.Duration
		edit  []func(*v1alpha1.ExpectedPerformance)
		wants []string
	}{
		{"39 ms passes", 39 * ms, 300 * ms, nil,
			[]string{"passed the entry dated 2026-10-23", "time per output token 39ms (limit 40ms)", "time to first token 300ms (limit 800ms)"}},
		{"790 ms to first token passes", 20 * ms, 790 * ms, nil, []string{"time to first token 790ms (limit 800ms)"}},
		{"entry ttft 50 ms: 140 ms passes", 20 * ms, 140 * ms, []func(*v1alpha1.ExpectedPerformance){func(e *v1alpha1.ExpectedPerformance) { e.TimeToFirstToken = "50ms" }},
			[]string{"time to first token 140ms (limit 150ms)"}},
		{"tolerance 50: 30 ms passes", 30 * ms, 300 * ms, []func(*v1alpha1.ExpectedPerformance){func(e *v1alpha1.ExpectedPerformance) { e.TolerancePercent = ptrTo(int32(50)) }},
			[]string{"time per output token 30ms (limit 30ms)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := gateReady(t, tc.tpot, gateMD(), oneEntry(tc.edit...), tc.ttft)
			msg := conditionMessage(&in, v1alpha1.ConditionQualified)
			for _, want := range tc.wants {
				if !strings.Contains(msg, want) {
					t.Errorf("Qualified's message %q does not hold %q", msg, want)
				}
			}
			if !strings.HasPrefix(msg, "every check passed; the behaviour check ran at ") || !strings.Contains(msg, " context tokens; throughput gate: ") ||
				strings.Contains(msg, "judged after warm-up") || strings.Contains(msg, "alpha") {
				t.Errorf("Qualified's message %q: want the checks' text with the behaviour check's context, then the gate's verdict in place of the alpha sentence", msg)
			}
			if c := apimeta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionQualified); c == nil || c.Status != "True" || c.Reason != v1alpha1.ReasonPassed {
				t.Errorf("Qualified is %+v, want True/Passed", c)
			}
			if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || g.ReferenceDate != "2026-10-23" {
				t.Errorf("status.gate %+v, want Passed against 2026-10-23", g)
			}
			gateRecordsOnly(t, in, rg)
		})
	}
}

// With no matching entry the gate records only, whatever the measurement: the
// same 41 ms that would miss passes with no restart, and the message says why.
func TestGateRecordsOnlyWithNoMatchingEntry(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		name    string
		md      []func(*v1alpha1.ModelDeployment)
		entries func(string) []v1alpha1.ExpectedPerformance
		wants   []string
		date    string
	}{
		{"max-num-seqs 32", []func(*v1alpha1.ModelDeployment){gateFlag("max-num-seqs", "32")},
			oneEntry(), []string{"no entry matches", "max-num-seqs=32"}, ""},
		{"targetSessions 16", []func(*v1alpha1.ModelDeployment){func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = 16 }},
			oneEntry(), []string{"no entry matches", "16 sessions"}, ""},
		{"a typical output of 256", []func(*v1alpha1.ModelDeployment){func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.OutputTokens.Typical = 256 }},
			oneEntry(), []string{"no entry matches", "256 output tokens"}, ""},
		{"another deployment ID", nil,
			func(string) []v1alpha1.ExpectedPerformance {
				return []v1alpha1.ExpectedPerformance{gateEntryFor("ba9876543210")}
			},
			[]string{"no entry matches", "deployment ID "}, ""},
		{"an entry dated 2026-08-04, 90 days old since 2026-11-02 00:00 UTC", nil,
			oneEntry(func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-08-04" }),
			[]string{"records only", "the matching entry dated 2026-08-04 is 90 days or more old"}, "2026-08-04"},
		{"two entries with one key", nil,
			func(id string) []v1alpha1.ExpectedPerformance {
				return []v1alpha1.ExpectedPerformance{gateEntryFor(id), gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-10-20" })}
			},
			[]string{"records only", "2 entries match", "2026-10-20", "2026-10-23"}, ""},
		{"two entries equal only by sameSetting, 0.9 and 0.90", []func(*v1alpha1.ModelDeployment){gateFlag("gpu-memory-utilization", "0.9")},
			func(id string) []v1alpha1.ExpectedPerformance {
				flags := func(util string) func(*v1alpha1.ExpectedPerformance) {
					return func(e *v1alpha1.ExpectedPerformance) {
						e.CapacityFlags = gateFlags("max-num-seqs", "16", "gpu-memory-utilization", util)
					}
				}
				return []v1alpha1.ExpectedPerformance{
					gateEntryFor(id, flags("0.9"), func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-10-20" }),
					gateEntryFor(id, flags("0.90")),
				}
			},
			[]string{"records only", "2 entries match", "2026-10-20", "2026-10-23"}, ""},
		{"an entry with a date that does not parse", nil,
			oneEntry(func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-13-45" }),
			[]string{"no entry matches", `"2026-13-45"`}, ""},
		{"a profile with no entry", nil,
			func(string) []v1alpha1.ExpectedPerformance { return nil }, []string{"records only", "no entry matches"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := gateReady(t, 41*ms, gateMD(tc.md...), tc.entries)
			msg := conditionMessage(&in, v1alpha1.ConditionQualified)
			for _, want := range append(tc.wants, "every check passed; the behaviour check ran at ", " context tokens; throughput gate: records only: ") {
				if !strings.Contains(msg, want) {
					t.Errorf("Qualified's message %q does not hold %q", msg, want)
				}
			}
			if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || g.ReferenceDate != tc.date {
				t.Errorf("status.gate %+v, want RecordOnly with reference date %q", g, tc.date)
			}
			gateRecordsOnly(t, in, rg)
		})
	}
}

// An entry 90 days from its date gates the day before and records only on it:
// 2026-08-05 ages at 2026-11-03 00:00 UTC, 16 hours after the rig's clock starts,
// so 41 ms is judged against it: its first miss restarts the worker. (The
// boundary to the second is in TestMatchReferenceAgeBoundary, on the matcher with
// the clock handed in.)
func TestGateStillGatesAnEntryOnItsLastDay(t *testing.T) {
	rg := newRig(t)
	rg.engine = gateSequence(41 * time.Millisecond)
	name, _ := gateEntries(rg, gateMD(), oneEntry(func(e *v1alpha1.ExpectedPerformance) { e.Date = "2026-08-05" }))
	startUntil(rg, "md", 10*time.Minute, "the first miss's restart", func() bool { return len(rg.rec.Find("failing", "WorkerRestart")) == 1 })
	in := rg.instance(name)
	if g := in.Status.Gate; g == nil || len(g.Misses) != 1 || g.ReferenceDate != "2026-08-05" || len(in.Status.Restarts) != 1 || in.Status.Restarts[0].Signal != "ThroughputMissed" {
		t.Errorf("status.gate %+v, restarts %+v, want the entry's first miss recorded with its ThroughputMissed restart", g, in.Status.Restarts)
	}
}

// The capacity flags are part of the key (HBEP-4): an Instance declaring
// gpu-memory-utilization 0.90 matches an entry with 0.9, not one with 0.85; an
// entry carrying enable-prefix-caching that the Instance does not declare
// matches nothing.
func TestGateKeysOnCapacityFlags(t *testing.T) {
	declares := gateFlag("gpu-memory-utilization", "0.90")
	flagsOf := func(kv ...string) func(*v1alpha1.ExpectedPerformance) {
		return func(e *v1alpha1.ExpectedPerformance) { e.CapacityFlags = gateFlags(kv...) }
	}
	for _, tc := range []struct {
		name    string
		md      func(*v1alpha1.ModelDeployment)
		entry   func(*v1alpha1.ExpectedPerformance)
		matches bool
	}{
		{"0.90 against an entry's 0.9", declares, flagsOf("max-num-seqs", "16", "gpu-memory-utilization", "0.9"), true},
		{"0.90 against an entry's 0.85", declares, flagsOf("max-num-seqs", "16", "gpu-memory-utilization", "0.85"), false},
		{"an entry's flag the Instance does not declare", func(*v1alpha1.ModelDeployment) {}, flagsOf("max-num-seqs", "16", "enable-prefix-caching", "true"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := gateReady(t, 39*time.Millisecond, gateMD(tc.md), oneEntry(tc.entry))
			msg, g := conditionMessage(&in, v1alpha1.ConditionQualified), in.Status.Gate
			if tc.matches && (g == nil || g.Verdict != v1alpha1.GatePassed || !strings.Contains(msg, "passed the entry dated 2026-10-23")) {
				t.Errorf("status.gate %+v, message %q, want Passed against the entry", g, msg)
			}
			if !tc.matches && (g == nil || g.Verdict != v1alpha1.GateRecordOnly || !strings.Contains(msg, "no entry matches")) {
				t.Errorf("status.gate %+v, message %q, want RecordOnly, no entry matches", g, msg)
			}
			gateRecordsOnly(t, in, rg)
		})
	}
}

// Once the gate has judged, no later warm-up is: a ready Instance restarted at
// rung 1 for a malfunction, then measuring 41 ms, keeps its Passed verdict and
// its message, though its status.throughput records the new measurement.
func TestGateJudgesTheFirstQualificationOnly(t *testing.T) {
	tpot := 39 * time.Millisecond
	rg := newRig(t)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			return gateRound(tpot, 300*time.Millisecond)(n, req)
		}}
	}
	name, _ := gateEntries(rg, gateMD(), oneEntry())
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	before := rg.instance(name)
	msg := conditionMessage(before, v1alpha1.ConditionQualified)
	if g := before.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || !strings.Contains(msg, "passed the entry dated 2026-10-23") {
		t.Fatalf("before the restart: status.gate %+v, message %q, want Passed", g, msg)
	}

	tpot = 41 * time.Millisecond // what the restarted worker's warm-up measures
	rg.agentOf(before.Spec.AllocationRef.Name).agent.CrashWorker(agentapi.WorkerName(0), 1)
	startUntil(rg, "md", 10*time.Minute, "the restarted Instance ready with the new measurement", func() bool {
		in := rg.instance(name)
		return len(rg.rec.Find("failing", "WorkerRestart")) == 1 && in.Status.Phase == v1alpha1.InstanceReady &&
			in.Status.Throughput != nil && in.Status.Throughput.TimePerOutputToken == "41ms"
	})
	after := rg.instance(name)
	if g := after.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || g.ReferenceDate != "2026-10-23" {
		t.Errorf("after the restart: status.gate %+v, want the Passed verdict kept", g)
	}
	if got := conditionMessage(after, v1alpha1.ConditionQualified); got != msg {
		t.Errorf("Qualified's message changed on a later warm-up: %q, was %q", got, msg)
	}
	if len(after.Status.Restarts) != 1 || after.Status.EndReason != "" {
		t.Errorf("restarts %+v, end reason %q, want the one malfunction restart and no end", after.Status.Restarts, after.Status.EndReason)
	}
}

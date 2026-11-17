package lifecycle

// The key's shape is the one the warm-up ran (HBEP-7, The throughput gate: its reference; #616 PR 2, carried from PR
// 1's hand-back): in this version a mis-keyed measurement can release an Allocation, so the gate keys on what ran, from
// the verification record the warm-up was asked with and the settled round itself, never from the workload as it is
// declared when the gate judges. The fixture is gate_rig_test.go's (8 sessions, contexts of 1,024, outputs of 128
// tokens), with a second entry for the shape a row's edit would give: it holds the limit that would release, so a
// mis-keyed row would restart or fail. A row that edits the workload does it inside the round the warm-up settles in,
// after the agent was asked and before the controller judges. 41 ms misses the strict entries (40 ms) and passes the
// generous ones (200 ms).

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// gateShapeEntries are two entries for the Instance's ID: the fixture's, for 8 sessions, with generous limits (200 ms
// per output token), and a strict one (20 ms, limit 40 ms) for the shape edit gives.
func gateShapeEntries(edit func(*v1alpha1.ExpectedPerformance)) func(string) []v1alpha1.ExpectedPerformance {
	return func(id string) []v1alpha1.ExpectedPerformance {
		generous := gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.TimePerOutputToken = "100ms" })
		strict := gateEntryFor(id, edit)
		return []v1alpha1.ExpectedPerformance{generous, strict}
	}
}

// gateRoundEdited is gateRound whose concurrent requests are changed by edit after it is built.
func gateRoundEdited(tpot time.Duration, edit func(*agentapi.WarmUpRound)) func(int32, agentapi.WarmUpRequest) agentapi.WarmUpRound {
	return func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
		round := gateRound(tpot, 300*time.Millisecond)(n, req)
		edit(&round)
		return round
	}
}

// A round that is not the shape the key names records only, saying why, and never fails or restarts: its concurrent
// requests number 4 where the worker's share is 8, or each produced 64 output tokens where the record's shapes ask
// for 128. The strict entry for that shape is there to release, and does not.
func TestGateRecordsOnlyWhenTheRoundIsNotTheShapeAskedFor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		round func(*agentapi.WarmUpRound)
		entry func(*v1alpha1.ExpectedPerformance)
		want  string
	}{
		{"four concurrent requests, not 8", func(r *agentapi.WarmUpRound) {
			kept := r.Requests[:0]
			concurrent := 0
			for _, q := range r.Requests {
				if q.Shape == agentapi.ShapeConcurrent {
					if concurrent++; concurrent > 4 {
						continue
					}
				}
				kept = append(kept, q)
			}
			r.Requests = kept
		}, func(e *v1alpha1.ExpectedPerformance) { e.Sessions = 4 }, "ran 4 concurrent requests, not the 8"},
		{"64 output tokens, not 128", func(r *agentapi.WarmUpRound) {
			for i := range r.Requests {
				if r.Requests[i].Shape == agentapi.ShapeConcurrent {
					r.Requests[i].OutputTokens = 64
				}
			}
		}, func(e *v1alpha1.ExpectedPerformance) { e.OutputTokens = 64 }, "produced 64 output tokens, not the 128"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			rg.engine = func(string) afake.Engine {
				return afake.Engine{WarmUpRound: gateRoundEdited(41*time.Millisecond, tc.round)}
			}
			gateEntries(rg, gateMD(), gateShapeEntries(tc.entry))
			startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
			in := rg.instances("md")[0]
			msg := conditionMessage(&in, v1alpha1.ConditionQualified)
			if !strings.Contains(msg, "records only: the warm-up did not run the shape its key names") || !strings.Contains(msg, tc.want) {
				t.Errorf("Qualified's message %q, want records only, saying the warm-up %q", msg, tc.want)
			}
			if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || g.ReferenceDate != "" {
				t.Errorf("status.gate %+v, want RecordOnly with no reference date", g)
			}
			gateRecordsOnly(t, in, rg)
		})
	}
}

// The workload edited while the warm-up runs, at its settling round. Sessions 8 to 16: the round ran 8 against the
// worker's share of 16 as declared now, so it is a mismatch and records only; keyed on the workload as declared it
// would have been judged against the strict entry for 16 sessions, 41 ms over its 40 ms, and restarted.
func TestGateDoesNotKeyOnAWorkloadEditedDuringTheWarmUp(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			if n == 2 {
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 16 })
			}
			return gateRound(41*time.Millisecond, 300*time.Millisecond)(n, req)
		}}
	}
	gateEntries(rg, gateMD(), gateShapeEntries(func(e *v1alpha1.ExpectedPerformance) { e.Sessions = 16 }))
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")[0]
	if msg := conditionMessage(&in, v1alpha1.ConditionQualified); !strings.Contains(msg, "records only: the warm-up did not run the shape its key names") ||
		!strings.Contains(msg, "ran 8 concurrent requests, not the 16") {
		t.Errorf("Qualified's message %q, want records only: 8 concurrent requests were run, not the 16 declared now", msg)
	}
	if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly {
		t.Errorf("status.gate %+v, want RecordOnly", g)
	}
	gateRecordsOnly(t, in, rg)
}

// The typical context edited from 1,024 to 2,048 while the warm-up runs. The verification record fixed the shapes the
// warm-up was asked with, 1,024, so the round is keyed on 1,024 whatever the workload says now: it is judged against
// the fixture's strict entry and its 41 ms misses, restarting the worker. (Keyed on the workload as declared, 2,048,
// no entry matches and nothing restarts.) The restarted worker verifies again under the workload as it is now, 2,048,
// so its retry, keyed on what it ran, matches no entry and records only: the Instance is ready.
func TestGateKeysOnTheContextTheRecordFixed(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			if n == 2 && req.Shapes.ContextTypical == 1024 {
				rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.ContextTokens.Typical = 2048 })
			}
			return gateRound(41*time.Millisecond, 300*time.Millisecond)(n, req)
		}}
	}
	name, _ := gateEntries(rg, gateMD(), oneEntry())
	startUntil(rg, "md", 10*time.Minute, "the first miss's restart", func() bool { return len(gateRestarts(rg, name)) > 0 })
	in := rg.instance(name)
	if sig := gateRestartSignals(in); len(sig) != 1 || sig[0] != "worker-0/ThroughputMissed" {
		t.Fatalf("recorded restarts %v, want worker-0/ThroughputMissed: the round ran 1,024 and the entry is for 1,024", sig)
	}
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	got := rg.instance(name)
	if msg := conditionMessage(got, v1alpha1.ConditionQualified); !strings.Contains(msg, "records only: no entry matches") || !strings.Contains(msg, "2048 context tokens") {
		t.Errorf("Qualified's message %q, want the retry, which ran 2,048, to match no entry", msg)
	}
	if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || len(g.Misses) != 1 || len(got.Status.Restarts) != 1 {
		t.Errorf("status.gate %+v, restarts %v, want RecordOnly with the first miss kept and the one restart", g, gateRestartSignals(got))
	}
}

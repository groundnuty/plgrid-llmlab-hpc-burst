package alpha

// The baseline row's judge and sequencer (baselinejudge.go, baselineflow.go) through Run and the dry world, with figures worked out by hand: a request's
// route is the FRONT's record's, a failure wins over INVALID, the guard stops the generator, the detach deletes the managed ModelDeployment, and the post
// carries what #764 asks of it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// runBaselineDry is one dry run of the baseline row through Run: its record directory, its failed checks, its error and the steps (for the world's calls).
func runBaselineDry(t *testing.T, fault string) (out string, failures []string, err error, st *drySteps) {
	t.Helper()
	return runBaselineDryWith(t, fault, DefaultPhases(CapSano))
}

// runBaselineDryWith is runBaselineDry with the phases the config holds.
func runBaselineDryWith(t *testing.T, fault string, phases []Phase) (out string, failures []string, err error, st *drySteps) {
	t.Helper()
	cfg := Config{Row: BaselineRow(), Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel,
		Phases: phases}
	rec, rerr := NewRecorder(cfg.Out)
	if rerr != nil {
		t.Fatal(rerr)
	}
	st = newDrySteps(t, cfg, "baseline")
	failures, err = Run(context.Background(), cfg, st, rec, t.Logf)
	return cfg.Out, failures, err, st
}

// obsOfRecord is the observation a run's record holds.
func obsOfRecord(t *testing.T, out string) BaselineObs {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(out, RecordFile))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Observation BaselineObs `json:"observation"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r.Observation
}

// Each fault that makes the reading measure nothing ends INVALID through Run: ErrBaselineInvalid with the reason's words, no failed check, never NOT RUN and
// never the bulk row's INVALID, the record's verdict "invalid" and the text to post headed INVALID. Faults here are a cap that never binds, a
// 503 in P3, and the rest of the INVALID rules.
func TestTheBaselinesInvalidRunsThroughRun(t *testing.T) {
	for _, c := range baselineInvalidCases {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err, _ := runBaselineDry(t, c.fault)
			if len(failures) != 0 {
				t.Fatalf("an INVALID run failed checks %q", failures)
			}
			var ie *BaselineInvalidError
			row := BaselineRow()
			if !errors.As(err, &ie) || !errors.Is(err, ErrBaselineInvalid) || !BaselineInvalidRun(row, err) || NotRun(row, err) || Invalid(row, err) {
				t.Fatalf("err = %v, want an INVALID baseline run alone", err)
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("the reason %q lacks %q", err.Error(), c.why)
			}
			if v := recordVerdict(t, out); v != "invalid" {
				t.Errorf("the record's verdict is %q, want invalid", v)
			}
			post, _ := os.ReadFile(filepath.Join(out, PostsFile))
			if !strings.HasPrefix(string(post), "beta-sano-llmlab-baseline: INVALID (posts to #764)\n") || !strings.Contains(string(post), "\n- the reading is invalid: ") || !strings.Contains(string(post), c.why) {
				t.Errorf("the text to post:\n%s", post)
			}
			if note := BaselineInvalidNote(row); !strings.Contains(note, baselineInvalidWords) || strings.Contains(note, c.why) {
				t.Errorf("the marker's note %q holds the fixed words alone", note)
			}
		})
	}
}

// A cap that never binds and the ruling on P3, read at their most direct: a P2 where the cap never bound is INVALID and not a pass; a 503 in P3 is INVALID; a logged
// KV under the cap's need ends the run before any phase.
func TestTheCapNeverBindingAndAThreeInP3AreInvalidNotPasses(t *testing.T) {
	_, failures, err, _ := runBaselineDry(t, "cap-never-binds")
	if len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) {
		t.Errorf("the cap never binding: failures %v, err %v, want INVALID", failures, err)
	}
	_, failures, err, _ = runBaselineDry(t, "detach-503")
	if len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) || !strings.Contains(err.Error(), "P3: a request was answered 503") {
		t.Errorf("a 503 in P3: failures %v, err %v, want INVALID naming P3", failures, err)
	}
	out, _, err, _ := runBaselineDry(t, "kv-below-the-cap")
	if !errors.Is(err, ErrBaselineInvalid) {
		t.Fatalf("a KV below the cap's need: %v", err)
	}
	if o := obsOfRecord(t, out); len(o.Phases) != 0 || !o.KVRead || o.LoggedKV != 100000 {
		t.Errorf("the run made %d phases with a logged KV of %d: none was to be run", len(o.Phases), o.LoggedKV)
	}
}

// A failure wins over INVALID: a run with a failed check and a reading that measures nothing is FAIL, the reasons stay in the observation, and Run returns
// the failures, not the INVALID error.
func TestAFailureWinsOverInvalidInTheBaseline(t *testing.T) {
	out, _, err, _ := runBaselineDry(t, "")
	if err != nil {
		t.Fatalf("the healthy run: %v", err)
	}
	o := obsOfRecord(t, out)
	// P2's LLMLab rows are taken out (the cap never bound: INVALID) and P1 is given an LLMLab request (a failure).
	for i := range o.Phases {
		switch o.Phases[i].Phase.Name {
		case PhaseP2:
			o.Phases[i].Rows = slices.DeleteFunc(o.Phases[i].Rows, func(r FrontRow) bool { return r.Emitter == "provider" })
			o.Phases[i].Gen = slices.DeleteFunc(o.Phases[i].Gen, func(g GenRow) bool { return g.Emitter == "provider" })
			o.Phases[i].LLMLabMetric = 0
		case PhaseP1:
			o.Phases[i].Rows = append(o.Phases[i].Rows, FrontRow{RequestID: "stray", Provider: LLMLabProviderName, Emitter: "provider", Status: 200, Start: o.Phases[i].Start, End: o.Phases[i].Start.Add(time.Second)})
			o.Phases[i].LLMLabMetric = 0
		}
	}
	fail, invalid := judgeBaseline(o)
	if len(fail) == 0 || len(invalid) == 0 {
		t.Fatalf("fail %q, invalid %q: want both", fail, invalid)
	}
	if w := baselineVerdictWord(fail, invalid); w != "FAIL" {
		t.Errorf("the verdict is %s, want FAIL", w)
	}
	post := baselinePost(BaselineRow(), o, fail, invalid)
	if !strings.HasPrefix(post, "beta-sano-llmlab-baseline: FAIL (posts to #764)\n- failed checks: ") || strings.Contains(post, "INVALID") {
		t.Errorf("the text to post:\n%s", post)
	}
}

// Every count is from the FRONT's usage records. The generator's own records name another route than the front's for every request; the judge reads
// the front's, so the reading is the healthy one (P2: exactly k = 2 first-turn requests answered by LLMLab) and the one failed check is the disagreement.
// A judge that counted from the generator's records would see LLMLab answer nothing, call the cap never bound, and fail the other way.
func TestTheJudgeCountsFromTheFrontsRecordsNotTheGenerators(t *testing.T) {
	out, _, err, _ := runBaselineDry(t, "")
	if err != nil {
		t.Fatal(err)
	}
	o := obsOfRecord(t, out)
	for i := range o.Phases {
		for j := range o.Phases[i].Gen {
			g := &o.Phases[i].Gen[j]
			g.Emitter, g.Provider, g.Instance = "instance", "alpha-dry", "default/dry-inst-1"
		}
	}
	fail, invalid := judgeBaseline(o)
	if len(invalid) != 0 {
		t.Errorf("INVALID %q: the judge read a route off the generator's records", invalid)
	}
	if len(fail) != 1 || !strings.HasPrefix(fail[0], "P2: ") || !strings.Contains(fail[0], "48 generator record(s) name another route than the front's usage record") {
		t.Fatalf("failed checks %q, want P2's disagreement alone (its 48 LLMLab requests): the other phases' routes agree", fail)
	}
	// What the front said is what the reading holds: P2's LLMLab requests, from the front's records.
	for _, po := range o.Phases {
		if po.Phase.Name != PhaseP2 {
			continue
		}
		joined, _, _, _ := joinPhase(po)
		llm := 0
		for _, r := range ReadRoutes(joined, o.LLMLabProvider) {
			if r.Route == RouteLLMLab {
				llm = r.Requests
			}
		}
		if llm != 2*24 {
			t.Errorf("P2's LLMLab requests are %d by the front's records, want 2 sessions x 24 turns = 48", llm)
		}
	}
}

// The guard: past 144 LLMLab requests in a phase the generator is STOPPED, not left to finish, the phase FAILS, and no later phase runs.
func TestTheLLMLabGuardStopsTheGeneratorAndFails(t *testing.T) {
	out, failures, err, st := runBaselineDry(t, "llmlab-over-its-guard")
	if err != nil {
		t.Fatalf("a guard that trips is a FAIL, not a broken run: %v", err)
	}
	if !strings.Contains(Summary(failures), "LLMLab was asked for more than 144 requests in one phase, so the generator was stopped") {
		t.Errorf("failures %q", failures)
	}
	o := obsOfRecord(t, out)
	names := []string{}
	var p2 PhaseObs
	for _, po := range o.Phases {
		names = append(names, po.Phase.Name)
		if po.Phase.Name == PhaseP2 {
			p2 = po
		}
	}
	if p2.Stopped != StopGuard {
		t.Errorf("P2 was stopped for %q, want %q", p2.Stopped, StopGuard)
	}
	if want := []string{PhaseP1, PhaseP1Long, PhaseP2}; !slices.Equal(names, want) {
		t.Errorf("the phases run were %v, want %v: nothing runs after a guard trips", names, want)
	}
	// The generator's own end: it was stopped by the sequencer, not by its work running out, which the dry world ends after two looks at the count.
	g := st.baseline.gens[3] // the ratio probe, P1, P1-long, then P2
	if !g.never {
		t.Fatal("the fault's generator ends by itself, so the test would not tell a stop from an end")
	}
	select {
	case <-g.Done():
	default:
		t.Error("the generator of the phase that tripped the guard was left running")
	}
}

// H2: the detach phase deletes the managed ModelDeployment first, and sees the front drop its target, before it sends one request; LLMLab then answers
// none of P3. When the managed ModelDeployment is not deleted, LLMLab answers the overflow and the phase FAILS.
func TestTheDetachPhaseDeletesTheManagedDeployment(t *testing.T) {
	out, failures, err, st := runBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("the healthy run: %v %v", failures, err)
	}
	i, j := slices.Index(st.baseline.calls, "DetachManaged"), slices.Index(st.baseline.calls, "StartGenerator P3")
	if i < 0 || j < 0 || i > j {
		t.Fatalf("the calls were %v: the managed ModelDeployment must be deleted before P3's generator starts", st.baseline.calls)
	}
	o := obsOfRecord(t, out)
	if !o.Detached {
		t.Error("the observation does not say the front dropped the managed target")
	}
	for _, po := range o.Phases {
		if po.Phase.Name == PhaseP3 && len(rowsFrom(po.Rows, LLMLabProviderName)) != 0 {
			t.Errorf("LLMLab answered %d request(s) of P3 after the detach", len(rowsFrom(po.Rows, LLMLabProviderName)))
		}
	}
	_, failures, err, st = runBaselineDry(t, "detach-skipped")
	if err != nil || !strings.Contains(Summary(failures), "request(s) after it was detached") || !strings.Contains(Summary(failures), "was not seen detached") {
		t.Errorf("with no deletion: failures %q, err %v", failures, err)
	}
	if slices.Contains(st.baseline.calls, "DetachManaged") && st.baseline.detached {
		t.Error("the fault did not skip the deletion")
	}
}

// The order of the steps: the managed leg and P0 before the GPU job, the logged KV after ready and before the gate, the gate before any phase, the phases in
// order with P1-long after the control and before the cap is exceeded, and the served deployment's end last.
func TestTheBaselinesStepsRunInTheirOrder(t *testing.T) {
	_, failures, err, st := runBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("%v %v", failures, err)
	}
	want := []string{"AttachManaged", "Probe", "StartServed", "LoggedKV", "Gate", "StartGenerator ratio", "StartGenerator P1", "StartGenerator P1-long", "StartGenerator P2", "DetachManaged", "StartGenerator P3", "EndServed"}
	if !slices.Equal(st.baseline.calls, want) {
		t.Errorf("the calls were\n%v\nwant\n%v", st.baseline.calls, want)
	}
}

// What the post carries (#764's item 4, the science agent's ruling H1): per route the sessions (distinct, and the most in flight), spills in and out,
// waits at the front ("0 by construction with LLMLab attached", labelled; P3's measured), time to first token p50 and p95, speed per session p50 and
// minimum, LLMLab's requests and tokens, and #769's mapping with each rule's text and line.
func TestTheBaselinePostCarriesWhatTheIssueAsks(t *testing.T) {
	out, failures, err, _ := runBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("%v %v", failures, err)
	}
	b, _ := os.ReadFile(filepath.Join(out, PostsFile))
	post := string(b)
	if !strings.HasPrefix(post, "beta-sano-llmlab-baseline: PASS (posts to #764)\n") {
		t.Errorf("the head:\n%s", post)
	}
	for _, w := range []string{
		"- P1 (control): N = 11, 24 turns, context 1024, turn 128, output 128 words, think 100ms, stagger 0s",
		"- P1-long (long): N = 11, 2 turns, context 24680",
		"- P2 (full): N = 13, 24 turns",
		"- P3 (detach): N = 13, 24 turns",
		// P2, by hand: 11 sessions on the Instance, 2 on LLMLab, 24 turns each: 264 and 48 requests.
		"  - the served Instance (Sano): 264 requests, 11 sessions (distinct), 11 at most in flight; spills in 0, out 48; errors 0; time to first token p50 50 ms, p95 50 ms;",
		"  - LLMLab (managed): 48 requests, 2 sessions (distinct), 2 at most in flight; spills in 48, out 0; errors 0; time to first token p50 300 ms, p95 300 ms;",
		"waits at the front: 0 by construction with LLMLab attached",
		"waits at the front, WITNESSED by the front's own count of requests in its FIFO (its /healthz waiting,",
		"- the cap is 11, a constant fixed before the run: floor(0.95 x 389120 / 32768)",
		"R4's declared charge for comparison: the typical shape's 1,152 tokens against KV 389120 is 337 sessions, capped at max-num-seqs 256, where the baseline's cap is 11",
		"- the mapping to #769 (hbep-764-amend at 9cc8368d)",
		"the integer budget, which reduces to today's count cap (hbep/0005-llmlab-compatible-front/README.md:314-318",
		"the budget in force at partial membership (hbep/0005-llmlab-compatible-front/README.md:319",
		"hpcburst_front_in_flight{workload, provider, model_deployment, instance_uid} (hbep/0008-observability-and-events/README.md:339)",
		"the FIFO per Workload, against today's single FIFO per ModelDeployment",
		"Workloads are routing inputs only",
	} {
		if !strings.Contains(post, w) {
			t.Errorf("the text to post lacks %q:\n%s", w, post)
		}
	}
	for _, w := range []string{"speed per session p50", "minimum", "spills in", "the engine, over"} {
		if !strings.Contains(post, w) {
			t.Errorf("the text to post lacks %q", w)
		}
	}
	for _, m := range Mapping769 {
		if !strings.Contains(post, m.Measurement) || !strings.Contains(post, m.Rule) || !strings.Contains(post, m.Where) {
			t.Errorf("the text to post lacks the mapping of %q", m.Measurement)
		}
	}
	// Counts and fixed labels: no instance, provider or deployment name of the run, no path.
	for _, w := range []string{"alpha-dry", "dry-inst", "default/dry", "/tmp"} {
		if strings.Contains(post, w) {
			t.Errorf("the text to post names %q", w)
		}
	}
}

// Every #769 rule the mapping names is a line of a file the baseline's own tree does not hold, so the test can only hold the table's shape: each entry
// names a measurement, a rule and a file with a line, and no two entries are one.
func TestTheMappingTo769NamesARuleAndWhereItIs(t *testing.T) {
	seen := map[string]bool{}
	for _, m := range Mapping769 {
		if m.Measurement == "" || m.Rule == "" || !strings.Contains(m.Where, "hbep/") || !strings.Contains(m.Where, "README.md:") || seen[m.Measurement] {
			t.Errorf("a mapping entry is incomplete or repeated: %+v", m)
		}
		seen[m.Measurement] = true
	}
	for _, rule := range []string{"integer budget", "FIFO per Workload", "partial membership", "instance_uid", "routing inputs only", "managedSessions"} {
		found := false
		for _, m := range Mapping769 {
			found = found || strings.Contains(m.Rule+m.Where+m.Measurement, rule)
		}
		if !found {
			t.Errorf("no mapping names %q, which the team lead asked for", rule)
		}
	}
}

// The usage records' intervals, by hand. A [0,4), B [1,3), C [2,6): the most at once is 3, over a limit of 1 the count is above it from t=1 to t=4 (3 s),
// and its top is 3.
func TestInFlightFromTheRecordsIntervals(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	rows := []FrontRow{{Start: at(0), End: at(4)}, {Start: at(1), End: at(3)}, {Start: at(2), End: at(6)}}
	if peak, over, top := inFlightStats(rows, 1); peak != 3 || over != 3*time.Second || top != 3 {
		t.Errorf("peak %d, longest over %s, top %d; want 3, 3s, 3", peak, over, top)
	}
	if peak, over, top := inFlightStats(rows, -1); peak != 3 || over != 0 || top != 0 {
		t.Errorf("with no limit: peak %d, over %s, top %d; want 3, 0, 0", peak, over, top)
	}
	// An interval ends before one that starts at the same instant: [0,1) and [1,2) are never two at once.
	if peak, _, _ := inFlightStats([]FrontRow{{Start: at(0), End: at(1)}, {Start: at(1), End: at(2)}}, -1); peak != 1 {
		t.Errorf("two back-to-back requests were counted %d at once", peak)
	}
	// The cap-spill rule, by hand (#776's review, point 5), cap 2, times in milliseconds: at least the cap in flight (Start <= t < End) at the arrival or within one tick
	// (1 ms) after it; a request that ends at t is released at t; no window before the arrival.
	ms := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Millisecond) }
	ours := []FrontRow{{Start: ms(1000), End: ms(9000)}, {Start: ms(1001), End: ms(9000)}}
	for _, c := range []struct {
		name    string
		served  []FrontRow
		arrival int
		held    bool
	}{
		{"both in flight at the arrival", ours, 5000, true},
		{"the second of ours is stamped one tick after the arrival: a healthy burst", ours, 1000, true},
		{"the second of ours is stamped two ticks after the arrival", ours, 999, false},
		{"the reviewer's case: none of ours at the arrival, both 50 ms later", []FrontRow{{Start: ms(1000), End: ms(9000)}, {Start: ms(1000), End: ms(9000)}}, 950, false},
		{"one of ours ends at the arrival: it is released at it", []FrontRow{{Start: ms(0), End: ms(5000)}, {Start: ms(0), End: ms(9000)}}, 5000, false},
		{"one of ours ends a tick after the arrival: still in flight", []FrontRow{{Start: ms(0), End: ms(5001)}, {Start: ms(0), End: ms(9000)}}, 5000, true},
		{"more than the cap in flight: the Instance is at its cap", append([]FrontRow{{Start: ms(0), End: ms(9000)}}, ours...), 5000, true},
		{"one short of the cap for the whole span", ours[:1], 5000, false},
		{"none of ours", nil, 5000, false},
	} {
		if got := heldItsCapAt(c.served, ms(c.arrival), 2); got != c.held {
			t.Errorf("%s: held its cap %t, want %t", c.name, got, c.held)
		}
	}
	if got := nearestRank([]float64{5, 1, 3, 2, 4}, 0.5); got != 3 {
		t.Errorf("p50 of 1..5 is %v, want 3", got)
	}
	if got := nearestRank([]float64{5, 1, 3, 2, 4}, 0.95); got != 5 {
		t.Errorf("p95 of 1..5 is %v, want 5", got)
	}
	if got := nearestRank([]float64{5, 1, 3, 2, 4}, 0); got != 1 {
		t.Errorf("the minimum of 1..5 is %v, want 1", got)
	}
}

// The live row reads the engine's logged KV with a command of its own on the login node. Run against a log shaped as vLLM v0.32.0 writes it (the line of
// kv_cache_utils.py:2327-2333, with its commas, once for each start), it prints the last start's number, and with no log it exits 3.
func TestTheEngineKVLineReadsTheLastStartsFigure(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "engine-worker-0.log")
	text := "---- 2026-11-18T14:20:00Z start: instance a\n(EngineCore pid=9) INFO 11-18 14:21:07 [kv_cache_utils.py:2327] GPU KV cache size: 100,000 tokens\n" +
		"(EngineCore pid=9) INFO 11-18 14:21:07 [kv_cache_utils.py:2331] Maximum concurrency for 32,768 tokens per request: 3.05x\n" +
		"---- 2026-11-18T14:30:00Z start: instance b\n(EngineCore pid=12) INFO 11-18 14:31:07 [kv_cache_utils.py:2327] GPU KV cache size: 389,120 tokens\n" +
		"(EngineCore pid=12) INFO 11-18 14:31:07 [kv_cache_utils.py:2331] Maximum concurrency for 32,768 tokens per request: 11.88x\n"
	if err := os.WriteFile(log, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	if !safePath.MatchString(log) {
		t.Fatalf("the test's path is not one the read accepts")
	}
	out, err := exec.Command("sh", "-c", engineKVLine(log)).Output()
	if err != nil {
		t.Fatal(err)
	}
	if kv, ok := ParseLoggedKV(out); !ok || kv != 389_120 {
		t.Errorf("read %q, parsed %d %t, want 389,120", out, kv, ok)
	}
	err = exec.Command("sh", "-c", engineKVLine(filepath.Join(dir, "none.log"))).Run()
	if ee := (*exec.ExitError)(nil); !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Errorf("with no log: %v, want exit 3", err)
	}
	if got := engineLogName; got != "engine-worker-0.log" {
		t.Errorf("the engine log's name is %q", got)
	}
}

// Changing H0's shape is a change of the config alone (ruling H0): phases overridden the way the settings override them (PhasesFromEnv) run through Run and
// the sequencer, the generator is given each phase's own config, and the post and the judge follow it. P2 at 5 turns gives LLMLab exactly k = 2 sessions of 5
// turns, 10 requests. The session COUNT is the constants' (#776's review, point 4): a P2 or a P1 whose N is not the cap + k, or the cap, is INVALID and never a
// pass, and no spill count is judged against an expectation the override moved. The test that let 14 sessions and three first-turn spills pass is inverted here.
func TestAChangedPhaseConfigRunsThroughTheSequencerAndTheJudge(t *testing.T) {
	phases, err := PhasesFromEnv(DefaultPhases(CapSano), `{"P2":{"turns":5},"P1-long":{"contextTokens":27000}}`)
	if err != nil {
		t.Fatal(err)
	}
	out, failures, err, st := runBaselineDryWith(t, "", phases)
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, err %v: a run of the changed config must pass", failures, err)
	}
	post, _ := os.ReadFile(filepath.Join(out, PostsFile))
	for _, w := range []string{"- P2 (full): N = 13, 5 turns, context 1024", "- P1-long (long): N = 11, 2 turns, context 27000",
		"LLMLab (managed): 10 requests, 2 sessions (distinct), 2 at most in flight; spills in 10"} {
		if !strings.Contains(string(post), w) {
			t.Errorf("the text to post lacks %q:\n%s", w, post)
		}
	}
	// The generator was given the changed configs, and the unchanged phase's own.
	for name, want := range map[string][]string{
		PhaseP2:     {`"sessions": 13`, `"turns": 5`},
		PhaseP1Long: {`"contextTokens": 27000`},
		PhaseP1:     {`"sessions": 11`, `"turns": 24`, `"contextTokens": 1024`},
	} {
		cfg := string(st.baseline.configs[name])
		for _, w := range want {
			if !strings.Contains(cfg, w) {
				t.Errorf("%s's generator config lacks %s:\n%s", name, w, cfg)
			}
		}
	}
	// A session count that is not the constants' is INVALID, never a pass, whatever LLMLab was given: 14 sessions with three first-turn spills, 11 with none,
	// a control of 5 and a detach of 14.
	for _, c := range []struct{ env, why string }{
		{`{"P2":{"sessions":14}}`, "P2 ran 14 sessions, and the row's constants, the cap 11 plus k = 2, say 13"},
		{`{"P2":{"sessions":11}}`, "P2 ran 11 sessions, and the row's constants, the cap 11 plus k = 2, say 13"},
		{`{"P1":{"sessions":5}}`, "the phase ran 5 sessions, and the row's constant for the cap says 11"},
		{`{"P3":{"sessions":14}}`, "the phase ran 14 sessions, and the row's constants, the cap 11 plus k = 2, say 13"},
	} {
		phases, err := PhasesFromEnv(DefaultPhases(CapSano), c.env)
		if err != nil {
			t.Fatal(err)
		}
		if _, failures, err, _ = runBaselineDryWith(t, "", phases); len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) || !strings.Contains(err.Error(), c.why) {
			t.Errorf("%s: failures %v, err %v, want INVALID with %q", c.env, failures, err, c.why)
		}
	}
}

// The engine's log is read while the Instance is up (the Allocation's directory goes with the job's release, internal/providerctl/delete.go:110): the
// logged KV is read once, after the Instance is ready and before any phase and before the served deployment's end, and nothing reads the directory
// after the release. The dry world takes the directory away at the release: a read after it fails and is marked.
func TestTheAllocationsDirectoryIsReadOnlyWhileTheInstanceIsUp(t *testing.T) {
	out, failures, err, st := runBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("%v %v", failures, err)
	}
	w := st.baseline
	if w.readAfterRelease || !w.released {
		t.Errorf("read after release %t, released %t: the row read the directory after its job was released, or never released it", w.readAfterRelease, w.released)
	}
	n := 0
	for _, c := range w.calls {
		if c == "LoggedKV" {
			n++
		}
	}
	kv, end, first := slices.Index(w.calls, "LoggedKV"), slices.Index(w.calls, "EndServed"), slices.Index(w.calls, "StartGenerator P1")
	if n != 1 || kv < 0 || kv > first || end != len(w.calls)-1 || end < first {
		t.Errorf("the calls were %v: want LoggedKV once, before the first phase, and EndServed last", w.calls)
	}
	if o := obsOfRecord(t, out); !o.KVRead || o.LoggedKV != KVSano {
		t.Errorf("the observation holds a logged KV of %d (read %t), want %d read before the release", o.LoggedKV, o.KVRead, KVSano)
	}
	// The control: the world takes the directory away at the release, so the same read after it is none.
	late := &dryBaseline{d: st, released: true}
	if kv, ok := late.LoggedKV(t.Context()); ok || kv != 0 || !late.readAfterRelease {
		t.Errorf("a read after the release gave %d, %t, and was not marked", kv, ok)
	}
}

// In the long phase the engine's requests waiting and its preemptions are READINGS (ruling H0): a run whose engine queued the long prompts and preempted
// three requests passes, and the post carries both readings and the KV use; the same readings in P2 are failures (dry_baseline_test.go, engine-preempts).
func TestTheLongPhasesEngineReadingsArePostedAndNeverFailed(t *testing.T) {
	out, failures, err, _ := runBaselineDry(t, "long-engine-readings")
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, err %v: the long phase's readings are no failure", failures, err)
	}
	post, _ := os.ReadFile(filepath.Join(out, PostsFile))
	if !strings.Contains(string(post), "requests waiting in 1 reads, the longest run 1, at most 2; preemptions 3; KV use at most 74.0% of the cache (vllm:kv_cache_usage_perc) over the 1 distinct read(s) taken inside the stretch with the cap's requests in flight (a READING, ruling H0") {
		t.Errorf("the text to post lacks the long phase's readings:\n%s", post)
	}
	if n := strings.Count(string(post), "(a READING, ruling H0"); n != 1 {
		t.Errorf("%d engine lines are marked a reading, want the long phase's alone:\n%s", n, post)
	}
}

// P0 completes before the served ModelDeployment is created, so before any sbatch (internal/alpha/baselineflow.go: Probe, then p0Problems, then StartServed;
// live, StartServed is the first call that creates the served ModelDeployment, in baseline_live_test.go, and AttachManaged before it makes only the stack, the
// Providers and the managed ModelDeployment). A P0 that fails ends the run there: the fake Slurm counts no submission, the calls stop after P0, and nothing is
// created or run. The verdict is FAIL when the front answered P0 (the managed Provider never became usable, or nothing was sent: the managed path is ours to
// get right) and INVALID when LLMLab answered it and failed, or gave fewer tokens than min_tokens asked (a failed LLMLab says nothing of the framework). It is
// no NOT RUN: that is a job that never started, and none was submitted. The control, a healthy run, submits exactly one job, after P0.
func TestAFailedP0EndsTheRunBeforeAnyJob(t *testing.T) {
	for _, c := range []struct {
		fault   string
		invalid bool
	}{{"p0-front-fails", false}, {"p0-not-sent", false}, {"p0-answered-by-an-instance", false}, {"p0-llmlab-fails", true}, {"min-tokens-ignored", true},
		// An HTTP 200 from LLMLab that is no completed stream with tokens, with min_tokens left out, authorises no GPU job.
		{"p0-empty-200", true}, {"p0-no-finish", true}, {"p0-zero-tokens", true}, {"p0-counts-null-no-events", true}, {"p0-cut-200", true}} {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err, st := runBaselineDry(t, c.fault)
			if n := st.plat.CountSubmits(); n != 0 {
				t.Errorf("the fake Slurm counts %d submission(s), want 0: the served ModelDeployment was created although P0 had failed", n)
			}
			if want := []string{"AttachManaged", "Probe"}; !slices.Equal(st.baseline.calls, want) {
				t.Errorf("the calls were %v, want %v: nothing follows a failed P0", st.baseline.calls, want)
			}
			switch {
			case c.invalid && (len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) || NotRun(BaselineRow(), err)):
				t.Errorf("failures %v, err %v: want INVALID alone, and no NOT RUN", failures, err)
			case !c.invalid && (len(failures) == 0 || err != nil):
				t.Errorf("failures %v, err %v: want a FAIL", failures, err)
			}
			o := obsOfRecord(t, out)
			if o.ServedCreated || len(o.Tags) != 0 || len(o.Phases) != 0 {
				t.Errorf("the observation says the served deployment was created %t, %d tag(s), %d phase(s)", o.ServedCreated, len(o.Tags), len(o.Phases))
			}
		})
	}
	// The controls: a healthy P0, and one whose counts are null but whose content events are positive (a completed stream with tokens), each submit one job,
	// after P0.
	for _, fault := range []string{"", "p0-counts-null-events"} {
		_, failures, err, st := runBaselineDry(t, fault)
		probe, served := slices.Index(st.baseline.calls, "Probe"), slices.Index(st.baseline.calls, "StartServed")
		if err != nil || len(failures) != 0 || st.plat.CountSubmits() != 1 || probe < 0 || served < probe {
			t.Errorf("the control %q: failures %v, err %v, %d submissions, Probe at %d and StartServed at %d: want a pass with one submission, after P0", fault, failures, err, st.plat.CountSubmits(), probe, served)
		}
	}
}

// The row's own settings (the cap, the deadline, one Instance, no hedge, the lease and the Providers) are none of them an input of the deployment ID
// (HBEP-3): pkg/deploymentid.Compute reads the serving spec alone (deploymentid.go:114), and hedgeWidth is "capacity, not a deployment-ID input"
// (api/v1alpha1/modeldeployment_types.go:341-342). So the served deployment the row creates has the example's serving spec exactly, and the ID the live
// check expects, 965fa2409193, is the example's.
func TestTheRowsSettingsAreNoInputOfTheDeploymentID(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "examples", SanoQwen3VLDeployment))
	if err != nil {
		t.Fatal(err)
	}
	var base v1alpha1.ModelDeployment
	if err := yaml.UnmarshalStrict(b, &base); err != nil {
		t.Fatal(err)
	}
	applied := applyPlan(&base, BaselineRow(), "alpha-0123456789")
	if !reflect.DeepEqual(applied.Spec.Source, base.Spec.Source) || applied.Spec.Model != base.Spec.Model {
		t.Errorf("the row changed the example's source or model: its deployment ID is no longer the example's")
	}
}

// Requests waiting in the engine fail a phase only when SUSTAINED, 3 consecutive DISTINCT reads, which at Sano's 10 s probe interval is about 20 to 30 s (the
// science agent's rulings of 18 Nov 2026 and on #776's review, point 7): N sessions that start together can have first prefills above the batched-token budget
// for a read or two. A read is told by the agent's own read time, since the controller's gauges repeat a read between reads, so one waiting read scraped
// several times counts once. Hand-typed runs: a blip of 1 and 2 in a row pass, 3 in a row fail, and two runs of 2 split by a clear read pass. Preemptions and
// running above the cap fail at any read, as before.
func TestEngineWaitingFailsOnlyWhenSustainedForThreeDistinctReads(t *testing.T) {
	if EngineWaitingSustain != 3 || EngineProbeInterval != 10*time.Second {
		t.Fatalf("sustain %d at a probe interval of %s, want 3 at 10s: 20 to 30 s", EngineWaitingSustain, EngineProbeInterval)
	}
	samples := func(waiting ...float64) []EngineSample {
		out := make([]EngineSample, len(waiting))
		for i, w := range waiting {
			out[i] = EngineSample{Running: 11, Waiting: w}
		}
		return out
	}
	for _, c := range []struct {
		name    string
		seq     []float64
		run     int
		failing bool
	}{
		{"none", []float64{0, 0, 0, 0, 0}, 0, false},
		{"a blip of 1", []float64{0, 1, 0, 0, 0}, 1, false},
		{"2 in a row", []float64{0, 1, 3, 0, 0}, 2, false},
		{"2 and 2", []float64{1, 1, 0, 1, 1}, 2, false},
		{"3 in a row", []float64{0, 1, 1, 1, 0}, 3, true},
		{"the whole phase", []float64{2, 2, 2, 2, 2}, 5, true},
	} {
		if got := longestWaitingRun(samples(c.seq...)); got != c.run {
			t.Errorf("%s: the longest run is %d, want %d", c.name, got, c.run)
		}
		po := PhaseObs{Phase: DefaultPhases(CapSano)[1+1], Engine: samples(c.seq...)} // P2
		fail, _ := judgePhase(BaselineObs{Cap: CapSano, LLMLabProvider: LLMLabProviderName}, po)
		got := false
		for _, f := range fail {
			got = got || strings.Contains(f, "the engine had requests waiting")
		}
		if got != c.failing {
			t.Errorf("%s: failed on waiting %t, want %t (%q)", c.name, got, c.failing, fail)
		}
	}
	// The failure names reads and the window they span, never polls or the old 6 s.
	po := PhaseObs{Phase: DefaultPhases(CapSano)[2], Engine: samples(1, 1, 1)}
	fail3, _ := judgePhase(BaselineObs{Cap: CapSano, LLMLabProvider: LLMLabProviderName}, po)
	if !strings.Contains(Summary(fail3), "3 consecutive distinct reads, past the 2 that a burst of first prefills explains (20s to 30s apart at Sano's probe interval of 10s)") {
		t.Errorf("the failure reads %q", Summary(fail3))
	}
	// The long phase reads, and never fails on waiting, however long.
	fail, _ := judgePhase(BaselineObs{Cap: CapSano, LLMLabProvider: LLMLabProviderName}, PhaseObs{Phase: DefaultPhases(CapSano)[1], Engine: samples(2, 2, 2, 2, 2)})
	for _, f := range fail {
		if strings.Contains(f, "waiting") {
			t.Errorf("the long phase failed on waiting requests: %q", f)
		}
	}
}

// The long shape is set from a MEASURED ratio, after the gate: one probe at the served Instance alone (model@<provider>, so it cannot spill), one session of one
// turn of 2,000 words and one output token. The dry engine's prompt of 2,000 words is 2,107 tokens (1.05 a word and the template's 7), a ratio of 1.0535, so 26,000
// tokens are 24,680 words (24,679 come to 25,999.3 and 24,680 to 26,000.4, the nearer); at 1.3 a word the probe reads 2,607 tokens, a ratio of 1.3035, and 19,946
// words (25,999.6). An absurd ratio, or a reading with no tokens, is INVALID and P1-long is not run; a probe the Instance did not answer is a failed check.
func TestTheRatioSetsTheLongContextsWords(t *testing.T) {
	for _, c := range []struct {
		fault, words, tpw string
	}{{"", "24680", "1.0535"}, {"ratio-wide", "19946", "1.3035"}} {
		out, failures, err, st := runBaselineDry(t, c.fault)
		if err != nil || len(failures) != 0 {
			t.Fatalf("%q: failures %v, err %v", c.fault, failures, err)
		}
		cfg := string(st.baseline.configs[PhaseP1Long])
		if !strings.Contains(cfg, `"contextTokens": `+c.words) {
			t.Errorf("%q: P1-long's generator config lacks %s words:\n%s", c.fault, c.words, cfg)
		}
		probe := string(st.baseline.configs[PhaseRatio])
		for _, w := range []string{`"model": "Qwen/Qwen3-VL-8B-Instruct@alpha-dry"`, `"sessions": 1`, `"turns": 1`, `"contextTokens": 2000`, `"outputTokens": 1`} {
			if !strings.Contains(probe, w) {
				t.Errorf("%q: the probe's generator config lacks %s:\n%s", c.fault, w, probe)
			}
		}
		post, _ := os.ReadFile(filepath.Join(out, PostsFile))
		if want := c.tpw + " tokens a word, so P1-long's contexts are " + c.words + " words for 26000 tokens (a shape, never a cap)"; !strings.Contains(string(post), want) {
			t.Errorf("%q: the text to post lacks %q:\n%s", c.fault, want, post)
		}
		if o := obsOfRecord(t, out); o.Ratio.LongWords == 0 || strings.Contains(strings.Join(st.baseline.calls, ","), "StartGenerator ratio,StartGenerator ratio") {
			t.Errorf("%q: the observation's long words %d, and the probe must be sent once", c.fault, o.Ratio.LongWords)
		}
	}
	// An absurd ratio: INVALID, P1-long not run, nothing else failed, and the other phases ran.
	out, failures, err, _ := runBaselineDry(t, "ratio-impossible")
	if len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) {
		t.Fatalf("failures %v, err %v: want INVALID", failures, err)
	}
	var ran []string
	for _, po := range obsOfRecord(t, out).Phases {
		ran = append(ran, po.Phase.Name)
	}
	if want := []string{PhaseP1, PhaseP2, PhaseP3}; !slices.Equal(ran, want) {
		t.Errorf("the phases run were %v, want %v: P1-long is INVALID before it starts and the others go on", ran, want)
	}
	// A probe the Instance fails to answer is a failed check, and sets nothing.
	if _, failures, err, _ = runBaselineDry(t, "ratio-probe-fails"); err != nil || !strings.Contains(Summary(failures), "the ratio probe was not answered by the served Instance") {
		t.Errorf("failures %v, err %v", failures, err)
	}
	// With the words given, no probe is sent.
	phases, _ := PhasesFromEnv(DefaultPhases(CapSano), `{"P1-long":{"contextTokens":25000}}`)
	if _, failures, err, st := runBaselineDryWith(t, "", phases); err != nil || len(failures) != 0 || slices.Contains(st.baseline.calls, "StartGenerator ratio") {
		t.Errorf("failures %v, err %v, calls %v: with the words given no probe is sent", failures, err, st.baseline.calls)
	}
}

// A gate the served Instance did not answer stops the row BEFORE any phase (the science agent's ruling, #776's review, point 2): the generator is never started,
// so LLMLab is exposed to none of the load, and the served job is ended by the normal cleanup (the sequencer's EndServed; the live Close for an error). Its
// verdict is FAIL and never INVALID: the gate's request is model@<provider> for the served Provider, which LLMLab can never answer, so a failure there is the
// front's or the Instance's own. A ratio probe the served Instance did not answer is the same hole by a second door and stops the row the same way; one it
// answered whole with no prompt tokens only leaves the long shape unset.
func TestAFailedGateStopsTheRowBeforeAnyPhase(t *testing.T) {
	noLoad := func(t *testing.T, calls []string) {
		t.Helper()
		for _, c := range calls {
			if strings.HasPrefix(c, "StartGenerator ") && c != "StartGenerator ratio" || c == "DetachManaged" {
				t.Errorf("the calls were %v: %q ran after a failed gate", calls, c)
			}
		}
	}
	for _, fault := range []string{"no-gate", "gate-503", "gate-engine-500", "gate-cut-200", "ratio-probe-fails"} {
		t.Run(fault, func(t *testing.T) {
			out, failures, err, st := runBaselineDry(t, fault)
			if err != nil || len(failures) == 0 || errors.Is(err, ErrBaselineInvalid) {
				t.Fatalf("failures %v, err %v: want a FAIL, never an INVALID", failures, err)
			}
			noLoad(t, st.baseline.calls)
			if n := st.plat.CountSubmits(); n != 1 {
				t.Errorf("%d submissions, want the served job alone", n)
			}
			if last := st.baseline.calls[len(st.baseline.calls)-1]; last != "EndServed" {
				t.Errorf("the calls ended with %q, want the served job's end", last)
			}
			o := obsOfRecord(t, out)
			if len(o.Phases) != 0 || o.StoppedBefore == "" {
				t.Errorf("%d phase(s) and stoppedBefore %q: want none and the step that stopped the row", len(o.Phases), o.StoppedBefore)
			}
			for _, f := range failures {
				if strings.Contains(f, "was not run") {
					t.Errorf("a failure names a phase that was not run, which is the stop's mark and no failure of its own: %q", f)
				}
			}
		})
	}
	// An error at the gate (no answer at all) ends the scenario at once: no phase, and the live Close ends the job by its tag.
	_, failures, err, st := runBaselineDry(t, "gate-error")
	if err == nil || len(failures) != 0 {
		t.Errorf("failures %v, err %v: want the scenario's error", failures, err)
	}
	noLoad(t, st.baseline.calls)
	if slices.Contains(st.baseline.calls, "StartGenerator ratio") {
		t.Errorf("the ratio probe ran after a gate that gave an error: %v", st.baseline.calls)
	}
}

// The 144-request guard FAILS CLOSED (the science agent's ruling, #776's review, point 8): a failed scrape of the front's per-provider request metric stops the
// phase, and one that fails before a phase means that phase's generator is never started; it is never a guard that is off for the phase. And the FINAL count,
// from the front's own records of every request LLMLab answered in the window, above 144 fails the verdict whatever the last poll said, since the guard polls
// and a burst between its last look and the generator's exit escapes it.
func TestTheLLMLabGuardFailsClosed(t *testing.T) {
	started := func(calls []string) (names []string) {
		for _, c := range calls {
			if strings.HasPrefix(c, "StartGenerator ") {
				names = append(names, strings.TrimPrefix(c, "StartGenerator "))
			}
		}
		return names
	}
	for _, c := range []struct {
		fault   string
		started []string // the generators started, in order
		stopped string   // the phase the guard's failed scrape ended, stopped or never started, if any
	}{
		// Every scrape fails: the first thing to need the guard is the ratio probe, whose generator is never started, and the row goes no further.
		{"metric-unread", nil, ""},
		// The scrape that starts P2 fails: P2's generator is never started, and nothing after it runs.
		{"metric-unread-at-p2-start", []string{PhaseRatio, PhaseP1, PhaseP1Long}, PhaseP2},
		// P2 starts, and its first poll's scrape fails: the generator is stopped, and nothing after it runs.
		{"metric-unread-midphase", []string{PhaseRatio, PhaseP1, PhaseP1Long, PhaseP2}, PhaseP2},
	} {
		t.Run(c.fault, func(t *testing.T) {
			out, failures, err, st := runBaselineDry(t, c.fault)
			if err != nil || !strings.Contains(Summary(failures), "the front's per-provider request metric could not be read, so the 144-request guard could not run") {
				t.Fatalf("failures %v, err %v: want a FAIL naming the guard that could not run", failures, err)
			}
			if got := started(st.baseline.calls); !slices.Equal(got, c.started) {
				t.Errorf("the generators started were %v, want %v", got, c.started)
			}
			if slices.Contains(st.baseline.calls, "DetachManaged") {
				t.Errorf("the managed ModelDeployment was detached after the guard failed")
			}
			for _, po := range obsOfRecord(t, out).Phases {
				if (po.Stopped == StopGuardUnread) != (po.Phase.Name == c.stopped) {
					t.Errorf("%s was stopped for %q, want the guard's failed scrape in %q alone", po.Phase.Name, po.Stopped, c.stopped)
				}
			}
		})
	}
	// The final count: every poll saw at most 100, so the generator was never stopped, and the front's records name 150 requests of LLMLab's.
	out, failures, err, _ := runBaselineDry(t, "llmlab-over-final-count")
	if err != nil || !strings.Contains(Summary(failures), "LLMLab answered 150 requests in the phase by the front's own records, over the guard of 144") {
		t.Fatalf("failures %v, err %v: want the final count to fail the verdict", failures, err)
	}
	for _, po := range obsOfRecord(t, out).Phases {
		if po.Stopped != "" {
			t.Errorf("%s was stopped for %q: the polls never saw the guard crossed, so only the final count can fail it", po.Phase.Name, po.Stopped)
		}
	}
}

// The controller's gauges repeat a read between reads, and a scrape every 2 s sees one read up to five times (Sano probes every 10 s): only a read NEWER than the
// last one taken, by the agent's own read time, is a sample, and a scrape with no read time is never fresh (#776's review, point 7). The read time is the
// agent's clock and is used only to tell reads apart.
func TestOnlyANewReadIsAnEngineSample(t *testing.T) {
	base := time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)
	read := func(sec int) EngineSample {
		return EngineSample{ReadAt: base.Add(time.Duration(sec) * time.Second), Waiting: 1}
	}
	s := BaselineSettings{lastRead: new(time.Time)}
	var po PhaseObs
	for _, smp := range []EngineSample{read(0), read(0), read(0), read(10), read(10), {Waiting: 1}, read(5), read(20)} {
		s.takeRead(&po, smp)
	}
	if len(po.Engine) != 3 {
		t.Fatalf("%d samples, want 3: the reads at 0, 10 and 20 s once each, a repeat, an unknown read time and an older read dropped", len(po.Engine))
	}
	// A read is taken once in the whole run: the next phase's first scrape of the last read is no sample of it.
	var next PhaseObs
	s.takeRead(&next, read(20))
	s.takeRead(&next, read(30))
	if len(next.Engine) != 1 || !next.Engine[0].ReadAt.Equal(base.Add(30*time.Second)) {
		t.Errorf("the next phase holds %+v, want the read at 30 s alone", next.Engine)
	}
}

// A KV figure is posted as at the cap only from a distinct read whose OWN time (ReadAt, the agent's clock) falls inside a stretch with the cap's requests in
// flight, [start, end): half-open at its end, since the front releases a request at the instant it ends (atCapWindows), so at that instant the cap no longer
// holds. The scrape that first saw a read (At) places nothing, and a sample with no ReadAt counts for nothing (the science agent's ruling on #764, 19 Nov 2026).
// By hand: eleven requests from 0 s to 40 s make the stretch [0, 40). Reads taken at 0 s, 20 s and 39.999 s count, though their scrapes came at 90 s; reads taken
// at -1 ms, at 40 s (the end) and at 40 s + 1 ms do not, though their scrapes came inside the stretch at 20 s; a sample with no ReadAt does not, whatever its At.
func TestKVIsPostedAsAtTheCapOnlyFromReadsInsideTheStretch(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	var joined []joinedRow
	for i := 0; i < CapSano; i++ {
		joined = append(joined, joinedRow{Gen: GenRow{Session: fmt.Sprintf("s%d", i), Turn: 1}, Front: FrontRow{Emitter: "instance", Start: t0, End: t0.Add(40 * time.Second)}})
	}
	read := func(taken, scraped time.Duration) EngineSample {
		return EngineSample{ReadAt: t0.Add(taken), At: t0.Add(scraped)}
	}
	po := PhaseObs{Engine: []EngineSample{
		read(0, 90*time.Second), read(20*time.Second, 90*time.Second), read(40*time.Second-time.Millisecond, 90*time.Second), // inside, the start included, scraped long after
		read(-time.Millisecond, 20*time.Second), read(40*time.Second, 20*time.Second), read(40*time.Second+time.Millisecond, 20*time.Second), // outside, scraped inside
		{At: t0.Add(20 * time.Second)}, // no ReadAt
	}}
	got := readsAtTheCap(BaselineObs{}, po, joined, LLMLabProviderName, CapSano)
	if len(got) != 3 || !got[0].ReadAt.Equal(t0) || !got[1].ReadAt.Equal(t0.Add(20*time.Second)) || !got[2].ReadAt.Equal(t0.Add(40*time.Second-time.Millisecond)) {
		t.Errorf("the reads at the cap are %v, want the reads taken at 0 s, 20 s and 39.999 s", got)
	}
	// One request short of the cap for the whole phase: no stretch, no read at the cap.
	if got := readsAtTheCap(BaselineObs{}, po, joined[:CapSano-1], LLMLabProviderName, CapSano); len(got) != 0 {
		t.Errorf("%d reads at the cap with %d requests in flight", len(got), CapSano-1)
	}
	// The reads are searched in every phase's samples, each once by its ReadAt: a read at 30 s in the next phase's samples counts, and one that both lists hold,
	// a phase that is also listed in the observation, counts once.
	next := PhaseObs{Engine: []EngineSample{read(30*time.Second, 95*time.Second), read(20*time.Second, 90*time.Second)}}
	o := BaselineObs{Phases: []PhaseObs{po, next}}
	if got := readsAtTheCap(o, po, joined, LLMLabProviderName, CapSano); len(got) != 4 {
		t.Errorf("%d reads at the cap over both phases' samples, want 4: the reads taken at 0 s, 20 s, 39.999 s and the next phase's at 30 s, the 20 s read once", len(got))
	}
}

// shortLongPhase is a dry run's P1-long with its front records cut to the Athena run's shape, first turns of 10.9 s and second turns of 3 s (#764), so its
// at-cap stretches are [0, 10.9 s) and [41 s, 44 s) from t0, the first request's start. next is the run's P2, whose own at-cap stretches are long after, and
// inP2 a time inside its first one.
func shortLongPhase(t *testing.T) (o BaselineObs, long, next PhaseObs, t0, inP2 time.Time) {
	t.Helper()
	out, _, _, _ := runBaselineDry(t, "")
	o = obsOfRecord(t, out)
	for _, po := range o.Phases {
		switch po.Phase.Name {
		case PhaseP1Long:
			long = po
		case PhaseP2:
			next = po
		}
	}
	if len(long.Rows) == 0 || len(next.Rows) == 0 {
		t.Fatal("the dry run's P1-long or P2 holds no front record")
	}
	t0 = long.Rows[0].Start
	for _, r := range long.Rows {
		if r.Start.Before(t0) {
			t0 = r.Start
		}
	}
	rows := slices.Clone(long.Rows)
	for i, r := range rows {
		d := 3 * time.Second
		if r.Start.Equal(t0) {
			d = 10900 * time.Millisecond
		}
		rows[i].End, rows[i].DurationMs = r.Start.Add(d), d.Milliseconds()
	}
	long.Rows = rows
	joined, _, _, _ := joinPhase(next)
	w2 := atCapWindows(rowsOn(joined, o.LLMLabProvider, RouteServed), o.Cap)
	if len(w2) == 0 || !w2[0][0].After(t0.Add(time.Minute)) {
		t.Fatalf("the dry P2's at-cap stretches %v are not after P1-long's", w2)
	}
	return o, long, next, t0, w2[0][0].Add(time.Second)
}

// The Athena run's turn-1 stretch lasted about 10.9 s and its later turns, prefix-cached, about 3 s (#764): a read counted only when its first scrape came more
// than a probe interval after the stretch began could never be inside one of 10.9 s or less. By the read's OWN time a stretch of that length holds a read.
// The check is the judge's own: judgePhase over shortLongPhase. By hand, in the first row the read taken at 5 s (scraped at 8 s) is the only one inside, and no
// scrape is more than 10 s after a stretch began, so the old rule counts none and the row kills it; in the second the read taken at -1 s was scraped at 10.5 s,
// inside the old rule's window and outside the stretch by its own time.
func TestAShortAtCapStretchIsWitnessedByAReadsOwnTime(t *testing.T) {
	o, long, _, t0, _ := shortLongPhase(t)
	sample := func(taken, scraped time.Duration) EngineSample {
		return EngineSample{ReadAt: t0.Add(taken), At: t0.Add(scraped), Running: float64(CapSano), KVUsage: 0.74}
	}
	const missing = "no distinct engine read was taken inside the stretch"
	for _, c := range []struct {
		name    string
		engine  []EngineSample
		witness int // the reads at the cap
	}{
		{"one read inside the short stretch, by its own time", []EngineSample{sample(-2*time.Second, time.Second), sample(5*time.Second, 8*time.Second), sample(25*time.Second, 27*time.Second)}, 1},
		{"the same stretch with no read inside by its own time, one scraped in the old rule's window", []EngineSample{sample(-2*time.Second, time.Second), sample(-time.Second, 10500*time.Millisecond), sample(25*time.Second, 27*time.Second)}, 0},
		{"a read just before the stretch and one just after it", []EngineSample{sample(-time.Millisecond, 5*time.Second), sample(10900*time.Millisecond+time.Millisecond, 10500*time.Millisecond)}, 0},
		{"a read taken at the start of the stretch alone", []EngineSample{sample(-2*time.Second, time.Second), sample(0, 3*time.Second)}, 1},
		{"a read taken at the end of the stretch alone: the request is released at that instant", []EngineSample{sample(-2*time.Second, time.Second), sample(10900*time.Millisecond, 12*time.Second)}, 0},
		{"a read taken just before the end of the stretch", []EngineSample{sample(-2*time.Second, time.Second), sample(10900*time.Millisecond-time.Millisecond, 12*time.Second)}, 1},
		{"a sample with no ReadAt, scraped inside the stretch", []EngineSample{{At: t0.Add(5 * time.Second), Running: float64(CapSano), KVUsage: 0.74}, sample(-2*time.Second, time.Second)}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			po := long
			po.Engine = c.engine
			o1 := o
			o1.Phases = []PhaseObs{po}
			fail, invalid := judgePhase(o1, po)
			joined, _, _, _ := joinPhase(po)
			if n := len(readsAtTheCap(o1, po, joined, o.LLMLabProvider, o.Cap)); n != c.witness {
				t.Errorf("%d reads at the cap, want %d", n, c.witness)
			}
			unwitnessed := slices.ContainsFunc(invalid, func(s string) bool { return strings.Contains(s, missing) })
			if unwitnessed != (c.witness == 0) {
				t.Errorf("the phase's INVALID reasons %q: the missing-read reason is %v, want %v", invalid, unwitnessed, c.witness == 0)
			}
			if c.witness > 0 && (len(fail) != 0 || len(invalid) != 0) {
				t.Errorf("a short stretch holding a read has failed checks %q or invalid reasons %q, want none", fail, invalid)
			}
			// The post says how the read was placed, and the limit of that, in a fixed sentence.
			post := baselinePost(BaselineRow(), o1, fail, invalid)
			if want := "a read is placed by the agent's own time of it, and counted when that falls inside the stretch; the node's clock against this host's is a stated limit"; !strings.Contains(post, want) {
				t.Errorf("the post does not say how a read is placed at the cap (%q):\n%s", want, post)
			}
		})
	}
}

// A read taken inside P1-long's stretch can be first scraped after the phase's last look (the stretch is 10.9 s, the phase ends at about 15 s, and a read reaches
// the controller up to a probe interval after it was taken), and then sits in the NEXT phase's samples. By the ruling, a read counts when its own time falls
// inside an at-cap stretch, so one lost to that bookkeeping would contradict it: the witness searches every phase's samples. But the stretches are P1-long's own
// (its front records): a read inside P2's at-cap stretch, and outside every one of P1-long's, is no witness at P1-long's cap, and it still counts for P2's own
// checks (the minimum of 2 reads), where it stays. Each phase keeps its reads in its own list.
func TestAReadInTheNextPhasesSamplesIsAtP1LongsCapOnlyByP1LongsOwnStretches(t *testing.T) {
	o, long, next, t0, inP2 := shortLongPhase(t)
	read := func(at time.Time) EngineSample {
		return EngineSample{ReadAt: at, At: at.Add(2 * time.Second), Running: float64(CapSano), KVUsage: 0.74}
	}
	own := []EngineSample{read(t0.Add(-2 * time.Second)), read(t0.Add(25 * time.Second))} // outside both of P1-long's stretches
	const missing = "no distinct engine read was taken inside the stretch"
	for _, c := range []struct {
		name    string
		next    []EngineSample
		both    bool // P1-long's own samples hold the read at 5 s too
		witness int
	}{
		{"a read inside P1-long's stretch, in the next phase's samples", []EngineSample{read(t0.Add(5 * time.Second)), read(inP2)}, false, 1},
		{"a read inside P1-long's second stretch, in the next phase's samples", []EngineSample{read(t0.Add(42 * time.Second)), read(inP2)}, false, 1},
		{"a read between P1-long's stretches, in the next phase's samples", []EngineSample{read(t0.Add(20 * time.Second)), read(inP2)}, false, 0},
		{"a read inside P2's own stretch and outside every one of P1-long's", []EngineSample{read(inP2), read(inP2.Add(500 * time.Millisecond))}, false, 0},
		{"a read both phases' samples hold, inside P1-long's stretch, counted once", []EngineSample{read(t0.Add(5 * time.Second)), read(inP2)}, true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			pl, pn := long, next
			pl.Engine, pn.Engine = slices.Clone(own), c.next
			if c.both {
				pl.Engine = append(pl.Engine, read(t0.Add(5*time.Second)))
			}
			wantOwn := len(pl.Engine)
			o1 := o
			o1.Phases = []PhaseObs{pl, pn}
			fail, invalid := judgePhase(o1, pl)
			joined, _, _, _ := joinPhase(pl)
			if n := len(readsAtTheCap(o1, pl, joined, o.LLMLabProvider, o.Cap)); n != c.witness {
				t.Errorf("%d reads at P1-long's cap, want %d", n, c.witness)
			}
			unwitnessed := slices.ContainsFunc(invalid, func(s string) bool { return strings.Contains(s, missing) })
			if unwitnessed != (c.witness == 0) {
				t.Errorf("P1-long's INVALID reasons %q: the missing-read reason is %v, want %v", invalid, unwitnessed, c.witness == 0)
			}
			if c.witness > 0 && (len(fail) != 0 || len(invalid) != 0) {
				t.Errorf("a read in the next phase's samples inside P1-long's stretch leaves failed checks %q or invalid reasons %q, want none", fail, invalid)
			}
			// The reads stay in their own phases' lists: P1-long's own are unchanged, and P2's two reads, the one inside P2's stretch among them, still satisfy
			// P2's own minimum of 2.
			if len(pl.Engine) != wantOwn || len(pn.Engine) != 2 {
				t.Errorf("the phases' own lists changed: P1-long %d reads, want %d, P2 %d, want 2", len(pl.Engine), wantOwn, len(pn.Engine))
			}
			if f2, _ := judgePhase(o1, pn); slices.ContainsFunc(f2, func(s string) bool { return strings.Contains(s, "fewer than 2 distinct engine reads") }) {
				t.Errorf("P2's own reads no longer satisfy its minimum of 2: %q", f2)
			}
		})
	}
}

// The detach phase's waits are WITNESSED by the front's own count of requests in its FIFO (its /healthz waiting), never inferred from the engine's peaks (the
// science agent's ruling on #764, #776's review, point 6). It passes only if the count reaches 2, is back at 0 when the phase ends (and no 503 capacity answer
// occurred, so both waiters were placed), no request reached LLMLab after the detach, and all 13 sessions' first turns ended with 200; the front holds ONE served
// deployment of the model, since the count sums every group. The post labels what it holds: a COUNT, a lower bound on the stretch, an indirect witness beside
// it, and the per-request durations as not witnessed.
func TestTheDetachPhaseIsWitnessedByTheFrontsOwnWaitingCount(t *testing.T) {
	out, failures, err, st := runBaselineDry(t, "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, err %v", failures, err)
	}
	post, _ := os.ReadFile(filepath.Join(out, PostsFile))
	for _, w := range []string{
		"waits at the front, WITNESSED by the front's own count of requests in its FIFO (its /healthz waiting,",
		"a COUNT and not a per-request wait): at most 2 at once, 0 when the phase ended; 503 capacity answers: 0; the longest stretch with a request in the FIFO was at least",
		"(a LOWER bound, one poll apart)",
		"the INDIRECT witness, from the front's records: the first-token times of the 2 over-cap first turns were [2500 2500] ms, against 2000 ms, the shortest service time of a held first turn",
		"(posted beside the count, no pass condition)",
		"the per-request wait DURATIONS are NOT witnessed until the front's waiting series (#769, PR 5)",
		"because the front held 1 served deployment(s) of the model (it must be exactly one)",
	} {
		if !strings.Contains(string(post), w) {
			t.Errorf("the text to post lacks %q:\n%s", w, post)
		}
	}
	if strings.Contains(string(post), "waits at the front, MEASURED") {
		t.Errorf("the text to post still says MEASURED about waits:\n%s", post)
	}
	// The count was read at the detach phase's look and nowhere else.
	if n := st.baseline.waitCalls; n < 6 {
		t.Errorf("the front's waiting count was read %d times in the detach phase, want at least 6", n)
	}
}

// longestWaitingStretch is a lower bound, by hand: reads 250 ms apart with counts 0, 2, 2, 1, 0 hold a request from the second read to the fourth, 500 ms; a count above
// 0 in one read alone is 0; of two runs the longer counts.
func TestTheLongestWaitingStretchIsALowerBound(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	for _, c := range []struct {
		name string
		s    []WaitSample
		want time.Duration
	}{
		{"none", []WaitSample{{at(0), 0}, {at(250), 0}}, 0},
		{"2, 2, 1 between zeros", []WaitSample{{at(0), 0}, {at(250), 2}, {at(500), 2}, {at(750), 1}, {at(1000), 0}}, 500 * time.Millisecond},
		{"one read alone", []WaitSample{{at(0), 0}, {at(250), 1}, {at(500), 0}}, 0},
		{"two runs, the longer counts", []WaitSample{{at(0), 1}, {at(250), 1}, {at(500), 0}, {at(750), 1}, {at(1000), 1}, {at(1250), 1}, {at(1500), 0}}, 500 * time.Millisecond},
		{"still waiting at the last read", []WaitSample{{at(0), 0}, {at(250), 2}, {at(500), 2}}, 250 * time.Millisecond},
	} {
		if got := longestWaitingStretch(c.s); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
	// The indirect witness, by hand: 13 first turns, the 2 largest first tokens are the over-cap ones, and the shortest service time of the 11 others is 1,900 ms.
	var joined []joinedRow
	for i := 0; i < 13; i++ {
		first, dur := int64(40+i), int64(1900+i*10)
		if i >= 11 {
			first, dur = int64(2500+i), int64(4500)
		}
		joined = append(joined, joinedRow{Gen: GenRow{Session: fmt.Sprintf("s%d", i), Turn: 1}, Front: FrontRow{Emitter: "instance", Status: 200, FirstTokenMs: &first, DurationMs: dur}})
	}
	over, held, ok := indirectWaitWitness(joined, LLMLabProviderName, 2)
	if !ok || held != 1900 || len(over) != 2 || over[0] != 2512 || over[1] != 2511 {
		t.Errorf("over-cap %v, held %d, %t; want [2512 2511], 1900, true", over, held, ok)
	}
	if _, _, ok := indirectWaitWitness(joined[:2], LLMLabProviderName, 2); ok {
		t.Error("an indirect witness was made of two first turns")
	}
}

// ParseDialFailures reads the front's slog JSON lines, typed by hand in the form of internal/front/proxy.go:253's: a line counts only if its msg EQUALS the text
// (never a substring of the line), its level is WARN, its target is ours and it names a request. A line that is not a JSON object, is cut short, or has a
// non-string member is skipped: never a failure found, and never a pass. It also returns the number of lines PARSED, a JSON object with a message: a line that
// is none of these (cut short, no JSON, an array, null, an object with no message) is not counted.
func TestDialFailuresAreParsedFromTheFrontsSlogLines(t *testing.T) {
	const ours, msg = "ns/inst-a", "no connection to the target; nothing was sent"
	line := func(level, m, req, target, errText string) string {
		return `{"time":"2026-11-18T12:00:00.123456789Z","level":"` + level + `","msg":"` + m + `","request":"` + req + `","target":"` + target + `","error":"` + errText + `"}` + "\n"
	}
	log := line("WARN", msg, "r1", ours, "dial tcp: refused") + // counts
		line("WARN", msg, "r2", "ns/inst-b", "x") + // another target
		line("INFO", msg, "r3", ours, "x") + // another level
		line("WARN", "another message", "r4", ours, msg) + // the text only in another line's error field
		line("WARN", msg, "", ours, "x") + // no request
		`{"level":"WARN","msg":"` + msg + `","request":"r5","target":"` + ours + `"` + "\n" + // cut short
		"not json at all\n" +
		`{"level":"WARN","msg":"` + msg + `","request":7,"target":"` + ours + `"}` + "\n" + // a request that is no string
		"[1,2]\nnull\n{}\n" + `{"level":"WARN","request":"r7","target":"` + ours + `"}` + "\n" + // none a JSON object with a message
		"\n" +
		line("WARN", msg, "r6", ours, "again")
	got, parsed := ParseDialFailures([]byte(log), ours)
	if len(got) != 2 || got[0] != (DialFailure{"r1", ours}) || got[1] != (DialFailure{"r6", ours}) {
		t.Errorf("parsed %+v, want r1 and r6 of the target alone", got)
	}
	if parsed != 6 {
		t.Errorf("%d lines parsed, want 6 (r1, r2, r3, r4, the one with no request, and r6)", parsed)
	}
	if got, parsed := ParseDialFailures(nil, ours); len(got) != 0 || parsed != 0 {
		t.Errorf("an empty log gave %+v and %d lines parsed", got, parsed)
	}
	// A whole line of the other front messages, none of them a dial failure.
	if got, parsed := ParseDialFailures([]byte(strings.Repeat(line("WARN", "a request was cut", "r1", ours, "x"), 3)), ours); len(got) != 0 || parsed != 3 {
		t.Errorf("another message gave %+v and %d lines parsed, want none and 3", got, parsed)
	}
}

// The guard's post line shows that the log read RAN and found what it expected, never a silent 0: the lines parsed and the dial failures, counts alone. A
// healthy run shows lines parsed above 0 and no failures (the dry front writes one line a generator run, the ratio probe's too, and the log is the run's whole, so each phase reads one more).
// An empty log, and one of lines that are none a slog JSON object, make every phase INVALID with 0 lines parsed posted: a log that was read and holds nothing
// of the front rules nothing out.
func TestTheDialFailureGuardPostsWhatItRead(t *testing.T) {
	const prefix = "  - dial-failure guard: front.log read, "
	post := func(fault string) (string, error) {
		out, failures, err, _ := runBaselineDry(t, fault)
		if len(failures) != 0 {
			t.Fatalf("%q: failed checks %q", fault, failures)
		}
		b, rerr := os.ReadFile(filepath.Join(out, PostsFile))
		if rerr != nil {
			t.Fatal(rerr)
		}
		return string(b), err
	}
	text, err := post("")
	if err != nil {
		t.Fatalf("the healthy run: %v", err)
	}
	for i, ph := range []string{"P1", "P1-long", "P2", "P3"} {
		want := fmt.Sprintf("%s%d lines parsed, 0 dial failures for our Instance\n", prefix, i+2)
		if !strings.Contains(text, want) {
			t.Errorf("the healthy post lacks %q (%s)", want, ph)
		}
	}
	if strings.Contains(text, "0 lines parsed") || strings.Contains(text, "held no parsed line") {
		t.Errorf("the healthy post shows a log with no parsed line:\n%s", text)
	}
	for _, fault := range []string{"front-log-empty", "front-log-malformed-only"} {
		text, err := post(fault)
		var ie *BaselineInvalidError
		if !errors.As(err, &ie) || !strings.Contains(err.Error(), "the front's log held no parsed line, so a failed dial of the Instance could not be ruled out") {
			t.Errorf("%s: err = %v, want INVALID for a log with no parsed line", fault, err)
		}
		if n := strings.Count(text, prefix+"0 lines parsed, 0 dial failures for our Instance\n"); n != 4 {
			t.Errorf("%s: %d phases post 0 lines parsed, want all 4:\n%s", fault, n, text)
		}
	}
}

// The front's log is the stack's PRIVATE state: read only to match request IDs, and only the COUNT reaches the text to post, the test's log, the record or the
// screen (team lead's condition on guard 2). The dry front's log carries invented text in the error and target fields of the line it writes for a request of P2;
// none of it, and not the matched request's ID, may appear in the post, the record, the run's log or the error that ends the run, while the count does.
func TestTheFrontLogIsPrivateAndOnlyItsCountIsPosted(t *testing.T) {
	cfg := Config{Row: BaselineRow(), Dry: true, DryFault: "dial-failure-p2", Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel,
		Phases: DefaultPhases(CapSano)}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	st := newDrySteps(t, cfg, "baseline")
	var logged strings.Builder
	logf := func(f string, a ...any) {
		fmt.Fprintf(&logged, f+"\n", a...)
		t.Logf(f, a...)
	}
	failures, err := Run(context.Background(), cfg, st, rec, logf)
	if len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) {
		t.Fatalf("failures %v, err %v: want INVALID", failures, err)
	}
	post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
	record, _ := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
	everywhere := map[string]string{"the post": string(post), "the record": string(record), "the run's log": logged.String(), "the run's error": err.Error()}
	// The record is the run's private file and holds the front's USAGE records, which name the Instance and the request IDs; what only the log holds, its error text
	// and its path, is in none of them. The Instance's key and the matched request's ID, which the log and the usage records share, are kept out of what is posted, logged
	// or printed.
	logOnly := []string{dryDialError, "INVENTED-9c1f", "192.0.2.77", "logs/front.log"}
	for where, text := range everywhere {
		for _, secret := range logOnly {
			if strings.Contains(text, secret) {
				t.Errorf("%s holds %q, which only the front's private log holds", where, secret)
			}
		}
		if where == "the record" {
			continue
		}
		for _, secret := range []string{dryServedInstance, "P2-s000-t01"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s holds %q, which names the front's private log's match", where, secret)
			}
		}
	}
	// The counts do reach them: in the post, the run's log and the error, and the other phases show none. P2 is the fourth run of the generator after the ratio probe's, so
	// its read finds the five lines the dry front wrote by then (one a run, and the failure's) and the one failure; N and K are the only things of the log the post holds, with the log's file name in the guard's line.
	if !strings.Contains(string(post), "  - dial-failure guard: front.log read, 5 lines parsed, 1 dial failures for our Instance\n") {
		t.Errorf("the post lacks the counts:\n%s", post)
	}
	if !strings.Contains(logged.String(), "P2:") || !strings.Contains(logged.String(), "5 log lines parsed, 1 resent after a failed dial") || !strings.Contains(err.Error(), "1 request(s) of the phase were resent after a failed dial of the Instance") {
		t.Errorf("the count is not in the run's log or the error:\n%s\n%v", logged.String(), err)
	}
	if n := strings.Count(string(post), ", 0 dial failures for our Instance\n"); n != 3 {
		t.Errorf("%d other phases show 0 dial failures, want 3 (P1, P1-long and P3)", n)
	}
}

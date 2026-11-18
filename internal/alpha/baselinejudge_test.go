package alpha

// The baseline row's judge and sequencer (baselinejudge.go, baselineflow.go) through Run and the dry world, with figures worked out by hand: a request's
// route is the FRONT's record's, a failure wins over INVALID, the guard stops the generator, the detach deletes the managed ModelDeployment, and the post
// carries what #764 asks of it.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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
	g := st.baseline.gens[2]
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
	want := []string{"AttachManaged", "Probe", "StartServed", "LoggedKV", "Gate", "StartGenerator P1", "StartGenerator P1-long", "StartGenerator P2", "DetachManaged", "StartGenerator P3", "EndServed"}
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
		"- P1-long (long): N = 11, 2 turns, context 26000",
		"- P2 (full): N = 13, 24 turns",
		"- P3 (detach): N = 13, 24 turns",
		// P2, by hand: 11 sessions on the Instance, 2 on LLMLab, 24 turns each: 264 and 48 requests.
		"  - the served Instance (Sano): 264 requests, 11 sessions (distinct), 11 at most in flight; spills in 0, out 48; errors 0; time to first token p50 50 ms, p95 50 ms;",
		"  - LLMLab (managed): 48 requests, 2 sessions (distinct), 2 at most in flight; spills in 48, out 0; errors 0; time to first token p50 300 ms, p95 300 ms;",
		"waits at the front: 0 by construction with LLMLab attached",
		"waits at the front, MEASURED (LLMLab detached): at most 13 requests in the front at once by the usage records, at most 11 running at the engine, so at most 2 waited",
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
	// The cap at an LLMLab arrival, with the tolerance of 100 ms each way: A [0,10) and B [0,10.05) held a cap of 2 until 10.0 and 10.05.
	served := []FrontRow{{Start: at(0), End: at(10)}, {Start: at(0), End: at(10.05)}}
	for _, c := range []struct {
		arrival float64
		held    bool
	}{{5, true}, {10.05, true}, {10.2, false}, {12, false}} {
		if got := heldItsCapAt(served, at(c.arrival), 2); got != c.held {
			t.Errorf("at %vs the Instance held its cap: %t, want %t", c.arrival, got, c.held)
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
// the sequencer, the generator is given each phase's own config, and the post and the judge follow it. P2 at 14 sessions and 5 turns makes k = 3, and LLMLab
// gets exactly 3 first-turn requests: the judge expects what the config says.
func TestAChangedPhaseConfigRunsThroughTheSequencerAndTheJudge(t *testing.T) {
	phases, err := PhasesFromEnv(DefaultPhases(CapSano), `{"P2":{"sessions":14,"turns":5},"P1-long":{"contextTokens":27000}}`)
	if err != nil {
		t.Fatal(err)
	}
	out, failures, err, st := runBaselineDryWith(t, "", phases)
	if err != nil || len(failures) != 0 {
		t.Fatalf("failures %v, err %v: a run of the changed config must pass", failures, err)
	}
	post, _ := os.ReadFile(filepath.Join(out, PostsFile))
	for _, w := range []string{"- P2 (full): N = 14, 5 turns, context 1024", "- P1-long (long): N = 11, 2 turns, context 27000",
		"LLMLab (managed): 15 requests, 3 sessions (distinct), 3 at most in flight; spills in 15"} {
		if !strings.Contains(string(post), w) {
			t.Errorf("the text to post lacks %q:\n%s", w, post)
		}
	}
	// The generator was given the changed configs, and the unchanged phase's own.
	for name, want := range map[string][]string{
		PhaseP2:     {`"sessions": 14`, `"turns": 5`},
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
	// A P2 whose sessions the cap holds (N at most the cap) cannot bind it: INVALID, however many LLMLab was given.
	phases, _ = PhasesFromEnv(DefaultPhases(CapSano), `{"P2":{"sessions":11}}`)
	if _, failures, err, _ = runBaselineDryWith(t, "", phases); len(failures) != 0 || !errors.Is(err, ErrBaselineInvalid) || !strings.Contains(err.Error(), "11 sessions against a cap of 11") {
		t.Errorf("a P2 of the cap's sessions: failures %v, err %v, want INVALID", failures, err)
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
	if !strings.Contains(string(post), "requests waiting in 1 samples, at most 2; preemptions 3; KV use at most 74.0% of the cache (vllm:kv_cache_usage_perc) (a READING, ruling H0") {
		t.Errorf("the text to post lacks the long phase's readings:\n%s", post)
	}
	if n := strings.Count(string(post), "(a READING, ruling H0"); n != 1 {
		t.Errorf("%d engine lines are marked a reading, want the long phase's alone:\n%s", n, post)
	}
}

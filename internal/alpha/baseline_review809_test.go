package alpha

// The fixes of PR 809's review that sit in the shared baseline judge and flow, so that Row 1 and its Athena twin have them alike (points 3 to 5): a run with no
// measurement phase never passes, a session that lost turns fails its phase, and the fit of P3 is checked again right before it is launched. The dry faults that
// exercise them are in dry_baseline_test.go and dry_baseline_athena_test.go, whose wants name each fix's words; these hold the judge's rules directly.

import (
	"strings"
	"testing"
)

// runOfRow is one dry run of either baseline row through Run.
func runOfRow(t *testing.T, row Row, fault string) (out string, failures []string, err error) {
	t.Helper()
	if row.Name == BetaAthenaBaselineName {
		out, failures, err, _ = runAthenaBaselineDry(t, fault)
		return out, failures, err
	}
	out, failures, err, _ = runBaselineDry(t, fault)
	return out, failures, err
}

// Point 3: the judge never passes a run with no measurement phase. The observation is a healthy run's with its phases taken out and the ratio probe stopped at
// its time bound, as the reviewer's case reads: the probe answered with prompt tokens as the bound fired. No failure, no other reason, and still INVALID.
func TestARunWithNoMeasurementPhaseNeverPasses(t *testing.T) {
	for _, row := range []Row{BaselineRow(), AthenaBaselineRow()} {
		t.Run(row.Name, func(t *testing.T) {
			out, failures, err := runOfRow(t, row, "")
			if err != nil || len(failures) != 0 {
				t.Fatalf("the healthy run: failures %v, err %v", failures, err)
			}
			o := obsOfRecord(t, out)
			if len(o.Phases) != 4 || !o.Ratio.Needed || o.Ratio.Row.Status != 200 {
				t.Fatalf("the healthy observation has %d phases and a ratio probe %+v", len(o.Phases), o.Ratio)
			}
			o.Phases, o.StoppedBefore, o.Ratio.Stopped = nil, "the ratio probe", StopBound
			fail, invalid := judgeBaseline(o)
			if len(fail) != 0 || len(invalid) != 1 || !strings.Contains(invalid[0], "no measurement phase was run (the run ended at the ratio probe before any phase (the ratio probe's generator was stopped: bound))") {
				t.Errorf("a run with no phase: failed %q, invalid %q; want one INVALID reason naming the probe and its bound", fail, invalid)
			}
			if BaselineVerdict(fail, invalid) != "INVALID" {
				t.Errorf("the verdict is %s, want INVALID", BaselineVerdict(fail, invalid))
			}
			// A failure still wins, and names itself: the gate that was not answered stays the one finding.
			o.StoppedBefore, o.Ratio.Stopped = "the gate", ""
			o.Gate.Row.Status = 503
			if fail, invalid = judgeBaseline(o); len(fail) == 0 || len(invalid) != 0 {
				t.Errorf("a stop at the gate: failed %q, invalid %q; want a failure alone", fail, invalid)
			}
		})
	}
}

// BaselineVerdict is the post's verdict word of a judged run, for the tests above.
func BaselineVerdict(fail, invalid []string) string { return baselineVerdictWord(fail, invalid) }

// Point 4 by the judge directly: a session of fewer turns than configured, with every record 200, FAILS the phase when the Instance answered it and is INVALID when
// LLMLab did; a session with a failed request is that request's finding alone, and a stopped phase has its own mark.
func TestASessionThatLostTurnsFailsItsPhase(t *testing.T) {
	out, failures, err := runOfRow(t, AthenaBaselineRow(), "")
	if err != nil || len(failures) != 0 {
		t.Fatalf("the healthy run: failures %v, err %v", failures, err)
	}
	healthy := obsOfRecord(t, out)
	phase := func(o BaselineObs, name string) *PhaseObs {
		for i := range o.Phases {
			if o.Phases[i].Phase.Name == name {
				return &o.Phases[i]
			}
		}
		t.Fatalf("no phase %s", name)
		return nil
	}
	drop := func(po *PhaseObs, session string, fromTurn int) {
		var gen []GenRow
		have := map[string]bool{}
		for _, g := range po.Gen {
			if g.Session == session && g.Turn >= fromTurn {
				continue
			}
			gen = append(gen, g)
			have[g.RequestID] = true
		}
		po.Gen = gen
		var rows []FrontRow
		for _, r := range po.Rows {
			if have[r.RequestID] {
				rows = append(rows, r)
			}
		}
		po.Rows = rows
	}
	judge := func(mutate func(o *BaselineObs)) (fail, invalid []string) {
		o := obsOfRecord(t, out)
		mutate(&o)
		return judgeBaseline(o)
	}
	// On the Instance: P1's session s001 stops after turn 3.
	fail, invalid := judge(func(o *BaselineObs) { drop(phase(*o, PhaseP1), "s001", 4) })
	if len(invalid) != 0 || len(fail) != 1 || !strings.Contains(fail[0], "P1: 1 session(s) ran fewer than the 24 turns the phase configured") {
		t.Errorf("a session of 3 turns on the Instance: failed %q, invalid %q", fail, invalid)
	}
	// On LLMLab: P2's first spilled session (s003, the 4th) stops after turn 5.
	fail, invalid = judge(func(o *BaselineObs) {
		po := phase(*o, PhaseP2)
		drop(po, "s003", 6)
		po.LLMLabMetric = float64(len(rowsFrom(po.Rows, LLMLabProviderName))) // the front's own count follows its records
	})
	if len(fail) != 0 || len(invalid) != 1 || !strings.Contains(invalid[0], "P2: 1 session(s) that LLMLab answered ran fewer than the 24 turns the phase configured") {
		t.Errorf("a session of 5 turns on LLMLab: failed %q, invalid %q", fail, invalid)
	}
	// A phase that was stopped is not judged by its turns: it has its own mark.
	fail, _ = judge(func(o *BaselineObs) { po := phase(*o, PhaseP1); drop(po, "s001", 4); po.Stopped = StopBound })
	for _, f := range fail {
		if strings.Contains(f, "ran fewer than") {
			t.Errorf("a stopped phase was judged by its turns: %q", f)
		}
	}
	// The healthy observation is judged clean, so the above are the rule's doing.
	if fail, invalid = judgeBaseline(healthy); len(fail) != 0 || len(invalid) != 0 {
		t.Errorf("the healthy observation: failed %q, invalid %q", fail, invalid)
	}
}

// Point 5 through the sequencer: when the wait for LLMLab to detach eats P3's margin, the fit is checked again right before the phase and P3 is not started: the
// observation records DidNotFit, three phases ran, the world's calls never started P3's generator, and the run is INVALID and not a pass.
func TestP3IsNotLaunchedWhenTheDetachWaitAteItsFit(t *testing.T) {
	for _, row := range []Row{BaselineRow(), AthenaBaselineRow()} {
		t.Run(row.Name, func(t *testing.T) {
			var st *drySteps
			var out string
			var failures []string
			var err error
			if row.Name == BetaAthenaBaselineName {
				out, failures, err, st = runAthenaBaselineDry(t, "p3-detach-eats-the-fit")
			} else {
				out, failures, err, st = runBaselineDry(t, "p3-detach-eats-the-fit")
			}
			if len(failures) != 0 || !BaselineInvalidRun(row, err) {
				t.Fatalf("failures %v, err %v: want INVALID alone", failures, err)
			}
			o := obsOfRecord(t, out)
			if o.DidNotFit != PhaseP3 || len(o.Phases) != 3 || !o.Detached {
				t.Errorf("DidNotFit %q with %d phases, detached %t: want P3, 3 and the managed ModelDeployment detached", o.DidNotFit, len(o.Phases), o.Detached)
			}
			for _, c := range st.baseline.calls {
				if c == "StartGenerator "+PhaseP3 {
					t.Errorf("P3's generator was started although it no longer fit: %v", st.baseline.calls)
				}
			}
		})
	}
}

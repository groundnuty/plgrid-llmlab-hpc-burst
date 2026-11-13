package rig

// The analysis on hand-built records, with every expected value counted by hand from them (never from
// the code): a death is a process that ended on its own, never a closed attempt; a closed attempt whose
// engine lived is a false death; a duplicate is a front request an engine began twice; a client retry
// begun again is a re-execution; success counts a request's last attempt.

import (
	"testing"
	"time"
)

func TestAnalyzeCountsFromTheRecords(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	cell := Cell{Name: "hand", Arm: "b", Fault: "F3"}
	results := []Result{
		// r1: unmarked, a whole 200, begun once.
		{ID: "s18id-x-1/c0", Base: "s18id-x-1", Status: 200, Complete: true, FirstByte: 10 * time.Millisecond},
		// r2: marked F3, cut on Instance 0, retried by the layer on Instance 1: a whole 200, begun twice.
		{ID: "s18id-x-2/c0", Base: "s18id-x-2", Marker: MarkF3, Status: 200, Complete: true, FirstByte: 30 * time.Millisecond},
		// r3: marked F1 on Instance 2: the engine died under it; 502, begun once.
		{ID: "s18id-x-3/c0", Base: "s18id-x-3", Marker: MarkF1 + "2", Status: 502},
		// r4: a client retry of r3's request after a 502, begun again on Instance 0: a re-execution.
		{ID: "s18id-x-4/c0", Base: "s18id-x-4", Marker: MarkF1 + "0", Status: 502},
		{ID: "s18id-x-4/c1", Base: "s18id-x-4", Attempt: 1, Marker: MarkF1 + "0", Status: 200, Complete: true, FirstByte: 50 * time.Millisecond},
		// r5: marked F3, cut on Instance 1 (which lives on), then cut again on 2 (which lives on): the
		// layer gave up, a 502, begun twice. Its two closed attempts are no death, and the front-side
		// candidate counts it.
		{ID: "s18id-x-5/c0", Base: "s18id-x-5", Marker: MarkF3, Status: 502},
	}
	begins := map[string][]Begin{
		"s18id-x-1/c0": {{Instance: 0, Admitted: true, Outcome: "completed"}},
		"s18id-x-2/c0": {{Instance: 0, Admitted: true, Outcome: "cancelled"}, {Instance: 1, Admitted: true, Outcome: "completed"}},
		"s18id-x-3/c0": {{Instance: 2, Admitted: true}},
		"s18id-x-4/c0": {{Instance: 0, Admitted: true}},
		"s18id-x-4/c1": {{Instance: 1, Admitted: true, Outcome: "completed"}},
		"s18id-x-5/c0": {{Instance: 1, Admitted: true, Outcome: "cancelled"}, {Instance: 2, Admitted: true, Outcome: "cancelled"}},
	}
	exits := []Exit{
		{Instance: 2, At: t0.Add(2 * time.Second)},
		{Instance: 0, At: t0.Add(4 * time.Second)},
		{Instance: 1, At: t0.Add(9 * time.Second), Stopped: true}, // the rig's own stop: no death
	}
	attempts := []Attempt{
		{ID: "s18id-x-2/c0", Home: 0, Target: 0, Kind: "closed", At: t0.Add(time.Second)}, // F3: Instance 0 lived on
		{ID: "s18id-x-2/c0", Home: 0, Target: 1, Kind: "ok", At: t0.Add(time.Second)},
		{ID: "s18id-x-3/c0", Home: 2, Target: 2, Kind: "closed", At: t0.Add(2 * time.Second)},
		{ID: "s18id-x-5/c0", Home: 1, Target: 1, Kind: "closed", At: t0.Add(6 * time.Second)},
		{ID: "s18id-x-5/c0", Home: 1, Target: 2, Kind: "closed", At: t0.Add(6 * time.Second)},
	}
	s := Analyze(cell, results, begins, exits, attempts, 2)
	want := map[string][2]float64{
		"requests":       {float64(s.Requests), 5},
		"marked":         {float64(s.Marked), 4},
		"fired":          {float64(s.Fired), 4}, // r2 and r5 cancelled, r3 and r4 died under
		"attempts":       {float64(s.Attempts), 6},
		"duplicates":     {float64(s.Duplicates), 2}, // r2 and r5, each one front request begun twice
		"re-executions":  {float64(s.ReExecutions), 1},
		"deaths":         {float64(s.Deaths), 2},      // the stop is no death; the 4 closed attempts are not counted
		"false deaths":   {float64(s.FalseDeaths), 3}, // r2's on 0, r5's on 1 and on 2
		"success":        {s.Success, 0.6},
		"success marked": {s.SuccessMarked, 0.5},
		"success fired":  {s.SuccessFired, 0.5},
		"corr front":     {float64(s.CorrFront), 1}, // r5, closed twice
		"corr 30s":       {float64(s.CorrController["30s"]), 1},
		"ttfb fired p50": {s.TTFBFiredP50, 30},
	}
	for name, v := range want {
		if v[0] != v[1] {
			t.Errorf("%s = %v, want %v", name, v[0], v[1])
		}
	}
}

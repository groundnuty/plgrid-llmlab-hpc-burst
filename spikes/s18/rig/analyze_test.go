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
		// r6: a poison request the client sent twice (the SDK's retry), each a front request of its own:
		// c0 died on Instance 2, c1 died on Instance 1. One closed attempt in each front request, so the
		// per-request candidate (i) misses it; by content (iii) it is two.
		{ID: "s18id-x-6/c0", Base: "s18id-x-6", Marker: MarkPoison, Status: 502, Code: "upstream_closed"},
		{ID: "s18id-x-6/c1", Base: "s18id-x-6", Attempt: 1, Marker: MarkPoison, Status: 502, Code: "upstream_closed"},
	}
	begins := map[string][]Begin{
		"s18id-x-1/c0": {{Instance: 0, Admitted: true, Outcome: "completed"}},
		"s18id-x-2/c0": {{Instance: 0, Admitted: true, Outcome: "cancelled"}, {Instance: 1, Admitted: true, Outcome: "completed"}},
		"s18id-x-3/c0": {{Instance: 2, Admitted: true}},
		"s18id-x-4/c0": {{Instance: 0, Admitted: true}},
		"s18id-x-4/c1": {{Instance: 1, Admitted: true, Outcome: "completed"}},
		"s18id-x-5/c0": {{Instance: 1, Admitted: true, Outcome: "cancelled"}, {Instance: 2, Admitted: true, Outcome: "cancelled"}},
		"s18id-x-6/c0": {{Instance: 2, Admitted: true}},
		"s18id-x-6/c1": {{Instance: 1, Admitted: true}},
	}
	exits := []Exit{
		{Instance: 2, At: t0.Add(2 * time.Second)},
		{Instance: 0, At: t0.Add(4 * time.Second)},
		{Instance: 1, At: t0.Add(9 * time.Second), Stopped: true}, // the rig's own stop: no death
		// Instance 0 dying again and again on its own: no correlation across Instances.
		{Instance: 0, At: t0.Add(40 * time.Second)},
		{Instance: 0, At: t0.Add(50 * time.Second)},
		{Instance: 0, At: t0.Add(60 * time.Second)},
		// r6's two deaths, of two Instances, a second apart, 40 s after the last of Instance 0's.
		{Instance: 2, At: t0.Add(100 * time.Second)},
		{Instance: 1, At: t0.Add(101 * time.Second)},
	}
	attempts := []Attempt{
		{ID: "s18id-x-2/c0", Home: 0, Target: 0, Kind: "closed", At: t0.Add(time.Second)}, // F3: Instance 0 lived on
		{ID: "s18id-x-2/c0", Home: 0, Target: 1, Kind: "ok", At: t0.Add(time.Second)},
		{ID: "s18id-x-3/c0", Home: 2, Target: 2, Kind: "closed", At: t0.Add(2 * time.Second)},
		{ID: "s18id-x-5/c0", Home: 1, Target: 1, Kind: "closed", At: t0.Add(6 * time.Second)},
		{ID: "s18id-x-5/c0", Home: 1, Target: 2, Kind: "closed", At: t0.Add(6 * time.Second)},
		{ID: "s18id-x-6/c0", Home: 2, Target: 2, Kind: "closed", At: t0.Add(100 * time.Second)},
		{ID: "s18id-x-6/c1", Home: 1, Target: 1, Kind: "closed", At: t0.Add(101 * time.Second)},
	}
	s := Analyze(cell, results, begins, exits, attempts, 2)
	want := map[string][2]float64{
		"requests":         {float64(s.Requests), 6},
		"marked":           {float64(s.Marked), 5},
		"fired":            {float64(s.Fired), 5}, // r2 and r5 cancelled, r3, r4 and r6 died under
		"attempts":         {float64(s.Attempts), 8},
		"duplicates":       {float64(s.Duplicates), 2},   // r2 and r5, each one front request begun twice
		"re-executions":    {float64(s.ReExecutions), 2}, // r4 and r6, each begun in two client attempts
		"deaths":           {float64(s.Deaths), 7},       // 2, 4, 40, 50, 60, 100, 101 s; the stop is none; 6 closed attempts
		"false deaths":     {float64(s.FalseDeaths), 3},  // r2's on 0, r5's on 1 and on 2
		"success":          {s.Success, 0.5},
		"success marked":   {s.SuccessMarked, 0.4},
		"success fired":    {s.SuccessFired, 0.4},
		"corr front":       {float64(s.CorrFront), 1}, // r5, closed twice in one front request
		"corr front false": {float64(s.CorrFrontFalse), 1},
		// Within 30 s after a death of ANOTHER Instance: 4 s (after 2 s), 101 s (after 100 s); 40, 50 and 60
		// s are Instance 0 again, and 100 s comes 40 s after 60 s: no other Instance's death in the 30 s before.
		"corr 30s":           {float64(s.CorrController["30s"]), 2},
		"corr content":       {float64(s.CorrContent), 2},      // r5 (two closed, one request), r6 (two, in two)
		"corr content false": {float64(s.CorrContentFalse), 1}, // r5: no engine died
		"ttfb fired p50":     {s.TTFBFiredP50, 30},
	}
	for name, v := range want {
		if v[0] != v[1] {
			t.Errorf("%s = %v, want %v", name, v[0], v[1])
		}
	}
}

// A poison request's cost: candidate (ii) fires on deaths of two distinct Instances within 30 s of each
// other, timed by the process ends, not on one Instance dying twice nor on deaths further apart; the
// content candidate counts its failures across the client's attempts; the span is first to last death.
func TestPoisonCostCountsInstancesWindowAndContent(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 21, 0, 0, 0, time.UTC)
	rs := []Result{{ID: "p/c0", Base: "p", Marker: MarkPoison, Status: 502, Code: "upstream_closed", Start: t0, End: 50 * time.Millisecond},
		{ID: "p/c1", Base: "p", Attempt: 1, Marker: MarkPoison, Status: 502, Code: "upstream_closed", Start: t0.Add(600 * time.Millisecond), End: 50 * time.Millisecond}}
	two := map[string][]Begin{"p/c0": {{Instance: 0, Admitted: true}}, "p/c1": {{Instance: 1, Admitted: true}}}
	one := map[string][]Begin{"p/c0": {{Instance: 0, Admitted: true}}, "p/c1": {{Instance: 0, Admitted: true}}}
	near := []Exit{{Instance: 0, At: t0.Add(10 * time.Millisecond)}, {Instance: 1, At: t0.Add(610 * time.Millisecond)},
		{Instance: 1, At: t0.Add(200 * time.Millisecond), Stopped: true}} // the rig's own stop: no death
	oneTwice := []Exit{{Instance: 0, At: t0.Add(10 * time.Millisecond)}, {Instance: 0, At: t0.Add(610 * time.Millisecond)}}
	// Attempts 40 s apart: each death in its own attempt's span, the two beyond the window.
	far := append([]Result(nil), rs...)
	far[1].Start = t0.Add(40 * time.Second)
	farExits := []Exit{{Instance: 0, At: t0.Add(10 * time.Millisecond)}, {Instance: 1, At: t0.Add(40*time.Second + 10*time.Millisecond)}}
	for _, c := range []struct {
		name   string
		rs     []Result
		begins map[string][]Begin
		exits  []Exit
		want   PoisonTrial
	}{
		{"two Instances 0.6 s apart", rs, two, near, PoisonTrial{Base: "p", Attempts: 2, Deaths: 2, Final: 502, CorrController: true, CorrContent: true, DeathSpan: 600 * time.Millisecond}},
		{"one Instance twice", rs, one, oneTwice, PoisonTrial{Base: "p", Attempts: 2, Deaths: 2, Final: 502, CorrController: false, CorrContent: true, DeathSpan: 600 * time.Millisecond}},
		{"two Instances 40 s apart", far, two, farExits, PoisonTrial{Base: "p", Attempts: 2, Deaths: 2, Final: 502, CorrController: false, CorrContent: true, DeathSpan: 40 * time.Second}},
	} {
		// Arm (a): no layer, so the front's own 502 upstream_closed answers are its closed failures.
		if got := PoisonCost("p", c.rs, c.begins, nil, c.exits); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
}

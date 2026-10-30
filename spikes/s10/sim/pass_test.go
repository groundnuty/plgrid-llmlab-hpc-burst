package sim

// The issue's pass criterion as a repeatable test, on a scaled-down copy of
// the experiment: 40 users instead of 160, and a KV cache per Instance
// scaled by the same quarter (375 k instead of 1.5 M tokens). It is not a
// copy of the tight regime: MaxRunning stays at 48, which 40 users never
// reach, so the per-Instance sequence limit never binds here. Expectations
// come from issue #27: the chosen key and bound beat round robin on hit
// rate and never exceed the bound, on every seed.

import "testing"

func scaledRun(t *testing.T, seed uint64, pol Policy, events ...Event) *Metrics {
	t.Helper()
	w := DefaultWorkload(seed)
	w.Users, w.Ramp, w.ArrivalEnd = 40, 120, 2400
	cl := DefaultCluster(375_000)
	e := NewEngine(cl, pol, w, nil, events)
	e.M.WinStart, e.M.WinEnd = 600, 2400
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	return e.M
}

// c = 2.0 is HBEP-5's decided default; 1.25 is the operator option.
var passBounds = []int{200, 125}

func TestPassCriterion(t *testing.T) {
	if testing.Short() {
		t.Skip("scaled experiment")
	}
	for _, seed := range []uint64{1, 2, 3} {
		rr := scaledRun(t, seed, Policy{Kind: RoundRobin})
		for _, pct := range passBounds {
			for _, key := range []KeyKind{KeyOpening, KeyHeaderOpening} {
				checkPass(t, seed, rr, scaledRun(t, seed, Policy{Kind: CHWBL, Key: key, Percent: pct}))
			}
		}
	}
}

// checkPass checks the hit rate, which can fail, and the bound, which
// cannot here: with the simulator's counts equal to KubeAI's (crossCheck)
// and c >= 1, the HBEP-5 rule is the inequality KubeAI itself applied, and
// the least-loaded member always satisfies it, so KubeAI never falls back
// to a member that fails it. The bound check is kept as a consistency
// check, not as evidence.
func checkPass(t *testing.T, seed uint64, rr, ch *Metrics) {
	t.Helper()
	p := ch.pol
	t.Logf("seed %d: hit rate round robin %.3f, %s/%s %.3f; above bound %d of %d placements; +1 over ceil %d",
		seed, rr.HitRate(), p.Name(), p.Key, ch.HitRate(), ch.KubeRuleViol, ch.Placements, ch.PaperViol)
	if ch.HitRate() <= rr.HitRate() {
		t.Errorf("seed %d: %s/%s hit rate %.3f does not beat round robin's %.3f", seed, p.Name(), p.Key, ch.HitRate(), rr.HitRate())
	}
	if ch.KubeRuleViol != 0 {
		t.Errorf("seed %d: %s/%s: %d placements above the bound", seed, p.Name(), p.Key, ch.KubeRuleViol)
	}
}

// A drain is where the bound can fail: without the release, KubeAI keeps
// the drained Instance's open requests in its total while n drops. In the
// same scaled setup, instance-1 drains at 1,500 s, and then a cold
// instance-4 joins at 1,200 s before instance-1 drains at 1,500 s
// (make-before-break). The unfixed balancer must break the members' bound
// somewhere, or this test proves nothing; that control is the empirical
// evidence. With the release (Policy.ReleaseOnDrain), the test is that
// KubeAI's total never exceeds the members' (inflation 0). Zero violations
// then follow from the accounting, as in steady state, and are only logged.
func TestPassCriterionHoldsThroughDrains(t *testing.T) {
	if testing.Short() {
		t.Skip("scaled experiment")
	}
	scenarios := map[string][]Event{
		"drain":    {{Time: 1500, Kind: Drain, Instance: 1}},
		"handover": {{Time: 1200, Kind: Join, Instance: 4}, {Time: 1500, Kind: Drain, Instance: 1}},
	}
	for name, ev := range scenarios {
		unfixed := map[int]int{}
		for _, seed := range []uint64{1, 2, 3} {
			for _, pct := range passBounds {
				for _, key := range []KeyKind{KeyOpening, KeyHeaderOpening} {
					pol := Policy{Kind: CHWBL, Key: key, Percent: pct}
					plain := scaledRun(t, seed, pol, ev...)
					unfixed[pct] += plain.KubeRuleViol
					pol.ReleaseOnDrain = true
					fixed := scaledRun(t, seed, pol, ev...)
					t.Logf("%s seed %d c=%d%% %s: unfixed above bound %d, inflation %d; released above bound %d, inflation %d",
						name, seed, pct, key, plain.KubeRuleViol, plain.MaxKubeInflation, fixed.KubeRuleViol, fixed.MaxKubeInflation)
					if plain.MaxKubeInflation == 0 {
						t.Errorf("%s seed %d c=%d%% %s: no inflation without the release; nothing was open on the drained Instance", name, seed, pct, key)
					}
					if fixed.MaxKubeInflation != 0 {
						t.Errorf("%s seed %d c=%d%% %s: KubeAI's total exceeded the members' by %d with the drain released", name, seed, pct, key, fixed.MaxKubeInflation)
					}
				}
			}
		}
		t.Logf("%s: unfixed placements above the bound by c: %v", name, unfixed)
		if unfixed[125]+unfixed[200] == 0 {
			t.Errorf("%s: the unfixed balancer never broke the bound; the test proves nothing", name)
		}
	}
}

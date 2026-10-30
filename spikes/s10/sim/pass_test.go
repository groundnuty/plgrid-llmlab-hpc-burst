package sim

// The issue's pass criterion as a repeatable test, on a scaled-down copy of
// the experiment: 40 users instead of 160, and a KV cache per Instance
// scaled by the same quarter (375 k instead of 1.5 M tokens, the "tight"
// regime where the cache about equals the working set). Expectations come
// from issue #27: the chosen key and bound beat round robin on hit rate
// and never exceed the bound, on every seed.

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

func TestPassCriterion(t *testing.T) {
	if testing.Short() {
		t.Skip("scaled experiment")
	}
	for _, seed := range []uint64{1, 2, 3} {
		rr := scaledRun(t, seed, Policy{Kind: RoundRobin})
		for _, key := range []KeyKind{KeyOpening, KeyHeaderOpening} {
			checkPass(t, seed, rr, scaledRun(t, seed, Policy{Kind: CHWBL, Key: key, Percent: 125}))
		}
	}
}

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

// In steady state the bound check above cannot fail: the simulator asserts
// that its counters equal KubeAI's, and the least-loaded member is always
// within the bound. A drain is where it can fail. In the same scaled setup,
// instance-1 drains at 1,500 s, and then a cold instance-4 joins at 1,200 s
// before instance-1 drains at 1,500 s (make-before-break). The unfixed
// balancer must break the members' bound somewhere (otherwise this test
// proves nothing); with the draining Instance released from the count
// (Policy.ReleaseOnDrain) no placement may break it.
func TestPassCriterionHoldsThroughDrains(t *testing.T) {
	if testing.Short() {
		t.Skip("scaled experiment")
	}
	scenarios := map[string][]Event{
		"drain":    {{Time: 1500, Kind: Drain, Instance: 1}},
		"handover": {{Time: 1200, Kind: Join, Instance: 4}, {Time: 1500, Kind: Drain, Instance: 1}},
	}
	for name, ev := range scenarios {
		unfixed := 0
		for _, seed := range []uint64{1, 2, 3} {
			for _, key := range []KeyKind{KeyOpening, KeyHeaderOpening} {
				pol := Policy{Kind: CHWBL, Key: key, Percent: 125}
				plain := scaledRun(t, seed, pol, ev...)
				unfixed += plain.KubeRuleViol
				pol.ReleaseOnDrain = true
				fixed := scaledRun(t, seed, pol, ev...)
				t.Logf("%s seed %d %s: above bound unfixed %d, released %d", name, seed, key, plain.KubeRuleViol, fixed.KubeRuleViol)
				if fixed.KubeRuleViol != 0 {
					t.Errorf("%s seed %d %s: %d placements above the bound with the drain released", name, seed, key, fixed.KubeRuleViol)
				}
			}
		}
		if unfixed == 0 {
			t.Errorf("%s: the unfixed balancer never broke the bound; the test proves nothing", name)
		}
	}
}

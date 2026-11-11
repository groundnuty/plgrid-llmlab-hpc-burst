package obscheck

// The gauge checks on their own, in an ordinary go test (see events_test.go).

import "testing"

func gaugeFixture() []Sample { return gaugeFixtureFor(DoubleNotReported) }

// gaugeFixtureFor is one read of an Instance whose engine exports every gauge but
// those in notReported, each of which is unknown not_reported.
func gaugeFixtureFor(notReported map[string]bool) []Sample {
	uid := map[string]string{"instance_uid": "u", "worker": "worker-0", "generation": "1", "worker_generation": "1"}
	var ss []Sample
	ss = append(ss, Sample{Name: "hpcburst_instance_info", Value: 1, Labels: map[string]string{"instance_uid": "u", "namespace": "default",
		"instance": "i", "model_deployment": "md", "provider": "p", "gpu_model": "g"}})
	for _, g := range EngineGauges {
		if notReported[g] {
			ss = append(ss, Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1,
				Labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": g, "reason": "not_reported"}})
			continue
		}
		for _, n := range seriesNames(g) {
			v := 1.0
			if g == GaugeGenerated {
				v = 9000
			}
			ss = append(ss, Sample{Name: n, Value: v, Labels: uid})
		}
	}
	return append(ss, Sample{Name: "hpcburst_instance_metrics_read_timestamp_seconds", Value: 1, Labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
}

func TestGaugeProblems(t *testing.T) {
	v := GaugeView{UID: "u", Namespace: "default", Name: "i", Deployment: "md", Provider: "p", GPUModel: "g", Reported: true, MinGenerated: 8000, NotReported: DoubleNotReported}
	if p := GaugeProblems(gaugeFixture(), v); len(p) != 0 {
		t.Fatalf("a correct exposition: %v", p)
	}
	for name, breakIt := range map[string]func([]Sample) []Sample{
		"no info series": func(s []Sample) []Sample { return s[1:] },
		"an unreported gauge exported as 0": func(s []Sample) []Sample {
			return append(s, Sample{Name: GaugePrefixQ, Value: 0, Labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
		},
		"a gauge both present and unknown": func(s []Sample) []Sample {
			return append(s, Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1,
				Labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": GaugeKV, "reason": "stale"}})
		},
		"a gauge neither present nor unknown": func(s []Sample) []Sample {
			var out []Sample
			for _, x := range s {
				if x.Name != GaugeWaiting {
					out = append(out, x)
				}
			}
			return out
		},
		"the summary without its count": func(s []Sample) []Sample {
			var out []Sample
			for _, x := range s {
				if x.Name != GaugeTPOT+"_count" {
					out = append(out, x)
				}
			}
			return out
		},
		"the generated tokens below what was served": func(s []Sample) []Sample {
			for i := range s {
				if s[i].Name == GaugeGenerated {
					s[i].Value = 10
				}
			}
			return s
		},
	} {
		if p := GaugeProblems(breakIt(gaugeFixture()), v); len(p) == 0 {
			t.Errorf("%s: gaugeProblems found nothing", name)
		}
	}
	stale := []Sample{{Name: "hpcburst_instance_info", Value: 1, Labels: map[string]string{"instance_uid": "s", "namespace": "default", "instance": "j",
		"model_deployment": "md", "provider": "p", "gpu_model": "g"}}}
	for _, g := range EngineGauges {
		stale = append(stale, Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1,
			Labels: map[string]string{"instance_uid": "s", "worker": "worker-0", "metric": g, "reason": "stale"}})
	}
	sv := GaugeView{UID: "s", Namespace: "default", Name: "j", Deployment: "md", Provider: "p", GPUModel: "g", StaleReason: "stale", NotReported: DoubleNotReported}
	if p := GaugeProblems(stale, sv); len(p) != 0 {
		t.Fatalf("a correct pending Instance: %v", p)
	}
	if p := GaugeProblems(stale[:5], sv); len(p) == 0 {
		t.Errorf("a pending Instance missing unknown samples: gaugeProblems found nothing")
	}
}

func TestParseExposition(t *testing.T) {
	ss, err := ParseExposition("# HELP x y\n# TYPE x counter\nx_total{a=\"1\",b=\"q\\\"z\"} 3\nup 1\nh_bucket{le=\"+Inf\"} 2\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 3 || ss[0].Name != "x_total" || ss[0].Labels["b"] != `q"z` || ss[0].Value != 3 || ss[1].Name != "up" || ss[2].Labels["le"] != "+Inf" {
		t.Errorf("parseExposition: %+v", ss)
	}
	if _, err := ParseExposition("x{a=\"1\" 3\n"); err == nil {
		t.Errorf("an unterminated label set parsed")
	}
}

func staleFixture(uid string) []Sample {
	ss := []Sample{{Name: "hpcburst_instance_info", Value: 1, Labels: map[string]string{"instance_uid": uid, "namespace": "default", "instance": "j",
		"model_deployment": "md", "provider": "p", "gpu_model": "g"}}}
	for _, g := range EngineGauges {
		ss = append(ss, Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1,
			Labels: map[string]string{"instance_uid": uid, "worker": "worker-0", "metric": g, "reason": "stale"}})
	}
	return ss
}

func TestPendingGaugeProblems(t *testing.T) {
	v := GaugeView{UID: "s", Namespace: "default", Name: "j", Deployment: "md", Provider: "p", GPUModel: "g", StaleReason: "stale", NotReported: DoubleNotReported}
	if p, none := PendingGaugeProblems(staleFixture("s"), v); len(p) != 0 || none {
		t.Fatalf("a pending Instance shown stale: %v (none %v)", p, none)
	}
	if p, none := PendingGaugeProblems(staleFixture("s")[:1], v); len(p) != 0 || !none {
		t.Fatalf("a pending Instance with its info series alone: %v (none %v), want no problem and none", p, none)
	}
	for name, ss := range map[string][]Sample{
		"no info series":        staleFixture("s")[1:],
		"some unknowns only":    staleFixture("s")[:4],
		"a gauge read as 0":     append(staleFixture("s"), Sample{Name: GaugeRunning, Labels: map[string]string{"instance_uid": "s", "worker": "worker-0"}}),
		"unknown, not as stale": append(staleFixture("s")[:1], Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1, Labels: map[string]string{"instance_uid": "s", "worker": "worker-0", "metric": GaugeRunning, "reason": "not_reported"}}),
		"the info series twice": append(staleFixture("s"), staleFixture("s")[0]),
	} {
		if p, _ := PendingGaugeProblems(ss, v); len(p) == 0 {
			t.Errorf("%s: pendingGaugeProblems found nothing", name)
		}
	}
}

func TestRestartReadProblems(t *testing.T) {
	neither := []Sample{}
	for _, s := range gaugeFixture() {
		if s.Name != GaugeWaiting {
			neither = append(neither, s)
		}
	}
	reads := [][]Sample{gaugeFixture(), staleFixture("u"), gaugeFixture()}
	if p, stale, fresh, unserved := RestartReadProblems(append(reads, nil), "u", DoubleNotReported); len(p) != 0 || stale != 1 || fresh != 2 || unserved != 1 {
		t.Fatalf("fresh, stale, fresh, none: %v, %d stale, %d fresh, %d unserved", p, stale, fresh, unserved)
	}
	if p, _, _, _ := RestartReadProblems([][]Sample{gaugeFixture(), neither}, "u", DoubleNotReported); len(p) == 0 {
		t.Errorf("a read with a gauge neither known nor unknown: restartReadProblems found nothing")
	}
	both := append(gaugeFixture(), Sample{Name: "hpcburst_instance_metrics_unknown", Value: 1, Labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": GaugeKV, "reason": "stale"}})
	if p, _, _, _ := RestartReadProblems([][]Sample{both}, "u", DoubleNotReported); len(p) == 0 {
		t.Errorf("a read with a gauge both known and unknown: restartReadProblems found nothing")
	}
	reported := append(gaugeFixture(), Sample{Name: GaugePrefixHit, Value: 0, Labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
	if p, _, _, _ := RestartReadProblems([][]Sample{reported}, "u", DoubleNotReported); len(p) == 0 {
		t.Errorf("a read with an unexported gauge at 0: restartReadProblems found nothing")
	}
	if p, _, _, _ := RestartReadProblems([][]Sample{gaugeFixture()[1:]}, "u", DoubleNotReported); len(p) == 0 {
		t.Errorf("a read with gauges and no info series: restartReadProblems found nothing")
	}
	// A stale read whose unexported gauges say not_reported is correct too.
	if p, stale, fresh, _ := RestartReadProblems([][]Sample{gaugeFixture(), withReason(staleFixture("u"), "not_reported", DoubleNotReported)}, "u", DoubleNotReported); len(p) != 0 || stale != 0 || fresh != 1 {
		t.Errorf("fresh, then stale with not_reported on the unexported gauges: %v, %d stale, %d fresh", p, stale, fresh)
	}
	// Review of PR #247: the reads of the probe, a fresh read and then the
	// stale read with another reason.
	exported := map[string]bool{}
	for _, g := range EngineGauges {
		exported[g] = !DoubleNotReported[g]
	}
	all := map[string]bool{}
	for _, g := range EngineGauges {
		all[g] = true
	}
	for name, read := range map[string][]Sample{
		"not_reported on the exported gauges": withReason(staleFixture("u"), "not_reported", exported),
		"agent_unreachable on every gauge":    withReason(staleFixture("u"), "agent_unreachable", all),
		"no reason on one gauge":              withReason(staleFixture("u"), "", map[string]bool{GaugeRunning: true}),
	} {
		if p, _, _, _ := RestartReadProblems([][]Sample{gaugeFixture(), read}, "u", DoubleNotReported); len(p) == 0 {
			t.Errorf("a fresh read, then %s: restartReadProblems found nothing", name)
		}
	}
}

// withReason sets the reason of the unknown samples of the named gauges.
func withReason(ss []Sample, reason string, gauges map[string]bool) []Sample {
	for i := range ss {
		if ss[i].Name == "hpcburst_instance_metrics_unknown" && gauges[ss[i].Labels["metric"]] {
			ss[i].Labels["reason"] = reason
		}
	}
	return ss
}

// The set an engine does not export is the caller's (issue #376, P3): under a real
// vLLM's set, which is empty (HBEP-8: v0.32.0 registers both prefix-cache counters
// unconditionally), a read with every gauge present passes, and the double's read,
// with the two prefix-cache gauges unknown not_reported, fails; under the double's
// set the reverse holds. So the set is read from the parameter, not fixed.
func TestTheNotReportedSetIsTheCallers(t *testing.T) {
	if len(VLLMNotReported) != 0 {
		t.Fatalf("VLLMNotReported is %v, want empty: a real vLLM exports every engine gauge", VLLMNotReported)
	}
	full, double := gaugeFixtureFor(VLLMNotReported), gaugeFixtureFor(DoubleNotReported)
	view := func(nr map[string]bool) GaugeView {
		return GaugeView{UID: "u", Namespace: "default", Name: "i", Deployment: "md", Provider: "p", GPUModel: "g", Reported: true, MinGenerated: 8000, NotReported: nr}
	}
	for _, c := range []struct {
		name string
		ss   []Sample
		nr   map[string]bool
		pass bool
	}{
		{"a vLLM read under the vLLM set", full, VLLMNotReported, true},
		{"the double's read under the vLLM set", double, VLLMNotReported, false},
		{"the double's read under the double's set", double, DoubleNotReported, true},
		{"a vLLM read under the double's set", full, DoubleNotReported, false},
	} {
		if p := GaugeProblems(c.ss, view(c.nr)); (len(p) == 0) != c.pass {
			t.Errorf("GaugeProblems, %s: %v, want pass %v", c.name, p, c.pass)
		}
		if p, _, _, _ := RestartReadProblems([][]Sample{c.ss}, "u", c.nr); (len(p) == 0) != c.pass {
			t.Errorf("RestartReadProblems, %s: %v, want pass %v", c.name, p, c.pass)
		}
	}
}

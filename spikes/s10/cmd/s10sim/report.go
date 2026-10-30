package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

// steady compares every policy and key with no membership change.
func (r *runner) steady(dir string, kvs []int) {
	var jobs []job
	for _, kv := range kvs {
		for _, p := range policies() {
			for _, seed := range r.seeds {
				jobs = append(jobs, job{kv: kv, seed: seed, pol: p, scen: "steady"})
			}
		}
	}
	res := r.run(jobs)
	var md, csv strings.Builder
	csv.WriteString("kv_tokens,seed,policy,key,hit_rate,later_turn_hit_rate,prefill_tokens,mean_latency_s,p95_latency_s,move_rate,distinct_keys,top_key_share,peak_inflight,max_place_ratio,bound_violations,paper_violations,max_place_excess,time_above_bound,active_convs,active_context_tokens,mean_inflight\n")
	for _, kv := range kvs {
		groups := groupBy(res, func(x result) bool { return x.kv == kv })
		fmt.Fprintf(&md, "\n### KV cache %d tokens per Instance\n\n", kv)
		writeWorkingSet(&md, groups, kv)
		md.WriteString("| policy / key | hit rate | later turns | prefill vs RR | mean / p95 latency (s) | moves | keys (top share) | peak in-flight | max load / mean | above bound (HBEP-5 rule) | +1 over ceil(c*mean) | time above c*mean |\n")
		md.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		rrPrefill := mean(groups[0], func(m *sim.Metrics) float64 { return float64(m.PrefillTokens) })
		for _, g := range groups {
			writeSteadyRow(&md, g, rrPrefill)
			for _, x := range g {
				writeSteadyCSV(&csv, x)
			}
		}
	}
	writeFile(dir, "steady.md", "## Steady state (window 1200-4800 s; mean over seeds, [min, max])\n"+md.String())
	writeFile(dir, "steady.csv", csv.String())
}

// groupBy keeps results matching keep and groups them by policy, in the
// order of policies().
func groupBy(res []result, keep func(result) bool) [][]result {
	var out [][]result
	idx := map[string]int{}
	for _, x := range res {
		if !keep(x) {
			continue
		}
		k := label(x.pol) + "|" + x.scen
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], x)
	}
	return out
}

func mean(g []result, f func(*sim.Metrics) float64) float64 {
	var s float64
	for _, x := range g {
		s += f(x.m)
	}
	return s / float64(len(g))
}

func span(g []result, f func(*sim.Metrics) float64) (lo, hi float64) {
	lo, hi = math.Inf(1), math.Inf(-1)
	for _, x := range g {
		v := f(x.m)
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	return lo, hi
}

func maxOf(g []result, f func(*sim.Metrics) float64) float64 {
	_, hi := span(g, f)
	return hi
}

func sumOf(g []result, f func(*sim.Metrics) float64) float64 {
	return mean(g, f) * float64(len(g))
}

func pct(v float64) string { return fmt.Sprintf("%.1f%%", 100*v) }

func writeWorkingSet(md *strings.Builder, groups [][]result, kv int) {
	g := groups[0] // round robin: the workload is the same for all policies
	c := mean(g, func(m *sim.Metrics) float64 { a, _, _ := m.WorkingSet(); return a })
	tok := mean(g, func(m *sim.Metrics) float64 { _, b, _ := m.WorkingSet(); return b })
	fmt.Fprintf(md, "Round robin's time-averaged working set: %.0f active conversations holding %.2f M context tokens, %.2fx the cluster's KV cache (%d x %d tokens).\n\n",
		c, tok/1e6, tok/float64(4*kv), 4, kv)
}

func writeSteadyRow(md *strings.Builder, g []result, rrPrefill float64) {
	p := g[0].pol
	hit := func(m *sim.Metrics) float64 { return m.HitRate() }
	lo, hi := span(g, hit)
	d := mean(g, func(m *sim.Metrics) float64 { n, _ := m.Keys(); return float64(n) })
	top := mean(g, func(m *sim.Metrics) float64 { _, t := m.Keys(); return t })
	keys := "-"
	if p.Hashed() {
		keys = fmt.Sprintf("%.0f (%s)", d, pct(top))
	}
	bound, paper, above := "-", "-", "-"
	if p.Kind == sim.CHWBL {
		bound = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.KubeRuleViol) }))
		paper = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.PaperViol) }))
		above = pct(mean(g, func(m *sim.Metrics) float64 { return m.TimeAboveBound() }))
	}
	fmt.Fprintf(md, "| %s | %s [%s, %s] | %s | %.2fx | %.1f / %.1f | %s | %s | %.0f | %.2f | %s | %s | %s |\n",
		label(p), pct(mean(g, hit)), pct(lo), pct(hi),
		pct(mean(g, func(m *sim.Metrics) float64 { return m.HitRate1() })),
		mean(g, func(m *sim.Metrics) float64 { return float64(m.PrefillTokens) })/rrPrefill,
		mean(g, func(m *sim.Metrics) float64 { return m.MeanLatency() }),
		mean(g, func(m *sim.Metrics) float64 { return m.Latency(0.95) }),
		pct(mean(g, func(m *sim.Metrics) float64 { return m.MoveRate() })),
		keys,
		maxOf(g, func(m *sim.Metrics) float64 { return float64(m.PeakLoad) }),
		maxOf(g, func(m *sim.Metrics) float64 { return m.MaxPlaceRatio }),
		bound, paper, above)
}

func writeSteadyCSV(csv *strings.Builder, x result) {
	m := x.m
	d, top := m.Keys()
	c, tok, inf := m.WorkingSet()
	fmt.Fprintf(csv, "%d,%d,%s,%s,%.5f,%.5f,%d,%.3f,%.3f,%.5f,%d,%.4f,%d,%.3f,%d,%d,%.3f,%.5f,%.2f,%.0f,%.2f\n",
		x.kv, x.seed, x.pol.Name(), x.pol.Key, m.HitRate(), m.HitRate1(), m.PrefillTokens, m.MeanLatency(), m.Latency(0.95),
		m.MoveRate(), d, top, m.PeakLoad, m.MaxPlaceRatio, m.KubeRuleViol, m.PaperViol, m.MaxPlaceExcess, m.TimeAboveBound(), c, tok, inf)
}

// eventPolicies are the configurations compared around membership events.
func eventPolicies() []sim.Policy {
	ps := []sim.Policy{{Kind: sim.RoundRobin}, {Kind: sim.LeastLoad}}
	for _, k := range []sim.KeyKind{sim.KeyHeaderOpening, sim.KeyOpening} {
		ps = append(ps, sim.Policy{Kind: sim.Affinity, Key: k})
		for _, pct := range []int{125, 150, 200} {
			ps = append(ps, sim.Policy{Kind: sim.CHWBL, Key: k, Percent: pct})
		}
	}
	return ps
}

var scenarios = []struct {
	name   string
	events []sim.Event
}{
	{"none", nil},
	{"drain", []sim.Event{{Time: evTime, Kind: sim.Drain, Instance: 1}}},
	{"leave", []sim.Event{{Time: evTime, Kind: sim.Leave, Instance: 1}}},
	{"handover", []sim.Event{{Time: evTime, Kind: sim.Join, Instance: 4}, {Time: evTime + 300, Kind: sim.Drain, Instance: 1}}},
}

// eventRuns measures what a drain, a leave and a handover do.
func (r *runner) eventRuns(dir string, kvs []int) {
	for _, kv := range kvs {
		r.eventRunsAt(dir, kv)
	}
}

func (r *runner) eventRunsAt(dir string, kv int) {
	var jobs []job
	for _, sc := range scenarios {
		for _, p := range eventPolicies() {
			for _, seed := range r.seeds {
				jobs = append(jobs, job{kv: kv, seed: seed, pol: p, events: sc.events, scen: sc.name})
			}
		}
	}
	res := r.run(jobs)
	base := map[string]*sim.Metrics{}
	for _, x := range res {
		if x.scen == "none" {
			base[fmt.Sprintf("%s|%d", label(x.pol), x.seed)] = x.m
		}
	}
	var md strings.Builder
	fmt.Fprintf(&md, "## Membership events (KV cache %d tokens per Instance; event at %.0f s; mean over seeds)\n", kv, evTime)
	for _, sc := range scenarios[1:] {
		writeEventTable(&md, sc.name, groupBy(res, func(x result) bool { return x.scen == sc.name }), base)
	}
	writeFile(dir, fmt.Sprintf("events-%d.md", kv), md.String())
}

func writeEventTable(md *strings.Builder, scen string, groups [][]result, base map[string]*sim.Metrics) {
	fmt.Fprintf(md, "\n### %s\n\n", scen)
	md.WriteString("| policy / key | checked | re-homed: forced | collateral | to new | cut | hit before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound (HBEP-5 rule) | +1 over ceil | KubeAI mean inflation (max req.) |\n")
	md.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, g := range groups {
		p := g[0].pol
		rc := func(f func(sim.RehomeCount) int) float64 {
			return mean(g, func(m *sim.Metrics) float64 {
				s := 0
				for _, c := range m.Rehomed {
					s += f(c)
				}
				return float64(s)
			})
		}
		win := func(from, to float64) (ev, bs float64) {
			ev = mean(g, func(m *sim.Metrics) float64 { return m.BucketHitRate(from, to) })
			var s float64
			for _, x := range g {
				s += base[fmt.Sprintf("%s|%d", label(x.pol), x.seed)].BucketHitRate(from, to)
			}
			return ev, s / float64(len(g))
		}
		before, _ := win(evTime-600, evTime)
		a1, b1 := win(evTime, evTime+300)
		a2, b2 := win(evTime+300, evTime+900)
		bound, paper := "-", "-"
		if p.Kind == sim.CHWBL {
			bound = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.KubeRuleViol) }))
			paper = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.PaperViol) }))
		}
		fmt.Fprintf(md, "| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %s | %s (%s) | %s (%s) | %s | %s | %.0f |\n",
			label(p), rc(func(c sim.RehomeCount) int { return c.Checked }),
			rc(func(c sim.RehomeCount) int { return c.Forced }),
			rc(func(c sim.RehomeCount) int { return c.Collateral }),
			rc(func(c sim.RehomeCount) int { return c.ToJoined }),
			mean(g, func(m *sim.Metrics) float64 { return float64(m.Aborted) }),
			pct(before), pct(a1), pct(b1), pct(a2), pct(b2), bound, paper,
			maxOf(g, func(m *sim.Metrics) float64 { return float64(m.MaxKubeInflation) }))
	}
}

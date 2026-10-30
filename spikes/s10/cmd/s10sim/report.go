package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

// steady compares every policy and key with no membership change.
func (r *runner) steady(dir string, kvs []int) {
	r.steadyAs(dir, kvs, policies(), "steady", "Steady state")
}

// steadyAs runs pols with no membership change at each KV size and writes
// name.md and name.csv.
func (r *runner) steadyAs(dir string, kvs []int, pols []sim.Policy, name, title string) {
	var jobs []job
	for _, kv := range kvs {
		for _, p := range pols {
			for _, seed := range r.seeds {
				jobs = append(jobs, job{kv: kv, seed: seed, pol: p, scen: "steady"})
			}
		}
	}
	res := r.run(jobs)
	var md, csv strings.Builder
	csv.WriteString(csvHeader)
	for _, kv := range kvs {
		groups := groupBy(res, func(x result) bool { return x.kv == kv })
		fmt.Fprintf(&md, "\n### KV cache %d tokens per Instance\n\n", kv)
		writeWorkingSet(&md, groups, kv)
		md.WriteString("| policy / key | hit rate | later turns | throughput vs RR (completed in window) | throughput vs RR (dispatched in window) | prefill tokens / request | mean / p95 latency (s) | moves | keys (top share) | peak in-flight (max) | max load / mean (max) | above bound, HBEP-5 rule (sum over seeds) | +1 over ceil(c*mean) (sum over seeds) | time above c*mean |\n")
		md.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		rrDone := mean(groups[0], func(m *sim.Metrics) float64 { return float64(m.DoneOutputTokens) })
		rrOut := mean(groups[0], func(m *sim.Metrics) float64 { return float64(m.OutputTokens) })
		for _, g := range groups {
			writeSteadyRow(&md, g, rrDone, rrOut)
			for _, x := range g {
				writeSteadyCSV(&csv, x)
			}
		}
	}
	writeFile(dir, name+".md", fmt.Sprintf("## %s (window %.0f-%.0f s; block %d tokens; prefill %.0f tokens/s)\n\nMeans over seeds, with [min, max] for the hit rate. Columns marked sum or max are sums or maxima over seeds. Round robin is the first row of each table.\n",
		title, winStart, winEnd, r.block, r.prefill)+md.String())
	writeFile(dir, name+".csv", csv.String())
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

func writeSteadyRow(md *strings.Builder, g []result, rrDone, rrOut float64) {
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
		pv := sumOf(g, func(m *sim.Metrics) float64 { return float64(m.PaperViol) })
		pl := sumOf(g, func(m *sim.Metrics) float64 { return float64(m.Placements) })
		paper = fmt.Sprintf("%.0f (%.2f%% of placements)", pv, 100*pv/pl)
		above = pct(mean(g, func(m *sim.Metrics) float64 { return m.TimeAboveBound() }))
	}
	fmt.Fprintf(md, "| %s | %s [%s, %s] | %s | %.2fx | %.2fx | %.0f | %.1f / %.1f | %s | %s | %.0f | %.2f | %s | %s | %s |\n",
		label(p), pct(mean(g, hit)), pct(lo), pct(hi),
		pct(mean(g, func(m *sim.Metrics) float64 { return m.HitRate1() })),
		mean(g, func(m *sim.Metrics) float64 { return float64(m.DoneOutputTokens) })/rrDone,
		mean(g, func(m *sim.Metrics) float64 { return float64(m.OutputTokens) })/rrOut,
		mean(g, func(m *sim.Metrics) float64 { return float64(m.PrefillTokens) / float64(m.Requests) }),
		mean(g, func(m *sim.Metrics) float64 { return m.MeanLatency() }),
		mean(g, func(m *sim.Metrics) float64 { return m.Latency(0.95) }),
		pct(mean(g, func(m *sim.Metrics) float64 { return m.MoveRate() })),
		keys,
		maxOf(g, func(m *sim.Metrics) float64 { return float64(m.PeakLoad) }),
		maxOf(g, func(m *sim.Metrics) float64 { return m.MaxPlaceRatio }),
		bound, paper, above)
}

const csvHeader = "scenario,kv_tokens,seed,policy,key,requests,output_tokens,prompt_tokens,hit_tokens,prefill_tokens,later_prompt_tokens,later_hit_tokens,mean_latency_s,p95_latency_s,move_rate,distinct_keys,top_key_share,peak_inflight,max_place_ratio,bound_violations,paper_violations,max_place_excess,time_above_bound,max_kubeai_inflation,max_rule_excess,aborted,compactions,active_convs,active_context_tokens,mean_inflight,inst_requests,inst_hit_rate,inst_peak_inflight,done_output_tokens,max_kubeai_mean_inflation,placements\n"

func writeSteadyCSV(csv *strings.Builder, x result) {
	m := x.m
	d, top := m.Keys()
	c, tok, inf := m.WorkingSet()
	var reqs, hits, peaks []string
	for _, st := range m.PerInst {
		reqs = append(reqs, fmt.Sprint(st.Requests))
		hits = append(hits, fmt.Sprintf("%.3f", ratioF(st.Hit, st.Prompt)))
		peaks = append(peaks, fmt.Sprint(st.PeakInFlight))
	}
	fmt.Fprintf(csv, "%s,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.5f,%d,%.4f,%d,%.3f,%d,%d,%.3f,%.5f,%d,%.2f,%d,%d,%.2f,%.0f,%.2f,%s,%s,%s,%d,%.2f,%d\n",
		x.scen, x.kv, x.seed, x.pol.Name(), x.pol.Key, m.Requests, m.OutputTokens, m.PromptTokens, m.HitTokens, m.PrefillTokens,
		m.PromptTokens1, m.HitTokens1, m.MeanLatency(), m.Latency(0.95), m.MoveRate(), d, top, m.PeakLoad, m.MaxPlaceRatio,
		m.KubeRuleViol, m.PaperViol, m.MaxPlaceExcess, m.TimeAboveBound(), m.MaxKubeInflation, m.MaxRuleExcess, m.Aborted, m.Compactions,
		c, tok, inf, strings.Join(reqs, ";"), strings.Join(hits, ";"), strings.Join(peaks, ";"), m.DoneOutputTokens, m.MaxKubeMeanInflation, m.Placements)
}

func ratioF(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
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

type scenario struct {
	name   string
	events []sim.Event
}

var scenarios = []scenario{
	{"none", nil},
	{"drain", []sim.Event{{Time: evTime, Kind: sim.Drain, Instance: 1}}},
	{"leave", []sim.Event{{Time: evTime, Kind: sim.Leave, Instance: 1}}},
	{"handover", []sim.Event{{Time: evTime, Kind: sim.Join, Instance: 4}, {Time: evTime + 300, Kind: sim.Drain, Instance: 1}}},
}

// eventRuns measures what a drain, a leave and a handover do.
func (r *runner) eventRuns(dir string, kvs []int) {
	for _, kv := range kvs {
		r.eventRunsAt(dir, kv, "events", eventPolicies(), scenarios)
	}
}

// drainFix runs the drain with the front releasing the draining Instance's
// requests from the balancer's count (Policy.ReleaseOnDrain).
func (r *runner) drainFix(dir string, kvs []int) {
	var ps []sim.Policy
	for _, k := range []sim.KeyKind{sim.KeyHeaderOpening, sim.KeyOpening} {
		for _, pct := range []int{125, 150, 200} {
			ps = append(ps, sim.Policy{Kind: sim.CHWBL, Key: k, Percent: pct, ReleaseOnDrain: true})
		}
	}
	for _, kv := range kvs {
		r.eventRunsAt(dir, kv, "drainfix", ps, []scenario{scenarios[0], scenarios[1], scenarios[3]})
	}
}

func (r *runner) eventRunsAt(dir string, kv int, name string, pols []sim.Policy, scens []scenario) {
	var jobs []job
	for _, sc := range scens {
		for _, p := range pols {
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
	var csv, evcsv strings.Builder
	csv.WriteString(csvHeader)
	evcsv.WriteString(eventCSVHeader)
	for _, x := range res {
		writeSteadyCSV(&csv, x)
		if x.scen != "none" {
			writeEventCSV(&evcsv, x, base[fmt.Sprintf("%s|%d", label(x.pol), x.seed)])
		}
	}
	writeFile(dir, fmt.Sprintf("%s-%d.csv", name, kv), csv.String())
	writeFile(dir, fmt.Sprintf("%s-%d-per-event.csv", name, kv), evcsv.String())
	var md strings.Builder
	fmt.Fprintf(&md, "## Membership events (KV cache %d tokens per Instance)\n\nOne row per event. Hit-rate windows start at that row's own event time: before = the 10 min before it, then 0-5 and 5-15 min after it, over requests dispatched in the window; the baseline is the same seed and policy with no event. Counts and hit rates are means over seeds; columns marked sum or max are sums or maxima over seeds. Raw per-seed values: %s-%d-per-event.csv.\n", kv, name, kv)
	for _, sc := range scens[1:] {
		writeEventTable(&md, sc.name, groupBy(res, func(x result) bool { return x.scen == sc.name }), base)
	}
	writeFile(dir, fmt.Sprintf("%s-%d.md", name, kv), md.String())
}

func writeEventTable(md *strings.Builder, scen string, groups [][]result, base map[string]*sim.Metrics) {
	fmt.Fprintf(md, "\n### %s\n\n", scen)
	md.WriteString("| policy / key | event | checked | re-homed: forced | collateral | to new | cut (whole run) | hit 10 min before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound, HBEP-5 rule (whole run, sum over seeds) | max over bound (req., max) | +1 over ceil (whole run, sum over seeds) | KubeAI total minus members' total (req., max) | KubeAI mean minus members' mean (req., max; the excess / members) |\n")
	md.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, g := range groups {
		for k := range g[0].m.Rehomed {
			writeEventRow(md, g, k, base)
		}
	}
}

// writeEventRow writes one policy's row for event k. The hit-rate windows
// start at event k's own time. The cut and bound columns cover the whole
// run; they repeat on each event's row.
func writeEventRow(md *strings.Builder, g []result, k int, base map[string]*sim.Metrics) {
	{
		p := g[0].pol
		rc := func(f func(sim.RehomeCount) int) float64 {
			return mean(g, func(m *sim.Metrics) float64 { return float64(f(m.Rehomed[k])) })
		}
		win := func(from, to float64) (ev, bs float64) {
			ev = mean(g, func(m *sim.Metrics) float64 { return m.BucketHitRate(from, to) })
			var s float64
			for _, x := range g {
				s += base[fmt.Sprintf("%s|%d", label(x.pol), x.seed)].BucketHitRate(from, to)
			}
			return ev, s / float64(len(g))
		}
		t0 := g[0].m.Rehomed[k].Event.Time
		before, _ := win(t0-600, t0)
		a1, b1 := win(t0, t0+300)
		a2, b2 := win(t0+300, t0+900)
		bound, paper, excess := "-", "-", "-"
		if p.Kind == sim.CHWBL {
			excess = fmt.Sprintf("%.1f", maxOf(g, func(m *sim.Metrics) float64 { return m.MaxRuleExcess }))
			bound = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.KubeRuleViol) }))
			paper = fmt.Sprintf("%.0f", sumOf(g, func(m *sim.Metrics) float64 { return float64(m.PaperViol) }))
		}
		ev := g[0].m.Rehomed[k].Event
		fmt.Fprintf(md, "| %s | %s %s at %.0f s | %.0f | %.0f | %.0f | %.0f | %.0f | %s | %s (%s) | %s (%s) | %s | %s | %s | %.0f | %.1f |\n",
			label(p), ev.Kind, fmt.Sprintf("instance-%d", ev.Instance), ev.Time, rc(func(c sim.RehomeCount) int { return c.Checked }),
			rc(func(c sim.RehomeCount) int { return c.Forced }),
			rc(func(c sim.RehomeCount) int { return c.Collateral }),
			rc(func(c sim.RehomeCount) int { return c.ToJoined }),
			mean(g, func(m *sim.Metrics) float64 { return float64(m.Aborted) }),
			pct(before), pct(a1), pct(b1), pct(a2), pct(b2), bound, excess, paper,
			maxOf(g, func(m *sim.Metrics) float64 { return float64(m.MaxKubeInflation) }),
			maxOf(g, func(m *sim.Metrics) float64 { return m.MaxKubeMeanInflation }))
	}
}

// eventWindows are the hit-rate windows around an event at t: the 10 min
// before it, and 0-5 and 5-15 min after it.
func eventWindows(t float64) [3][2]float64 {
	return [3][2]float64{{t - 600, t}, {t, t + 300}, {t + 300, t + 900}}
}

const eventCSVHeader = "scenario,kv_tokens,seed,policy,key,event_index,event_kind,event_instance,event_time_s,checked,forced,collateral,to_joined,aborted_whole_run," +
	"hit_tokens_before,prompt_tokens_before,hit_tokens_0_5,prompt_tokens_0_5,hit_tokens_5_15,prompt_tokens_5_15," +
	"base_hit_tokens_before,base_prompt_tokens_before,base_hit_tokens_0_5,base_prompt_tokens_0_5,base_hit_tokens_5_15,base_prompt_tokens_5_15\n"

// writeEventCSV writes one row per event of a run: its re-homing counts and
// the hit and prompt token sums of each window, for the run and for its
// no-event baseline.
func writeEventCSV(csv *strings.Builder, x result, base *sim.Metrics) {
	for k, rc := range x.m.Rehomed {
		ev := rc.Event
		fmt.Fprintf(csv, "%s,%d,%d,%s,%s,%d,%s,instance-%d,%.0f,%d,%d,%d,%d,%d", x.scen, x.kv, x.seed, x.pol.Name(), x.pol.Key,
			k, ev.Kind, ev.Instance, ev.Time, rc.Checked, rc.Forced, rc.Collateral, rc.ToJoined, x.m.Aborted)
		for _, m := range []*sim.Metrics{x.m, base} {
			for _, w := range eventWindows(ev.Time) {
				h, p := m.BucketTokens(w[0], w[1])
				fmt.Fprintf(csv, ",%d,%d", h, p)
			}
		}
		csv.WriteString("\n")
	}
}

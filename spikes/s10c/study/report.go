package study

import (
	"fmt"
	"math"
	"strings"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// AllOptions is S10b's four options followed by option 5.
func AllOptions() []b.Option { return append(append([]b.Option(nil), b.Options...), Option5) }

func meanOf(cs []b.Cell, f func(b.Cell) float64) float64 {
	var s float64
	for _, c := range cs {
		s += f(c)
	}
	return s / float64(len(cs))
}

func maxOf(cs []b.Cell, f func(b.Cell) float64) float64 {
	m := math.Inf(-1)
	for _, c := range cs {
		m = math.Max(m, f(c))
	}
	return m
}

// Evaluate computes the pass rule's inputs for the five options under key,
// by S10b's definitions (b.Evaluate does the same for options 1-4 only, and
// is the oracle for this function in TestEvaluateAgreesWithS10b, since its
// option list is fixed to S10b's four).
func Evaluate(cells []b.Cell, key string) []b.OptionVerdict {
	gs := b.Groups(cells)
	var out []b.OptionVerdict
	for _, o := range AllOptions() {
		v := b.OptionVerdict{Option: o.Name, Peak: map[string]int{}, AboveS: map[string]float64{}, Cond1Tight: true, Cond1All: true}
		for _, rg := range b.FullMembership {
			g, ok := b.Find(gs, rg.Name, key, o.Name)
			if !ok { // no data cannot show the option stays within the limit
				v.Cond1All = false
				if rg.Name == b.Tight.Name {
					v.Cond1Tight = false
				}
				continue
			}
			v.Peak[rg.Name] = int(maxOf(g.Cells, func(c b.Cell) float64 { return float64(c.PeakRun) }))
			v.AboveS[rg.Name] = meanOf(g.Cells, func(c b.Cell) float64 { return c.AboveRunS }) * float64(len(g.Cells))
			ok1 := v.Peak[rg.Name] <= b.EngineLimit && v.AboveS[rg.Name] == 0
			v.Cond1All = v.Cond1All && ok1
			if rg.Name == b.Tight.Name {
				v.Cond1Tight = ok1
			}
		}
		if d, ok := b.Find(gs, b.Drain.Name, key, o.Name); ok {
			base, _ := b.Find(gs, b.Tight.Name, key, o.Name)
			v.Hit05 = meanOf(d.Cells, func(c b.Cell) float64 { return c.Hit05 })
			v.Hit515 = meanOf(d.Cells, func(c b.Cell) float64 { return c.Hit515 })
			v.Drop05 = meanOf(base.Cells, func(c b.Cell) float64 { return c.Hit05 }) - v.Hit05
			v.Drop515 = meanOf(base.Cells, func(c b.Cell) float64 { return c.Hit515 }) - v.Hit515
			v.DrainScore = (v.Hit05 + v.Hit515) / 2
		}
		if s, ok := b.Find(gs, b.Scarce.Name, key, o.Name); ok {
			v.ScarceHit = meanOf(s.Cells, func(c b.Cell) float64 { return c.HitRate })
			if rr, ok := b.Baseline(gs, b.Scarce.Name); ok {
				v.ScarceVsRR = v.ScarceHit - meanOf(rr.Cells, func(c b.Cell) float64 { return c.HitRate })
			}
		}
		out = append(out, v)
	}
	return out
}

// Missing lists the runs a verdict on key needs and the cells lack: round
// robin and all five options in every regime on every one of
// b.RequiredSeeds. It is empty for a complete set.
func Missing(cells []b.Cell, key string) []string {
	have := map[string]bool{}
	for _, c := range cells {
		have[fmt.Sprintf("%s|%s|%s|%d", c.Regime, c.Key, c.Option, c.Seed)] = true
	}
	var out []string
	for _, rg := range b.Regimes {
		for _, o := range append([]b.Option{b.RoundRobin}, AllOptions()...) {
			k := key
			if o.ID == b.RoundRobin.ID {
				k = "-"
			}
			var seeds []string
			for _, s := range b.RequiredSeeds {
				if !have[fmt.Sprintf("%s|%s|%s|%d", rg.Name, k, o.Name, s)] {
					seeds = append(seeds, fmt.Sprint(s))
				}
			}
			if len(seeds) > 0 {
				out = append(out, fmt.Sprintf("%s / %s: seeds %s", rg.Name, o.Name, strings.Join(seeds, ", ")))
			}
		}
	}
	return out
}

func pct(v float64) string { return fmt.Sprintf("%.1f%%", 100*v) }

// VerdictMarkdown renders the pass rule over the five options for one key,
// in the layout of S10b's verdict.md. A set that lacks runs gets no verdict,
// only the list of what is missing.
func VerdictMarkdown(cells []b.Cell, key string) string {
	if missing := Missing(cells, key); len(missing) > 0 {
		var sb strings.Builder
		fmt.Fprintf(&sb, "Key %s: INCOMPLETE. No verdict; these runs are missing:\n\n", key)
		for _, m := range missing {
			fmt.Fprintf(&sb, "- %s\n", m)
		}
		return sb.String()
	}
	vs := Evaluate(cells, key)
	var sb strings.Builder
	fmt.Fprintf(&sb, "Key %s, %d seeds.\n\n", key, len(b.Groups(cells)[0].Cells))
	sb.WriteString("| option | peak in flight: tight / scarce / half prefill (max over seeds) | time above 48, summed over seeds (s): tight / scarce / half prefill | condition 1, tight | condition 1, all three | hit 0-5 min after drain (drop vs no drain) | hit 5-15 min after drain (drop) | scarce hit (vs RR) |\n|---|---|---|---|---|---|---|---|\n")
	for _, v := range vs {
		fmt.Fprintf(&sb, "| %s | %d / %d / %d | %.0f / %.0f / %.0f | %v | %v | %s (%.1f pts) | %s (%.1f pts) | %s (%+.1f pts) |\n",
			v.Option, v.Peak[b.Tight.Name], v.Peak[b.Scarce.Name], v.Peak[b.HalfPrefill.Name],
			v.AboveS[b.Tight.Name], v.AboveS[b.Scarce.Name], v.AboveS[b.HalfPrefill.Name],
			v.Cond1Tight, v.Cond1All, pct(v.Hit05), 100*v.Drop05, pct(v.Hit515), 100*v.Drop515, pct(v.ScarceHit), 100*v.ScarceVsRR)
	}
	for _, rule := range []struct {
		name string
		cond func(b.OptionVerdict) bool
	}{
		{"condition 1 in all three full-membership regimes", func(v b.OptionVerdict) bool { return v.Cond1All }},
		{"condition 1 in the tight regime only", func(v b.OptionVerdict) bool { return v.Cond1Tight }},
	} {
		q, bd, bs, w := b.Pass(vs, rule.cond)
		fmt.Fprintf(&sb, "\nQualifying on %s: %v.", rule.name, q)
		switch {
		case len(q) == 0:
			sb.WriteString(" None meets the engine limit, so none passes.")
		case w != "" && len(q) == 1:
			sb.WriteString(" It is the only qualifier, so it passes by default; the drain and scarce comparison does not discriminate.")
		case w != "":
			fmt.Fprintf(&sb, " Best after the drain and in the scarce regime: %s. It passes.", w)
		default:
			fmt.Fprintf(&sb, " Best after the drain: %s. Best in the scarce regime: %s. The rankings split.", bd, bs)
		}
	}
	sb.WriteString("\n")
	return sb.String()
}

// ExtraCSV is option 5's own measures, one row per run: what S10b's cell has
// no column for. SwitchT is the first switch of the bound and the cap
// together; spills and holds are split at it.
func ExtraCSV(rs []Result) string {
	var sb strings.Builder
	sb.WriteString("regime,seed,switches,first_switch_time_s,bound_before_pct,bound_after_pct,cap_before,cap_after,spills_before_switch,spills_from_switch,held_before_switch,held_from_switch\n")
	for _, r := range rs {
		x := r.Extra
		fmt.Fprintf(&sb, "%s,%d,%d,%.1f,%d,%d,%d,%d,%d,%d,%d,%d\n", r.Cell.Regime, r.Cell.Seed, x.Switches, x.SwitchT,
			x.BoundFrom, x.BoundTo, x.CapFrom, x.CapTo, x.SpillsBefore, x.SpillsAfter, x.HeldBefore, x.HeldAfter)
	}
	return sb.String()
}

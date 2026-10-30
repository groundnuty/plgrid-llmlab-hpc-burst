package study

import (
	"fmt"
	"strings"
)

// OptionVerdict collects what the pass rule of issue #45 needs for one
// option under one key.
type OptionVerdict struct {
	Option string
	// Condition 1, per full-membership regime: the largest in-flight count
	// on any Instance in any seed, and the time above the engine limit
	// summed over seeds. The option meets it when the peak is at most the
	// limit and the time is zero.
	Peak       map[string]int
	AboveS     map[string]float64
	Cond1Tight bool
	Cond1All   bool
	// Loss after the unreplaced drain: hit rate 0-5 and 5-15 min after it,
	// and the drop against the option's own run without the drain.
	Hit05, Hit515   float64
	Drop05, Drop515 float64
	DrainScore      float64 // mean of the two windows' hit rates
	// Scarce regime: hit rate, and against round robin's.
	ScarceHit, ScarceVsRR float64
}

// Evaluate computes every option's verdict inputs under key.
func Evaluate(cells []Cell, key string) []OptionVerdict {
	gs := Groups(cells)
	var out []OptionVerdict
	for _, o := range Options {
		v := OptionVerdict{Option: o.Name, Peak: map[string]int{}, AboveS: map[string]float64{}, Cond1Tight: true, Cond1All: true}
		for _, rg := range FullMembership {
			g, ok := Find(gs, rg.Name, key, o.Name)
			if !ok { // no data cannot show the option stays within the limit
				v.Cond1All = false
				if rg.Name == Tight.Name {
					v.Cond1Tight = false
				}
				continue
			}
			v.Peak[rg.Name] = int(g.max(func(c Cell) float64 { return float64(c.PeakRun) }))
			v.AboveS[rg.Name] = g.sum(func(c Cell) float64 { return c.AboveRunS })
			ok1 := v.Peak[rg.Name] <= EngineLimit && v.AboveS[rg.Name] == 0
			v.Cond1All = v.Cond1All && ok1
			if rg.Name == Tight.Name {
				v.Cond1Tight = ok1
			}
		}
		if d, ok := Find(gs, Drain.Name, key, o.Name); ok {
			base, _ := Find(gs, Tight.Name, key, o.Name)
			v.Hit05 = d.mean(func(c Cell) float64 { return c.Hit05 })
			v.Hit515 = d.mean(func(c Cell) float64 { return c.Hit515 })
			v.Drop05 = base.mean(func(c Cell) float64 { return c.Hit05 }) - v.Hit05
			v.Drop515 = base.mean(func(c Cell) float64 { return c.Hit515 }) - v.Hit515
			v.DrainScore = (v.Hit05 + v.Hit515) / 2
		}
		if s, ok := Find(gs, Scarce.Name, key, o.Name); ok {
			v.ScarceHit = s.mean(func(c Cell) float64 { return c.HitRate })
			if rr, ok := Baseline(gs, Scarce.Name); ok {
				v.ScarceVsRR = v.ScarceHit - rr.mean(func(c Cell) float64 { return c.HitRate })
			}
		}
		out = append(out, v)
	}
	return out
}

// Pass applies the rule: among the options that meet condition 1, the one
// with the highest drain score and the highest scarce hit rate. When the
// two rankings name different options, it returns both and no single
// winner. winner is "" when nobody qualifies or the rankings split.
func Pass(vs []OptionVerdict, cond func(OptionVerdict) bool) (qualifying []string, bestDrain, bestScarce, winner string) {
	var bd, bs *OptionVerdict
	for i := range vs {
		v := &vs[i]
		if !cond(*v) {
			continue
		}
		qualifying = append(qualifying, v.Option)
		if bd == nil || v.DrainScore > bd.DrainScore {
			bd = v
		}
		if bs == nil || v.ScarceHit > bs.ScarceHit {
			bs = v
		}
	}
	if bd != nil {
		bestDrain, bestScarce = bd.Option, bs.Option
		if bestDrain == bestScarce {
			winner = bestDrain
		}
	}
	return
}

// VerdictMarkdown renders the pass rule for one key.
func VerdictMarkdown(cells []Cell, key string) string {
	vs := Evaluate(cells, key)
	var b strings.Builder
	fmt.Fprintf(&b, "Key %s, seeds %d.\n\n", key, len(Groups(cells)[0].Cells))
	b.WriteString("| option | peak in flight: tight / scarce / half prefill (max over seeds) | time above 48, summed over seeds (s): tight / scarce / half prefill | condition 1, tight | condition 1, all three | hit 0-5 min after drain (drop vs no drain) | hit 5-15 min after drain (drop) | scarce hit (vs RR) |\n|---|---|---|---|---|---|---|---|\n")
	for _, v := range vs {
		fmt.Fprintf(&b, "| %s | %d / %d / %d | %.0f / %.0f / %.0f | %v | %v | %s (%.1f pts) | %s (%.1f pts) | %s (%+.1f pts) |\n",
			v.Option, v.Peak[Tight.Name], v.Peak[Scarce.Name], v.Peak[HalfPrefill.Name],
			v.AboveS[Tight.Name], v.AboveS[Scarce.Name], v.AboveS[HalfPrefill.Name],
			v.Cond1Tight, v.Cond1All, pct(v.Hit05), 100*v.Drop05, pct(v.Hit515), 100*v.Drop515, pct(v.ScarceHit), 100*v.ScarceVsRR)
	}
	for _, rule := range []struct {
		name string
		cond func(OptionVerdict) bool
	}{
		{"condition 1 in all three full-membership regimes", func(v OptionVerdict) bool { return v.Cond1All }},
		{"condition 1 in the tight regime only", func(v OptionVerdict) bool { return v.Cond1Tight }},
	} {
		q, bd, bs, w := Pass(vs, rule.cond)
		fmt.Fprintf(&b, "\nQualifying on %s: %v.", rule.name, q)
		switch {
		case len(q) == 0:
			b.WriteString(" None meets the engine limit, so none passes.")
		case w != "":
			fmt.Fprintf(&b, " Best after the drain and in the scarce regime: %s. It passes.", w)
		default:
			fmt.Fprintf(&b, " Best after the drain: %s. Best in the scarce regime: %s. The rankings split.", bd, bs)
		}
	}
	b.WriteString("\n")
	return b.String()
}

package lifecycle

// The monotone property of scaleDownTargets, over generated deployments (the science agent's ruling on #635; the Fable
// review's caveat that the table in TestScaleDownTargetsAreMonotoneWithASurplus is one hand-written shape). A lower
// desired count takes every target a higher one takes, and more, by Instance UID and by Allocation UID. The shapes
// come from a fixed seed, so a failure is the same every run and names its shape. The property is the oracle: no
// expected list is computed here, and the generator's own counters, which are not expectations, show that the shapes
// reach what the property is about (marks, live replacements, warm Allocations, k of 0 and 1, two Providers, hedges).

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// genShape is one generated deployment and what it holds, for the coverage counters.
type genShape struct {
	f                                                *selFx
	desired, k                                       int
	marks, replacements, warm, hedges, providersUsed int
	desc                                             string
}

// generate builds a deployment of 1 to 8 Instances: each ready, started, passed or pending, on Provider A or B, with
// a live replacement at one in four, a hedge member on the other Provider at one in ten, a ranked-out mark between
// two ready ones at one in three shapes, and 0 to 2 warm Allocations; k is 0 or 1.
func generate(rng *rand.Rand) genShape {
	g := genShape{f: newSelFx(), k: rng.Intn(2)}
	n := 1 + rng.Intn(8)
	var ready []string
	provs := map[string]bool{}
	var desc []string
	for i := range n {
		name := fmt.Sprintf("i%d", i)
		prov := "prov-a"
		if rng.Intn(10) < 3 {
			prov = "prov-b"
		}
		provs[prov] = true
		opts := []selOpt{onProv(prov), createdAt(hoT(8, rng.Intn(60), rng.Intn(60)))}
		phase := v1alpha1.InstancePending
		kind := "pending"
		switch r := rng.Intn(100); {
		case r < 35:
			phase, kind = v1alpha1.InstanceReady, "ready"
			opts = append(opts, hardEnd(hoT(11+rng.Intn(4), rng.Intn(60), 0)), jobStartedAt(hoT(9, rng.Intn(30), 0)), readyAt(hoT(9, 30+rng.Intn(20), 0)))
			ready = append(ready, name)
		case r < 55:
			phase, kind = v1alpha1.InstanceStarting, "started"
			opts = append(opts, jobStartedAt(hoT(9+rng.Intn(2), rng.Intn(60), 0)))
		case r < 70:
			phase, kind = v1alpha1.InstanceVerifying, "passed"
			opts = append(opts, passedChecksOpt(), jobStartedAt(hoT(9+rng.Intn(2), rng.Intn(60), 0)))
		}
		g.f.add(name, phase, opts...)
		desc = append(desc, name+":"+kind+"@"+prov)
		if rng.Intn(4) == 0 {
			g.f.add("r"+name, v1alpha1.InstancePending, replacementOf(name), onProv(prov))
			g.replacements++
			desc = append(desc, "r"+name+" replaces "+name)
		}
		if rng.Intn(10) == 0 {
			other := "prov-b"
			if prov == "prov-b" {
				other = "prov-a"
			}
			provs[other] = true
			hopts := []selOpt{onProv(other), inHedge(name), createdAt(hoT(8, rng.Intn(60), rng.Intn(60)))}
			hphase := v1alpha1.InstancePending
			if rng.Intn(2) == 0 {
				hphase = v1alpha1.InstanceStarting
				hopts = append(hopts, jobStartedAt(hoT(9, rng.Intn(60), 0)))
			}
			g.f.add("h"+name, hphase, hopts...)
			g.hedges++
			desc = append(desc, "h"+name+" in the hedge of "+name)
		}
	}
	// A mark: one ready Instance without a live replacement made way for another ready one, healthy from 0 to 19 s
	// before the pass, so some drains are due and some are not.
	if len(ready) >= 2 && rng.Intn(3) == 0 {
		peer, cand := ready[0], ready[1+rng.Intn(len(ready)-1)]
		for _, in := range g.f.insts {
			if in.Name == peer {
				in.Annotations = map[string]string{annotationRankedOut: cand}
			}
			if in.Name == cand {
				healthyAt(hoT(10, 0, 0).Add(-time.Duration(rng.Intn(20))*time.Second))(in, nil)
			}
		}
		g.marks++
		desc = append(desc, peer+" marked for "+cand)
	}
	for j := range rng.Intn(3) {
		prov := "prov-a"
		if rng.Intn(2) == 0 {
			prov = "prov-b"
		}
		g.f.warmAlloc(fmt.Sprintf("w%d", j), prov)
		g.warm++
	}
	g.desired = 1 + rng.Intn(n)
	g.providersUsed = len(provs)
	g.desc = fmt.Sprintf("instances %d, k %d: %s", g.desired, g.k, strings.Join(desc, ", "))
	return g
}

// targetKeys are the targets of scaleDownTargets by Instance UID and Allocation UID, sorted.
func targetKeys(ts []scaleTarget) []string {
	var out []string
	for _, t := range ts {
		out = append(out, "instance "+string(t.instance.UID), "allocation "+string(t.allocation.UID))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func TestScaleDownTargetsAreMonotoneOverGeneratedDeployments(t *testing.T) {
	rng := rand.New(rand.NewSource(616))
	const shapes = 600
	var marks, replacements, warm, hedges, twoProviders, k1, withTargets, left int
	for i := range shapes {
		g := generate(rng)
		p := g.f.pass(int32(g.desired), int32(g.k))
		prim := p.primaries()
		var prev []string
		for s := 0; s <= len(prim); s++ {
			got := targetKeys(p.scaleDownTargets(prim, s))
			if again := targetKeys(p.scaleDownTargets(prim, s)); !slices.Equal(got, again) {
				t.Fatalf("shape %d, lowered by %d: two calls differ: %v and %v\n%s", i, s, got, again, g.desc)
			}
			for _, k := range prev {
				if !slices.Contains(got, k) {
					t.Fatalf("shape %d: lowering by %d drops %s, which lowering by %d takes: not monotone\n%s\nby %d: %v\nby %d: %v",
						i, s, k, s-1, g.desc, s-1, prev, s, got)
				}
			}
			prev = got
		}
		// The counters: what the shapes reach, not what they must give.
		marks += min(g.marks, 1)
		replacements += min(g.replacements, 1)
		warm += min(g.warm, 1)
		hedges += min(g.hedges, 1)
		if g.providersUsed > 1 {
			twoProviders++
		}
		if g.k == 1 {
			k1++
		}
		if len(targetKeys(p.scaleDownTargets(prim, 0))) > 0 {
			withTargets++
		}
		if _, surplus := selectPrimaries(p.selection(nil), g.desired); len(surplus) > len(p.scaleUnits(g.desired)) {
			left++ // some surplus Instance is left alone: k at work
		}
	}
	for _, c := range []struct {
		what string
		n    int
		min  int
	}{
		{"shapes with a mark", marks, 60}, {"shapes with a live replacement", replacements, 200}, {"shapes with a warm Allocation", warm, 200},
		{"shapes with a hedge", hedges, 40}, {"shapes on two Providers", twoProviders, 150}, {"shapes with k 1", k1, 200},
		{"shapes with a target at the current count", withTargets, 200}, {"shapes that leave a surplus Instance alone", left, 100},
	} {
		if c.n < c.min {
			t.Errorf("%s: %d of %d, want at least %d: the generator no longer reaches what the property is about", c.what, c.n, shapes, c.min)
		}
	}
}

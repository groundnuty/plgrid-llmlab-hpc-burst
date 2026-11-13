package run

// The retry layer: at budget 0 it equals arm (a) request for request, so it changes nothing but the
// retries; and each arm acts on each fault as its policy says.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

// outcome is what a request came to, as the client and the engines saw it.
type outcome struct {
	Status   int
	Code     string
	Detail   string
	Complete bool
	Begun    int
	Deaths   int
}

// script sends the same scripted requests on a stand: an unmarked one, then the fault's marked one aimed
// at Instance 1, streamed and not; it returns each request's outcome, by its place in the script.
func script(t *testing.T, fault string, p *rig.Policy) []outcome {
	t.Helper()
	r := stand(t, fault, p)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, s, _ := pinned(t, ctx, r, "eq"+fault, 1)
	marker := map[string]string{"": "", "F1": rig.MarkF1 + "1", "F2": rig.MarkF2 + "1", "F3": rig.MarkF3, "F4": rig.MarkPoison}[fault]
	var results []rig.Result
	var dead []int
	for _, stream := range []bool{false, true} {
		for _, m := range []string{"", marker} {
			before := len(deaths(r))
			results = append(results, c.Do(ctx, c.NextBase(), 0, s, stream, m))
			eventually(t, 10*time.Second, "the engines' records settled", func() bool {
				begins, _ := r.Begins()
				for _, b := range begins[results[len(results)-1].ID] {
					if b.Admitted && b.Outcome == "" && r.Doubles.Alive(b.Instance) {
						return false
					}
				}
				return true
			})
			dead = append(dead, len(deaths(r))-before)
			if err := r.AllReady(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	begins, err := r.Begins()
	if err != nil {
		t.Fatal(err)
	}
	var out []outcome
	for k, res := range results {
		out = append(out, outcome{res.Status, res.Code, res.Detail, res.Complete, rig.Admissions(begins[res.ID]), dead[k]})
	}
	return out
}

func TestTheLayerAtBudget0EqualsAlphaRequestForRequest(t *testing.T) {
	for _, fault := range []string{"", "F1", "F2", "F3", "F4"} {
		t.Run("fault="+fault, func(t *testing.T) {
			p := rig.Budget0
			a, l := script(t, fault, nil), script(t, fault, &p)
			if len(a) != len(l) {
				t.Fatalf("%d requests under (a), %d under the layer", len(a), len(l))
			}
			for k := range a {
				if a[k] != l[k] {
					t.Errorf("request %d: (a) %+v, the layer at budget 0 %+v; want the same", k, a[k], l[k])
				}
			}
		})
	}
}

// Each arm on each fault, for the marked request aimed at Instance 1 (the script's 2nd and 4th): what
// HBEP-5's question expects of it. (b): one retry to another Instance; (c) with a full bucket: retries to
// every other Instance; (d): (c) and the two-deaths stop.
func TestEachArmOnEachFault(t *testing.T) {
	b := rig.Policy{Name: "b", MaxRetries: 1}
	c := func() *rig.Policy { return &rig.Policy{Name: "c", MaxRetries: 2, Bucket: rig.NewBucket(10, 1)} }
	d := func() *rig.Policy {
		return &rig.Policy{Name: "d", MaxRetries: 2, Bucket: rig.NewBucket(10, 1), StopAfterDeaths: 2}
	}
	whole := outcome{Status: 200, Complete: true, Begun: 2}
	tunnel := func(begun, deaths int) outcome {
		return outcome{Status: 502, Code: "upstream_closed", Detail: "tunnel", Begun: begun, Deaths: deaths}
	}
	for _, tc := range []struct {
		arm   string
		p     func() *rig.Policy
		fault string
		want  outcome
	}{
		// F1, F2, F3 are one failure: a retry to another Instance answers it, begun twice (a duplicate).
		{"b", func() *rig.Policy { return &b }, "F1", outcome{200, "", "", true, 2, 1}},
		{"b", func() *rig.Policy { return &b }, "F2", whole},
		{"b", func() *rig.Policy { return &b }, "F3", whole},
		// F4 kills every engine it reaches: (b) two, (c) all three, (d) stops at two.
		{"b", func() *rig.Policy { return &b }, "F4", tunnel(2, 2)},
		{"c", c, "F4", tunnel(3, 3)},
		{"d", d, "F4", tunnel(2, 2)},
		{"d", d, "F1", outcome{200, "", "", true, 2, 1}},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.arm, tc.fault), func(t *testing.T) {
			got := script(t, tc.fault, tc.p())
			for _, k := range []int{1, 3} {
				if got[k] != tc.want {
					t.Errorf("the marked request (stream %v): %+v, want %+v", k == 3, got[k], tc.want)
				}
			}
			for _, k := range []int{0, 2} {
				if got[k] != (outcome{Status: 200, Complete: true, Begun: 1}) {
					t.Errorf("the unmarked request (stream %v): %+v, want a whole 200 begun once", k == 2, got[k])
				}
			}
		})
	}
}

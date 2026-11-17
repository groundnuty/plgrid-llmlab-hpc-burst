package lifecycle

// A hedge's started loser kept as a candidate (HBEP-7, Over-allocation and qualification (beta), Hedging's started
// losers; How it counts; #616, PR 5), on hand-built objects: the selection reads the candidate mark, reservedOn
// agrees with it, and the start-up outcome of a candidate's end. Every expected set is written out by hand.

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestTheSelectionAndReservedOnReadAKeptLoser: one hand-written table through the selection and reservedOn, with
// Providers A and B. A kept loser "l" (job started, verifying, annotation candidate-of naming its winner "w") is its
// Provider's surplus while "w" is countable, whatever the count, and holds no reserved slot; once "w" has ended it is
// chosen like any Instance and holds one; in a replacement hedge it is surplus though its predecessor "p" still counts,
// and a replacement winner that a ready candidate outranked is surplus too, so step 7 takes its drain. The chosen set,
// the surplus and the reserved slots on A are listed by hand.
func TestTheSelectionAndReservedOnReadAKeptLoser(t *testing.T) {
	profile := &v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}
	cand := map[string]string{annotationCandidateOf: "w"}
	replacement := func(extra map[string]string) map[string]string {
		m := map[string]string{v1alpha1.AnnotationPredecessor: "p"}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	for _, c := range []struct {
		name    string
		desired int32
		insts   []v1alpha1.Instance
		chosen  []string
		surplus []string
		reserve []string // on A, "replacement:<Instance>"
	}{
		{"a kept loser beside a ready primary and its winner is surplus, though the count would choose it", 2,
			[]v1alpha1.Instance{
				crossInstance("p", provA, v1alpha1.InstanceReady, nil),
				crossInstance("w", provB, v1alpha1.InstanceReady, nil),
				crossInstance("l", provA, v1alpha1.InstanceVerifying, cand)},
			[]string{"p", "w"}, []string{"l"}, []string{"replacement:p"}},
		{"its winner has ended: it is chosen like any Instance and holds a slot", 2,
			[]v1alpha1.Instance{
				crossInstance("p", provA, v1alpha1.InstanceReady, nil),
				crossInstance("w", provB, v1alpha1.InstanceEnded, nil),
				crossInstance("l", provA, v1alpha1.InstanceVerifying, cand)},
			[]string{"l", "p"}, nil, []string{"replacement:l", "replacement:p"}},
		{"in a replacement hedge it is surplus though its predecessor still counts; the winner is no primary", 1,
			[]v1alpha1.Instance{
				crossInstance("p", provA, v1alpha1.InstanceReady, nil),
				crossInstance("w", provB, v1alpha1.InstanceReady, replacement(nil)),
				crossInstance("l", provA, v1alpha1.InstanceVerifying, replacement(cand))},
			[]string{"p"}, []string{"l"}, nil},
		{"a replacement winner that a ready candidate outranked is surplus, for step 7 to drain", 1,
			[]v1alpha1.Instance{
				crossInstance("p", provA, v1alpha1.InstanceReady, nil),
				crossInstance("w", provB, v1alpha1.InstanceReady, replacement(map[string]string{annotationRankedOut: "l"})),
				crossInstance("l", provA, v1alpha1.InstanceReady, replacement(cand))},
			[]string{"p"}, []string{"w"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
			md.Spec.Instances = c.desired
			var allocs []v1alpha1.Allocation
			for _, in := range c.insts {
				if !ended(&in) {
					allocs = append(allocs, parityAlloc(in.Name, true))
				}
			}
			byName := map[string]*v1alpha1.Allocation{}
			for i := range allocs {
				byName[allocs[i].Name] = &allocs[i]
			}
			ptrs := make([]*v1alpha1.Instance, len(c.insts))
			for i := range c.insts {
				ptrs[i] = &c.insts[i]
			}
			chosen, surplus := selectPrimaries(selection{insts: ptrs, allocOf: func(in *v1alpha1.Instance) *v1alpha1.Allocation { return byName[in.Spec.AllocationRef.Name] }}, int(c.desired))
			names := func(ins []*v1alpha1.Instance) []string {
				var out []string
				for _, in := range ins {
					out = append(out, in.Name)
				}
				slices.Sort(out)
				return out
			}
			if got := names(chosen); !slices.Equal(got, c.chosen) {
				t.Errorf("chosen %v, want %v", got, c.chosen)
			}
			if got := names(surplus); !slices.Equal(got, c.surplus) {
				t.Errorf("surplus %v, want %v", got, c.surplus)
			}
			slotAllocs := map[string]*v1alpha1.Allocation{}
			for i := range allocs {
				slotAllocs[rigNS+"/"+allocs[i].Name] = &allocs[i]
			}
			slots, err := reservedOn(context.Background(), parityReader{insts: c.insts, allocs: allocs, md: md}, provA, profile, slotAllocs)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, s := range slots {
				kind := map[slotKind]string{slotForReplacement: "replacement", slotForSuccessor: "successor", slotForOwnAllocation: "own"}[s.kind]
				got = append(got, kind+":"+strings.TrimPrefix(s.key, rigNS+"/alloc-"))
			}
			if !slices.Equal(got, c.reserve) {
				t.Errorf("reserved slots on A %v, want %v", got, c.reserve)
			}
		})
	}
}

// TestAKeptLosersEndIsAnOutcomeOnlyWithItsMark: a hedge's started loser that ended with HedgeLost adds a success when
// it carries the candidate mark (it passed every check before its ranking point), and none when it does not (a loser
// ended at the win, HBEP-7's Start-up outcomes row); the mark changes no other end: a failure stays one, a drain adds
// none, a ranked-out one is a success as before.
func TestAKeptLosersEndIsAnOutcomeOnlyWithItsMark(t *testing.T) {
	e := hoT(8, 5, 0)
	mark := map[string]string{annotationCandidateOf: "w"}
	for _, c := range []struct {
		name   string
		reason string
		ann    map[string]string
		want   string // "" for none
	}{
		{"HedgeLost with its mark", v1alpha1.ReasonHedgeLost, mark, v1alpha1.StartOutcomeReady},
		{"HedgeLost with no mark: ended at the win", v1alpha1.ReasonHedgeLost, nil, ""},
		{"RankedOut with its mark", v1alpha1.ReasonRankedOut, mark, v1alpha1.StartOutcomeReady},
		{"QualificationFailed with its mark", v1alpha1.ReasonQualificationFailed, mark, v1alpha1.StartOutcomeFailed},
		{"Drained with its mark: scaled down", v1alpha1.ReasonDrained, mark, ""},
	} {
		in := outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, c.reason, "")
		in.Annotations = c.ann
		o, ok := startOutcomeOf(in, started("a", "u1"))
		got := ""
		if ok {
			got = o.Result
		}
		if got != c.want || (ok && !o.Time.Time.Equal(e)) {
			t.Errorf("%s: %v %+v, want %q at 08:05:00", c.name, ok, o, c.want)
		}
	}
}

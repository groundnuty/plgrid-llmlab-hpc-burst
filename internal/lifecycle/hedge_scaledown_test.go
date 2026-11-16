package lifecycle

// scaleDownTargets is monotone in the count it drops (#604; the seam #589's Scale plan reads, h589, 16 Nov 2026): a
// larger surplus, a lower desired count, takes every target a smaller one takes, and more. The planner reads a
// target that both its calls return as a trim, so the property is what it relies on. The fixture's order is written
// out by hand from step 7's rule: pending first, newest first; then started, newest first by a hedge's first member;
// then ready, the nearest hard end first; a primary's replacement hedge first, its members before it.

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestScaleDownTargetsAreMonotone(t *testing.T) {
	t0 := hoT(9, 0, 0)
	var insts []*v1alpha1.Instance
	allocs := map[string]*v1alpha1.Allocation{}
	add := func(name, group, pred string, created time.Duration, phase v1alpha1.InstancePhase, started bool, leaseEnd *time.Time) {
		in := member(name, group, t0.Add(created), phase, nil)
		if pred != "" {
			if in.Annotations == nil {
				in.Annotations = map[string]string{}
			}
			in.Annotations[v1alpha1.AnnotationPredecessor] = pred
		}
		if leaseEnd != nil {
			le := metav1.NewTime(*leaseEnd)
			in.Status.LeaseEnd = &le
		}
		a := &v1alpha1.Allocation{}
		if started {
			s := metav1.NewTime(t0)
			a.Status.AtProvider.StartTime = &s
		}
		insts = append(insts, in)
		allocs[in.Spec.AllocationRef.Name] = a
	}
	noon, eleven := hoT(12, 0, 0), hoT(11, 0, 0)
	add("r1", "", "", 0, v1alpha1.InstanceReady, true, &noon)             // ready, hard end 12:00
	add("r2", "", "", time.Second, v1alpha1.InstanceReady, true, &eleven) // ready, hard end 11:00
	add("r1-repl", "", "r1", 2*time.Second, v1alpha1.InstancePending, false, nil)
	add("h", "", "", time.Minute, v1alpha1.InstancePending, false, nil) // a hedge, its first member created 09:01
	add("h-b", "h", "", 5*time.Minute, v1alpha1.InstanceStarting, true, nil)
	add("s", "", "", 2*time.Minute, v1alpha1.InstanceStarting, true, nil) // started, created 09:02
	add("p1", "", "", 3*time.Minute, v1alpha1.InstancePending, false, nil)
	add("p2", "", "", 4*time.Minute, v1alpha1.InstancePending, false, nil)
	p := &pass{insts: insts, allocs: allocs}
	prim := p.primaries()
	if len(prim) != 6 {
		t.Fatalf("%d primaries, want six units: r1, r2, the hedge h, s, p1 and p2", len(prim))
	}
	// Each surplus's targets, by name, written out: units p2, p1, s, h, r2, r1 in that order.
	want := [][]string{
		nil,
		{"p2"},
		{"p2", "p1"},
		{"p2", "p1", "s"},
		{"p2", "p1", "s", "h", "h-b"},
		{"p2", "p1", "s", "h", "h-b", "r2"},
		{"p2", "p1", "s", "h", "h-b", "r2", "r1-repl", "r1"},
	}
	var prev []string
	for surplus := 0; surplus <= len(prim); surplus++ {
		var got []string
		for _, tg := range p.scaleDownTargets(prim, surplus) {
			got = append(got, tg.instance.Name)
		}
		if !slices.Equal(got, want[surplus]) {
			t.Fatalf("surplus %d: targets %v, want %v", surplus, got, want[surplus])
		}
		for _, n := range prev {
			if !slices.Contains(got, n) {
				t.Fatalf("surplus %d drops %s, which surplus %d takes: not monotone", surplus, n, surplus-1)
			}
		}
		again := p.scaleDownTargets(prim, surplus)
		if len(again) != len(got) {
			t.Fatalf("surplus %d: a second call returned %d targets, want %d", surplus, len(again), len(got))
		}
		prev = got
	}
}

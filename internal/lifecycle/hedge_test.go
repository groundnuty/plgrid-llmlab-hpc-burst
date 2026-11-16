package lifecycle

// The hedge's pure rules (HBEP-7, Hedging (beta); #604 PR 1): how members are grouped, which one wins, which one
// stands for the hedge, and which ends are failures. Every expected value is written out here, by name.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// member is an Instance named name, created at created, in phase phase, ready at ready (nil for none), in the hedge
// whose first member is group ("" for a first member or a hedge of one).
func member(name, group string, created time.Time, phase v1alpha1.InstancePhase, ready *time.Time) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS, CreationTimestamp: metav1.NewTime(created)}}
	if group != "" {
		in.Annotations = map[string]string{v1alpha1.AnnotationHedgeGroup: group}
	}
	in.Spec.AllocationRef.Name = "alloc-" + name
	in.Status.Phase = phase
	if ready != nil {
		t := metav1.NewTime(*ready)
		in.Status.ReadyTime = &t
	}
	return in
}

func names(ins []*v1alpha1.Instance) []string {
	var out []string
	for _, in := range ins {
		out = append(out, in.Name)
	}
	return out
}

func TestGroupHedgesKeepsTheFirstSeenOrder(t *testing.T) {
	t0 := hoT(9, 0, 0)
	insts := []*v1alpha1.Instance{
		member("solo", "", t0, v1alpha1.InstanceReady, nil),
		member("h1", "", t0, v1alpha1.InstancePending, nil),
		member("other", "", t0, v1alpha1.InstancePending, nil),
		member("h1-b", "h1", t0, v1alpha1.InstancePending, nil),
		member("gone", "", t0, v1alpha1.InstanceEnded, nil),
		member("h1-c", "h1", t0, v1alpha1.InstancePending, nil),
	}
	gs := groupHedges(insts, func(in *v1alpha1.Instance) bool { return in.Status.Phase != v1alpha1.InstanceEnded })
	want := [][]string{{"solo"}, {"h1", "h1-b", "h1-c"}, {"other"}}
	if len(gs) != len(want) {
		t.Fatalf("%d groups, want %d: %+v", len(gs), len(want), gs)
	}
	for i, g := range gs {
		if got := names(g.members); len(got) != len(want[i]) || g.key != want[i][0] {
			t.Fatalf("group %d is %s %v, want %v", i, g.key, got, want[i])
		}
		for j := range want[i] {
			if g.members[j].Name != want[i][j] {
				t.Fatalf("group %d is %v, want %v", i, names(g.members), want[i])
			}
		}
	}
	// A hedge whose first member has ended is still found by its later members' annotation.
	gs = groupHedges([]*v1alpha1.Instance{member("x-b", "x", t0, v1alpha1.InstancePending, nil), member("x-c", "x", t0, v1alpha1.InstancePending, nil)},
		func(*v1alpha1.Instance) bool { return true })
	if len(gs) != 1 || gs[0].key != "x" || len(gs[0].members) != 2 {
		t.Fatalf("the hedge of an ended first member: %+v, want one hedge x of two", gs)
	}
}

func TestHedgeWinnerIsTheFirstReady(t *testing.T) {
	t0 := hoT(9, 0, 0)
	r1, r2 := hoT(9, 30, 0), hoT(9, 31, 0)
	for _, c := range []struct {
		name    string
		members []*v1alpha1.Instance
		want    string
	}{
		{"none ready", []*v1alpha1.Instance{member("a", "", t0, v1alpha1.InstancePending, nil), member("b", "a", t0, v1alpha1.InstanceStarting, nil)}, ""},
		{"the earlier ready time wins, whatever the creation order", []*v1alpha1.Instance{
			member("a", "", t0, v1alpha1.InstanceReady, &r2), member("b", "a", t0.Add(time.Minute), v1alpha1.InstanceReady, &r1)}, "b"},
		{"the same ready time: the older member", []*v1alpha1.Instance{
			member("b", "a", t0.Add(time.Minute), v1alpha1.InstanceReady, &r1), member("a", "", t0, v1alpha1.InstanceReady, &r1)}, "a"},
		{"the same ready and creation times: by name", []*v1alpha1.Instance{
			member("m-2", "m-1", t0, v1alpha1.InstanceReady, &r1), member("m-1", "", t0, v1alpha1.InstanceReady, &r1)}, "m-1"},
		{"a ready time without the ready phase does not win", []*v1alpha1.Instance{
			member("a", "", t0, v1alpha1.InstanceDraining, &r1), member("b", "a", t0, v1alpha1.InstanceReady, &r2)}, "b"},
	} {
		w := hedgeWinner(c.members)
		got := ""
		if w != nil {
			got = w.Name
		}
		if got != c.want {
			t.Errorf("%s: winner %q, want %q", c.name, got, c.want)
		}
	}
}

func TestHedgeRepresentativeIsTheMostAdvanced(t *testing.T) {
	t0 := hoT(9, 0, 0)
	r1 := hoT(9, 30, 0)
	started := map[string]bool{}
	allocOf := func(in *v1alpha1.Instance) *v1alpha1.Allocation {
		a := &v1alpha1.Allocation{}
		if started[in.Name] {
			s := metav1.NewTime(t0)
			a.Status.AtProvider.StartTime = &s
		}
		return a
	}
	older, newer := member("old", "", t0, v1alpha1.InstancePending, nil), member("new", "old", t0.Add(time.Minute), v1alpha1.InstancePending, nil)
	if r := hedgeRepresentative([]*v1alpha1.Instance{newer, older}, allocOf); r.Name != "old" {
		t.Errorf("both pending: %s, want the older, old", r.Name)
	}
	started["new"] = true
	if r := hedgeRepresentative([]*v1alpha1.Instance{older, newer}, allocOf); r.Name != "new" {
		t.Errorf("the newer's job started: %s, want new, so the hedge reads as started", r.Name)
	}
	ready := member("ready", "old", t0.Add(2*time.Minute), v1alpha1.InstanceReady, &r1)
	if r := hedgeRepresentative([]*v1alpha1.Instance{older, newer, ready}, allocOf); r.Name != "ready" {
		t.Errorf("one ready: %s, want ready", r.Name)
	}
}

func TestAHedgeLossIsACleanEnd(t *testing.T) {
	end := time.Now()
	for _, c := range []struct {
		reason string
		failed bool
	}{
		{v1alpha1.ReasonDrained, false},
		{v1alpha1.ReasonHedgeLost, false},
		{v1alpha1.ReasonAllocationFailed, true},
		{v1alpha1.ReasonAllocationEndedEarly, true},
	} {
		in := member("x", "", end, v1alpha1.InstanceEnded, nil)
		in.Status.EndReason = c.reason
		e := metav1.NewTime(end)
		in.Status.EndTime = &e
		if got := releaseRequired(in); got != c.failed {
			t.Errorf("%s: releaseRequired %v, want %v", c.reason, got, c.failed)
		}
		if got := failedEnd(in); got != c.failed {
			t.Errorf("%s: failedEnd %v, want %v", c.reason, got, c.failed)
		}
	}
	lost := member("x", "", end, v1alpha1.InstanceDraining, nil)
	lost.Annotations = map[string]string{annotationHedgeLost: "winner"}
	if got := drainEndReason(lost); got != v1alpha1.ReasonHedgeLost {
		t.Errorf("a hedge loser's drain ends %s, want HedgeLost", got)
	}
	if got := drainEndReason(member("y", "", end, v1alpha1.InstanceDraining, nil)); got != v1alpha1.ReasonDrained {
		t.Errorf("a drain ends %s, want Drained", got)
	}
}

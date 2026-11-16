package lifecycle

// The selection of primaries (HBEP-7, Over-allocation and qualification (beta), How it counts; #616, PR 3). Every
// expected order is written out by hand in the row, by Instance name; none is computed by selectPrimaries,
// primaries or scaleDownTargets.

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// selFx is a population of Instances and their Allocations, built row by row.
type selFx struct {
	insts  []*v1alpha1.Instance
	allocs map[string]*v1alpha1.Allocation
}

type selOpt func(*v1alpha1.Instance, *v1alpha1.Allocation)

func newSelFx() *selFx { return &selFx{allocs: map[string]*v1alpha1.Allocation{}} }

// add makes an Instance on Provider "prov-a", created at 09:00 unless an option says otherwise, with an Allocation
// whose job has not started.
func (f *selFx) add(name string, phase v1alpha1.InstancePhase, opts ...selOpt) *v1alpha1.Instance {
	in := member(name, "", hoT(9, 0, 0), phase, nil)
	in.UID = types.UID("uid-" + name)
	in.Labels = map[string]string{v1alpha1.LabelProvider: "prov-a"}
	a := &v1alpha1.Allocation{}
	a.Name = in.Spec.AllocationRef.Name
	a.Labels = map[string]string{v1alpha1.LabelProvider: "prov-a"}
	for _, o := range opts {
		o(in, a)
	}
	f.insts = append(f.insts, in)
	f.allocs[a.Name] = a
	return in
}

func (f *selFx) allocOf(in *v1alpha1.Instance) *v1alpha1.Allocation {
	return f.allocs[in.Spec.AllocationRef.Name]
}

func (f *selFx) pass(instances int32, overAllocation int32) *pass {
	md := &v1alpha1.ModelDeployment{Spec: v1alpha1.ModelDeploymentSpec{Instances: instances, OverAllocation: &overAllocation}}
	return &pass{md: md, insts: f.insts, allocs: f.allocs, now: hoT(10, 0, 0), stale: map[types.UID]bool{}}
}

func createdAt(t time.Time) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) { in.CreationTimestamp = metav1.NewTime(t) }
}

func jobStartedAt(t time.Time) selOpt {
	return func(_ *v1alpha1.Instance, a *v1alpha1.Allocation) {
		s := metav1.NewTime(t)
		a.Status.AtProvider.StartTime = &s
		a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	}
}

func hardEnd(t time.Time) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) { e := metav1.NewTime(t); in.Status.LeaseEnd = &e }
}

func readyAt(t time.Time) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) { r := metav1.NewTime(t); in.Status.ReadyTime = &r }
}

// passedChecksOpt makes the three conditions True.
func passedChecksOpt() selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
		for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
			in.Status.Conditions = append(in.Status.Conditions, metav1.Condition{Type: c, Status: metav1.ConditionTrue})
		}
	}
}

func replacementOf(pred string) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[v1alpha1.AnnotationPredecessor] = pred
	}
}

func onProv(name string) selOpt {
	return func(in *v1alpha1.Instance, a *v1alpha1.Allocation) {
		in.Labels[v1alpha1.LabelProvider], a.Labels[v1alpha1.LabelProvider] = name, name
	}
}

func rankedOutBy(candidate string) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[annotationRankedOut] = candidate
	}
}

func inHedge(first string) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[v1alpha1.AnnotationHedgeGroup] = first
	}
}

func withUID(u string) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) { in.UID = types.UID(u) }
}

func tpotIs(d time.Duration) selOpt {
	return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
		in.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(d.String())}
	}
}

func (f *selFx) selection() selection {
	return selection{insts: f.insts, allocOf: f.allocOf}
}

func namesOf(ins []*v1alpha1.Instance) []string {
	out := []string{}
	for _, in := range ins {
		out = append(out, in.Name)
	}
	return out
}

func sameNames(t *testing.T, what string, got []*v1alpha1.Instance, want ...string) {
	t.Helper()
	if g := namesOf(got); !slices.Equal(g, want) {
		t.Errorf("%s: %v, want %v", what, g, want)
	}
}

// TestSelectionOrder: HBEP-7's order, by tier, one row per boundary the order draws. The population:
//
//	rr   ready, hard end 12:00, with a live replacement (rr-repl, pending)   tier 0
//	rd   ready, hard end 13:00                                                tier 1
//	nr   starting, job started 00:10 (10:10 on the fixture's clock), with a live replacement   tier 2
//	pa   verifying, passed its checks, job started 00:20                      tier 3
//	st   starting, job started 00:30                                          tier 4
//	pe   pending                                                              tier 5
//
// so the order is rr, rd, nr, pa, st, pe. A replacement whose predecessor counts (rr-repl, nr-repl) is in neither
// list. "A live replacement before ready" (the order before the science agent's ruling on #616's plan, Q3) would put
// nr above rd, and desired 2 would choose rr and nr.
func TestSelectionOrder(t *testing.T) {
	f := newSelFx()
	f.add("pe", v1alpha1.InstancePending)
	f.add("st", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 30, 0)))
	f.add("pa", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 20, 0)), passedChecksOpt())
	f.add("nr", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 10, 0)))
	f.add("nr-repl", v1alpha1.InstancePending, replacementOf("nr"))
	f.add("rd", v1alpha1.InstanceReady, hardEnd(hoT(13, 0, 0)), jobStartedAt(hoT(9, 0, 0)))
	f.add("rr", v1alpha1.InstanceReady, hardEnd(hoT(12, 0, 0)), jobStartedAt(hoT(9, 0, 0)))
	f.add("rr-repl", v1alpha1.InstancePending, replacementOf("rr"))
	for _, c := range []struct {
		desired        int
		chosen, surplu []string
	}{
		{0, nil, []string{"rr", "rd", "nr", "pa", "st", "pe"}},
		{1, []string{"rr"}, []string{"rd", "nr", "pa", "st", "pe"}},
		{2, []string{"rr", "rd"}, []string{"nr", "pa", "st", "pe"}},
		{3, []string{"rr", "rd", "nr"}, []string{"pa", "st", "pe"}},
		{4, []string{"rr", "rd", "nr", "pa"}, []string{"st", "pe"}},
		{5, []string{"rr", "rd", "nr", "pa", "st"}, []string{"pe"}},
		{6, []string{"rr", "rd", "nr", "pa", "st", "pe"}, nil},
		{9, []string{"rr", "rd", "nr", "pa", "st", "pe"}, nil},
	} {
		chosen, surplus := selectPrimaries(f.selection(), c.desired)
		sameNames(t, "desired chosen", chosen, c.chosen...)
		sameNames(t, "desired surplus", surplus, c.surplu...)
	}
}

// TestSelectionAmongEquals: the ties HBEP-7 breaks. Ready ones: the later hard end first, so a scale-down drains the
// nearest. The others: the earlier job start, then the older creation, then the UID. Each row's order is by hand.
func TestSelectionAmongEquals(t *testing.T) {
	t.Run("ready ones by the later hard end", func(t *testing.T) {
		f := newSelFx()
		f.add("r-1100", v1alpha1.InstanceReady, hardEnd(hoT(11, 0, 0)))
		f.add("r-1300", v1alpha1.InstanceReady, hardEnd(hoT(13, 0, 0)))
		f.add("r-1200", v1alpha1.InstanceReady, hardEnd(hoT(12, 0, 0)))
		chosen, surplus := selectPrimaries(f.selection(), 1)
		sameNames(t, "chosen", chosen, "r-1300")
		sameNames(t, "surplus", surplus, "r-1200", "r-1100")
	})
	t.Run("started ones by the earlier job start, then the older creation, then the UID", func(t *testing.T) {
		f := newSelFx()
		// All four started. s-late started at 10:30; the other three at 10:10. Among those, creation: s-new 09:30,
		// s-old and s-uid-b 09:00. s-uid-b has the UID "b", s-old "a".
		f.add("s-late", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 30, 0)), createdAt(hoT(8, 0, 0)))
		f.add("s-new", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 10, 0)), createdAt(hoT(9, 30, 0)))
		f.add("s-uid-b", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 10, 0)), createdAt(hoT(9, 0, 0)), withUID("b"))
		f.add("s-old", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 10, 0)), createdAt(hoT(9, 0, 0)), withUID("a"))
		_, surplus := selectPrimaries(f.selection(), 0)
		sameNames(t, "order", surplus, "s-old", "s-uid-b", "s-new", "s-late")
	})
	t.Run("pending ones by the older creation", func(t *testing.T) {
		f := newSelFx()
		f.add("p-2", v1alpha1.InstancePending, createdAt(hoT(9, 2, 0)))
		f.add("p-1", v1alpha1.InstancePending, createdAt(hoT(9, 1, 0)))
		chosen, surplus := selectPrimaries(f.selection(), 1)
		sameNames(t, "chosen", chosen, "p-1")
		sameNames(t, "surplus", surplus, "p-2")
	})
}

// TestSelectionReadsTheMark: a peer that carries the ranked-out mark is surplus once the Instance the mark names is
// ready, whatever its rank. P (hard end 12:15) would be chosen over X (12:10) by the later hard end; marked for X,
// which is ready, it is surplus and X is chosen. While X is not ready the mark changes nothing, and P stays chosen.
func TestSelectionReadsTheMark(t *testing.T) {
	for _, c := range []struct {
		name          string
		xPhase        v1alpha1.InstancePhase
		wantChosen    string
		wantSurplus   string
		xPassedChecks bool
	}{
		{"the named Instance is ready", v1alpha1.InstanceReady, "X", "P", false},
		{"the named Instance is verifying and passed", v1alpha1.InstanceVerifying, "P", "X", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSelFx()
			f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 15, 0)), rankedOutBy("X"))
			opts := []selOpt{hardEnd(hoT(12, 10, 0)), jobStartedAt(hoT(10, 0, 0))}
			if c.xPassedChecks {
				opts = append(opts, passedChecksOpt())
			}
			f.add("X", c.xPhase, opts...)
			chosen, surplus := selectPrimaries(f.selection(), 1)
			sameNames(t, "chosen", chosen, c.wantChosen)
			sameNames(t, "surplus", surplus, c.wantSurplus)
		})
	}
	t.Run("a mark that names nothing changes nothing", func(t *testing.T) {
		f := newSelFx()
		f.add("P", v1alpha1.InstanceReady, hardEnd(hoT(12, 15, 0)), rankedOutBy("gone"))
		f.add("X", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		chosen, _ := selectPrimaries(f.selection(), 1)
		sameNames(t, "chosen", chosen, "P")
	})
}

// TestSelectionCountsAHedgeOnceAndNeverARankedReplacement: a hedge is one unit, by its most advanced member, and
// its first member's creation orders it; a replacement whose predecessor counts is no unit, but one whose
// predecessor ended is a primary. Both are by hand.
func TestSelectionCountsAHedgeOnceAndNeverARankedReplacement(t *testing.T) {
	f := newSelFx()
	// h: a hedge of h (pending, created 09:01) and h-b (started 10:00, created 09:05): one unit, tier 4, ordered by
	// h's creation, 09:01. s: started 10:00, created 09:02. So h before s; the hedge's rep is h-b.
	f.add("h", v1alpha1.InstancePending, createdAt(hoT(9, 1, 0)))
	f.add("h-b", v1alpha1.InstanceStarting, createdAt(hoT(9, 5, 0)), inHedge("h"), jobStartedAt(hoT(10, 0, 0)))
	f.add("s", v1alpha1.InstanceStarting, createdAt(hoT(9, 2, 0)), jobStartedAt(hoT(10, 0, 0)))
	// gone ended: its replacement is a primary of its own now.
	f.add("gone", v1alpha1.InstanceEnded)
	f.add("after-gone", v1alpha1.InstancePending, createdAt(hoT(9, 3, 0)), replacementOf("gone"))
	chosen, surplus := selectPrimaries(f.selection(), 2)
	sameNames(t, "chosen", chosen, "h-b", "s")
	sameNames(t, "surplus", surplus, "after-gone")
}

// TestSelectionIsPureAndDeterministic: it writes nothing and never reads the order it was given: the same
// population in the reverse order gives the same lists, and the input is left as it was.
func TestSelectionIsPureAndDeterministic(t *testing.T) {
	f := newSelFx()
	f.add("a", v1alpha1.InstanceReady, hardEnd(hoT(12, 0, 0)))
	f.add("b", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 0, 0)))
	f.add("c", v1alpha1.InstancePending)
	f.add("d", v1alpha1.InstanceVerifying, passedChecksOpt(), jobStartedAt(hoT(10, 5, 0)))
	before := make([]v1alpha1.Instance, len(f.insts))
	for i, in := range f.insts {
		before[i] = *in.DeepCopy()
	}
	chosen, surplus := selectPrimaries(f.selection(), 2)
	sameNames(t, "chosen", chosen, "a", "d")
	sameNames(t, "surplus", surplus, "b", "c")
	rev := newSelFx()
	rev.allocs = f.allocs
	for i := len(f.insts) - 1; i >= 0; i-- {
		rev.insts = append(rev.insts, f.insts[i])
	}
	chosen, surplus = selectPrimaries(rev.selection(), 2)
	sameNames(t, "reversed chosen", chosen, "a", "d")
	sameNames(t, "reversed surplus", surplus, "b", "c")
	for i, in := range f.insts {
		if !slices.Equal([]string{in.Name, string(in.Status.Phase)}, []string{before[i].Name, string(before[i].Status.Phase)}) ||
			len(in.Status.Conditions) != len(before[i].Status.Conditions) {
			t.Errorf("Instance %s was changed by the selection", in.Name)
		}
	}
}

// TestSelectionTreatsOnlyTheNamedInstanceAsPassed: the pass ranks an Instance whose checks it has just judged, as
// passed; the others are read from their status.
func TestSelectionTreatsOnlyTheNamedInstanceAsPassed(t *testing.T) {
	f := newSelFx()
	x := f.add("x", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 30, 0)))
	f.add("y", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 0, 0)))
	// Without the override y, the earlier job start, is chosen; with x read as passed, x is.
	chosen, _ := selectPrimaries(f.selection(), 1)
	sameNames(t, "no override", chosen, "y")
	s := f.selection()
	s.passed = x
	chosen, _ = selectPrimaries(s, 1)
	sameNames(t, "x as passed", chosen, "x")
}

// TestPrimariesAndScaleDownTargetsReadOneSelection: primaries() is the selection's chosen list at spec.instances,
// and what scaleDownTargets takes at the current count is exactly the selection's surplus (with k 0), so the two
// are the same function: here a population of six, instances 2, hand-ordered. Chosen: ready-with-replacement rr and
// ready rd. Surplus, least advanced first: pe (pending), st (started), pa (passed) and nr (not ready, live
// replacement nr-repl, taken with it). Lowering to 1 adds rd.
func TestPrimariesAndScaleDownTargetsReadOneSelection(t *testing.T) {
	f := newSelFx()
	f.add("pe", v1alpha1.InstancePending)
	f.add("st", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 30, 0)))
	f.add("pa", v1alpha1.InstanceVerifying, jobStartedAt(hoT(10, 20, 0)), passedChecksOpt())
	f.add("nr", v1alpha1.InstanceStarting, jobStartedAt(hoT(10, 10, 0)))
	f.add("nr-repl", v1alpha1.InstancePending, replacementOf("nr"))
	f.add("rd", v1alpha1.InstanceReady, hardEnd(hoT(13, 0, 0)), jobStartedAt(hoT(9, 0, 0)))
	f.add("rr", v1alpha1.InstanceReady, hardEnd(hoT(12, 0, 0)), jobStartedAt(hoT(9, 0, 0)))
	f.add("rr-repl", v1alpha1.InstancePending, replacementOf("rr"))
	p := f.pass(2, 0)
	prim := p.primaries()
	sameNames(t, "primaries", prim, "rr", "rd")
	var got []string
	for _, tg := range p.scaleDownTargets(prim, 0) {
		got = append(got, tg.instance.Name)
	}
	if want := []string{"pe", "st", "pa", "nr-repl", "nr"}; !slices.Equal(got, want) {
		t.Errorf("targets at the current count: %v, want %v", got, want)
	}
	got = nil
	for _, tg := range p.scaleDownTargets(prim, 1) {
		got = append(got, tg.instance.Name)
	}
	if want := []string{"pe", "st", "pa", "nr-repl", "nr", "rd"}; !slices.Equal(got, want) {
		t.Errorf("targets lowering to 1: %v, want %v", got, want)
	}
}

// TestPassedIsReadFromStatusAlone: the three conditions True, and with several workers every worker warmed up. A
// restart resets them, so a restarted Instance is no longer passed. The agent's report is not read: the reserved
// slot reads the same status under a Provider's lock, where the agent's backend is never taken.
func TestPassedIsReadFromStatusAlone(t *testing.T) {
	f := newSelFx()
	in := f.add("a", v1alpha1.InstanceVerifying, passedChecksOpt())
	if !passedChecks(in) {
		t.Fatal("verified, qualified and warmed up: want passed")
	}
	for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
		cp := in.DeepCopy()
		for i := range cp.Status.Conditions {
			if cp.Status.Conditions[i].Type == c {
				cp.Status.Conditions[i].Status = metav1.ConditionUnknown
			}
		}
		if passedChecks(cp) {
			t.Errorf("%s Unknown: want not passed", c)
		}
	}
}

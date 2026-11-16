package e2e

// Hedging's API (beta; HBEP-7, What the controller reads and Hedging (beta); #604): the ModelDeployment's
// spec.hedgeWidth, defaulted to 2 and bounded 1 to 8, and its status.startOutcomes, each class's last 20 start-up
// outcomes with the rate kept as two counts that must agree with them. Every expected value is written out here.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestHedgeWidthDefaultsAndBounds: a deployment written without hedgeWidth reads back 2, the API server's
// default; 1 and 8 are accepted, and 0 and 9 refused.
func TestHedgeWidthDefaultsAndBounds(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "hedge-default")
	must(t, c.Create(bg(), md), "create without hedgeWidth")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if got.Spec.HedgeWidth == nil || *got.Spec.HedgeWidth != 2 {
		t.Fatalf("hedgeWidth read back as %v, want the default 2", got.Spec.HedgeWidth)
	}
	for _, tc := range []struct {
		n  int32
		ok bool
	}{{0, false}, {1, true}, {8, true}, {9, false}} {
		md := newModelDeployment(ns, "hedge-bounds-"+string(rune('a'+tc.n)))
		v := tc.n
		md.Spec.HedgeWidth = &v
		err := c.Create(bg(), md)
		if tc.ok {
			must(t, err, "create with hedgeWidth in bounds")
			back := &v1alpha1.ModelDeployment{}
			must(t, c.Get(bg(), objKey(md), back), "get")
			if back.Spec.HedgeWidth == nil || *back.Spec.HedgeWidth != tc.n {
				t.Errorf("hedgeWidth %d read back as %v", tc.n, back.Spec.HedgeWidth)
			}
		} else {
			requireRefused(t, err, "hedgeWidth")
		}
	}
}

// outcome is one start-up outcome, at a fixed time.
func outcome(uid, result, reason string) v1alpha1.StartOutcome {
	return v1alpha1.StartOutcome{AllocationUID: types.UID(uid), Result: result, Reason: reason,
		Time: metav1.NewTime(time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC))}
}

// classOutcomes is one class's outcomes with the counts given.
func classOutcomes(failures, count int32, outcomes ...v1alpha1.StartOutcome) v1alpha1.ClassStartOutcomes {
	return v1alpha1.ClassStartOutcomes{AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "a100", CPUArch: "x86_64"},
		Outcomes: outcomes, Failures: failures, Count: count}
}

// withStartOutcomes writes the classes to a fresh deployment's status and returns the error of that write.
func withStartOutcomes(t *testing.T, ns, name string, classes ...v1alpha1.ClassStartOutcomes) (*v1alpha1.ModelDeployment, error) {
	t.Helper()
	md := newModelDeployment(ns, name)
	must(t, c.Create(bg(), md), "create")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	got.Status.StartOutcomes = classes
	return got, c.Status().Update(bg(), got)
}

// TestStartOutcomesRoundTrip: a class with one Ready and one Failed outcome, 1 failure of 2, is kept as written;
// so is a class with no outcome yet, 0 of 0.
func TestStartOutcomesRoundTrip(t *testing.T) {
	ns := newNamespace(t)
	md, err := withStartOutcomes(t, ns, "outcomes",
		classOutcomes(1, 2, outcome("uid-1", "Ready", ""), outcome("uid-2", "Failed", "RegistrationTimeout")))
	must(t, err, "write two outcomes")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if n := len(got.Status.StartOutcomes); n != 1 {
		t.Fatalf("%d classes read back, want 1", n)
	}
	cl := got.Status.StartOutcomes[0]
	if cl.Failures != 1 || cl.Count != 2 || len(cl.Outcomes) != 2 ||
		cl.Outcomes[0].AllocationUID != "uid-1" || cl.Outcomes[0].Result != "Ready" || cl.Outcomes[0].Reason != "" ||
		cl.Outcomes[1].AllocationUID != "uid-2" || cl.Outcomes[1].Result != "Failed" || cl.Outcomes[1].Reason != "RegistrationTimeout" {
		t.Fatalf("read back %+v, want 1 of 2: uid-1 Ready, uid-2 Failed RegistrationTimeout", cl)
	}
	_, err = withStartOutcomes(t, ns, "no-outcomes", classOutcomes(0, 0))
	must(t, err, "write a class with no outcome yet")
}

// TestStartOutcomesCountsAgree: count is the number of outcomes kept, and failures the number whose result is
// Failed. Each is refused when it disagrees, also against an absent list.
func TestStartOutcomesCountsAgree(t *testing.T) {
	ns := newNamespace(t)
	ready, failed := outcome("uid-1", "Ready", ""), outcome("uid-2", "Failed", "Requeued")
	for _, tc := range []struct {
		name string
		cl   v1alpha1.ClassStartOutcomes
		want string
	}{
		{"count above the outcomes", classOutcomes(1, 3, ready, failed), "count is the number of outcomes kept"},
		{"count below the outcomes", classOutcomes(1, 1, ready, failed), "count is the number of outcomes kept"},
		{"a count with no outcome", classOutcomes(0, 1), "count is the number of outcomes kept"},
		{"failures below the Failed outcomes", classOutcomes(0, 2, ready, failed), "failures is the number of outcomes kept whose result is Failed"},
		{"failures above the Failed outcomes", classOutcomes(2, 2, ready, failed), "failures is the number of outcomes kept whose result is Failed"},
		{"failures with no outcome", classOutcomes(1, 0), "failures is the number of outcomes kept whose result is Failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := withStartOutcomes(t, ns, uniq("counts"), tc.cl)
			requireRefused(t, err, tc.want)
		})
	}
}

// TestStartOutcomeReasonOnlyForAFailure: a Failed outcome names its reason, and a Ready one names none.
func TestStartOutcomeReasonOnlyForAFailure(t *testing.T) {
	ns := newNamespace(t)
	const want = "reason is set for a Failed outcome, and only for one"
	_, err := withStartOutcomes(t, ns, "failed-no-reason", classOutcomes(1, 1, outcome("uid-1", "Failed", "")))
	requireRefused(t, err, want)
	_, err = withStartOutcomes(t, ns, "ready-with-reason", classOutcomes(0, 1, outcome("uid-1", "Ready", "RegistrationTimeout")))
	requireRefused(t, err, want)
}

// TestStartOutcomesBounds: at most 20 outcomes a class, each Allocation once, and a result of Ready or Failed.
func TestStartOutcomesBounds(t *testing.T) {
	ns := newNamespace(t)
	var twenty, twentyOne []v1alpha1.StartOutcome
	for i := 0; i < 21; i++ {
		o := outcome("uid-"+string(rune('a'+i)), "Ready", "")
		if i < 20 {
			twenty = append(twenty, o)
		}
		twentyOne = append(twentyOne, o)
	}
	_, err := withStartOutcomes(t, ns, "twenty", classOutcomes(0, 20, twenty...))
	must(t, err, "write 20 outcomes")
	_, err = withStartOutcomes(t, ns, "twenty-one", classOutcomes(0, 21, twentyOne...))
	requireRefused(t, err, "outcomes")
	_, err = withStartOutcomes(t, ns, "twice", classOutcomes(0, 2, outcome("uid-1", "Ready", ""), outcome("uid-1", "Ready", "")))
	requireRefused(t, err, "uid-1")
	_, err = withStartOutcomes(t, ns, "started", classOutcomes(0, 1, outcome("uid-1", "Started", "")))
	requireRefused(t, err, "result")
}

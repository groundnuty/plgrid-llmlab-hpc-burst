package agentcli

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func endSpec(md string, end *v1alpha1.EndParameters) v1alpha1.OperationSpec {
	return v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}, End: end, Owner: userA}
}

func releaseSpec(md string, r *v1alpha1.ReleaseParameters) v1alpha1.OperationSpec {
	return v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}, Release: r, Owner: userA}
}

// The idempotency comparison: when an Operation exists under the name, is it the same request? Each row
// is judged by hand from HBEP-10 (Idempotency): the action, the target, the parameters and the owner.
func TestSameRequest(t *testing.T) {
	scale := func(n int32, owner string) v1alpha1.OperationSpec { s := scaleSpec("m", n); s.Owner = owner; return s }
	sel := func(m map[string]string, exprs ...metav1.LabelSelectorRequirement) *v1alpha1.ReleaseParameters {
		return &v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: m, MatchExpressions: exprs}}
	}
	in := func(key string, vals ...string) metav1.LabelSelectorRequirement {
		return metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpIn, Values: vals}
	}
	rows := []struct {
		name string
		a, b v1alpha1.OperationSpec
		want bool
	}{
		{"the same scale", scale(1, userA), scale(1, userA), true},
		{"another count", scale(1, userA), scale(0, userA), false},
		{"another owner", scale(1, userA), scale(1, userB), false},
		{"an absent end equals {delete: false}", endSpec("m", nil), endSpec("m", &v1alpha1.EndParameters{}), true},
		{"an absent end equals an explicit false", endSpec("m", nil), endSpec("m", &v1alpha1.EndParameters{Delete: false}), true},
		{"end with delete differs from a plain end", endSpec("m", nil), endSpec("m", &v1alpha1.EndParameters{Delete: true}), false},
		{"end of another deployment", endSpec("m", nil), endSpec("m2", nil), false},
		{"scale and end differ", scale(0, userA), endSpec("m", nil), false},
		{"allocations are a set: order does not matter", releaseSpec("m", &v1alpha1.ReleaseParameters{Allocations: []string{"b", "a"}}),
			releaseSpec("m", &v1alpha1.ReleaseParameters{Allocations: []string{"a", "b"}}), true},
		{"other allocations", releaseSpec("m", &v1alpha1.ReleaseParameters{Allocations: []string{"a"}}),
			releaseSpec("m", &v1alpha1.ReleaseParameters{Allocations: []string{"a", "b"}}), false},
		{"names are not a selector", releaseSpec("m", &v1alpha1.ReleaseParameters{Allocations: []string{"a"}}), releaseSpec("m", sel(map[string]string{"x": "a"})), false},
		{"the same selector", releaseSpec("m", sel(map[string]string{"x": "a", "y": "b"})), releaseSpec("m", sel(map[string]string{"y": "b", "x": "a"})), true},
		{"the same selector with expressions in another order", releaseSpec("m", sel(nil, in("x", "a", "b"), in("y", "c"))),
			releaseSpec("m", sel(nil, in("y", "c"), in("x", "a", "b"))), true},
		{"another selector", releaseSpec("m", sel(map[string]string{"x": "a"})), releaseSpec("m", sel(map[string]string{"x": "b"})), false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			a, b := r.a, r.b
			if a.Owner == "" {
				a.Owner = userA
			}
			if b.Owner == "" {
				b.Owner = userA
			}
			if got := sameRequest(&a, &b); got != r.want {
				t.Errorf("sameRequest = %v, want %v", got, r.want)
			}
			if got := sameRequest(&b, &a); got != r.want {
				t.Errorf("sameRequest is not symmetric: reversed gave %v", got)
			}
		})
	}
}

// Through the command: one Operation after two calls with a key, and a third with another request is refused.
func TestAKeyMakesOneOperation(t *testing.T) {
	w := newWorld(t, nil)
	w.controller("k", simplePlan(planAB), "")
	for i := 1; i <= 2; i++ {
		if code := w.run("", "scale", "m", "--instances", "1", "--key", "k"); code != exitOK {
			t.Fatalf("call %d: exit %d, stderr %q", i, code, w.errs.String())
		}
	}
	contains(t, "stderr of the retry", w.errs.String(), "Operation k exists with the same request")
	if got := w.operations(); len(got) != 1 || got[0] != "k" {
		t.Fatalf("Operations %v, want one", got)
	}
	if code := w.run("", "scale", "m", "--instances", "0", "--key", "k"); code != exitFail {
		t.Fatalf("a different request under the key: exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "Operation k exists with a different request")
	lacks(t, "stdout", w.out.String(), "plan "+planAB) // nothing is waited for or printed
	if op := w.operation("k"); op.Spec.Scale.Instances != 1 {
		t.Errorf("the stored request changed to %d", op.Spec.Scale.Instances)
	}
	// Another identity's Operation of that name and request is not the same request: another owner.
	w.user = userB
	if code := w.run("", "scale", "m", "--instances", "1", "--key", "k"); code != exitFail {
		t.Fatalf("another owner under the key: exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "exists with a different request")
}

package e2e

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// An Approval names an Operation by name and UID. The UID is an example.
const approvalOperationUID = "00000000-0000-0000-0000-0000000000c1"

// newApproval returns an Approval that admission accepts. The approver pin, an
// admission policy of a later change, ties approver to the requester; the CRD
// holds only the shape.
func newApproval(ns, name string) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: "example-op", UID: approvalOperationUID},
			PlanID:    opPlanID,
			Approver:  v1alpha1.ApproverIdentity{Username: "example-person", Groups: []string{"example-group", "system:authenticated"}},
		},
	}
}

type approvalCase struct {
	name string
	edit func(*v1alpha1.Approval)
	want string // refusals only
}

func runApprovalCases(t *testing.T, refused, accepted []approvalCase) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			a := newApproval(ns, uniq("ap"))
			tc.edit(a)
			requireRefused(t, c.Create(bg(), a), tc.want)
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			a := newApproval(ns, uniq("ap"))
			tc.edit(a)
			must(t, c.Create(bg(), a), "create Approval")
		})
	}
}

// The schema's bounds on an Approval: it names an Operation and a plan, and its
// approver has a user name and at most 64 groups.
func TestApprovalSpecSchemaBounds(t *testing.T) {
	groups := func(n int) []string {
		g := make([]string, n)
		for i := range g {
			g[i] = "example-group-" + strings.Repeat("a", i%5+1)
		}
		return g
	}
	runApprovalCases(t, []approvalCase{
		{"no operation name", func(a *v1alpha1.Approval) { a.Spec.Operation.Name = "" }, "spec.operation.name"},
		{"no operation UID", func(a *v1alpha1.Approval) { a.Spec.Operation.UID = "" }, "spec.operation.uid"},
		{"no plan ID", func(a *v1alpha1.Approval) { a.Spec.PlanID = "" }, "spec.planID"},
		{"a plan ID of 65 characters", func(a *v1alpha1.Approval) { a.Spec.PlanID = strings.Repeat("a", 65) }, "spec.planID"},
		{"no user name", func(a *v1alpha1.Approval) { a.Spec.Approver.Username = "" }, "spec.approver.username"},
		{"65 groups", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = groups(65) }, "spec.approver.groups"},
		{"a group of 1025 characters", func(a *v1alpha1.Approval) {
			a.Spec.Approver.Groups = []string{strings.Repeat("a", 1025)}
		}, "spec.approver.groups[0]"},
	}, []approvalCase{
		{"no groups", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = nil }, ""},
		{"64 groups", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = groups(64) }, ""},
		{"a group of 1024 characters", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = []string{strings.Repeat("a", 1024)} }, ""},
		{"a user name of 1024 characters", func(a *v1alpha1.Approval) { a.Spec.Approver.Username = strings.Repeat("a", 1024) }, ""},
	})
}

// An Approval is immutable: it is bound to one plan and one approver, so an
// update that changes any part of its spec is refused. This is the rule
// approvals:$.spec#0. The controls are an update that changes no part of the
// spec and one that changes only metadata.
func TestApprovalSpecIsImmutable(t *testing.T) {
	const msg = "an Approval's spec is immutable"
	ns := newNamespace(t)
	cases := []struct {
		name string
		edit func(*v1alpha1.Approval)
		want string // "" for an update that is accepted
	}{
		{"a changed operation name", func(a *v1alpha1.Approval) { a.Spec.Operation.Name = "another-op" }, msg},
		{"a changed operation UID", func(a *v1alpha1.Approval) { a.Spec.Operation.UID = "00000000-0000-0000-0000-0000000000c2" }, msg},
		{"a changed plan ID", func(a *v1alpha1.Approval) { a.Spec.PlanID = opAnotherPlan }, msg},
		{"a changed user name", func(a *v1alpha1.Approval) { a.Spec.Approver.Username = "another-person" }, msg},
		{"a group added", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = append(a.Spec.Approver.Groups, "another-group") }, msg},
		{"a group removed", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = a.Spec.Approver.Groups[:1] }, msg},
		{"all groups removed", func(a *v1alpha1.Approval) { a.Spec.Approver.Groups = nil }, msg},
		{"an unchanged spec", func(a *v1alpha1.Approval) {}, ""},
		{"a label", func(a *v1alpha1.Approval) { a.Labels = map[string]string{"example": "x"} }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newApproval(ns, uniq("ap"))
			must(t, c.Create(bg(), a), "create Approval")
			stored := a.DeepCopy()
			edited := a.DeepCopy()
			tc.edit(edited)
			err := c.Update(bg(), edited)
			if tc.want == "" {
				must(t, err, "update Approval")
				return
			}
			requireRefused(t, err, tc.want)
			got := &v1alpha1.Approval{}
			must(t, c.Get(bg(), objKey(a), got), "get Approval after the refused update")
			if !equality.Semantic.DeepEqual(got.Spec, stored.Spec) {
				t.Errorf("a refused update changed the stored spec: %+v, was %+v", got.Spec, stored.Spec)
			}
		})
	}
}

package e2e

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Fixtures. The owner has the shape of an agent's ServiceAccount user name; it
// is an example, as every name here is.
const (
	opOwner       = "system:serviceaccount:example-agents:example-agent"
	opDeployment  = "example-md"
	opAllocation  = "example-md-abcde"
	opPlanID      = "0123456789abcdef"
	opAnotherPlan = "fedcba9876543210"
)

// newOperation returns a Scale Operation that admission accepts. The tests that
// need another action build it with newEndOperation or newReleaseOperation, so a
// refusal case is never "this object with one block taken out".
func newOperation(ns, name string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.OperationSpec{
			Action:             v1alpha1.OperationActionScale,
			ModelDeploymentRef: v1alpha1.ObjectReference{Name: opDeployment},
			Scale:              &v1alpha1.ScaleParameters{Instances: 1},
			Owner:              opOwner,
		},
	}
}

// newEndOperation returns an End Operation with no parameter block, which is
// valid: a plain End needs none.
func newEndOperation(ns, name string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.OperationSpec{
			Action:             v1alpha1.OperationActionEnd,
			ModelDeploymentRef: v1alpha1.ObjectReference{Name: opDeployment},
			Owner:              opOwner,
		},
	}
}

// newReleaseOperation returns a Release Operation that names one Allocation.
func newReleaseOperation(ns, name string) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.OperationSpec{
			Action:             v1alpha1.OperationActionRelease,
			ModelDeploymentRef: v1alpha1.ObjectReference{Name: opDeployment},
			Release:            &v1alpha1.ReleaseParameters{Allocations: []string{opAllocation}},
			Owner:              opOwner,
		},
	}
}

// opCase is one Operation the API server is asked to create. build returns the
// whole object, so each case states what it is.
type opCase struct {
	name  string
	build func(ns, name string) *v1alpha1.Operation
	want  string // refusals only: the rule's message
}

func runOpCases(t *testing.T, refused, accepted []opCase) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, c.Create(bg(), tc.build(ns, uniq("op"))), tc.want)
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			must(t, c.Create(bg(), tc.build(ns, uniq("op"))), "create Operation")
		})
	}
}

// withOperation returns a builder that starts from base and applies edit.
func withOperation(base func(ns, name string) *v1alpha1.Operation, edit func(*v1alpha1.Operation)) func(ns, name string) *v1alpha1.Operation {
	return func(ns, name string) *v1alpha1.Operation {
		op := base(ns, name)
		edit(op)
		return op
	}
}

// opUpdate is one update of a stored Operation: base creates it, edit changes it.
type opUpdate struct {
	name string
	base func(ns, name string) *v1alpha1.Operation
	edit func(*v1alpha1.Operation)
	want string // refusals only: the rule's message
}

// updateOperation creates the case's Operation, reads it back and sends the
// edited copy as an update. It returns the stored object before the edit and
// the update's error.
func updateOperation(t *testing.T, ns string, tc opUpdate) (*v1alpha1.Operation, error) {
	t.Helper()
	op := tc.base(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	stored := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), stored), "get Operation")
	edited := stored.DeepCopy()
	tc.edit(edited)
	return stored, c.Update(bg(), edited)
}

// runOpUpdates runs each refused update, and checks that the stored spec is
// still the one created; then each accepted update.
func runOpUpdates(t *testing.T, refused, accepted []opUpdate) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			stored, err := updateOperation(t, ns, tc)
			requireRefused(t, err, tc.want)
			got := &v1alpha1.Operation{}
			must(t, c.Get(bg(), objKey(stored), got), "get Operation after the refused update")
			if !equality.Semantic.DeepEqual(got.Spec, stored.Spec) {
				t.Errorf("a refused update changed the stored spec: %+v, was %+v", got.Spec, stored.Spec)
			}
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			_, err := updateOperation(t, ns, tc)
			must(t, err, "update Operation")
		})
	}
}

func scaleOf(instances int32) *v1alpha1.ScaleParameters {
	return &v1alpha1.ScaleParameters{Instances: instances}
}

// A Scale Operation sets scale, and neither end nor release. This is the rule
// operations:$.spec#4.
func TestScaleOperationSetsScaleAndNothingElse(t *testing.T) {
	const msg = "a Scale Operation sets scale"
	runOpCases(t, []opCase{
		{"no scale block", withOperation(newOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = nil }), msg},
		{"scale with an end block", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{Delete: true}
		}), msg},
		{"scale with a release block", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Allocations: []string{opAllocation}}
		}), msg},
	}, []opCase{
		{"scale to zero", withOperation(newOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = scaleOf(0) }), ""},
		{"scale to a lower count", func(ns, name string) *v1alpha1.Operation {
			op := newOperation(ns, name)
			op.Spec.Scale = scaleOf(3)
			return op
		}, ""},
	})
}

// An End Operation sets end, or nothing, and neither scale nor release. This is
// the rule operations:$.spec#5.
func TestEndOperationSetsEndOrNothingElse(t *testing.T) {
	const msg = "an End Operation sets end, or nothing"
	runOpCases(t, []opCase{
		{"end with a scale block", withOperation(newEndOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = scaleOf(1) }), msg},
		{"end with a release block", withOperation(newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Allocations: []string{opAllocation}}
		}), msg},
		{"end with its own block and a scale block", withOperation(newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{Delete: true}
			op.Spec.Scale = scaleOf(1)
		}), msg},
	}, []opCase{
		{"a plain end, with no block", newEndOperation, ""},
		{"an end that deletes", withOperation(newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{Delete: true}
		}), ""},
		{"an empty end block", withOperation(newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{}
		}), ""},
	})
}

// A Release Operation sets release, and neither scale nor end. This is the rule
// operations:$.spec#6.
func TestReleaseOperationSetsReleaseAndNothingElse(t *testing.T) {
	const msg = "a Release Operation sets release"
	runOpCases(t, []opCase{
		{"no release block", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) { op.Spec.Release = nil }), msg},
		{"release with a scale block", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = scaleOf(1) }), msg},
		{"release with an end block", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{}
		}), msg},
	}, []opCase{
		{"release by name", newReleaseOperation, ""},
		{"release by selector", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"example": "x"}}}
		}), ""},
	})
}

// A Release names exactly one of allocations and a selector, and the names are a
// non-empty set of bounded strings. This is the rule operations:$.spec.release#1
// (one of the two); the other cases are the schema's bounds.
func TestReleaseNamesAllocationsOrASelector(t *testing.T) {
	const msg = "a Release names exactly one of allocations and selector"
	byName := func(names ...string) func(op *v1alpha1.Operation) {
		return func(op *v1alpha1.Operation) { op.Spec.Release = &v1alpha1.ReleaseParameters{Allocations: names} }
	}
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{"example": "x"}}
	many := make([]string, 65)
	for i := range many {
		many[i] = "example-md-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
	}
	runOpCases(t, []opCase{
		{"both names and a selector", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Allocations: []string{opAllocation}, Selector: sel}
		}), msg},
		{"neither", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{}
		}), msg},
		{"an empty name", withOperation(newReleaseOperation, byName("")), "should be at least 1 chars long"},
		{"a name of 254 characters", withOperation(newReleaseOperation, byName(strings.Repeat("a", 254))), "may not be more than 253"},
		{"65 names", withOperation(newReleaseOperation, byName(many...)), "must have at most 64 items"},
		{"a name twice", withOperation(newReleaseOperation, byName(opAllocation, opAllocation)), "Duplicate value"},
	}, []opCase{
		{"one name", withOperation(newReleaseOperation, byName(opAllocation)), ""},
		{"64 names", withOperation(newReleaseOperation, byName(many[:64]...)), ""},
		{"a name of 253 characters", withOperation(newReleaseOperation, byName(strings.Repeat("a", 253))), ""},
		{"a selector with match labels", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Selector: sel}
		}), ""},
		{"a selector with an expression", withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release = &v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "example", Operator: metav1.LabelSelectorOpIn, Values: []string{"x", "y"}}}}}
		}), ""},
	})
}

// An empty allocations list, written as JSON, is refused: the typed client omits
// an empty slice, so this goes through the unstructured client.
func TestReleaseWithAnEmptyAllocationsListIsRefused(t *testing.T) {
	ns := newNamespace(t)
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(), "kind": "Operation",
		"metadata": map[string]any{"name": uniq("op"), "namespace": ns},
		"spec": map[string]any{
			"action": "Release", "owner": opOwner, "modelDeploymentRef": map[string]any{"name": opDeployment},
			"release": map[string]any{"allocations": []any{}},
		},
	}}
	requireRefused(t, c.Create(bg(), u), "release.allocations", "should have at least 1 items")
	// The control: the same object with one name is accepted.
	u2 := u.DeepCopy()
	u2.SetName(uniq("op"))
	must(t, unstructured.SetNestedStringSlice(u2.Object, []string{opAllocation}, "spec", "release", "allocations"), "set the name")
	must(t, c.Create(bg(), u2), "create the Operation with one name")
}

// The schema's bounds on the other fields of an Operation: the action is one of
// three, the owner is a bounded non-empty string, a count is not negative, a
// confirm names a plan.
func TestOperationSpecSchemaBounds(t *testing.T) {
	runOpCases(t, []opCase{
		{"an unknown action", withOperation(newOperation, func(op *v1alpha1.Operation) { op.Spec.Action = "Repurpose" }), "Unsupported value"},
		{"an empty owner", withOperation(newOperation, func(op *v1alpha1.Operation) { op.Spec.Owner = "" }), "spec.owner"},
		{"an owner of 1025 characters", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Owner = strings.Repeat("a", 1025)
		}), "spec.owner"},
		{"a negative instance count", withOperation(newOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = scaleOf(-1) }), "spec.scale.instances"},
		{"no deployment name", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.ModelDeploymentRef.Name = ""
		}), "spec.modelDeploymentRef.name"},
		{"an empty plan ID", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{}
		}), "spec.confirm.planID"},
		{"a plan ID of 65 characters", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: strings.Repeat("a", 65)}
		}), "spec.confirm.planID"},
	}, []opCase{
		{"an owner of 1024 characters", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Owner = strings.Repeat("a", 1024)
		}), ""},
		{"a plan ID of 64 characters", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: strings.Repeat("a", 64)}
		}), ""},
	})
}

// An Operation's spec is immutable except confirm: each field, and each
// optional block's presence, refuses a change. These are the rules
// operations:$.spec.action#0, $.spec.modelDeploymentRef#0, $.spec.owner#0,
// $.spec.scale#0, $.spec.end#0, $.spec.release#0, and the three presence rules
// $.spec#1, #2 and #3.
//
// Each refusal is checked by its own message, not by "refused": a change of the
// action, and the adding or removing of a Scale or a Release block, also break
// the action rules, so the refusal alone would not say which rule fired. A
// plain End takes no block, so the presence of its end block is the one change
// that no other rule refuses; that case is the presence rule's own.
func TestOperationSpecIsImmutable(t *testing.T) {
	const immutable = " is immutable"
	const presence = " cannot be added or removed after create"
	release := func(op *v1alpha1.Operation) {
		op.Spec.Release = &v1alpha1.ReleaseParameters{Allocations: []string{opAllocation}}
	}
	endDeleting := withOperation(newEndOperation, func(op *v1alpha1.Operation) { op.Spec.End = &v1alpha1.EndParameters{Delete: true} })
	bySelector := withOperation(newReleaseOperation, func(op *v1alpha1.Operation) {
		op.Spec.Release = &v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"example": "x"}}}
	})
	runOpUpdates(t, []opUpdate{
		{"a changed action", newEndOperation, func(op *v1alpha1.Operation) { op.Spec.Action = v1alpha1.OperationActionRelease }, "spec.action" + immutable},
		{"a changed deployment", newOperation, func(op *v1alpha1.Operation) { op.Spec.ModelDeploymentRef.Name = "another-md" }, "spec.modelDeploymentRef" + immutable},
		{"a changed owner", newOperation, func(op *v1alpha1.Operation) { op.Spec.Owner = "another-owner" }, "spec.owner" + immutable},
		{"a changed instance count", newOperation, func(op *v1alpha1.Operation) { op.Spec.Scale.Instances = 2 }, "spec.scale" + immutable},
		{"a changed end block", endDeleting, func(op *v1alpha1.Operation) { op.Spec.End = &v1alpha1.EndParameters{} }, "spec.end" + immutable},
		{"a changed allocation list", newReleaseOperation, func(op *v1alpha1.Operation) {
			op.Spec.Release.Allocations = []string{"another-alloc"}
		}, "spec.release" + immutable},
		{"a changed selector", bySelector, func(op *v1alpha1.Operation) {
			op.Spec.Release.Selector.MatchLabels["example"] = "y"
		}, "spec.release" + immutable},
		{"a scale block removed", newOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = nil }, "spec.scale" + presence},
		{"a scale block added to an End", newEndOperation, func(op *v1alpha1.Operation) { op.Spec.Scale = scaleOf(1) }, "spec.scale" + presence},
		{"an end block added to a plain End", newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.End = &v1alpha1.EndParameters{Delete: true}
		}, "spec.end" + presence},
		{"an end block removed from an End", endDeleting, func(op *v1alpha1.Operation) { op.Spec.End = nil }, "spec.end" + presence},
		{"a release block removed", newReleaseOperation, func(op *v1alpha1.Operation) { op.Spec.Release = nil }, "spec.release" + presence},
		{"a release block added to an End", newEndOperation, release, "spec.release" + presence},
	}, []opUpdate{
		// The controls: the rules refuse a change, not an update.
		{"an unchanged spec", newOperation, func(op *v1alpha1.Operation) {}, ""},
		{"a purpose annotation", newOperation, func(op *v1alpha1.Operation) {
			op.Annotations = map[string]string{v1alpha1.AnnotationPurpose: "example purpose"}
		}, ""},
		{"a label", newEndOperation, func(op *v1alpha1.Operation) { op.Labels = map[string]string{"example": "x"} }, ""},
		{"a first confirm", newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
		}, ""},
	})
}

// confirm is set once: the owner's first update that sets it is accepted, and
// after that it cannot be changed or removed. This is the rule
// operations:$.spec#0.
func TestOperationConfirmIsSetOnce(t *testing.T) {
	const msg = "confirm is set once"
	confirmed := withOperation(newOperation, func(op *v1alpha1.Operation) {
		op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
	})
	runOpUpdates(t, []opUpdate{
		{"a changed plan ID", confirmed, func(op *v1alpha1.Operation) { op.Spec.Confirm.PlanID = opAnotherPlan }, msg},
		{"a removed confirm", confirmed, func(op *v1alpha1.Operation) { op.Spec.Confirm = nil }, msg},
		{"a changed plan ID on an End", withOperation(newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
		}), func(op *v1alpha1.Operation) { op.Spec.Confirm.PlanID = opAnotherPlan }, msg},
	}, []opUpdate{
		{"a first confirm", newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
		}, ""},
		{"a first confirm on an End", newEndOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: opPlanID}
		}, ""},
		{"the same confirm again", confirmed, func(op *v1alpha1.Operation) { op.Annotations = map[string]string{"example": "x"} }, ""},
	})
}

// A status that arrives with a create is dropped: the status subresource is the
// only way to write it, so an agent that creates an Operation already
// "Executing", with a committed target, gets neither (HBEP-10, Owner and
// purpose; the admission policies rely on this). The answer to the create and a
// fresh read both show an empty status.
func TestOperationStatusSentWithACreateIsDropped(t *testing.T) {
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	op.Status = v1alpha1.OperationStatus{
		Phase: v1alpha1.OperationPhaseExecuting,
		Committed: []v1alpha1.CommittedTarget{{
			Kind: v1alpha1.TargetKindAllocation, Name: opAllocation, UID: "00000000-0000-0000-0000-000000000001",
			Generation: 1, Effect: v1alpha1.PlanEffectReleaseStarted,
		}},
	}
	// The request body carries the status: without it, an empty answer would
	// prove nothing about the server.
	body, err := json.Marshal(op)
	must(t, err, "marshal")
	if !strings.Contains(string(body), `"status":{"phase":"Executing"`) || !strings.Contains(string(body), `"committed":[`) {
		t.Fatalf("the create does not carry the status: %s", body)
	}
	sent := op.DeepCopy()
	must(t, c.Create(bg(), op), "create Operation with a status")
	if !reflect.DeepEqual(op.Status, v1alpha1.OperationStatus{}) {
		t.Errorf("the create's answer carries a status: %+v", op.Status)
	}
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get Operation")
	if !reflect.DeepEqual(got.Status, v1alpha1.OperationStatus{}) {
		t.Errorf("the stored Operation has a status: %+v", got.Status)
	}
	// The control: the status subresource does store the same status, so the
	// empty status above was the create's doing, not a field the schema loses.
	got.Status = sent.Status
	must(t, c.Status().Update(bg(), got), "write the status through the subresource")
	again := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), again), "get Operation after the status write")
	if again.Status.Phase != v1alpha1.OperationPhaseExecuting || len(again.Status.Committed) != 1 {
		t.Errorf("the status subresource did not store the status: %+v", again.Status)
	}
}

// The status subresource takes a full plan: one target per effect, every figure
// of a release cost, the committed list and the report. A figure that is not
// known is stored as its reason, with no value.
func TestOperationStatusRoundTripsAPlan(t *testing.T) {
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")

	status := fullOperationStatus()
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get Operation")
	got.Status = status
	must(t, c.Status().Update(bg(), got), "write the status")

	back := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), back), "get Operation after the status write")
	if !equality.Semantic.DeepEqual(back.Status, status) {
		t.Errorf("the status read back differs:\n got %+v\nwant %+v", back.Status, status)
	}
	// An unknown figure has its reason and no value: the raw object has no
	// "duration" key in it, where a zero would have written "0s".
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Operation"))
	must(t, c.Get(bg(), objKey(op), u), "get unstructured")
	targets, _, _ := unstructured.NestedSlice(u.Object, "status", "plan", "targets")
	last, _ := targets[len(targets)-1].(map[string]any)
	cost, _ := last["cost"].(map[string]any)
	figure, _ := cost["estimateNow"].(map[string]any)
	if figure["unknown"] == nil || figure["time"] != nil {
		t.Errorf("the unknown estimate is stored as %v, want a reason and no time", figure)
	}
}

// The status subresource refuses a value outside an enum or a pattern.
func TestOperationStatusRefusesValuesOutsideItsEnums(t *testing.T) {
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	cases := []struct {
		name string
		edit func(*v1alpha1.OperationStatus)
		want string
	}{
		{"an unknown phase", func(s *v1alpha1.OperationStatus) { s.Phase = "Finished" }, "status.phase"},
		{"an unknown effect", func(s *v1alpha1.OperationStatus) { s.Plan.Targets[0].Effect = "Explode" }, "status.plan.targets[0].effect"},
		{"an unknown target kind", func(s *v1alpha1.OperationStatus) { s.Plan.Targets[0].Kind = "Provider" }, "status.plan.targets[0].kind"},
		{"an unknown outcome state", func(s *v1alpha1.OperationStatus) { s.Targets[0].State = "Pending" }, "status.targets[0].state"},
		{"a GPU-hours figure of two decimals", func(s *v1alpha1.OperationStatus) {
			s.Plan.Targets[0].Cost.GPUHoursLeft.GPUHours = "36.25"
		}, "status.plan.targets[0].cost.gpuHoursLeft.gpuHours"},
		{"a negative GPU-hours figure", func(s *v1alpha1.OperationStatus) {
			s.Plan.Targets[0].Cost.GPUHoursLeft.GPUHours = "-1.0"
		}, "status.plan.targets[0].cost.gpuHoursLeft.gpuHours"},
		{"an outcome message of 1025 characters", func(s *v1alpha1.OperationStatus) {
			s.Targets[0].Message = strings.Repeat("a", 1025)
		}, "status.targets[0].message"},
	}
	for _, tc := range cases {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			got := &v1alpha1.Operation{}
			must(t, c.Get(bg(), objKey(op), got), "get Operation")
			got.Status = fullOperationStatus()
			tc.edit(&got.Status)
			requireRefused(t, c.Status().Update(bg(), got), tc.want)
		})
	}
	t.Run("accept every plan effect", func(t *testing.T) {
		for _, effect := range allPlanEffects() {
			got := &v1alpha1.Operation{}
			must(t, c.Get(bg(), objKey(op), got), "get Operation")
			got.Status = fullOperationStatus()
			got.Status.Plan.Targets[0].Effect = effect
			if err := c.Status().Update(bg(), got); err != nil {
				t.Errorf("effect %s: %v", effect, err)
			}
		}
	})
}

// allPlanEffects lists the effects, written out by hand from HBEP-10
// (Operations, The plan) and the science agent's ruling that adds EndStarting,
// so a constant that goes missing from the enum fails here.
func allPlanEffects() []v1alpha1.PlanEffect {
	return []v1alpha1.PlanEffect{
		"CancelPending", "DrainInstance", "DrainLastReady", "EndStarting", "ReleaseKept", "ReleaseStarted", "Keep", "SetEndTime", "DeleteDeployment",
	}
}

// fullOperationStatus is a status that admission accepts, with every part
// filled. The times are literals, whole seconds, so what is read back equals
// what was sent.
func fullOperationStatus() v1alpha1.OperationStatus {
	observed := metav1.NewTime(time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC))
	finished := metav1.NewTime(time.Date(2026, 11, 15, 10, 5, 0, 0, time.UTC))
	cost := func() *v1alpha1.ReleaseCost {
		return &v1alpha1.ReleaseCost{
			LeaseLeft:     &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: 3 * time.Hour}, ObservedAt: &observed},
			GPUHoursLeft:  &v1alpha1.GPUHoursFigure{GPUHours: "36.0", ObservedAt: &observed},
			QueueAge:      &v1alpha1.DurationFigure{Unknown: "the Allocation has started", ObservedAt: &observed},
			StartEstimate: &v1alpha1.TimeFigure{Unknown: "the Provider gave no estimate", ObservedAt: &observed, Advisory: true},
			LastReady:     true,
			QueueWaits: &v1alpha1.QueueWaitsFigure{
				Median: &metav1.Duration{Duration: 30 * time.Minute}, P90: &metav1.Duration{Duration: 2 * time.Hour},
				Max: &metav1.Duration{Duration: 5 * time.Hour}, Count: 20, ObservedAt: &observed,
			},
			EstimateNow:  &v1alpha1.TimeFigure{Unknown: "the estimate operation is not built", Advisory: true},
			StartToReady: &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: 12 * time.Minute}, ObservedAt: &observed},
		}
	}
	var targets []v1alpha1.PlanTarget
	for i, effect := range allPlanEffects() {
		targets = append(targets, v1alpha1.PlanTarget{
			Kind: v1alpha1.TargetKindAllocation, Name: opAllocation, UID: types.UID("00000000-0000-0000-0000-00000000000" + string(rune('1'+i))),
			Generation: 1, Provider: "example-provider", Effect: effect, NeedsApproval: true, ApprovalRule: "release of a started Allocation",
			Policies: []v1alpha1.AppliedPolicy{{
				Name: "global", UID: "00000000-0000-0000-0000-0000000000a1", Generation: 2,
				Approvers: []v1alpha1.Approver{{User: "example-person"}, {Group: "example-group"},
					{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "example-agents", Name: "example-agent"}}},
			}},
			Cost: cost(),
		})
	}
	return v1alpha1.OperationStatus{
		Phase:              v1alpha1.OperationPhasePartlyFailed,
		ObservedGeneration: 1,
		Conditions: []metav1.Condition{{
			Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonExecutionNotBuilt,
			Message: "the controller plans Operations and does not execute them yet", LastTransitionTime: observed,
		}},
		Plan: &v1alpha1.OperationPlan{
			ID: opPlanID, ObservedAt: observed,
			ModelDeployment: v1alpha1.PlannedDeployment{Name: opDeployment, UID: "00000000-0000-0000-0000-0000000000b1", Generation: 4},
			Targets:         targets,
		},
		Committed: []v1alpha1.CommittedTarget{{
			Kind: v1alpha1.TargetKindAllocation, Name: opAllocation, UID: "00000000-0000-0000-0000-000000000001",
			Generation: 1, Effect: v1alpha1.PlanEffectReleaseStarted,
		}},
		Targets: []v1alpha1.TargetOutcome{{
			Kind: v1alpha1.TargetKindAllocation, Name: opAllocation, UID: "00000000-0000-0000-0000-000000000001",
			State: v1alpha1.TargetStateFailed, Reason: "CancelFailed", Message: "scancel was refused", LastTransitionTime: observed,
		}},
		FinishedAt: &finished,
	}
}

// The three kinds are served with the scopes and subresources HBEP-10 gives
// them: an Operation and an Approval are namespaced and an ApprovalPolicy is
// cluster-scoped, and only the Operation has a status subresource. This asks
// the API server's discovery, as kubectl api-resources does.
func TestAgentPrimitiveKindsAreServedWithTheirSubresources(t *testing.T) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	must(t, err, "discovery client")
	list, err := dc.ServerResourcesForGroupVersion(v1alpha1.GroupVersion.String())
	must(t, err, "discovery of "+v1alpha1.GroupVersion.String())
	got := map[string]metav1.APIResource{}
	for _, r := range list.APIResources {
		got[r.Name] = r
	}
	for name, namespaced := range map[string]bool{"operations": true, "approvals": true, "approvalpolicies": false} {
		r, ok := got[name]
		if !ok {
			t.Errorf("resource %s is not served", name)
			continue
		}
		if r.Namespaced != namespaced {
			t.Errorf("resource %s: namespaced=%v, want %v", name, r.Namespaced, namespaced)
		}
	}
	if _, ok := got["operations/status"]; !ok {
		t.Error("operations has no status subresource")
	}
	for _, name := range []string{"approvals/status", "approvalpolicies/status"} {
		if _, ok := got[name]; ok {
			t.Errorf("%s is served: the kind has no status subresource", name)
		}
	}
}

// EndStarting is a plan effect: an Instance still starting on a started
// Allocation, ended at once (HBEP-7, Reconcile loop, step 7). The status write
// is the admin's, as the lifecycle controller's would be. The enum has no CEL
// rule, so mutate-cel has no row for it; the near misses that are refused show
// the accepted value is in the enum and not a field the schema leaves open.
func TestPlanEffectEndStartingIsInTheEnum(t *testing.T) {
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	write := func(effect v1alpha1.PlanEffect) error {
		got := &v1alpha1.Operation{}
		must(t, c.Get(bg(), objKey(op), got), "get Operation")
		got.Status = fullOperationStatus()
		got.Status.Plan.Targets[0].Effect = effect
		got.Status.Plan.Targets[0].Kind = v1alpha1.TargetKindInstance
		got.Status.Committed[0].Effect = effect
		return c.Status().Update(bg(), got)
	}
	must(t, write("EndStarting"), "write a plan with the effect EndStarting")
	back := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), back), "get Operation")
	if got := back.Status.Plan.Targets[0].Effect; got != v1alpha1.PlanEffectEndStarting {
		t.Errorf("the stored effect is %q, want EndStarting", got)
	}
	if got := back.Status.Committed[0].Effect; got != v1alpha1.PlanEffectEndStarting {
		t.Errorf("the stored committed effect is %q, want EndStarting", got)
	}
	for _, near := range []v1alpha1.PlanEffect{"EndStart", "endStarting", "EndStartingNow", ""} {
		requireRefused(t, write(near), "Unsupported value")
	}
}

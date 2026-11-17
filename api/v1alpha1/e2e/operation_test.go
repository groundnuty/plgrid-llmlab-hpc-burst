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

// confirmWith is a setup that confirms the plan with this id. An Operation
// cannot be created already confirmed, so a case that starts from a confirmed
// one confirms it by an update first.
func confirmWith(planID string) func(*v1alpha1.Operation) {
	return func(op *v1alpha1.Operation) { op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID} }
}

// updateOperation creates the case's Operation, reads it back, runs setup (when
// there is one) as an update that must be accepted, and sends the edited copy as
// an update. It returns the stored object before the edit and the update's error.
func updateOperation(t *testing.T, ns string, tc opUpdate, setup func(*v1alpha1.Operation)) (*v1alpha1.Operation, error) {
	t.Helper()
	op := tc.base(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	stored := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), stored), "get Operation")
	if setup != nil {
		prepared := stored.DeepCopy()
		setup(prepared)
		must(t, c.Update(bg(), prepared), "update Operation into the case's starting state")
		must(t, c.Get(bg(), objKey(op), stored), "get Operation after the setup")
	}
	edited := stored.DeepCopy()
	tc.edit(edited)
	return stored, c.Update(bg(), edited)
}

// runOpUpdates runs each refused update, and checks that the stored spec is
// still the one created; then each accepted update.
func runOpUpdates(t *testing.T, refused, accepted []opUpdate) {
	t.Helper()
	runOpUpdatesAfter(t, nil, refused, accepted)
}

// runOpUpdatesAfter is runOpUpdates for cases that start from an Operation that
// setup has first updated, as confirmWith does.
func runOpUpdatesAfter(t *testing.T, setup func(*v1alpha1.Operation), refused, accepted []opUpdate) {
	t.Helper()
	ns := newNamespace(t)
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			stored, err := updateOperation(t, ns, tc, setup)
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
			_, err := updateOperation(t, ns, tc, setup)
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

// A Release selector, when there is one, names matchLabels or matchExpressions:
// an empty selector matches every object, so it would ask for every Allocation of
// the deployment. This is the rule operations:$.spec.release#2. The typed client
// omits an empty map and an empty list, so the cases that write them go through
// the unstructured client.
func TestReleaseSelectorIsNotEmpty(t *testing.T) {
	const msg = "name the Allocations, or give a selector with matchLabels or matchExpressions"
	ns := newNamespace(t)
	bySelector := func(selector map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": v1alpha1.GroupVersion.String(), "kind": "Operation",
			"metadata": map[string]any{"name": uniq("op"), "namespace": ns},
			"spec": map[string]any{
				"action": "Release", "owner": opOwner, "modelDeploymentRef": map[string]any{"name": opDeployment},
				"release": map[string]any{"selector": selector},
			},
		}}
	}
	expression := map[string]any{"key": "example", "operator": "In", "values": []any{"x"}}
	for _, tc := range []struct {
		name     string
		selector map[string]any
	}{
		{"a selector with nothing in it", map[string]any{}},
		{"a selector with an empty map and an empty list", map[string]any{"matchLabels": map[string]any{}, "matchExpressions": []any{}}},
		{"a selector with an empty map", map[string]any{"matchLabels": map[string]any{}}},
		{"a selector with an empty list", map[string]any{"matchExpressions": []any{}}},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, c.Create(bg(), bySelector(tc.selector)), msg)
		})
	}
	for _, tc := range []struct {
		name     string
		selector map[string]any
	}{
		{"a selector with a label", map[string]any{"matchLabels": map[string]any{"example": "x"}}},
		{"a selector with an expression", map[string]any{"matchExpressions": []any{expression}}},
		{"a selector with a label and an expression", map[string]any{
			"matchLabels": map[string]any{"example": "x"}, "matchExpressions": []any{expression}}},
		{"a selector with a label and an empty list", map[string]any{
			"matchLabels": map[string]any{"example": "x"}, "matchExpressions": []any{}}},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			must(t, c.Create(bg(), bySelector(tc.selector)), "create Operation")
		})
	}
	// The typed form of the first case: a non-nil selector with no fields.
	t.Run("refuse an empty selector from the typed client", func(t *testing.T) {
		op := newReleaseOperation(ns, uniq("op"))
		op.Spec.Release = &v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{}}
		requireRefused(t, c.Create(bg(), op), msg)
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
	}, []opCase{
		{"an owner of 1024 characters", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Owner = strings.Repeat("a", 1024)
		}), ""},
	})
}

// A confirm names a plan: its id is not empty and has at most 64 characters.
// The owner sets it by an update, since a create that sets it is refused.
func TestOperationConfirmNamesAPlan(t *testing.T) {
	const msg = "spec.confirm.planID"
	runOpUpdates(t, []opUpdate{
		{"an empty plan ID", newOperation, func(op *v1alpha1.Operation) { op.Spec.Confirm = &v1alpha1.OperationConfirm{} }, msg},
		{"a plan ID of 65 characters", newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: strings.Repeat("a", 65)}
		}, msg},
	}, []opUpdate{
		{"a plan ID of 64 characters", newOperation, confirmWith(strings.Repeat("a", 64)), ""},
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
// operations:$.spec#0. Every refusal starts from an Operation confirmed by an
// update, since a create cannot set confirm (TestOperationConfirmIsRefusedAtCreate).
func TestOperationConfirmIsSetOnce(t *testing.T) {
	const msg = "confirm is set once"
	runOpUpdatesAfter(t, confirmWith(opPlanID), []opUpdate{
		{"a changed plan ID", newOperation, func(op *v1alpha1.Operation) { op.Spec.Confirm.PlanID = opAnotherPlan }, msg},
		{"a removed confirm", newOperation, func(op *v1alpha1.Operation) { op.Spec.Confirm = nil }, msg},
		{"a changed plan ID on an End", newEndOperation, func(op *v1alpha1.Operation) { op.Spec.Confirm.PlanID = opAnotherPlan }, msg},
		{"a changed plan ID on a Release", newReleaseOperation, func(op *v1alpha1.Operation) { op.Spec.Confirm.PlanID = opAnotherPlan }, msg},
	}, []opUpdate{
		{"the same confirm again", newOperation, func(op *v1alpha1.Operation) { op.Annotations = map[string]string{"example": "x"} }, ""},
	})
	runOpUpdates(t, nil, []opUpdate{
		{"a first confirm", newOperation, confirmWith(opPlanID), ""},
		{"a first confirm on an End", newEndOperation, confirmWith(opPlanID), ""},
		{"a first confirm on a Release", newReleaseOperation, confirmWith(opPlanID), ""},
	})
}

// A create that sets confirm is refused: the owner confirms by an update, after
// the controller has written a plan (HBEP-10, Confirm: "absent until the owner
// confirms"). The set-once rule reads the old object, so it runs only on an
// update; this rule takes the old value as optional and runs on a create too. It
// is the rule operations:$.spec#7. Without it an agent could work out a plan id
// and create an Operation already confirmed, which would run, when no target
// needs approval, with nobody having read the plan. The same create without
// confirm is accepted, and so is the update that sets confirm for the first time.
func TestOperationConfirmIsRefusedAtCreate(t *testing.T) {
	const msg = "spec.confirm cannot be set when an Operation is created"
	withConfirm := func(base func(ns, name string) *v1alpha1.Operation) func(ns, name string) *v1alpha1.Operation {
		return withOperation(base, func(op *v1alpha1.Operation) { confirmWith(opPlanID)(op) })
	}
	runOpCases(t, []opCase{
		{"a Scale created confirmed", withConfirm(newOperation), msg},
		{"an End created confirmed", withConfirm(newEndOperation), msg},
		{"a Release created confirmed", withConfirm(newReleaseOperation), msg},
		{"a Scale created with a confirm of a bad plan ID", withOperation(newOperation, func(op *v1alpha1.Operation) {
			op.Spec.Confirm = &v1alpha1.OperationConfirm{}
		}), msg},
	}, []opCase{
		{"a Scale created unconfirmed", newOperation, ""},
		{"an End created unconfirmed", newEndOperation, ""},
		{"a Release created unconfirmed", newReleaseOperation, ""},
	})
	// The rule is for the create: the same Operation, confirmed by an update, is
	// accepted, which is what the owner does after reading the plan.
	runOpUpdates(t, nil, []opUpdate{
		{"a first confirm by update", newOperation, confirmWith(opPlanID), ""},
		{"a first confirm by update on an End", newEndOperation, confirmWith(opPlanID), ""},
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

// A Scale's record of the count it gave back is set once (HBEP-10, The report, as amended by #698). Two rules say so: the
// struct's transition rule, operations:$.status.countGivenBack#0, that a set record is not changed, and the parent's,
// operations:$.status#0, that it is not removed, since a transition rule on the field does not fire when the field is gone.
// A first write is accepted, with and without a count written, and the same value again is accepted.
func TestOperationCountGivenBackIsSetOnce(t *testing.T) {
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	two, three := int32(2), int32(3)
	write := func(edit func(*v1alpha1.OperationStatus)) error {
		got := &v1alpha1.Operation{}
		must(t, c.Get(bg(), objKey(op), got), "get Operation")
		edit(&got.Status)
		return c.Status().Update(bg(), got)
	}
	if err := write(func(s *v1alpha1.OperationStatus) {
		s.CountGivenBack = &v1alpha1.CountGivenBack{Units: 1, Instances: &two}
	}); err != nil {
		t.Fatalf("a first record is refused: %v", err)
	}
	if err := write(func(s *v1alpha1.OperationStatus) { s.Phase = v1alpha1.OperationPhaseExecuting }); err != nil {
		t.Errorf("an update that leaves the record as it is: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.OperationStatus)
		want string
	}{
		{"a changed count of units", func(s *v1alpha1.OperationStatus) { s.CountGivenBack.Units = 2 }, "status.countGivenBack is set once: it cannot be changed"},
		{"a changed count written", func(s *v1alpha1.OperationStatus) { s.CountGivenBack.Instances = &three }, "status.countGivenBack is set once: it cannot be changed"},
		{"a removed count written", func(s *v1alpha1.OperationStatus) { s.CountGivenBack.Instances = nil }, "status.countGivenBack is set once: it cannot be changed"},
		{"a removed record", func(s *v1alpha1.OperationStatus) { s.CountGivenBack = nil }, "status.countGivenBack is set once: it cannot be removed"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) { requireRefused(t, write(tc.edit), tc.want) })
	}
	other := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), other), "create another Operation")
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(other), got), "get Operation")
	got.Status.CountGivenBack = &v1alpha1.CountGivenBack{Units: 1} // a person's count stands: units, no count written
	must(t, c.Status().Update(bg(), got), "a first record with no count written")
	zero := int32(0)
	third := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), third), "create a third Operation")
	for _, bad := range []*v1alpha1.CountGivenBack{{Units: 0}, {Units: 1, Instances: &zero}} {
		g := &v1alpha1.Operation{}
		must(t, c.Get(bg(), objKey(third), g), "get Operation")
		g.Status.CountGivenBack = bad
		requireRefused(t, c.Status().Update(bg(), g), "status.countGivenBack")
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
			Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonExecuting,
			Message: "1 of 1 committed targets failed and are retried with backoff", LastTransitionTime: observed,
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

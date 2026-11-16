package e2e

// A field added AFTER the guard stays refused (issue #589, PR 3; team-lead, from #603). The capacity guard
// compares every field of the spec but the three it allowlists by iterating the map, so a field a later
// release adds to the CRD is refused for an agent with no change to the guard. The CRD's real fields cannot
// show that: they all exist when the guard is written, and a guard that listed them by name would pass the
// schema-completeness test as well. That is the excluded case, and this test makes it: it adds an optional
// property to the LIVE ModelDeployment CRD's schema (spec.h589FutureField, a string), waits until the
// CRD is Established, a List answers and the API server keeps the field (it would prune an unknown one),
// creates a ModelDeployment that sets it as agent A (a create may set any field), and shows that A's edit
// of it, and A's addition of it to a deployment that has none, are refused by the capacity guard. The
// person's same edits pass. Last it puts the CRD back exactly, compares the schema with the original, and
// waits again.
//
// Nothing in this package runs in parallel (no test calls t.Parallel), so no other test sees the changed
// schema. The mutant that makes the guard's spec rule list today's fields by name fails this test and no
// other.

import (
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const futureField = "h589FutureField"

// mdCRDVersion returns the served version's schema of the ModelDeployment CRD.
func mdCRDVersion(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceDefinitionVersion {
	t.Helper()
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == "v1alpha1" {
			return &crd.Spec.Versions[i]
		}
	}
	t.Fatal("the ModelDeployment CRD has no version v1alpha1")
	return nil
}

// crdServes reports whether the CRD is Established, a List answers, and a dry-run create that sets the
// future field returns it (an unknown field is pruned, so a schema that does not have it yet shows no field).
func crdServes(t *testing.T, ns string, wantField bool) (bool, error) {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(bg(), client.ObjectKey{Name: "modeldeployments." + frameworkGroup}, crd); err != nil {
		return false, err
	}
	established := false
	for _, cond := range crd.Status.Conditions {
		if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
			established = true
		}
	}
	if !established {
		return false, nil
	}
	if err := c.List(bg(), &v1alpha1.ModelDeploymentList{}, client.InNamespace(ns)); err != nil {
		return false, err
	}
	probe := futureMD(ns, uniq("md-probe"), "probe", "")
	got, err := dynamicOf(t).Resource(mdResource).Namespace(ns).Create(bg(), probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return false, err
	}
	v, found, _ := unstructured.NestedString(got.Object, "spec", futureField)
	return found == wantField && (!wantField || v == "probe"), nil
}

// futureMD is a ModelDeployment as unstructured, with the future field set when value is not empty, and
// the owner annotation when owner is not empty.
func futureMD(ns, name, value, owner string) *unstructured.Unstructured {
	md := newModelDeployment(ns, name)
	md.Spec.Instances = 3
	if owner != "" {
		md.Annotations = map[string]string{v1alpha1.AnnotationOwner: owner}
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(md)
	if err != nil {
		panic(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("ModelDeployment")
	if value != "" {
		_ = unstructured.SetNestedField(u.Object, value, "spec", futureField)
	}
	return u
}

func TestAFieldAddedToTheCRDAfterTheGuardIsRefusedForAnAgent(t *testing.T) {
	a := newAgents(t, "agent-a", "agent-b")[0]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)

	const crdName = "modeldeployments." + frameworkGroup
	original := &apiextensionsv1.CustomResourceDefinition{}
	must(t, c.Get(bg(), client.ObjectKey{Name: crdName}, original), "read the ModelDeployment CRD")
	originalSchema := mdCRDVersion(t, original).Schema.DeepCopy()
	if _, has := originalSchema.OpenAPIV3Schema.Properties["spec"].Properties[futureField]; has {
		t.Fatalf("the CRD already has a property %s", futureField)
	}

	// Put the CRD back exactly, whatever happens, and wait again.
	t.Cleanup(func() {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur := &apiextensionsv1.CustomResourceDefinition{}
			if err := c.Get(bg(), client.ObjectKey{Name: crdName}, cur); err != nil {
				return err
			}
			mdCRDVersion(t, cur).Schema = originalSchema.DeepCopy()
			return c.Update(bg(), cur)
		})
		if err != nil {
			t.Errorf("clean-up: put the ModelDeployment CRD back: %v", err)
			return
		}
		eventually(t, 60*time.Second, "the CRD to serve its original schema again", func() (bool, error) {
			ok, err := crdServes(t, ns, false)
			return ok, err
		})
		got := &apiextensionsv1.CustomResourceDefinition{}
		must(t, c.Get(bg(), client.ObjectKey{Name: crdName}, got), "read the CRD after the clean-up")
		if !apiequality.Semantic.DeepEqual(mdCRDVersion(t, got).Schema, originalSchema) {
			t.Errorf("the ModelDeployment CRD's schema after the clean-up is not the original")
		}
	})

	// Add the property to the live schema, and wait until it is served.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &apiextensionsv1.CustomResourceDefinition{}
		if err := c.Get(bg(), client.ObjectKey{Name: crdName}, cur); err != nil {
			return err
		}
		specSchema := mdCRDVersion(t, cur).Schema.OpenAPIV3Schema.Properties["spec"]
		specSchema.Properties[futureField] = apiextensionsv1.JSONSchemaProps{Type: "string"}
		mdCRDVersion(t, cur).Schema.OpenAPIV3Schema.Properties["spec"] = specSchema
		return c.Update(bg(), cur)
	})
	must(t, err, "add "+futureField+" to the ModelDeployment CRD")
	eventually(t, 60*time.Second, "the CRD to be Established and to keep "+futureField, func() (bool, error) { return crdServes(t, ns, true) })

	// An agent's create may set any field: it sets the future one. The person makes a second deployment
	// that has none.
	requireAgentIdentity(t, a)
	dyn := a.dyn.Resource(mdResource).Namespace(ns)
	with := futureMD(ns, uniq("md-with"), "first", a.username())
	_, err = dyn.Create(bg(), with, metav1.CreateOptions{})
	must(t, err, "the agent's create of a deployment that sets "+futureField)
	without := futureMD(ns, uniq("md-without"), "", a.username())
	_, err = dyn.Create(bg(), without, metav1.CreateOptions{})
	must(t, err, "the agent's create of a deployment that does not")
	stored := func(name string) (string, bool) {
		got, err := dynamicOf(t).Resource(mdResource).Namespace(ns).Get(bg(), name, metav1.GetOptions{})
		must(t, err, "read "+name)
		v, found, _ := unstructured.NestedString(got.Object, "spec", futureField)
		return v, found
	}
	if v, found := stored(with.GetName()); !found || v != "first" {
		t.Fatalf("the stored deployment has %s = %q (%v), want first: the schema does not keep the field", futureField, v, found)
	}

	// The agent's edit of it, and its addition of it, are refused by the capacity guard, with the rule's
	// message; the stored object keeps its value.
	for _, tc := range []struct{ what, name, patch string }{
		{"the edit of the future field", with.GetName(), `{"spec":{"` + futureField + `":"second"}}`},
		{"the removal of the future field", with.GetName(), `{"spec":{"` + futureField + `":null}}`},
		{"the addition of the future field", without.GetName(), `{"spec":{"` + futureField + `":"added"}}`},
	} {
		_, err := dyn.Patch(bg(), tc.name, types.MergePatchType, []byte(tc.patch), metav1.PatchOptions{})
		requireDenied(t, "the agent's "+tc.what, err, capacityGuard, "may change only spec.instances")
	}
	if v, found := stored(with.GetName()); !found || v != "first" {
		t.Errorf("after the refused edits %s = %q (%v), want first", futureField, v, found)
	}
	if _, found := stored(without.GetName()); found {
		t.Errorf("after the refused addition the deployment has %s", futureField)
	}

	// The same edits from the person pass (dry runs: the person is outside the guard).
	for _, tc := range []struct{ what, name, patch string }{
		{"the edit", with.GetName(), `{"spec":{"` + futureField + `":"second"}}`},
		{"the addition", without.GetName(), `{"spec":{"` + futureField + `":"added"}}`},
	} {
		_, err := person.dyn.Resource(mdResource).Namespace(ns).Patch(bg(), tc.name, types.MergePatchType, []byte(tc.patch), metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
		requireAccepted(t, "the person's "+tc.what+" of the future field (a dry run)", err)
	}
}

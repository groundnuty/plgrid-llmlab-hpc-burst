package e2e

import (
	"bytes"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const policyName = "hpcburst-protect-crds"

// installGuard applies config/admission/crd-deletion-guard.yaml and returns the
// binding, whose removal is the explicit uninstall step.
func installGuard(t *testing.T) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	t.Helper()
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(readAll(t, configDir("admission")+"/crd-deletion-guard.yaml")), 4096)
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	must(t, dec.Decode(policy), "decode the policy")
	must(t, dec.Decode(binding), "decode the binding")
	if policy.Name != policyName || binding.Spec.PolicyName != policyName {
		t.Fatalf("the manifest names %q / %q, want %q", policy.Name, binding.Spec.PolicyName, policyName)
	}
	must(t, c.Create(bg(), policy), "create the policy")
	t.Cleanup(func() { _ = c.Delete(bg(), policy) })
	must(t, c.Create(bg(), binding), "create the binding")
	t.Cleanup(func() { _ = c.Delete(bg(), binding) })
	return binding
}

// dryRunDelete deletes crd with the dry-run flag: admission runs, nothing is
// removed. A real delete that slipped past a policy that is not yet active would
// remove a CRD of this suite.
func dryRunDelete(name string) error {
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return c.Delete(bg(), crd, client.DryRunAll)
}

// The deletion guard (HBEP-11, Admission): deleting a framework CRD is refused
// while the policy is bound, an unrelated CRD can still be deleted, and removing
// the binding is the explicit uninstall step that lets deletion through.
func TestCRDDeletionGuard(t *testing.T) {
	const ours = "providers.hpcburst.groundnuty.github.io"

	// A CRD of another group, to show the guard is scoped to ours.
	other := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.other.example.invalid"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "other.example.invalid", Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "Widget", Plural: "widgets", ListKind: "WidgetList", Singular: "widget"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"}}}},
		},
	}
	must(t, c.Create(bg(), other), "create the unrelated CRD")
	cleanup(t, other)

	// On a host whose launcher installs the policy (HBEP-11), it is already
	// there. The test then neither creates nor removes it, and skips the two
	// steps that need it absent.
	hostInstalled := false
	if err := c.Get(bg(), client.ObjectKey{Name: policyName}, &admissionregistrationv1.ValidatingAdmissionPolicy{}); err == nil {
		hostInstalled = true
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("look for the policy: %v", err)
	}

	var binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding
	if hostInstalled {
		t.Log("the host installed the deletion guard; the before and after steps are skipped")
	} else {
		// Before the policy exists, a dry-run delete of our CRD is accepted: the
		// refusal below is the policy's doing, not something else's.
		must(t, dryRunDelete(ours), "dry-run delete of a framework CRD before the policy exists")
		binding = installGuard(t)
	}

	// Policies take effect asynchronously: poll with dry-run deletes.
	var refusal error
	eventually(t, 60*time.Second, "the policy to refuse deleting a framework CRD", func() (bool, error) {
		refusal = dryRunDelete(ours)
		return refusal != nil, nil
	})
	if !apierrors.IsForbidden(refusal) {
		t.Fatalf("want a Forbidden refusal, got %T: %v", refusal, refusal)
	}
	for _, name := range []string{
		"providers", "platformprofiles", "modeldeployments", "allocations", "instances"} {
		err := dryRunDelete(name + ".hpcburst.groundnuty.github.io")
		if !apierrors.IsForbidden(err) {
			t.Errorf("deleting the %s CRD: want Forbidden, got %v", name, err)
		}
	}
	if err := refusal.Error(); !bytes.Contains([]byte(err), []byte("hpcburst-protect-crds")) {
		t.Errorf("the refusal does not say how to proceed: %s", err)
	}

	// The guard is scoped: a CRD of another group deletes, for real.
	must(t, dryRunDelete(other.Name), "dry-run delete of an unrelated CRD")
	must(t, c.Delete(bg(), other), "delete the unrelated CRD")

	// Uninstall order: remove the binding first. Then deletion goes through (dry run).
	if binding != nil {
		must(t, c.Delete(bg(), binding), "remove the binding")
		eventually(t, 60*time.Second, "deletion of a framework CRD to be allowed again", func() (bool, error) {
			return dryRunDelete(ours) == nil, nil
		})
	}

	// The suite's CRDs are all still there.
	for _, name := range []string{ours} {
		must(t, c.Get(bg(), client.ObjectKey{Name: name}, &apiextensionsv1.CustomResourceDefinition{}), "CRD "+name)
	}
}

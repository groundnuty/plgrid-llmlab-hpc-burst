package v1alpha1

import (
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
)

// Compile-time assertions: Allocation is a crossplane-runtime managed resource
// that the provider's managed reconciler accepts (HBEP-4, HBEP-11). If an
// accessor is missing, this file does not compile and go vet fails.
var (
	_ resource.ModernManaged          = (*Allocation)(nil)
	_ resource.ReconciliationObserver = (*Allocation)(nil)
)

// The accessors forward to the embedded crossplane fields. A forwarding method
// that wrote to a copy would still compile, so this exercises each pair.
func TestAllocationAccessorsForwardToTheEmbeddedFields(t *testing.T) {
	a := &Allocation{}

	ref := &xpv2.ProviderConfigReference{Kind: ProviderConfigKind, Name: "helios-a"}
	a.SetProviderConfigReference(ref)
	if a.Spec.ProviderConfigReference != ref || a.GetProviderConfigReference() != ref {
		t.Errorf("provider config reference not forwarded: %+v", a.Spec.ProviderConfigReference)
	}

	secret := &xpv2.LocalSecretReference{Name: "conn"}
	a.SetWriteConnectionSecretToReference(secret)
	if a.Spec.WriteConnectionSecretToReference != secret || a.GetWriteConnectionSecretToReference() != secret {
		t.Errorf("connection secret reference not forwarded: %+v", a.Spec.WriteConnectionSecretToReference)
	}

	policies := xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate}
	a.SetManagementPolicies(policies)
	if len(a.Spec.ManagementPolicies) != 2 || len(a.GetManagementPolicies()) != 2 || a.GetManagementPolicies()[1] != xpv2.ManagementActionCreate {
		t.Errorf("management policies not forwarded: %v", a.Spec.ManagementPolicies)
	}

	a.SetObservedGeneration(7)
	if a.Status.ObservedGeneration != 7 || a.GetObservedGeneration() != 7 {
		t.Errorf("observed generation not forwarded: %d", a.Status.ObservedGeneration)
	}

	a.SetConditions(xpv2.Available())
	if got := a.GetCondition(xpv2.TypeReady); got.Status != xpv2.Available().Status {
		t.Errorf("condition not forwarded: %+v", got)
	}
	if got := a.Status.GetCondition(xpv2.TypeReady); got.Reason != xpv2.ReasonAvailable {
		t.Errorf("condition not stored on the status: %+v", got)
	}
}

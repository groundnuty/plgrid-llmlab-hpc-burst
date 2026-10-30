package v1alpha1

import (
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
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

	a.SetObservedGeneration(0)
	a.SetLastHandledReconcileAt("2026-10-30T12:00:00Z")
	if a.Status.LastHandledReconcileAt != "2026-10-30T12:00:00Z" || a.GetLastHandledReconcileAt() != "2026-10-30T12:00:00Z" {
		t.Errorf("reconcile token not forwarded: %q", a.Status.LastHandledReconcileAt)
	}

	a.SetConditions(xpv2.Available())
	if got := a.GetCondition(xpv2.TypeReady); got.Status != xpv2.Available().Status {
		t.Errorf("condition not forwarded: %+v", got)
	}
	if len(a.Status.Conditions) != 1 || a.Status.Conditions[0].Reason != xpv2.ReasonAvailable {
		t.Errorf("condition not stored on the status: %+v", a.Status.Conditions)
	}
	// A condition of the same type replaces the old one; another type is added.
	a.SetConditions(xpv2.Unavailable(), xpv2.ReconcileSuccess())
	if got := a.GetCondition(xpv2.TypeReady); got.Reason != xpv2.ReasonUnavailable {
		t.Errorf("the Ready condition was not replaced: %+v", got)
	}
	if len(a.Status.Conditions) != 2 {
		t.Errorf("conditions = %+v, want one per type", a.Status.Conditions)
	}
	// A type that was never set reads as Unknown, not as a zero value.
	if got := (&Allocation{}).GetCondition(xpv2.TypeSynced); got.Status != corev1.ConditionUnknown {
		t.Errorf("an unset condition = %+v, want Unknown", got)
	}
}

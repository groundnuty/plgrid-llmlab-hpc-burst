package v1alpha1

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
)

// The accessors that make *Allocation a crossplane-runtime managed resource
// (resource.ModernManaged and resource.ReconciliationObserver). Embedding the
// crossplane structures in the Spec and Status fields does not expose their
// methods on Allocation itself, so each one forwards. The shape follows spike
// S4 (spikes/s4/apis/v1alpha1/allocation.go). The compile-time assertions that
// the set is complete are in crossplane_test.go, which keeps crossplane-runtime
// out of the import graph of every consumer of this package.

// GetCondition of this Allocation.
func (a *Allocation) GetCondition(ct xpv2.ConditionType) xpv2.Condition {
	return a.Status.GetCondition(ct)
}

// SetConditions of this Allocation.
func (a *Allocation) SetConditions(c ...xpv2.Condition) { a.Status.SetConditions(c...) }

// GetManagementPolicies of this Allocation.
func (a *Allocation) GetManagementPolicies() xpv2.ManagementPolicies {
	return a.Spec.ManagementPolicies
}

// SetManagementPolicies of this Allocation.
func (a *Allocation) SetManagementPolicies(p xpv2.ManagementPolicies) {
	a.Spec.ManagementPolicies = p
}

// GetProviderConfigReference of this Allocation.
func (a *Allocation) GetProviderConfigReference() *xpv2.ProviderConfigReference {
	return a.Spec.ProviderConfigReference
}

// SetProviderConfigReference of this Allocation.
func (a *Allocation) SetProviderConfigReference(p *xpv2.ProviderConfigReference) {
	a.Spec.ProviderConfigReference = p
}

// GetWriteConnectionSecretToReference of this Allocation.
func (a *Allocation) GetWriteConnectionSecretToReference() *xpv2.LocalSecretReference {
	return a.Spec.WriteConnectionSecretToReference
}

// SetWriteConnectionSecretToReference of this Allocation.
func (a *Allocation) SetWriteConnectionSecretToReference(r *xpv2.LocalSecretReference) {
	a.Spec.WriteConnectionSecretToReference = r
}

// GetObservedGeneration of this Allocation.
func (a *Allocation) GetObservedGeneration() int64 { return a.Status.GetObservedGeneration() }

// SetObservedGeneration of this Allocation.
func (a *Allocation) SetObservedGeneration(g int64) { a.Status.SetObservedGeneration(g) }

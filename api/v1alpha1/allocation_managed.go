package v1alpha1

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
)

// The accessors that make *Allocation a crossplane-runtime managed resource
// (resource.ModernManaged and resource.ReconciliationObserver). The Spec and
// Status fields declare crossplane's fields with its types and JSON names, and
// each accessor reads or writes one. The shape follows spike
// S4 (spikes/s4/apis/v1alpha1/allocation.go). The compile-time assertions that
// the set is complete are in crossplane_test.go, which keeps crossplane-runtime
// out of the import graph of every consumer of this package.

// GetCondition of this Allocation.
func (a *Allocation) GetCondition(ct xpv2.ConditionType) xpv2.Condition {
	cs := xpv2.ConditionedStatus{Conditions: a.Status.Conditions}
	return cs.GetCondition(ct)
}

// SetConditions of this Allocation. Like crossplane's own, it replaces the
// condition of the same type and keeps the list sorted by type.
func (a *Allocation) SetConditions(c ...xpv2.Condition) {
	cs := xpv2.ConditionedStatus{Conditions: a.Status.Conditions}
	cs.SetConditions(c...)
	a.Status.Conditions = cs.Conditions
}

// GetManagementPolicies of this Allocation.
func (a *Allocation) GetManagementPolicies() xpv2.ManagementPolicies {
	return xpv2.ManagementPolicies(a.Spec.ManagementPolicies)
}

// SetManagementPolicies of this Allocation.
func (a *Allocation) SetManagementPolicies(p xpv2.ManagementPolicies) {
	a.Spec.ManagementPolicies = []xpv2.ManagementAction(p)
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
func (a *Allocation) GetObservedGeneration() int64 { return a.Status.ObservedGeneration }

// SetObservedGeneration of this Allocation.
func (a *Allocation) SetObservedGeneration(g int64) { a.Status.ObservedGeneration = g }

// GetLastHandledReconcileAt of this Allocation.
func (a *Allocation) GetLastHandledReconcileAt() string { return a.Status.LastHandledReconcileAt }

// SetLastHandledReconcileAt of this Allocation.
func (a *Allocation) SetLastHandledReconcileAt(token string) { a.Status.LastHandledReconcileAt = token }

// Package v1alpha1 is a stand-in for the real api/v1alpha1, with the reason
// constants the scan test resolves.
package v1alpha1

// Typed is a named string type, as xpv2.ConditionReason is.
type Typed string

const (
	ReasonA            = "A"
	ReasonB            = "B"
	ReasonC            = "C"
	EventReasonD       = "D"
	ReasonTyped  Typed = "Typed"

	// NotAReason is no Reason constant by its name, so the scan ignores it.
	ConditionNotAReason = "NotAReason"
)

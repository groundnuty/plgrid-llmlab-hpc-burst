// Package managed is a stand-in for the real internal/managed: the variables the
// scan reads, and every way the real controller writes a condition.
package managed

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

// Condition stands for metav1.Condition.
type Condition struct{ Type, Status, Reason, Message string }

// SetStatusCondition stands for meta.SetStatusCondition.
func SetStatusCondition(conds *[]Condition, c Condition) {}

var watched = []struct {
	typ string
	bad bool
}{
	{v1alpha1.ConditionWatched, true},
}

var badAction = map[string]string{
	v1alpha1.ReasonA: "failing",
}

// direct writes a watched condition with a constant.
func direct(conds *[]Condition) {
	SetStatusCondition(conds, Condition{Type: v1alpha1.ConditionWatched, Reason: v1alpha1.ReasonA})
}

// viaClosure writes through a closure that is only called, as apply does. The
// second call writes a condition that is not watched, but the closure's one
// call of the setter can write either: the reasons are an over-approximation.
func viaClosure(conds *[]Condition) {
	cond := func(typ, status, reason string) {
		SetStatusCondition(conds, Condition{Type: typ, Status: status, Reason: reason})
	}
	cond(v1alpha1.ConditionWatched, "True", v1alpha1.ReasonB)
	cond(v1alpha1.ConditionOther, "True", v1alpha1.ReasonC)
}

// viaVariable fills a variable field by field, as SetManagedReadiness does.
func viaVariable(conds *[]Condition, ok bool) {
	c := Condition{Type: v1alpha1.ConditionWatched}
	if ok {
		c.Reason = v1alpha1.ReasonC
	} else {
		c.Reason = v1alpha1.EventReasonD
	}
	SetStatusCondition(conds, c)
}

// notWatched writes a condition of another type: it contributes nothing.
func notWatched(conds *[]Condition) {
	SetStatusCondition(conds, Condition{Type: v1alpha1.ConditionOther, Reason: string(v1alpha1.ReasonTyped)})
}

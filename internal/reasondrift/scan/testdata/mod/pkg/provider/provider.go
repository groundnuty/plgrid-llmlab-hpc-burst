// Package provider is a stand-in for the real pkg/provider: a constructor of an
// event, which raises nothing until a recorder receives the value.
package provider

// ManagedEvent is an event ready for a recorder.
type ManagedEvent struct{ Type, Reason, Action, Note string }

// NewManagedEvent builds an event.
func NewManagedEvent(action, reason, note string) ManagedEvent {
	return ManagedEvent{Type: "Normal", Reason: reason, Action: action, Note: note}
}

// GetName makes a ManagedEvent usable where a regarding object is wanted, so a
// test can pass one at the wrong parameter.
func (ManagedEvent) GetName() string { return "" }

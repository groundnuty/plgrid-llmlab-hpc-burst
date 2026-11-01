package v1alpha1

import "testing"

// The names other components match exactly, written out here so a rename shows.
func TestNamesForTheLifecycleControllerAndTheFront(t *testing.T) {
	for got, want := range map[string]string{
		LeaseFront:               "front",
		ReasonCatalogEntryListed: "CatalogEntryListed",
		ReasonAgentRegistered:    "AgentRegistered",
		EventReasonShapesFit:     "ShapesFit",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if LeaseFront == LeaseLifecycle {
		t.Errorf("the front's Lease and the lifecycle controller's Lease share the name %q", LeaseFront)
	}
}

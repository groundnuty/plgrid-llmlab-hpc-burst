package v1alpha1

import "testing"

// The names other components match exactly, written out here so a rename shows.
func TestNamesForTheLifecycleControllerAndTheFront(t *testing.T) {
	for got, want := range map[string]string{
		LeaseFront:               "front",
		ReasonCatalogEntryListed: "CatalogEntryListed",
		ReasonAgentRegistered:    "AgentRegistered",
		EventReasonShapesFit:     "ShapesFit",
		// HBEP-8's Reasons table spells it TunnelOrphaned.
		EventReasonTunnelOrphaned: "TunnelOrphaned",
		// HBEP-7, Hedging (beta): the hedge loser's end reason and replaced event's reason, and the hedge's annotation.
		ReasonHedgeLost:      "HedgeLost",
		AnnotationHedgeGroup: "hpcburst.groundnuty.github.io/hedge-group",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if LeaseFront == LeaseLifecycle {
		t.Errorf("the front's Lease and the lifecycle controller's Lease share the name %q", LeaseFront)
	}
}

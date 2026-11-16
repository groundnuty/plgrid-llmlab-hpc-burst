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
		// HBEP-7, Over-allocation and qualification (beta): the ranked-out Instance's end reason and replaced event's
		// reason, and the suspended expected-performance entry's degraded event's reason (HBEP-8's Reasons table).
		ReasonRankedOut:        "RankedOut",
		ReasonReferenceSuspect: "ReferenceSuspect",
		// HBEP-7, The throughput gate: the gate's four verdicts, written on Instances and start-up outcomes.
		GatePassed:     "Passed",
		GateMissed:     "Missed",
		GateSuspended:  "Suspended",
		GateRecordOnly: "RecordOnly",
		// HBEP-4, The remaining groups (beta): the billing units and the profile's dial-home modes, as the
		// shipped profiles spell them, and the accounting record's beta fields as its unknown list names them
		// (HBEP-8, Provider accounting, Billing).
		string(BillingUnitGPUHour):        "gpu-hour",
		string(BillingUnitCPUHour):        "cpu-hour",
		string(ProfileDialHomeSSH):        "ssh",
		string(ProfileDialHomeHostDialed): "host-dialed",
		AccountingFieldAllocatedBilling:   "allocatedBilling",
		AccountingFieldBillingUnit:        "billingUnit",
		AccountingFieldGrant:              "grant",
		AccountingFieldAccount:            "account",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if LeaseFront == LeaseLifecycle {
		t.Errorf("the front's Lease and the lifecycle controller's Lease share the name %q", LeaseFront)
	}
}

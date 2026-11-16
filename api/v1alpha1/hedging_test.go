package v1alpha1

import "testing"

// HBEP-7, What the controller reads: hedgeWidth is 2 by default; a class keeps its last 20 start-up outcomes. The
// values are written out here, not read from the constants they check.
func TestHedgeWidthCountAndTheOutcomeBounds(t *testing.T) {
	var s ModelDeploymentSpec
	if got := s.HedgeWidthCount(); got != 2 {
		t.Errorf("an absent hedgeWidth counts %d, want 2", got)
	}
	one := int32(1)
	s.HedgeWidth = &one
	if got := s.HedgeWidthCount(); got != 1 {
		t.Errorf("hedgeWidth 1 counts %d, want 1", got)
	}
	if MaxStartOutcomes != 20 || StartOutcomeReady != "Ready" || StartOutcomeFailed != "Failed" {
		t.Errorf("MaxStartOutcomes %d, results %q and %q; want 20, Ready and Failed", MaxStartOutcomes, StartOutcomeReady, StartOutcomeFailed)
	}
}

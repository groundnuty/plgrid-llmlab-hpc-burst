package v1alpha1

import "testing"

// HBEP-7, What the controller reads: overAllocation is 1 by default and 0 turns it off; HBEP-4, Expected
// performance: an entry's tolerance is 100 percent by default. The values are written out here, not read from the
// constants they check.
func TestOverAllocationCountAndTolerancePercentDefaults(t *testing.T) {
	var s ModelDeploymentSpec
	if got := s.OverAllocationCount(); got != 1 {
		t.Errorf("an absent overAllocation counts %d, want 1", got)
	}
	for _, k := range []int32{0, 1} {
		s.OverAllocation = &k
		if got := s.OverAllocationCount(); got != k {
			t.Errorf("overAllocation %d counts %d", k, got)
		}
	}

	var e ExpectedPerformance
	if got := e.TolerancePercentOf(); got != 100 {
		t.Errorf("an absent tolerancePercent is %d, want 100", got)
	}
	for _, p := range []int32{10, 50, 400} {
		e.TolerancePercent = &p
		if got := e.TolerancePercentOf(); got != p {
			t.Errorf("tolerancePercent %d is %d", p, got)
		}
	}
}

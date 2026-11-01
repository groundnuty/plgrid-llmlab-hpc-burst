package provider

import (
	"errors"
	"fmt"
	"testing"
)

// Only Slurm's three submit-count reason names are a full submit limit
// (HBEP-4, QOS limits); every other policy reason, a stand-alone breach
// included, is not. The names are written out by hand from HBEP-4.
func TestIsSubmitLimitReason(t *testing.T) {
	for _, name := range []string{"QOSMaxSubmitJobPerUserLimit", "AssocMaxSubmitJobLimit", "MaxSubmitJobsPerAccount"} {
		if !IsSubmitLimitReason(name) {
			t.Errorf("%s: not a submit limit, want one", name)
		}
	}
	for _, name := range []string{
		"QOSMaxWallDurationPerJobLimit", "QOSMaxGRESPerUser", "AssocGrpBillingMinutes", "QOSMaxSubmitJobPerUserLimitX",
		"qosmaxsubmitjobperuserlimit", " QOSMaxSubmitJobPerUserLimit", "",
	} {
		if IsSubmitLimitReason(name) {
			t.Errorf("%q: a submit limit, want none", name)
		}
	}
}

// A caller finds a policy refusal through any wrapping, and classifies it by
// its reason name; the text is what Slurm printed.
func TestPolicyRefusalIsFoundThroughWrapping(t *testing.T) {
	full := &PolicyRefusal{Reason: "QOSMaxSubmitJobPerUserLimit", Line: "sbatch: error: QOSMaxSubmitJobPerUserLimit"}
	other := &PolicyRefusal{Reason: "QOSMaxWallDurationPerJobLimit"}
	for _, tc := range []struct {
		name     string
		err      error
		found    bool
		limit    bool
		wantText string
	}{
		{"a full submit limit", fmt.Errorf("attempt 3: %w", full), true, true, full.Line},
		{"another policy reason, no line given", fmt.Errorf("sbatch: %w", other), true, false, "sbatch: error: QOSMaxWallDurationPerJobLimit"},
		{"a refusal with no reason line", errors.New("sbatch: Batch job submission failed: Invalid account"), false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pr *PolicyRefusal
			if got := errors.As(tc.err, &pr); got != tc.found {
				t.Fatalf("errors.As = %v, want %v", got, tc.found)
			}
			if !tc.found {
				return
			}
			if pr.SubmitLimit() != tc.limit {
				t.Errorf("SubmitLimit = %v, want %v", pr.SubmitLimit(), tc.limit)
			}
			if pr.Error() != tc.wantText {
				t.Errorf("Error() = %q, want %q", pr.Error(), tc.wantText)
			}
		})
	}
}

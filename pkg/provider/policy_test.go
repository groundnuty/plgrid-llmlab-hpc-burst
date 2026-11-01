package provider

import (
	"errors"
	"fmt"
	"testing"
)

// Only Slurm's five submit-count reason names are a full submit limit
// (HBEP-4, QOS limits; the two group names from PR #155's review); every other
// policy reason, a stand-alone breach included, is not. The names are written
// out by hand.
func TestIsSubmitLimitReason(t *testing.T) {
	for _, tc := range []struct {
		name  string
		group bool
	}{
		{"QOSMaxSubmitJobPerUserLimit", false}, {"AssocMaxSubmitJobLimit", false}, {"MaxSubmitJobsPerAccount", false},
		{"QOSGrpSubmitJobsLimit", true}, {"AssocGrpSubmitJobsLimit", true},
	} {
		if !IsSubmitLimitReason(tc.name) {
			t.Errorf("%s: not a submit limit, want one", tc.name)
		}
		if IsGroupSubmitLimitReason(tc.name) != tc.group {
			t.Errorf("%s: group = %v, want %v", tc.name, !tc.group, tc.group)
		}
	}
	for _, name := range []string{
		"QOSMaxWallDurationPerJobLimit", "QOSMaxGRESPerUser", "AssocGrpBillingMinutes", "QOSMaxSubmitJobPerUserLimitX",
		"QOSGrpJobsLimit", "AssocGrpJobsLimit", "qosmaxsubmitjobperuserlimit", " QOSMaxSubmitJobPerUserLimit", "",
	} {
		if IsSubmitLimitReason(name) || IsGroupSubmitLimitReason(name) {
			t.Errorf("%q: a submit limit, want none", name)
		}
	}
}

// A group submit limit's message says the slot is shared by the grant's users;
// a per-user one's does not (`@hpc-burst-science-agent`, 1 Nov 2026).
func TestGroupSubmitLimitMessageNamesTheGrant(t *testing.T) {
	for _, tc := range []struct {
		pr   PolicyRefusal
		want string
	}{
		{PolicyRefusal{Reason: "QOSGrpSubmitJobsLimit", Line: "sbatch: error: QOSGrpSubmitJobsLimit"},
			"sbatch: error: QOSGrpSubmitJobsLimit (group limit, shared by the grant's users)"},
		{PolicyRefusal{Reason: "AssocGrpSubmitJobsLimit"},
			"sbatch: error: AssocGrpSubmitJobsLimit (group limit, shared by the grant's users)"},
		{PolicyRefusal{Reason: "QOSMaxSubmitJobPerUserLimit", Line: "sbatch: error: QOSMaxSubmitJobPerUserLimit"},
			"sbatch: error: QOSMaxSubmitJobPerUserLimit"},
	} {
		if got := tc.pr.Error(); got != tc.want {
			t.Errorf("%s: Error() = %q, want %q", tc.pr.Reason, got, tc.want)
		}
		if !tc.pr.SubmitLimit() {
			t.Errorf("%s: SubmitLimit() = false, want true", tc.pr.Reason)
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

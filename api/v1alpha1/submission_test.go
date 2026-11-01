package v1alpha1

import (
	"strings"
	"testing"
)

// A submission record's code goes with a Rejected attempt only, and is
// SubmitLimitReached or absent (review of PR #155, N6); the attempt counts from
// 1 and the state is one of the five.
func TestSubmissionRecordValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    SubmissionRecord
		want string // "" accepts; else a fragment of the error
	}{
		{"a Rejected attempt with the code", SubmissionRecord{Attempt: 2, State: SubmissionRejected, Code: ReasonSubmitLimitReached}, ""},
		{"a Rejected attempt without a code", SubmissionRecord{Attempt: 1, State: SubmissionRejected}, ""},
		{"an InFlight claim", SubmissionRecord{Attempt: 3, State: SubmissionInFlight}, ""},
		{"Done", SubmissionRecord{Attempt: 1, State: SubmissionDone}, ""},
		{"Unknown", SubmissionRecord{Attempt: 1, State: SubmissionUnknown}, ""},
		{"Abandoned", SubmissionRecord{Attempt: 1, State: SubmissionAbandoned}, ""},
		{"the code on a Done attempt", SubmissionRecord{Attempt: 1, State: SubmissionDone, Code: ReasonSubmitLimitReached}, "on a Rejected attempt only"},
		{"the code on an Unknown attempt", SubmissionRecord{Attempt: 1, State: SubmissionUnknown, Code: ReasonSubmitLimitReached}, "on a Rejected attempt only"},
		{"the code on an InFlight claim", SubmissionRecord{Attempt: 1, State: SubmissionInFlight, Code: ReasonSubmitLimitReached}, "on a Rejected attempt only"},
		{"another code", SubmissionRecord{Attempt: 1, State: SubmissionRejected, Code: "SubmissionRejected"}, "is not SubmitLimitReached"},
		{"attempt 0", SubmissionRecord{Attempt: 0, State: SubmissionRejected}, "attempts count from 1"},
		{"an unknown state", SubmissionRecord{Attempt: 1, State: "Pending"}, "is not one of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("Validate = %v, want nil", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("Validate = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

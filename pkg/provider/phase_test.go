package provider

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The table is HBEP-4's (Core operations, Phases), written out by hand.
func TestSlurmPhase(t *testing.T) {
	for _, tc := range []struct {
		state    []string
		held     bool
		requeued bool
		want     v1alpha1.AllocationPhase
	}{
		{[]string{"PENDING"}, false, false, v1alpha1.AllocationPending},
		{[]string{"PENDING"}, true, false, v1alpha1.AllocationHeld},
		{[]string{"RUNNING"}, false, false, v1alpha1.AllocationRunning},
		{[]string{"RUNNING", "COMPLETING"}, false, false, v1alpha1.AllocationRunning},
		{[]string{"SUSPENDED"}, false, false, v1alpha1.AllocationRunning},
		{[]string{"COMPLETED"}, false, false, v1alpha1.AllocationEnded},
		{[]string{"TIMEOUT"}, false, false, v1alpha1.AllocationEnded},
		{[]string{"CANCELLED"}, false, false, v1alpha1.AllocationEnded},
		{[]string{"FAILED"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"NODE_FAIL"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"OUT_OF_MEMORY"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"BOOT_FAIL"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"PREEMPTED"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"DEADLINE"}, false, false, v1alpha1.AllocationFailed},
		// A requeue in any state is Failed, held or not, kept once seen.
		{[]string{"PENDING"}, true, true, v1alpha1.AllocationFailed},
		{[]string{"RUNNING"}, false, true, v1alpha1.AllocationFailed},
		{[]string{"PENDING", "REQUEUED"}, true, false, v1alpha1.AllocationFailed},
		// REQUEUED as the base state, or in any position, as spike S4 accepts it.
		{[]string{"REQUEUED"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"requeued", "PENDING"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"RUNNING", "COMPLETING", "REQUEUED"}, false, false, v1alpha1.AllocationFailed},
		{[]string{"COMPLETED"}, false, true, v1alpha1.AllocationFailed},
	} {
		got, err := SlurmPhase(tc.state, tc.held, tc.requeued)
		if err != nil || got != tc.want {
			t.Errorf("SlurmPhase(%v, held=%v, requeued=%v) = %q, %v; want %q", tc.state, tc.held, tc.requeued, got, err, tc.want)
		}
	}
	for _, unknown := range [][]string{nil, {}, {"REVOKED"}, {"SOMETHING_NEW"}} {
		if got, err := SlurmPhase(unknown, false, false); err == nil {
			t.Errorf("SlurmPhase(%v) = %q, want an error: an unknown state is unknown", unknown, got)
		}
	}
}

func TestHoldOf(t *testing.T) {
	for _, tc := range []struct {
		held   bool
		reason string
		want   HoldKind
	}{
		{false, "None", HoldNone},
		{false, "Priority", HoldNone},
		{true, "JobHeldUser", HoldUser},
		{false, "JobHeldUser", HoldUser},
		{true, "", HoldUser},
		{true, "JobHeldAdmin", HoldAdmin},
		{false, "JobHeldAdmin", HoldAdmin},
	} {
		if got := HoldOf(tc.held, tc.reason); got != tc.want {
			t.Errorf("HoldOf(%v, %q) = %q, want %q", tc.held, tc.reason, got, tc.want)
		}
	}
	if (Job{Hold: HoldAdmin}).Held() != true || (Job{}).Held() {
		t.Error("Job.Held disagrees with Hold")
	}
}

func TestInvalidProfileErrorNamesEveryField(t *testing.T) {
	err := &InvalidProfileError{Provider: "helios-a", Fields: []FieldError{
		{Field: "spec.inflightWindow", Message: "must exceed 60s"},
		{Field: "spec.jsonDataParser", Message: `unknown version "v0.0.39"`},
	}}
	msg := err.Error()
	for _, want := range []string{"helios-a", "spec.inflightWindow", "spec.jsonDataParser"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not name %s", msg, want)
		}
	}
}

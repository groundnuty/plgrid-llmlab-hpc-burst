package provider

import (
	"fmt"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Slurm's job state reasons for a hold (HBEP-4, The Slurm dialect, Held).
const (
	ReasonHeldUser  = "JobHeldUser"
	ReasonHeldAdmin = "JobHeldAdmin"
)

// SlurmPhase maps a Slurm job record to its phase (HBEP-4, Core operations,
// Phases). state is the job's state as Slurm lists it, base state first;
// held says the job is held (hold true, a set priority of 0, or a reason that
// starts with JobHeld); requeued says a requeue was seen. The provider and
// the fake both call it, so they agree on every phase.
//
//   - Failed: a requeue in any state, or FAILED, NODE_FAIL, OUT_OF_MEMORY,
//     BOOT_FAIL, PREEMPTED or DEADLINE;
//   - Ended: COMPLETED, TIMEOUT (the lease end) or CANCELLED;
//   - Held: a held job that has not started;
//   - Pending: PENDING;
//   - Running: RUNNING, or SUSPENDED (a suspended job keeps its allocation and
//     its lease runs on; HBEP-4 lists it among the live states).
//
// Any other base state, or none, is an error: the phase is unknown.
func SlurmPhase(state []string, held, requeued bool) (v1alpha1.AllocationPhase, error) {
	if requeued {
		return v1alpha1.AllocationFailed, nil
	}
	if len(state) == 0 {
		return "", fmt.Errorf("job has no state")
	}
	for _, flag := range state[1:] {
		if strings.EqualFold(flag, "REQUEUED") {
			return v1alpha1.AllocationFailed, nil
		}
	}
	switch base := strings.ToUpper(state[0]); base {
	case "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "PREEMPTED", "DEADLINE":
		return v1alpha1.AllocationFailed, nil
	case "COMPLETED", "TIMEOUT", "CANCELLED":
		return v1alpha1.AllocationEnded, nil
	case "PENDING":
		if held {
			return v1alpha1.AllocationHeld, nil
		}
		return v1alpha1.AllocationPending, nil
	case "RUNNING", "SUSPENDED":
		return v1alpha1.AllocationRunning, nil
	default:
		return "", fmt.Errorf("unknown Slurm base state %q", base)
	}
}

// HoldOf returns who holds a job from its state reason and hold signals: an
// admin hold for JobHeldAdmin, a user hold for any other held job, else none.
func HoldOf(held bool, reason string) HoldKind {
	switch {
	case reason == ReasonHeldAdmin:
		return HoldAdmin
	case held || strings.HasPrefix(reason, "JobHeld"):
		return HoldUser
	default:
		return HoldNone
	}
}

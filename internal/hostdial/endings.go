package hostdial

import (
	"strings"
)

// EndKind is what a relay's end means for its restart (HBEP-6, What ends a
// relay).
type EndKind int

const (
	// EndRetried: started again after the backoff (ssh dropping or killed, a
	// missing preamble or unexpected output, "agent not ready").
	EndRetried EndKind = iota
	// EndSuperseded: the agent keeps a newer relay; final, with no event.
	EndSuperseded
	// EndJobGone: the job is not listed, ended or completing; final, with no
	// event, since the job's end stops the Allocation's relays anyway.
	EndJobGone
	// EndFailed: final, and the controller raises RelayFailed (srun's
	// step-limit refusal, more than the restart limit within its window).
	EndFailed
)

func (k EndKind) String() string {
	switch k {
	case EndSuperseded:
		return "superseded"
	case EndJobGone:
		return "the job's end"
	case EndFailed:
		return "failed"
	}
	return "retried"
}

// Final reports whether the relay is not started again.
func (k EndKind) Final() bool { return k != EndRetried }

// srunJobGone are srun's refusals that the job does not exist or is ending
// (Slurm's error table, slurm_errno.c: ESLURM_INVALID_JOB_ID and
// ESLURM_ALREADY_DONE; the fake Slurm prints the same words, and "job is not
// running" for a job whose batch shell has ended).
var srunJobGone = []string{"Invalid job id specified", "Job/step already completing or completed", "job is not running"}

// srunStepLimit is srun's step-limit refusal (ESLURM_STEP_LIMIT, "Step limit
// reached for this job"). Real srun's exact line is not measured yet; the
// first live run is to show it (HBEP-4, open question 16).
const srunStepLimit = "Step limit reached for this job"

// ClassifyEnd reads a relay's standard error as its ssh client kept it and
// says what its end means, with a fixed description that repeats nothing of
// the text: the relay's own lines (RelayMessage) and srun's refusals are
// recognized, and anything else, an ssh failure or a drop, is retried.
func ClassifyEnd(stderr string) (EndKind, string) {
	switch {
	case strings.Contains(stderr, relayLinePrefix+"superseded"):
		return EndSuperseded, "superseded: the agent keeps a newer relay"
	case strings.Contains(stderr, relayLinePrefix+"agent not ready"):
		return EndRetried, "agent not ready"
	case strings.Contains(stderr, srunStepLimit):
		return EndFailed, "srun refused the step: step limit reached for this job"
	}
	for _, s := range srunJobGone {
		if strings.Contains(stderr, s) {
			return EndJobGone, "srun refused the step: the job is not running"
		}
	}
	return EndRetried, ""
}

package fakeslurm

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// Failure texts of sbatch, as Slurm prints them.
const (
	// sbatchRejectMsg is a definite refusal: slurmctld rejected the job and
	// retrying cannot help.
	sbatchRejectMsg = "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified\n"
	// sbatchLostReplyMsg is what the client prints when the reply never
	// arrives, although slurmctld may have accepted the job.
	sbatchLostReplyMsg = "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n"
)

const (
	gatePoll = 50 * time.Millisecond
	// alignOffset is how far past a whole second FAKE_SLURM_SBATCH_FAIL_ALIGN
	// lands the failure.
	alignOffset = 30 * time.Millisecond
)

// claimMarker creates the marker file atomically (O_EXCL). It reports whether
// this call created it; an existing marker is not an error, any other failure
// is.
func claimMarker(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if errors.Is(err, fs.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim marker: %w", err)
	}
	return true, f.Close()
}

// sbatchFailed logs sbatch_failed, prints msg and returns exit 1; nothing was
// registered.
func (a *app) sbatchFailed(msg string) int {
	if err := a.locked(func(t *tx) error {
		t.emit("sbatch_failed", "sbatch", 0, intp(1))
		return nil
	}); err != nil {
		return a.internalError(err)
	}
	fmt.Fprint(a.env.Stderr, msg)
	return 1
}

// failOnce implements FAKE_SLURM_SBATCH_FAIL_ONCE: the call that creates the
// marker fails like FAKE_SLURM_SBATCH_FAIL, later calls go on. done reports
// that the call is over with the returned exit code.
func (a *app) failOnce() (code int, done bool) {
	if a.cfg.FailOnce == "" {
		return 0, false
	}
	claimed, err := claimMarker(a.cfg.FailOnce)
	if err != nil {
		return a.internalError(err), true
	}
	if !claimed {
		return 0, false
	}
	if a.cfg.FailAlign {
		a.pause("fail_align", alignDelay(a.env.Now()))
	}
	return a.sbatchFailed(sbatchRejectMsg), true
}

// alignDelay is the time from now to 30 ms past the next whole second (UTC).
func alignDelay(now time.Time) time.Duration {
	return now.Truncate(time.Second).Add(time.Second + alignOffset).Sub(now)
}

// waitGate implements FAKE_SLURM_SBATCH_GATE: it logs sbatch_gate_wait, then
// polls outside the lock until the gate file exists. A process killed while
// it waits has registered nothing.
func (a *app) waitGate() (code int, done bool) {
	if a.cfg.GatePath == "" {
		return 0, false
	}
	if err := a.locked(func(t *tx) error {
		t.emit("sbatch_gate_wait", "sbatch", 0, nil)
		return nil
	}); err != nil {
		return a.internalError(err), true
	}
	for {
		_, err := os.Stat(a.cfg.GatePath)
		if err == nil {
			return 0, false
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return a.internalError(fmt.Errorf("gate: %w", err)), true
		}
		a.hook("gate_wait")
		a.env.Sleep(gatePoll)
	}
}

// loseReply implements FAKE_SLURM_SBATCH_LOST_REPLY_ONCE: the job is already
// registered; the call that creates the marker prints the timeout error
// instead of the id.
func (a *app) loseReply() (code int, done bool) {
	if a.cfg.LostReply == "" {
		return 0, false
	}
	claimed, err := claimMarker(a.cfg.LostReply)
	if err != nil {
		return a.internalError(err), true
	}
	if !claimed {
		return 0, false
	}
	fmt.Fprint(a.env.Stderr, sbatchLostReplyMsg)
	return 1, true
}

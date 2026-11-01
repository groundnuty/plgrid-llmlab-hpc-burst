package fake

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"

// A job the scheduler registers late (Script.RegisteredLate). The Submit that
// lost its reply registered nothing; slurmctld registers the job when the next
// sbatch arrives from anyone, before that sbatch is checked, so the job holds
// its user's slot against that sbatch (HBEP-4, QOS limits). Registration at the
// next Submit is deterministic and needs no goroutine, like fakeslurm's
// LostReplyLate.

// lateJob is a request whose job is still to be registered, with the Script
// that lost its reply.
type lateJob struct {
	req provider.SubmitRequest
	sc  Script
}

// registerLate registers the jobs whose reply was lost and which the scheduler
// had not registered yet, held when their request had Hold. The caller holds
// b.mu.
func (b *Batch) registerLate() {
	late := b.late
	b.late = nil
	for _, l := range late {
		b.register(l.req, l.sc)
	}
}

// LateJobs returns how many jobs a RegisteredLate Script is still to register:
// a test waits on it, not on a window.
func (b *Batch) LateJobs() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.late)
}

package fake

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Job returns the job's current record, ignoring Vanish and FailReads.
func (b *Batch) Job(id string) (provider.Job, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j, ok := b.jobs[id]
	if !ok {
		return provider.Job{}, false
	}
	return b.record(j, b.clock.Now()), true
}

// The test controls below panic on an unknown job ID: a test that names a job
// the fake does not hold has a bug, and a silent no-op would hide it.

func (b *Batch) must(id string) *jobRec {
	j, ok := b.jobs[id]
	if !ok {
		panic("fake: unknown job " + id)
	}
	return j
}

// CancelExternally cancels the job outside the framework: a live job ends as
// CANCELLED now. The call log has op "external-cancel".
func (b *Batch) CancelExternally(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j := b.must(id)
	b.log("external-cancel", id)
	now := b.clock.Now()
	if live(b.record(j, now)) {
		j.cancelledAt = &now
	}
}

// Requeue requeues the job: Requeued is true and RestartCount 1, so its phase
// is Failed (HBEP-4, Phases).
func (b *Batch) Requeue(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j := b.must(id)
	j.requeued, j.restartCount = true, 1
}

// Vanish hides the job from the queue listing and from accounting.
func (b *Batch) Vanish(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.must(id).vanished = true
}

// Reappear undoes Vanish.
func (b *Batch) Reappear(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.must(id).vanished = false
}

// FailReads makes FindByTag and Observe return err; nil clears it.
func (b *Batch) FailReads(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readErr = err
}

package fake

import (
	"fmt"
	"time"

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

// Requeue requeues the job as scontrol requeue does. The job returns to
// PENDING: its restart count goes up by one, Requeued is true, its start is
// cleared, it is eligible again from now, and it starts after its script's
// QueueWait from now. The phase is Failed, as for any requeued job (HBEP-4,
// Phases; provider.SlurmPhase). A job that is held stays held: a requeue does
// not lift a hold. The call log has op "requeue" with the job ID. A job that
// has ended cannot be requeued, so it panics.
func (b *Batch) Requeue(id string) { b.requeue(id, "requeue", false) }

// RequeueHold requeues the job like Requeue, and holds it by its user (reason
// JobHeldUser) until the hold is lifted, as scontrol requeuehold does; the lift
// makes it eligible again from then. A job held by an administrator stays held
// by the administrator. The call log has op "requeuehold" with the job ID. A
// job that has ended cannot be requeued, so it panics.
func (b *Batch) RequeueHold(id string) { b.requeue(id, "requeuehold", true) }

func (b *Batch) requeue(id, op string, hold bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j := b.must(id)
	now := b.clock.Now()
	if !live(b.record(j, now)) {
		panic("fake: job " + id + " has ended: it cannot be requeued")
	}
	b.log(op, id)
	if r := b.record(j, now); r.StartTime != nil {
		j.priorRun += now.Sub(*r.StartTime)
	}
	j.restartCount++
	j.requeued = true
	j.eligible = now
	if hold && j.hold != provider.HoldAdmin {
		j.hold = provider.HoldUser
	}
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

// LagAccounting makes ReadAccounting (sacct -j) find nothing for the job while
// the queue listing and Observe still show it, as accounting does when it lags
// the queue; false undoes it. A read that finds nothing is logged all the same.
func (b *Batch) LagAccounting(id string, lag bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.must(id).accountingLags = lag
}

// ForgetRequeue makes every later read show the job as never requeued: one
// execution, the latest, with a restart count of 0, as a reply that lost the
// earlier execution does (review of PR 118, Astra B3). The Controller's stored
// status keeps the requeue it saw.
func (b *Batch) ForgetRequeue(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j := b.must(id)
	j.restartCount, j.requeued, j.priorRun = 0, false, 0
}

// OmitElapsed makes accounting's record of the job lack time.elapsed, the
// counts as usual, so a read of an ended job from accounting is incomplete;
// false undoes it.
func (b *Batch) OmitElapsed(id string, omit bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.must(id).noElapsed = omit
}

// FailReads makes FindByTag, Observe and Poll return err; nil clears it.
func (b *Batch) FailReads(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readErr = err
}

// FailNextLiftHold makes the next LiftHold call return err, once, and change
// nothing: the job stays held. The call is logged with result LiftFailed. Calls
// queue, one error per future LiftHold. A nil err is a test bug and panics.
func (b *Batch) FailNextLiftHold(err error) {
	if err == nil {
		panic("fake: FailNextLiftHold needs an error")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.liftErrs = append(b.liftErrs, err)
}

// SetTimeLimit is an administrator changing a held, pending or running job's
// time limit, typically raising it. A running job's HardEnd is its start plus
// the new limit at the next read, and its phase follows: a limit below the time
// already run ends the job at once. The call log has op "set-time-limit" with
// the job ID and the limit. A job that has ended cannot be revived, so it
// panics, as does a limit that is not positive.
func (b *Batch) SetTimeLimit(id string, limit time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	j := b.must(id)
	if limit <= 0 {
		panic(fmt.Sprintf("fake: time limit %s of job %s is not positive", limit, id))
	}
	if !live(b.record(j, b.clock.Now())) {
		panic("fake: job " + id + " has ended: its time limit cannot change")
	}
	b.log("set-time-limit", id, limit.String())
	j.limit = limit
}

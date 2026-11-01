package fakeslurm

// Ported from spikes/s4/fakeslurm/ctl.go (fakeslurmctl): the job controls a
// test uses to move jobs, now methods on the Platform. New: the slots, the
// phantom jobs, SetEstimate, SetTimeLimit, Job and Jobs.

import (
	"fmt"
	"time"
)

// withJob runs fn on job id with mu held.
func (p *Platform) withJob(id int, fn func(j *job, now time.Time) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	j := p.find(id)
	if j == nil {
		return fmt.Errorf("fakeslurm: no job %d", id)
	}
	return fn(j, p.clock.Now())
}

// slotsOf is the partition's Slots, 0 for unlimited or an unlisted partition.
func (p *Platform) slotsOf(partition string) int {
	pt, _ := p.partition(partition)
	return pt.Slots
}

// running counts the platform's running jobs in a partition.
func (p *Platform) running(partition string) int {
	n := 0
	for _, j := range p.jobs {
		if j.partition == partition && j.state() == Running {
			n++
		}
	}
	return n
}

// startJob moves a pending job to RUNNING when its partition has a free slot
// and the user's GPU cap, if the QOS sets one, allows it (canStart).
func (p *Platform) startJob(j *job, now time.Time) error {
	if !j.held && j.state() == Pending {
		if err := p.canStart(j); err != nil {
			return err
		}
	}
	if n := p.slotsOf(j.partition); n > 0 && p.running(j.partition)+p.phantoms[j.partition] >= n {
		return fmt.Errorf("job %d cannot start: all %d slots of partition %s are taken (phantom jobs hold %d; FreePhantoms releases them)",
			j.id, n, j.partition, p.phantoms[j.partition])
	}
	return j.markStart(now)
}

// endJob ends a pending or running job. A running job's slot goes at once to a
// phantom job of another user.
func (p *Platform) endJob(j *job, now time.Time, state string) error {
	wasRunning := j.state() == Running
	if err := j.markEnd(now, state); err != nil {
		return err
	}
	if wasRunning && p.slotsOf(j.partition) > 0 {
		p.phantoms[j.partition]++
	}
	return nil
}

// requeueJob requeues a job; a running execution frees its slot like an end.
func (p *Platform) requeueJob(j *job, now time.Time) {
	if j.state() == Running && p.slotsOf(j.partition) > 0 {
		p.phantoms[j.partition]++
	}
	j.markRequeue(now)
}

// Start moves a pending job to RUNNING. It is refused for a held job, for a
// job that is not pending, and when the partition has no free slot.
func (p *Platform) Start(id int) error {
	return p.withJob(id, func(j *job, now time.Time) error { return p.startJob(j, now) })
}

// End closes a pending or running job in a terminal state (COMPLETED, FAILED,
// CANCELLED, TIMEOUT or NODE_FAIL).
func (p *Platform) End(id int, state string) error {
	return p.withJob(id, func(j *job, now time.Time) error { return p.endJob(j, now, state) })
}

// Requeue closes the current execution as REQUEUED and opens a PENDING one
// under the same job ID, with the restart count raised.
func (p *Platform) Requeue(id int) error {
	return p.withJob(id, func(j *job, now time.Time) error { p.requeueJob(j, now); return nil })
}

// RequeueHold requeues the job, then puts the new execution on a user hold.
func (p *Platform) RequeueHold(id int) error {
	return p.withJob(id, func(j *job, now time.Time) error {
		p.requeueJob(j, now)
		j.hold(HeldUser)
		return nil
	})
}

// HoldAdmin puts a pending job on an admin hold (JobHeldAdmin).
func (p *Platform) HoldAdmin(id int) error {
	return p.withJob(id, func(j *job, _ time.Time) error {
		if j.state() != Pending {
			return fmt.Errorf("job %d is %s, not PENDING", j.id, j.state())
		}
		j.hold(HeldAdmin)
		return nil
	})
}

// AcctHide makes sacct omit every record of the job (slurmdbd does not have it
// yet); squeue is unaffected. AcctShow undoes it.
func (p *Platform) AcctHide(id int) error { return p.setHidden(id, true) }

// AcctShow undoes AcctHide.
func (p *Platform) AcctShow(id int) error { return p.setHidden(id, false) }

func (p *Platform) setHidden(id int, hidden bool) error {
	return p.withJob(id, func(j *job, _ time.Time) error { j.acctHidden = hidden; return nil })
}

// ForgetRequeue models an accounting purge: every execution but the latest is
// dropped and the restart count is 0.
func (p *Platform) ForgetRequeue(id int) error {
	return p.withJob(id, func(j *job, _ time.Time) error { j.forgetRequeues(); return nil })
}

// SetEstimate sets the start estimate squeue shows for a pending job; nil
// clears it.
func (p *Platform) SetEstimate(id int, t *time.Time) error {
	return p.withJob(id, func(j *job, _ time.Time) error {
		j.estimate = nil
		if t != nil {
			u := t.UTC()
			j.estimate = &u
		}
		return nil
	})
}

// SetTimeLimit changes the job's time limit, in minutes: a centre's extension.
// A running job's projected end_time follows.
func (p *Platform) SetTimeLimit(id, minutes int) error {
	return p.withJob(id, func(j *job, _ time.Time) error { j.timeLimit = minutes; return nil })
}

// FreePhantoms releases the slots that phantom jobs of other users hold, in
// the named partitions, or in all of them when none is named.
func (p *Platform) FreePhantoms(partitions ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(partitions) == 0 {
		p.phantoms = map[string]int{}
		return
	}
	for _, name := range partitions {
		delete(p.phantoms, name)
	}
}

// Phantoms is the number of slots in a partition that phantom jobs hold.
func (p *Platform) Phantoms(partition string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.phantoms[partition]
}

// Job returns a snapshot of the job, and false when there is none.
func (p *Platform) Job(id int) (Job, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j := p.find(id)
	if j == nil {
		return Job{}, false
	}
	return j.snapshot(), true
}

// Jobs returns a snapshot of every job, by job ID.
func (p *Platform) Jobs() []Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Job, len(p.jobs))
	for i, j := range p.jobs {
		out[i] = j.snapshot()
	}
	return out
}

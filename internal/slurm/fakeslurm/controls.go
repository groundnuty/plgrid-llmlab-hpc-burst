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

// running counts the jobs that hold a slot of a partition: the running ones,
// and the ended ones whose node is not free yet. Slurm frees the job's submit
// count and its GPUs when it sets COMPLETING, but a node only once its epilog
// has completed: make_node_comp keeps the node's completing count up and
// make_node_idle sets it idle, and puts it back in the idle bitmap, only when
// the count reaches 0 (src/slurmctld/node_mgr.c: Slurm 24.11.5, lines 4599-4646
// and 4752-4845), and node selection takes no node that is COMPLETING
// (_pick_best_nodes in src/slurmctld/node_scheduler.c: lines 1832-1846). So a
// completing job still holds its slot here. The caller holds mu.
func (p *Platform) running(partition string) int {
	p.settleSlots()
	n := 0
	for _, j := range p.jobs {
		if j.partition == partition && (j.state() == Running || j.slotOwed) {
			n++
		}
	}
	return n
}

// slotsTaken is the slots of a partition that are not free: the jobs holding
// one (running) and the phantoms. The two are read in order, after the settle
// that running makes, since Go does not order a plain read against a call in
// one expression. The caller holds mu.
func (p *Platform) slotsTaken(partition string) int {
	held := p.running(partition)
	return held + p.phantoms[partition]
}

// settleSlots hands the slot of every job whose node is free, one that has
// ended or was requeued and no longer completes, to a phantom job of another
// user, as an ended job's slot always goes (endJob). It is lazy: the epilog ends
// with the platform's clock, so nothing calls it at that moment, and every
// reader of the slots and of the phantoms settles first. The caller holds mu.
func (p *Platform) settleSlots() {
	for _, j := range p.jobs {
		if j.slotOwed && !j.completing() {
			j.slotOwed = false
			p.phantoms[j.partition]++
		}
	}
}

// startJob moves a pending job to RUNNING when its partition has a free slot
// and the user's GPU cap, if the QOS sets one, allows it (canStart).
func (p *Platform) startJob(j *job, now time.Time) error {
	if !j.held && j.state() == Pending {
		if err := p.canStart(j); err != nil {
			return err
		}
	}
	if n := p.slotsOf(j.partition); n > 0 && p.slotsTaken(j.partition) >= n {
		return fmt.Errorf("job %d cannot start: all %d slots of partition %s are taken (phantom jobs hold %d; FreePhantoms releases them)",
			j.id, n, j.partition, p.phantoms[j.partition])
	}
	return j.markStart(now)
}

// endJob ends a pending or running job. A running job's slot goes to a phantom
// job of another user once its node is free: at once when the job is not
// completing, else when it stops (settleSlots). In run mode its processes are
// stopped: TERM, then KILL after grace, or KILL at once for no grace.
func (p *Platform) endJob(j *job, now time.Time, state string, grace time.Duration) error {
	wasRunning := j.state() == Running
	if err := j.markEnd(now, state); err != nil {
		return err
	}
	if wasRunning && p.slotsOf(j.partition) > 0 {
		j.slotOwed = true
	}
	p.stop(j.cur().run, grace)
	// Slurm frees the job's submit slot and its GPUs when it sets the end
	// state, which also sets COMPLETING while processes or the epilog run, so a
	// pending job they held back starts now, not when the processes are gone.
	p.schedule(now)
	return nil
}

// requeueJob requeues a job; a running execution frees its slot like an end.
// In run mode its processes get KILL at once, and the new execution starts
// once they are gone.
func (p *Platform) requeueJob(j *job, now time.Time) {
	if j.state() == Running && p.slotsOf(j.partition) > 0 {
		j.slotOwed = true
	}
	p.stop(j.cur().run, 0)
	j.markRequeue(now)
}

// Start moves a pending job to RUNNING. It is refused for a held job, for a
// job that is not pending, and when the partition has no free slot. In run
// mode the fake starts jobs itself, and Start is refused.
func (p *Platform) Start(id int) error {
	return p.withJob(id, func(j *job, now time.Time) error {
		if p.run != nil {
			return fmt.Errorf("fakeslurm: job %d: run mode starts a job itself once it is eligible; hold it, or use slots, to keep it pending", j.id)
		}
		return p.startJob(j, now)
	})
}

// End closes a pending or running job in a terminal state (COMPLETED, FAILED,
// CANCELLED, TIMEOUT or NODE_FAIL). In run mode a running job's processes get
// KILL at once, as when its node fails.
func (p *Platform) End(id int, state string) error {
	return p.withJob(id, func(j *job, now time.Time) error {
		if err := p.endJob(j, now, state, 0); err != nil {
			return err
		}
		p.schedule(now)
		return nil
	})
}

// Requeue closes the current execution as REQUEUED and opens a PENDING one
// under the same job ID, with the restart count raised.
func (p *Platform) Requeue(id int) error {
	return p.withJob(id, func(j *job, now time.Time) error {
		p.requeueJob(j, now)
		p.schedule(now)
		return nil
	})
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

// HoldCompleting makes an ended job COMPLETING until it is told otherwise (on
// false): squeue lists its end state with the flag, nothing purges it, and its
// accounting record keeps the end state alone, as in Slurm. It is the control
// of the tests that run no processes, where run mode's own flag (processes
// that live, RunMode.Epilog) does not exist. The job must have ended, or be the
// PENDING execution a requeue opened (Slurm lists a job requeued while its nodes
// clean up as ["PENDING", "COMPLETING"]); run mode refuses it: there the
// processes and the epilog decide. A flag held on a pending execution stays when
// the job is cancelled, which turns it CANCELLED with the flag.
func (p *Platform) HoldCompleting(id int, on bool) error {
	return p.withJob(id, func(j *job, _ time.Time) error {
		if p.run != nil {
			return fmt.Errorf("fakeslurm: job %d: run mode decides COMPLETING from its processes and RunMode.Epilog", id)
		}
		if on && !IsTerminal(j.state()) && !j.requeuedPending() {
			return fmt.Errorf("fakeslurm: job %d is %s: only an ended job, or a requeued one that is pending again, can be COMPLETING", id, j.state())
		}
		j.cur().heldCompleting = on
		return nil
	})
}

// RenameListed makes squeue list the job under name while sacct keeps the name
// the job was submitted with: a fixture for two sources that disagree about the
// job's name, which the plugin must not let hide the queue's evidence. The job
// is found by its ID in both, and by its submitted name in accounting only. An
// empty name undoes it.
func (p *Platform) RenameListed(id int, name string) error {
	return p.withJob(id, func(j *job, _ time.Time) error { j.listedName = name; return nil })
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
// A running job's projected end_time follows, and in run mode so does its
// timer, counted from the start; a limit already passed ends it TIMEOUT at
// once.
func (p *Platform) SetTimeLimit(id, minutes int) error {
	return p.withJob(id, func(j *job, _ time.Time) error {
		j.timeLimit = minutes
		p.rearmLimit(j.cur().run, minutes)
		return nil
	})
}

// FreePhantoms releases the slots that phantom jobs of other users hold, in
// the named partitions, or in all of them when none is named.
func (p *Platform) FreePhantoms(partitions ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.schedule(p.clock.Now())
	p.settleSlots() // a node whose epilog has ended is freed to a phantom first, and this frees that too
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
	p.settleSlots()
	return p.phantoms[partition]
}

// snapshot is the job's snapshot. In run mode an execution that is terminal
// and never had a process (cancelled while pending or held, ended by a
// control) has nothing left to wait for, so it is Finished. The caller holds
// mu.
func (p *Platform) snapshot(j *job) Job {
	s := j.snapshot()
	if p.run != nil && j.cur().run == nil && IsTerminal(j.state()) {
		s.Finished = true
	}
	return s
}

// Job returns a snapshot of the job, and false when there is none.
func (p *Platform) Job(id int) (Job, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j := p.find(id)
	if j == nil {
		return Job{}, false
	}
	return p.snapshot(j), true
}

// Jobs returns a snapshot of every job, by job ID.
func (p *Platform) Jobs() []Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Job, len(p.jobs))
	for i, j := range p.jobs {
		out[i] = p.snapshot(j)
	}
	return out
}

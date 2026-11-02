package fake

import (
	"fmt"
	"strconv"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The per-user QOS limits (HBEP-4, QOS limits). Every job in one Batch belongs
// to the one user whose login the framework uses, so a limit counts all of the
// Batch's jobs: those the framework submitted and the outside jobs of
// AddOutsideJob.

// submitLimitRefusal returns the refusal a Submit at now gets from Config's
// MaxSubmitPerUser, or nil when the limit is unset or has room. The caller holds
// b.mu. The Line is the one a scripted refusal (Script.Policy) has.
func (b *Batch) submitLimitRefusal(now time.Time) *provider.PolicyRefusal {
	limit := b.cfg.MaxSubmitPerUser
	if limit == 0 || b.liveJobs(now) < limit {
		return nil
	}
	return &provider.PolicyRefusal{
		Reason: provider.ReasonQOSMaxSubmitJobPerUserLimit,
		Line:   "sbatch: error: " + provider.ReasonQOSMaxSubmitJobPerUserLimit,
	}
}

// liveJobs counts the user's jobs with no end time at now: held, pending or
// running. A job that has ended is not counted, however recently. A vanished
// job is: Vanish hides a job from reads, and the scheduler still holds it. The
// caller holds b.mu.
func (b *Batch) liveJobs(now time.Time) int {
	n := 0
	for _, j := range b.jobs {
		if live(b.record(j, now)) {
			n++
		}
	}
	return n
}

// AddOutsideJob registers a job the framework did not submit: the user's other
// work, which the scheduler counts against the user's limits all the same, the
// cap on the user's GPUs (Config.MaxGPUsPerUser) included. The job has the
// given name, partition, GPU count and time limit; it is unheld, eligible at
// the clock's now, and runs the DefaultScript (no queue wait), so it runs at
// once, unless the GPU cap holds it back like any job, and ends at its limit
// unless CancelExternally ends it sooner. It returns the job's ID.
//
// It is set-up state, not a call of the framework: the call log gets no entry,
// no queued Script is consumed, and the submit limit does not refuse it. A name
// that is not the framework's tag keeps FindByTag away from it. It panics on a
// name that fails the safe pattern, an unknown partition, a negative GPU count
// or a limit that is not positive (a zero limit would end at once and hold no
// slot).
func (b *Batch) AddOutsideJob(name, partition string, gpus int32, limit time.Duration) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !safeValue.MatchString(name) {
		panic(fmt.Sprintf("fake: outside job name %q is empty or unsafe", name))
	}
	known := false
	for _, p := range b.cfg.Partitions {
		known = known || p.Name == partition
	}
	switch {
	case !known:
		panic(fmt.Sprintf("fake: outside job in unknown partition %q", partition))
	case gpus < 0:
		panic(fmt.Sprintf("fake: outside job has %d gpus", gpus))
	case limit <= 0:
		panic(fmt.Sprintf("fake: outside job has time limit %s, which is not positive", limit))
	}
	now := b.clock.Now()
	j := &jobRec{
		id:        strconv.Itoa(b.nextID),
		tag:       name,
		name:      name,
		partition: partition,
		limit:     limit,
		gpus:      gpus,
		submit:    now,
		script:    DefaultScript,
		eligible:  now,
		seq:       b.takeSeq(),
	}
	b.nextID++
	b.jobs[j.id] = j
	return j.id
}

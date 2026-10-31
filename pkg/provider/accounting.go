package provider

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// JobAccounting is what the scheduler's accounting reported about a job: the
// fields of the records a plugin already reads (HBEP-4, The Slurm dialect,
// Accounting), and no more. A nil pointer means unknown, never 0 (HBEP-8,
// Provider accounting). The numbers are allocated, not billed.
type JobAccounting struct {
	// AllocatedCPUs is the cpu entry of the latest execution's tres.allocated;
	// nil when the record has none.
	AllocatedCPUs *int32
	// AllocatedGPUs is the gres entry named gpu (untyped, else the typed
	// entries summed) of the latest execution's tres.allocated; nil when the
	// record lists none. A job that asked for no GPU has none listed, which
	// AccountingOf reads as 0 only when the allocation list itself was read.
	AllocatedGPUs *int32
	// GPUsListed is true when the latest execution's tres.allocated lists a
	// gres entry named gpu or gpu:*, readable or not. An entry whose count is
	// null, missing, negative or too large leaves AllocatedGPUs nil, and
	// AccountingOf never reads that as 0 (review of PR 118, Astra B8).
	GPUsListed bool
	// Elapsed is time.elapsed summed over every execution the accounting read
	// returned. nil when an execution has no elapsed time, or when the read did
	// not return all of the job's executions (a restart count that the records
	// do not account for).
	Elapsed *time.Duration
	// Executions is the number of records the read returned for the job, one
	// per execution.
	Executions int
}

// AccountingOf returns the Allocation's status.atProvider.accounting from a
// read of its recorded job at now (HBEP-8, Provider accounting). It is nil
// while the job has not started: its hours are unknown, never 0.
//
//   - Allocated counts come from job.Accounting; a count it lacks keeps its
//     value in prior, the stored accounting, which an earlier read set. A job
//     that asked for no GPU (requestedGPUs 0), whose allocation list was read
//     (a cpu count is known) and lists no GPU entry at all, has 0 GPUs; one
//     whose list has a GPU entry it cannot read has unknown GPUs; a job that asked for
//     GPUs and whose list shows none has unknown GPUs, so a change in accounting's
//     shape never reads as 0 GPU-hours.
//   - While the job runs, the elapsed time runs from the scheduler's start time
//     to now. Once it has ended, it is the end time minus the start time when the
//     final state came from the queue listing, and otherwise accounting's
//     time.elapsed (job.Accounting.Elapsed), summed over the executions. A
//     requeued job read from the queue listing has its earlier executions
//     unknown, so its elapsed time is unknown.
//   - ReadTime is now, in whole seconds, UTC, and the running elapsed time is
//     counted to that same second.
func AccountingOf(job Job, requestedGPUs int32, prior *v1alpha1.AllocationAccounting, now time.Time) *v1alpha1.AllocationAccounting {
	if job.StartTime == nil {
		return nil
	}
	read := now.UTC().Truncate(time.Second)
	start := job.StartTime.UTC().Truncate(time.Second)
	out := &v1alpha1.AllocationAccounting{ReadTime: metav1.NewTime(read)}
	if a := job.Accounting; a != nil {
		out.AllocatedCPUs, out.AllocatedGPUs = copyCount(a.AllocatedCPUs), copyCount(a.AllocatedGPUs)
	}
	if prior != nil {
		if out.AllocatedCPUs == nil {
			out.AllocatedCPUs = copyCount(prior.AllocatedCPUs)
		}
		if out.AllocatedGPUs == nil {
			out.AllocatedGPUs = copyCount(prior.AllocatedGPUs)
		}
	}
	if out.AllocatedGPUs == nil && out.AllocatedCPUs != nil && requestedGPUs == 0 && (job.Accounting == nil || !job.Accounting.GPUsListed) {
		zero := int32(0)
		out.AllocatedGPUs = &zero
	}
	var elapsed *time.Duration
	switch {
	case job.EndTime == nil:
		if !job.Requeued {
			d := max(read.Sub(start), 0)
			elapsed = &d
		}
	case job.Source == SourceAccounting:
		if job.Accounting != nil {
			elapsed = job.Accounting.Elapsed
		}
	default:
		if end := job.EndTime.UTC().Truncate(time.Second); !job.Requeued && !end.Before(start) {
			d := end.Sub(start)
			elapsed = &d
		}
	}
	if elapsed != nil {
		s := int64(*elapsed / time.Second)
		out.ElapsedSeconds = &s
	}
	return out
}

func copyCount(p *int32) *int32 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// AccountingRefresh is how old a running job's stored accounting may get
// before a read writes it again: the interval at which the Provider's
// lastSuccessfulStatusRead is refreshed (science agent, 31 Oct 2026, on the
// review of PR 118).
const AccountingRefresh = v1alpha1.ProviderPollInterval

// MirrorAccounting returns the status.atProvider.accounting to store after a
// read of the recorded job at now, given prior, the stored observation
// (science agent, 31 Oct 2026, on the review of PR 118: the status-write
// cadence). It is AccountingOf's, except that prior's accounting is kept as it
// is, readTime included, when storing the new one would be a write that
// carries nothing:
//   - nothing but readTime would change (PR 76: no identical status writes);
//   - the job is still running, the allocated counts are unchanged, and prior
//     was read less than AccountingRefresh ago. Its elapsed time is still
//     elapsedSeconds at readTime; a reader that needs it now counts on from
//     readTime.
//
// A change in a count, the job's end, and a stored block a minute old are
// written at once.
func MirrorAccounting(job Job, requestedGPUs int32, prior *v1alpha1.AllocationObservation, now time.Time) *v1alpha1.AllocationAccounting {
	next := AccountingOf(job, requestedGPUs, prior.Accounting, now)
	if keepStored(prior, next, job.EndTime != nil) {
		return prior.Accounting.DeepCopy()
	}
	return next
}

// keepStored reports whether storing next in place of prior's accounting
// would change nothing a reader needs (MirrorAccounting).
func keepStored(prior *v1alpha1.AllocationObservation, next *v1alpha1.AllocationAccounting, ended bool) bool {
	stored := prior.Accounting
	if stored == nil || next == nil || !sameCount(stored.AllocatedCPUs, next.AllocatedCPUs) || !sameCount(stored.AllocatedGPUs, next.AllocatedGPUs) {
		return false
	}
	if ended && prior.EndTime == nil {
		return false // the end is written at once, with the time it was read
	}
	if sameSeconds(stored.ElapsedSeconds, next.ElapsedSeconds) {
		return true
	}
	return !ended && stored.ElapsedSeconds != nil && next.ElapsedSeconds != nil &&
		next.ReadTime.Sub(stored.ReadTime.Time) < AccountingRefresh
}

func sameCount(a, b *int32) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }

func sameSeconds(a, b *int64) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }

// AccountingLost returns the accounting of a job whose final read never came
// (HBEP-8: "A Lost job is recorded with its elapsed time unknown"): the counts
// from the stored accounting, and no elapsed time. It is nil when the job was
// never seen started, since its hours are then unknown.
func AccountingLost(startTime *time.Time, requestedGPUs int32, prior *v1alpha1.AllocationAccounting, now time.Time) *v1alpha1.AllocationAccounting {
	if startTime == nil {
		return nil
	}
	return AccountingOf(Job{StartTime: startTime, EndTime: startTime, Source: SourceAccounting}, requestedGPUs, prior, now)
}

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
//     (a cpu count is known) and lists no GPU, has 0 GPUs; a job that asked for
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
	if out.AllocatedGPUs == nil && out.AllocatedCPUs != nil && requestedGPUs == 0 {
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

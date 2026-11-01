package fake

import (
	"context"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// liveAccounting sets the observation's status.atProvider.accounting from the
// read of the recorded job (HBEP-8, Provider accounting). It runs on every
// observe that found the job, and it is nil for a job that has not started.
//
// The allocated counts come from one read of accounting alone
// (Batch.ReadAccounting), made when accountingDue says so. Whether it is due
// is judged from the stored status alone, never from memory (review of PR 118,
// Astra B5), so a status write that fails, or a restart, costs one more read and
// never loses one. The block is provider.MirrorAccounting's, at its
// status-write cadence, and a pass that made the read stores its readTime,
// which times the next one. A read that fails, or succeeds but returns nothing,
// is retried once a minute from that readTime until the stored accounting has
// the CPU count; the other passes make no extra Batch call, because the counts
// are kept in the stored status and the elapsed time is worked out from the
// scheduler's own start time.
//
// A failed accounting read is a failed read of the pass, like a failed listing
// (Provider readiness), and not an error: the elapsed time is still shown.
func (c *Controller) liveAccounting(ctx context.Context, p *pass, s *allocState, job provider.Job) {
	made := false
	if accountingDue(job, s.prior, p.now) {
		made = true
		if read, found, err := c.readAccounting(ctx, p, job.ID); err == nil && found {
			// Only the counts are taken: the Source of the job stays the one
			// its final state came from, which AccountingOf reads.
			job.Accounting = read.Accounting
		}
	}
	s.obs.Accounting = provider.MirrorAccounting(job, requestedGPUs(s), s.prior, p.now, made)
}

// accountingDue reports whether the pass makes the accounting read of the job
// (the real controller's allocatedRead): the job has a start time, accounting
// did not already give its record (the listing did not show it), and it is not
// requeued, whose counts are unknown whatever the read would show (science
// agent, 31 Oct 2026). Then the stored accounting decides: none, or one with no
// allocatedCPUs whose readTime is at least provider.AccountingRefresh before
// now. A stored CPU count ends the reads.
func accountingDue(job provider.Job, prior *v1alpha1.AllocationObservation, now time.Time) bool {
	if job.StartTime == nil || job.Accounting != nil || prior.Requeued || provider.AccountingRequeued(job) {
		return false
	}
	s := prior.Accounting
	return s == nil || (s.AllocatedCPUs == nil && now.Sub(s.ReadTime.Time) >= provider.AccountingRefresh)
}

// readAccounting is Batch.ReadAccounting, noted in the pass like findByTag.
func (c *Controller) readAccounting(ctx context.Context, p *pass, id string) (provider.Job, bool, error) {
	p.read = true
	job, found, err := c.Batch.ReadAccounting(ctx, id)
	if err != nil {
		p.readFailed = true
	}
	return job, found, err
}

// requestedGPUs is the GPU count the Allocation asked for, 0 when it has no
// batch parameters.
func requestedGPUs(s *allocState) int32 {
	if bp := s.a.Spec.ForProvider.Batch; bp != nil {
		return bp.GPUs
	}
	return 0
}

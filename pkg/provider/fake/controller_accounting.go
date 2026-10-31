package fake

import (
	"context"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// liveAccounting sets the observation's status.atProvider.accounting from the
// read of the recorded job (HBEP-8, Provider accounting). It runs on every
// observe that found the job, and it is nil for a job that has not started.
//
// The allocated counts come from one read of accounting alone
// (Batch.ReadAccounting), made when the job has a start time, the stored
// accounting has no allocatedCPUs, and the job has not been read this way by
// this Controller value; a requeued job that starts again is another execution,
// read once more. A read that fails is retried at the next pass; one
// that succeeds but returns nothing counts as the one read. A restarted
// Controller reads once more for a job whose stored accounting still lacks the
// CPU count, and no more than that: the other passes make no extra Batch call,
// because the counts are kept in the stored status and the elapsed time is
// worked out from the scheduler's own start time.
//
// A failed accounting read is a failed read of the pass, like a failed listing
// (Provider readiness), and not an error: the elapsed time is still shown.
func (c *Controller) liveAccounting(ctx context.Context, p *pass, s *allocState, job provider.Job) {
	prior := s.prior.Accounting
	if job.StartTime != nil && (prior == nil || prior.AllocatedCPUs == nil) && !c.accountingWasRead(executionKey(job)) {
		read, found, err := c.readAccounting(ctx, p, job.ID)
		if err == nil {
			c.accountingRead[executionKey(job)] = struct{}{}
			if found {
				// Only the counts are taken: the Source of the job stays the
				// one its final state came from, which AccountingOf reads.
				job.Accounting = read.Accounting
			}
		}
	}
	s.obs.Accounting = provider.AccountingOf(job, requestedGPUs(s), prior, p.now)
}

// executionKey names one execution of a job: its ID and start time. The real
// controller makes its one accounting read per execution, since a requeued job
// that starts again has an allocation of its own.
func executionKey(job provider.Job) string {
	return fmt.Sprintf("%s@%d", job.ID, job.StartTime.Unix())
}

// accountingWasRead reports whether this Controller value has already made its
// one accounting read of the execution. The map is made on first use, because
// tests build a Controller as a struct literal.
func (c *Controller) accountingWasRead(key string) bool {
	if c.accountingRead == nil {
		c.accountingRead = map[string]struct{}{}
	}
	_, ok := c.accountingRead[key]
	return ok
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

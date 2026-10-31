package fake

import (
	"context"
	"errors"
	"fmt"
	"strings"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// readBack is HBEP-4's read-back (The Slurm dialect), run on an observe of the
// recorded job while the Allocation is not deleting:
//
//   - A job whose name is not the tag was renamed by the platform and can never
//     be found by tag again, so deletion's cancel by tag would miss it. The
//     observation goes to phase Refused with reason ReasonJobRenamed, and the
//     job is cancelled while it is live. The name is checked on every observe,
//     so a failed cancel is retried; a cancelled job has an end time, so it is
//     not cancelled twice.
//   - Otherwise, on the first observe after the job is established only (HBEP-4:
//     "on the first observe after a job is established"; the stored
//     observation does not yet name the job), a job whose partition, time limit
//     or GPU count differs from spec.forProvider.batch sets the condition
//     SubmittedAsOther True, reason ReasonReadBackMismatch, with a message
//     naming each differing field and both values. The condition is never
//     cleared, and the phase keeps mirroring the job. A later change, such as
//     an administrator's extension of the time limit, is a later hard end that
//     the observation carries, never a mismatch.
//
// Refused, once set, is kept (writeStatus); a Refused set by hand stays, with
// its reason. The real provider's managed reconciler owns the Synced
// condition, so the read-back sets none.
func (c *Controller) readBack(ctx context.Context, p *pass, s *allocState, job provider.Job) error {
	bp := s.a.Spec.ForProvider.Batch
	if bp == nil {
		return nil
	}
	if job.Name != s.tag {
		return c.refuseRenamed(ctx, s, job)
	}
	if s.prior.JobID == job.ID {
		return nil // not the first observe of this job: its settings were read back then
	}
	diffs := readBackDiffs(bp, job)
	if len(diffs) == 0 || s.a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == corev1.ConditionTrue {
		return nil // nothing differs, or the condition is already raised and stays
	}
	s.conds = append(s.conds, xpv2.Condition{
		Type:               v1alpha1.ConditionSubmittedAsOther,
		Status:             corev1.ConditionTrue,
		Reason:             v1alpha1.ReasonReadBackMismatch,
		LastTransitionTime: metav1.NewTime(p.now),
		Message:            "job " + job.ID + " was established with other settings than requested: " + strings.Join(diffs, "; "),
	})
	return nil
}

// refuseRenamed handles a job stored under a name other than the tag.
func (c *Controller) refuseRenamed(ctx context.Context, s *allocState, job provider.Job) error {
	if s.prior.Phase != v1alpha1.AllocationRefused {
		s.obs.Phase, s.obs.Reason = v1alpha1.AllocationRefused, v1alpha1.ReasonJobRenamed
	}
	if !live(job) {
		return nil
	}
	for _, r := range c.Batch.Cancel(ctx, []string{job.ID}) {
		if r.Outcome == provider.CancelError && !errors.Is(r.Err, provider.ErrNotFound) {
			return fmt.Errorf("cancel renamed job %s: %w", r.JobID, r.Err)
		}
	}
	return nil
}

// readBackDiffs lists the fields of job that differ from the request, each with
// both values, in the order partition, time limit, GPUs.
func readBackDiffs(bp *v1alpha1.BatchParameters, job provider.Job) []string {
	var diffs []string
	if job.Partition != bp.Partition {
		diffs = append(diffs, fmt.Sprintf("partition: requested %q, got %q", bp.Partition, job.Partition))
	}
	// A limit that does not parse was refused before any call, so no job exists
	// for it; it cannot differ.
	if want, err := bp.TimeLimit.AsDuration(); err == nil && job.TimeLimit != want {
		diffs = append(diffs, fmt.Sprintf("timeLimit: requested %s, got %s", want, job.TimeLimit))
	}
	if job.GPUs != bp.GPUs {
		diffs = append(diffs, fmt.Sprintf("gpus: requested %d, got %d", bp.GPUs, job.GPUs))
	}
	return diffs
}

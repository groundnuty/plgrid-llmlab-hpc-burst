package fake

import (
	"context"
	"errors"
	"fmt"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// allocState is one Allocation's state within one pass.
type allocState struct {
	a   *v1alpha1.Allocation
	tag string
	// prior is the stored status, copied before any Update overwrites a.Status.
	prior *v1alpha1.AllocationObservation
	// obs is the status to write.
	obs *v1alpha1.AllocationObservation
	// rec is the submission record, nil when there is none.
	rec *v1alpha1.SubmissionRecord
	// cond is the SubmissionResolved condition to set, nil to leave the stored
	// one as it is. It is applied in writeStatus, after the last Update: an
	// Update restores a.Status from the store and would drop it.
	cond *xpv2.Condition
}

// syncAllocation handles one Allocation, in the order: metadata, deletion, the
// job's establishment, then the observation of the recorded job.
func (c *Controller) syncAllocation(ctx context.Context, p *pass, a *v1alpha1.Allocation) error {
	s := &allocState{a: a, tag: v1alpha1.JobTag(a.Namespace, a.Name, a.UID), prior: a.Status.AtProvider.DeepCopy()}
	s.obs = s.prior.DeepCopy()
	s.obs.Tag = s.tag
	if err := c.syncMetadata(ctx, s); err != nil {
		return err
	}
	if !a.DeletionTimestamp.IsZero() {
		return c.syncDeleting(ctx, p, s)
	}
	var err error
	if s.rec, err = record(a); err != nil {
		return err
	}
	if a.Annotations[v1alpha1.AnnotationJobID] == "" {
		settled, err := c.establish(ctx, p, s)
		if err != nil {
			return err
		}
		if !settled {
			return c.writeStatus(ctx, s, nil)
		}
		if s.rec, err = record(a); err != nil {
			return err
		}
	}
	return c.writeStatus(ctx, s, c.observe(ctx, p, s))
}

// syncMetadata adds the finalizer and the external name, in one Update.
func (c *Controller) syncMetadata(ctx context.Context, s *allocState) error {
	a := s.a
	changed := false
	if a.DeletionTimestamp.IsZero() && controllerutil.AddFinalizer(a, v1alpha1.FinalizerCancelAll) {
		changed = true
	}
	if a.Annotations[v1alpha1.AnnotationExternalName] != s.tag {
		if a.Annotations == nil {
			a.Annotations = map[string]string{}
		}
		a.Annotations[v1alpha1.AnnotationExternalName] = s.tag
		changed = true
	}
	if changed {
		if err := update(ctx, c.Client, a); err != nil {
			return fmt.Errorf("update metadata: %w", err)
		}
	}
	return nil
}

// writeStatus writes the status, unless neither the observation nor the
// condition changed, and returns err (the earlier failure of the same
// Allocation, which does not stop the write) joined with the write's own.
func (c *Controller) writeStatus(ctx context.Context, s *allocState, err error) error {
	stored := append([]xpv2.Condition(nil), s.a.Status.Conditions...)
	if s.cond != nil {
		s.a.SetConditions(*s.cond)
	}
	if apiequality.Semantic.DeepEqual(s.prior, s.obs) && apiequality.Semantic.DeepEqual(stored, s.a.Status.Conditions) {
		return err
	}
	s.a.Status.AtProvider = *s.obs // after the last Update, which restores the stored status
	if uerr := updateStatus(ctx, c.Client, s.a); uerr != nil {
		return errors.Join(err, fmt.Errorf("update status: %w", uerr))
	}
	return err
}

// observe settles the recorded job in hold mode, then reads it and mirrors it
// into the observation. It returns the failure of the settle, if any, after the
// observation is complete, so the status still shows the job.
func (c *Controller) observe(ctx context.Context, p *pass, s *allocState) error {
	id := s.a.Annotations[v1alpha1.AnnotationJobID]
	var settleErr error
	if c.HoldRelease {
		settleErr = c.settle(ctx, p, s, id)
	}
	job, found, err := c.observeJob(ctx, p, id)
	if err != nil {
		// A failed read is not absence: nothing changes, not even the clock of
		// absentSince.
		*s.obs = *s.prior.DeepCopy()
		return settleErr
	}
	c.mirror(p, s, id, job, found)
	return settleErr
}

// mirror writes what the read of the recorded job showed into the observation.
func (c *Controller) mirror(p *pass, s *allocState, id string, job provider.Job, found bool) {
	obs, prior := s.obs, s.prior
	obs.JobID, obs.Submission = id, s.rec
	if !found {
		if obs.AbsentSince == nil {
			obs.AbsentSince = mt(p.now)
		}
		if p.now.Sub(obs.AbsentSince.Time) > c.lostGrace() {
			obs.Phase, obs.Reason = v1alpha1.AllocationLost, absentMessage
		}
		return
	}
	obs.AbsentSince = nil
	obs.Phase = job.Phase
	obs.Held = job.Held()
	obs.Requeued = obs.Requeued || job.Requeued // kept once seen
	obs.Reason = job.Reason
	if obs.Requeued {
		obs.Reason = v1alpha1.ReasonRequeued
	}
	obs.RestartCount = job.RestartCount
	obs.SubmitTime = mt(job.SubmitTime)
	obs.StartEstimate, obs.StartTime = mtp(job.StartEstimate), mtp(job.StartTime)
	obs.HardEnd, obs.EndTime = mtp(job.HardEnd), mtp(job.EndTime)
	if job.StartTime != nil && prior.StartTime == nil {
		p.waits = append(p.waits, v1alpha1.QueueWait{JobID: id, SubmitTime: *mt(job.SubmitTime), StartTime: *mt(*job.StartTime)})
	}
}

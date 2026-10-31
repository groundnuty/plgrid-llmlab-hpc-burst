package fake

import (
	"context"
	"fmt"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// recordQueueWait writes the recorded job's queue wait to the Provider's
// status, as soon as the job is observed with a start time and the stored
// observation's queueWaitRecorded is false (docs/development.md, Queue waits).
// Only after that write succeeded does it set queueWaitRecorded in the
// observation to write. A failure leaves it false and is returned, so the next
// pass retries; the Allocation's own status write still happens. A wait that is
// already in the Provider's status (the Allocation's write failed after the
// Provider's succeeded) is not added again and costs no write.
func (c *Controller) recordQueueWait(ctx context.Context, s *allocState, id string, job provider.Job, found bool) error {
	if !found || job.StartTime == nil || s.prior.QueueWaitRecorded {
		return nil
	}
	var prov v1alpha1.Provider
	if err := c.Client.Get(ctx, client.ObjectKey{Name: c.Provider}, &prov); err != nil {
		return fmt.Errorf("get Provider %s to record the queue wait of job %s: %w", c.Provider, id, err)
	}
	old := prov.DeepCopy()
	prov.Status.QueueWaits = addQueueWait(prov.Status.QueueWaits, v1alpha1.QueueWait{
		JobID: id, SubmitTime: *mt(job.SubmitTime), StartTime: *mt(*job.StartTime),
	})
	if err := writeProviderStatus(ctx, c.Client, old, &prov); err != nil {
		return fmt.Errorf("record the queue wait of job %s: %w", id, err)
	}
	s.obs.QueueWaitRecorded = true
	return nil
}

// addQueueWait is v1alpha1.AddQueueWait, with the list kept oldest start first
// whatever order the jobs were observed in. A job already in the list changes
// nothing.
func addQueueWait(waits []v1alpha1.QueueWait, w v1alpha1.QueueWait) []v1alpha1.QueueWait {
	for _, old := range waits {
		if old.JobID == w.JobID {
			return waits
		}
	}
	out := v1alpha1.AddQueueWait(waits, w)
	sort.SliceStable(out, func(i, k int) bool { return out[i].StartTime.Before(&out[k].StartTime) })
	return out
}

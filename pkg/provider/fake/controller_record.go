package fake

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// keepAccounting writes the Allocation's accounting record, acct-<UID>, when a
// deleting Allocation has no live job and no unresolved attempt, before its
// files and finalizer go (HBEP-8, Provider accounting). It uses the jobs the
// deleting pass already read (deletingJobs) and makes no Batch call. wait is
// true when the finalizer is to be kept for the final read; err keeps it too.
//
//   - The recorded job is the one the job-ID annotation names, else the job
//     adoption would pick. With neither there is nothing to account, and no
//     record is written.
//   - A recorded job that was read has ended: its hours are AccountingOf's over
//     the stored accounting, which keeps the counts of an earlier read. One
//     that never started has every hours field unknown, and its end time if it
//     has one; that is written at once.
//   - A recorded job that no source shows, and that the stored status shows
//     started, is waited for up to LostGrace from status.atProvider.absentSince,
//     which the first such pass sets (the only status a deleting Allocation is
//     given) and a live Lost pass may have set earlier. When the grace has
//     passed, the record has the elapsed and end times unknown, and the counts
//     and start time of the stored status. One never seen started is written at
//     once, with nothing known.
//
// The record is created if absent: an AlreadyExists is success, and the record
// is never read back, updated or owned by the Allocation. A failed create
// returns the error, so the finalizer stays.
func (c *Controller) keepAccounting(ctx context.Context, p *pass, s *allocState, jobs []provider.Job) (wait bool, err error) {
	id := s.a.Annotations[v1alpha1.AnnotationJobID]
	if id == "" && len(jobs) > 0 {
		id = pickAdopted(jobs).ID
	}
	if id == "" {
		return false, nil
	}
	data := v1alpha1.AccountingRecordData{Provider: c.Provider, Tag: s.tag, JobIDs: jobIDsWith(jobs, id)}
	if bp := s.a.Spec.ForProvider.Batch; bp != nil {
		data.Partition = bp.Partition
	}
	if job, ok := jobByID(jobs, id); ok {
		fillFromJob(&data, job, requestedGPUs(s), s.prior.Accounting, p)
	} else if s.prior.StartTime != nil {
		if wait, err := c.waitForFinalRead(ctx, p, s); wait || err != nil {
			return wait, err
		}
		fillFromStored(&data, s.prior)
	}
	rec, err := v1alpha1.NewAccountingRecord(s.a.Namespace, s.a.UID, modelDeploymentName(s), data)
	if err != nil {
		return false, fmt.Errorf("accounting record: %w", err)
	}
	err = c.Client.Create(ctx, rec, client.FieldOwner(v1alpha1.FieldManagerProvider))
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create accounting record %s: %w", rec.Name, err)
	}
	return false, nil
}

// waitForFinalRead keeps the finalizer while a started job is absent from both
// sources and LostGrace has not passed since status.atProvider.absentSince,
// strictly as the Lost rule is (equal waits, a second more writes). The first
// such pass sets the anchor with a status write of that one field; a stored
// anchor, from a live pass or an earlier deleting one, is kept, so a restarted
// Controller keeps the wait.
func (c *Controller) waitForFinalRead(ctx context.Context, p *pass, s *allocState) (wait bool, err error) {
	if s.prior.AbsentSince == nil {
		obs := s.prior.DeepCopy()
		obs.AbsentSince = mt(p.now)
		s.a.Status.AtProvider = *obs // after the last Update, which restores the stored status
		if err := updateStatus(ctx, c.Client, s.a); err != nil {
			return false, fmt.Errorf("update status: %w", err)
		}
		return true, nil
	}
	return p.now.Sub(s.prior.AbsentSince.Time) <= c.lostGrace(), nil
}

// fillFromJob sets the hours fields and times of data from the recorded job.
func fillFromJob(data *v1alpha1.AccountingRecordData, job provider.Job, gpus int32, stored *v1alpha1.AllocationAccounting, p *pass) {
	if acct := provider.AccountingOf(job, gpus, stored, p.now); acct != nil {
		data.AllocatedCPUs, data.AllocatedGPUs, data.ElapsedSeconds = acct.AllocatedCPUs, acct.AllocatedGPUs, acct.ElapsedSeconds
	}
	data.StartTime, data.EndTime = mtp(job.StartTime), mtp(job.EndTime)
}

// fillFromStored sets what the stored status knew of a job no source shows any
// more: the allocated counts and the start time. The elapsed and end times are
// unknown, since the last stored values were a running snapshot.
func fillFromStored(data *v1alpha1.AccountingRecordData, stored *v1alpha1.AllocationObservation) {
	if acct := stored.Accounting; acct != nil {
		data.AllocatedCPUs, data.AllocatedGPUs = acct.AllocatedCPUs, acct.AllocatedGPUs
	}
	data.StartTime = stored.StartTime
}

// jobByID returns the job with the ID among those the pass read.
func jobByID(jobs []provider.Job, id string) (provider.Job, bool) {
	for _, j := range jobs {
		if j.ID == id {
			return j, true
		}
	}
	return provider.Job{}, false
}

// jobIDsWith returns the IDs of the jobs read, and the recorded job's ID when
// no read showed it (it carried the tag, so it belongs in the list), in numeric
// order. It sorts a copy and leaves jobs as it was.
func jobIDsWith(jobs []provider.Job, recorded string) []string {
	all := append([]provider.Job(nil), jobs...)
	if _, ok := jobByID(jobs, recorded); !ok {
		all = append(all, provider.Job{ID: recorded})
	}
	byID(all)
	ids := make([]string, len(all))
	for i, j := range all {
		ids[i] = j.ID
	}
	return ids
}

// modelDeploymentName is the ModelDeployment the Allocation is assigned to, or
// "" when it is assigned to none.
func modelDeploymentName(s *allocState) string {
	if ref := s.a.Spec.ModelDeploymentRef; ref != nil {
		return ref.Name
	}
	return ""
}

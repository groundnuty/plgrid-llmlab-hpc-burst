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
// It mirrors the real controller's step, with the same order of rules.
//
// The recorded job is the one the job-ID annotation names, else the job
// adoption would pick. With neither there is nothing to account, and no record
// is written. Then, in order:
//   - a requeued job (provider.AccountingRequeued of the recorded job read, with
//     the stored status's sticky flag; or the flag alone when no read showed it)
//     is written at once with its counts and elapsed time unknown, and its start
//     and end times from the read if the job was read, else from the stored
//     status;
//   - a read of the recorded job that shows it never started, or whose
//     provider.AccountingOf has the elapsed time, is written at once, from the
//     read: the stored accounting keeps the counts of an earlier read;
//   - else a known final observation in the stored status (provider.FinalKnown)
//     is written at once, from the stored status: a later absence or incomplete
//     read never erases it;
//   - else a recorded job no read showed, and the stored status never showed
//     started, is written at once with nothing known;
//   - else the step waits, for a recorded job that started, for the read that
//     gives the final elapsed time, whether no source shows the job or the read
//     of it lacks the elapsed time (incomplete). The wait is at most LostGrace
//     from status.atProvider.absentSince, which the first such pass sets (the
//     only status a deleting Allocation is given) and a live Lost pass may have
//     set earlier. After it the record is written with what is known and the
//     elapsed time unknown: from the read if the job was read, else the counts
//     and start time of the stored status, and its end time.
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
	gpus := requestedGPUs(s)
	if bp := s.a.Spec.ForProvider.Batch; bp != nil {
		data.Partition = bp.Partition
	}
	o := s.prior
	var recorded *provider.Job
	var read *v1alpha1.AllocationAccounting
	requeued := o.Requeued
	if job, ok := jobByID(jobs, id); ok {
		recorded = &job
		j := job
		j.Requeued = j.Requeued || o.Requeued
		requeued = provider.AccountingRequeued(j)
		read = provider.AccountingOf(j, gpus, o.Accounting, p.now)
	}
	fromRead := func() {
		data.StartTime, data.EndTime = mtp(recorded.StartTime), mtp(recorded.EndTime)
		fillAccounting(&data, read)
	}
	switch {
	case requeued:
		// Its counts and elapsed time are unknown however long it waits.
		data.StartTime, data.EndTime = o.StartTime, o.EndTime
		if recorded != nil {
			data.StartTime, data.EndTime = mtp(recorded.StartTime), mtp(recorded.EndTime)
		}
	case recorded != nil && (recorded.StartTime == nil || read.ElapsedSeconds != nil):
		fromRead()
	case provider.FinalKnown(o):
		data.StartTime, data.EndTime = o.StartTime, o.EndTime
		fillAccounting(&data, o.Accounting)
	case recorded == nil && o.StartTime == nil:
		// A job never seen started: no hours to wait for.
	default:
		if wait, err := c.waitForFinalRead(ctx, p, s); wait || err != nil {
			return wait, err
		}
		if recorded != nil {
			fromRead()
		} else {
			data.StartTime, data.EndTime = o.StartTime, o.EndTime
			fillAccounting(&data, provider.AccountingLost(&o.StartTime.Time, gpus, o.Accounting, p.now))
		}
	}
	rec, err := v1alpha1.NewAccountingRecord(s.a.Namespace, s.a.UID, data)
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
// sources, or read without its elapsed time, and LostGrace has not passed since
// status.atProvider.absentSince, strictly as the Lost rule is (equal waits, a
// second more writes). The first such pass sets the anchor with a status write
// of that one field; a stored anchor, from a live pass or an earlier deleting
// one, is kept, so a restarted Controller keeps the wait.
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

// fillAccounting copies the counts and the elapsed time into the record; nil
// leaves them unknown.
func fillAccounting(data *v1alpha1.AccountingRecordData, acct *v1alpha1.AllocationAccounting) {
	if acct != nil {
		data.AllocatedCPUs, data.AllocatedGPUs, data.ElapsedSeconds = acct.AllocatedCPUs, acct.AllocatedGPUs, acct.ElapsedSeconds
	}
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
